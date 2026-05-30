from __future__ import annotations

import json
import queue
import threading
import time as time_module
import urllib.error
import urllib.parse
import urllib.request
from itertools import count
from typing import Any

from homestock.models import HttpCallbackSpec
from homestock.redaction import redact_log_text
from homestock.ops_log import LogSource, ops_log


class CallbackDispatcher:
    _STOP = object()

    def __init__(
        self,
        logger: Any | None = None,
        max_attempts: int = 3,
        retry_delay_seconds: float = 2.0,
        request_timeout_seconds: float = 10.0,
    ) -> None:
        self._logger = logger
        self._max_attempts = max(max_attempts, 1)
        self._retry_delay_seconds = max(retry_delay_seconds, 0.0)
        self._request_timeout_seconds = max(request_timeout_seconds, 0.1)
        self._queue: queue.Queue[tuple[int, HttpCallbackSpec] | object] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._closed = False
        self._callback_ids = count(1)
        ops_log(LogSource.WEBHOOK,
            "CallbackDispatcher initialized "
            f"max_attempts={self._max_attempts} "
            f"retry_delay_seconds={self._retry_delay_seconds} "
            f"request_timeout_seconds={self._request_timeout_seconds}",
        )

    @property
    def drain_timeout_seconds(self) -> float:
        return (
            self._max_attempts * self._request_timeout_seconds
            + max(self._max_attempts - 1, 0) * self._retry_delay_seconds
            + 1.0
        )

    def dispatch(self, callback: HttpCallbackSpec) -> dict[str, Any]:
        callback_id = next(self._callback_ids)
        redacted_url = self._redact_url(callback.url)
        with self._thread_lock:
            if self._closed:
                ops_log(LogSource.WEBHOOK,
                    f"callback queue rejected id={callback_id} url={redacted_url} reason=dispatcher_closed",
                )
                return {"queued": False, "delivered": None, "error": "callback dispatcher closed"}
            self._ensure_worker_locked()
            self._queue.put_nowait((callback_id, callback))
            queue_size = self._queue.qsize()
        ops_log(LogSource.WEBHOOK,
            f"callback queued id={callback_id} method={callback.method} url={redacted_url} "
            f"body_present={callback.body is not None} body_format={callback.body_format or 'none'} "
            f"queue_size={queue_size}",
        )
        self._log_info(
            "HTTP callback queued id=%s method=%s url=%s body_present=%s body_format=%s queue_size=%s",
            callback_id,
            callback.method,
            redacted_url,
            callback.body is not None,
            callback.body_format or "none",
            queue_size,
        )
        return {"queued": True, "delivered": None, "error": None}

    def close(self, timeout: float = 1.0) -> None:
        ops_log(LogSource.WEBHOOK, f"CallbackDispatcher.close requested queue_size={self._queue.qsize()}")
        with self._thread_lock:
            if self._closed:
                ops_log(LogSource.WEBHOOK, "CallbackDispatcher.close skipped already_closed=True")
                return
            self._closed = True
            thread = self._thread
            if thread is not None:
                self._queue.put_nowait(self._STOP)
        if thread is not None:
            thread.join(timeout)
        ops_log(LogSource.WEBHOOK,
            "CallbackDispatcher.close complete "
            f"worker_alive={thread.is_alive() if thread is not None else False} "
            f"queue_size={self._queue.qsize()}",
        )

    def wait_for_idle(self, timeout: float = 1.0) -> bool:
        deadline = time_module.monotonic() + timeout
        while time_module.monotonic() < deadline:
            if self._queue.unfinished_tasks == 0:
                return True
            time_module.sleep(0.01)
        return self._queue.unfinished_tasks == 0

    def _ensure_worker_locked(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        ops_log(LogSource.WEBHOOK, "starting webhook dispatch worker")
        self._thread = threading.Thread(
            target=self._run_worker,
            name="homestock-webhook-dispatch",
            daemon=True,
        )
        self._thread.start()
        ops_log(LogSource.WEBHOOK, f"webhook dispatch worker started name={self._thread.name}")

    def _run_worker(self) -> None:
        ops_log(LogSource.WEBHOOK, f"webhook dispatch worker entered thread_id={threading.get_ident()}")
        while True:
            item = self._queue.get()
            try:
                if item is self._STOP:
                    ops_log(LogSource.WEBHOOK, "webhook dispatch worker stop received")
                    return
                assert isinstance(item, tuple)
                callback_id, callback = item
                self._dispatch_with_retries(callback_id, callback)
            finally:
                self._queue.task_done()

    def _dispatch_with_retries(self, callback_id: int, callback: HttpCallbackSpec) -> None:
        last_error: str | None = None
        redacted_url = self._redact_url(callback.url)
        ops_log(LogSource.WEBHOOK,
            f"callback delivery begin id={callback_id} method={callback.method} url={redacted_url} "
            f"max_attempts={self._max_attempts}",
        )
        for attempt in range(1, self._max_attempts + 1):
            attempt_started = time_module.monotonic()
            ops_log(LogSource.WEBHOOK,
                f"callback delivery attempt begin id={callback_id} url={redacted_url} "
                f"attempt={attempt}/{self._max_attempts}",
            )
            try:
                outcome = self._send_once(callback)
            except Exception as exc:
                outcome = {"delivered": False, "error": str(exc)}
            elapsed_ms = int((time_module.monotonic() - attempt_started) * 1000)
            if outcome.get("delivered"):
                ops_log(LogSource.WEBHOOK,
                    f"callback delivery success id={callback_id} url={redacted_url} "
                    f"attempt={attempt}/{self._max_attempts} elapsed_ms={elapsed_ms}",
                )
                self._log_info(
                    "HTTP callback delivered id=%s url=%s attempt=%s/%s elapsed_ms=%s",
                    callback_id,
                    redacted_url,
                    attempt,
                    self._max_attempts,
                    elapsed_ms,
                )
                return
            last_error = redact_log_text(str(outcome.get("error") or "unknown callback delivery failure"))
            if attempt < self._max_attempts:
                ops_log(LogSource.WEBHOOK,
                    f"callback delivery attempt failed id={callback_id} url={redacted_url} "
                    f"attempt={attempt}/{self._max_attempts} elapsed_ms={elapsed_ms} "
                    f"retry_delay_seconds={self._retry_delay_seconds} error={last_error}",
                )
                self._log_warning(
                    "HTTP callback failed for %s on attempt %s/%s; retrying: %s",
                    redacted_url,
                    attempt,
                    self._max_attempts,
                    last_error,
                )
                time_module.sleep(self._retry_delay_seconds)
        ops_log(LogSource.WEBHOOK,
            f"callback delivery final failure id={callback_id} url={redacted_url} "
            f"attempts={self._max_attempts} error={last_error}",
        )
        self._log_warning(
            "HTTP callback failed for %s after %s attempt(s): %s",
            redacted_url,
            self._max_attempts,
            last_error,
        )

    def _send_once(self, callback: HttpCallbackSpec) -> dict[str, Any]:
        request = self._build_request(callback)
        try:
            with urllib.request.urlopen(request, timeout=self._request_timeout_seconds) as response:
                response.read()
            return {"delivered": True, "error": None}
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return {"delivered": False, "error": str(exc)}

    def _log_info(self, message: str, *args: Any) -> None:
        safe_message = redact_log_text(message)
        safe_args = tuple(redact_log_text(str(arg)) for arg in args)
        if self._logger is not None:
            self._logger.info(safe_message, *safe_args)
            return
        ops_log(LogSource.WEBHOOK, safe_message % safe_args if safe_args else safe_message)

    def _log_warning(self, message: str, *args: Any) -> None:
        safe_message = redact_log_text(message)
        safe_args = tuple(redact_log_text(str(arg)) for arg in args)
        if self._logger is not None:
            self._logger.warning(safe_message, *safe_args)
            return
        ops_log(LogSource.WEBHOOK, safe_message % safe_args if safe_args else safe_message)

    def _build_request(self, callback: HttpCallbackSpec) -> urllib.request.Request:
        body_bytes: bytes | None = None
        headers = {key: value for key, value in callback.headers.items()}
        if callback.body is not None:
            body_format = callback.body_format or "json"
            if body_format == "text":
                body_bytes = str(callback.body).encode("utf-8")
                headers = self._with_default_content_type(headers, "text/plain; charset=utf-8")
            elif body_format == "json":
                body_bytes = json.dumps(callback.body, ensure_ascii=False).encode("utf-8")
                headers = self._with_default_content_type(headers, "application/json; charset=utf-8")
            else:
                body_bytes = urllib.parse.urlencode(callback.body, doseq=True).encode("utf-8")
                headers = self._with_default_content_type(
                    headers,
                    "application/x-www-form-urlencoded; charset=utf-8",
                )
        return urllib.request.Request(
            callback.url,
            data=body_bytes,
            headers=headers,
            method=callback.method,
        )

    @staticmethod
    def _redact_url(url: str) -> str:
        try:
            urllib.parse.urlsplit(url)
        except ValueError:
            return "<invalid-url>"
        return "<redacted-url>"

    @staticmethod
    def _with_default_content_type(headers: dict[str, str], content_type: str) -> dict[str, str]:
        if any(key.lower() == "content-type" for key in headers):
            return headers
        merged = dict(headers)
        merged["Content-Type"] = content_type
        return merged
