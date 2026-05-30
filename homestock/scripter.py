from __future__ import annotations

import copy
import json
import logging
import os
import queue
import re
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Protocol, TextIO
from zoneinfo import ZoneInfo

from homestock.models import HttpCallbackSpec
from homestock.ops_log import LogSource, clear_ops_log_sink, normalize_log_source, ops_log, set_ops_log_sink
from homestock.redaction import redact_for_output, redact_log_text
from homestock.webhook import CallbackDispatcher


try:
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9))


def _kst_now() -> datetime:
    return datetime.now(KST)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


SCRIPTER_LOGGER_NAME = "homestock.scripter"
SCRIPTER_LOG_FILE_NAME = "scripter.log"
SCRIPTER_LOG_RETENTION_DAYS = 5
SCRIPTER_LOG_LEVEL = "info"
SCRIPTER_CRASH_LOG_FILE_NAME = "last_crash_log.txt"
ISOLATE_PROCESS_LOGGER_NAME = SCRIPTER_LOGGER_NAME
ISOLATE_PROCESS_LOG_FILE_NAME = SCRIPTER_LOG_FILE_NAME
ISOLATE_PROCESS_LOG_RETENTION_DAYS = SCRIPTER_LOG_RETENTION_DAYS
HEARTBEAT_SILENT_TIMEOUT_SECONDS = 180
HEARTBEAT_SILENT_TIMEOUT = timedelta(seconds=HEARTBEAT_SILENT_TIMEOUT_SECONDS)
HEARTBEAT_SILENT_REPEAT_INTERVAL_SECONDS = HEARTBEAT_SILENT_TIMEOUT_SECONDS
HEARTBEAT_SILENT_REPEAT_INTERVAL = timedelta(seconds=HEARTBEAT_SILENT_REPEAT_INTERVAL_SECONDS)
HEARTBEAT_SILENT_EVENT_TYPE = "heartbeat_silent"
HEARTBEAT_REVIVED_EVENT_TYPE = "heartbeat_revived"
HEARTBEAT_LOCK_TIMEOUT_SECONDS = HEARTBEAT_SILENT_TIMEOUT_SECONDS
HEARTBEAT_LOCK_TIMEOUT = HEARTBEAT_SILENT_TIMEOUT
HEARTBEAT_LOCK_REPEAT_INTERVAL_SECONDS = HEARTBEAT_SILENT_REPEAT_INTERVAL_SECONDS
HEARTBEAT_LOCK_REPEAT_INTERVAL = HEARTBEAT_SILENT_REPEAT_INTERVAL
HEARTBEAT_LOCK_EVENT_TYPE = HEARTBEAT_SILENT_EVENT_TYPE
HEARTBEAT_UNLOCK_EVENT_TYPE = HEARTBEAT_REVIVED_EVENT_TYPE
HEARTBEAT_SILENT_ALERT_LIMIT = 5
HEARTBEAT_STATUS_LOG_INTERVAL_SECONDS = 120
HEARTBEAT_STATUS_LOG_INTERVAL = timedelta(seconds=HEARTBEAT_STATUS_LOG_INTERVAL_SECONDS)
SCRIPTER_EVENT_QUEUE_MAXSIZE = 1000
SCRIPTER_DEBUG_WRITER_QUEUE_MULTIPLIER = 100
SCRIPTER_INTERVAL_SECONDS = 60
SCRIPTER_QUEUE_OVERFLOW_EVENT_TYPE = "scripter_queue_overflow"
ISOLATE_PROCESS_START_TIMEOUT_SECONDS = 5.0
FATAL_SYSTEM_CALLBACK_DRAIN_TIMEOUT_SECONDS = 10.0
_LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
    "fatal": logging.CRITICAL,
}


def _normalize_log_level(level: str) -> str:
    normalized = str(level or "info").strip().lower()
    return normalized if normalized in _LOG_LEVELS else "info"


def _log_level_value(level: str) -> int:
    return _LOG_LEVELS[_normalize_log_level(level)]


def _log_level_enabled(level: str, minimum_level: str) -> bool:
    return _log_level_value(level) >= _log_level_value(minimum_level)


def _writer_queue_maxsize_for_log_level(maxsize: int, log_level: str) -> int:
    resolved = max(maxsize, 1)
    if _normalize_log_level(log_level) == "debug":
        return resolved * SCRIPTER_DEBUG_WRITER_QUEUE_MULTIPLIER
    return resolved


class _RedactingLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact_log_text(super().format(record))


def _scripter_log_formatter() -> logging.Formatter:
    return _RedactingLogFormatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
    )


@dataclass(frozen=True)
class ScripterLogConfig:
    log_dir: str | Path = Path(".runtime") / "scripter"
    retention_days: int = SCRIPTER_LOG_RETENTION_DAYS
    logger_name: str = SCRIPTER_LOGGER_NAME
    file_name: str = SCRIPTER_LOG_FILE_NAME
    log_level: str = SCRIPTER_LOG_LEVEL


def _close_logger_handlers(logger: logging.Logger) -> None:
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def configure_logger(
    config: ScripterLogConfig | None = None,
    *,
    stream: TextIO | None = None,
) -> logging.Logger:
    resolved = config or ScripterLogConfig()
    log_path = Path(resolved.log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    backup_count = max(resolved.retention_days - 1, 0)
    handler_level = _log_level_value(resolved.log_level)

    logger = logging.getLogger(resolved.logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    _close_logger_handlers(logger)

    stdout_handler = logging.StreamHandler(stream or sys.stdout)
    stdout_handler.setLevel(handler_level)
    stdout_handler.setFormatter(_scripter_log_formatter())

    file_handler = TimedRotatingFileHandler(
        log_path / resolved.file_name,
        when="midnight",
        interval=1,
        backupCount=backup_count,
        encoding="utf-8",
        utc=False,
    )
    file_handler.setLevel(handler_level)
    file_handler.setFormatter(_scripter_log_formatter())

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)
    return logger


def write_crash_log(
    *,
    role: str,
    source: str,
    message: str,
    exc: BaseException | None = None,
    log_dir: str | Path | None = None,
    extra: dict[str, Any] | None = None,
) -> Path | None:
    timestamp = _kst_now().isoformat()
    trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)) if exc is not None else ""
    lines = [
        f"timestamp={timestamp}",
        f"role={role}",
        f"source={redact_log_text(source)}",
        f"message={redact_log_text(message)}",
        f"pid={os.getpid()}",
        f"cwd={Path.cwd()}",
        f"python_executable={sys.executable}",
        f"log_dir={log_dir if log_dir is not None else '<default>'}",
    ]
    if exc is not None:
        lines.extend(
            [
                f"exception_type={exc.__class__.__name__}",
                f"exception_message={redact_log_text(str(exc))}",
            ]
        )
    if extra:
        lines.append("extra=" + json.dumps(redact_for_output(extra), ensure_ascii=False, default=str))
    if trace:
        lines.extend(["traceback:", redact_log_text(trace)])
    content = "\n".join(lines).rstrip() + "\n"

    candidate_dirs: list[Path] = []
    if log_dir is not None:
        candidate_dirs.append(Path(log_dir))
    candidate_dirs.extend([Path(".runtime") / "scripter", Path(".runtime"), Path.cwd()])
    for directory in candidate_dirs:
        try:
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / SCRIPTER_CRASH_LOG_FILE_NAME
            path.write_text(content, encoding="utf-8")
            return path
        except Exception:
            continue
    return None


def is_heartbeat_silent(
    last_heartbeat_at: datetime,
    *,
    now: datetime | None = None,
    timeout: timedelta = HEARTBEAT_SILENT_TIMEOUT,
) -> bool:
    current = now or datetime.now(last_heartbeat_at.tzinfo)
    return current - last_heartbeat_at >= timeout


is_heartbeat_locked = is_heartbeat_silent


@dataclass(frozen=True)
class ScripterEvent:
    kind: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    recorded_at: str = field(default_factory=_utc_timestamp)

    def to_dict(self, *, redact: bool = False) -> dict[str, Any]:
        payload = redact_for_output(self.payload) if redact else self.payload
        return {
            "kind": redact_log_text(self.kind) if redact else self.kind,
            "event_type": redact_log_text(self.event_type) if redact else self.event_type,
            "payload": payload,
            "recorded_at": redact_log_text(self.recorded_at) if redact else self.recorded_at,
        }


def _validate_system_callback_configs(callbacks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(callbacks, list):
        raise ValueError("system callback config callbacks must be a list")
    validated: list[dict[str, Any]] = []
    for index, raw in enumerate(callbacks):
        if not isinstance(raw, dict):
            raise ValueError(f"system callback config entry {index} must be an object")
        callback = raw.get("httpCallback")
        if not isinstance(callback, dict):
            raise ValueError(f"system callback config entry {index} missing httpCallback")
        method = str(callback.get("method") or "").strip()
        url = str(callback.get("url") or "").strip()
        headers = callback.get("headers")
        body = callback.get("body")
        if not method:
            raise ValueError(f"system callback config entry {index} missing httpCallback.method")
        if not url:
            raise ValueError(f"system callback config entry {index} missing httpCallback.url")
        if headers is not None and not isinstance(headers, dict):
            raise ValueError(f"system callback config entry {index} httpCallback.headers must be an object")
        if body is not None and not isinstance(body, dict):
            raise ValueError(f"system callback config entry {index} httpCallback.body must be an object")
        validated.append(copy.deepcopy(raw))
    return validated


def _log_payload(
    level: str,
    source: str,
    message: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "level": _normalize_log_level(level),
        "source": normalize_log_source(source),
        "message": message,
        "payload": copy.deepcopy(payload or {}),
    }


def _system_callback_payload(
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"message": message}
    if details:
        payload["details"] = copy.deepcopy(details)
    return payload


class Scripter(Protocol):
    def start(self) -> None:
        ...

    def log(
        self,
        level: str,
        source: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        ...

    def error(
        self,
        source: str,
        message: str,
        exc: BaseException | None = None,
        callstack: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        ...

    def system_callback(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        ...

    def heartbeat(self, name: str, payload: dict[str, Any] | None = None) -> None:
        ...

    def configure_system_callbacks(self, callbacks: list[dict[str, Any]]) -> None:
        ...

    def close(self) -> None:
        ...


class HeartbeatSilenceMonitor:
    def __init__(
        self,
        heartbeat_name: str,
        scripter: Scripter,
        *,
        started_at: datetime | None = None,
        timeout: timedelta = HEARTBEAT_SILENT_TIMEOUT,
        repeat_interval: timedelta = HEARTBEAT_SILENT_REPEAT_INTERVAL,
        status_log_interval: timedelta = HEARTBEAT_STATUS_LOG_INTERVAL,
        silent_alert_limit: int = HEARTBEAT_SILENT_ALERT_LIMIT,
        logger: logging.Logger | None = None,
    ) -> None:
        if timeout <= timedelta(0):
            raise ValueError("timeout must be positive")
        if repeat_interval <= timedelta(0):
            raise ValueError("repeat_interval must be positive")
        if status_log_interval <= timedelta(0):
            raise ValueError("status_log_interval must be positive")
        if silent_alert_limit < 0:
            raise ValueError("silent_alert_limit must not be negative")
        self._heartbeat_name = heartbeat_name
        self._scripter = scripter
        self._timeout = timeout
        self._repeat_interval = repeat_interval
        self._status_log_interval = status_log_interval
        self._silent_alert_limit = silent_alert_limit
        self._logger = logger
        self._last_heartbeat_at = started_at or _kst_now()
        self._locked = False
        self._locked_since_at: datetime | None = None
        self._last_lock_report_at: datetime | None = None
        self._silent_alert_count = 0
        self._silent_alert_suppression_logged = False
        self._last_status_log_at = self._last_heartbeat_at
        self._heartbeat_seen_since_status_log = False
        self._lock = threading.RLock()

    def heartbeat(
        self,
        payload: dict[str, Any] | None = None,
        *,
        at: datetime | None = None,
        last_event_action: dict[str, Any] | None = None,
    ) -> None:
        current = at or self._now_like(self._last_heartbeat_at)
        unlock_payload: dict[str, Any] | None = None
        status_log_payload: dict[str, Any] | None = None
        with self._lock:
            was_locked = self._locked
            previous_heartbeat_at = self._last_heartbeat_at
            locked_since_at = self._locked_since_at
            last_lock_report_at = self._last_lock_report_at
            self._heartbeat_seen_since_status_log = True
            self._last_heartbeat_at = current
            status_log_payload = self._take_status_log_payload(current)
            self._locked = False
            self._locked_since_at = None
            self._last_lock_report_at = None
            self._silent_alert_count = 0
            self._silent_alert_suppression_logged = False
            if was_locked:
                unlock_payload = self._unlock_payload(
                    current,
                    previous_heartbeat_at,
                    locked_since_at,
                    last_lock_report_at,
                    last_event_action,
                )
        self._safe_call(self._scripter.heartbeat, self._heartbeat_name, payload)
        self._log_heartbeat_status(status_log_payload)
        if unlock_payload is not None:
            self._safe_call(
                self._scripter.system_callback,
                HEARTBEAT_REVIVED_EVENT_TYPE,
                unlock_payload["message"],
                unlock_payload["details"],
            )
            if self._logger:
                self._logger.info(
                    "Heartbeat revived name=%s seconds_since_heartbeat=%s",
                    self._heartbeat_name,
                    unlock_payload["details"]["seconds_since_heartbeat"],
                )

    def on_interval(
        self,
        *,
        now: datetime | None = None,
        last_event_action: dict[str, Any] | None = None,
    ) -> bool:
        current = now or self._now_like(self._last_heartbeat_at)
        payload: dict[str, Any] | None = None
        status_log_payload: dict[str, Any] | None = None
        with self._lock:
            status_log_payload = self._take_status_log_payload(current)
            locked_now = is_heartbeat_silent(self._last_heartbeat_at, now=current, timeout=self._timeout)
            if not locked_now:
                self._locked = False
                self._last_lock_report_at = None
                self._log_heartbeat_status(status_log_payload)
                return False

            repeat = self._locked
            if repeat and self._last_lock_report_at is not None:
                if current - self._last_lock_report_at < self._repeat_interval:
                    self._log_heartbeat_status(status_log_payload)
                    return False

            self._locked = True
            if self._locked_since_at is None:
                self._locked_since_at = current
            self._last_lock_report_at = current
            if self._silent_alert_count >= self._silent_alert_limit:
                if not self._silent_alert_suppression_logged and self._logger:
                    self._logger.warning(
                        "Heartbeat silent alert limit reached name=%s limit=%s; suppressing further alerts",
                        self._heartbeat_name,
                        self._silent_alert_limit,
                    )
                self._silent_alert_suppression_logged = True
                payload = None
            else:
                self._silent_alert_count += 1
                payload = self._lock_payload(current, repeat, last_event_action)

        self._log_heartbeat_status(status_log_payload)
        if payload is None:
            return False
        self._safe_call(
            self._scripter.system_callback,
            HEARTBEAT_SILENT_EVENT_TYPE,
            payload["message"],
            payload["details"],
        )
        if self._logger:
            self._logger.warning(
                "Heartbeat %s name=%s seconds_since_heartbeat=%s silent_alert_count=%s/%s",
                "still silent" if payload["details"]["repeat"] else "silent",
                self._heartbeat_name,
                payload["details"]["seconds_since_heartbeat"],
                payload["details"]["silent_alert_count"],
                payload["details"]["silent_alert_limit"],
            )
        return True

    def check(
        self,
        *,
        now: datetime | None = None,
        last_event_action: dict[str, Any] | None = None,
    ) -> bool:
        return self.on_interval(now=now, last_event_action=last_event_action)

    def _lock_payload(
        self,
        current: datetime,
        repeat: bool,
        last_event_action: dict[str, Any] | None,
    ) -> dict[str, Any]:
        seconds_since_heartbeat = int((current - self._last_heartbeat_at).total_seconds())
        details: dict[str, Any] = {
            "heartbeat_name": self._heartbeat_name,
            "last_heartbeat_at": self._last_heartbeat_at.isoformat(),
            "checked_at": current.isoformat(),
            "seconds_since_heartbeat": seconds_since_heartbeat,
            "timeout_seconds": int(self._timeout.total_seconds()),
            "repeat_interval_seconds": int(self._repeat_interval.total_seconds()),
            "silent_alert_count": self._silent_alert_count,
            "silent_alert_limit": self._silent_alert_limit,
            "repeat": repeat,
        }
        if last_event_action is not None:
            details["last_event_action"] = copy.deepcopy(last_event_action)
        return {
            "message": f"heartbeat {'still silent' if repeat else 'silent'}: {self._heartbeat_name}",
            "details": details,
        }

    def _unlock_payload(
        self,
        current: datetime,
        previous_heartbeat_at: datetime,
        locked_since_at: datetime | None,
        last_lock_report_at: datetime | None,
        last_event_action: dict[str, Any] | None,
    ) -> dict[str, Any]:
        seconds_since_heartbeat = int((current - previous_heartbeat_at).total_seconds())
        details: dict[str, Any] = {
            "heartbeat_name": self._heartbeat_name,
            "last_heartbeat_at": previous_heartbeat_at.isoformat(),
            "recovered_heartbeat_at": current.isoformat(),
            "seconds_since_heartbeat": seconds_since_heartbeat,
            "timeout_seconds": int(self._timeout.total_seconds()),
        }
        if locked_since_at is not None:
            details["silent_since_at"] = locked_since_at.isoformat()
            details["seconds_silent"] = int((current - locked_since_at).total_seconds())
        if last_lock_report_at is not None:
            details["last_silent_report_at"] = last_lock_report_at.isoformat()
        if last_event_action is not None:
            details["last_event_action"] = copy.deepcopy(last_event_action)
        return {
            "message": f"heartbeat revived: {self._heartbeat_name}",
            "details": details,
        }

    def _take_status_log_payload(self, current: datetime) -> dict[str, Any] | None:
        if current - self._last_status_log_at < self._status_log_interval:
            return None
        heartbeat_seen = self._heartbeat_seen_since_status_log
        self._heartbeat_seen_since_status_log = False
        status = "received" if heartbeat_seen else "missing"
        payload = {
            "status": status,
            "heartbeat_name": self._heartbeat_name,
            "window_started_at": self._last_status_log_at.isoformat(),
            "checked_at": current.isoformat(),
            "last_heartbeat_at": self._last_heartbeat_at.isoformat(),
            "seconds_since_heartbeat": int((current - self._last_heartbeat_at).total_seconds()),
            "status_log_interval_seconds": int(self._status_log_interval.total_seconds()),
        }
        self._last_status_log_at = current
        return payload

    def _log_heartbeat_status(self, payload: dict[str, Any] | None) -> None:
        if payload is None or self._logger is None:
            return
        log = self._logger.info if payload["status"] == "received" else self._logger.warning
        log(
            "Heartbeat %s name=%s window_started_at=%s checked_at=%s "
            "last_heartbeat_at=%s seconds_since_heartbeat=%s",
            "beating" if payload["status"] == "received" else "silent",
            payload["heartbeat_name"],
            payload["window_started_at"],
            payload["checked_at"],
            payload["last_heartbeat_at"],
            payload["seconds_since_heartbeat"],
        )

    def _safe_call(self, func: Any, *args: Any) -> None:
        try:
            func(*args)
        except Exception:
            if self._logger:
                self._logger.exception("Scripter heartbeat silence monitor call failed")

    @staticmethod
    def _now_like(reference: datetime) -> datetime:
        if reference.tzinfo is None:
            return datetime.now()
        return datetime.now(reference.tzinfo)


HeartbeatLockMonitor = HeartbeatSilenceMonitor


class NoopScripter:
    def start(self) -> None:
        clear_ops_log_sink()
        return None

    def log(
        self,
        level: str,
        source: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        return None

    def error(
        self,
        source: str,
        message: str,
        exc: BaseException | None = None,
        callstack: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        return None

    def system_callback(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        return None

    def heartbeat(self, name: str, payload: dict[str, Any] | None = None) -> None:
        return None

    def configure_system_callbacks(self, callbacks: list[dict[str, Any]]) -> None:
        return None

    def close(self) -> None:
        clear_ops_log_sink()
        return None


class _RuntimeMonitorEmitter:
    def __init__(self, runtime: "_ScripterRuntime") -> None:
        self._runtime = runtime

    def start(self) -> None:
        return None

    def log(
        self,
        level: str,
        source: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._runtime.dispatch_log_now(level, source, message, payload)

    def error(
        self,
        source: str,
        message: str,
        exc: BaseException | None = None,
        callstack: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._runtime.dispatch_error_now(source, message, exc=exc, callstack=callstack, payload=payload)

    def system_callback(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._runtime.dispatch_system_callback_now(event_type, message, details)

    def heartbeat(self, name: str, payload: dict[str, Any] | None = None) -> None:
        return None

    def configure_system_callbacks(self, callbacks: list[dict[str, Any]]) -> None:
        return None

    def close(self) -> None:
        return None


class _ScripterRuntime:
    _REPLACEMENT_PATTERN = re.compile(r"\{\{([a-zA-Z0-9_]+)\}\}")

    def __init__(
        self,
        logger: logging.Logger | None = None,
        dispatcher: CallbackDispatcher | None = None,
        *,
        queue_maxsize: int = SCRIPTER_EVENT_QUEUE_MAXSIZE,
        interval_seconds: float = SCRIPTER_INTERVAL_SECONDS,
        fatal_on_worker_error: bool = False,
        crash_log_dir: str | Path | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)
        self._dispatcher = dispatcher or CallbackDispatcher(self._logger)
        self._callbacks: list[dict[str, Any]] = []
        self._lock = threading.RLock()
        self._queue: queue.Queue[ScripterEvent] = queue.Queue(maxsize=max(queue_maxsize, 1))
        self._closed = threading.Event()
        self._worker_thread: threading.Thread | None = None
        self._interval_thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._interval_seconds = max(interval_seconds, 0.1)
        self._monitors: dict[str, HeartbeatSilenceMonitor] = {}
        self._monitor_emitter = _RuntimeMonitorEmitter(self)
        self._last_event_action: dict[str, Any] | None = None
        self._queue_overflow_alert_active = False
        self._delivery_stats: dict[str, dict[str, Any]] = {}
        self._fatal_on_worker_error = fatal_on_worker_error
        self._crash_log_dir = crash_log_dir

    def log(
        self,
        level: str,
        source: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._enqueue(ScripterEvent("log", source, _log_payload(level, source, message, payload)))

    def error(
        self,
        source: str,
        message: str,
        exc: BaseException | None = None,
        callstack: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._enqueue(ScripterEvent("error", source, self._error_payload(source, message, exc, callstack, payload)))

    def system_callback(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._enqueue(ScripterEvent("system_callback", event_type, _system_callback_payload(message, details)))

    def heartbeat(self, name: str, payload: dict[str, Any] | None = None) -> None:
        self._ensure_interval_started()
        self._enqueue(ScripterEvent("heartbeat", name, copy.deepcopy(payload or {})))

    def configure_system_callbacks(self, callbacks: list[dict[str, Any]]) -> None:
        if self._closed.is_set():
            raise RuntimeError("Scripter runtime is closed")
        copied = _validate_system_callback_configs(callbacks)
        with self._lock:
            self._callbacks = copied
            self._prune_delivery_stats_locked(copied)
        self._logger.info("Scripter system callbacks configured count=%s", len(copied))

    def close(self, timeout: float = 5.0) -> None:
        drain_timeout = max(timeout, getattr(self._dispatcher, "drain_timeout_seconds", timeout))
        self._closed.set()
        self.wait_for_idle(timeout=drain_timeout)
        worker = self._worker_thread
        if worker is not None:
            worker.join(timeout=timeout)
        interval = self._interval_thread
        if interval is not None:
            interval.join(timeout=min(timeout, 1.0))
        self._dispatcher.close(timeout=drain_timeout)

    def wait_for_idle(self, timeout: float = 1.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._queue.unfinished_tasks == 0:
                remaining = max(deadline - time.monotonic(), 0.0)
                return self._dispatcher.wait_for_idle(timeout=remaining)
            time.sleep(0.01)
        return self._queue.unfinished_tasks == 0 and self._dispatcher.wait_for_idle(timeout=0.0)

    def on_interval(self, *, now: datetime | None = None) -> None:
        current = now or _kst_now()
        with self._lock:
            monitors = list(self._monitors.values())
            last_event_action = copy.deepcopy(self._last_event_action)
        for monitor in monitors:
            monitor.on_interval(now=current, last_event_action=last_event_action)

    def dispatch_log_now(
        self,
        level: str,
        source: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event = ScripterEvent("log", source, _log_payload(level, source, message, payload))
        self._remember_event_action(event)
        self._log_event(event)

    def dispatch_error_now(
        self,
        source: str,
        message: str,
        exc: BaseException | None = None,
        callstack: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event = ScripterEvent("error", source, self._error_payload(source, message, exc, callstack, payload))
        self._remember_event_action(event)
        self._log_error_event(event)

    def dispatch_error_payload_now(self, event_type: str, payload: dict[str, Any]) -> None:
        event = ScripterEvent("error", event_type, copy.deepcopy(payload))
        self._remember_event_action(event)
        self._log_error_event(event)

    def dispatch_system_callback_now(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        event = ScripterEvent("system_callback", event_type, _system_callback_payload(message, details))
        self._remember_event_action(event)
        self._dispatch_system_callback(event.event_type, message, details)

    def dispatch_heartbeat_now(self, name: str, payload: dict[str, Any] | None = None) -> None:
        self._ensure_interval_started()
        event = ScripterEvent("heartbeat", name, copy.deepcopy(payload or {}))
        self._remember_event_action(event)
        monitor = self._heartbeat_monitor(event.event_type)
        with self._lock:
            last_event_action = copy.deepcopy(self._last_event_action)
        monitor.heartbeat(event.payload, last_event_action=last_event_action)

    def log_ops(
        self,
        source: str,
        message: str,
        level: str = "info",
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.dispatch_log_now(level, source, message, payload)

    def log_startup(self, source: str, message: str, level: str = "info") -> None:
        self.log_ops(source, message, level)

    def _enqueue(self, event: ScripterEvent) -> None:
        self._remember_event_action(event)
        if self._closed.is_set():
            self._logger.warning(
                "Scripter event dropped after close kind=%s event_type=%s",
                redact_log_text(event.kind),
                redact_log_text(event.event_type),
            )
            return
        self._ensure_worker_started()
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            self._handle_queue_overflow(event)

    def _ensure_worker_started(self) -> None:
        with self._thread_lock:
            if self._worker_thread is not None and self._worker_thread.is_alive():
                return
            self._worker_thread = threading.Thread(
                target=self._run_worker,
                name="homestock-scripter-runtime",
                daemon=True,
            )
            self._worker_thread.start()

    def _ensure_interval_started(self) -> None:
        with self._thread_lock:
            if self._interval_thread is not None and self._interval_thread.is_alive():
                return
            self._interval_thread = threading.Thread(
                target=self._run_interval_loop,
                name="homestock-scripter-interval",
                daemon=True,
            )
            self._interval_thread.start()

    def _run_worker(self) -> None:
        while True:
            if self._closed.is_set() and self._queue.empty():
                return
            try:
                event = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                self._handle_event(event)
            except Exception as exc:
                self._logger.exception(
                    "Scripter runtime event handling failed kind=%s event_type=%s",
                    redact_log_text(event.kind),
                    redact_log_text(event.event_type),
                )
                if self._fatal_on_worker_error:
                    self._fatal_worker_error(exc, event)
            finally:
                if self._queue.qsize() == 0:
                    self._queue_overflow_alert_active = False
                self._queue.task_done()

    def _run_interval_loop(self) -> None:
        while not self._closed.wait(self._interval_seconds):
            try:
                self.on_interval()
            except Exception as exc:
                self._logger.exception("Scripter runtime interval failed")
                if self._fatal_on_worker_error:
                    self._fatal_worker_error(exc, ScripterEvent("interval", "scripter_interval", {}))

    def _handle_event(self, event: ScripterEvent) -> None:
        if event.kind == "startup_log":
            self.log_ops(
                event.event_type,
                str(event.payload.get("message") or ""),
                str(event.payload.get("level") or "info"),
                event.payload.get("payload") if isinstance(event.payload.get("payload"), dict) else None,
            )
            return
        if event.kind == "log":
            self._log_event(event)
            return
        if event.kind == "error":
            self._log_error_event(event)
            return
        if event.kind == "system_callback":
            message = str(event.payload.get("message") or event.event_type)
            details = self._extract_details(event.payload)
            self._logger.info("Scripter system callback action event_type=%s", redact_log_text(event.event_type))
            self._dispatch_system_callback(event.event_type, message, details)
            return
        if event.kind == "heartbeat":
            monitor = self._heartbeat_monitor(event.event_type)
            with self._lock:
                last_event_action = copy.deepcopy(self._last_event_action)
            monitor.heartbeat(event.payload, last_event_action=last_event_action)
            return
        if event.kind == "config" and event.event_type == "system_callbacks":
            callbacks = event.payload.get("callbacks")
            if not isinstance(callbacks, list):
                self._logger.warning("Ignoring malformed system callback config event")
                return
            try:
                copied = _validate_system_callback_configs(callbacks)
            except ValueError as exc:
                self._logger.warning(
                    "Ignoring malformed system callback config event: %s",
                    redact_log_text(str(exc)),
                )
                return
            with self._lock:
                self._callbacks = copied
                self._prune_delivery_stats_locked(copied)
            self._logger.info("Scripter system callbacks configured count=%s", len(copied))
            return
        self._logger.warning(
            "Scripter unknown event ignored kind=%s event_type=%s",
            redact_log_text(event.kind),
            redact_log_text(event.event_type),
        )

    def _log_event(self, event: ScripterEvent) -> None:
        level = _normalize_log_level(str(event.payload.get("level") or "info"))
        source = str(event.payload.get("source") or event.event_type)
        message = redact_log_text(str(event.payload.get("message") or ""))
        payload = event.payload.get("payload")
        self._logger.log(_log_level_value(level), "%s %s", redact_log_text(source), message)
        if isinstance(payload, dict) and payload:
            self._logger.log(
                _log_level_value(level),
                "%s details=%s",
                redact_log_text(source),
                json.dumps(redact_for_output(payload), ensure_ascii=False, default=str),
            )

    def _log_error_event(self, event: ScripterEvent) -> None:
        source = str(event.payload.get("source") or event.event_type)
        message = redact_log_text(str(event.payload.get("message") or ""))
        exception_type = redact_log_text(str(event.payload.get("exception_type") or ""))
        error = redact_log_text(str(event.payload.get("error") or ""))
        callstack = redact_log_text(str(event.payload.get("callstack") or ""))
        payload = event.payload.get("payload")
        suffix = f" exception_type={exception_type} error={error}" if exception_type or error else ""
        self._logger.error("%s %s%s", redact_log_text(source), message, suffix)
        if isinstance(payload, dict) and payload:
            self._logger.error(
                "%s details=%s",
                redact_log_text(source),
                json.dumps(redact_for_output(payload), ensure_ascii=False, default=str),
            )
        if callstack:
            self._logger.error("%s callstack=%s", redact_log_text(source), callstack)

    def _heartbeat_monitor(self, name: str) -> HeartbeatSilenceMonitor:
        with self._lock:
            monitor = self._monitors.get(name)
            if monitor is None:
                monitor = HeartbeatSilenceMonitor(name, self._monitor_emitter, logger=self._logger)
                self._monitors[name] = monitor
            return monitor

    def _remember_event_action(self, event: ScripterEvent) -> None:
        payload_keys = list(event.payload.keys())[:20]
        summary: dict[str, Any] = {
            "kind": event.kind,
            "event_type": event.event_type,
            "recorded_at": event.recorded_at,
        }
        if payload_keys:
            summary["payload_keys"] = payload_keys
        message = event.payload.get("message")
        if isinstance(message, str) and message:
            summary["message"] = message[:200]
        with self._lock:
            self._last_event_action = redact_for_output(summary)

    def _handle_queue_overflow(self, dropped_event: ScripterEvent) -> None:
        with self._lock:
            already_alerted = self._queue_overflow_alert_active
            self._queue_overflow_alert_active = True
            last_event_action = copy.deepcopy(self._last_event_action)
        self._logger.warning(
            "Scripter event queue overflow; dropping event kind=%s event_type=%s queue_size=%s queue_maxsize=%s",
            redact_log_text(dropped_event.kind),
            redact_log_text(dropped_event.event_type),
            self._queue.qsize(),
            self._queue.maxsize,
        )
        if already_alerted:
            return
        self.dispatch_system_callback_now(
            SCRIPTER_QUEUE_OVERFLOW_EVENT_TYPE,
            "Scripter event queue overflow",
            {
                "queue_size": self._queue.qsize(),
                "queue_maxsize": self._queue.maxsize,
                "dropped_event_kind": dropped_event.kind,
                "dropped_event_type": dropped_event.event_type,
                "last_event_action": last_event_action,
            },
        )

    def _dispatch_system_callback(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        safe_event_type = redact_log_text(event_type)
        with self._lock:
            callbacks = self._callbacks
            if not callbacks:
                return
            occurred_at = _kst_now().strftime("%Y%m%d%H%M%S")
            default_body: dict[str, Any] = {
                "event_type": event_type,
                "message": message,
                "occurred_at": occurred_at,
            }
            if details:
                default_body["details"] = details
            replacements = self._system_replacements(event_type, message, occurred_at, details)
            for raw in callbacks:
                callback_raw = raw.get("httpCallback")
                if not isinstance(callback_raw, dict):
                    self._logger.warning(
                        "Skipping malformed system callback config system_callback_id=%s",
                        redact_log_text(str(raw.get("system_callback_id") or "")),
                    )
                    continue
                system_callback_id = str(raw.get("system_callback_id") or "")
                callback = self._http_callback_from_dict(callback_raw)
                if callback.body is None:
                    rendered_callback = HttpCallbackSpec(
                        method=callback.method,
                        url=callback.url,
                        headers=dict(callback.headers),
                        body=copy.deepcopy(default_body),
                        body_format="json",
                    )
                else:
                    rendered_callback = self._render_http_callback(callback, replacements)
                dispatch_result = self._dispatcher.dispatch(rendered_callback) or {}
                stats_key = system_callback_id or f"callback:{id(raw)}"
                stats = self._delivery_stats.setdefault(stats_key, {"sent_event_count": 0})
                stats["last_event_at"] = occurred_at
                stats["sent_event_count"] = int(stats.get("sent_event_count") or 0) + 1
                ops_log(
                    LogSource.SCRIPTER,
                    f"system callback queued event_type={event_type} "
                    f"system_callback_id={system_callback_id} "
                    f"queued={dispatch_result.get('queued')}",
                )
                self._logger.info(
                    "System callback queued event_type=%s system_callback_id=%s queued=%s error=%s",
                    safe_event_type,
                    system_callback_id,
                    dispatch_result.get("queued"),
                    redact_log_text(str(dispatch_result.get("error") or "")),
                )
                if dispatch_result.get("queued") is False:
                    self._logger.warning(
                        "System callback queueing failed for %s: %s",
                        system_callback_id,
                        redact_log_text(str(dispatch_result.get("error") or "")),
                    )

    def _fatal_worker_error(self, exc: BaseException, event: ScripterEvent) -> None:
        details = {
            "exception_type": exc.__class__.__name__,
            "error": str(exc),
            "failed_event_kind": event.kind,
            "failed_event_type": event.event_type,
        }
        write_crash_log(
            role="scripter_child",
            source="scripter_child.runtime_worker",
            message="Scripter child runtime worker fatal",
            exc=exc,
            log_dir=self._crash_log_dir,
            extra=details,
        )
        try:
            self.dispatch_system_callback_now(
                "scripter_child_fatal",
                "Scripter child runtime fatal",
                details,
            )
            drain_timeout = max(
                FATAL_SYSTEM_CALLBACK_DRAIN_TIMEOUT_SECONDS,
                getattr(self._dispatcher, "drain_timeout_seconds", FATAL_SYSTEM_CALLBACK_DRAIN_TIMEOUT_SECONDS),
            )
            self._dispatcher.wait_for_idle(timeout=drain_timeout)
        except Exception:
            self._logger.exception("Scripter child fatal system callback failed")
        os._exit(1)

    def _prune_delivery_stats_locked(self, callbacks: list[dict[str, Any]]) -> None:
        active_ids = {str(raw.get("system_callback_id") or "") for raw in callbacks}
        active_ids.discard("")
        if not active_ids:
            self._delivery_stats.clear()
            return
        self._delivery_stats = {
            key: value for key, value in self._delivery_stats.items() if key in active_ids
        }

    @staticmethod
    def _extract_details(payload: dict[str, Any]) -> dict[str, Any] | None:
        details = payload.get("details")
        if isinstance(details, dict):
            return copy.deepcopy(details)
        remaining = {key: value for key, value in payload.items() if key != "message"}
        return copy.deepcopy(remaining) if remaining else None

    @staticmethod
    def _error_payload(
        source: str,
        message: str,
        exc: BaseException | None,
        callstack: str | None,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        resolved_callstack = callstack
        if resolved_callstack is None:
            if exc is not None:
                resolved_callstack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            else:
                resolved_callstack = "".join(traceback.format_stack(limit=50))
        result: dict[str, Any] = {
            "source": source,
            "message": message,
            "callstack": resolved_callstack,
            "payload": copy.deepcopy(payload or {}),
        }
        if exc is not None:
            result["exception_type"] = exc.__class__.__name__
            result["error"] = str(exc)
        return result

    def _system_replacements(
        self,
        event_type: str,
        message: str,
        occurred_at: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        technical_parts: list[str] = []
        required_technical_defaults = {
            "method_name": "unknown",
            "attempted_signature": "unknown",
            "error_state": "unknown",
            "error_code": "unknown",
            "error_message": "unknown",
            "sys_msgs": "none",
            "exception": "unknown",
        }
        if details:
            optional_keys = (
                "subscription_kind",
                "subscription_id",
                "code",
                "subject",
                "types",
                "error",
                "process",
                "reason",
                "heartbeat_name",
                "last_heartbeat_at",
                "checked_at",
                "recovered_heartbeat_at",
                "silent_since_at",
                "last_silent_report_at",
                "seconds_since_heartbeat",
                "seconds_silent",
                "timeout_seconds",
                "repeat_interval_seconds",
                "silent_alert_count",
                "silent_alert_limit",
                "last_event_action",
                "repeat",
            )
            for key in optional_keys:
                value = details.get(key)
                if value in (None, "", []):
                    continue
                if isinstance(value, list):
                    rendered = ",".join(str(item) for item in value)
                else:
                    rendered = str(value)
                technical_parts.append(f"{key}={rendered}")
            for key, default_value in required_technical_defaults.items():
                value = details.get(key)
                if isinstance(value, list):
                    rendered = ",".join(str(item) for item in value) if value else default_value
                elif value in (None, ""):
                    rendered = default_value
                else:
                    rendered = str(value)
                technical_parts.append(f"{key}={rendered}")
        return {
            "tag": event_type,
            "name": message,
            "callstack": " | ".join(technical_parts),
            "occurred_at": occurred_at,
        }

    @staticmethod
    def _http_callback_from_dict(raw: dict[str, Any]) -> HttpCallbackSpec:
        return HttpCallbackSpec(
            method=str(raw["method"]),
            url=str(raw["url"]),
            headers={str(key): str(value) for key, value in dict(raw.get("headers") or {}).items()},
            body=raw.get("body"),
            body_format=raw.get("bodyFormat"),
        )

    def _render_http_callback(
        self,
        callback: HttpCallbackSpec,
        replacements: dict[str, str],
    ) -> HttpCallbackSpec:
        if callback.body is None:
            return callback
        return HttpCallbackSpec(
            method=callback.method,
            url=callback.url,
            headers=dict(callback.headers),
            body=self._render_template_value(callback.body, replacements),
            body_format=callback.body_format,
        )

    def _render_template_value(self, value: Any, replacements: dict[str, str]) -> Any:
        if isinstance(value, dict):
            return {key: self._render_template_value(item, replacements) for key, item in value.items()}
        if isinstance(value, list):
            return [self._render_template_value(item, replacements) for item in value]
        if isinstance(value, str):
            return self._replace_tokens(value, replacements)
        return value

    def _replace_tokens(self, template: str, replacements: dict[str, str]) -> str:
        def replace_match(match: re.Match[str]) -> str:
            return replacements.get(match.group(1), "")

        return self._REPLACEMENT_PATTERN.sub(replace_match, template)


class InProcessScripter:
    def __init__(
        self,
        logger: logging.Logger | None = None,
        dispatcher: CallbackDispatcher | None = None,
        *,
        log_config: ScripterLogConfig | None = None,
        log_dir: str | Path = Path(".runtime") / "scripter",
        retention_days: int = SCRIPTER_LOG_RETENTION_DAYS,
        queue_maxsize: int = SCRIPTER_EVENT_QUEUE_MAXSIZE,
        interval_seconds: float = SCRIPTER_INTERVAL_SECONDS,
    ) -> None:
        self._provided_logger = logger
        self._dispatcher = dispatcher
        self._log_config = log_config or ScripterLogConfig(log_dir=log_dir, retention_days=retention_days)
        self._queue_maxsize = queue_maxsize
        self._interval_seconds = interval_seconds
        self._runtime: _ScripterRuntime | None = None
        self._logger: logging.Logger | None = None
        self._lock = threading.RLock()

    def start(self) -> None:
        with self._lock:
            if self._runtime is not None:
                return
            logger = self._provided_logger or configure_logger(self._log_config)
            self._logger = logger
            self._runtime = _ScripterRuntime(
                logger=logger,
                dispatcher=self._dispatcher,
                queue_maxsize=self._queue_maxsize,
                interval_seconds=self._interval_seconds,
            )
            set_ops_log_sink(self._record_ops_log, owner=self)

    def log(
        self,
        level: str,
        source: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._runtime_or_start().log(level, source, message, payload)

    def error(
        self,
        source: str,
        message: str,
        exc: BaseException | None = None,
        callstack: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._runtime_or_start().error(source, message, exc, callstack, payload)

    def system_callback(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._runtime_or_start().system_callback(event_type, message, details)

    def heartbeat(self, name: str, payload: dict[str, Any] | None = None) -> None:
        self._runtime_or_start().heartbeat(name, payload)

    def configure_system_callbacks(self, callbacks: list[dict[str, Any]]) -> None:
        self._runtime_or_start().configure_system_callbacks(callbacks)

    def close(self) -> None:
        clear_ops_log_sink(owner=self)
        runtime = self._runtime
        if runtime is not None:
            runtime.close()
        if self._provided_logger is None and self._logger is not None:
            _close_logger_handlers(self._logger)
            self._logger = None
        self._runtime = None

    def wait_for_idle(self, timeout: float = 1.0) -> bool:
        runtime = self._runtime
        if runtime is None:
            return True
        return runtime.wait_for_idle(timeout)

    def on_interval(self, *, now: datetime | None = None) -> None:
        self._runtime_or_start().on_interval(now=now)

    def _runtime_or_start(self) -> _ScripterRuntime:
        self.start()
        if self._runtime is None:  # pragma: no cover - defensive
            raise RuntimeError("InProcessScripter failed to start")
        return self._runtime

    def _record_ops_log(
        self,
        source: str,
        message: str,
        level: str = "info",
        payload: dict[str, Any] | None = None,
    ) -> None:
        runtime = self._runtime
        if runtime is not None:
            runtime.log_ops(source, message, level, payload)


class MockScripter:
    def __init__(self) -> None:
        self.logs: list[ScripterEvent] = []
        self.errors: list[ScripterEvent] = []
        self.system_callbacks: list[ScripterEvent] = []
        self.heartbeats: list[ScripterEvent] = []
        self.system_callback_configs: list[list[dict[str, Any]]] = []
        self.ops_logs: list[tuple[str, str, str, dict[str, Any] | None]] = []
        self.started = False
        self.closed = False

    def start(self) -> None:
        self.started = True
        set_ops_log_sink(self.record_ops_log, owner=self)

    def log(
        self,
        level: str,
        source: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.logs.append(ScripterEvent("log", source, _log_payload(level, source, message, payload)))

    def error(
        self,
        source: str,
        message: str,
        exc: BaseException | None = None,
        callstack: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.errors.append(
            ScripterEvent("error", source, _ScripterRuntime._error_payload(source, message, exc, callstack, payload))
        )

    def system_callback(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.system_callbacks.append(ScripterEvent("system_callback", event_type, _system_callback_payload(message, details)))

    def heartbeat(self, name: str, payload: dict[str, Any] | None = None) -> None:
        self.heartbeats.append(ScripterEvent("heartbeat", name, copy.deepcopy(payload or {})))

    def configure_system_callbacks(self, callbacks: list[dict[str, Any]]) -> None:
        self.system_callback_configs.append(copy.deepcopy(callbacks))

    def record_ops_log(
        self,
        source: str,
        message: str,
        level: str = "info",
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.ops_logs.append((source, message, level, copy.deepcopy(payload)))

    def close(self) -> None:
        clear_ops_log_sink(owner=self)
        self.closed = True


class _JsonlScripter:
    def __init__(self, sink: TextIO | None, *, redact_output: bool) -> None:
        self._sink = sink
        self._redact_output = redact_output
        self._lock = threading.RLock()
        self._closed = False

    def start(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot start closed Scripter")
            if self._sink is None:
                raise RuntimeError("Scripter sink is not configured")
            set_ops_log_sink(self._record_ops_log, owner=self)

    def log(
        self,
        level: str,
        source: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._write(ScripterEvent("log", source, _log_payload(level, source, message, payload)))

    def error(
        self,
        source: str,
        message: str,
        exc: BaseException | None = None,
        callstack: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._write(ScripterEvent("error", source, _ScripterRuntime._error_payload(source, message, exc, callstack, payload)))

    def system_callback(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._write(ScripterEvent("system_callback", event_type, _system_callback_payload(message, details)))

    def heartbeat(self, name: str, payload: dict[str, Any] | None = None) -> None:
        self._write(ScripterEvent("heartbeat", name, payload or {}))

    def configure_system_callbacks(self, callbacks: list[dict[str, Any]]) -> None:
        self._write(
            ScripterEvent("config", "system_callbacks", {"callbacks": _validate_system_callback_configs(callbacks)}),
            required=True,
        )

    def close(self) -> None:
        clear_ops_log_sink(owner=self)
        with self._lock:
            self._closed = True
            if self._sink is None:
                return
            flush = getattr(self._sink, "flush", None)
            if callable(flush):
                flush()

    def _write(self, event: ScripterEvent, *, required: bool = False) -> None:
        payload = self._serialize_event(event)
        with self._lock:
            if self._closed:
                if required:
                    raise RuntimeError("Scripter is closed")
                return
            if self._sink is None:
                raise RuntimeError("Scripter has not been started")
            self._write_payload(payload)

    def _record_ops_log(
        self,
        source: str,
        message: str,
        level: str = "info",
        payload: dict[str, Any] | None = None,
    ) -> None:
        minimum_level = getattr(self, "_log_level", None)
        if minimum_level is not None and not _log_level_enabled(level, str(minimum_level)):
            return
        self.log(level, source, message, payload)

    def _serialize_event(self, event: ScripterEvent) -> str:
        return json.dumps(
            event.to_dict(redact=self._redact_output),
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )

    def _write_payload(self, payload: str) -> None:
        if self._sink is None:
            raise RuntimeError("Scripter has not been started")
        self._sink.write(payload + "\n")
        flush = getattr(self._sink, "flush", None)
        if callable(flush):
            flush()


class StdoutScripter(_JsonlScripter):
    def __init__(self, stream: TextIO | None = None) -> None:
        super().__init__(stream or sys.stdout, redact_output=True)


class IsolateProcessScripter(_JsonlScripter):
    _WRITER_STOP = object()

    def __init__(
        self,
        sink: TextIO | None = None,
        *,
        command: list[str] | None = None,
        log_dir: str | Path = Path(".runtime") / "scripter",
        retention_days: int = SCRIPTER_LOG_RETENTION_DAYS,
        log_level: str = SCRIPTER_LOG_LEVEL,
        python_executable: str | None = None,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        close_timeout: float = 3.0,
        start_timeout: float = ISOLATE_PROCESS_START_TIMEOUT_SECONDS,
        writer_queue_maxsize: int = SCRIPTER_EVENT_QUEUE_MAXSIZE,
    ) -> None:
        self._process: subprocess.Popen | None = None
        self._owns_sink = sink is None
        self._command = command
        self._log_dir = log_dir
        self._retention_days = retention_days
        self._log_level = _normalize_log_level(log_level)
        self._python_executable = python_executable
        self._cwd = cwd
        self._env = env
        self._close_timeout = max(close_timeout, 0.1)
        self._start_timeout = max(start_timeout, 0.1)
        self._writer_queue_maxsize = _writer_queue_maxsize_for_log_level(writer_queue_maxsize, self._log_level)
        self._writer_queue: queue.Queue[str | object] | None = None
        self._writer_thread: threading.Thread | None = None
        self._writer_error: BaseException | None = None
        self._closing = False
        super().__init__(sink, redact_output=sink is not None)

    def start(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot start closed IsolateProcessScripter")
            if self._sink is not None:
                set_ops_log_sink(self._record_ops_log, owner=self)
                return
            ready_file = self._ready_file_path() if self._command is None else None
            if ready_file is not None:
                try:
                    ready_file.unlink(missing_ok=True)
                except Exception:
                    pass
            try:
                process = self._start_child(
                    command=self._command,
                    log_dir=self._log_dir,
                    retention_days=self._retention_days,
                    log_level=self._log_level,
                    python_executable=self._python_executable,
                    cwd=self._cwd,
                    env=self._env,
                    ready_file=ready_file,
                )
            except Exception as exc:
                ops_log(LogSource.SCRIPTER, f"IsolateProcessScripter child start failed: {exc.__class__.__name__}: {exc}")
                write_crash_log(
                    role="main",
                    source="scripter.isolate_start",
                    message="IsolateProcessScripter child start failed",
                    exc=exc,
                    log_dir=self._log_dir,
                    extra=self._child_process_context(),
                )
                raise
            try:
                self._wait_for_child_start(process, ready_file)
            except Exception as exc:
                self._terminate_child_process(process)
                ops_log(
                    LogSource.SCRIPTER,
                    f"IsolateProcessScripter child readiness failed: {exc.__class__.__name__}: {exc}",
                )
                write_crash_log(
                    role="main",
                    source="scripter.isolate_start",
                    message="IsolateProcessScripter child readiness failed",
                    exc=exc,
                    log_dir=self._log_dir,
                    extra=self._child_process_context(),
                )
                raise
            if process.stdin is None:
                self._terminate_child_process(process)
                ops_log(LogSource.SCRIPTER, "IsolateProcessScripter child stdin was not created")
                write_crash_log(
                    role="main",
                    source="scripter.isolate_start",
                    message="IsolateProcessScripter child stdin was not created",
                    log_dir=self._log_dir,
                    extra=self._child_process_context(),
                )
                raise RuntimeError("IsolateProcessScripter child stdin was not created")
            self._process = process
            self._sink = process.stdin
            self._start_writer_thread_locked()
            set_ops_log_sink(self._record_ops_log, owner=self)

    def close(self) -> None:
        clear_ops_log_sink(owner=self)
        close_payload = self._serialize_event(ScripterEvent("control", "close", {}))
        with self._lock:
            already_closed = self._closed
            self._closed = True
            self._closing = True
            writer_queue = self._writer_queue
            sink = self._sink
        if writer_queue is not None:
            if not already_closed:
                self._put_writer_control_payload(writer_queue, close_payload)
            self._stop_writer_thread()
        else:
            if not already_closed and sink is not None:
                try:
                    self._write_payload(close_payload)
                except Exception:
                    pass
                flush = getattr(sink, "flush", None)
                if callable(flush):
                    try:
                        flush()
                    except Exception:
                        pass
        if not self._owns_sink or self._process is None:
            return
        try:
            self._process.wait(timeout=self._close_timeout)
        except subprocess.TimeoutExpired:
            self._terminate_child_process(self._process)
        try:
            if self._process.stdin is not None:
                self._process.stdin.close()
        except Exception:
            pass

    def _write(self, event: ScripterEvent, *, required: bool = False) -> None:
        writer_queue = self._writer_queue
        if writer_queue is None:
            super()._write(event, required=required)
            return
        payload = self._serialize_event(event)
        with self._lock:
            if self._closed:
                if required:
                    raise RuntimeError("Scripter is closed")
                return
            if self._writer_error is not None:
                raise RuntimeError("IsolateProcessScripter writer failed") from self._writer_error
            try:
                writer_queue.put_nowait(payload)
            except queue.Full as exc:
                self._fatal_writer_failure(
                    exc,
                    "IsolateProcessScripter writer queue full",
                    {"queue_maxsize": self._writer_queue_maxsize},
                )

    def _start_writer_thread_locked(self) -> None:
        self._writer_queue = queue.Queue(maxsize=self._writer_queue_maxsize)
        self._redact_output = False
        self._writer_thread = threading.Thread(
            target=self._run_writer,
            name="homestock-isolate-scripter-writer",
            daemon=True,
        )
        self._writer_thread.start()

    def _run_writer(self) -> None:
        writer_queue = self._writer_queue
        if writer_queue is None:
            return
        while True:
            item = writer_queue.get()
            try:
                if item is self._WRITER_STOP:
                    return
                assert isinstance(item, str)
                self._write_payload(item)
            except Exception as exc:
                with self._lock:
                    self._writer_error = exc
                    closing = self._closing or self._closed
                if closing:
                    ops_log(
                        LogSource.SCRIPTER,
                        f"IsolateProcessScripter writer stopped during close: {exc.__class__.__name__}: {exc}",
                    )
                    return
                try:
                    sys.stderr.write(f"IsolateProcessScripter writer failed: {exc.__class__.__name__}: {exc}\n")
                    sys.stderr.flush()
                except Exception:
                    pass
                write_crash_log(
                    role="main",
                    source="scripter.isolate_writer",
                    message="IsolateProcessScripter writer failed",
                    exc=exc,
                    log_dir=self._log_dir,
                )
                os._exit(1)
            finally:
                writer_queue.task_done()

    def _stop_writer_thread(self) -> None:
        writer_queue = self._writer_queue
        if writer_queue is None:
            return
        try:
            writer_queue.put(self._WRITER_STOP, timeout=self._close_timeout)
        except Exception:
            pass
        writer = self._writer_thread
        if writer is not None:
            writer.join(timeout=self._close_timeout)

    def _put_writer_control_payload(self, writer_queue: queue.Queue[str | object], payload: str) -> None:
        try:
            writer_queue.put(payload, timeout=self._close_timeout)
        except Exception as exc:
            ops_log(
                LogSource.SCRIPTER,
                f"IsolateProcessScripter close control event was not queued: {exc.__class__.__name__}: {exc}",
            )

    def _fatal_writer_failure(
        self,
        exc: BaseException,
        message: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._writer_error = exc
        try:
            sys.stderr.write(f"{message}: {exc.__class__.__name__}: {exc}\n")
            sys.stderr.flush()
        except Exception:
            pass
        write_crash_log(
            role="main",
            source="scripter.isolate_writer",
            message=message,
            exc=exc,
            log_dir=self._log_dir,
            extra=extra,
        )
        os._exit(1)

    @staticmethod
    def _start_child(
        *,
        command: list[str] | None,
        log_dir: str | Path,
        retention_days: int,
        log_level: str,
        python_executable: str | None,
        cwd: str | Path | None,
        env: dict[str, str] | None,
        ready_file: Path | None = None,
    ) -> subprocess.Popen:
        resolved_command = command or [
            python_executable or sys.executable,
            "-m",
            "homestock.scripter_child",
            "--log-dir",
            str(log_dir),
            "--retention-days",
            str(max(retention_days, 1)),
            "--log-level",
            _normalize_log_level(log_level),
        ]
        if command is None and ready_file is not None:
            resolved_command.extend(["--ready-file", str(ready_file)])
        return subprocess.Popen(
            resolved_command,
            stdin=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            cwd=str(cwd) if cwd is not None else None,
            env=env,
        )

    def _ready_file_path(self) -> Path:
        return Path(tempfile.gettempdir()) / f"homestock_scripter_ready_{os.getpid()}_{id(self)}.json"

    def _child_process_context(self) -> dict[str, Any]:
        return {
            "command": self._command,
            "cwd": str(self._cwd) if self._cwd is not None else None,
        }

    def _wait_for_child_start(self, process: subprocess.Popen, ready_file: Path | None) -> None:
        if ready_file is None:
            time.sleep(min(self._start_timeout, 0.2))
            return_code = process.poll()
            if return_code is not None:
                raise RuntimeError(f"IsolateProcessScripter child exited during startup return_code={return_code}")
            return
        deadline = time.monotonic() + self._start_timeout
        while time.monotonic() < deadline:
            return_code = process.poll()
            if return_code is not None:
                raise RuntimeError(f"IsolateProcessScripter child exited before ready return_code={return_code}")
            if ready_file.exists():
                try:
                    ready_file.unlink(missing_ok=True)
                except Exception:
                    pass
                return
            time.sleep(0.05)
        raise TimeoutError(f"IsolateProcessScripter child did not become ready within {self._start_timeout:.1f}s")

    def _terminate_child_process(self, process: subprocess.Popen) -> None:
        try:
            if process.poll() is not None:
                return
            process.terminate()
            process.wait(timeout=self._close_timeout)
        except subprocess.TimeoutExpired:
            try:
                process.kill()
                process.wait(timeout=self._close_timeout)
            except Exception:
                pass
        except Exception:
            pass
