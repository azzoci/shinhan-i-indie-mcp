from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

from homestock.scripter import (
    FATAL_SYSTEM_CALLBACK_DRAIN_TIMEOUT_SECONDS,
    ISOLATE_PROCESS_LOGGER_NAME,
    ISOLATE_PROCESS_LOG_RETENTION_DAYS,
    SCRIPTER_EVENT_QUEUE_MAXSIZE,
    SCRIPTER_INTERVAL_SECONDS,
    ScripterLogConfig,
    _ScripterRuntime,
    _validate_system_callback_configs,
    configure_logger,
    write_crash_log,
)
from homestock.ops_log import LogSource, clear_ops_log_sink, ops_log, set_ops_log_sink
from homestock.redaction import redact_log_text


def process_scripter_event_line(
    line: str,
    runtime: _ScripterRuntime,
    logger: logging.Logger,
) -> bool:
    try:
        raw = json.loads(line)
    except json.JSONDecodeError as exc:
        logger.warning("Ignoring invalid scripter JSONL event: %s", exc)
        return True
    return process_scripter_event(raw, runtime, logger)


def process_scripter_event(
    raw: Any,
    runtime: _ScripterRuntime,
    logger: logging.Logger,
) -> bool:
    if not isinstance(raw, dict):
        logger.warning("Ignoring non-object scripter event: %r", type(raw).__name__)
        return True
    kind = str(raw.get("kind") or "")
    event_type = str(raw.get("event_type") or "")
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        payload = {}

    if kind == "control" and event_type == "close":
        logger.info("Scripter child close requested")
        return False
    if kind == "startup_log":
        runtime.log_ops(
            event_type,
            str(payload.get("message") or ""),
            str(payload.get("level") or "info"),
            payload.get("payload") if isinstance(payload.get("payload"), dict) else None,
        )
        return True
    if kind == "log":
        runtime.dispatch_log_now(
            str(payload.get("level") or "info"),
            str(payload.get("source") or event_type),
            str(payload.get("message") or ""),
            payload.get("payload") if isinstance(payload.get("payload"), dict) else None,
        )
        return True
    if kind == "error":
        runtime.dispatch_error_payload_now(event_type, payload)
        return True
    if kind == "system_callback":
        details = payload.get("details")
        if not isinstance(details, dict):
            details = {key: value for key, value in payload.items() if key != "message"}
            if not details:
                details = None
        runtime.dispatch_system_callback_now(
            event_type,
            str(payload.get("message") or event_type),
            details,
        )
        return True
    if kind == "heartbeat":
        runtime.dispatch_heartbeat_now(event_type, payload)
        return True
    if kind == "config" and event_type == "system_callbacks":
        callbacks = payload.get("callbacks")
        if not isinstance(callbacks, list):
            logger.warning("Ignoring malformed system callback config event")
            return True
        try:
            callbacks = _validate_system_callback_configs(callbacks)
        except ValueError as exc:
            logger.warning("Ignoring malformed system callback config event: %s", redact_log_text(str(exc)))
            return True
        runtime.configure_system_callbacks(callbacks)
        return True

    logger.warning(
        "Ignoring unknown scripter event kind=%s event_type=%s",
        redact_log_text(kind),
        redact_log_text(event_type),
    )
    return True


def run_scripter_child(
    input_stream: TextIO,
    runtime: _ScripterRuntime,
    logger: logging.Logger,
    *,
    stop_on_eof: bool = False,
    eof_sleep_seconds: float = 1.0,
) -> None:
    logger.info("Scripter child event loop started")
    while True:
        try:
            line = input_stream.readline()
        except Exception:
            logger.exception("Scripter child stdin read failed")
            if stop_on_eof:
                raise
            time.sleep(eof_sleep_seconds)
            continue
        if line == "":
            if stop_on_eof:
                logger.info("Scripter child stdin EOF")
                return
            time.sleep(eof_sleep_seconds)
            continue
        keep_running = process_scripter_event_line(line, runtime, logger)
        if not keep_running:
            return


def _utf8_stdin_text_stream(input_stream: TextIO) -> TextIO:
    # The parent writes JSONL to the child stdin as UTF-8. On Windows, sys.stdin
    # can otherwise decode pipe bytes with the active code page and produce
    # low-surrogate mojibake in Korean callback text before json.loads().
    buffer = getattr(input_stream, "buffer", None)
    if buffer is None:
        return input_stream
    return io.TextIOWrapper(buffer, encoding="utf-8", errors="strict", newline="")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the homestock scripter child runtime.")
    parser.add_argument("--log-dir", default=str(Path(".runtime") / "scripter"))
    parser.add_argument("--retention-days", type=int, default=ISOLATE_PROCESS_LOG_RETENTION_DAYS)
    parser.add_argument("--log-level", default=os.getenv("HOMESTOCK_SCRIPTER_LOG_LEVEL", "info"))
    parser.add_argument("--queue-maxsize", type=int, default=SCRIPTER_EVENT_QUEUE_MAXSIZE)
    parser.add_argument("--interval-seconds", type=float, default=SCRIPTER_INTERVAL_SECONDS)
    parser.add_argument("--ready-file", default="")
    return parser.parse_args(argv)


def _configure_child_logger(args: argparse.Namespace) -> logging.Logger:
    try:
        return configure_logger(
            ScripterLogConfig(
                log_dir=args.log_dir,
                retention_days=max(args.retention_days, 1),
                logger_name=ISOLATE_PROCESS_LOGGER_NAME,
                log_level=args.log_level,
            )
        )
    except Exception as exc:
        ops_log(LogSource.SCRIPTER, f"logger setup failed: {exc.__class__.__name__}: {exc}")
        write_crash_log(
            role="scripter_child",
            source="scripter_child.logger_setup",
            message="Scripter child logger setup failed",
            exc=exc,
            log_dir=args.log_dir,
        )
        raise


def _write_ready_file(args: argparse.Namespace, logger: logging.Logger) -> None:
    if not args.ready_file:
        return
    path = Path(args.ready_file)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "ready_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        logger.info("Scripter child ready file written path=%s", path)
    except Exception as exc:
        logger.exception("Scripter child ready file write failed")
        write_crash_log(
            role="scripter_child",
            source="scripter_child.ready_file",
            message="Scripter child ready file write failed",
            exc=exc,
            log_dir=args.log_dir,
            extra={"ready_file": str(path)},
        )
        raise


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        logger = _configure_child_logger(args)
    except Exception:
        return 1
    logger.info("Scripter child starting")

    runtime: _ScripterRuntime | None = None
    try:
        runtime = _ScripterRuntime(
            logger=logger,
            queue_maxsize=max(args.queue_maxsize, 1),
            interval_seconds=max(args.interval_seconds, 0.1),
            fatal_on_worker_error=True,
            crash_log_dir=args.log_dir,
        )
        set_ops_log_sink(runtime.log_ops, owner=runtime)
        _write_ready_file(args, logger)
        run_scripter_child(
            _utf8_stdin_text_stream(sys.stdin),
            runtime,
            logger,
            stop_on_eof=True,
        )
        logger.info("Scripter child stopped")
        return 0
    except KeyboardInterrupt:
        logger.info("Scripter child interrupted")
        return 0
    except Exception as exc:
        logger.exception("Scripter child runtime fatal")
        if runtime is not None:
            try:
                runtime.dispatch_system_callback_now(
                    "scripter_child_fatal",
                    "Scripter child runtime fatal",
                    {"exception_type": exc.__class__.__name__, "error": str(exc)},
                )
                runtime.wait_for_idle(timeout=FATAL_SYSTEM_CALLBACK_DRAIN_TIMEOUT_SECONDS)
            except Exception:
                logger.exception("Scripter child fatal system callback failed")
        write_crash_log(
            role="scripter_child",
            source="scripter_child.runtime",
            message="Scripter child runtime fatal",
            exc=exc,
            log_dir=args.log_dir,
        )
        raise
    finally:
        if runtime is not None:
            clear_ops_log_sink(owner=runtime)
            try:
                runtime.close()
            except Exception:
                logger.exception("Scripter child runtime close failed")


if __name__ == "__main__":
    raise SystemExit(main())
