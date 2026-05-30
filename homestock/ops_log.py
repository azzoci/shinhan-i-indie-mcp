from __future__ import annotations

import json
import sys
import threading
from datetime import datetime
from typing import Any, Callable

from homestock.redaction import redact_for_output, redact_log_text


class LogSource:
    STARTUP_SERVER = "startup.server"
    STARTUP_BACKEND = "startup.backend"
    STARTUP_REAL = "startup.real"
    STARTUP_RUNTIME = "startup.runtime"
    STARTUP_MANAGE = "startup.manage"
    STARTUP_MCP = "startup.mcp"
    MCP_TOOL = "mcp.tool"
    RT_RUNTIME = "rt.runtime"
    RT_INDI = "rt.indi"
    RT_REAL = "rt.real"
    TR_REAL = "tr.real"
    WEBHOOK = "webhook"
    MANAGE = "manage"
    HEARTBEAT = "heartbeat"
    SCRIPTER = "scripter"


_LEGACY_LOG_SOURCE_ALIASES = {
    "startup.webhook": LogSource.WEBHOOK,
    "startup.indi.rt": LogSource.RT_INDI,
    "startup.runtime.rt": LogSource.RT_RUNTIME,
    "startup.runtime.state": LogSource.STARTUP_RUNTIME,
}


def normalize_log_source(source: str) -> str:
    normalized = str(source or "").strip()
    if not normalized:
        return LogSource.MANAGE
    return _LEGACY_LOG_SOURCE_ALIASES.get(normalized, normalized)


OpsLogSink = Callable[[str, str, str, dict[str, Any] | None], None]
_sink_lock = threading.Lock()
_sink: OpsLogSink | None = None
_sink_owner: object | None = None
_local = threading.local()


def set_ops_log_sink(sink: OpsLogSink, owner: object | None = None) -> None:
    global _sink, _sink_owner
    with _sink_lock:
        _sink = sink
        _sink_owner = owner


def clear_ops_log_sink(owner: object | None = None) -> None:
    global _sink, _sink_owner
    with _sink_lock:
        if owner is not None and _sink_owner is not owner:
            return
        _sink = None
        _sink_owner = None


def ops_log(
    source: str,
    message: str,
    *,
    level: str = "info",
    payload: dict[str, Any] | None = None,
) -> None:
    normalized_source = normalize_log_source(source)
    with _sink_lock:
        sink = _sink
    if sink is not None and not getattr(_local, "active", False):
        _local.active = True
        try:
            sink(normalized_source, message, level, payload)
            return
        except Exception as exc:
            _stdout_ops_log(
                LogSource.SCRIPTER,
                f"ops_log sink failed: {exc.__class__.__name__}: {exc}",
                level="error",
            )
        finally:
            _local.active = False
    _stdout_ops_log(normalized_source, message, level=level, payload=payload)


def _stdout_ops_log(
    source: str,
    message: str,
    *,
    level: str = "info",
    payload: dict[str, Any] | None = None,
) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    safe_source = redact_log_text(source)
    safe_message = redact_log_text(message)
    detail = ""
    if payload:
        detail = " details=" + json.dumps(redact_for_output(payload), ensure_ascii=False, default=str)
    line = f"[homestock.ops][{timestamp}][{level.lower()}][{safe_source}] {safe_message}{detail}"
    try:
        print(line, flush=True)
    except OSError:
        try:
            sys.stderr.write(line + "\n")
            sys.stderr.flush()
        except OSError:
            return
