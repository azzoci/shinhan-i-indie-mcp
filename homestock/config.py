from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal


BackendName = Literal["mock", "real"]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def _env_log_level(name: str, default: str = "info") -> str:
    value = os.getenv(name, default).strip().lower()
    return value if value in {"debug", "info", "warning", "warn", "error", "critical", "fatal"} else default


@dataclass(frozen=True)
class Settings:
    backend: BackendName = "mock"
    allow_live_orders: bool = False
    host: str = "0.0.0.0"
    port: int = 8000
    runtime_state_dir: str | None = None
    use_threaded_real_client: bool = True
    scripter_log_dir: str | None = None
    scripter_log_retention_days: int = 5
    scripter_log_level: str = "info"
    holding_alert_config_path: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        backend = os.getenv("INDI_BACKEND", "mock").strip().lower()
        if backend not in {"mock", "real"}:
            raise ValueError("INDI_BACKEND must be 'mock' or 'real'")

        return cls(
            backend=backend,  # type: ignore[arg-type]
            allow_live_orders=_env_bool("ALLOW_LIVE_ORDERS", False),
            host=os.getenv("HOMESTOCK_HOST", "0.0.0.0").strip() or "0.0.0.0",
            port=int(os.getenv("HOMESTOCK_PORT", "8000")),
            runtime_state_dir=(os.getenv("HOMESTOCK_RUNTIME_STATE_DIR", "").strip() or None),
            holding_alert_config_path=(os.getenv("HOMESTOCK_HOLDING_ALERT_CONFIG", "").strip() or None),
            use_threaded_real_client=_env_bool("HOMESTOCK_USE_THREADED_REAL_CLIENT", True),
            scripter_log_dir=(os.getenv("HOMESTOCK_SCRIPTER_LOG_DIR", "").strip() or None),
            scripter_log_retention_days=max(_env_int("HOMESTOCK_SCRIPTER_LOG_RETENTION_DAYS", 5), 1),
            scripter_log_level=_env_log_level("HOMESTOCK_SCRIPTER_LOG_LEVEL", "info"),
        )
