from __future__ import annotations

from typing import Any

from homestock.ops_log import clear_ops_log_sink, ops_log, set_ops_log_sink


def set_startup_log_sink(sink: Any, owner: object | None = None) -> None:
    set_ops_log_sink(sink, owner=owner)


def clear_startup_log_sink(owner: object | None = None) -> None:
    clear_ops_log_sink(owner=owner)


def startup_log(stage: str, message: str, *, level: str = "info") -> None:
    ops_log(stage, message, level=level)
