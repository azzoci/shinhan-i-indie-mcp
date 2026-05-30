from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def format_display_decimal(value: Any) -> str:
    if value is None:
        return ""
    try:
        decimal_value = Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return str(value)
    if decimal_value == decimal_value.to_integral_value():
        return f"{int(decimal_value):,}"
    return f"{decimal_value:,.10f}".rstrip("0").rstrip(".")
