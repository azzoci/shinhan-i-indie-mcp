from __future__ import annotations

import re
from typing import Any


_PASSWORD_NAME_PATTERN = "|".join(
    (
        r"homestock[_-]?account[_-]?password",
        r"account[_-]?password",
        r"accountpassword",
        r"account[_-]?passwd",
        r"accountpasswd",
        r"account[_-]?pwd",
        r"accountpwd",
        r"acct[_-]?password",
        r"acct[_-]?pwd",
        r"password",
        r"passwd",
        r"pwd",
        r"계좌\s*비밀\s*번호",
        r"계좌비밀번호",
        r"계좌비번",
    )
)
ACCOUNT_PASSWORD_KEY_PATTERN = re.compile(
    rf"(?:{_PASSWORD_NAME_PATTERN})",
    re.IGNORECASE,
)
JSON_PASSWORD_VALUE_PATTERN = re.compile(
    rf"([\"'])([^\"']*(?:{_PASSWORD_NAME_PATTERN})[^\"']*)\1"
    r"(\s*:\s*)([\"'])(.*?)\4",
    re.IGNORECASE,
)
QUOTED_PASSWORD_ASSIGNMENT_PATTERN = re.compile(
    rf"(\b(?:{_PASSWORD_NAME_PATTERN})\b\s*[:=]\s*)"
    r"([\"']).*?\2",
    re.IGNORECASE,
)
PASSWORD_ASSIGNMENT_PATTERN = re.compile(
    rf"(\b(?:{_PASSWORD_NAME_PATTERN})\b\s*[:=]\s*)"
    r"([^,\s;\"'}\]]+)",
    re.IGNORECASE,
)
KOREAN_PASSWORD_ASSIGNMENT_PATTERN = re.compile(
    r"((?:계좌\s*비밀\s*번호|계좌비밀번호|계좌비번)\s*[:=]\s*)"
    r"([^,\s;\"'}\]]+)",
    re.IGNORECASE,
)
PASSWORD_WORD_VALUE_PATTERN = re.compile(
    rf"(\b(?:{_PASSWORD_NAME_PATTERN})\b\s+)"
    r"(?!is\b|was\b|will\b|missing\b|not\b)([^,\s;\"')}\]]+)",
    re.IGNORECASE,
)
PASSWORD_CLI_OPTION_PATTERN = re.compile(
    rf"^(?:-{{1,2}}|/)(?:{_PASSWORD_NAME_PATTERN})$",
    re.IGNORECASE,
)
_SAFE_STATUS_VALUES = {"set", "missing", "unset", "<unset>", "none", "null", "false", "true"}


def is_http_url(value: str) -> bool:
    lower = value.lower()
    return lower.startswith("http://") or lower.startswith("https://")


def _is_sensitive_cli_option(value: str) -> bool:
    return bool(PASSWORD_CLI_OPTION_PATTERN.match(value.strip()))


def redact_for_output(value: Any, parent_key: str = "") -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if ACCOUNT_PASSWORD_KEY_PATTERN.search(key_text):
                redacted[key_text] = "<redacted>"
            else:
                redacted[key_text] = redact_for_output(item, key_text)
        return redacted
    if isinstance(value, list):
        redacted_items: list[Any] = []
        redact_next = False
        for item in value:
            if redact_next and isinstance(item, str):
                redacted_items.append("<redacted>")
                redact_next = False
                continue
            redacted_items.append(redact_for_output(item, parent_key))
            redact_next = isinstance(item, str) and _is_sensitive_cli_option(item)
        return redacted_items
    if isinstance(value, tuple):
        return [redact_for_output(item, parent_key) for item in value]
    if isinstance(value, str):
        return redact_log_text(value)
    return value


def redact_log_text(value: str) -> str:
    value = JSON_PASSWORD_VALUE_PATTERN.sub(r"\1\2\1\3\4<redacted>\4", value)
    value = QUOTED_PASSWORD_ASSIGNMENT_PATTERN.sub(r"\1\2<redacted>\2", value)

    def replace_secret(match: re.Match[str]) -> str:
        if match.group(2).strip().lower() in _SAFE_STATUS_VALUES:
            return match.group(0)
        return f"{match.group(1)}<redacted>"

    value = PASSWORD_ASSIGNMENT_PATTERN.sub(replace_secret, value)
    value = KOREAN_PASSWORD_ASSIGNMENT_PATTERN.sub(replace_secret, value)
    return PASSWORD_WORD_VALUE_PATTERN.sub(replace_secret, value)
