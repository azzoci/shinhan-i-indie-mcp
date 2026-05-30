from __future__ import annotations

import re
import sys
from pathlib import Path


ASCII_RE = re.compile(rb"[A-Za-z0-9_]{3,}")
UTF16_RE = re.compile(rb"(?:[\x20-\x7E]\x00){3,}")


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: extract_xtr_tokens.py FILE")
        return 2

    data = Path(argv[1]).read_bytes()
    seen: set[str] = set()

    for match in ASCII_RE.finditer(data):
        token = match.group().decode("ascii", errors="ignore")
        if token not in seen:
            seen.add(token)
            print(token)

    for match in UTF16_RE.finditer(data):
        token = match.group().decode("utf-16-le", errors="ignore")
        if token not in seen:
            seen.add(token)
            print(token)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
