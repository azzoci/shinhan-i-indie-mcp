from __future__ import annotations

import os
import subprocess
import sys


def run_command(command: str) -> None:
    print(f"$ {command}")
    completed = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        errors="replace",
    )
    if completed.stdout:
        print(completed.stdout.rstrip())
    if completed.stderr:
        print(completed.stderr.rstrip())
    print(f"[exit={completed.returncode}]")
    print()


def main() -> int:
    print(f"python={sys.executable}")
    print(f"pid={os.getpid()}")
    print(f"SESSIONNAME={os.environ.get('SESSIONNAME', '')}")
    print()
    run_command("whoami")
    run_command("tasklist /v | findstr /i \"giexpert indi python\"")
    run_command(
        "powershell -NoProfile -Command "
        "\"Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -match 'giexpert|indi|python' } | "
        "Select-Object Name,ProcessId,SessionId,CommandLine | "
        "ConvertTo-Json -Depth 3\""
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
