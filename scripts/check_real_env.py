from __future__ import annotations

import ctypes
import os
import platform
import sys
from dataclasses import dataclass
from typing import Callable


PROG_ID = "GIEXPERTCONTROL.GiExpertControlCtrl.1"


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


def check_python_bitness() -> CheckResult:
    architecture = platform.architecture()[0]
    return CheckResult(
        name="Python bitness",
        ok=architecture == "32bit",
        detail=f"{architecture} ({sys.executable})",
    )


def check_environment() -> CheckResult:
    backend = os.getenv("INDI_BACKEND", "")
    allow_live_orders = os.getenv("ALLOW_LIVE_ORDERS", "")
    session_name = os.getenv("SESSIONNAME", "")
    session_id = current_session_id()
    return CheckResult(
        name="Environment variables",
        ok=True,
        detail=(
            f"INDI_BACKEND={backend or '<unset>'}, "
            f"ALLOW_LIVE_ORDERS={allow_live_orders or '<unset>'}, "
            f"SESSIONNAME={session_name or '<unset>'}, "
            f"SESSIONID={session_id if session_id is not None else '<unknown>'}"
        ),
    )


def current_session_id() -> int | None:
    session_id = ctypes.c_uint()
    if ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session_id)):
        return int(session_id.value)
    return None


def check_interactive_session() -> CheckResult:
    session_name = os.getenv("SESSIONNAME", "").strip()
    session_id = current_session_id()
    interactive = session_id != 0 if session_id is not None else session_name.lower() not in {"", "services"}
    return CheckResult(
        name="Interactive session",
        ok=interactive,
        detail=(
            f"SESSIONNAME={session_name or '<unset>'}, "
            f"SESSIONID={session_id if session_id is not None else '<unknown>'}"
        ),
    )


def check_qaxcontainer_import() -> CheckResult:
    try:
        from PyQt5.QAxContainer import QAxWidget  # noqa: F401
    except Exception as exc:
        return CheckResult(
            name="PyQt5 QAxContainer import",
            ok=False,
            detail=f"{type(exc).__name__}: {exc}",
        )
    return CheckResult(name="PyQt5 QAxContainer import", ok=True, detail="import ok")


def check_indi_ocx_creation() -> CheckResult:
    try:
        from PyQt5.QtWidgets import QApplication
        from PyQt5.QAxContainer import QAxWidget
    except Exception as exc:
        return CheckResult(
            name="Indi OCX creation",
            ok=False,
            detail=f"PyQt5 import failed: {type(exc).__name__}: {exc}",
        )

    app = QApplication.instance() or QApplication([])
    control = QAxWidget(PROG_ID)
    ok = not control.isNull()
    control.clear()
    app.quit()
    return CheckResult(
        name="Indi OCX creation",
        ok=ok,
        detail=f"ProgID={PROG_ID}, created={ok}",
    )


def print_result(result: CheckResult) -> None:
    status = "OK" if result.ok else "FAIL"
    print(f"[{status}] {result.name}: {result.detail}")


def main() -> int:
    checks: list[Callable[[], CheckResult]] = [
        check_python_bitness,
        check_environment,
        check_interactive_session,
        check_qaxcontainer_import,
        check_indi_ocx_creation,
    ]
    results = [check() for check in checks]
    for result in results:
        print_result(result)

    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
