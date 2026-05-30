from __future__ import annotations

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtWidgets import QApplication


PROG_ID = "GIEXPERTCONTROL.GiExpertControlCtrl.1"


def main() -> int:
    app = QApplication.instance() or QApplication([])
    control = QAxWidget(PROG_ID)
    if control.isNull():
        print("FAILED_TO_CREATE_OCX")
        return 1

    documentation = control.generateDocumentation()
    print(documentation[:12000])
    control.clear()
    app.quit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
