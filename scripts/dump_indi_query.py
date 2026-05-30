from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QObject, QTimer
from PyQt5.QtWidgets import QApplication


PROG_ID = "GIEXPERTCONTROL.GiExpertControlCtrl.1"


@dataclass
class DumpResult:
    query: str
    rqid: int
    received_rqid: int | None
    timeout: bool
    error_state: int
    error_code: str
    error_message: str
    single_row_count: int
    multi_row_count: int
    single_data: list[str]
    multi_data: list[list[str]]
    sys_messages: list[int]


class IndiQueryDumper(QObject):
    def __init__(self, query_name: str, single_inputs: list[str]) -> None:
        super().__init__()
        self._query_name = query_name
        self._single_inputs = single_inputs
        self._control = QAxWidget(PROG_ID)
        if self._control.isNull():
            raise RuntimeError(f"failed to create Indi OCX: {PROG_ID}")

        self._loop = QEventLoop()
        self._received_rqid: int | None = None
        self._timed_out = False
        self._sys_messages: list[int] = []

        self._control.ReceiveData.connect(self._on_receive_data)
        self._control.ReceiveSysMsg.connect(self._on_receive_sys_msg)

    def run(self, timeout_ms: int = 10000) -> DumpResult:
        if not self._control.dynamicCall("SetQueryName(QVariant)", self._query_name):
            raise RuntimeError(f"SetQueryName failed for {self._query_name}")

        for index, value in enumerate(self._single_inputs):
            ok = self._control.dynamicCall("SetSingleData(int, QVariant)", index, value)
            if not ok:
                raise RuntimeError(f"SetSingleData failed at index {index}: {value}")

        rqid = int(self._control.dynamicCall("RequestData()"))
        if rqid <= 0:
            return self._collect_result(rqid)

        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(self._on_timeout)
        timer.start(timeout_ms)
        self._loop.exec_()
        timer.stop()
        return self._collect_result(rqid)

    def _on_receive_data(self, rqid: int) -> None:
        self._received_rqid = rqid
        if self._loop.isRunning():
            self._loop.quit()

    def _on_receive_sys_msg(self, msg_id: int) -> None:
        self._sys_messages.append(msg_id)

    def _on_timeout(self) -> None:
        self._timed_out = True
        if self._loop.isRunning():
            self._loop.quit()

    def _collect_result(self, rqid: int) -> DumpResult:
        single_row_count = int(self._control.dynamicCall("GetSingleRowCount()"))
        multi_row_count = int(self._control.dynamicCall("GetMultiRowCount()"))
        error_state = int(self._control.dynamicCall("GetErrorState()"))
        error_code = str(self._control.dynamicCall("GetErrorCode()"))
        error_message = str(self._control.dynamicCall("GetErrorMessage()"))

        single_data: list[str] = []
        for index in range(64):
            value = str(self._control.dynamicCall("GetSingleData(int)", index))
            if not value and index >= max(single_row_count, 1):
                break
            single_data.append(value)

        multi_data: list[list[str]] = []
        for row in range(max(multi_row_count, 0)):
            row_values: list[str] = []
            for index in range(64):
                value = str(self._control.dynamicCall("GetMultiData(int, int)", row, index))
                if not value and index > 0 and all(not cell for cell in row_values[-3:]):
                    break
                row_values.append(value)
            multi_data.append(row_values)

        self._control.clear()
        return DumpResult(
            query=self._query_name,
            rqid=rqid,
            received_rqid=self._received_rqid,
            timeout=self._timed_out,
            error_state=error_state,
            error_code=error_code,
            error_message=error_message,
            single_row_count=single_row_count,
            multi_row_count=multi_row_count,
            single_data=single_data,
            multi_data=multi_data,
            sys_messages=self._sys_messages,
        )


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: dump_indi_query.py QUERY_NAME [single_input...]")
        return 2

    app = QApplication.instance() or QApplication([])
    dumper = IndiQueryDumper(argv[1], argv[2:])
    result = dumper.run()
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    app.quit()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
