from __future__ import annotations

import ctypes
import html
import json
import os
import platform
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from homestock.indi.base import IndiClient
from homestock.models import (
    Account,
    AccountLedgerItem,
    AccountSummary,
    BalanceItem,
    DailyPrice,
    Execution,
    ForeignFlowRanking,
    FundamentalPoint,
    GoldAccountBalance,
    GoldAccountSummary,
    GoldBalanceItem,
    GoldDailyPrice,
    GoldIntradayPrice,
    GoldOrderRequest,
    GoldProduct,
    GoldQuoteSnapshot,
    HealthStatus,
    IntradayPrice,
    InvestorFlowPoint,
    MarketInvestorFlowPoint,
    MarketIndexPricePoint,
    MarketNewsItem,
    MarketScannerItem,
    NewsContent,
    OpenOrder,
    OrderBook,
    OrderBookLevel,
    OrderRequest,
    OrderResult,
    QuoteSnapshot,
    Stock,
    TopMover,
    TradeHistoryItem,
)
from homestock.ops_log import LogSource, ops_log


@dataclass
class _QueryResult:
    rqid: int
    error_state: int
    error_code: str
    error_message: str
    single_row_count: int
    multi_row_count: int


class RealIndiClient(IndiClient):
    PROG_ID = "GIEXPERTCONTROL.GiExpertControlCtrl.1"
    ACCOUNT_PASSWORD_ENV = "HOMESTOCK_ACCOUNT_PASSWORD"
    INDI_MAIN_PROCESS_NAME = "GiExpertMain.exe"
    _STOCK_MASTER_CACHE_FILE_NAME = "stock_master_cache.json"
    _NEWS_ANCHOR_PATTERN = re.compile(r'(?is)<a\b([^>]*)href=["\']([^"\']+)["\']([^>]*)>(.*?)</a>')
    _DART_RCP_NO_PATTERN = re.compile(
        r"(?i)(?:rcpNo|rcept_no|rceptNo)=([0-9]{14})|openReportViewer\(['\"]([0-9]{14})['\"]"
    )
    _MARKET_NAMES = {
        "0": "KOSPI",
        "1": "KOSDAQ",
    }
    _RANKING_MARKETS = {"kospi": "0", "kosdaq": "1", "all": "2"}
    _FLOW_DIRECTIONS = {"buy": "1", "sell": "2"}
    _MOVER_DIRECTIONS = {"up": "0", "down": "1"}
    _HIGH_LOW_MODES = {"new_high": "0", "new_low": "1", "52w_high": "2", "52w_low": "3"}
    _LIMIT_MODES = {"upper": "1", "lower": "4"}
    _ACCOUNT_LEDGER_TRANSACTION_TYPES = {
        "ALL": "0",
        "0": "0",
        "SELL": "1",
        "1": "1",
        "BUY": "2",
        "2": "2",
        "DEPOSIT": "3",
        "3": "3",
        "WITHDRAW": "4",
        "4": "4",
        "TRANSFER_IN": "5",
        "5": "5",
        "TRANSFER_OUT": "6",
        "6": "6",
        "BUY_SELL": "8",
        "8": "8",
        "DEPOSIT_WITHDRAW": "9",
        "9": "9",
        "TRANSFER_IN_OUT": "A",
        "A": "A",
        "FX": "B",
        "B": "B",
        "ELS_DLS": "C",
        "C": "C",
        "DIVIDEND": "D",
        "D": "D",
        "LOAN_INTEREST": "E",
        "E": "E",
        "CREDIT_INTEREST": "F",
        "F": "F",
    }
    _ACCOUNT_LEDGER_MARKETS = {
        "ALL": "0",
        "0": "0",
        "DOMESTIC": "1",
        "1": "1",
        "OVERSEAS": "2",
        "2": "2",
    }
    _MARKET_INDEX_SPECS = {
        "kospi200": {"query_name": "TR_ICHART", "symbol": "2101"},
        "sp500": {"query_name": "TR_INCHART", "symbol": "SPI@SPX"},
        "nasdaq": {"query_name": "TR_INCHART", "symbol": "NAS@IXIC"},
        "usdkrw": {"query_name": "TR_INCHART", "symbol": "USDKRWCOMP"},
    }
    _ACCOUNT_PRODUCT_NAME_BY_CODE = {
        "01": "종합계좌",
        "10": "코스피선물옵션",
        "11": "코스닥선물옵션",
        "21": "증권저축잔고",
        "70": "금현물",
    }
    _ACCOUNT_PRODUCT_CODE_CANDIDATES = tuple(_ACCOUNT_PRODUCT_NAME_BY_CODE.keys())
    _STOCK_PRICE_RT_TYPE = "UC"
    _STOCK_PRICE_RT_TYPES = {"SC", "UC"}
    _RT_FIELD_COUNTS = {"SC": 26, "UC": 27, "SH": 56, "UH": 114, "N0": 14, "N2": 14}
    _GOLD_RT_FIELD_COUNTS = {"XC": 20, "XH": 69}
    _GOLD_PRODUCT_CODE = "70"
    _GOLD_PRODUCTS = {
        "M04020000": GoldProduct(
            code="M04020000",
            standard_code="KRD040200002",
            name="금 99.99_1kg",
            english_name="Gold 99.99_1kg",
            listed_date="20260423",
            trading_unit=1,
        ),
        "M04020100": GoldProduct(
            code="M04020100",
            standard_code="KRD040201000",
            name="미니금 99.99_100g",
            english_name="Mini Gold 99.99_100g",
            listed_date="20260423",
            trading_unit=1,
        ),
    }
    _ORDER_BOOK_UH_TIMEOUT_MS = 12000
    _NEWS_TYPE_LABELS = {
        "A": "info",
        "M": "mt",
        "E": "ed",
        "Y": "yonhap",
        "H": "hankyung",
        "I": "internal",
        "F": "market_commentary",
        "P": "disclosure",
        "Q": "disclosure",
        "S": "disclosure",
        "G": "disclosure",
        "N": "disclosure",
        "T": "disclosure",
        "U": "overseas",
        "OA": "all",
    }

    def __init__(self) -> None:
        init_started_at = time.perf_counter()

        def init_log(step: str, message: str) -> None:
            elapsed_ms = int((time.perf_counter() - init_started_at) * 1000)
            ops_log(LogSource.STARTUP_REAL, f"{step} elapsed_ms={elapsed_ms} {message}")

        init_log("I00", "begin RealIndiClient.__init__")
        ops_log(LogSource.STARTUP_REAL, "RealIndiClient.__init__ entered")
        architecture = platform.architecture()[0]
        init_log(
            "I01",
            "process snapshot "
            f"pid={os.getpid()} thread_id={threading.get_ident()} "
            f"python_architecture={architecture} platform={platform.platform()} "
            f"cwd={os.getcwd()} SESSIONNAME={os.getenv('SESSIONNAME', '<unset>')}",
        )
        ops_log(LogSource.STARTUP_REAL, f"python_architecture={architecture}")
        ops_log(LogSource.STARTUP_REAL, f"SESSIONNAME={os.getenv('SESSIONNAME', '<unset>')}")
        try:
            session_id = self._current_session_id()
            init_log("I01", f"windows session snapshot session_id={session_id}")
            ops_log(LogSource.STARTUP_REAL, f"session_id={session_id}")
        except Exception as exc:
            init_log("I01", f"windows session snapshot failed {exc.__class__.__name__}: {exc}")
            ops_log(LogSource.STARTUP_REAL, f"session_id=unavailable ({exc.__class__.__name__}: {exc})")
        init_log("I02", "validating 32-bit Python requirement")
        if architecture != "32bit":
            init_log("I02", "validation failed: RealIndiClient requires 32-bit Python")
            ops_log(LogSource.STARTUP_REAL, "RealIndiClient requires 32-bit Python; aborting")
            raise RuntimeError("RealIndiClient requires 32-bit Python")
        init_log("I02", "validation ok")
        init_log("I03", "import PyQt5 QAxContainer/QEventLoop/QApplication begin")
        ops_log(LogSource.STARTUP_REAL, "importing PyQt5 QAxContainer/QEventLoop/QApplication")
        try:
            from PyQt5.QAxContainer import QAxWidget  # type: ignore
            from PyQt5.QtCore import QEventLoop, QTimer  # type: ignore
            from PyQt5.QtWidgets import QApplication  # type: ignore
        except ImportError as exc:
            init_log("I03", f"PyQt5 import failed {exc.__class__.__name__}: {exc}")
            ops_log(LogSource.STARTUP_REAL, f"PyQt5 import failed: {exc}")
            raise RuntimeError("RealIndiClient requires PyQt5 with QAxContainer") from exc
        init_log("I03", "PyQt5 import ok")
        ops_log(LogSource.STARTUP_REAL, "PyQt5 import ok")

        init_log("I04", "QApplication lookup begin")
        self._qax_widget_cls = QAxWidget
        existing_app = QApplication.instance()
        init_log("I04", f"QApplication existing_instance={existing_app is not None}")
        ops_log(LogSource.STARTUP_REAL, f"QApplication existing_instance={existing_app is not None}")
        init_log("I04", "QApplication create/reuse begin")
        self._app = existing_app or QApplication([])
        init_log("I04", "QApplication ready")
        ops_log(LogSource.STARTUP_REAL, "QApplication ready")
        self._event_loop_cls = QEventLoop
        self._timer_cls = QTimer
        init_log("I05", f"create TR OCX begin prog_id={self.PROG_ID}")
        ops_log(LogSource.STARTUP_REAL, f"creating TR OCX prog_id={self.PROG_ID}")
        self._tr_control = QAxWidget(self.PROG_ID)
        init_log("I05", f"create TR OCX returned is_null={self._tr_control.isNull()}")
        ops_log(LogSource.STARTUP_REAL, f"TR OCX created is_null={self._tr_control.isNull()}")
        init_log("I06", f"create RT OCX begin prog_id={self.PROG_ID}")
        ops_log(LogSource.STARTUP_REAL, f"creating RT OCX prog_id={self.PROG_ID}")
        self._rt_control = QAxWidget(self.PROG_ID)
        init_log("I06", f"create RT OCX returned is_null={self._rt_control.isNull()}")
        ops_log(LogSource.STARTUP_REAL, f"RT OCX created is_null={self._rt_control.isNull()}")
        # 기존 디버그/프로브 스크립트가 `_control`을 직접 참조하므로,
        # 하위 호환을 위해 TR 전용 인스턴스를 기존 이름으로도 노출한다.
        self._control = self._tr_control
        self._ocx_ready = not self._tr_control.isNull() and not self._rt_control.isNull()
        init_log("I07", f"OCX readiness checked ocx_ready={self._ocx_ready}")
        ops_log(LogSource.STARTUP_REAL, f"ocx_ready={self._ocx_ready}")
        self._log_control_snapshot("I07 post-create OCX snapshot")
        if not self._ocx_ready:
            init_log("I07", f"failed to create Indi OCX prog_id={self.PROG_ID}")
            ops_log(LogSource.STARTUP_REAL, f"failed to create Indi OCX prog_id={self.PROG_ID}")
            raise RuntimeError(f"failed to create Indi OCX: {self.PROG_ID}")

        init_log("I08", "initializing in-memory request/realtime state")
        self._pending_rqid: int | None = None
        self._received_rqid: int | None = None
        self._timed_out = False
        self._sys_msg_ids: list[int] = []
        self._active_event_loop = None
        self._active_rt_event_loop = None
        self._pending_rt_type: str | None = None
        self._pending_rt_code: str | None = None
        self._received_rt_type: str | None = None
        self._received_rt_code: str | None = None
        self._rt_timed_out = False
        self._tr_control_lock = threading.RLock()
        self._rt_control_lock = threading.RLock()
        self._rt_subscription_counts: dict[tuple[str, str], int] = {}
        self._rt_news_registered = False
        self._rt_disclosure_registered = False
        self._rt_snapshots: dict[tuple[str, str], list[str]] = {}
        self._rt_listeners: list[Callable[[dict[str, Any]], None]] = []
        self._gold_rt_control = None
        self._gold_rt_control_lock = threading.RLock()
        self._gold_active_rt_event_loop = None
        self._gold_pending_rt_type: str | None = None
        self._gold_pending_rt_code: str | None = None
        self._gold_received_rt_type: str | None = None
        self._gold_received_rt_code: str | None = None
        self._gold_rt_timed_out = False
        self._gold_rt_subscription_counts: dict[tuple[str, str], int] = {}
        self._gold_rt_snapshots: dict[tuple[str, str], list[str]] = {}
        self._gold_rt_listeners: list[Callable[[dict[str, Any]], None]] = []
        self._last_rt_error_details: dict[str, Any] | None = None
        init_log("I08", "in-memory request/realtime state ready")
        init_log("I09", f"capturing {self.INDI_MAIN_PROCESS_NAME} initial generation begin")
        self._giexpert_main_generation = self._capture_giexpert_main_generation()
        init_log(
            "I09",
            f"{self.INDI_MAIN_PROCESS_NAME} initial_generation="
            f"{self._format_process_generation(self._giexpert_main_generation)}",
        )
        ops_log(LogSource.STARTUP_REAL,
            f"{self.INDI_MAIN_PROCESS_NAME} initial_generation="
            f"{self._format_process_generation(self._giexpert_main_generation)}",
        )
        self._giexpert_main_current_generation = self._giexpert_main_generation
        self._giexpert_main_restarted = False
        self._giexpert_main_restart_message = ""
        self._stock_cache: list[Stock] = []
        self._kospi200_codes: set[str] = set()
        init_log("I10", "connect TR ReceiveData/ReceiveSysMsg handlers begin")
        ops_log(LogSource.STARTUP_REAL, "connecting TR ReceiveData/ReceiveSysMsg handlers")
        self._tr_control.ReceiveData.connect(self._on_receive_data)
        self._tr_control.ReceiveSysMsg.connect(self._on_receive_sys_msg)
        init_log("I10", "connect TR handlers complete")
        init_log("I11", "connect RT ReceiveRTData/ReceiveSysMsg handlers begin")
        ops_log(LogSource.STARTUP_REAL, "connecting RT ReceiveRTData/ReceiveSysMsg handlers")
        self._rt_control.ReceiveRTData.connect(self._on_receive_rt_data)
        self._rt_control.ReceiveSysMsg.connect(self._on_receive_sys_msg)
        init_log("I11", "connect RT handlers complete")
        self._log_control_snapshot("I12 pre-stock-master-restore control snapshot")
        init_log("I13", "stock master cache restore begin startup_query=disabled")
        ops_log(LogSource.STARTUP_REAL, "restoring stock master cache from disk only; startup stock_mst request disabled")
        self._restore_stock_master_cache()
        init_log(
            "I13",
            f"stock master cache restore complete stocks={len(self._stock_cache)} "
            f"kospi200_codes={len(self._kospi200_codes)}",
        )
        self._log_control_snapshot("I14 post-stock-master-restore control snapshot")
        ops_log(LogSource.STARTUP_REAL,
            f"stock master cache ready stocks={len(self._stock_cache)} "
            f"kospi200_codes={len(self._kospi200_codes)}",
        )
        init_log("I99", "RealIndiClient.__init__ complete")
        ops_log(LogSource.STARTUP_REAL, "RealIndiClient.__init__ complete")

    def health_check(self, live_orders_allowed: bool) -> HealthStatus:
        session_id = self._current_session_id()
        process_status = self._check_giexpert_main_generation()
        comm_state = self._comm_state()
        error_state = int(self._tr_control.dynamicCall("GetErrorState()"))
        error_code = self._text(self._tr_control.dynamicCall("GetErrorCode()"))
        error_message = self._text(self._tr_control.dynamicCall("GetErrorMessage()"))
        gold_rt_subscriptions = {
            f"{rt_type}:{code}": count
            for (rt_type, code), count in getattr(self, "_gold_rt_subscription_counts", {}).items()
        }
        login_ready = (
            self._ocx_ready
            and bool(process_status["running"])
            and not bool(process_status["restarted"])
        )
        ops_log(LogSource.MANAGE,
            "health_check status-only probe "
            f"ocx_ready={self._ocx_ready} comm_state={comm_state} "
            f"error_state={error_state} error_code={error_code} "
            f"login_ready={login_ready} indi_process_running={process_status['running']} "
            f"indi_process_restarted={process_status['restarted']} "
            f"rt_news_registered={self._rt_news_registered} "
            f"rt_disclosure_registered={self._rt_disclosure_registered} "
            f"gold_rt_control_ready={self._gold_rt_control is not None} "
            f"gold_rt_subscription_count={sum(gold_rt_subscriptions.values())}",
        )
        return HealthStatus(
            ok=(
                self._ocx_ready
                and bool(process_status["running"])
                and not bool(process_status["restarted"])
            ),
            backend="real",
            python_architecture=platform.architecture()[0],
            ocx_ready=self._ocx_ready,
            login_ready=login_ready,
            live_orders_allowed=live_orders_allowed,
            message=(
                "real backend status-only probe "
                f"(AccountList probe disabled, comm_state={comm_state}, "
                f"error_state={error_state}, error_code={error_code}, error_message={error_message}, "
                f"session_name={os.getenv('SESSIONNAME', '<unset>')}, session_id={session_id}, "
                f"indi_process={process_status['message']})"
            ),
            indi_process_running=bool(process_status["running"]),
            indi_process_restarted=bool(process_status["restarted"]),
            indi_process_message=str(process_status["message"]),
            rt_news_registered=self._rt_news_registered,
            rt_disclosure_registered=self._rt_disclosure_registered,
            gold_rt_control_ready=self._gold_rt_control is not None,
            gold_rt_subscription_count=sum(gold_rt_subscriptions.values()),
            gold_rt_subscriptions=gold_rt_subscriptions,
        )

    def check_indi_process_status(self) -> dict[str, object]:
        return self._check_giexpert_main_generation()

    def pump_events(self) -> None:
        self._app.processEvents()

    def close(self) -> None:
        ops_log(LogSource.STARTUP_REAL, "RealIndiClient.close entered")
        self._close_gold_realtime_registrations()
        self._close_realtime_registrations()
        self._disconnect_control_signals()
        self._delete_control_widgets()
        ops_log(LogSource.STARTUP_REAL, "RealIndiClient.close complete")

    def _close_realtime_registrations(self) -> None:
        registrations = list(getattr(self, "_rt_subscription_counts", {}).keys())
        ops_log(LogSource.MANAGE,
            f"closing realtime registrations count={len(registrations)} "
            f"news_registered={getattr(self, '_rt_news_registered', False)} "
            f"disclosure_registered={getattr(self, '_rt_disclosure_registered', False)}",
        )
        for rt_type, code in registrations:
            try:
                ops_log(LogSource.MANAGE, f"unregister realtime begin rt_type={rt_type} code={code}")
                if rt_type == "N2":
                    self._unregister_disclosure_realtime()
                elif rt_type == "N0":
                    self._unregister_news_realtime()
                else:
                    self._unregister_realtime(rt_type, code)
                ops_log(LogSource.MANAGE, f"unregister realtime success rt_type={rt_type} code={code}")
            except Exception as exc:
                ops_log(LogSource.MANAGE,
                    f"unregister realtime failed rt_type={rt_type} code={code} error={exc.__class__.__name__}: {exc}",
                )
                ops_log(LogSource.MANAGE, f"Failed to unregister realtime {rt_type} {code} during close: {exc}")
        self._rt_subscription_counts.clear()
        self._rt_news_registered = False
        self._rt_disclosure_registered = False
        self._rt_snapshots.clear()
        self._rt_listeners.clear()
        ops_log(LogSource.MANAGE, "realtime registration state cleared")

    def _close_gold_realtime_registrations(self) -> None:
        counts = getattr(self, "_gold_rt_subscription_counts", None)
        registrations = list(counts.keys()) if isinstance(counts, dict) else []
        ops_log(LogSource.MANAGE, f"closing gold realtime registrations count={len(registrations)}")
        for rt_type, code in registrations:
            try:
                ops_log(LogSource.MANAGE, f"unregister gold realtime begin rt_type={rt_type} code={code}")
                self._unregister_gold_realtime(rt_type, code)
                ops_log(LogSource.MANAGE, f"unregister gold realtime success rt_type={rt_type} code={code}")
            except Exception as exc:
                ops_log(LogSource.MANAGE,
                    f"unregister gold realtime failed rt_type={rt_type} code={code} "
                    f"error={exc.__class__.__name__}: {exc}",
                )
        if isinstance(counts, dict):
            counts.clear()
        snapshots = getattr(self, "_gold_rt_snapshots", None)
        if isinstance(snapshots, dict):
            snapshots.clear()
        listeners = getattr(self, "_gold_rt_listeners", None)
        if isinstance(listeners, list):
            listeners.clear()
        ops_log(LogSource.MANAGE, "gold realtime registration state cleared")

    def _disconnect_control_signals(self) -> None:
        signal_names = ("ReceiveData", "ReceiveRTData", "ReceiveSysMsg")
        disconnected = 0
        for control_name in ("_tr_control", "_rt_control", "_gold_rt_control"):
            control = getattr(self, control_name, None)
            if control is None:
                ops_log(LogSource.MANAGE, f"signal disconnect skipped control={control_name} reason=missing_control")
                continue
            for signal_name in signal_names:
                signal = getattr(control, signal_name, None)
                if signal is None:
                    ops_log(LogSource.MANAGE, f"signal disconnect skipped control={control_name} signal={signal_name} reason=missing_signal")
                    continue
                try:
                    signal.disconnect()
                    disconnected += 1
                except Exception:
                    continue
        ops_log(LogSource.MANAGE, f"signal disconnect complete disconnected={disconnected}")

    def _delete_control_widgets(self) -> None:
        deleted = 0
        for control_name in ("_tr_control", "_rt_control", "_gold_rt_control"):
            control = getattr(self, control_name, None)
            if control is None:
                ops_log(LogSource.MANAGE, f"deleteLater skipped control={control_name} reason=missing_control")
                continue
            try:
                control.deleteLater()
                deleted += 1
                ops_log(LogSource.MANAGE, f"deleteLater queued control={control_name}")
            except Exception as exc:
                ops_log(LogSource.MANAGE,
                    f"deleteLater failed control={control_name} error={exc.__class__.__name__}: {exc}",
                )
                ops_log(LogSource.MANAGE, f"Failed to delete {control_name} during close: {exc}")
        try:
            self._app.processEvents()
            ops_log(LogSource.MANAGE, f"delete control widgets processEvents complete deleted={deleted}")
        except Exception:
            ops_log(LogSource.MANAGE, f"delete control widgets processEvents failed deleted={deleted}")
            pass

    def list_stocks(self) -> list[Stock]:
        if not self._stock_cache:
            self._load_stock_master_cache()
        return list(self._stock_cache)

    def list_gold_products(self) -> list[GoldProduct]:
        products: list[GoldProduct] = []
        for fallback in self._GOLD_PRODUCTS.values():
            try:
                self._request("XB", [fallback.code], timeout_ms=5000)
                code = self._single_text(1) or fallback.code
                if code not in self._GOLD_PRODUCTS:
                    code = fallback.code
                products.append(
                    GoldProduct(
                        code=code,
                        standard_code=self._single_text(0) or fallback.standard_code,
                        name=self._single_text(4) or fallback.name,
                        english_name=self._single_text(5) or fallback.english_name,
                        listed_date=self._single_text(6) or fallback.listed_date,
                        trading_unit=self._single_int(13) or fallback.trading_unit,
                    )
                )
            except Exception as exc:
                ops_log(LogSource.TR_REAL,
                    f"XB gold product lookup failed code={fallback.code} "
                    f"error={exc.__class__.__name__}: {exc}; using catalog fallback",
                )
                products.append(fallback)
        return products

    def get_daily_prices(self, code: str, start_date: str | None, end_date: str | None) -> list[DailyPrice]:
        normalized_start = self._normalize_indi_date(start_date, "start_date")
        normalized_end = self._normalize_indi_date(end_date, "end_date")
        result = self._request(
            "TR_SCHART",
            [
                code,
                "D",
                "1",
                normalized_start or "00000000",
                normalized_end or "99999999",
                "9999",
            ],
        )
        prices: list[DailyPrice] = []
        for row in range(result.multi_row_count):
            date = self._multi_text(row, 0)
            if not date:
                continue
            prices.append(
                DailyPrice(
                    date=date,
                    open=self._multi_int(row, 2),
                    high=self._multi_int(row, 3),
                    low=self._multi_int(row, 4),
                    close=self._multi_int(row, 5),
                    volume=self._multi_int(row, 9),
                )
            )
        return prices

    def get_intraday_prices(self, code: str, date: str, interval_minutes: int = 5) -> list[IntradayPrice]:
        normalized_date = self._normalize_indi_date(date, "date")
        if normalized_date is None:
            raise ValueError("date is required")
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be greater than zero")
        result = self._request(
            "TR_SCHART",
            [
                code,
                "M",
                str(interval_minutes),
                normalized_date,
                normalized_date,
                "9999",
            ],
        )
        prices: list[IntradayPrice] = []
        for row in range(result.multi_row_count):
            fields = [self._multi_text(row, index) for index in range(10)]
            price = self._build_intraday_price_point(fields, normalized_date)
            if price is not None:
                prices.append(price)
        return sorted(prices, key=lambda item: (item.date, item.time), reverse=True)

    def get_gold_daily_prices(
        self,
        code: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[GoldDailyPrice]:
        normalized_code = self.normalize_gold_code(code)
        normalized_start = self._normalize_indi_date(start_date, "start_date")
        normalized_end = self._normalize_indi_date(end_date, "end_date")
        result = self._request(
            "TR_GLCHART",
            [
                normalized_code,
                "D",
                "1",
                normalized_start or "00000000",
                normalized_end or "99999999",
                "9999",
            ],
        )
        prices: list[GoldDailyPrice] = []
        for row in range(result.multi_row_count):
            date = self._multi_text(row, 0)
            if not date:
                continue
            prices.append(
                GoldDailyPrice(
                    date=date,
                    open=self._multi_int(row, 2),
                    high=self._multi_int(row, 3),
                    low=self._multi_int(row, 4),
                    close=self._multi_int(row, 5),
                    volume=self._multi_int(row, 6),
                    turnover=self._multi_int(row, 7),
                )
            )
        return prices

    def get_gold_intraday_prices(
        self,
        code: str,
        date: str,
        interval_minutes: int = 5,
    ) -> list[GoldIntradayPrice]:
        normalized_code = self.normalize_gold_code(code)
        normalized_date = self._normalize_indi_date(date, "date")
        if normalized_date is None:
            raise ValueError("date is required")
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be greater than zero")
        result = self._request(
            "TR_GLCHART",
            [
                normalized_code,
                "M",
                str(interval_minutes),
                normalized_date,
                normalized_date,
                "9999",
            ],
        )
        prices: list[GoldIntradayPrice] = []
        for row in range(result.multi_row_count):
            item = GoldIntradayPrice(
                date=self._multi_text(row, 0) or normalized_date,
                time=self._normalize_intraday_time(self._multi_text(row, 1)),
                open=self._multi_int(row, 2),
                high=self._multi_int(row, 3),
                low=self._multi_int(row, 4),
                close=self._multi_int(row, 5),
                volume=self._multi_int(row, 6),
                turnover=self._multi_int(row, 7),
            )
            if item.date:
                prices.append(item)
        return sorted(prices, key=lambda item: (item.date, item.time), reverse=True)

    def get_market_index_prices(
        self,
        start_date: str,
        end_date: str,
    ) -> dict[str, list[MarketIndexPricePoint]]:
        normalized_start = self._normalize_indi_date(start_date, "start_date")
        normalized_end = self._normalize_indi_date(end_date, "end_date")
        if normalized_start is None or normalized_end is None:
            raise ValueError("start_date and end_date are required")
        if normalized_start > normalized_end:
            raise ValueError("start_date must be on or before end_date")

        request_count = self._index_request_count(normalized_start, normalized_end)
        kospi200_symbol = str(self._MARKET_INDEX_SPECS["kospi200"]["symbol"])
        sp500_symbol = str(self._MARKET_INDEX_SPECS["sp500"]["symbol"])
        nasdaq_symbol = str(self._MARKET_INDEX_SPECS["nasdaq"]["symbol"])
        usdkrw_symbol = str(self._MARKET_INDEX_SPECS["usdkrw"]["symbol"])
        return {
            "kospi200": self._get_kospi200_index_prices(kospi200_symbol, normalized_start, normalized_end, request_count),
            "sp500": self._get_overseas_index_prices(sp500_symbol, normalized_start, normalized_end, request_count),
            "nasdaq": self._get_overseas_index_prices(nasdaq_symbol, normalized_start, normalized_end, request_count),
            "usdkrw": self._get_overseas_index_prices(usdkrw_symbol, normalized_start, normalized_end, request_count),
        }

    def get_sector_index_prices(
        self,
        sector_code: str,
        start_date: str,
        end_date: str,
        interval: str = "D",
    ) -> list[MarketIndexPricePoint]:
        normalized_start = self._normalize_indi_date(start_date, "start_date")
        normalized_end = self._normalize_indi_date(end_date, "end_date")
        if normalized_start is None or normalized_end is None:
            raise ValueError("start_date and end_date are required")
        if normalized_start > normalized_end:
            raise ValueError("start_date must be on or before end_date")
        normalized_interval = interval.strip().upper() or "D"
        if normalized_interval != "D":
            raise ValueError("only daily sector index prices are supported")
        code = sector_code.strip()
        if not code:
            raise ValueError("sector_code is required")
        return self._get_kospi200_index_prices(
            code,
            normalized_start,
            normalized_end,
            self._index_request_count(normalized_start, normalized_end),
        )

    def get_stock_sector_profile(self, code: str) -> dict[str, Any]:
        normalized_code = self._normalize_stock_code(code)
        # Indi 업종 상세 TR은 브로커 환경마다 제공 범위가 달라서 v1은 known KOSPI200
        # 보유 종목 mapping을 우선 사용하고, 모르면 unavailable profile을 반환한다.
        profiles = {
            "005930": ("2155", "코스피200 정보기술"),
            "000660": ("2155", "코스피200 정보기술"),
            "005380": ("2151", "코스피200 경기소비재"),
            "005387": ("2151", "코스피200 경기소비재"),
            "462870": ("2154", "코스피200 커뮤니케이션서비스"),
        }
        sector_code, sector_name = profiles.get(normalized_code, ("", ""))
        return {
            "code": normalized_code,
            "sector_code": sector_code,
            "sector_name": sector_name,
            "source": "known_kospi200_mapping" if sector_code else "unavailable",
        }

    def subscribe_realtime_price(self, code: str) -> dict[str, object]:
        normalized_code = self._normalize_stock_code(code)
        rt_type = self._STOCK_PRICE_RT_TYPE
        key = (rt_type, normalized_code)
        already_subscribed = self._rt_subscription_counts.get(key, 0) > 0
        # 일반 TR 요청이 같은 OCX 인스턴스의 내부 상태를 건드릴 수 있으므로,
        # 명시적 구독 요청이 들어오면 이미 구독 중이어도 등록 호출을 한 번 더 보내 상태를 복구한다.
        self._register_realtime(
            rt_type,
            normalized_code,
            wait_for_first_tick=False,
            timeout_ms=6000,
        )
        self._rt_subscription_counts[key] = self._rt_subscription_counts.get(key, 0) + 1
        return {
            "subscribed": True,
            "code": normalized_code,
            "rt_type": rt_type,
            "already_subscribed": already_subscribed,
            "message": "realtime price subscription registered",
        }

    def unsubscribe_realtime_price(self, code: str) -> dict[str, object]:
        normalized_code = self._normalize_stock_code(code)
        rt_type = self._STOCK_PRICE_RT_TYPE
        key = (rt_type, normalized_code)
        previous_count = self._rt_subscription_counts.get(key, 0)
        was_subscribed = previous_count > 0
        if was_subscribed:
            remaining_count = previous_count - 1
            if remaining_count > 0:
                self._rt_subscription_counts[key] = remaining_count
            else:
                self._rt_subscription_counts.pop(key, None)
                self._unregister_realtime(rt_type, normalized_code)
        return {
            "subscribed": False,
            "code": normalized_code,
            "rt_type": rt_type,
            "was_subscribed": was_subscribed,
            "remaining_subscriptions": max(previous_count - 1, 0),
            "message": "realtime price subscription removed" if was_subscribed else "realtime price subscription was not registered",
        }

    def subscribe_gold_realtime_price(self, code: str) -> dict[str, object]:
        normalized_code = self.normalize_gold_code(code)
        key = ("XC", normalized_code)
        already_subscribed = self._gold_rt_subscription_counts.get(key, 0) > 0
        self._register_gold_realtime(
            "XC",
            normalized_code,
            wait_for_first_tick=False,
            timeout_ms=6000,
        )
        self._gold_rt_subscription_counts[key] = self._gold_rt_subscription_counts.get(key, 0) + 1
        return {
            "subscribed": True,
            "code": normalized_code,
            "rt_type": "XC",
            "already_subscribed": already_subscribed,
            "message": "gold realtime price subscription registered",
        }

    def unsubscribe_gold_realtime_price(self, code: str) -> dict[str, object]:
        normalized_code = self.normalize_gold_code(code)
        key = ("XC", normalized_code)
        previous_count = self._gold_rt_subscription_counts.get(key, 0)
        was_subscribed = previous_count > 0
        if was_subscribed:
            remaining_count = previous_count - 1
            if remaining_count > 0:
                self._gold_rt_subscription_counts[key] = remaining_count
            else:
                self._gold_rt_subscription_counts.pop(key, None)
                self._unregister_gold_realtime("XC", normalized_code)
        return {
            "subscribed": False,
            "code": normalized_code,
            "rt_type": "XC",
            "was_subscribed": was_subscribed,
            "remaining_subscriptions": max(previous_count - 1, 0),
            "message": "gold realtime price subscription removed" if was_subscribed else "gold realtime price subscription was not registered",
        }

    def subscribe_disclosure_feed(self, code: str) -> dict[str, object]:
        normalized_code = self._normalize_stock_code(code)
        key = ("N2", "")
        already_subscribed = self._rt_subscription_counts.get(key, 0) > 0
        already_indi_registered = self._rt_disclosure_registered
        rt_disclosure_registered_now = False
        ops_log(LogSource.RT_REAL,
            f"N2 subscribe_disclosure_feed requested code={normalized_code} "
            f"already_subscribed={already_subscribed} already_indi_registered={already_indi_registered} "
            f"current_count={self._rt_subscription_counts.get(key, 0)}",
        )
        if not already_indi_registered:
            self._register_disclosure_realtime()
            rt_disclosure_registered_now = True
        self._rt_subscription_counts[key] = 1
        ops_log(LogSource.RT_REAL,
            f"N2 subscribe_disclosure_feed accepted code={normalized_code} "
            f"new_count={self._rt_subscription_counts.get(key, 0)} "
            f"rt_disclosure_registered_now={rt_disclosure_registered_now}",
        )
        return {
            "subscribed": True,
            "code": normalized_code,
            "rt_type": "N2",
            "already_subscribed": already_subscribed,
            "already_indi_registered": already_indi_registered,
            "rt_disclosure_registered_now": rt_disclosure_registered_now,
            "message": "disclosure subscription registered",
        }

    def unsubscribe_disclosure_feed(self, code: str) -> dict[str, object]:
        normalized_code = self._normalize_stock_code(code)
        key = ("N2", "")
        previous_count = self._rt_subscription_counts.get(key, 0)
        was_subscribed = previous_count > 0
        if was_subscribed:
            remaining_count = previous_count - 1
            if remaining_count > 0:
                self._rt_subscription_counts[key] = remaining_count
            else:
                self._rt_subscription_counts.pop(key, None)
                self._unregister_disclosure_realtime()
        return {
            "subscribed": False,
            "code": normalized_code,
            "rt_type": "N2",
            "was_subscribed": was_subscribed,
            "message": "disclosure subscription removed" if was_subscribed else "disclosure subscription was not registered",
        }

    def subscribe_news_feed(self, code: str | None = None) -> dict[str, object]:
        normalized_code = self._normalize_stock_code(code) if code else "*"
        key = ("N0", "")
        already_subscribed = self._rt_subscription_counts.get(key, 0) > 0
        already_indi_registered = self._rt_news_registered
        rt_news_registered_now = False
        ops_log(LogSource.RT_REAL,
            f"N0 subscribe_news_feed requested code={normalized_code} "
            f"already_subscribed={already_subscribed} already_indi_registered={already_indi_registered} "
            f"current_count={self._rt_subscription_counts.get(key, 0)}",
        )
        if not already_indi_registered:
            self._register_news_realtime()
            rt_news_registered_now = True
        self._rt_subscription_counts[key] = 1
        ops_log(LogSource.RT_REAL,
            f"N0 subscribe_news_feed accepted code={normalized_code} "
            f"new_count={self._rt_subscription_counts.get(key, 0)} "
            f"rt_news_registered_now={rt_news_registered_now}",
        )
        return {
            "subscribed": True,
            "code": None if normalized_code == "*" else normalized_code,
            "rt_type": "N0",
            "already_subscribed": already_subscribed,
            "already_indi_registered": already_indi_registered,
            "rt_news_registered_now": rt_news_registered_now,
            "message": "news subscription registered",
        }

    def unsubscribe_news_feed(self, code: str | None = None) -> dict[str, object]:
        normalized_code = self._normalize_stock_code(code) if code else "*"
        key = ("N0", "")
        previous_count = self._rt_subscription_counts.get(key, 0)
        was_subscribed = previous_count > 0
        if was_subscribed:
            remaining_count = previous_count - 1
            if remaining_count > 0:
                self._rt_subscription_counts[key] = remaining_count
            else:
                self._rt_subscription_counts.pop(key, None)
                self._unregister_news_realtime()
        return {
            "subscribed": False,
            "code": None if normalized_code == "*" else normalized_code,
            "rt_type": "N0",
            "was_subscribed": was_subscribed,
            "message": "news subscription removed" if was_subscribed else "news subscription was not registered",
        }

    def register_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._rt_control_lock:
            if listener not in self._rt_listeners:
                self._rt_listeners.append(listener)

    def unregister_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._rt_control_lock:
            self._rt_listeners = [item for item in self._rt_listeners if item is not listener]

    def register_gold_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._gold_rt_control_lock:
            if listener not in self._gold_rt_listeners:
                self._gold_rt_listeners.append(listener)

    def unregister_gold_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._gold_rt_control_lock:
            self._gold_rt_listeners = [item for item in self._gold_rt_listeners if item is not listener]

    def normalize_stock_code(self, code: str | None) -> str:
        return self._normalize_stock_code(code)

    def normalize_gold_code(self, code: str | None) -> str:
        cleaned = (code or "M04020000").strip().upper()
        if cleaned not in self._GOLD_PRODUCTS:
            raise ValueError("gold code must be one of M04020000 or M04020100")
        return cleaned

    def get_last_rt_error_details(self) -> dict[str, Any] | None:
        if self._last_rt_error_details is None:
            return None
        return dict(self._last_rt_error_details)

    def get_accounts(self) -> list[Account]:
        result = self._request("AccountList")
        account_rows: list[tuple[str, str]] = []
        for row in range(result.multi_row_count):
            account_no = self._multi_text(row, 0)
            if not account_no:
                continue
            account_rows.append((account_no, self._multi_text(row, 1)))

        account_password = os.getenv(self.ACCOUNT_PASSWORD_ENV, "").strip()
        accounts: list[Account] = []
        for account_no, account_name in account_rows:
            metadata = self._get_account_metadata(account_no, account_password) if account_password else {}
            product_code = self._text_or_none(metadata.get("product_code"))
            product_name = self._text_or_none(metadata.get("product_name"))
            parent_product_code = self._text_or_none(metadata.get("parent_product_code"))
            accounts.append(
                Account(
                    account_no=account_no,
                    name=account_name,
                    product_code=product_code,
                    product_name=product_name or self._ACCOUNT_PRODUCT_NAME_BY_CODE.get(product_code or ""),
                    parent_product_code=parent_product_code,
                )
            )
        return accounts

    def get_account_summary(self, account_no: str) -> AccountSummary:
        account_password = os.getenv(self.ACCOUNT_PASSWORD_ENV, "").strip()
        if not account_password:
            raise RuntimeError(f"{self.ACCOUNT_PASSWORD_ENV} is required for account summary queries")

        self._request(
            "SABA610Q1",
            [
                account_no,
                "01",
                account_password,
                "1",
                "1",
                "0",
                "0",
                "0",
            ],
        )
        # SABA610Q1은 현금/주식 잔고 기준의 요약 수치를 내려준다.
        # 현재 AccountSummary 모델에는 가장 가까운 의미의 필드로 대응시킨다.
        cash_total_deposit = self._single_int(2)           # 예수금
        cash_withdrawable_amount = self._single_int(9)     # 인출가능금액
        cash_orderable_amount = self._single_int(12)       # 주문가능현금
        cash_total_purchase_amount = self._single_int(15)  # 주식매수금액
        cash_total_evaluation_amount = self._single_int(18)  # 주식평가금액
        cash_total_profit_loss = self._single_int(19)      # 미실현손익금액
        cash_total_return_rate = self._single_float(22)    # 미실현손익율
        cash_estimated_total_deposit = self._single_int(8)  # D+2 추정예수금

        self._request(
            "SABA655Q1",
            [
                account_no,
                "01",
                account_password,
            ],
        )
        return AccountSummary(
            account_no=account_no,
            total_deposit=cash_total_deposit,
            orderable_amount=cash_orderable_amount,
            withdrawable_amount=cash_withdrawable_amount,
            total_purchase_amount=cash_total_purchase_amount,
            total_evaluation_amount=cash_total_evaluation_amount,
            total_profit_loss=cash_total_profit_loss,
            total_return_rate=cash_total_return_rate,
            estimated_total_deposit=cash_estimated_total_deposit,
            net_asset_value=self._single_int(0),
            total_asset_value=self._single_int(1),
            stock_asset_value=self._single_int(3),
        )

    def get_gold_account_summary(self, account_no: str) -> GoldAccountSummary:
        summary, _ = self._request_gold_account_balance_parts(account_no, "gold account summary query")
        return summary

    def get_gold_account_balance(self, account_no: str) -> GoldAccountBalance:
        summary, balances = self._request_gold_account_balance_parts(account_no, "gold account balance query")
        if len(balances) > 1:
            raise RuntimeError(f"SABA835Q1 returned multiple gold balance rows: {len(balances)}")
        return GoldAccountBalance(
            account_no=account_no,
            summary=summary,
            balance=balances[0] if balances else None,
        )

    def _request_gold_account_balance_parts(
        self,
        account_no: str,
        password_reason: str,
    ) -> tuple[GoldAccountSummary, list[GoldBalanceItem]]:
        account_password = self._account_password(password_reason)
        result = self._request(
            "SABA835Q1",
            [
                account_no,
                self._GOLD_PRODUCT_CODE,
                account_password,
                "1",
            ],
        )
        summary = GoldAccountSummary(
            account_no=account_no,
            total_deposit=self._single_int(0),
            orderable_amount=self._single_int(1),
            withdrawable_amount=self._single_int(2),
            total_purchase_amount=self._single_int(3),
            total_evaluation_amount=self._single_int(4),
            total_profit_loss=self._single_int(5),
            total_asset_value=self._single_int(6),
            total_return_rate=self._single_float(7),
            total_margin=self._single_int(8),
            settlement_buy_amount1=self._single_int(9),
            settlement_sell_amount1=self._single_int(10),
            buy_settlement_amount=self._single_int(11),
            sell_settlement_amount=self._single_int(12),
            estimated_total_deposit=self._single_int(13),
        )
        balances = self._read_gold_balance_rows(account_no, result.multi_row_count)
        return summary, balances

    def _read_gold_balance_rows(self, account_no: str, row_count: int) -> list[GoldBalanceItem]:
        items: list[GoldBalanceItem] = []
        for row in range(row_count):
            code = self._multi_text(row, 0)
            name = self._multi_text(row, 1)
            if not code:
                continue
            normalized_code = self.normalize_gold_code(code)
            items.append(
                GoldBalanceItem(
                    account_no=account_no,
                    code=normalized_code,
                    name=name or self._GOLD_PRODUCTS[normalized_code].name,
                    credit_type=self._multi_text(row, 2),
                    quantity=self._multi_int(row, 3),
                    sellable_quantity=self._multi_int(row, 4),
                    restricted_quantity=self._multi_int(row, 5),
                    deliverable_quantity=self._multi_int(row, 6),
                    avg_price=self._multi_int(row, 7),
                    current_price=self._multi_int(row, 8),
                    price_change=self._multi_int(row, 9),
                    purchase_amount=self._multi_int(row, 10),
                    credit_amount=self._multi_int(row, 11),
                    valuation_amount=self._multi_int(row, 12),
                    profit_loss=self._multi_int(row, 13),
                    return_rate=self._multi_float(row, 14),
                    trading_unit=self._multi_int(row, 15),
                    security_type=self._multi_text(row, 16),
                )
            )
        return items

    def _get_account_metadata(self, account_no: str, account_password: str) -> dict[str, str]:
        metadata = self._get_account_metadata_from_specific_day_balance(account_no, account_password)
        if metadata:
            return metadata
        metadata = self._get_account_metadata_from_trade_history(account_no, account_password)
        if metadata:
            return metadata
        return {}

    def _get_account_metadata_from_specific_day_balance(self, account_no: str, account_password: str) -> dict[str, str]:
        today = datetime.now().strftime("%Y%m%d")
        for product_code in self._ACCOUNT_PRODUCT_CODE_CANDIDATES:
            try:
                result = self._request(
                    "SAAA612QB",
                    [
                        account_no,
                        product_code,
                        today,
                        "1",
                        "",
                        account_password,
                    ],
                )
            except RuntimeError:
                continue
            if result.multi_row_count <= 0:
                continue
            resolved_product_code = self._multi_text(0, 9) or product_code
            return {
                "product_code": resolved_product_code,
                "product_name": self._multi_text(0, 2) or self._ACCOUNT_PRODUCT_NAME_BY_CODE.get(resolved_product_code, ""),
                "parent_product_code": self._multi_text(0, 11),
            }
        return {}

    def _get_account_metadata_from_trade_history(self, account_no: str, account_password: str) -> dict[str, str]:
        today = datetime.now()
        start_date = (today - timedelta(days=365)).strftime("%Y%m%d")
        end_date = today.strftime("%Y%m%d")
        for product_code in self._ACCOUNT_PRODUCT_CODE_CANDIDATES:
            try:
                result = self._request(
                    "SABA233Q5",
                    [
                        "1",
                        "",
                        account_no,
                        product_code,
                        account_password,
                        start_date,
                        end_date,
                        "",
                        "0",
                        "1",
                        "0",
                        "1",
                        "0",
                    ],
                )
            except RuntimeError:
                continue
            if result.multi_row_count <= 0:
                continue
            resolved_product_code = self._multi_text(0, 1) or product_code
            return {
                "product_code": resolved_product_code,
                "product_name": self._ACCOUNT_PRODUCT_NAME_BY_CODE.get(resolved_product_code, ""),
                "parent_product_code": "",
            }
        return {}

    @staticmethod
    def _text_or_none(value: object) -> str | None:
        text = str(value or "").strip()
        return text or None

    def get_fundamentals(
        self,
        code: str,
        consolidated: bool = True,
        quarterly: bool = True,
    ) -> list[FundamentalPoint]:
        result = self._request(
            "TR4_FUNDA3",
            [
                code,
                "1" if consolidated else "0",
                "1" if quarterly else "0",
            ],
        )
        items: list[FundamentalPoint] = []
        for row in range(result.multi_row_count):
            date = self._multi_text(row, 0)
            if not date:
                continue
            eps_growth = self._multi_float(row, 11)
            per_value = self._multi_float(row, 13)
            items.append(
                FundamentalPoint(
                    code=code,
                    date=date,
                    period_type="quarterly" if quarterly else "annual",
                    revenue=self._multi_int(row, 2),
                    operating_income=self._multi_int(row, 3),
                    net_income=self._multi_int(row, 6),
                    operating_margin=self._multi_float(row, 7),
                    net_margin=self._multi_float(row, 8),
                    roe=self._multi_float(row, 9),
                    eps=self._multi_float(row, 10),
                    eps_growth=eps_growth,
                    bps=self._multi_float(row, 12),
                    per=per_value,
                    per_ttm=self._multi_float(row, 14),
                    pbr=self._multi_float(row, 15),
                    dps=self._multi_float(row, 16),
                    dividend_yield=self._multi_float(row, 17),
                    ev_ebitda=self._multi_float(row, 18),
                    peg=self._calculate_peg(per_value, eps_growth),
                )
            )
        return items

    def get_quote_snapshot(self, code: str) -> QuoteSnapshot:
        self._request("TR_1110_11", [code])
        return QuoteSnapshot(
            code=code,
            current_price=self._single_int(24),
            previous_close=self._single_int(0),
            change_amount=self._single_int(2),
            change_percent=self._single_float(3),
            year_high=self._single_int(8),
            year_low=self._single_int(9),
            year_high_date=self._single_text(10),
            year_low_date=self._single_text(11),
            upper_limit=self._single_int(12),
            lower_limit=self._single_int(13),
            market_cap=self._single_int(15),
            foreign_ownership_ratio=self._single_float(17),
            eps=self._single_float(21),
            per=self._single_float(22),
        )

    def get_gold_quote_snapshot(self, code: str) -> GoldQuoteSnapshot:
        normalized_code = self.normalize_gold_code(code)
        snapshot = self._get_gold_rt_snapshot_once("XC", normalized_code, timeout_ms=3000)
        if not snapshot:
            raise RuntimeError(f"gold quote unavailable: {normalized_code}")
        return self._build_gold_quote_snapshot(snapshot, normalized_code)

    def get_investor_flow_by_stock(
        self,
        code: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[InvestorFlowPoint]:
        normalized_start = self._normalize_indi_date(start_date, "start_date")
        normalized_end = self._normalize_indi_date(end_date, "end_date")
        result = self._request(
            "TR_1206",
            [
                code,
                normalized_start or "00000000",
                normalized_end or "99999999",
                "1",
                "0",
            ],
        )
        items: list[InvestorFlowPoint] = []
        for row in range(result.multi_row_count):
            date = self._multi_text(row, 0)
            if not date:
                continue
            items.append(
                InvestorFlowPoint(
                    code=code,
                    date=date,
                    close=self._multi_int(row, 1),
                    volume=self._multi_int(row, 7),
                    retail_net=self._multi_int(row, 10),
                    retail_cumulative_net=self._multi_int(row, 13),
                    foreign_net=self._multi_int(row, 16),
                    foreign_cumulative_net=self._multi_int(row, 19),
                    institution_net=self._multi_int(row, 22),
                    institution_cumulative_net=self._multi_int(row, 25),
                )
            )
        return items

    def get_market_investor_flow_intraday(
        self,
        include_institution_breakdown: bool = False,
    ) -> list[MarketInvestorFlowPoint]:
        result = self._request(
            "TR_1202_B",
            [
                "0001",
                "01",
                "1",
                "010",
            ],
        )
        items: list[MarketInvestorFlowPoint] = []
        for row in range(result.multi_row_count):
            item_time = self._market_flow_row_time(row)
            items.append(
                MarketInvestorFlowPoint(
                    time=item_time,
                    retail=self._market_flow_group(row, 1),
                    foreign=self._market_flow_group(row, 4),
                    institution=self._market_flow_group(row, 7),
                    institution_breakdown=(
                        self._market_flow_institution_breakdown(row) if include_institution_breakdown else None
                    ),
                )
            )
        return items

    def _market_flow_institution_breakdown(self, row: int) -> dict[str, dict[str, int]]:
        return {
            "securities": self._market_flow_group(row, 10),
            "investment_trust": self._market_flow_group(row, 13),
            "bank": self._market_flow_group(row, 16),
            "merchant_bank": self._market_flow_group(row, 19),
            "insurance": self._market_flow_group(row, 22),
            "pension_fund": self._market_flow_group(row, 25),
            "other_corporation": self._market_flow_group(row, 28),
            "other_foreign": self._market_flow_group(row, 31),
            "futures_dealer": self._market_flow_group(row, 34),
            "private_fund": self._market_flow_group(row, 37),
        }

    def _market_flow_row_time(self, row: int) -> str:
        primary = self._multi_text(row, 0)
        fallback = self._multi_text(row, 40)
        return self._normalize_news_time(primary or fallback, "time") or "000000"

    def _market_flow_group(self, row: int, start_index: int) -> dict[str, int]:
        return {
            "buy": self._multi_int(row, start_index),
            "sell": self._multi_int(row, start_index + 1),
            "net": self._multi_int(row, start_index + 2),
        }

    def get_foreign_flow_rankings(
        self,
        market: str = "all",
        consecutive_days: int = 3,
        direction: str = "buy",
    ) -> list[ForeignFlowRanking]:
        result = self._request(
            "TR_1406",
            [
                self._RANKING_MARKETS.get(market, "2"),
                str(max(1, consecutive_days)),
                self._FLOW_DIRECTIONS.get(direction, "1"),
            ],
        )
        items: list[ForeignFlowRanking] = []
        for row in range(result.multi_row_count):
            code = self._multi_text(row, 0)
            if not code:
                continue
            items.append(
                ForeignFlowRanking(
                    code=code,
                    market=self._multi_text(row, 1),
                    name=self._multi_text(row, 2),
                    current_price=self._multi_int(row, 3),
                    change_percent=self._multi_float(row, 6),
                    foreign_cumulative_volume=self._multi_int(row, 7),
                    foreign_ownership_ratio=self._multi_float(row, 8),
                    institution_cumulative_volume=self._multi_int(row, 9),
                    listed_shares=self._multi_int(row, 10),
                )
            )
        return items

    def get_top_movers(
        self,
        market: str = "all",
        direction: str = "up",
        date: str | None = None,
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[TopMover]:
        result = self._request(
            "TR_1863",
            [
                self._RANKING_MARKETS.get(market, "2"),
                self._MOVER_DIRECTIONS.get(direction, "0"),
                "0" if direction == "up" else "-30",
                "30" if direction == "up" else "0",
                "1000",
                "0" if date is None else "1",
                date or datetime.now().strftime("%Y%m%d"),
                "0",
                "1",
            ],
        )
        items: list[TopMover] = []
        for row in range(result.multi_row_count):
            code = self._multi_text(row, 1)
            if not code:
                continue
            items.append(
                TopMover(
                    rank=self._multi_int(row, 0),
                    code=code,
                    name=self._multi_text(row, 2),
                    current_price=self._multi_int(row, 3),
                    change_percent=self._multi_float(row, 6),
                    volume=self._multi_int(row, 8),
                    trade_strength=self._multi_float(row, 7),
                )
            )
        items = self._filter_kospi200_items(items, kospi200_only)
        return self._limit_items(items, limit)

    def list_stock_news(self, code: str, date: str | None = None) -> list[MarketNewsItem]:
        normalized_code = self._normalize_stock_code(code)
        result = self._request(
            "TR_3100_D",
            [
                # TR_3100_D의 뉴스_종목코드는 카탈로그 예시가 6자리 현물코드(예: 055550)다.
                # 주문 TR처럼 A 접두어를 붙이면 종목 헤드라인이 비는 경우가 있어,
                # 여기서는 정규화된 6자리 코드 그대로 전달한다.
                normalized_code,
                # TR_3100_D 구분: 1=전체, 2=뉴스, 3=공시.
                # MCP 표면 설명은 "뉴스/공시 헤드라인"이므로 기본은 전체로 둔다.
                "1",
                date or datetime.now().strftime("%Y%m%d"),
            ],
        )
        items: list[MarketNewsItem] = []
        for row in range(result.multi_row_count):
            article_id = self._multi_text(row, 4)
            if not article_id:
                continue
            items.append(
                MarketNewsItem(
                    date=self._multi_text(row, 0),
                    time=self._multi_text(row, 1),
                    title=self._multi_text(row, 2),
                    news_type=self._multi_text(row, 3),
                    news_type_label=self._news_type_label(self._multi_text(row, 3)),
                    code=normalized_code,
                    article_id=article_id,
                )
            )
        return items

    def list_market_flow_news(
        self,
        date: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> list[MarketNewsItem]:
        normalized_date = self._normalize_indi_date(date, "date") or datetime.now().strftime("%Y%m%d")
        start = self._normalize_news_time(from_time, "from_time")
        end = self._normalize_news_time(to_time, "to_time")
        if start is not None and end is not None and start > end:
            raise ValueError("from_time must be on or before to_time")

        result = self._request(
            "TR_3102_CT",
            [
                "09",
                normalized_date,
            ],
        )
        items: list[MarketNewsItem] = []
        for row in range(result.multi_row_count):
            article_id = self._multi_text(row, 5)
            if not article_id:
                continue
            item_time = self._normalize_news_time(self._multi_text(row, 1), "time") or "000000"
            if start is not None and item_time < start:
                continue
            if end is not None and item_time > end:
                continue
            item_type = self._multi_text(row, 3)
            items.append(
                MarketNewsItem(
                    date=self._multi_text(row, 0) or normalized_date,
                    time=item_time,
                    title=self._multi_text(row, 2),
                    news_type=item_type,
                    news_type_label=self._news_type_label(item_type),
                    code=self._multi_text(row, 4),
                    article_id=article_id,
                )
            )
        return items

    def get_news_content(self, news_type: str, date: str, article_id: str) -> NewsContent:
        result = self._request(
            "TR_3100",
            [
                news_type,
                date,
                article_id,
            ],
        )
        del result
        extracted_codes = self._chunk_codes(self._multi_text(0, 3))
        content_parts = [self._multi_text(0, index) for index in range(4, 10)]
        raw_html = "\n".join(part for part in content_parts if part).strip()
        content = self._clean_news_content_html(raw_html)
        links = self._extract_news_content_links(raw_html)
        return NewsContent(
            news_type=self._multi_text(0, 0),
            news_type_label=self._news_type_label(self._multi_text(0, 0)),
            date=self._multi_text(0, 1),
            time=self._multi_text(0, 2),
            extracted_codes=extracted_codes,
            content=content,
            raw_html=raw_html,
            links=links,
            rcpNo=self._extract_news_content_rcp_no(raw_html, links),
        )

    @staticmethod
    def _normalize_news_query_type(news_type: str) -> tuple[str, str | None]:
        cleaned = str(news_type or "").strip().upper()
        if not cleaned:
            raise ValueError("news_type is required")
        if cleaned in {"*", "ALL", "OA"}:
            return "OA", None
        if cleaned == "A":
            return "OA", "A"
        return cleaned, cleaned

    @staticmethod
    def _normalize_news_time(value: str | None, field_name: str) -> str | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        digits = "".join(ch for ch in raw if ch.isdigit())
        if not digits:
            raise ValueError(f"{field_name} must contain digits")
        if len(digits) <= 2:
            normalized = digits.zfill(2) + "0000"
        elif len(digits) <= 4:
            normalized = digits.zfill(4) + "00"
        elif len(digits) == 5:
            normalized = digits.zfill(6)
        else:
            normalized = digits[:6]
        hour = int(normalized[:2])
        minute = int(normalized[2:4])
        second = int(normalized[4:6])
        if hour > 23 or minute > 59 or second > 59:
            raise ValueError(f"{field_name} must be a valid HHMMSS time")
        return normalized

    def get_volume_surge(
        self,
        market: str = "all",
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        result = self._request(
            "TR_1864",
            [
                self._RANKING_MARKETS.get(market, "2"),
                "1",
                "1",
                "1000",
                "1",
                "0",
            ],
        )
        items: list[MarketScannerItem] = []
        for row in range(result.multi_row_count):
            code = self._multi_text(row, 1)
            if not code:
                continue
            items.append(
                MarketScannerItem(
                    code=code,
                    name=self._multi_text(row, 2),
                    current_price=self._multi_int(row, 3),
                    change_percent=self._multi_float(row, 6),
                    volume=self._multi_int(row, 7),
                    metric_value=self._multi_float(row, 8),
                    metric_label="volume_surge_rate",
                )
            )
        items = self._filter_kospi200_items(items, kospi200_only)
        return self._limit_items(items, limit)

    def get_new_highs_lows(
        self,
        market: str = "all",
        mode: str = "new_high",
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        result = self._request(
            "TR_1505_03",
            [
                self._RANKING_MARKETS.get(market, "2"),
                self._HIGH_LOW_MODES.get(mode, "0"),
                "0",
                "1",
                "0",
            ],
        )
        items: list[MarketScannerItem] = []
        for row in range(result.multi_row_count):
            code = self._multi_text(row, 0)
            if not code:
                continue
            items.append(
                MarketScannerItem(
                    code=code,
                    name=self._multi_text(row, 1),
                    current_price=self._multi_int(row, 2),
                    change_percent=self._multi_float(row, 5),
                    volume=self._multi_int(row, 8),
                    metric_value=self._multi_float(row, 6),
                    metric_label="new_high_price" if mode in {"new_high", "52w_high"} else "new_low_price",
                )
            )
        items = self._filter_kospi200_items(items, kospi200_only)
        return self._limit_items(items, limit)

    def get_limit_hits(
        self,
        market: str = "all",
        mode: str = "upper",
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        result = self._request(
            "TR_1860",
            [
                self._RANKING_MARKETS.get(market, "2"),
                self._LIMIT_MODES.get(mode, "1"),
                datetime.now().strftime("%Y%m%d"),
                "0",
                "1",
                "0",
            ],
        )
        items: list[MarketScannerItem] = []
        for row in range(result.multi_row_count):
            code = self._multi_text(row, 0)
            if not code:
                continue
            items.append(
                MarketScannerItem(
                    code=code,
                    name=self._multi_text(row, 1),
                    current_price=self._multi_int(row, 3),
                    change_percent=self._multi_float(row, 6),
                    volume=self._multi_int(row, 12),
                    metric_value=self._multi_float(row, 10),
                    metric_label="consecutive_days",
                )
            )
        return self._filter_kospi200_items(items, kospi200_only)

    def get_order_book(self, code: str) -> OrderBook:
        normalized_code = self._normalize_stock_code(code)
        try:
            snapshot = self._get_rt_snapshot_once("UH", normalized_code, timeout_ms=self._ORDER_BOOK_UH_TIMEOUT_MS)
        except (RuntimeError, TimeoutError):
            snapshot = None
        if snapshot:
            return self._build_integrated_order_book(normalized_code, snapshot)
        return self._get_best_order_book_from_tr(normalized_code)

    def get_gold_order_book(self, code: str) -> OrderBook:
        normalized_code = self.normalize_gold_code(code)
        try:
            snapshot = self._get_gold_rt_snapshot_once("XH", normalized_code, timeout_ms=3000)
        except (RuntimeError, TimeoutError) as exc:
            return self._unavailable_order_book(
                normalized_code,
                source="XH",
                message=f"gold order book unavailable: {exc}",
            )
        if not snapshot:
            return self._unavailable_order_book(
                normalized_code,
                source="XH",
                message="gold order book unavailable: empty XH snapshot",
            )
        return self._build_gold_order_book(normalized_code, snapshot)

    def get_cash_order_book_snapshot(self, code: str) -> OrderBook:
        normalized_code = self._normalize_stock_code(code)
        try:
            snapshot = self._get_rt_snapshot_once("SH", normalized_code, timeout_ms=3000)
        except (RuntimeError, TimeoutError) as exc:
            return self._unavailable_order_book(
                normalized_code,
                source="SH",
                message=f"cash order book unavailable: no fresh SH tick ({exc})",
            )
        if not snapshot:
            return self._unavailable_order_book(
                normalized_code,
                source="SH",
                message="cash order book unavailable: empty SH snapshot",
            )
        return self._build_cash_order_book(normalized_code, snapshot)

    def get_balance(self, account_no: str) -> list[BalanceItem]:
        account_password = os.getenv(self.ACCOUNT_PASSWORD_ENV, "").strip()
        if not account_password:
            raise RuntimeError(f"{self.ACCOUNT_PASSWORD_ENV} is required for balance queries")

        result = self._request(
            "SABA200QB",
            [
                account_no,
                "01",
                account_password,
            ],
        )
        items: list[BalanceItem] = []
        for row in range(result.multi_row_count):
            code = self._multi_text(row, 0)
            name = self._multi_text(row, 1)
            if not code and not name:
                continue
            items.append(
                BalanceItem(
                    account_no=account_no,
                    code=code,
                    name=name,
                    quantity=self._multi_int(row, 2),
                    avg_price=self._multi_int(row, 6),
                    current_price=self._multi_int(row, 5),
                )
            )
        return items

    def get_gold_balance(self, account_no: str) -> list[GoldBalanceItem]:
        _, balances = self._request_gold_account_balance_parts(account_no, "gold balance query")
        return balances

    def get_executions(self, account_no: str) -> list[Execution]:
        account_password = os.getenv(self.ACCOUNT_PASSWORD_ENV, "").strip()
        if not account_password:
            raise RuntimeError(f"{self.ACCOUNT_PASSWORD_ENV} is required for execution queries")

        result = self._request(
            "SABA231Q1",
            [
                datetime.now().strftime("%Y%m%d"),
                account_no,
                account_password,
                "00",
                "0",
                "0",
                "*",
                "01",
                "Y",
            ],
        )
        executions: list[Execution] = []
        for row in range(result.multi_row_count):
            order_id = self._multi_text(row, 0)
            code = self._multi_text(row, 13)
            if not order_id and not code:
                continue
            side_code = self._multi_text(row, 4)
            executions.append(
                Execution(
                    order_id=order_id,
                    code=code,
                    side="buy" if side_code == "2" else "sell",
                    quantity=self._multi_int(row, 24),
                    price=self._multi_int(row, 25),
                    status=self._multi_text(row, 31) or self._multi_text(row, 23),
                    executed_at=self._format_execution_timestamp(
                        datetime.now().strftime("%Y%m%d"),
                        self._multi_text(row, 22),
                    ),
                    raw_order_id=order_id,
                    original_order_id=self._multi_text(row, 1),
                    sor_order_id=self._multi_text(row, 47),
                    sor_original_order_id=self._multi_text(row, 48),
                )
            )
        return executions

    def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
        account_password = self._account_password("open order queries")
        normalized_code = self._normalize_stock_code(code) if code else None

        result = self._request(
            "SABA231Q1",
            [
                datetime.now().strftime("%Y%m%d"),
                account_no,
                account_password,
                "00",
                "2",  # 체결구분: 미체결
                "0",  # 건별구분: 합산(주문건별합산)
                self._build_stock_order_code(normalized_code) if normalized_code else "*",
                "",
                "Y",
            ],
        )
        items: list[OpenOrder] = []
        for row in range(result.multi_row_count):
            order_id = self._multi_text(row, 0)
            raw_code = self._multi_text(row, 13)
            current_code = self._normalize_stock_code(raw_code)
            if not order_id or not current_code:
                continue
            if normalized_code and current_code != normalized_code:
                continue
            quantity = self._multi_int(row, 15)
            filled_quantity = self._multi_int(row, 24)
            unfilled_quantity = self._multi_int(row, 26)
            if unfilled_quantity <= 0:
                continue
            items.append(
                OpenOrder(
                    order_id=order_id,
                    code=current_code,
                    name=self._multi_text(row, 14),
                    side="buy" if self._multi_text(row, 4) == "2" else "sell",
                    order_type=self._normalize_order_type_name(self._multi_text(row, 30)),
                    price=self._multi_int(row, 16),
                    quantity=quantity,
                    filled_quantity=filled_quantity,
                    unfilled_quantity=unfilled_quantity,
                    order_time=self._format_compact_timestamp(
                        datetime.now().strftime("%Y%m%d"),
                        self._multi_text(row, 18),
                    ),
                    status="partial" if filled_quantity > 0 else "pending",
                    raw_order_id=order_id,
                    original_raw_order_id=self._multi_text(row, 1),
                    order_method_code=self._multi_text(row, 41),
                    order_method_name=self._multi_text(row, 42),
                    order_exchange_code=self._multi_text(row, 43),
                    order_exchange_name=self._multi_text(row, 44),
                    sor_order_id=self._multi_text(row, 47),
                    sor_original_order_id=self._multi_text(row, 48),
                    credit_trade_type=self._multi_text(row, 29),
                )
            )
        return items

    def get_trade_history(
        self,
        account_no: str,
        code: str | None,
        start_date: str,
        end_date: str | None = None,
    ) -> list[TradeHistoryItem]:
        account_password = self._account_password("trade history queries")
        normalized_code = self._normalize_stock_code(code) if code else None
        normalized_start_date = self._normalize_indi_date(start_date, "start_date")
        normalized_end_date = self._normalize_indi_date(end_date, "end_date") or datetime.now().strftime("%Y%m%d")
        start_day = datetime.strptime(normalized_start_date or datetime.now().strftime("%Y%m%d"), "%Y%m%d").date()
        end_day = datetime.strptime(normalized_end_date, "%Y%m%d").date()

        items: list[TradeHistoryItem] = []
        current_start = start_day
        while current_start <= end_day:
            current_end = min(current_start + timedelta(days=364), end_day)
            result = self._request(
                "SABA233Q5",
                [
                    "1",  # 고객계좌조회구분코드: 계좌번호 기준
                    "",   # 고객번호
                    account_no,
                    "01",  # 상품
                    account_password,
                    current_start.strftime("%Y%m%d"),
                    current_end.strftime("%Y%m%d"),
                    self._build_stock_order_code(normalized_code) if normalized_code else "",
                    "0",  # 주식조회구분코드: 전체
                    "1",  # 구분: 매매일
                    "0",  # 조회구분: 전체
                    "1",  # 처리구분: 건별
                    "0",  # 조회구분코드: 일별
                ],
            )
            for row in range(result.multi_row_count):
                raw_code = self._multi_text(row, 4)
                current_code = self._normalize_stock_code(raw_code)
                if normalized_code and current_code != normalized_code:
                    continue
                items.append(
                    TradeHistoryItem(
                        date=self._multi_text(row, 2),
                        trade_type=self._multi_text(row, 3),
                        code=current_code,
                        raw_code=raw_code,
                        name=self._multi_text(row, 5),
                        quantity=self._multi_int(row, 6),
                        price=self._multi_int(row, 7),
                        fee=self._multi_int(row, 8),
                        tax=self._multi_int(row, 9),
                        trade_amount=self._multi_int(row, 10),
                        credit_amount=self._multi_int(row, 11),
                        unpaid_repayment=self._multi_int(row, 14),
                        overdue_fee=self._multi_int(row, 15),
                        credit_interest=self._multi_int(row, 12),
                        change_amount=self._multi_int(row, 13),
                        final_amount=self._multi_int(row, 13),
                        order_channel="",
                    )
                )
            current_start = current_end + timedelta(days=1)
        return items

    def get_account_ledger(
        self,
        account_no: str,
        start_date: str,
        end_date: str | None = None,
        transaction_type: str = "all",
        market: str = "all",
        include_mmw: bool = False,
        include_rp_details: bool = False,
        code: str | None = None,
        product_code: str | None = None,
        admin: str | None = None,
    ) -> list[AccountLedgerItem]:
        account_password = self._account_password("account ledger queries")
        normalized_code = self._normalize_stock_code(code) if code else None
        normalized_start_date = self._normalize_indi_date(start_date, "start_date")
        normalized_end_date = self._normalize_indi_date(end_date, "end_date") or datetime.now().strftime("%Y%m%d")
        start_day = datetime.strptime(normalized_start_date or datetime.now().strftime("%Y%m%d"), "%Y%m%d").date()
        end_day = datetime.strptime(normalized_end_date, "%Y%m%d").date()
        resolved_product_code = self._resolve_account_ledger_product_code(account_no, account_password, product_code)
        transaction_type_code = self._normalize_account_ledger_transaction_type(transaction_type)
        market_code = self._normalize_account_ledger_market(market)
        admin_value = (admin or "").strip()

        items: list[AccountLedgerItem] = []
        current_start = start_day
        while current_start <= end_day:
            current_end = min(current_start + timedelta(days=364), end_day)
            result = self._request(
                "SACA132Q1",
                [
                    admin_value,
                    account_no,
                    resolved_product_code,
                    account_password,
                    current_start.strftime("%Y%m%d"),
                    current_end.strftime("%Y%m%d"),
                    transaction_type_code,
                    market_code,
                    "1" if include_mmw else "0",
                    "1" if include_rp_details else "0",
                    self._build_stock_order_code(normalized_code) if normalized_code else "",
                ],
            )
            for row in range(result.multi_row_count):
                raw_code = self._multi_text(row, 3)
                current_code = self._normalize_stock_code(raw_code)
                if normalized_code and current_code != normalized_code:
                    continue
                items.append(
                    AccountLedgerItem(
                        date=self._multi_text(row, 0),
                        transaction_type=self._multi_text(row, 1),
                        summary=self._multi_text(row, 2),
                        code=current_code,
                        raw_code=raw_code,
                        name=self._multi_text(row, 4),
                        quantity=self._multi_int(row, 5),
                        price=self._multi_int(row, 6),
                        fee=self._multi_int(row, 7),
                        tax=self._multi_int(row, 8),
                        trade_amount=self._multi_int(row, 9),
                        credit_amount=self._multi_int(row, 10),
                        unpaid_repayment=self._multi_int(row, 11),
                        credit_interest=self._multi_int(row, 12),
                        overdue_fee=self._multi_int(row, 13),
                        substitute_account=self._multi_text(row, 14),
                        change_amount=self._multi_int(row, 15),
                        final_amount=self._multi_int(row, 16),
                        loan_date=self._multi_text(row, 17),
                        maturity_date=self._multi_text(row, 18),
                        product_no=self._multi_text(row, 19),
                        bond_type=self._multi_text(row, 20),
                        transaction_id=self._multi_text(row, 21),
                        taxable_amount=self._multi_int(row, 22),
                        deposit_usage_fee=self._multi_int(row, 23),
                        order_user_id=self._multi_text(row, 24),
                        requester_name=self._multi_text(row, 25),
                        financial_institution_name=self._multi_text(row, 26),
                    )
                )
            current_start = current_end + timedelta(days=1)
        return items

    def _get_kospi200_index_prices(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        request_count: int,
    ) -> list[MarketIndexPricePoint]:
        result = self._request(
            "TR_ICHART",
            [
                symbol,
                "D",
                "1",
                start_date,
                end_date,
                str(request_count),
            ],
        )
        items: list[MarketIndexPricePoint] = []
        for row in range(result.multi_row_count):
            fields = [self._multi_text(row, index) for index in range(8)]
            point = self._build_kospi200_index_price_point(fields)
            if point is not None:
                items.append(point)
        return self._sort_index_price_points(items)

    def _get_overseas_index_prices(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        request_count: int,
    ) -> list[MarketIndexPricePoint]:
        result = self._request(
            "TR_INCHART",
            [
                symbol,
                "D",
                "1",
                start_date,
                end_date,
                str(request_count),
            ],
        )
        items: list[MarketIndexPricePoint] = []
        for row in range(result.multi_row_count):
            fields = [self._multi_text(row, index) for index in range(7)]
            point = self._build_overseas_index_price_point(fields)
            if point is not None:
                items.append(point)
        return self._sort_index_price_points(items)

    @staticmethod
    def _build_intraday_price_point(fields: list[str], fallback_date: str) -> IntradayPrice | None:
        date = RealIndiClient._text(fields[0] if len(fields) > 0 else "") or fallback_date
        time_text = RealIndiClient._text(fields[1] if len(fields) > 1 else "")
        if len(date) == 10 and date[4] == "-" and date[7] == "-":
            date = date.replace("-", "")
        if not date or not date.isdigit():
            return None
        time_text = RealIndiClient._normalize_intraday_time(time_text)
        return IntradayPrice(
            date=date,
            time=time_text,
            open=RealIndiClient._safe_int_from_fields(fields, 2),
            high=RealIndiClient._safe_int_from_fields(fields, 3),
            low=RealIndiClient._safe_int_from_fields(fields, 4),
            close=RealIndiClient._safe_int_from_fields(fields, 5),
            volume=RealIndiClient._safe_int_from_fields(fields, 9),
        )

    @staticmethod
    def _normalize_intraday_time(time_text: str) -> str:
        cleaned = RealIndiClient._text(time_text)
        if not cleaned:
            return "000000"
        digits = "".join(ch for ch in cleaned if ch.isdigit())
        if not digits:
            return "000000"
        if len(digits) <= 4:
            return digits.zfill(4) + "00"
        if len(digits) == 5:
            return digits.zfill(6)
        return digits[:6]

    @staticmethod
    def _build_kospi200_index_price_point(fields: list[str]) -> MarketIndexPricePoint | None:
        date = RealIndiClient._text(fields[0] if len(fields) > 0 else "")
        if not date:
            return None
        return MarketIndexPricePoint(
            date=date,
            open=RealIndiClient._parse_float_text(fields[2] if len(fields) > 2 else ""),
            high=RealIndiClient._parse_float_text(fields[3] if len(fields) > 3 else ""),
            low=RealIndiClient._parse_float_text(fields[4] if len(fields) > 4 else ""),
            close=RealIndiClient._parse_float_text(fields[5] if len(fields) > 5 else ""),
        )

    @staticmethod
    def _build_overseas_index_price_point(fields: list[str]) -> MarketIndexPricePoint | None:
        date = RealIndiClient._text(fields[0] if len(fields) > 0 else "")
        if not date:
            return None
        return MarketIndexPricePoint(
            date=date,
            open=RealIndiClient._parse_float_text(fields[2] if len(fields) > 2 else ""),
            high=RealIndiClient._parse_float_text(fields[3] if len(fields) > 3 else ""),
            low=RealIndiClient._parse_float_text(fields[4] if len(fields) > 4 else ""),
            close=RealIndiClient._parse_float_text(fields[5] if len(fields) > 5 else ""),
        )

    def _resolve_account_ledger_product_code(
        self,
        account_no: str,
        account_password: str,
        product_code: str | None,
    ) -> str:
        if product_code:
            return product_code.strip()
        metadata = self._get_account_metadata(account_no, account_password)
        resolved_product_code = self._text_or_none(metadata.get("product_code"))
        return resolved_product_code or "01"

    @classmethod
    def _normalize_account_ledger_transaction_type(cls, transaction_type: str) -> str:
        key = transaction_type.strip().upper() or "ALL"
        if key not in cls._ACCOUNT_LEDGER_TRANSACTION_TYPES:
            raise ValueError(
                "unsupported transaction_type; use one of all, sell, buy, deposit, withdraw, "
                "transfer_in, transfer_out, buy_sell, deposit_withdraw, transfer_in_out, fx, "
                "els_dls, dividend, loan_interest, credit_interest, or the raw Indi code"
            )
        return cls._ACCOUNT_LEDGER_TRANSACTION_TYPES[key]

    @classmethod
    def _normalize_account_ledger_market(cls, market: str) -> str:
        key = market.strip().upper() or "ALL"
        if key not in cls._ACCOUNT_LEDGER_MARKETS:
            raise ValueError("unsupported market; use one of all, domestic, overseas, or the raw Indi code 0/1/2")
        return cls._ACCOUNT_LEDGER_MARKETS[key]

    @staticmethod
    def _sort_index_price_points(items: list[MarketIndexPricePoint]) -> list[MarketIndexPricePoint]:
        return sorted(items, key=lambda item: item.date, reverse=True)

    @staticmethod
    def _index_request_count(start_date: str, end_date: str) -> int:
        start_value = datetime.strptime(start_date, "%Y%m%d")
        end_value = datetime.strptime(end_date, "%Y%m%d")
        days = (end_value - start_value).days + 1
        return max(10, min(days + 10, 9999))

    def place_order(self, request: OrderRequest) -> OrderResult:
        account_password = self._account_password("order placement")
        inputs = {
            0: request.account_no,
            1: "01",
            2: account_password,
            3: "",
            4: "",
            5: "0",
            # 신용 주문, 대주, 상환 주문은 의도적으로 지원하지 않는다.
            # 실수로 신용 주문이 열리지 않도록 신용거래구분은 항상 보통 주문("00")으로 고정한다.
            # MCP 입력에도 신용 관련 옵션을 노출하지 않으며, 이 값은 코드에서 바꾸지 않는 것을 기본 정책으로 둔다.
            6: "00",
            7: "2" if request.side == "buy" else "1",
            8: self._build_stock_order_code(request.code),
            9: str(request.quantity),
            10: str(request.price or 0),
            11: "1",
            12: self._order_type_code(request.order_type),
            13: "0",
            14: "0",
            15: "",
            16: "",
            20: "",
            21: "Y",
            36: "",
            37: "0",
        }
        return self._submit_stock_order(
            query_name="SABA101U1",
            inputs=inputs,
            action="place_order",
        )

    def modify_order(self, request: OrderRequest) -> OrderResult:
        account_password = self._account_password("order modification")
        return self._submit_stock_order(
            query_name="SABA102U1",
            inputs={
                0: request.account_no,
                1: "01",
                2: account_password,
                3: "",
                4: "",
                5: "0",
                # 정정 주문은 예외적으로 기존 신용 주문을 정리할 수 있어야 한다.
                # credit_trade_type을 주지 않으면 비신용("00") 정정으로 처리하고,
                # 기존 신용 주문 구분을 유지해야 할 때만 호출자가 명시적으로 값을 넣는다.
                6: request.credit_trade_type or "00",
                7: "3",
                8: self._build_stock_order_code(request.code),
                9: str(request.quantity),
                10: str(request.price or 0),
                11: "1",
                12: self._order_type_code(request.order_type, modification=True),
                13: "0",
                14: "",
                15: "",
                16: request.original_order_id or "",
                35: request.order_method_code or "1",
                36: "",
                37: request.sor_original_order_id or "",
            },
            action="modify_order",
        )

    def cancel_order(self, request: OrderRequest) -> OrderResult:
        account_password = self._account_password("order cancellation")
        return self._submit_stock_order(
            query_name="SABA102U1",
            inputs={
                0: request.account_no,
                1: "01",
                2: account_password,
                3: "",
                4: "",
                5: "0",
                # 취소 주문은 기존 신용 주문 위험을 줄이는 목적까지 막지 않도록 예외를 둔다.
                # 기본값은 여전히 비신용("00")이지만, 이미 접수된 신용 주문을 취소할 때는
                # 원주문의 신용거래구분을 명시적으로 전달할 수 있게 열어둔다.
                6: request.credit_trade_type or "00",
                7: "4",
                8: self._build_stock_order_code(request.code),
                9: str(request.quantity),
                10: "0",
                11: "1",
                12: "Z",
                13: "0",
                14: "",
                15: "",
                16: request.original_order_id or "",
                35: request.order_method_code or "1",
                36: "",
                37: request.sor_original_order_id or "",
            },
            action="cancel_order",
        )

    def place_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        action_code = "2" if request.side == "buy" else "1"
        return self._submit_gold_order(request, action_code=action_code, action_name="place_gold_order")

    def modify_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        return self._submit_gold_order(request, action_code="3", action_name="modify_gold_order")

    def cancel_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        return self._submit_gold_order(request, action_code="4", action_name="cancel_gold_order")

    def _request(
        self,
        query_name: str,
        single_inputs: list[str] | dict[int, str] | None = None,
        timeout_ms: int = 10000,
    ) -> _QueryResult:
        with self._tr_control_lock:
            request_started_at = time.perf_counter()

            def request_elapsed_ms() -> int:
                return int((time.perf_counter() - request_started_at) * 1000)

            ops_log(LogSource.TR_REAL,
                f"request start query={query_name} inputs={len(single_inputs or [])} "
                f"timeout_ms={timeout_ms} elapsed_ms={request_elapsed_ms()}",
            )
            self._reset_request_state()
            if not self._tr_control.dynamicCall("SetQueryName(QString)", query_name):
                ops_log(LogSource.TR_REAL, f"SetQueryName failed query={query_name} elapsed_ms={request_elapsed_ms()}")
                raise RuntimeError(f"SetQueryName failed: {query_name}")

            if isinstance(single_inputs, dict):
                indexed_inputs = sorted(single_inputs.items())
            else:
                indexed_inputs = list(enumerate(single_inputs or []))

            for index, value in indexed_inputs:
                if not self._tr_control.dynamicCall("SetSingleData(int, QString)", index, value):
                    ops_log(LogSource.TR_REAL,
                        f"SetSingleData failed query={query_name} index={index} elapsed_ms={request_elapsed_ms()}",
                    )
                    raise RuntimeError(f"SetSingleData failed: {query_name} index={index}")

            rqid = int(self._tr_control.dynamicCall("RequestData()"))
            ops_log(LogSource.TR_REAL, f"RequestData returned query={query_name} rqid={rqid} elapsed_ms={request_elapsed_ms()}")
            if rqid <= 0:
                ops_log(LogSource.TR_REAL,
                    f"RequestData failed query={query_name} elapsed_ms={request_elapsed_ms()}: "
                    f"{self._format_error('error')}",
                )
                raise RuntimeError(self._format_error(f"RequestData failed: {query_name}"))

            self._pending_rqid = rqid
            event_loop = self._event_loop_cls()
            self._active_event_loop = event_loop
            timer = self._timer_cls()
            timer.setSingleShot(True)
            timer.timeout.connect(lambda: self._on_timeout(event_loop))
            timer.start(timeout_ms)
            event_loop.exec_()
            timer.stop()
            self._active_event_loop = None

            if self._timed_out:
                ops_log(LogSource.TR_REAL,
                    f"request timeout query={query_name} timeout_ms={timeout_ms} elapsed_ms={request_elapsed_ms()}",
                )
                raise TimeoutError(f"{query_name} timed out after {timeout_ms}ms")
            if self._received_rqid != rqid:
                ops_log(LogSource.TR_REAL,
                    f"unexpected rqid query={query_name} expected={rqid} "
                    f"received={self._received_rqid} elapsed_ms={request_elapsed_ms()}",
                )
                raise RuntimeError(f"{query_name} returned unexpected rqid: expected {rqid}, got {self._received_rqid}")

            result = _QueryResult(
                rqid=rqid,
                error_state=int(self._tr_control.dynamicCall("GetErrorState()")),
                error_code=self._text(self._tr_control.dynamicCall("GetErrorCode()")),
                error_message=self._text(self._tr_control.dynamicCall("GetErrorMessage()")),
                single_row_count=int(self._tr_control.dynamicCall("GetSingleRowCount()")),
                multi_row_count=int(self._tr_control.dynamicCall("GetMultiRowCount()")),
            )
            ops_log(LogSource.TR_REAL,
                f"request complete query={query_name} rqid={result.rqid} "
                f"error_state={result.error_state} error_code={result.error_code} "
                f"single_rows={result.single_row_count} multi_rows={result.multi_row_count} "
                f"elapsed_ms={request_elapsed_ms()}",
            )
            if result.error_state != 0:
                ops_log(LogSource.TR_REAL,
                    f"request error query={query_name} elapsed_ms={request_elapsed_ms()}: "
                    f"{self._format_error('error', result)}",
                )
                raise RuntimeError(self._format_error(f"{query_name} failed", result))
            return result

    def _comm_state(self) -> int:
        return int(self._tr_control.dynamicCall("GetCommState()"))

    def _log_control_snapshot(self, label: str) -> None:
        ops_log(LogSource.STARTUP_REAL,
            f"{label} tr={self._format_control_snapshot(getattr(self, '_tr_control', None))} "
            f"rt={self._format_control_snapshot(getattr(self, '_rt_control', None))}",
        )

    def _format_control_snapshot(self, control: Any) -> str:
        if control is None:
            return "missing"

        parts = [f"is_null={self._safe_control_is_null(control)}"]
        for key, signature in (
            ("comm_state", "GetCommState()"),
            ("error_state", "GetErrorState()"),
            ("error_code", "GetErrorCode()"),
            ("error_message", "GetErrorMessage()"),
        ):
            parts.append(f"{key}={self._safe_control_dynamic_call(control, signature)}")
        return "{" + ", ".join(parts) + "}"

    @staticmethod
    def _safe_control_is_null(control: Any) -> str:
        try:
            return str(bool(control.isNull()))
        except Exception as exc:
            return f"unavailable({exc.__class__.__name__}: {exc})"

    def _safe_control_dynamic_call(self, control: Any, signature: str) -> str:
        try:
            value = control.dynamicCall(signature)
        except Exception as exc:
            return f"unavailable({exc.__class__.__name__}: {exc})"
        return self._text(value)

    @classmethod
    def _capture_giexpert_main_generation(cls) -> tuple[tuple[int, int], ...]:
        return cls._capture_process_generation(cls.INDI_MAIN_PROCESS_NAME)

    @staticmethod
    def _capture_process_generation(process_name: str) -> tuple[tuple[int, int], ...]:
        if platform.system().lower() != "windows":
            return ()

        kernel32 = ctypes.windll.kernel32
        th32cs_snapprocess = 0x00000002
        invalid_handle_value = ctypes.c_void_p(-1).value
        max_path = 260

        class ProcessEntry32(ctypes.Structure):
            _fields_ = [
                ("dwSize", ctypes.c_ulong),
                ("cntUsage", ctypes.c_ulong),
                ("th32ProcessID", ctypes.c_ulong),
                ("th32DefaultHeapID", ctypes.c_void_p),
                ("th32ModuleID", ctypes.c_ulong),
                ("cntThreads", ctypes.c_ulong),
                ("th32ParentProcessID", ctypes.c_ulong),
                ("pcPriClassBase", ctypes.c_long),
                ("dwFlags", ctypes.c_ulong),
                ("szExeFile", ctypes.c_wchar * max_path),
            ]

        kernel32.CreateToolhelp32Snapshot.argtypes = [ctypes.c_ulong, ctypes.c_ulong]
        kernel32.CreateToolhelp32Snapshot.restype = ctypes.c_void_p
        kernel32.Process32FirstW.argtypes = [ctypes.c_void_p, ctypes.POINTER(ProcessEntry32)]
        kernel32.Process32FirstW.restype = ctypes.c_int
        kernel32.Process32NextW.argtypes = [ctypes.c_void_p, ctypes.POINTER(ProcessEntry32)]
        kernel32.Process32NextW.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int

        snapshot = kernel32.CreateToolhelp32Snapshot(th32cs_snapprocess, 0)
        if snapshot == invalid_handle_value:
            return ()

        target_name = process_name.lower()
        entries: list[tuple[int, int]] = []
        try:
            entry = ProcessEntry32()
            entry.dwSize = ctypes.sizeof(ProcessEntry32)
            has_entry = kernel32.Process32FirstW(snapshot, ctypes.byref(entry))
            while has_entry:
                if str(entry.szExeFile).lower() == target_name:
                    pid = int(entry.th32ProcessID)
                    entries.append((pid, RealIndiClient._process_creation_filetime(pid)))
                has_entry = kernel32.Process32NextW(snapshot, ctypes.byref(entry))
        finally:
            kernel32.CloseHandle(snapshot)

        return tuple(sorted(entries))

    @staticmethod
    def _process_creation_filetime(pid: int) -> int:
        kernel32 = ctypes.windll.kernel32
        process_query_limited_information = 0x1000
        process_query_information = 0x0400

        class FileTime(ctypes.Structure):
            _fields_ = [
                ("dwLowDateTime", ctypes.c_ulong),
                ("dwHighDateTime", ctypes.c_ulong),
            ]

        kernel32.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        kernel32.OpenProcess.restype = ctypes.c_void_p
        kernel32.GetProcessTimes.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
            ctypes.POINTER(FileTime),
        ]
        kernel32.GetProcessTimes.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int

        handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
        if not handle:
            handle = kernel32.OpenProcess(process_query_information, False, pid)
        if not handle:
            return 0
        try:
            created = FileTime()
            exited = FileTime()
            kernel = FileTime()
            user = FileTime()
            if not kernel32.GetProcessTimes(
                handle,
                ctypes.byref(created),
                ctypes.byref(exited),
                ctypes.byref(kernel),
                ctypes.byref(user),
            ):
                return 0
            return (int(created.dwHighDateTime) << 32) + int(created.dwLowDateTime)
        finally:
            kernel32.CloseHandle(handle)

    def _check_giexpert_main_generation(self) -> dict[str, object]:
        try:
            current = self._capture_giexpert_main_generation()
        except Exception as exc:
            message = f"{self.INDI_MAIN_PROCESS_NAME} process monitor failed: {exc}"
            return {
                "running": False,
                "restarted": self._giexpert_main_restarted,
                "message": message,
            }

        baseline = self._giexpert_main_generation
        self._giexpert_main_current_generation = current
        if current != baseline:
            message = (
                f"{self.INDI_MAIN_PROCESS_NAME} generation changed "
                f"(server_start={self._format_process_generation(baseline)}, "
                f"current={self._format_process_generation(current)})"
            )
            if not self._giexpert_main_restarted:
                ops_log(LogSource.MANAGE, message)
            self._giexpert_main_restarted = True
            self._giexpert_main_restart_message = message
        else:
            message = (
                f"{self.INDI_MAIN_PROCESS_NAME} generation unchanged "
                f"({self._format_process_generation(current)})"
            )
        if self._giexpert_main_restarted:
            message = self._giexpert_main_restart_message
        return {
            "running": bool(current),
            "restarted": self._giexpert_main_restarted,
            "message": message,
        }

    @staticmethod
    def _format_process_generation(generation: tuple[tuple[int, int], ...]) -> str:
        if not generation:
            return "not_running"
        return ";".join(
            f"pid={pid},created_at={RealIndiClient._filetime_to_text(created_at)}"
            for pid, created_at in generation
        )

    @staticmethod
    def _filetime_to_text(filetime: int) -> str:
        if filetime <= 0:
            return "unknown"
        created_at = datetime(1601, 1, 1) + timedelta(microseconds=filetime // 10)
        return created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    @staticmethod
    def _current_session_id() -> int | None:
        session_id = ctypes.c_uint()
        if ctypes.windll.kernel32.ProcessIdToSessionId(os.getpid(), ctypes.byref(session_id)):
            return int(session_id.value)
        return None

    @staticmethod
    def _is_interactive_session() -> bool:
        session_id = RealIndiClient._current_session_id()
        if session_id is not None:
            return session_id != 0
        session_name = os.getenv("SESSIONNAME", "").strip().lower()
        return session_name not in {"", "services"}

    def _account_password(self, purpose: str) -> str:
        account_password = os.getenv(self.ACCOUNT_PASSWORD_ENV, "").strip()
        if not account_password:
            raise RuntimeError(f"{self.ACCOUNT_PASSWORD_ENV} is required for {purpose}")
        return account_password

    def _prime_stock_master_cache(self) -> None:
        try:
            self._load_stock_master_cache()
        except Exception as exc:
            ops_log(LogSource.STARTUP_REAL,
                f"stock master cache prime skipped: {exc.__class__.__name__}: {exc}",
            )
            # 서버 시작 시점의 캐시 로드는 best-effort 로만 수행한다.
            # 권한/로그인 상태가 아직 준비되지 않은 경우에도 프로세스 자체는 살아 있어야 한다.
            self._stock_cache = []
            self._kospi200_codes = set()

    def _stock_master_cache_path(self) -> Path:
        state_dir = os.getenv("HOMESTOCK_RUNTIME_STATE_DIR", "").strip()
        return Path(state_dir or ".runtime") / self._STOCK_MASTER_CACHE_FILE_NAME

    def _restore_stock_master_cache(self) -> None:
        path = self._stock_master_cache_path()
        if not path.exists():
            ops_log(LogSource.STARTUP_REAL, f"stock master cache restore skipped: file not found path={path}")
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            raw_stocks = payload.get("stocks", [])
            raw_kospi200_codes = payload.get("kospi200_codes", [])
            stocks: list[Stock] = []
            for item in raw_stocks:
                if not isinstance(item, dict):
                    continue
                code = self._text(item.get("code"))
                if not code:
                    continue
                stocks.append(
                    Stock(
                        code=code,
                        name=self._text(item.get("name")),
                        market=self._text(item.get("market")),
                    )
                )
            self._stock_cache = stocks
            self._kospi200_codes = {
                self._text(code)
                for code in raw_kospi200_codes
                if self._text(code)
            }
            ops_log(LogSource.STARTUP_REAL,
                f"stock master cache restored path={path} stocks={len(self._stock_cache)} "
                f"kospi200_codes={len(self._kospi200_codes)}",
            )
        except Exception as exc:
            ops_log(LogSource.STARTUP_REAL,
                f"stock master cache restore failed path={path}: {exc.__class__.__name__}: {exc}",
            )
            self._stock_cache = []
            self._kospi200_codes = set()

    def _save_stock_master_cache(self) -> None:
        path = self._stock_master_cache_path()
        payload = {
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "stocks": [stock.to_dict() for stock in self._stock_cache],
            "kospi200_codes": sorted(self._kospi200_codes),
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = path.with_name(f"{path.name}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            temp_path.replace(path)
            ops_log(LogSource.STARTUP_REAL,
                f"stock master cache saved path={path} stocks={len(self._stock_cache)} "
                f"kospi200_codes={len(self._kospi200_codes)}",
            )
        except Exception as exc:
            ops_log(LogSource.STARTUP_REAL,
                f"stock master cache save failed path={path}: {exc.__class__.__name__}: {exc}",
            )

    def _load_stock_master_cache(self) -> None:
        result = self._request("stock_mst")
        stocks: list[Stock] = []
        kospi200_codes: set[str] = set()
        for row in range(result.multi_row_count):
            code = self._multi_text(row, 1)
            if not code:
                continue
            stocks.append(
                Stock(
                    code=code,
                    name=self._multi_text(row, 3),
                    market=self._MARKET_NAMES.get(self._multi_text(row, 2), self._multi_text(row, 2)),
                )
            )
            kospi200_sector = self._multi_text(row, 4)
            if kospi200_sector and kospi200_sector != "0":
                kospi200_codes.add(code)
        self._stock_cache = stocks
        self._kospi200_codes = kospi200_codes
        self._save_stock_master_cache()


    @staticmethod
    def _limit_items(items: list[Any], limit: int | None) -> list[Any]:
        if limit is None:
            return items
        return items[: max(limit, 0)]

    def _filter_kospi200_items(self, items: list[Any], kospi200_only: bool) -> list[Any]:
        if not kospi200_only:
            return items
        if not self._stock_cache:
            self._load_stock_master_cache()
        return [item for item in items if getattr(item, "code", "") in self._kospi200_codes]

    def _reset_request_state(self) -> None:
        self._pending_rqid = None
        self._received_rqid = None
        self._timed_out = False
        self._sys_msg_ids.clear()

    def _reset_rt_wait_state(self) -> None:
        self._pending_rt_type = None
        self._pending_rt_code = None
        self._received_rt_type = None
        self._received_rt_code = None
        self._rt_timed_out = False

    def _reset_gold_rt_wait_state(self) -> None:
        self._gold_pending_rt_type = None
        self._gold_pending_rt_code = None
        self._gold_received_rt_type = None
        self._gold_received_rt_code = None
        self._gold_rt_timed_out = False

    def _on_receive_data(self, rqid: int) -> None:
        self._received_rqid = rqid
        if (
            self._pending_rqid == rqid
            and self._active_event_loop is not None
            and self._active_event_loop.isRunning()
        ):
            self._active_event_loop.quit()

    def _on_receive_sys_msg(self, msg_id: int) -> None:
        self._sys_msg_ids.append(msg_id)

    def _on_timeout(self, event_loop: object) -> None:
        self._timed_out = True
        event_loop.quit()

    def _on_rt_timeout(self, event_loop: object) -> None:
        self._rt_timed_out = True
        event_loop.quit()

    def _on_gold_rt_timeout(self, event_loop: object) -> None:
        self._gold_rt_timed_out = True
        event_loop.quit()

    def _on_receive_rt_data(self, *args: object) -> None:
        rt_type = self._text(args[0]) if args else ""
        if not rt_type:
            rt_type = self._pending_rt_type or ""
        field_count = self._RT_FIELD_COUNTS.get(rt_type, 0)
        fields = [self._rt_single_text(index) for index in range(field_count)]
        raw_code = fields[1] if len(fields) > 1 else ""
        code = self._normalize_rt_event_code(rt_type, raw_code or self._pending_rt_code or "")
        if rt_type and code:
            self._rt_snapshots[(rt_type, code)] = fields
            self._received_rt_type = rt_type
            self._received_rt_code = code
        event = self._build_rt_event(rt_type, fields)
        if event is not None:
            self._notify_rt_listeners(event)
        if self._active_rt_event_loop is not None and self._active_rt_event_loop.isRunning():
            if (
                self._pending_rt_type is None
                or (self._received_rt_type == self._pending_rt_type and self._received_rt_code == self._pending_rt_code)
            ):
                self._active_rt_event_loop.quit()

    def _on_receive_gold_rt_data(self, *args: object) -> None:
        rt_type = self._text(args[0]) if args else ""
        if not rt_type:
            rt_type = self._gold_pending_rt_type or ""
        field_count = self._GOLD_RT_FIELD_COUNTS.get(rt_type, 0)
        fields = [self._gold_rt_single_text(index) for index in range(field_count)]
        raw_code = fields[1] if len(fields) > 1 else ""
        code_source = raw_code or self._gold_pending_rt_code or ""
        try:
            code = self.normalize_gold_code(code_source) if code_source else ""
        except ValueError:
            code = ""
        if rt_type and code:
            self._gold_rt_snapshots[(rt_type, code)] = fields
            self._gold_received_rt_type = rt_type
            self._gold_received_rt_code = code
        event = self._build_gold_rt_event(rt_type, fields)
        if event is not None:
            self._notify_gold_rt_listeners(event)
        if self._gold_active_rt_event_loop is not None and self._gold_active_rt_event_loop.isRunning():
            if (
                self._gold_pending_rt_type is None
                or (
                    self._gold_received_rt_type == self._gold_pending_rt_type
                    and self._gold_received_rt_code == self._gold_pending_rt_code
                )
            ):
                self._gold_active_rt_event_loop.quit()

    def _single_text(self, index: int) -> str:
        return self._text(self._tr_control.dynamicCall("GetSingleData(int)", index))

    def _rt_single_text(self, index: int) -> str:
        return self._text(self._rt_control.dynamicCall("GetSingleData(int)", index))

    def _gold_rt_single_text(self, index: int) -> str:
        return self._text(self._ensure_gold_rt_control().dynamicCall("GetSingleData(int)", index))

    def _single_int(self, index: int) -> int:
        text = self._single_text(index).replace(",", "").strip()
        if not text:
            return 0
        try:
            return int(float(text))
        except ValueError:
            return 0

    def _single_float(self, index: int) -> float:
        text = self._single_text(index).replace(",", "").strip()
        if not text:
            return 0.0
        try:
            return float(text)
        except ValueError:
            return 0.0

    def _multi_text(self, row: int, index: int) -> str:
        return self._text(self._tr_control.dynamicCall("GetMultiData(int, int)", row, index))

    def _multi_int(self, row: int, index: int) -> int:
        text = self._multi_text(row, index).replace(",", "").strip()
        if not text:
            return 0
        try:
            return int(float(text))
        except ValueError:
            return 0

    def _multi_float(self, row: int, index: int) -> float:
        return self._parse_float_text(self._multi_text(row, index))

    @staticmethod
    def _text(value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _parse_float_text(value: object) -> float:
        text = RealIndiClient._text(value).replace(",", "")
        if not text:
            return 0.0
        try:
            return float(text)
        except ValueError:
            return 0.0

    def _format_error(self, prefix: str, result: _QueryResult | None = None) -> str:
        error_state = int(self._tr_control.dynamicCall("GetErrorState()")) if result is None else result.error_state
        error_code = self._text(self._tr_control.dynamicCall("GetErrorCode()")) if result is None else result.error_code
        error_message = self._text(self._tr_control.dynamicCall("GetErrorMessage()")) if result is None else result.error_message
        sys_msgs = ",".join(str(msg_id) for msg_id in self._sys_msg_ids)
        detail = f"error_state={error_state}, error_code={error_code}, error_message={error_message}"
        if sys_msgs:
            detail += f", sys_msgs={sys_msgs}"
        return f"{prefix} ({detail})"

    def _submit_stock_order(
        self,
        query_name: str,
        inputs: list[str] | dict[int, str],
        action: str,
    ) -> OrderResult:
        self._request(query_name, inputs)
        # 주문 응답은 주문번호가 여러 거래소/SOR 경로로 나뉘어 돌아올 수 있다.
        # ORC 주문번호는 raw로만 보존하고 대표 주문번호 판단에는 쓰지 않는다.
        order_ids = [self._single_text(index) for index in range(4)]
        accepted_order_id = next((order_id for order_id in order_ids[:3] if order_id and order_id != "0"), None)
        # 메세지1~3은 브로커 응답을 사람이 읽기 쉽게 이어 붙인 값이다.
        messages = [self._single_text(index) for index in range(5, 8)]
        message = " | ".join(part for part in messages if part) or f"{action} completed"
        return OrderResult(
            accepted=accepted_order_id is not None,
            live_order=True,
            order_id=accepted_order_id,
            message=message,
            raw={
                "query_name": query_name,
                "order_ids": order_ids,
                "sor_order_id": order_ids[0],
                "krx_order_id": order_ids[1],
                "nxt_order_id": order_ids[2],
                "orc_order_id": order_ids[3],
                "submitted_order_method_code": self._submitted_stock_order_method_code(query_name, inputs),
                "message_type": self._single_text(4),
                "messages": messages,
            },
        )

    @staticmethod
    def _submitted_stock_order_method_code(query_name: str, inputs: list[str] | dict[int, str]) -> str:
        if not isinstance(inputs, dict):
            return ""
        if query_name == "SABA101U1":
            return str(inputs.get(37, ""))
        if query_name == "SABA102U1":
            return str(inputs.get(35, ""))
        return ""

    def _submit_gold_order(
        self,
        request: GoldOrderRequest,
        *,
        action_code: str,
        action_name: str,
    ) -> OrderResult:
        normalized_code = self.normalize_gold_code(request.code)
        if request.side not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        if request.quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        if action_name in {"place_gold_order", "modify_gold_order"} and (request.price is None or request.price <= 0):
            raise ValueError("gold place/modify orders require a positive limit price")
        if action_code in {"3", "4"} and not request.original_order_id:
            raise ValueError("original_order_id is required")
        account_password = self._account_password(action_name)
        inputs = {
            0: request.account_no,
            1: self._GOLD_PRODUCT_CODE,
            2: account_password,
            3: "",
            4: "",
            5: action_code,
            6: normalized_code,
            7: str(request.quantity),
            8: "0" if action_code == "4" else str(request.price or 0),
            9: "1",
            10: "2",
            11: "0",
            12: request.original_order_id or "",
            13: "N",
            14: "Y",
            15: "",
            16: "",
            17: "",
            18: "",
            19: "",
            20: "",
        }
        self._request("SABA871U1", inputs)
        raw_order_id = self._single_text(0)
        order_id = raw_order_id if raw_order_id and raw_order_id != "0" else None
        messages = [self._single_text(index) for index in range(3, 6)]
        message = " | ".join(part for part in messages if part) or f"{action_name} completed"
        return OrderResult(
            accepted=order_id is not None,
            live_order=True,
            order_id=order_id,
            message=message,
            raw={
                "query_name": "SABA871U1",
                "order_id": order_id,
                "channel_order_id": self._single_text(1),
                "message_type": self._single_text(2),
                "messages": messages,
                "action_code": action_code,
                "product_code": self._GOLD_PRODUCT_CODE,
                "quote_type_code": "2",
            },
        )

    @staticmethod
    def _chunk_codes(raw_codes: str) -> list[str]:
        cleaned = raw_codes.strip()
        if not cleaned:
            return []
        return [cleaned[index : index + 6] for index in range(0, len(cleaned), 6) if cleaned[index : index + 6]]

    def _register_realtime(
        self,
        rt_type: str,
        code: str,
        wait_for_first_tick: bool = True,
        timeout_ms: int = 3000,
    ) -> None:
        normalized_code = self._normalize_stock_code(code)
        with self._rt_control_lock:
            self._reset_rt_wait_state()
            self._invoke_realtime_method("RequestRTReg", rt_type, normalized_code)
            if wait_for_first_tick:
                self._wait_for_rt_snapshot(rt_type, normalized_code, timeout_ms=timeout_ms)

    def _unregister_realtime(self, rt_type: str, code: str) -> None:
        normalized_code = self._normalize_stock_code(code)
        with self._rt_control_lock:
            self._reset_rt_wait_state()
            self._invoke_realtime_method("UnRequestRTReg", rt_type, normalized_code)
            self._rt_snapshots.pop((rt_type, normalized_code), None)

    def _ensure_rt_snapshot(self, rt_type: str, code: str, timeout_ms: int = 3000) -> list[str] | None:
        normalized_code = self._normalize_stock_code(code)
        cached = self._rt_snapshots.get((rt_type, normalized_code))
        if cached:
            return cached
        self._register_realtime(rt_type, normalized_code, wait_for_first_tick=True, timeout_ms=timeout_ms)
        return self._rt_snapshots.get((rt_type, normalized_code))

    def _get_rt_snapshot_once(self, rt_type: str, code: str, timeout_ms: int = 3000) -> list[str] | None:
        normalized_code = self._normalize_stock_code(code)
        try:
            self._register_realtime(rt_type, normalized_code, wait_for_first_tick=True, timeout_ms=timeout_ms)
            return self._rt_snapshots.get((rt_type, normalized_code))
        finally:
            # one-shot snapshot은 성공/timeout 여부와 무관하게 등록을 정리한다.
            # 일부 RT 타입(UH)은 해제 호출이 실패해도 snapshot 자체는 확보되는 경우가 있어,
            # 해제 실패가 조회 결과 반환까지 깨지지 않도록 보수적으로 무시한다.
            try:
                self._unregister_realtime(rt_type, normalized_code)
            except RuntimeError:
                pass

    def _register_gold_realtime(
        self,
        rt_type: str,
        code: str,
        wait_for_first_tick: bool = True,
        timeout_ms: int = 3000,
    ) -> None:
        normalized_code = self.normalize_gold_code(code)
        with self._gold_rt_control_lock:
            self._ensure_gold_rt_control()
            self._reset_gold_rt_wait_state()
            self._invoke_gold_realtime_method("RequestRTReg", rt_type, normalized_code)
            if wait_for_first_tick:
                self._wait_for_gold_rt_snapshot(rt_type, normalized_code, timeout_ms=timeout_ms)

    def _unregister_gold_realtime(self, rt_type: str, code: str) -> None:
        normalized_code = self.normalize_gold_code(code)
        with self._gold_rt_control_lock:
            self._ensure_gold_rt_control()
            self._reset_gold_rt_wait_state()
            self._invoke_gold_realtime_method("UnRequestRTReg", rt_type, normalized_code)
            self._gold_rt_snapshots.pop((rt_type, normalized_code), None)

    def _get_gold_rt_snapshot_once(self, rt_type: str, code: str, timeout_ms: int = 3000) -> list[str] | None:
        normalized_code = self.normalize_gold_code(code)
        key = (rt_type, normalized_code)
        if self._gold_rt_subscription_counts.get(key, 0) > 0:
            cached = self._gold_rt_snapshots.get(key)
            if cached:
                return cached
            with self._gold_rt_control_lock:
                self._ensure_gold_rt_control()
                self._reset_gold_rt_wait_state()
                self._wait_for_gold_rt_snapshot(rt_type, normalized_code, timeout_ms=timeout_ms)
            return self._gold_rt_snapshots.get(key)
        try:
            self._register_gold_realtime(rt_type, normalized_code, wait_for_first_tick=True, timeout_ms=timeout_ms)
            return self._gold_rt_snapshots.get((rt_type, normalized_code))
        finally:
            try:
                self._unregister_gold_realtime(rt_type, normalized_code)
            except RuntimeError:
                pass

    def _ensure_gold_rt_control(self) -> Any:
        control = getattr(self, "_gold_rt_control", None)
        if control is not None:
            return control
        ops_log(LogSource.RT_REAL, f"creating gold RT OCX prog_id={self.PROG_ID}")
        control = self._qax_widget_cls(self.PROG_ID)
        if control.isNull():
            ops_log(LogSource.RT_REAL, f"failed to create gold RT OCX prog_id={self.PROG_ID}")
            raise RuntimeError(f"failed to create gold RT OCX: {self.PROG_ID}")
        control.ReceiveRTData.connect(self._on_receive_gold_rt_data)
        control.ReceiveSysMsg.connect(self._on_receive_sys_msg)
        self._gold_rt_control = control
        ops_log(LogSource.RT_REAL, "gold RT OCX created and handlers connected")
        return control

    def _invoke_gold_realtime_method(self, method_name: str, rt_type: str, code: str) -> None:
        if self._try_invoke_gold_realtime_method(method_name, rt_type, code):
            return
        raise RuntimeError(f"{method_name} failed: {rt_type} {code}")

    def _try_invoke_gold_realtime_method(self, method_name: str, rt_type: str, code: str) -> str | None:
        self._last_rt_error_details = None
        control = self._ensure_gold_rt_control()
        attempts = [(f"{method_name}(QVariant, QVariant)", [rt_type, code])]
        for signature, args in attempts:
            try:
                result = control.dynamicCall(signature, *args)
            except Exception as exc:
                self._capture_gold_rt_error_details(rt_type, code, method_name, signature, exception=str(exc))
                if self._accept_gi005_request_rt_reg_warning(rt_type, code, signature, f"exception={exc}"):
                    return signature
                continue
            if result not in (None, False, 0, ""):
                self._last_rt_error_details = None
                return signature
            self._capture_gold_rt_error_details(rt_type, code, method_name, signature)
            if self._accept_gi005_request_rt_reg_warning(rt_type, code, signature, f"result={result!r}"):
                return signature
        return None

    def _capture_gold_rt_error_details(
        self,
        rt_type: str,
        code: str,
        method_name: str,
        attempted_signature: str,
        exception: str | None = None,
    ) -> None:
        control = getattr(self, "_gold_rt_control", None)
        try:
            error_state = int(control.dynamicCall("GetErrorState()")) if control is not None else 0
        except Exception:
            error_state = 0
        try:
            error_code = self._text(control.dynamicCall("GetErrorCode()")) if control is not None else ""
        except Exception:
            error_code = ""
        try:
            error_message = self._text(control.dynamicCall("GetErrorMessage()")) if control is not None else ""
        except Exception:
            error_message = ""
        details: dict[str, Any] = {
            "rt_type": rt_type,
            "code": code,
            "method_name": method_name,
            "attempted_signature": attempted_signature,
            "error_state": error_state,
            "error_code": error_code,
            "error_message": error_message,
            "control": "gold_rt",
            "sys_msgs": [str(msg_id) for msg_id in getattr(self, "_sys_msg_ids", [])],
        }
        if exception:
            details["exception"] = exception
        self._last_rt_error_details = details

    def _invoke_realtime_method(self, method_name: str, rt_type: str, code: str) -> None:
        if self._try_invoke_realtime_method(method_name, rt_type, code):
            return
        raise RuntimeError(f"{method_name} failed: {rt_type} {code}")

    def _try_invoke_realtime_method(
        self,
        method_name: str,
        rt_type: str,
        code: str,
        *,
        allow_partial_signatures: bool = False,
    ) -> str | None:
        self._last_rt_error_details = None
        attempts = [(f"{method_name}(QVariant, QVariant)", [rt_type, code])]
        if allow_partial_signatures:
            attempts.extend(
                (
                    (f"{method_name}(QString)", [rt_type]),
                    (f"{method_name}()", []),
                )
            )
        for signature, args in attempts:
            try:
                result = self._rt_control.dynamicCall(signature, *args)
            except Exception as exc:
                self._capture_rt_error_details(rt_type, code, method_name, signature, exception=str(exc))
                if self._accept_gi005_request_rt_reg_warning(rt_type, code, signature, f"exception={exc}"):
                    return signature
                continue
            if result not in (None, False, 0, ""):
                self._last_rt_error_details = None
                return signature
            self._capture_rt_error_details(rt_type, code, method_name, signature)
            if self._accept_gi005_request_rt_reg_warning(rt_type, code, signature, f"result={result!r}"):
                return signature
        return None

    def _capture_rt_error_details(
        self,
        rt_type: str,
        code: str,
        method_name: str,
        attempted_signature: str,
        exception: str | None = None,
    ) -> None:
        try:
            error_state = int(self._rt_control.dynamicCall("GetErrorState()"))
        except Exception:
            error_state = 0
        try:
            error_code = self._text(self._rt_control.dynamicCall("GetErrorCode()"))
        except Exception:
            error_code = ""
        try:
            error_message = self._text(self._rt_control.dynamicCall("GetErrorMessage()"))
        except Exception:
            error_message = ""
        details: dict[str, Any] = {
            "rt_type": rt_type,
            "code": code,
            "method_name": method_name,
            "attempted_signature": attempted_signature,
            "error_state": error_state,
            "error_code": error_code,
            "error_message": error_message,
            "sys_msgs": [str(msg_id) for msg_id in getattr(self, "_sys_msg_ids", [])],
        }
        if exception:
            details["exception"] = exception
        self._last_rt_error_details = details

    def _accept_gi005_request_rt_reg_warning(
        self,
        rt_type: str,
        code: str,
        attempted_signature: str,
        failure_summary: str,
    ) -> bool:
        details = self._last_rt_error_details or {}
        error_code = str(details.get("error_code") or "").strip().upper()
        method_name = str(details.get("method_name") or "").strip()
        if method_name != "RequestRTReg" or error_code != "GI005":
            return False

        message = (
            f"{rt_type} RequestRTReg GI005 warning accepted code={code} "
            f"signature={attempted_signature} {failure_summary} details={details}"
        )
        ops_log(LogSource.RT_REAL, message)
        return True

    def _build_rt_event(self, rt_type: str, fields: list[str]) -> dict[str, Any] | None:
        if rt_type in self._STOCK_PRICE_RT_TYPES:
            uc_offset = 1 if rt_type == "UC" and len(fields) > 26 else 0
            info_cls_raw = fields[3] if uc_offset else ""
            return {
                "rt_type": rt_type,
                "code": self._normalize_rt_event_code(rt_type, fields[1] if len(fields) > 1 else ""),
                "time": fields[2] if len(fields) > 2 else "",
                "current_price": self._parse_float_text(fields[3 + uc_offset] if len(fields) > 3 + uc_offset else ""),
                "change_type": fields[4 + uc_offset] if len(fields) > 4 + uc_offset else "",
                "change_amount": self._parse_float_text(fields[5 + uc_offset] if len(fields) > 5 + uc_offset else ""),
                "change_percent": self._parse_float_text(fields[6 + uc_offset] if len(fields) > 6 + uc_offset else ""),
                "cumulative_volume": self._parse_float_text(fields[7 + uc_offset] if len(fields) > 7 + uc_offset else ""),
                "market_phase": fields[17 + uc_offset] if len(fields) > 17 + uc_offset else "",
                "trade_type": fields[16 + uc_offset] if len(fields) > 16 + uc_offset else "",
                "raw_fields": list(fields),
                "uc_field_count": len(fields) if rt_type == "UC" else 0,
                "uc_info_cls_raw": info_cls_raw,
                "uc_info_cls_present": bool(info_cls_raw),
                "title": None,
            }
        if rt_type == "SH":
            code = self._normalize_rt_event_code("SH", fields[1] if len(fields) > 1 else "")
            order_book = self._build_cash_order_book(code, fields)
            return {
                "rt_type": "SH",
                "code": code,
                "time": fields[2] if len(fields) > 2 else "",
                "received_at": order_book.received_at,
                "market_phase": order_book.market_phase,
                "levels": [level.to_dict() for level in order_book.levels],
                "source": order_book.source,
                "title": None,
            }
        if rt_type in {"N0", "N2"}:
            news_type = fields[0] if len(fields) > 0 else ""
            return {
                "rt_type": rt_type,
                "news_type": news_type,
                "news_type_label": self._news_type_label(news_type),
                "date": fields[1] if len(fields) > 1 else "",
                "article_id": fields[2] if len(fields) > 2 else "",
                "deleted_flag": fields[3] if len(fields) > 3 else None,
                "time": fields[4] if len(fields) > 4 else "",
                "code": self._normalize_rt_event_code(rt_type, fields[5] if len(fields) > 5 else ""),
                "title": fields[13] if len(fields) > 13 and fields[13] else None,
            }
        return None

    def _build_gold_rt_event(self, rt_type: str, fields: list[str]) -> dict[str, Any] | None:
        if rt_type == "XC":
            raw_code = fields[1] if len(fields) > 1 else ""
            if not raw_code:
                return None
            try:
                code = self.normalize_gold_code(raw_code)
            except ValueError:
                return None
            return {
                "rt_type": "XC",
                "code": code,
                "standard_code": fields[0] if len(fields) > 0 else "",
                "time": fields[2] if len(fields) > 2 else "",
                "current_price": self._parse_float_text(fields[3] if len(fields) > 3 else ""),
                "change_type": fields[4] if len(fields) > 4 else "",
                "change_amount": self._parse_float_text(fields[5] if len(fields) > 5 else ""),
                "change_percent": self._parse_float_text(fields[6] if len(fields) > 6 else ""),
                "cumulative_volume": self._parse_float_text(fields[7] if len(fields) > 7 else ""),
                "turnover": self._parse_float_text(fields[8] if len(fields) > 8 else ""),
                "fill_size": self._parse_float_text(fields[9] if len(fields) > 9 else ""),
                "best_ask": self._parse_float_text(fields[18] if len(fields) > 18 else ""),
                "best_bid": self._parse_float_text(fields[19] if len(fields) > 19 else ""),
                "title": None,
            }
        if rt_type == "XH":
            raw_code = fields[1] if len(fields) > 1 else ""
            if not raw_code:
                return None
            try:
                code = self.normalize_gold_code(raw_code)
            except ValueError:
                return None
            order_book = self._build_gold_order_book(code, fields)
            return {
                "rt_type": "XH",
                "code": code,
                "time": fields[2] if len(fields) > 2 else "",
                "received_at": order_book.received_at,
                "market_phase": order_book.market_phase,
                "levels": [level.to_dict() for level in order_book.levels],
                "source": order_book.source,
                "title": None,
            }
        return None

    def _build_gold_quote_snapshot(self, fields: list[str], fallback_code: str) -> GoldQuoteSnapshot:
        code = self.normalize_gold_code(fields[1] if len(fields) > 1 and fields[1] else fallback_code)
        fallback = self._GOLD_PRODUCTS[code]
        return GoldQuoteSnapshot(
            code=code,
            standard_code=fields[0] if len(fields) > 0 and fields[0] else fallback.standard_code,
            trade_time=fields[2] if len(fields) > 2 else "",
            current_price=self._parse_float_text(fields[3] if len(fields) > 3 else ""),
            change_type=fields[4] if len(fields) > 4 else "",
            change_amount=self._parse_float_text(fields[5] if len(fields) > 5 else ""),
            change_percent=self._parse_float_text(fields[6] if len(fields) > 6 else ""),
            cumulative_volume=self._parse_float_text(fields[7] if len(fields) > 7 else ""),
            turnover=self._parse_float_text(fields[8] if len(fields) > 8 else ""),
            fill_size=self._parse_float_text(fields[9] if len(fields) > 9 else ""),
            open=self._parse_float_text(fields[10] if len(fields) > 10 else ""),
            high=self._parse_float_text(fields[11] if len(fields) > 11 else ""),
            low=self._parse_float_text(fields[12] if len(fields) > 12 else ""),
            open_time=fields[13] if len(fields) > 13 else "",
            high_time=fields[14] if len(fields) > 14 else "",
            low_time=fields[15] if len(fields) > 15 else "",
            block_volume=self._parse_float_text(fields[16] if len(fields) > 16 else ""),
            trade_side=fields[17] if len(fields) > 17 else "",
            best_ask=self._parse_float_text(fields[18] if len(fields) > 18 else ""),
            best_bid=self._parse_float_text(fields[19] if len(fields) > 19 else ""),
        )

    @classmethod
    def _build_gold_order_book(cls, code: str, fields: list[str]) -> OrderBook:
        levels = [
            cls._build_level(
                ask_price=cls._safe_int_from_fields(fields, 4 + offset * 4),
                bid_price=cls._safe_int_from_fields(fields, 5 + offset * 4),
                ask_size=cls._safe_int_from_fields(fields, 6 + offset * 4),
                bid_size=cls._safe_int_from_fields(fields, 7 + offset * 4),
            )
            for offset in range(5)
        ]
        return OrderBook(
            code=code,
            received_at=fields[2].strip() if len(fields) > 2 else "",
            market_phase=cls._market_phase_from_cash_quote(
                fields[3] if len(fields) > 3 else "",
                fields[46] if len(fields) > 46 else "",
            ),
            levels=levels,
            source="XH",
        )

    def _notify_rt_listeners(self, event: dict[str, Any]) -> None:
        for listener in list(self._rt_listeners):
            try:
                listener(dict(event))
            except Exception as exc:
                ops_log(LogSource.RT_REAL, f"RT listener failed for {event.get('rt_type')}: {exc}")

    def _notify_gold_rt_listeners(self, event: dict[str, Any]) -> None:
        for listener in list(self._gold_rt_listeners):
            try:
                listener(dict(event))
            except Exception as exc:
                ops_log(LogSource.RT_REAL, f"Gold RT listener failed for {event.get('rt_type')}: {exc}")

    def _wait_for_rt_snapshot(self, rt_type: str, code: str, timeout_ms: int) -> None:
        self._pending_rt_type = rt_type
        self._pending_rt_code = code
        self._received_rt_type = None
        self._received_rt_code = None
        self._rt_timed_out = False
        event_loop = self._event_loop_cls()
        self._active_rt_event_loop = event_loop
        timer = self._timer_cls()
        timer.setSingleShot(True)
        timer.timeout.connect(lambda: self._on_rt_timeout(event_loop))
        timer.start(timeout_ms)
        event_loop.exec_()
        timer.stop()
        self._active_rt_event_loop = None
        if self._rt_timed_out:
            raise TimeoutError(f"{rt_type} realtime snapshot timed out after {timeout_ms}ms")

    def _wait_for_gold_rt_snapshot(self, rt_type: str, code: str, timeout_ms: int) -> None:
        self._gold_pending_rt_type = rt_type
        self._gold_pending_rt_code = code
        self._gold_received_rt_type = None
        self._gold_received_rt_code = None
        self._gold_rt_timed_out = False
        event_loop = self._event_loop_cls()
        self._gold_active_rt_event_loop = event_loop
        timer = self._timer_cls()
        timer.setSingleShot(True)
        timer.timeout.connect(lambda: self._on_gold_rt_timeout(event_loop))
        timer.start(timeout_ms)
        event_loop.exec_()
        timer.stop()
        self._gold_active_rt_event_loop = None
        if self._gold_rt_timed_out:
            raise TimeoutError(f"{rt_type} gold realtime snapshot timed out after {timeout_ms}ms")

    @classmethod
    def _build_cash_order_book(cls, code: str, fields: list[str]) -> OrderBook:
        levels = [
            cls._build_level(
                ask_price=cls._safe_int_from_fields(fields, 4 + offset * 4),
                bid_price=cls._safe_int_from_fields(fields, 5 + offset * 4),
                ask_size=cls._safe_int_from_fields(fields, 6 + offset * 4),
                bid_size=cls._safe_int_from_fields(fields, 7 + offset * 4),
            )
            for offset in range(5)
        ]
        market_phase = cls._market_phase_from_cash_quote(fields[3], fields[48])
        return OrderBook(
            code=code,
            received_at=fields[2].strip(),
            market_phase=market_phase,
            levels=levels,
            source="SH",
        )

    @classmethod
    def _build_integrated_order_book(cls, code: str, fields: list[str]) -> OrderBook:
        levels = [
            cls._build_level(
                ask_price=cls._safe_int_from_fields(fields, 5 + offset * 4),
                bid_price=cls._safe_int_from_fields(fields, 45 + offset * 4),
                ask_size=cls._safe_int_from_fields(fields, 8 + offset * 4),
                bid_size=cls._safe_int_from_fields(fields, 48 + offset * 4),
            )
            for offset in range(5)
        ]
        market_phase = f"krx:{fields[3].strip() or '-'}|nxt:{fields[4].strip() or '-'}"
        return OrderBook(
            code=code,
            received_at=fields[2].strip(),
            market_phase=market_phase,
            levels=levels,
            source="UH",
        )

    def _get_best_order_book_from_tr(self, code: str) -> OrderBook:
        try:
            result = self._request("TR_RB002", ["0"])
        except (RuntimeError, TimeoutError) as exc:
            return self._unavailable_order_book(
                code,
                source="TR_RB002",
                message=f"order book unavailable: no fresh UH tick and TR_RB002 failed ({exc})",
            )

        for row in range(result.multi_row_count):
            row_code = self._normalize_stock_code(self._multi_text(row, 1))
            if row_code != code:
                continue
            received_at = self._multi_text(row, 2).strip()
            market_phase = self._market_phase_from_tr_rb002(self._multi_text(row, 17))
            if market_phase == "after_hours_close":
                return self._unavailable_order_book(
                    code,
                    source="TR_RB002",
                    received_at=received_at,
                    market_phase=market_phase,
                    message="after-hours close trading uses the closing price; no order book is available",
                )
            ask_price = self._multi_int(row, 20)
            bid_price = self._multi_int(row, 21)
            if ask_price or bid_price:
                return OrderBook(
                    code=code,
                    received_at=received_at,
                    market_phase=market_phase,
                    levels=[
                        OrderBookLevel(
                            ask_price=ask_price,
                            ask_size=0,
                            bid_price=bid_price,
                            bid_size=0,
                        )
                    ],
                    source="TR_RB002",
                    partial=True,
                    message="fresh UH depth tick unavailable; returning TR best bid/ask only",
                )
            return self._unavailable_order_book(
                code,
                source="TR_RB002",
                received_at=received_at,
                market_phase=market_phase,
                message="no current best bid/ask quote from TR_RB002; stock may be inactive or not trading",
            )

        return self._unavailable_order_book(
            code,
            source="TR_RB002",
            message="no current quote row from TR_RB002; stock may be inactive or not trading",
        )

    @staticmethod
    def _unavailable_order_book(
        code: str,
        *,
        source: str,
        message: str,
        received_at: str = "",
        market_phase: str = "no_quote",
    ) -> OrderBook:
        return OrderBook(
            code=code,
            received_at=received_at,
            market_phase=market_phase,
            levels=[],
            source=source,
            available=False,
            message=message,
        )

    @staticmethod
    def _build_level(ask_price: int, bid_price: int, ask_size: int, bid_size: int) -> OrderBookLevel:
        return OrderBookLevel(
            ask_price=ask_price,
            ask_size=ask_size,
            bid_price=bid_price,
            bid_size=bid_size,
        )

    @staticmethod
    def _safe_int_from_fields(fields: list[str], index: int) -> int:
        if index >= len(fields):
            return 0
        text = fields[index].replace(",", "").strip()
        if not text:
            return 0
        try:
            return int(float(text))
        except ValueError:
            return 0

    @staticmethod
    def _market_phase_from_cash_quote(market_phase_code: str, simultaneous_code: str) -> str:
        if simultaneous_code in {"1", "2"}:
            return "auction"
        return {
            "1": "regular",
            "2": "pre_market",
            "3": "after_hours_close",
            "4": "after_hours_single",
        }.get(market_phase_code.strip(), market_phase_code.strip() or "unknown")

    @staticmethod
    def _market_phase_from_tr_rb002(market_phase_code: str) -> str:
        return {
            "1": "regular",
            "2": "pre_market",
            "3": "after_hours_close",
            "4": "after_hours_single",
            "7": "buy_in",
            "8": "same_day_buy_in",
        }.get(market_phase_code.strip(), market_phase_code.strip() or "unknown")

    @staticmethod
    def _normalize_stock_code(code: str) -> str:
        cleaned = code.strip().upper()
        if cleaned.startswith(("A", "J", "Q")) and len(cleaned) > 1:
            return cleaned[1:]
        return cleaned

    @classmethod
    def _normalize_rt_event_code(cls, rt_type: str, code: str) -> str:
        cleaned = cls._text(code).strip().upper()
        if not cleaned:
            return ""
        if rt_type in {"SC", "UC", "SH", "UH", "N0", "N2"}:
            return cls._normalize_stock_code(cleaned)
        return cleaned

    @staticmethod
    def _build_stock_order_code(code: str) -> str:
        cleaned = code.strip().upper()
        # 신한 Indi 주문 TR은 종목코드 앞에 시장 접두어가 붙은 형식을 기대한다.
        # 일반적인 6자리 종목코드는 "A"를 붙여 주문 코드로 보정한다.
        if cleaned.startswith(("A", "J", "Q")):
            return cleaned
        return f"A{cleaned}"

    @staticmethod
    def _order_type_code(order_type: str, modification: bool = False) -> str:
        # 현재 MCP 주문 API는 시장가/지정가만 다룬다.
        # 신용 조건부, IOC/FOK, 시간외 등 더 위험하거나 복잡한 호가 유형은 아직 열어두지 않는다.
        if order_type == "market":
            return "1"
        return "Z" if modification else "2"

    @staticmethod
    def _normalize_order_type_name(order_type_name: str) -> str:
        if "시장가" in order_type_name:
            return "market"
        return "limit"

    @staticmethod
    def _calculate_peg(per_value: float, eps_growth: float) -> float | None:
        if eps_growth <= 0:
            return None
        return round(per_value / eps_growth, 4)

    @staticmethod
    def _normalize_indi_date(value: str | None, field_name: str) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            return None
        if len(cleaned) == 10 and cleaned[4] == "-" and cleaned[7] == "-":
            return cleaned.replace("-", "")
        if len(cleaned) == 8 and cleaned.isdigit():
            return cleaned
        raise ValueError(f"{field_name} must be YYYYMMDD or YYYY-MM-DD")

    @staticmethod
    def _format_execution_timestamp(date_text: str, time_text: str) -> str:
        date_value = date_text.strip()
        time_value = time_text.strip()
        if len(date_value) == 8 and len(time_value) == 6:
            return f"{date_value}T{time_value}"
        return date_value or time_value

    @staticmethod
    def _format_compact_timestamp(date_text: str, time_text: str) -> str:
        date_value = date_text.strip()
        time_value = time_text.strip()
        if len(date_value) == 8 and len(time_value) == 6:
            return f"{date_value}{time_value}"
        return date_value or time_value

    @staticmethod
    def _clean_news_content_html(content: str) -> str:
        cleaned = content.strip()
        if not cleaned:
            return ""

        cleaned = RealIndiClient._NEWS_ANCHOR_PATTERN.sub(RealIndiClient._replace_news_anchor, cleaned)
        cleaned = re.sub(r"(?i)<br\s*/?>", "\n", cleaned)
        cleaned = re.sub(r"(?i)</(p|div|tr|h[1-6])\s*>", "\n\n", cleaned)
        cleaned = re.sub(r"(?i)</li\s*>", "\n", cleaned)
        cleaned = re.sub(r"(?i)<li\b[^>]*>", "- ", cleaned)
        cleaned = re.sub(r"(?i)<[^>]+>", "", cleaned)
        cleaned = html.unescape(cleaned)
        cleaned = cleaned.replace("\xa0", " ")
        cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
        cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

        paragraphs: list[str] = []
        for paragraph in re.split(r"\n{2,}", cleaned):
            lines = [re.sub(r"\s+", " ", line).strip() for line in paragraph.split("\n") if line.strip()]
            if not lines:
                continue

            parts: list[str] = []
            current_text: list[str] = []
            for line in lines:
                if line.startswith("- "):
                    if current_text:
                        parts.append(" ".join(current_text))
                        current_text = []
                    parts.append(line)
                    continue
                current_text.append(line)

            if current_text:
                parts.append(" ".join(current_text))

            paragraphs.append("\n".join(parts))

        return "\n\n".join(paragraphs).strip()

    @classmethod
    def _extract_news_content_links(cls, content: str) -> list[dict[str, str]]:
        links: list[dict[str, str]] = []
        for match in cls._NEWS_ANCHOR_PATTERN.finditer(content or ""):
            href = html.unescape(match.group(2) or "").strip()
            inner_html = match.group(4) or ""
            text = re.sub(r"(?is)<[^>]+>", "", inner_html)
            text = html.unescape(text).replace("\xa0", " ").strip()
            if not href and not text:
                continue

            link = {"text": text, "href": href}
            rcp_no = cls._extract_rcp_no_from_text(href) or cls._extract_rcp_no_from_text(match.group(0))
            if rcp_no:
                link["rcpNo"] = rcp_no
            links.append(link)
        return links

    @classmethod
    def _extract_news_content_rcp_no(cls, content: str, links: list[dict[str, str]] | None = None) -> str | None:
        for link in links or cls._extract_news_content_links(content):
            rcp_no = link.get("rcpNo")
            if rcp_no:
                return rcp_no
        return cls._extract_rcp_no_from_text(content)

    @classmethod
    def _extract_rcp_no_from_text(cls, value: str) -> str | None:
        decoded = html.unescape(value or "")
        match = cls._DART_RCP_NO_PATTERN.search(decoded)
        if match is None:
            return None
        return next((group for group in match.groups() if group), None)

    @classmethod
    def _news_type_label(cls, news_type: str) -> str:
        cleaned = news_type.strip().upper()
        if not cleaned:
            return "unknown"
        return cls._NEWS_TYPE_LABELS.get(cleaned, f"unknown({cleaned})")

    @staticmethod
    def _replace_news_anchor(match: re.Match[str]) -> str:
        href = html.unescape(match.group(2) or "")
        inner_html = match.group(4) or ""
        inner_text = re.sub(r"(?is)<[^>]+>", "", inner_html)
        inner_text = html.unescape(inner_text).replace("\xa0", " ").strip()

        if not href.lower().startswith("hts:"):
            return inner_text

        code_match = re.search(r"(?<!\d)(?:[AJQ])?(\d{6})(?!\d)", href, re.IGNORECASE)
        if not code_match:
            return inner_text

        code = code_match.group(1)
        if not inner_text:
            return f"({code})"
        return f"{inner_text}({code})"

    def _register_disclosure_realtime(self) -> None:
        with self._rt_control_lock:
            ops_log(LogSource.RT_REAL, "N2 RequestRTReg begin subject=* set_query_name=False")
            self._reset_rt_wait_state()
            self._last_rt_error_details = None
            signature = "RequestRTReg(QVariant, QVariant)"
            ops_log(LogSource.RT_REAL, f"N2 RequestRTReg calling signature={signature} args=(N2, *)")
            try:
                result = self._rt_control.dynamicCall(signature, "N2", "*")
            except Exception as exc:
                self._capture_rt_error_details("N2", "*", "RequestRTReg", signature, exception=str(exc))
                if self._accept_gi005_request_rt_reg_warning("N2", "*", signature, f"exception={exc}"):
                    self._rt_disclosure_registered = True
                    return
                ops_log(LogSource.RT_REAL,
                    f"N2 RequestRTReg exception signature={signature} exception={exc} "
                    f"details={self._last_rt_error_details}",
                )
                raise RuntimeError("RequestRTReg failed: N2 *") from exc
            ops_log(LogSource.RT_REAL, f"N2 RequestRTReg returned result={result!r}")
            if result not in (None, False, 0, ""):
                self._rt_disclosure_registered = True
                ops_log(LogSource.RT_REAL, f"N2 RequestRTReg success signature={signature} result={result!r}")
                return
            self._capture_rt_error_details("N2", "*", "RequestRTReg", signature)
            if self._accept_gi005_request_rt_reg_warning("N2", "*", signature, f"result={result!r}"):
                self._rt_disclosure_registered = True
                return
            ops_log(LogSource.RT_REAL,
                f"N2 RequestRTReg failed signature={signature} result={result!r} "
                f"details={self._last_rt_error_details}",
            )
        raise RuntimeError("RequestRTReg failed: N2 *")

    def _unregister_disclosure_realtime(self) -> None:
        with self._rt_control_lock:
            ops_log(LogSource.RT_REAL, "N2 UnRequestRTReg begin")
            self._reset_rt_wait_state()
            if not self._rt_control.dynamicCall("SetQueryName(QString)", "N2"):
                ops_log(LogSource.RT_REAL, "N2 SetQueryName failed before UnRequestRTReg")
                raise RuntimeError("SetQueryName failed: N2")
            ops_log(LogSource.RT_REAL, "N2 SetQueryName ok")
            self._last_rt_error_details = None
            signature = "UnRequestRTReg(QVariant)"
            ops_log(LogSource.RT_REAL, f"N2 UnRequestRTReg calling signature={signature} args=(N2)")
            try:
                result = self._rt_control.dynamicCall(signature, "N2")
            except Exception as exc:
                self._capture_rt_error_details("N2", "", "UnRequestRTReg", signature, exception=str(exc))
                ops_log(LogSource.RT_REAL,
                    f"N2 UnRequestRTReg exception signature={signature} exception={exc} "
                    f"details={self._last_rt_error_details}",
                )
                raise RuntimeError("UnRequestRTReg failed: N2") from exc
            ops_log(LogSource.RT_REAL, f"N2 UnRequestRTReg returned result={result!r}")
            if result not in (None, False, 0, ""):
                self._rt_disclosure_registered = False
                self._rt_snapshots.pop(("N2", ""), None)
                ops_log(LogSource.RT_REAL, f"N2 UnRequestRTReg success signature={signature} result={result!r}")
                return
            self._capture_rt_error_details("N2", "", "UnRequestRTReg", signature)
            ops_log(LogSource.RT_REAL,
                f"N2 UnRequestRTReg failed signature={signature} result={result!r} "
                f"details={self._last_rt_error_details}",
            )
        raise RuntimeError("UnRequestRTReg failed: N2")

    def _register_news_realtime(self) -> None:
        with self._rt_control_lock:
            ops_log(LogSource.RT_REAL, "N0 RequestRTReg begin subject=* set_query_name=False")
            self._reset_rt_wait_state()
            self._last_rt_error_details = None
            signature = "RequestRTReg(QVariant, QVariant)"
            ops_log(LogSource.RT_REAL, f"N0 RequestRTReg calling signature={signature} args=(N0, *)")
            try:
                result = self._rt_control.dynamicCall(signature, "N0", "*")
            except Exception as exc:
                self._capture_rt_error_details("N0", "*", "RequestRTReg", signature, exception=str(exc))
                if self._accept_gi005_request_rt_reg_warning("N0", "*", signature, f"exception={exc}"):
                    self._rt_news_registered = True
                    return
                ops_log(LogSource.RT_REAL,
                    f"N0 RequestRTReg exception signature={signature} exception={exc} "
                    f"details={self._last_rt_error_details}",
                )
                raise RuntimeError("RequestRTReg failed: N0 *") from exc
            ops_log(LogSource.RT_REAL, f"N0 RequestRTReg returned result={result!r}")
            if result not in (None, False, 0, ""):
                self._rt_news_registered = True
                ops_log(LogSource.RT_REAL, f"N0 RequestRTReg success signature={signature} result={result!r}")
                return
            self._capture_rt_error_details("N0", "*", "RequestRTReg", signature)
            if self._accept_gi005_request_rt_reg_warning("N0", "*", signature, f"result={result!r}"):
                self._rt_news_registered = True
                return
            ops_log(LogSource.RT_REAL,
                f"N0 RequestRTReg failed signature={signature} result={result!r} "
                f"details={self._last_rt_error_details}",
            )
        raise RuntimeError("RequestRTReg failed: N0 *")

    def _unregister_news_realtime(self) -> None:
        with self._rt_control_lock:
            ops_log(LogSource.RT_REAL, "N0 UnRequestRTReg begin")
            self._reset_rt_wait_state()
            if not self._rt_control.dynamicCall("SetQueryName(QString)", "N0"):
                ops_log(LogSource.RT_REAL, "N0 SetQueryName failed before UnRequestRTReg")
                raise RuntimeError("SetQueryName failed: N0")
            ops_log(LogSource.RT_REAL, "N0 SetQueryName ok")
            self._last_rt_error_details = None
            signature = "UnRequestRTReg(QString, QString)"
            ops_log(LogSource.RT_REAL, f"N0 UnRequestRTReg calling signature={signature} args=(N0, '')")
            try:
                result = self._rt_control.dynamicCall(signature, "N0", "")
            except Exception as exc:
                self._capture_rt_error_details("N0", "", "UnRequestRTReg", signature, exception=str(exc))
                ops_log(LogSource.RT_REAL,
                    f"N0 UnRequestRTReg exception signature={signature} exception={exc} "
                    f"details={self._last_rt_error_details}",
                )
                raise RuntimeError("UnRequestRTReg failed: N0") from exc
            ops_log(LogSource.RT_REAL, f"N0 UnRequestRTReg returned result={result!r}")
            if result not in (None, False, 0, ""):
                self._rt_news_registered = False
                self._rt_snapshots.pop(("N0", ""), None)
                ops_log(LogSource.RT_REAL, f"N0 UnRequestRTReg success signature={signature} result={result!r}")
                return
            self._capture_rt_error_details("N0", "", "UnRequestRTReg", signature)
            ops_log(LogSource.RT_REAL,
                f"N0 UnRequestRTReg failed signature={signature} result={result!r} "
                f"details={self._last_rt_error_details}",
            )
        raise RuntimeError("UnRequestRTReg failed: N0")
