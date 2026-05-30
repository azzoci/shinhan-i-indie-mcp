from __future__ import annotations

import os
import re
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from homestock.analysis import build_technical_indicators, detect_chart_patterns
from homestock.dart_viewer import (
    disclosure_content_split_selector,
    disclosure_to_html,
    looks_like_disclosure_body_html,
)
from homestock.display_format import format_display_decimal
from homestock.gold_runtime_state import GoldRuntimeStateManager
from homestock.holding_alerts import HoldingAlertManager
from homestock.indi import IndiClient
from homestock.models import DailyPrice, DisclosureContent, GoldOrderRequest, HttpCallbackSpec, IntradayPrice, OpenOrder, OrderRequest, OrderResult
from homestock.order_guard import OrderGuard
from homestock.runtime_state import RuntimeStateManager
from homestock.scripter import Scripter, write_crash_log
from homestock.ops_log import LogSource, ops_log
from homestock.webhook import CallbackDispatcher


class _UnavailableGoldRuntimeState:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def close(self) -> None:
        return None

    def health_status(self) -> dict[str, Any]:
        return {
            "available": False,
            "active_alert_count": 0,
            "active_callback_count": 0,
            "owned_price_codes": {},
            "state_trading_date": "",
            "message": str(self._error),
            "error_type": self._error.__class__.__name__,
        }

    def register_gold_price_alert(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self._raise_unavailable()

    def list_gold_price_alerts(self) -> list[dict[str, Any]]:
        self._raise_unavailable()

    def cancel_gold_price_alert(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self._raise_unavailable()

    def register_gold_price_callback(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self._raise_unavailable()

    def list_gold_price_callbacks(self) -> list[dict[str, Any]]:
        self._raise_unavailable()

    def cancel_gold_price_callback(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self._raise_unavailable()

    def _raise_unavailable(self) -> None:
        raise RuntimeError(f"gold runtime unavailable: {self._error}") from self._error


class HomestockTools:
    _TECHNICAL_WARMUP_DAYS = 200
    _INDI_RECREATED_EVENT_TYPE = "ocx_recreated"
    _INDI_RECREATED_MESSAGE = "OCX 재생성 감지. 자동 재시작은 수행하지 않음"
    _INDI_PROCESS_MONITOR_INTERVAL_ENV = "HOMESTOCK_INDI_PROCESS_MONITOR_INTERVAL_SECONDS"
    _INDI_EVENT_PUMP_HEARTBEAT_NAME = "indi_event_pump"
    _INDI_EVENT_PUMP_HEARTBEAT_INTERVAL_SECONDS = 5.0
    _STARTUP_FAILED_EVENT_TYPE = "mcp_startup_failed"
    _STARTUP_FAILED_MESSAGE = "MCP 서버 구동 실패"
    _ORDER_CARRYOVER_STATUS_DESCRIPTIONS: Final[dict[str, str]] = {
        "success": "자동 이월 주문을 성공적으로 접수했습니다.",
        "missed": "자동 이월을 실행하지 못했습니다.",
        "chaos": "자동 이월을 안전하게 판단할 수 없어 중단했습니다.",
        "registered_too_late_for_transition": "전환 시작 직전 또는 이후에 등록되어 당일 해당 전환은 실행하지 않았습니다.",
        "transition_window_missed": "전환 실행창을 지나 자동 이월을 시도하지 못했습니다.",
        "transition_window_elapsed_before_reorder": "취소 확인 후 전환 실행창이 지나 신규 주문을 넣지 않았습니다.",
        "open_order_not_found": "등록된 원 주문을 미체결 목록에서 찾지 못했습니다.",
        "matched_multiple_open_orders": "등록 주문 식별자가 여러 미체결 주문과 매칭되어 자동 판단을 중단했습니다.",
        "partial_fill_chaos": "부분 체결이 확인되어 과주문 방지를 위해 자동 이월을 중단했습니다.",
        "order_method_is_not_sor": "등록 주문이 SOR 주문이 아니어서 자동 이월 대상에서 제외했습니다.",
        "original_order_id_unavailable": "원 주문번호를 확인할 수 없어 자동 이월을 중단했습니다.",
        "sor_original_order_id_unavailable": "SOR 원주문번호를 확인할 수 없어 자동 이월을 중단했습니다.",
        "credit_order_not_supported": "신용 주문은 자동 이월 대상이 아닙니다.",
        "only_limit_orders_are_supported": "지정가 주문만 자동 이월할 수 있습니다.",
        "limit_price_unavailable": "지정가를 확인할 수 없어 자동 이월을 중단했습니다.",
        "unfilled_quantity_unavailable": "미체결 잔량이 없어 자동 이월할 수 없습니다.",
        "order_exchange_is_not_nxt": "프리마켓에서 본장으로 넘길 NXT 미체결 주문이 아닙니다.",
        "order_exchange_is_not_krx": "본장에서 애프터마켓으로 넘길 KRX 미체결 주문이 아닙니다.",
        "side_changed": "등록 당시와 주문 방향이 달라 자동 이월을 중단했습니다.",
        "order_type_changed": "등록 당시와 주문 유형이 달라 자동 이월을 중단했습니다.",
        "price_changed": "등록 당시와 주문 가격이 달라 자동 이월을 중단했습니다.",
        "cancel_exception": "원 주문 취소 요청 중 예외가 발생했습니다.",
        "cancel_not_accepted": "원 주문 취소 요청이 접수되지 않았습니다.",
        "cancel_confirmation_exception": "취소 확인 조회 중 예외가 발생했습니다.",
        "cancel_unconfirmed_open_order_still_exists": "취소 후에도 원 주문이 미체결 목록에 남아 있어 신규 주문을 넣지 않았습니다.",
        "cancelled_quantity_unconfirmed": "취소된 수량을 확정할 수 없어 신규 주문을 넣지 않았습니다.",
        "confirmed": "취소 수량 확인이 완료되었습니다.",
        "confirmed_no_execution_after_cancel": "취소 후 추가 체결이 없어 미체결 잔량 전체를 취소 수량으로 확인했습니다.",
        "no_cancelled_quantity_to_carryover": "이월할 취소 확정 수량이 없습니다.",
        "place_exception": "신규 SOR 주문 전송 중 예외가 발생했습니다.",
        "place_not_accepted": "신규 SOR 주문이 접수되지 않았습니다.",
        "exception": "자동 이월 처리 중 예외가 발생했습니다.",
    }

    def __init__(
        self,
        client: IndiClient,
        order_guard: OrderGuard,
        scripter: Scripter,
        runtime_state_dir: str | None = None,
        scripter_log_dir: str | None = None,
        holding_alert_config_path: str | None = None,
    ) -> None:
        ops_log(LogSource.STARTUP_MANAGE, "HomestockTools.__init__ entered")
        self._client = client
        self._order_guard = order_guard
        self._scripter: Final[Scripter] = scripter
        self._scripter_log_dir = scripter_log_dir
        self._indi_recreated_callback_lock = threading.Lock()
        self._indi_recreated_callback_sent = False
        self._indi_process_monitor_stop = threading.Event()
        self._indi_process_monitor_thread: threading.Thread | None = None
        self._indi_event_pump_heartbeat_stop = threading.Event()
        self._indi_event_pump_heartbeat_thread: threading.Thread | None = None
        self._order_carryover_stop = threading.Event()
        self._order_carryover_thread: threading.Thread | None = None
        self._order_carryover_lock = threading.RLock()
        self._order_carryovers: list[dict[str, Any]] = []
        self._order_carryover_dispatcher = CallbackDispatcher()
        readiness_status = self._probe_pre_runtime_readiness()
        self._fail_startup_if_not_ready(readiness_status, runtime_state_dir)
        ops_log(LogSource.STARTUP_MANAGE,
            f"creating RuntimeStateManager client={client.__class__.__name__} "
            f"runtime_state_dir={runtime_state_dir or '<default>'}",
        )
        self._runtime_state = RuntimeStateManager(
            client,
            runtime_state_dir,
            fall_safe_executor=self._execute_fall_safe_order,
            system_event_recorder=self._record_system_event,
            system_callbacks_configurer=self._configure_system_callbacks,
        )
        ops_log(LogSource.STARTUP_MANAGE, "RuntimeStateManager ready")
        try:
            self._gold_runtime_state = GoldRuntimeStateManager(
                client,
                runtime_state_dir,
                system_event_recorder=self._record_system_event,
            )
        except Exception as exc:
            ops_log(LogSource.STARTUP_MANAGE,
                f"GoldRuntimeStateManager unavailable; continuing without gold runtime alerts: "
                f"{exc.__class__.__name__}: {exc}",
            )
            self._gold_runtime_state = _UnavailableGoldRuntimeState(exc)
            try:
                self._record_system_event(
                    event_type="gold_runtime_startup_failed",
                    message=f"금현물 런타임 구동 실패: {exc}",
                    details={"error": str(exc), "error_type": exc.__class__.__name__},
                )
            except Exception as event_exc:
                ops_log(LogSource.STARTUP_MANAGE,
                    f"gold runtime startup failure system callback dispatch failed: "
                    f"{event_exc.__class__.__name__}: {event_exc}",
                )
        else:
            ops_log(LogSource.STARTUP_MANAGE, "GoldRuntimeStateManager ready")
        self._holding_alerts = HoldingAlertManager(
            client,
            state_dir=runtime_state_dir,
            config_path=holding_alert_config_path,
        )
        ops_log(LogSource.STARTUP_MANAGE, "HoldingAlertManager ready")
        ops_log(LogSource.STARTUP_MANAGE, "starting Indi event pump heartbeat if supported")
        self._start_indi_event_pump_heartbeat_if_available()
        ops_log(LogSource.STARTUP_MANAGE, "starting Indi process monitor if supported")
        self._start_indi_process_monitor_if_available()
        ops_log(LogSource.STARTUP_MANAGE, "HomestockTools.__init__ complete")

    def close(self) -> None:
        ops_log(LogSource.MANAGE, "HomestockTools.close entered")
        self._safe_close_action("order carryover worker stop", self._stop_order_carryover_worker)
        self._safe_close_action("order carryover callback dispatcher close", self._close_order_carryover_dispatcher)
        self._safe_close_action("Indi event pump heartbeat stop", self._stop_indi_event_pump_heartbeat)
        self._safe_close_action("Indi process monitor stop", self._stop_indi_process_monitor)
        runtime_state = getattr(self, "_runtime_state", None)
        gold_runtime_state = getattr(self, "_gold_runtime_state", None)
        holding_alerts = getattr(self, "_holding_alerts", None)
        self._safe_close_action("HoldingAlertManager close", getattr(holding_alerts, "close", None))
        self._safe_close_action("GoldRuntimeStateManager close", getattr(gold_runtime_state, "close", None))
        self._safe_close_action("RuntimeStateManager close", getattr(runtime_state, "close", None))
        self._safe_close_action("Indi client close", getattr(self._client, "close", None))
        self._safe_close_action("Scripter close", self._scripter.close)
        ops_log(LogSource.MANAGE, "HomestockTools.close complete")

    @staticmethod
    def _safe_close_action(label: str, action: Any) -> None:
        if not callable(action):
            return
        try:
            action()
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"{label} failed: {exc.__class__.__name__}: {exc}")

    def health_check(self) -> dict[str, Any]:
        status = self._client.health_check(self._order_guard.allow_live_orders).to_dict()
        status["gold_runtime"] = self._gold_runtime_health_status()
        self._dispatch_indi_process_recreated_callback(status)
        return status

    def _gold_runtime_health_status(self) -> dict[str, Any]:
        health_status = getattr(self._gold_runtime_state, "health_status", None)
        if not callable(health_status):
            return {
                "available": False,
                "active_alert_count": 0,
                "active_callback_count": 0,
                "owned_price_codes": {},
                "state_trading_date": "",
                "message": "gold runtime health unavailable",
            }
        try:
            return dict(health_status())
        except Exception as exc:
            return {
                "available": False,
                "active_alert_count": 0,
                "active_callback_count": 0,
                "owned_price_codes": {},
                "state_trading_date": "",
                "message": str(exc),
                "error_type": exc.__class__.__name__,
            }

    def _probe_pre_runtime_readiness(self) -> dict[str, Any]:
        ops_log(LogSource.STARTUP_MANAGE,
            "pre-RuntimeStateManager readiness probe start "
            "(client.health_check, status-only for real backend; no AccountList TR)",
        )
        try:
            status = self._client.health_check(self._order_guard.allow_live_orders).to_dict()
        except Exception as exc:
            ops_log(LogSource.STARTUP_MANAGE,
                f"pre-RuntimeStateManager readiness probe exception: "
                f"{exc.__class__.__name__}: {exc}",
            )
            return {
                "ok": False,
                "backend": self._client.__class__.__name__,
                "exception_type": exc.__class__.__name__,
                "message": str(exc),
            }
        ops_log(LogSource.STARTUP_MANAGE,
            "pre-RuntimeStateManager readiness probe result "
            f"ok={status.get('ok')} "
            f"backend={status.get('backend')} "
            f"ocx_ready={status.get('ocx_ready')} "
            f"login_ready={status.get('login_ready')} "
            f"live_orders_allowed={status.get('live_orders_allowed')} "
            f"indi_process_running={status.get('indi_process_running')} "
            f"indi_process_restarted={status.get('indi_process_restarted')}",
        )
        ops_log(LogSource.STARTUP_MANAGE,
            f"pre-RuntimeStateManager readiness probe message={status.get('message')}",
        )
        return status

    def _fail_startup_if_not_ready(
        self,
        readiness_status: dict[str, Any],
        runtime_state_dir: str | None,
    ) -> None:
        if bool(readiness_status.get("ok")):
            return
        if str(readiness_status.get("backend", "")).lower() == "mock":
            return
        message = str(readiness_status.get("message") or "readiness probe failed")
        details = {
            "readiness_status": readiness_status,
        }
        ops_log(LogSource.STARTUP_MANAGE,
            "readiness probe failed before RuntimeStateManager restore; "
            "dispatching startup failure system callback and aborting startup",
        )
        failure_state: RuntimeStateManager | None = None
        try:
            failure_state = RuntimeStateManager(
                self._client,
                runtime_state_dir,
                fall_safe_executor=self._execute_fall_safe_order,
                restore_realtime=False,
                system_event_recorder=self._record_system_event,
                system_callbacks_configurer=self._configure_system_callbacks,
            )
            failure_state.dispatch_system_event(
                event_type=self._STARTUP_FAILED_EVENT_TYPE,
                message=f"{self._STARTUP_FAILED_MESSAGE}: {message}",
                details=details,
            )
        except Exception as exc:
            ops_log(LogSource.STARTUP_MANAGE,
                f"startup failure system callback dispatch failed: "
                f"{exc.__class__.__name__}: {exc}",
            )
        finally:
            if failure_state is not None:
                try:
                    failure_state.close()
                except Exception as close_exc:
                    ops_log(LogSource.STARTUP_MANAGE,
                        f"startup failure RuntimeStateManager close failed: "
                        f"{close_exc.__class__.__name__}: {close_exc}",
                    )
        raise RuntimeError(f"{self._STARTUP_FAILED_MESSAGE}: {message}")

    def _start_indi_event_pump_heartbeat_if_available(self) -> None:
        snapshot = getattr(self._client, "event_pump_snapshot", None)
        if not callable(snapshot):
            ops_log(LogSource.STARTUP_MANAGE, "Indi event pump heartbeat skipped: client has no event_pump_snapshot")
            return
        interval_seconds = self._indi_event_pump_heartbeat_interval_seconds()
        ops_log(LogSource.STARTUP_MANAGE, f"Indi event pump heartbeat starting interval_seconds={interval_seconds}")
        thread = threading.Thread(
            target=self._monitor_indi_event_pump,
            args=(snapshot, interval_seconds),
            name="homestock-indi-event-pump-heartbeat",
            daemon=True,
        )
        self._indi_event_pump_heartbeat_thread = thread
        thread.start()
        ops_log(LogSource.STARTUP_MANAGE, f"Indi event pump heartbeat thread started name={thread.name}")

    def _monitor_indi_event_pump(
        self,
        snapshot: Callable[[], dict[str, object]],
        interval_seconds: float,
    ) -> None:
        last_seen_pump_count: int | None = None
        while not self._indi_event_pump_heartbeat_stop.wait(interval_seconds):
            try:
                state = snapshot()
            except Exception as exc:
                if self._indi_event_pump_heartbeat_stop.is_set():
                    return
                ops_log(LogSource.MANAGE, f"Indi event pump heartbeat snapshot failed: {exc.__class__.__name__}: {exc}")
                continue
            if self._indi_event_pump_heartbeat_stop.is_set():
                return
            pump_count = int(state.get("pump_count") or 0)
            if pump_count <= 0:
                continue
            if last_seen_pump_count is not None and pump_count <= last_seen_pump_count:
                continue
            last_seen_pump_count = pump_count
            payload = self._indi_event_pump_heartbeat_payload(state)
            self._safe_scripter_call("heartbeat", self._INDI_EVENT_PUMP_HEARTBEAT_NAME, payload)

    def _indi_event_pump_heartbeat_payload(self, state: dict[str, object]) -> dict[str, object]:
        payload: dict[str, object] = {
            "pump_count": int(state.get("pump_count") or 0),
            "observed_at": datetime.now().astimezone().isoformat(),
            "pump_interval_seconds": state.get("pump_interval_seconds"),
            "worker_thread_alive": bool(state.get("worker_thread_alive")),
            "event_thread_alive": bool(state.get("event_thread_alive")),
        }
        last_pump_monotonic = state.get("last_pump_monotonic")
        if isinstance(last_pump_monotonic, (int, float)):
            payload["seconds_since_last_pump"] = max(0.0, time.monotonic() - float(last_pump_monotonic))
        return payload

    def _stop_indi_event_pump_heartbeat(self) -> None:
        self._indi_event_pump_heartbeat_stop.set()
        thread = self._indi_event_pump_heartbeat_thread
        if thread is None or threading.get_ident() == thread.ident:
            return
        thread.join(timeout=min(self._indi_event_pump_heartbeat_interval_seconds(), 5.0))
        ops_log(LogSource.MANAGE,
            f"Indi event pump heartbeat stopped alive={thread.is_alive() if thread is not None else False}",
        )

    def _start_indi_process_monitor_if_available(self) -> None:
        if getattr(self._client, "INDI_MAIN_PROCESS_NAME", None) is None:
            ops_log(LogSource.STARTUP_MANAGE, "Indi process monitor skipped: client has no INDI_MAIN_PROCESS_NAME")
            return
        monitor = getattr(self._client, "check_indi_process_status", None)
        if not callable(monitor):
            ops_log(LogSource.STARTUP_MANAGE, "Indi process monitor skipped: client has no status callable")
            return
        interval_seconds = self._indi_process_monitor_interval_seconds()
        ops_log(LogSource.STARTUP_MANAGE, f"Indi process monitor starting interval_seconds={interval_seconds}")
        thread = threading.Thread(
            target=self._monitor_indi_process,
            args=(monitor, interval_seconds),
            name="homestock-indi-process-monitor",
            daemon=True,
        )
        self._indi_process_monitor_thread = thread
        thread.start()
        ops_log(LogSource.STARTUP_MANAGE, f"Indi process monitor thread started name={thread.name}")

    def _monitor_indi_process(self, monitor: Callable[[], dict[str, object]], interval_seconds: float) -> None:
        while not self._indi_process_monitor_stop.wait(interval_seconds):
            try:
                status = monitor()
            except Exception as exc:
                if self._indi_process_monitor_stop.is_set():
                    return
                ops_log(LogSource.MANAGE, f"GiExpertMain.exe monitor failed: {exc.__class__.__name__}: {exc}")
                self._safe_scripter_call(
                    "error",
                    "tools.indi_process_monitor",
                    "GiExpertMain.exe monitor failed",
                    exc,
                    None,
                    None,
                )
                self._safe_scripter_call(
                    "system_callback",
                    "indi_monitor_failed",
                    "GiExpertMain.exe monitor failed",
                    {
                        "exception_type": exc.__class__.__name__,
                        "error": str(exc),
                    },
                )
                continue
            if self._indi_process_monitor_stop.is_set():
                return
            if self._dispatch_indi_process_recreated_callback(status):
                return

    def _stop_indi_process_monitor(self) -> None:
        self._indi_process_monitor_stop.set()
        thread = self._indi_process_monitor_thread
        if thread is None or threading.get_ident() == thread.ident:
            return
        thread.join(timeout=min(self._indi_process_monitor_interval_seconds(), 5.0))
        ops_log(LogSource.MANAGE,
            f"Indi process monitor stopped alive={thread.is_alive() if thread is not None else False}",
        )

    def _ensure_order_carryover_worker(self) -> None:
        with self._order_carryover_lock:
            thread = self._order_carryover_thread
            if thread is not None and thread.is_alive():
                return
            self._order_carryover_stop.clear()
            thread = threading.Thread(
                target=self._order_carryover_loop,
                name="homestock-order-carryover",
                daemon=True,
            )
            self._order_carryover_thread = thread
            thread.start()
            ops_log(LogSource.MANAGE, f"order carryover worker started name={thread.name}")

    def _order_carryover_loop(self) -> None:
        interval_seconds = self._order_carryover_interval_seconds()
        while not self._order_carryover_stop.wait(interval_seconds):
            try:
                self._process_due_order_carryovers()
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"order carryover worker failed: {exc.__class__.__name__}: {exc}")
            if not self._has_pending_order_carryovers():
                return

    def _stop_order_carryover_worker(self) -> None:
        self._order_carryover_stop.set()
        thread = self._order_carryover_thread
        if thread is None or threading.get_ident() == thread.ident:
            return
        thread.join(timeout=min(self._order_carryover_interval_seconds(), 5.0))
        ops_log(
            LogSource.MANAGE,
            f"order carryover worker stopped alive={thread.is_alive() if thread is not None else False}",
        )

    def _close_order_carryover_dispatcher(self) -> None:
        dispatcher = getattr(self, "_order_carryover_dispatcher", None)
        if dispatcher is None:
            return
        dispatcher.wait_for_idle(timeout=5.0)
        dispatcher.close(timeout=5.0)

    @staticmethod
    def _order_carryover_interval_seconds() -> float:
        return 5.0

    def _dispatch_indi_process_recreated_callback(self, status: dict[str, Any]) -> bool:
        if not bool(status.get("indi_process_restarted") or status.get("restarted")):
            return False
        with self._indi_recreated_callback_lock:
            if self._indi_recreated_callback_sent:
                return True
            self._indi_recreated_callback_sent = True
        message = str(
            status.get("indi_process_message")
            or status.get("message")
            or "GiExpertMain.exe generation changed"
        )
        details = {
            "process": "GiExpertMain.exe",
            "reason": message,
            "status": dict(status),
        }
        self._safe_scripter_call(
            "system_callback",
            self._INDI_RECREATED_EVENT_TYPE,
            self._INDI_RECREATED_MESSAGE,
            details,
        )
        self._safe_scripter_call(
            "log",
            "warning",
            "tools",
            f"{self._INDI_RECREATED_MESSAGE}: {message}",
            {"details": details},
        )
        return True

    def _indi_process_monitor_interval_seconds(self) -> float:
        raw_value = os.getenv(self._INDI_PROCESS_MONITOR_INTERVAL_ENV, "").strip()
        if not raw_value:
            return 5.0
        try:
            return max(float(raw_value), 1.0)
        except ValueError:
            ops_log(LogSource.MANAGE,
                f"Invalid {self._INDI_PROCESS_MONITOR_INTERVAL_ENV}={raw_value!r}; using 5 seconds",
            )
            self._safe_scripter_call(
                "log",
                "warning",
                "tools",
                f"Invalid {self._INDI_PROCESS_MONITOR_INTERVAL_ENV}; using 5 seconds",
                {"raw_value": raw_value},
            )
            return 5.0

    def _indi_event_pump_heartbeat_interval_seconds(self) -> float:
        return self._INDI_EVENT_PUMP_HEARTBEAT_INTERVAL_SECONDS

    def _safe_scripter_call(self, method_name: str, *args: Any, **kwargs: Any) -> None:
        try:
            method = getattr(self._scripter, method_name)
            method(*args, **kwargs)
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"Scripter {method_name} failed: {exc.__class__.__name__}: {exc}")
            write_crash_log(
                role="main",
                source=f"tools.scripter.{method_name}",
                message=f"Scripter {method_name} failed",
                exc=exc,
                log_dir=self._scripter_log_dir,
            )
            raise

    def _configure_system_callbacks(self, callbacks: list[dict[str, Any]]) -> None:
        self._safe_scripter_call("configure_system_callbacks", callbacks)

    def _record_system_event(
        self,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._safe_scripter_call("system_callback", event_type, message, details)

    def list_stocks(self) -> list[dict[str, Any]]:
        return [stock.to_dict() for stock in self._client.list_stocks()]

    def list_gold_products(self) -> list[dict[str, Any]]:
        return [product.to_dict() for product in self._client.list_gold_products()]

    def get_daily_prices(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return [price.to_dict() for price in self._client.get_daily_prices(code, start_date, end_date)]

    def get_stock_weekly_prices(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        prices = self._filter_daily_prices(
            self._client.get_daily_prices(code, start_date, end_date),
            start_date,
            end_date,
        )
        return self._build_weekly_price_rows(prices)

    def get_intraday_prices(
        self,
        code: str,
        date: str,
        interval_minutes: int = 5,
    ) -> list[dict[str, Any]]:
        return [price.to_dict() for price in self._client.get_intraday_prices(code, date, interval_minutes)]

    def get_gold_daily_prices(
        self,
        code: str = "M04020000",
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return [price.to_dict() for price in self._client.get_gold_daily_prices(code, start_date, end_date)]

    def get_gold_intraday_prices(
        self,
        code: str = "M04020000",
        date: str | None = None,
        interval_minutes: int = 5,
    ) -> list[dict[str, Any]]:
        if date is None:
            raise ValueError("date is required")
        return [price.to_dict() for price in self._client.get_gold_intraday_prices(code, date, interval_minutes)]

    def get_market_index_prices(
        self,
        start_date: str,
        end_date: str,
    ) -> dict[str, list[dict[str, Any]]]:
        return {
            index_id: [point.to_dict() for point in points]
            for index_id, points in self._client.get_market_index_prices(start_date, end_date).items()
        }

    def get_sector_index_prices(
        self,
        sector_code: str,
        start_date: str,
        end_date: str,
        interval: str = "D",
    ) -> list[dict[str, Any]]:
        return [
            point.to_dict()
            for point in self._client.get_sector_index_prices(sector_code, start_date, end_date, interval)
        ]

    def get_stock_sector_profile(self, code: str) -> dict[str, Any]:
        return self._client.get_stock_sector_profile(code)

    def get_stock_decision_indicator_context(self, code: str, date: str | None = None) -> dict[str, Any]:
        return self._holding_alerts.get_alert_indicator_context(code, date)

    def get_stock_market_environment_indicators(
        self,
        code: str,
        as_of_date: str | None = None,
        as_of_time: str | None = None,
    ) -> dict[str, Any]:
        normalized_code = self._client.normalize_stock_code(code)
        normalized_date = self._normalize_date_value(as_of_date) or self._today_kst()
        normalized_time = self._normalize_time_value(as_of_time)
        completed_end_date = self._completed_daily_end_date(normalized_date, normalized_time)
        start_52w = (datetime.strptime(completed_end_date, "%Y%m%d") - timedelta(days=370)).strftime("%Y%m%d")

        daily = sorted(
            self._filter_daily_prices(
                self._client.get_daily_prices(normalized_code, start_52w, completed_end_date),
                start_52w,
                completed_end_date,
            ),
            key=lambda item: self._normalize_date_value(item.date) or item.date,
        )
        return self._stock_market_environment_indicators_from_daily(
            normalized_code,
            normalized_date,
            normalized_time,
            completed_end_date,
            daily,
        )

    def _stock_market_environment_indicators_from_daily(
        self,
        normalized_code: str,
        normalized_date: str,
        normalized_time: str | None,
        completed_end_date: str,
        daily: list[DailyPrice],
    ) -> dict[str, Any]:
        start_market = (datetime.strptime(completed_end_date, "%Y%m%d") - timedelta(days=180)).strftime("%Y%m%d")
        stock_config = self._holding_alerts._stock_config(normalized_code)
        market = self._market_environment_context(stock_config, start_market, completed_end_date)
        sector_profile = self._client.get_stock_sector_profile(normalized_code)
        sector = self._sector_environment_context(sector_profile, start_market, completed_end_date)
        return {
            "code": normalized_code,
            "as_of_date": normalized_date,
            "as_of_time": normalized_time,
            "completed_daily_end_date": completed_end_date,
            "backtest_policy": {
                "daily_bars": "uses bars through completed_daily_end_date only",
                "intraday_cutoff": "not used by this tool; pass as_of_time to avoid using an unfinished daily bar",
                "current_quote_snapshot": "not used",
            },
            "market": market,
            "sector": sector,
            "relative_strength": self._relative_strength_from_daily(daily, market),
            "trading_value": self._trading_value_from_daily(daily),
            "high_52w": self._high_52w_from_daily(daily),
            "fx": self._market_index_environment_context("usdkrw", completed_end_date)
            if self._is_overseas_etf_config(stock_config)
            else {"source": "not_applicable"},
            "overseas": self._market_index_environment_context(
                str(stock_config.get("overseas_index") or "nasdaq"),
                completed_end_date,
            )
            if self._is_overseas_etf_config(stock_config)
            else {"source": "not_applicable"},
            "sector_profile": sector_profile,
        }

    def refresh_decision_baselines(
        self,
        account_no: str | None = None,
        code: str | None = None,
        date: str | None = None,
    ) -> dict[str, Any]:
        return self._holding_alerts.refresh_decision_baselines(account_no, code, date)

    def get_decision_baseline_cache(
        self,
        account_no: str | None = None,
        code: str | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        return self._holding_alerts.get_decision_baseline_cache(account_no, code)

    def get_alert_indicator_context(self, code: str, date: str | None = None) -> dict[str, Any]:
        return self._holding_alerts.get_alert_indicator_context(code, date)

    def calculate_trade_size(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._holding_alerts.calculate_trade_size(payload)

    def run_holding_alert_scan(self, account_no: str, dry_run: bool = True) -> dict[str, Any]:
        return self._holding_alerts.run_scan(account_no, dry_run)

    def run_alert_validation(self, account_no: str, lookback_trading_days: int = 60) -> dict[str, Any]:
        return self._holding_alerts.run_validation(account_no, lookback_trading_days)

    def register_holding_alert_runner(
        self,
        accountNo: str | None = None,
        httpCallback: dict[str, Any] | None = None,
        heldCode: list[str] | str | None = None,
        wannaCode: list[str] | str | None = None,
        code: list[str] | str | None = None,
        dry_run: bool = False,
        account_no: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(heldCode, bool):
            dry_run = heldCode
            heldCode = None
        resolved_account = accountNo or account_no
        if not resolved_account:
            raise ValueError("accountNo is required")
        if httpCallback is None:
            raise ValueError("httpCallback is required")
        if heldCode is None and code is not None:
            heldCode = code
        return self._holding_alerts.register_runner(
            resolved_account,
            self._parse_http_callback(httpCallback),
            held_code=heldCode,
            wanna_code=wannaCode,
            dry_run=dry_run,
        )

    def list_holding_alert_runners(self) -> list[dict[str, Any]]:
        return self._holding_alerts.list_runners()

    def cancel_holding_alert_runner(self, runner_id: str) -> dict[str, Any]:
        return self._holding_alerts.cancel_runner(runner_id)

    def subscribe_realtime_price(self, code: str) -> dict[str, object]:
        try:
            return self._client.subscribe_realtime_price(code)
        except NotImplementedError as exc:
            return {
                "subscribed": False,
                "code": code,
                "rt_type": "UC",
                "already_subscribed": False,
                "message": str(exc),
            }

    def unsubscribe_realtime_price(self, code: str) -> dict[str, object]:
        try:
            return self._client.unsubscribe_realtime_price(code)
        except NotImplementedError as exc:
            return {
                "subscribed": False,
                "code": code,
                "rt_type": "UC",
                "was_subscribed": False,
                "remaining_subscriptions": 0,
                "message": str(exc),
            }

    def subscribe_disclosure(
        self,
        code: str,
        httpCallback: dict[str, Any],
        devCallback: bool = False,
    ) -> dict[str, Any]:
        return self._runtime_state.subscribe_disclosure(code, self._parse_http_callback(httpCallback), devCallback)

    def unsubscribe_disclosure(self, subscription_id: str) -> dict[str, Any]:
        return self._runtime_state.unsubscribe_disclosure(subscription_id)

    def list_disclosure_subscriptions(self) -> list[dict[str, Any]]:
        return self._runtime_state.list_disclosure_subscriptions()

    def subscribe_news(
        self,
        types: list[str],
        httpCallback: dict[str, Any],
        code: str | None = None,
        devCallback: bool = False,
    ) -> dict[str, Any]:
        return self._runtime_state.subscribe_news(types, self._parse_http_callback(httpCallback), code, devCallback)

    def unsubscribe_news(self, subscription_id: str) -> dict[str, Any]:
        return self._runtime_state.unsubscribe_news(subscription_id)

    def list_news_subscriptions(self) -> list[dict[str, Any]]:
        return self._runtime_state.list_news_subscriptions()

    def register_system_callback(self, httpCallback: dict[str, Any]) -> dict[str, Any]:
        parsed = self._parse_http_callback(httpCallback)
        return self._runtime_state.register_system_callback(parsed)

    def list_system_callbacks(self) -> list[dict[str, Any]]:
        return self._runtime_state.list_system_callbacks()

    def unregister_system_callback(self, system_callback_id: str) -> dict[str, Any]:
        return self._runtime_state.unregister_system_callback(system_callback_id)

    def register_price_alert(
        self,
        code: str,
        condition: str,
        threshold: float,
        window_minutes: int | None = None,
        message: str = "",
        httpCallback: dict[str, Any] | None = None,
        debounce_seconds: float | None = None,
        once_only: bool = False,
    ) -> dict[str, Any]:
        if httpCallback is None:
            raise ValueError("httpCallback is required")
        return self._runtime_state.register_price_alert(
            code,
            condition,
            threshold,
            window_minutes,
            message,
            self._parse_http_callback(httpCallback),
            debounce_seconds,
            once_only,
        )

    def list_price_alerts(self) -> list[dict[str, Any]]:
        return self._runtime_state.list_price_alerts()

    def cancel_price_alert(self, alert_id: str | None = None, code: str | None = None) -> dict[str, Any]:
        return self._runtime_state.cancel_price_alert(alert_id, code)

    def register_recovery_fail_alert(
        self,
        code: str,
        breach_price: float,
        recovery_price: float,
        failure_minutes: float = 3,
        recovery_minutes: float = 3,
        valid_after: str = "11:00",
        httpCallback: dict[str, Any] | None = None,
        once_only: bool = True,
    ) -> dict[str, Any]:
        if httpCallback is None:
            raise ValueError("httpCallback is required")
        return self._runtime_state.register_recovery_fail_alert(
            code,
            breach_price,
            recovery_price,
            failure_minutes,
            recovery_minutes,
            valid_after,
            self._parse_http_callback(httpCallback),
            once_only,
        )

    def register_uptrend_end_alert(
        self,
        code: str,
        start_price: float,
        end_price: float,
        end_minutes: float = 3,
        valid_after: str = "09:00",
        httpCallback: dict[str, Any] | None = None,
        once_only: bool = True,
    ) -> dict[str, Any]:
        if httpCallback is None:
            raise ValueError("httpCallback is required")
        return self._runtime_state.register_uptrend_end_alert(
            code,
            start_price,
            end_price,
            end_minutes,
            valid_after,
            self._parse_http_callback(httpCallback),
            once_only,
        )

    def register_stock_price_callback(
        self,
        code: str,
        step: float,
        httpCallback: dict[str, Any] | None = None,
        price_filter: str | None = None,
    ) -> dict[str, Any]:
        if httpCallback is None:
            raise ValueError("httpCallback is required")
        return self._runtime_state.register_stock_price_callback(
            code,
            step,
            self._parse_http_callback(httpCallback),
            price_filter,
        )

    def list_stock_price_callbacks(self) -> list[dict[str, Any]]:
        return self._runtime_state.list_stock_price_callbacks()

    def cancel_stock_price_callback(
        self,
        stock_price_callback_id: str | None = None,
        code: str | None = None,
    ) -> dict[str, Any]:
        return self._runtime_state.cancel_stock_price_callback(stock_price_callback_id, code)

    def register_gold_price_alert(
        self,
        code: str,
        condition: str,
        threshold: float,
        window_minutes: int | None = None,
        message: str = "",
        httpCallback: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if httpCallback is None:
            raise ValueError("httpCallback is required")
        return self._gold_runtime_state.register_gold_price_alert(
            code,
            condition,
            threshold,
            window_minutes,
            message,
            self._parse_http_callback(httpCallback),
        )

    def list_gold_price_alerts(self) -> list[dict[str, Any]]:
        return self._gold_runtime_state.list_gold_price_alerts()

    def cancel_gold_price_alert(self, alert_id: str | None = None, code: str | None = None) -> dict[str, Any]:
        return self._gold_runtime_state.cancel_gold_price_alert(alert_id, code)

    def register_gold_price_callback(
        self,
        code: str,
        step: float,
        httpCallback: dict[str, Any] | None = None,
        price_filter: str | None = None,
    ) -> dict[str, Any]:
        if httpCallback is None:
            raise ValueError("httpCallback is required")
        return self._gold_runtime_state.register_gold_price_callback(
            code,
            step,
            self._parse_http_callback(httpCallback),
            price_filter,
        )

    def list_gold_price_callbacks(self) -> list[dict[str, Any]]:
        return self._gold_runtime_state.list_gold_price_callbacks()

    def cancel_gold_price_callback(
        self,
        gold_price_callback_id: str | None = None,
        code: str | None = None,
    ) -> dict[str, Any]:
        return self._gold_runtime_state.cancel_gold_price_callback(gold_price_callback_id, code)

    def register_fall_safe(
        self,
        account_no: str,
        code: str,
        trigger_price: float,
        quantity: int,
        httpCallback: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        parsed = self._parse_http_callback(httpCallback) if httpCallback is not None else None
        return self._runtime_state.register_fall_safe(account_no, code, trigger_price, quantity, parsed)

    def list_fall_safes(self) -> list[dict[str, Any]]:
        return self._runtime_state.list_fall_safes()

    def cancel_fall_safe(self, fall_safe_id: str) -> dict[str, Any]:
        return self._runtime_state.cancel_fall_safe(fall_safe_id)

    def get_accounts(self) -> list[dict[str, Any]]:
        return [account.to_dict() for account in self._client.get_accounts()]

    def get_account_summary(self, account_no: str) -> dict[str, Any]:
        return self._client.get_account_summary(account_no).to_dict()

    def get_gold_account_balance(self, account_no: str) -> dict[str, Any]:
        return self._client.get_gold_account_balance(account_no).to_dict()

    def get_fundamentals(
        self,
        code: str,
        consolidated: bool = True,
        quarterly: bool = True,
    ) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_fundamentals(code, consolidated, quarterly)]

    def get_quote_snapshot(self, code: str) -> dict[str, Any]:
        return self._client.get_quote_snapshot(code).to_dict()

    def get_gold_quote_snapshot(self, code: str = "M04020000") -> dict[str, Any]:
        return self._client.get_gold_quote_snapshot(code).to_dict()

    def get_investor_flow_by_stock(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_investor_flow_by_stock(code, start_date, end_date)]

    def get_market_investor_flow_intraday(
        self,
        include_institution_breakdown: bool = False,
    ) -> list[dict[str, Any]]:
        return [
            item.to_dict()
            for item in self._client.get_market_investor_flow_intraday(include_institution_breakdown)
        ]

    def get_foreign_flow_rankings(
        self,
        market: str = "all",
        consecutive_days: int = 3,
        direction: str = "buy",
    ) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_foreign_flow_rankings(market, consecutive_days, direction)]

    def get_top_movers(
        self,
        market: str = "all",
        direction: str = "up",
        date: str | None = None,
        limit: int = 100,
        kospi200_only: bool = False,
    ) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_top_movers(market, direction, date, limit, kospi200_only)]

    def list_stock_news(self, code: str, date: str | None = None) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.list_stock_news(code, date)]

    def list_market_flow_news(
        self,
        date: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.list_market_flow_news(date, from_time, to_time)]

    def get_news_content(self, news_type: str, date: str, article_id: str) -> dict[str, Any]:
        return self._client.get_news_content(news_type, date, article_id).to_dict()

    def get_disclosure_content(self, rcpNo: str) -> dict[str, Any]:
        normalized = self._normalize_dart_rcp_no(rcpNo)
        return self._get_disclosure_content_by_rcp_no(normalized)

    def get_disclosure_content_from_article(
        self,
        date: str,
        article_id: str,
        news_type: str = "5",
    ) -> dict[str, Any]:
        normalized_date = self._normalize_news_article_date(date)
        article = str(article_id or "").strip()
        if not article:
            raise ValueError("article_id is required for DART disclosure article queries")

        resolved_news_type = str(news_type or "5").strip()
        news = self._client.get_news_content(resolved_news_type, normalized_date, article)
        raw_html = str(getattr(news, "raw_html", "") or "").strip()
        rcp_no = str(getattr(news, "rcpNo", "") or "").strip()
        if looks_like_disclosure_body_html(raw_html, news_type=resolved_news_type):
            # Full disclosure bodies can contain related-disclosure links; those
            # extracted receipt numbers are not necessarily the current article.
            return DisclosureContent(
                rcpNo=None,
                content=raw_html,
                content_format="html",
                source="news_raw_html",
                viewer_url="",
                dtd=None,
                print_page_break_selector=disclosure_content_split_selector(raw_html),
            ).to_dict()
        if not rcp_no:
            raise ValueError("rcpNo could not be found in the news article body")
        normalized = self._normalize_dart_rcp_no(rcp_no)
        return self._get_disclosure_content_by_rcp_no(normalized)

    @staticmethod
    def _get_disclosure_content_by_rcp_no(rcpNo: str) -> dict[str, Any]:
        document = disclosure_to_html(rcpNo)
        return DisclosureContent(
            rcpNo=rcpNo,
            content=document.content.strip(),
            content_format="html",
            source=document.source,
            viewer_url=document.viewer_url,
            dtd=document.dtd,
            print_page_break_selector=document.print_page_break_selector,
        ).to_dict()

    @staticmethod
    def _normalize_dart_rcp_no(rcpNo: str) -> str:
        normalized = str(rcpNo or "").strip()
        if not normalized.isdigit() or len(normalized) != 14:
            raise ValueError("rcpNo must be a 14-digit DART receipt number")
        return normalized

    @staticmethod
    def _normalize_news_article_date(date: str | None) -> str:
        normalized = re.sub(r"\D", "", str(date or "").strip())
        if len(normalized) != 8:
            raise ValueError("date must be YYYYMMDD when article_id is used for DART disclosure queries")
        return normalized

    def get_volume_surge(
        self,
        market: str = "all",
        limit: int = 100,
        kospi200_only: bool = False,
    ) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_volume_surge(market, limit, kospi200_only)]

    def get_new_highs_lows(
        self,
        market: str = "all",
        mode: str = "new_high",
        limit: int = 100,
        kospi200_only: bool = False,
    ) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_new_highs_lows(market, mode, limit, kospi200_only)]

    def get_limit_hits(
        self,
        market: str = "all",
        mode: str = "upper",
        kospi200_only: bool = False,
    ) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_limit_hits(market, mode, kospi200_only)]

    def get_order_book(self, code: str) -> dict[str, Any]:
        try:
            return self._client.get_order_book(code).to_dict()
        except NotImplementedError as exc:
            return {
                "code": code,
                "received_at": "",
                "market_phase": "unavailable",
                "levels": [],
                "source": "",
                "partial": False,
                "available": False,
                "message": str(exc),
            }

    def get_gold_order_book(self, code: str = "M04020000") -> dict[str, Any]:
        try:
            return self._client.get_gold_order_book(code).to_dict()
        except NotImplementedError as exc:
            return {
                "code": code,
                "received_at": "",
                "market_phase": "unavailable",
                "levels": [],
                "source": "",
                "partial": False,
                "available": False,
                "message": str(exc),
            }

    def subscribe_gold_realtime_price(self, code: str = "M04020000") -> dict[str, object]:
        try:
            return self._client.subscribe_gold_realtime_price(code)
        except NotImplementedError as exc:
            return {
                "subscribed": False,
                "code": code,
                "rt_type": "XC",
                "already_subscribed": False,
                "message": str(exc),
            }

    def unsubscribe_gold_realtime_price(self, code: str = "M04020000") -> dict[str, object]:
        try:
            return self._client.unsubscribe_gold_realtime_price(code)
        except NotImplementedError as exc:
            return {
                "subscribed": False,
                "code": code,
                "rt_type": "XC",
                "was_subscribed": False,
                "remaining_subscriptions": 0,
                "message": str(exc),
            }

    def get_cash_order_book_snapshot(self, code: str) -> dict[str, Any]:
        return self._holding_alerts.get_cash_order_book_snapshot(code)

    def get_technical_indicators(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        warmup_start_date = self._expanded_start_date(start_date, self._TECHNICAL_WARMUP_DAYS)
        prices = self._filter_daily_prices(
            self._client.get_daily_prices(code, warmup_start_date, end_date),
            warmup_start_date,
            end_date,
        )
        return self._daily_technical_indicator_rows(prices, start_date, end_date)

    def _daily_technical_indicator_rows(
        self,
        prices: list[DailyPrice],
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        indicators = build_technical_indicators(prices)
        if start_date is None and end_date is None:
            return indicators
        normalized_start = self._normalize_date_value(start_date)
        normalized_end = self._normalize_date_value(end_date)
        return [
            row
            for row in indicators
            if self._date_in_range(str(row["date"]), normalized_start, normalized_end)
        ]

    def _weekly_technical_indicator_rows(
        self,
        weekly_rows: list[dict[str, Any]],
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        weekly_items = self._weekly_rows_to_daily_prices(weekly_rows)
        indicators = build_technical_indicators(weekly_items)
        metadata_by_date = {str(row["end_date"]): row for row in weekly_rows}
        normalized_start = self._normalize_date_value(start_date)
        normalized_end = self._normalize_date_value(end_date)
        result: list[dict[str, Any]] = []
        for row in indicators:
            row_date = str(row["date"])
            if not self._date_in_range(row_date, normalized_start, normalized_end):
                continue
            metadata = metadata_by_date.get(row_date, {})
            result.append({**metadata, **row, "date": row_date})
        return result

    def _intraday_technical_indicator_rows(
        self,
        intraday: list[IntradayPrice],
        interval_minutes: int,
        as_of_time: str | None,
    ) -> list[dict[str, Any]]:
        indicator_items = [
            DailyPrice(
                date=f"{item.date}{item.time}",
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
            )
            for item in intraday
        ]
        indicators = build_technical_indicators(indicator_items)
        metadata_by_timestamp = {
            f"{item.date}{item.time}": {
                "date": item.date,
                "time": item.time,
                "timestamp": f"{item.date}{item.time}",
                "open": item.open,
                "high": item.high,
                "low": item.low,
                "close": item.close,
                "volume": item.volume,
            }
            for item in intraday
        }
        vwap_by_timestamp = self._intraday_vwap_by_timestamp(intraday)
        volume_ratio_by_timestamp = self._intraday_volume_ratio_by_timestamp(intraday)
        result: list[dict[str, Any]] = []
        for row in indicators:
            timestamp = str(row["date"])
            metadata = metadata_by_timestamp.get(timestamp)
            if metadata is None:
                continue
            result.append(
                {
                    **row,
                    **metadata,
                    "vwap": vwap_by_timestamp.get(timestamp),
                    "session_volume_ratio": volume_ratio_by_timestamp.get(timestamp),
                    "interval_minutes": interval_minutes,
                    "as_of_time": as_of_time,
                }
            )
        return result

    @staticmethod
    def _intraday_prices_to_daily_like_rows(intraday: list[IntradayPrice]) -> list[DailyPrice]:
        return [
            DailyPrice(
                date=f"{item.date}{item.time}",
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
            )
            for item in intraday
        ]

    def get_stock_technical_indicators_daily(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.get_technical_indicators(code, start_date, end_date)

    def get_stock_technical_indicators_weekly(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        warmup_start_date = self._expanded_start_date(start_date, self._TECHNICAL_WARMUP_DAYS * 7)
        prices = self._filter_daily_prices(
            self._client.get_daily_prices(code, warmup_start_date, end_date),
            warmup_start_date,
            end_date,
        )
        weekly_rows = self._build_weekly_price_rows(prices)
        return self._weekly_technical_indicator_rows(weekly_rows, start_date, end_date)

    def get_stock_technical_indicators_intraday(
        self,
        code: str,
        date: str,
        interval_minutes: int = 5,
        as_of_time: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_date = self._normalize_date_value(date)
        if normalized_date is None:
            raise ValueError("date must be YYYYMMDD or YYYY-MM-DD")
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be greater than zero")
        normalized_time = self._normalize_time_value(as_of_time)
        intraday = self._filtered_intraday_prices(
            [
                item
                for item in self._client.get_intraday_prices(code, normalized_date, interval_minutes)
                if self._normalize_date_value(item.date) == normalized_date
            ],
            normalized_time,
        )
        return self._intraday_technical_indicator_rows(intraday, interval_minutes, normalized_time)

    def get_stock_chart_pattern_candidates(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        lookback_days: int = 120,
    ) -> list[dict[str, Any]]:
        if lookback_days <= 0:
            raise ValueError("lookback_days must be greater than zero")
        prices = self._filter_daily_prices(
            self._client.get_daily_prices(code, start_date, end_date),
            start_date,
            end_date,
        )
        return detect_chart_patterns(prices, lookback_days=lookback_days)

    def get_stock_chart_patterns_daily(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        lookback_days: int = 120,
    ) -> list[dict[str, Any]]:
        return self.get_stock_chart_pattern_candidates(code, start_date, end_date, lookback_days)

    def get_stock_chart_patterns_weekly(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        lookback_weeks: int = 120,
    ) -> list[dict[str, Any]]:
        if lookback_weeks <= 0:
            raise ValueError("lookback_weeks must be greater than zero")
        prices = self._filter_daily_prices(
            self._client.get_daily_prices(code, start_date, end_date),
            start_date,
            end_date,
        )
        weekly_items = self._weekly_rows_to_daily_prices(self._build_weekly_price_rows(prices))
        return detect_chart_patterns(weekly_items, lookback_days=lookback_weeks)

    def get_stock_chart_patterns_intraday(
        self,
        code: str,
        date: str,
        interval_minutes: int = 5,
        as_of_time: str | None = None,
        lookback_bars: int = 120,
    ) -> list[dict[str, Any]]:
        normalized_date = self._normalize_date_value(date)
        if normalized_date is None:
            raise ValueError("date must be YYYYMMDD or YYYY-MM-DD")
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be greater than zero")
        if lookback_bars <= 0:
            raise ValueError("lookback_bars must be greater than zero")
        normalized_time = self._normalize_time_value(as_of_time)
        intraday = self._filtered_intraday_prices(
            [
                item
                for item in self._client.get_intraday_prices(code, normalized_date, interval_minutes)
                if self._normalize_date_value(item.date) == normalized_date
            ],
            normalized_time,
        )
        pattern_items = self._intraday_prices_to_daily_like_rows(intraday)
        return detect_chart_patterns(pattern_items, lookback_days=lookback_bars)

    def get_stock_technical_analysis_bundle(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        as_of_time: str | None = None,
        include_intraday: bool = True,
        intraday_interval_minutes: int = 5,
        lookback_days: int = 180,
        lookback_weeks: int = 120,
        lookback_bars: int = 120,
    ) -> dict[str, Any]:
        return self._stock_technical_analysis_bundle(
            code=code,
            start_date=start_date,
            end_date=end_date,
            as_of_time=as_of_time,
            include_intraday=include_intraday,
            intraday_interval_minutes=intraday_interval_minutes,
            lookback_days=lookback_days,
            lookback_weeks=lookback_weeks,
            lookback_bars=lookback_bars,
            live=False,
        )

    def get_stock_technical_analysis_bundle_live(
        self,
        code: str,
        start_date: str | None = None,
        date: str | None = None,
        include_intraday: bool = True,
        intraday_interval_minutes: int = 5,
        lookback_days: int = 180,
        lookback_weeks: int = 120,
        lookback_bars: int = 120,
        include_quote_snapshot: bool = True,
        include_order_book: bool = True,
        include_news: bool = True,
        news_limit: int = 20,
        include_investor_flow: bool = True,
        include_fundamentals: bool = True,
        include_holding_alert_context: bool = True,
    ) -> dict[str, Any]:
        if news_limit < 0:
            raise ValueError("news_limit must be zero or greater")
        result = self._stock_technical_analysis_bundle(
            code=code,
            start_date=start_date,
            end_date=date,
            as_of_time=None,
            include_intraday=include_intraday,
            intraday_interval_minutes=intraday_interval_minutes,
            lookback_days=lookback_days,
            lookback_weeks=lookback_weeks,
            lookback_bars=lookback_bars,
            live=True,
        )
        normalized_code = str(result["code"])
        normalized_date = str(result["end_date"])
        data_status = result["data_status"]
        live_context: dict[str, Any] = {}

        if include_quote_snapshot:
            live_context["quote_snapshot"] = self._context_component(
                data_status,
                "quote_snapshot",
                lambda: self.get_quote_snapshot(normalized_code),
            )
        else:
            data_status["quote_snapshot"] = {"status": "skipped"}

        if include_order_book:
            live_context["order_book"] = self._context_component(
                data_status,
                "order_book",
                lambda: self.get_order_book(normalized_code),
            )
        else:
            data_status["order_book"] = {"status": "skipped"}

        if include_news:
            news_headlines = self._context_component(
                data_status,
                "news_headlines",
                lambda: self.list_stock_news(normalized_code, normalized_date)[:news_limit],
            )
            live_context["news_headlines"] = news_headlines if isinstance(news_headlines, list) else []
        else:
            data_status["news_headlines"] = {"status": "skipped"}
            live_context["news_headlines"] = []

        if include_investor_flow:
            investor_flow = self._context_component(
                data_status,
                "investor_flow",
                lambda: self.get_investor_flow_by_stock(normalized_code, str(result["start_date"]), normalized_date),
            )
            live_context["investor_flow"] = investor_flow if isinstance(investor_flow, list) else []
        else:
            data_status["investor_flow"] = {"status": "skipped"}
            live_context["investor_flow"] = []

        if include_fundamentals:
            fundamentals = self._context_component(
                data_status,
                "fundamentals",
                lambda: self.get_fundamentals(normalized_code),
            )
            live_context["fundamentals"] = fundamentals if isinstance(fundamentals, list) else []
        else:
            data_status["fundamentals"] = {"status": "skipped"}
            live_context["fundamentals"] = []

        if include_holding_alert_context:
            live_context["holding_alert_indicator_context"] = self._context_component(
                data_status,
                "holding_alert_indicator_context",
                lambda: self.get_stock_decision_indicator_context(normalized_code, normalized_date),
            )
        else:
            data_status["holding_alert_indicator_context"] = {"status": "skipped"}

        result["live_context"] = live_context
        return result

    def _stock_technical_analysis_bundle(
        self,
        *,
        code: str,
        start_date: str | None,
        end_date: str | None,
        as_of_time: str | None,
        include_intraday: bool,
        intraday_interval_minutes: int,
        lookback_days: int,
        lookback_weeks: int,
        lookback_bars: int,
        live: bool,
    ) -> dict[str, Any]:
        if lookback_days <= 0:
            raise ValueError("lookback_days must be greater than zero")
        if lookback_weeks <= 0:
            raise ValueError("lookback_weeks must be greater than zero")
        if lookback_bars <= 0:
            raise ValueError("lookback_bars must be greater than zero")
        if intraday_interval_minutes <= 0:
            raise ValueError("intraday_interval_minutes must be greater than zero")

        normalized_code = self._client.normalize_stock_code(code)
        normalized_end = self._normalize_date_value(end_date) or self._today_kst()
        normalized_time = None if live else self._normalize_time_value(as_of_time)
        completed_end_date = normalized_end if live else self._completed_daily_end_date(normalized_end, normalized_time)
        normalized_start = self._normalize_date_value(start_date)
        if normalized_start is None:
            normalized_start = (
                datetime.strptime(completed_end_date, "%Y%m%d") - timedelta(days=lookback_days)
            ).strftime("%Y%m%d")
        warmup_start_date = (
            datetime.strptime(normalized_start, "%Y%m%d") - timedelta(days=self._TECHNICAL_WARMUP_DAYS * 7)
        ).strftime("%Y%m%d")

        data_status: dict[str, Any] = {}
        all_daily_price_items = self._context_component(
            data_status,
            "daily_prices",
            lambda: self._client.get_daily_prices(normalized_code, warmup_start_date, completed_end_date),
        )
        if not isinstance(all_daily_price_items, list):
            all_daily_price_items = []
        all_daily_price_items = sorted(
            [
                item
                for item in all_daily_price_items
                if isinstance(item, DailyPrice) and self._date_in_range(item.date, warmup_start_date, completed_end_date)
            ],
            key=lambda item: self._normalize_date_value(item.date) or item.date,
        )
        daily_price_items = [
            item
            for item in all_daily_price_items
            if self._date_in_range(item.date, normalized_start, completed_end_date)
        ]
        daily_prices = [item.to_dict() for item in daily_price_items]
        if data_status.get("daily_prices", {}).get("status") != "unavailable":
            data_status["daily_prices"] = {
                "status": "empty" if not daily_prices else "available",
                "count": len(daily_prices),
                "source_count": len(all_daily_price_items),
            }

        weekly_rows_all = self._context_component(
            data_status,
            "weekly_prices",
            lambda: self._build_weekly_price_rows(all_daily_price_items),
        )
        if not isinstance(weekly_rows_all, list):
            weekly_rows_all = []
        weekly_rows = [
            row
            for row in weekly_rows_all
            if self._date_in_range(str(row.get("end_date") or ""), normalized_start, completed_end_date)
        ]
        if data_status.get("weekly_prices", {}).get("status") != "unavailable":
            data_status["weekly_prices"] = {
                "status": "empty" if not weekly_rows else "available",
                "count": len(weekly_rows),
                "source_count": len(weekly_rows_all),
            }

        daily_technical_indicators = self._context_component(
            data_status,
            "daily_technical_indicators",
            lambda: self._daily_technical_indicator_rows(
                all_daily_price_items,
                normalized_start,
                completed_end_date,
            ),
        )
        weekly_technical_indicators = self._context_component(
            data_status,
            "weekly_technical_indicators",
            lambda: self._weekly_technical_indicator_rows(
                weekly_rows_all,
                normalized_start,
                completed_end_date,
            ),
        )
        daily_chart_patterns = self._context_component(
            data_status,
            "daily_chart_pattern_candidates",
            lambda: detect_chart_patterns(daily_price_items, lookback_days=lookback_days),
        )
        weekly_chart_patterns = self._context_component(
            data_status,
            "weekly_chart_pattern_candidates",
            lambda: detect_chart_patterns(
                self._weekly_rows_to_daily_prices(weekly_rows),
                lookback_days=lookback_weeks,
            ),
        )
        market_environment = self._context_component(
            data_status,
            "market_environment_indicators",
            lambda: self._stock_market_environment_indicators_from_daily(
                normalized_code,
                normalized_end,
                normalized_time,
                completed_end_date,
                all_daily_price_items,
            ),
        )

        intraday_prices: list[dict[str, Any]] = []
        intraday_technical_indicators: list[dict[str, Any]] = []
        intraday_chart_patterns: list[dict[str, Any]] = []
        if include_intraday:
            intraday_items = self._context_component(
                data_status,
                "intraday_prices",
                lambda: self._filtered_intraday_prices(
                    [
                        item
                        for item in self._client.get_intraday_prices(
                            normalized_code,
                            normalized_end,
                            intraday_interval_minutes,
                        )
                        if self._normalize_date_value(item.date) == normalized_end
                    ],
                    normalized_time,
                ),
            )
            if not isinstance(intraday_items, list):
                intraday_items = []
            intraday_prices = [item.to_dict() for item in intraday_items if isinstance(item, IntradayPrice)]
            if data_status.get("intraday_prices", {}).get("status") != "unavailable":
                data_status["intraday_prices"] = {
                    "status": "empty" if not intraday_prices else "available",
                    "count": len(intraday_prices),
                }
            intraday_technical_indicators = self._context_component(
                data_status,
                "intraday_technical_indicators",
                lambda: self._intraday_technical_indicator_rows(
                    [item for item in intraday_items if isinstance(item, IntradayPrice)],
                    intraday_interval_minutes,
                    normalized_time,
                ),
            )
            intraday_chart_patterns = self._context_component(
                data_status,
                "intraday_chart_pattern_candidates",
                lambda: detect_chart_patterns(
                    self._intraday_prices_to_daily_like_rows(
                        [item for item in intraday_items if isinstance(item, IntradayPrice)]
                    ),
                    lookback_days=lookback_bars,
                ),
            )
        else:
            data_status["intraday_prices"] = {"status": "skipped"}
            data_status["intraday_technical_indicators"] = {"status": "skipped"}
            data_status["intraday_chart_pattern_candidates"] = {"status": "skipped"}

        mode = "live_not_backtest_safe" if live else "backtest_safe"
        backtest_policy = (
            {
                "mode": mode,
                "daily_bars": "uses latest backend daily bars through end_date; rows may include current-session or revised data",
                "intraday_cutoff": "no as_of_time cutoff is applied by this bundle; it uses the latest intraday rows returned by the backend",
                "current_quote_snapshot": "included in live_context when requested",
                "news": "included in live_context when requested",
                "order_book": "included in live_context when requested",
                "suitable_for_backtesting": False,
            }
            if live
            else {
                "mode": mode,
                "daily_bars": "uses daily bars through completed_daily_end_date only",
                "intraday_cutoff": "if as_of_time is provided, intraday bars after that time are excluded",
                "current_quote_snapshot": "not used",
                "news": "not used",
                "order_book": "not used",
                "suitable_for_backtesting": True,
            }
        )

        return {
            "code": normalized_code,
            "mode": mode,
            "start_date": normalized_start,
            "end_date": normalized_end,
            "as_of_time": normalized_time,
            "completed_daily_end_date": completed_end_date,
            "lookback_days": lookback_days,
            "lookback_weeks": lookback_weeks,
            "lookback_bars": lookback_bars,
            "backtest_policy": backtest_policy,
            "price_bars": {
                "daily": daily_prices,
                "weekly": weekly_rows,
                "intraday": intraday_prices,
            },
            "technical_indicators": {
                "daily": daily_technical_indicators,
                "weekly": weekly_technical_indicators,
                "intraday": intraday_technical_indicators,
            },
            "chart_patterns": {
                "daily": daily_chart_patterns,
                "weekly": weekly_chart_patterns,
                "intraday": intraday_chart_patterns,
            },
            "market_environment_indicators": market_environment,
            "data_status": data_status,
        }

    def get_stock_analysis_context(
        self,
        code: str,
        end_date: str | None = None,
        lookback_days: int = 180,
        include_intraday: bool = True,
        intraday_interval_minutes: int = 5,
        include_news: bool = True,
        news_limit: int = 20,
    ) -> dict[str, Any]:
        if lookback_days <= 0:
            raise ValueError("lookback_days must be greater than zero")
        if intraday_interval_minutes <= 0:
            raise ValueError("intraday_interval_minutes must be greater than zero")
        if news_limit < 0:
            raise ValueError("news_limit must be zero or greater")

        normalized_end = self._normalize_date_value(end_date) or self._today_kst()
        start_date = (datetime.strptime(normalized_end, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")
        warmup_start_date = (
            datetime.strptime(normalized_end, "%Y%m%d") - timedelta(days=max(lookback_days, self._TECHNICAL_WARMUP_DAYS))
        ).strftime("%Y%m%d")
        data_status: dict[str, Any] = {}

        all_daily_price_items = self._context_component(
            data_status,
            "daily_prices",
            lambda: self._client.get_daily_prices(code, warmup_start_date, normalized_end),
        )
        if not isinstance(all_daily_price_items, list):
            all_daily_price_items = []
        all_daily_price_items = [
            item
            for item in all_daily_price_items
            if isinstance(item, DailyPrice) and self._date_in_range(item.date, warmup_start_date, normalized_end)
        ]
        daily_price_items = [
            item
            for item in all_daily_price_items
            if self._date_in_range(item.date, start_date, normalized_end)
        ]
        daily_prices = [item.to_dict() for item in daily_price_items]
        if data_status.get("daily_prices", {}).get("status") != "unavailable":
            data_status["daily_prices"] = {
                "status": "empty" if not daily_prices else "available",
                "count": len(daily_prices),
                "source_count": len(all_daily_price_items),
            }
        weekly_prices = self._context_component(
            data_status,
            "weekly_prices",
            lambda: self._build_weekly_price_rows(daily_price_items),
        )
        daily_technical_indicators = self._context_component(
            data_status,
            "daily_technical_indicators",
            lambda: [
                row
                for row in build_technical_indicators(all_daily_price_items)
                if self._date_in_range(str(row["date"]), start_date, normalized_end)
            ],
        )
        chart_pattern_candidates = self._context_component(
            data_status,
            "chart_pattern_candidates",
            lambda: detect_chart_patterns(daily_price_items, lookback_days=lookback_days),
        )
        quote_snapshot = self._context_component(
            data_status,
            "quote_snapshot",
            lambda: self.get_quote_snapshot(code),
        )
        decision_indicator_context = self._context_component(
            data_status,
            "decision_indicator_context",
            lambda: self.get_stock_decision_indicator_context(code, normalized_end),
        )
        market_index_prices = self._context_component(
            data_status,
            "market_index_prices",
            lambda: self.get_market_index_prices(start_date, normalized_end),
        )
        sector_profile = self._context_component(
            data_status,
            "sector_profile",
            lambda: self.get_stock_sector_profile(code),
        )
        sector_index_prices: list[dict[str, Any]] = []
        sector_code = str((sector_profile or {}).get("sector_code") or "") if isinstance(sector_profile, dict) else ""
        if sector_code:
            sector_index_prices = self._context_component(
                data_status,
                "sector_index_prices",
                lambda: self.get_sector_index_prices(sector_code, start_date, normalized_end, "D"),
            )
        else:
            data_status["sector_index_prices"] = {
                "status": "skipped",
                "message": "sector_code unavailable",
            }

        intraday_prices: list[dict[str, Any]] = []
        if include_intraday:
            intraday_prices = self._context_component(
                data_status,
                "intraday_prices",
                lambda: self.get_intraday_prices(code, normalized_end, intraday_interval_minutes),
            )
        else:
            data_status["intraday_prices"] = {"status": "skipped"}

        news_headlines: list[dict[str, Any]] = []
        if include_news:
            news_headlines = self._context_component(
                data_status,
                "news_headlines",
                lambda: self.list_stock_news(code, normalized_end)[:news_limit],
            )
        else:
            data_status["news_headlines"] = {"status": "skipped"}

        return {
            "code": code,
            "start_date": start_date,
            "end_date": normalized_end,
            "lookback_days": lookback_days,
            "daily_prices": daily_prices,
            "weekly_prices": weekly_prices,
            "intraday_prices": intraday_prices,
            "daily_technical_indicators": daily_technical_indicators,
            "chart_pattern_candidates": chart_pattern_candidates,
            "quote_snapshot": quote_snapshot,
            "decision_indicator_context": decision_indicator_context,
            "market_index_prices": market_index_prices,
            "sector_profile": sector_profile,
            "sector_index_prices": sector_index_prices,
            "news_headlines": news_headlines,
            "data_status": data_status,
        }

    def get_balance(self, account_no: str) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_balance(account_no)]

    def get_executions(self, account_no: str) -> list[dict[str, Any]]:
        return [execution.to_dict() for execution in self._client.get_executions(account_no)]

    def get_open_orders(self, account_no: str, code: str | None = None) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self._client.get_open_orders(account_no, code)]

    def get_trade_history(
        self,
        account_no: str,
        code: str | None,
        start_date: str,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return [
            item.to_dict()
            for item in self._client.get_trade_history(account_no, code, start_date, end_date)
        ]

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
    ) -> list[dict[str, Any]]:
        return [
            item.to_dict()
            for item in self._client.get_account_ledger(
                account_no,
                start_date,
                end_date,
                transaction_type,
                market,
                include_mmw,
                include_rp_details,
                code,
                product_code,
                admin,
            )
        ]

    def place_order(
        self,
        account_no: str,
        code: str,
        side: str,
        quantity: int,
        price: int | None = None,
        order_type: str = "limit",
    ) -> dict[str, Any]:
        request = self._build_order_request(account_no, code, side, quantity, price, order_type)
        blocked = self._order_guard.block_if_needed(request, "place_order")
        if blocked is not None:
            return blocked.to_dict()
        self._validate_stock_order_session_policy(request, "place_order")
        return self._execute_order_action(self._client.place_order, request, "place_order")

    def modify_order(
        self,
        account_no: str,
        code: str,
        side: str,
        quantity: int,
        original_order_id: str,
        price: int | None = None,
        order_type: str = "limit",
        credit_trade_type: str | None = None,
    ) -> dict[str, Any]:
        request = self._build_order_request(
            account_no,
            code,
            side,
            quantity,
            price,
            order_type,
            original_order_id,
            credit_trade_type,
        )
        blocked = self._order_guard.block_if_needed(request, "modify_order")
        if blocked is not None:
            return blocked.to_dict()
        self._validate_stock_order_session_policy(request, "modify_order")
        request = self._resolve_stock_order_request(request, "modify_order")
        return self._execute_order_action(self._client.modify_order, request, "modify_order")

    def cancel_order(
        self,
        account_no: str,
        code: str,
        side: str,
        quantity: int,
        original_order_id: str,
        credit_trade_type: str | None = None,
    ) -> dict[str, Any]:
        request = self._build_order_request(
            account_no,
            code,
            side,
            quantity,
            None,
            "market",
            original_order_id,
            credit_trade_type,
        )
        blocked = self._order_guard.block_if_needed(request, "cancel_order")
        if blocked is not None:
            return blocked.to_dict()
        self._validate_stock_order_session_policy(request, "cancel_order")
        request = self._resolve_stock_order_request(request, "cancel_order")
        return self._execute_order_action(self._client.cancel_order, request, "cancel_order")

    def place_gold_order(
        self,
        account_no: str,
        code: str,
        side: str,
        quantity: int,
        price: int,
    ) -> dict[str, Any]:
        request = self._build_gold_order_request(account_no, code, side, quantity, price, None, "place")
        guard_request = self._gold_guard_request(request)
        blocked = self._order_guard.block_if_needed(guard_request, "place_gold_order")
        if blocked is not None:
            return blocked.to_dict()
        return self._execute_gold_order_action(self._client.place_gold_order, request, "place_gold_order")

    def modify_gold_order(
        self,
        account_no: str,
        code: str,
        side: str,
        quantity: int,
        original_order_id: str,
        price: int,
    ) -> dict[str, Any]:
        request = self._build_gold_order_request(account_no, code, side, quantity, price, original_order_id, "modify")
        guard_request = self._gold_guard_request(request)
        blocked = self._order_guard.block_if_needed(guard_request, "modify_gold_order")
        if blocked is not None:
            return blocked.to_dict()
        return self._execute_gold_order_action(self._client.modify_gold_order, request, "modify_gold_order")

    def cancel_gold_order(
        self,
        account_no: str,
        code: str,
        side: str,
        quantity: int,
        original_order_id: str,
    ) -> dict[str, Any]:
        request = self._build_gold_order_request(account_no, code, side, quantity, None, original_order_id, "cancel")
        guard_request = self._gold_guard_request(request)
        blocked = self._order_guard.block_if_needed(guard_request, "cancel_gold_order")
        if blocked is not None:
            return blocked.to_dict()
        return self._execute_gold_order_action(self._client.cancel_gold_order, request, "cancel_gold_order")

    @staticmethod
    def _build_order_request(
        account_no: str,
        code: str,
        side: str,
        quantity: int,
        price: int | None,
        order_type: str,
        original_order_id: str | None = None,
        credit_trade_type: str | None = None,
    ) -> OrderRequest:
        if side not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        if order_type not in {"limit", "market"}:
            raise ValueError("order_type must be 'limit' or 'market'")
        return OrderRequest(
            account_no=account_no,
            code=code,
            side=side,  # type: ignore[arg-type]
            quantity=quantity,
            price=price,
            order_type=order_type,  # type: ignore[arg-type]
            original_order_id=original_order_id,
            credit_trade_type=credit_trade_type,
        )

    def register_order_carryover(
        self,
        account_no: str,
        code: str,
        order_id: str,
        premarket_to_regular: bool = True,
        regular_to_aftermarket: bool = True,
        httpCallback: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not premarket_to_regular and not regular_to_aftermarket:
            raise ValueError("at least one order carryover transition must be enabled")
        parsed_callback = self._parse_http_callback(httpCallback) if httpCallback is not None else None
        item = self._resolve_open_order(account_no, code, order_id)
        reason = self._order_carryover_common_skip_reason(item)
        if reason is not None:
            raise ValueError(f"order is not eligible for carryover: {reason}")

        record = self._build_order_carryover_record(
            account_no,
            item,
            premarket_to_regular,
            regular_to_aftermarket,
            parsed_callback,
        )
        record_identifiers = set(record["current_order_identifiers"])
        with self._order_carryover_lock:
            self._remove_expired_order_carryovers_locked(self._now_kst().strftime("%Y%m%d"))
            for existing in self._order_carryovers:
                existing_identifiers = {str(value) for value in existing.get("current_order_identifiers") or []}
                if record_identifiers & existing_identifiers:
                    raise ValueError("order_carryover is already registered for this order")
            self._order_carryovers.append(record)
        self._refresh_order_carryover_state(dispatch_callbacks=True)
        self._ensure_order_carryover_worker()
        return self._public_order_carryover_by_id(record["carryover_id"]) or self._public_order_carryover(record)

    def list_order_carryovers(
        self,
        account_no: str | None = None,
        code: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_code = self._normalize_stock_code_for_match(code) if code else None
        self._refresh_order_carryover_state(dispatch_callbacks=True)
        with self._order_carryover_lock:
            records = list(self._order_carryovers)
        return [
            self._public_order_carryover(record)
            for record in records
            if (not account_no or record.get("account_no") == account_no)
            and (not normalized_code or record.get("code") == normalized_code)
        ]

    def cancel_order_carryover(
        self,
        carryover_id: str | None = None,
        account_no: str | None = None,
        code: str | None = None,
        order_id: str | None = None,
    ) -> dict[str, Any]:
        if not any([carryover_id, account_no, code, order_id]):
            raise ValueError("carryover_id or at least one filter is required")
        normalized_code = self._normalize_stock_code_for_match(code) if code else None
        normalized_order_id = str(order_id or "").strip()

        cancelled: list[dict[str, Any]] = []
        in_flight: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        with self._order_carryover_lock:
            for record in self._order_carryovers:
                if self._order_carryover_matches(record, carryover_id, account_no, normalized_code, normalized_order_id):
                    if record.get("in_flight_transition"):
                        in_flight.append(record)
                        remaining.append(record)
                    else:
                        cancelled.append(record)
                else:
                    remaining.append(record)
            self._order_carryovers = remaining

        return {
            "cancelled": bool(cancelled),
            "cancelled_count": len(cancelled),
            "in_flight_count": len(in_flight),
            "carryover_ids": [str(record["carryover_id"]) for record in cancelled],
            "remaining_count": len(remaining),
            "message": self._order_carryover_cancel_message(cancelled, in_flight),
        }

    @staticmethod
    def _order_carryover_cancel_message(cancelled: list[dict[str, Any]], in_flight: list[dict[str, Any]]) -> str:
        if cancelled:
            return "order carryover registration cancelled"
        if in_flight:
            return "matching order carryover registration is already in flight"
        return "matching order carryover registration not found"

    @staticmethod
    def _order_result_identifiers(result: dict[str, Any]) -> list[str]:
        raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
        values = [
            result.get("order_id"),
            raw.get("sor_order_id"),
            raw.get("krx_order_id"),
            raw.get("nxt_order_id"),
        ]
        return sorted({str(value).strip() for value in values if str(value or "").strip() and str(value).strip() != "0"})

    @staticmethod
    def _open_order_identifiers(item: OpenOrder) -> list[str]:
        values = [
            item.order_id,
            item.raw_order_id,
            item.original_raw_order_id,
            item.sor_order_id,
            item.sor_original_order_id,
        ]
        return sorted({str(value).strip() for value in values if str(value or "").strip()})

    def _build_order_carryover_record(
        self,
        account_no: str,
        item: OpenOrder,
        premarket_to_regular: bool,
        regular_to_aftermarket: bool,
        http_callback: HttpCallbackSpec | None = None,
    ) -> dict[str, Any]:
        now_kst = self._now_kst()
        now = now_kst.strftime("%Y%m%d%H%M%S")
        return {
            "carryover_id": f"order_carryover_{now_kst.strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
            "account_no": account_no,
            "code": self._normalize_stock_code_for_match(item.code),
            "name": item.name,
            "side": item.side,
            "order_type": item.order_type,
            "price": item.price,
            "quantity": item.quantity,
            "unfilled_quantity": item.unfilled_quantity,
            "current_order_id": item.order_id,
            "current_order_identifiers": self._open_order_identifiers(item),
            "original_order_identifiers": self._open_order_identifiers(item),
            "premarket_to_regular": bool(premarket_to_regular),
            "regular_to_aftermarket": bool(regular_to_aftermarket),
            "attempted_dates": {
                "premarket_to_regular": "",
                "regular_to_aftermarket": "",
            },
            "transition_statuses": {
                "premarket_to_regular": "pending" if premarket_to_regular else "",
                "regular_to_aftermarket": "pending" if regular_to_aftermarket else "",
            },
            "last_status": "pending",
            "last_status_at": "",
            "in_flight_transition": "",
            "last_result": None,
            "httpCallback": http_callback.to_dict() if http_callback is not None else None,
            "created_at": now,
            "updated_at": now,
        }

    @staticmethod
    def _public_order_carryover(record: dict[str, Any]) -> dict[str, Any]:
        public = dict(record)
        public["current_order_identifiers"] = list(record.get("current_order_identifiers") or [])
        public["original_order_identifiers"] = list(record.get("original_order_identifiers") or [])
        public["attempted_dates"] = dict(record.get("attempted_dates") or {})
        public["transition_statuses"] = dict(record.get("transition_statuses") or {})
        if isinstance(record.get("httpCallback"), dict):
            callback = dict(record["httpCallback"])
            if isinstance(callback.get("headers"), dict):
                callback["headers"] = dict(callback["headers"])
            if isinstance(callback.get("body"), dict):
                callback["body"] = dict(callback["body"])
            public["httpCallback"] = callback
        if isinstance(record.get("last_result"), dict):
            public["last_result"] = dict(record["last_result"])
        return public

    def _public_order_carryover_by_id(self, carryover_id: object) -> dict[str, Any] | None:
        with self._order_carryover_lock:
            for record in self._order_carryovers:
                if record.get("carryover_id") == carryover_id:
                    return self._public_order_carryover(record)
        return None

    def _order_carryover_matches(
        self,
        record: dict[str, Any],
        carryover_id: str | None,
        account_no: str | None,
        code: str | None,
        order_id: str,
    ) -> bool:
        if carryover_id and record.get("carryover_id") != carryover_id:
            return False
        if account_no and record.get("account_no") != account_no:
            return False
        if code and record.get("code") != code:
            return False
        if order_id:
            identifiers = {
                str(value)
                for value in (record.get("current_order_identifiers") or []) + (record.get("original_order_identifiers") or [])
                if str(value)
            }
            if order_id not in identifiers:
                return False
        return True

    def _resolve_open_order(self, account_no: str, code: str, order_id: str) -> OpenOrder:
        normalized_code = self._normalize_stock_code_for_match(code)
        normalized_order_id = str(order_id or "").strip()
        if not normalized_order_id:
            raise ValueError("order_id is required")

        matches = []
        for item in self._client.get_open_orders(account_no, normalized_code):
            if normalized_order_id in self._open_order_identifiers(item):
                matches.append(item)
        if not matches:
            raise ValueError(f"open order not found for order_id={order_id}")
        if len(matches) > 1:
            raise ValueError(f"order_id={order_id} matched multiple open orders")
        return matches[0]

    def _process_due_order_carryovers(self) -> None:
        self._refresh_order_carryover_state(dispatch_callbacks=True)
        transitions = self._due_order_carryover_transitions()
        if not transitions:
            return
        current_date = self._now_kst().strftime("%Y%m%d")
        with self._order_carryover_lock:
            records = list(self._order_carryovers)

        for record in records:
            for transition in transitions:
                if not bool(record.get(transition)):
                    continue
                claimed = self._claim_order_carryover_attempt(record.get("carryover_id"), transition, current_date)
                if claimed is None:
                    continue
                if self._order_carryover_registered_too_late_for_transition(claimed, transition, current_date):
                    result = self._order_carryover_skip_result(
                        claimed,
                        transition,
                        "registered_too_late_for_transition",
                    )
                else:
                    try:
                        result = self._execute_order_carryover(claimed, transition)
                    except Exception as exc:
                        result = self._order_carryover_exception_result(claimed, transition, exc)
                updated = self._update_order_carryover_after_transition(claimed, transition, result)
                if updated is not None and isinstance(updated.get("last_result"), dict):
                    self._dispatch_order_carryover_callback(updated, dict(updated["last_result"]))

    def _claim_order_carryover_attempt(
        self,
        carryover_id: object,
        transition: str,
        current_date: str,
    ) -> dict[str, Any] | None:
        with self._order_carryover_lock:
            for record in self._order_carryovers:
                if record.get("carryover_id") != carryover_id:
                    continue
                if not bool(record.get(transition)):
                    return None
                attempted_dates = record.get("attempted_dates")
                if not isinstance(attempted_dates, dict):
                    attempted_dates = {}
                    record["attempted_dates"] = attempted_dates
                if attempted_dates.get(transition) == current_date:
                    return None
                transition_statuses = record.get("transition_statuses")
                if not isinstance(transition_statuses, dict):
                    transition_statuses = {}
                    record["transition_statuses"] = transition_statuses
                attempted_dates[transition] = current_date
                transition_statuses[transition] = "in_progress"
                record["in_flight_transition"] = transition
                record["updated_at"] = self._now_kst().strftime("%Y%m%d%H%M%S")
                return self._public_order_carryover(record)
        return None

    def _due_order_carryover_transitions(self) -> list[str]:
        seconds = self._seconds_since_kst_midnight(self._now_kst())
        premarket_to_regular_start = self._time_to_seconds("09:00:30")
        regular_to_aftermarket_start = self._time_to_seconds("15:30:00")
        window_seconds = self._order_carryover_active_window_seconds()
        if premarket_to_regular_start <= seconds < premarket_to_regular_start + window_seconds:
            return ["premarket_to_regular"]
        if regular_to_aftermarket_start <= seconds < regular_to_aftermarket_start + window_seconds:
            return ["regular_to_aftermarket"]
        return []

    def _order_carryover_transition_window_active(self, transition: str) -> bool:
        transition_start = self._order_carryover_transition_start_seconds(transition)
        if transition_start is None:
            return False
        seconds = self._seconds_since_kst_midnight(self._now_kst())
        return transition_start <= seconds < transition_start + self._order_carryover_active_window_seconds()

    def _update_order_carryover_after_transition(
        self,
        record: dict[str, Any],
        transition: str,
        result: dict[str, Any],
    ) -> dict[str, Any] | None:
        carryover_id = record.get("carryover_id")
        with self._order_carryover_lock:
            for current in self._order_carryovers:
                if current.get("carryover_id") != carryover_id:
                    continue
                status = self._order_carryover_result_status(result)
                result["status"] = status
                self._enrich_order_carryover_status_desc(result)
                current["last_result"] = result
                updated_at = self._now_kst().strftime("%Y%m%d%H%M%S")
                current["updated_at"] = updated_at
                current["last_status"] = status
                current["last_status_at"] = updated_at
                current["in_flight_transition"] = ""
                transition_statuses = current.get("transition_statuses")
                if not isinstance(transition_statuses, dict):
                    transition_statuses = {}
                    current["transition_statuses"] = transition_statuses
                transition_statuses[transition] = status
                if result.get("executed"):
                    identifiers = result.get("placed_order_identifiers") or []
                    if identifiers:
                        current["current_order_identifiers"] = list(identifiers)
                    place_result = result.get("place_result") if isinstance(result.get("place_result"), dict) else {}
                    current["current_order_id"] = place_result.get("order_id") or current.get("current_order_id")
                    current["quantity"] = result.get("quantity") or current.get("quantity")
                    current["unfilled_quantity"] = result.get("quantity") or current.get("unfilled_quantity")
                return self._public_order_carryover(current)
        return None

    def _order_carryover_registered_too_late_for_transition(
        self,
        record: dict[str, Any],
        transition: str,
        current_date: str,
    ) -> bool:
        created_at = str(record.get("created_at") or "")
        if len(created_at) < 14 or not created_at[:14].isdigit():
            return True
        if created_at[:8] != current_date:
            return False
        transition_start = self._order_carryover_transition_start_seconds(transition)
        if transition_start is None:
            return True
        created_seconds = int(created_at[8:10]) * 3600 + int(created_at[10:12]) * 60 + int(created_at[12:14])
        return created_seconds >= transition_start - self._order_carryover_min_lead_seconds()

    @classmethod
    def _order_carryover_transition_start_seconds(cls, transition: str) -> int | None:
        if transition == "premarket_to_regular":
            return cls._time_to_seconds("09:00:30")
        if transition == "regular_to_aftermarket":
            return cls._time_to_seconds("15:30:00")
        return None

    @staticmethod
    def _order_carryover_min_lead_seconds() -> int:
        return 10

    @staticmethod
    def _order_carryover_active_window_seconds() -> int:
        return 60

    def _refresh_order_carryover_state(self, dispatch_callbacks: bool = False) -> None:
        now_kst = self._now_kst()
        current_date = now_kst.strftime("%Y%m%d")
        with self._order_carryover_lock:
            self._remove_expired_order_carryovers_locked(current_date)
            missed_records = self._mark_missed_order_carryover_transitions_locked(now_kst)
        if dispatch_callbacks:
            for record in missed_records:
                result = record.get("last_result")
                if isinstance(result, dict):
                    self._dispatch_order_carryover_callback(record, result)

    def _remove_expired_order_carryovers_locked(self, current_date: str) -> None:
        self._order_carryovers = [
            record
            for record in self._order_carryovers
            if self._order_carryover_created_date(record) == current_date
        ]

    def _mark_missed_order_carryover_transitions_locked(self, now_kst: datetime) -> list[dict[str, Any]]:
        current_date = now_kst.strftime("%Y%m%d")
        seconds = self._seconds_since_kst_midnight(now_kst)
        now = now_kst.strftime("%Y%m%d%H%M%S")
        missed_records: list[dict[str, Any]] = []
        for record in self._order_carryovers:
            if self._order_carryover_created_date(record) != current_date:
                continue
            attempted_dates = record.get("attempted_dates")
            if not isinstance(attempted_dates, dict):
                attempted_dates = {}
                record["attempted_dates"] = attempted_dates
            transition_statuses = record.get("transition_statuses")
            if not isinstance(transition_statuses, dict):
                transition_statuses = {}
                record["transition_statuses"] = transition_statuses
            for transition in ("premarket_to_regular", "regular_to_aftermarket"):
                if not bool(record.get(transition)) or attempted_dates.get(transition) == current_date:
                    continue
                transition_start = self._order_carryover_transition_start_seconds(transition)
                if transition_start is None:
                    continue
                if seconds < transition_start + self._order_carryover_active_window_seconds():
                    continue
                reason = (
                    "registered_too_late_for_transition"
                    if self._order_carryover_registered_too_late_for_transition(record, transition, current_date)
                    else "transition_window_missed"
                )
                result = self._order_carryover_skip_result(record, transition, reason)
                result["status"] = "missed"
                self._enrich_order_carryover_status_desc(result)
                attempted_dates[transition] = current_date
                transition_statuses[transition] = "missed"
                record["last_result"] = result
                record["last_status"] = "missed"
                record["last_status_at"] = now
                record["updated_at"] = now
                missed_records.append(self._public_order_carryover(record))
        return missed_records

    @staticmethod
    def _order_carryover_created_date(record: dict[str, Any]) -> str:
        created_at = str(record.get("created_at") or "")
        return created_at[:8] if len(created_at) >= 8 and created_at[:8].isdigit() else ""

    def _has_pending_order_carryovers(self) -> bool:
        current_date = self._now_kst().strftime("%Y%m%d")
        with self._order_carryover_lock:
            return self._has_pending_order_carryovers_locked(current_date)

    def _has_pending_order_carryovers_locked(self, current_date: str) -> bool:
        for record in self._order_carryovers:
            if self._order_carryover_created_date(record) != current_date:
                continue
            attempted_dates = record.get("attempted_dates") if isinstance(record.get("attempted_dates"), dict) else {}
            if record.get("in_flight_transition"):
                return True
            for transition in ("premarket_to_regular", "regular_to_aftermarket"):
                if bool(record.get(transition)) and attempted_dates.get(transition) != current_date:
                    return True
        return False

    @staticmethod
    def _order_carryover_result_status(result: dict[str, Any]) -> str:
        status = str(result.get("status") or "").strip()
        if status in {"success", "missed", "chaos"}:
            return status
        return "success" if bool(result.get("executed")) else "missed"

    @classmethod
    def _order_carryover_status_desc(cls, status: object, detail: object = "") -> str:
        detail_text = str(detail or "").strip()
        if detail_text:
            return cls._ORDER_CARRYOVER_STATUS_DESCRIPTIONS.get(
                detail_text,
                "자동 이월 처리 중 분류되지 않은 사유입니다.",
            )
        status_text = str(status or "").strip()
        return cls._ORDER_CARRYOVER_STATUS_DESCRIPTIONS.get(status_text, "")

    @classmethod
    def _enrich_order_carryover_status_desc(cls, result: dict[str, Any]) -> None:
        detail = result.pop("reason", "")
        result.pop("reason_desc", None)
        result["status_desc"] = cls._order_carryover_status_desc(result.get("status"), detail)
        confirmation = result.get("cancel_confirmation")
        if isinstance(confirmation, dict) and "reason" in confirmation:
            confirmation["status_desc"] = cls._order_carryover_status_desc("", confirmation.get("reason"))
            confirmation.pop("reason", None)
            confirmation.pop("reason_desc", None)

    @staticmethod
    def _order_carryover_skip_result(
        record: dict[str, Any],
        transition: str,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "carryover_id": record.get("carryover_id"),
            "transition": transition,
            "account_no": record.get("account_no"),
            "code": record.get("code"),
            "executed": False,
            "skipped": True,
            "status": "missed",
            "reason": reason,
            "cancel_result": None,
            "place_result": None,
            "placed_order_identifiers": [],
        }

    @staticmethod
    def _order_carryover_exception_result(
        record: dict[str, Any],
        transition: str,
        exc: Exception,
    ) -> dict[str, Any]:
        return {
            "carryover_id": record.get("carryover_id"),
            "transition": transition,
            "account_no": record.get("account_no"),
            "code": record.get("code"),
            "executed": False,
            "skipped": True,
            "status": "missed",
            "reason": "exception",
            "error_type": exc.__class__.__name__,
            "message": str(exc),
            "cancel_result": None,
            "place_result": None,
            "placed_order_identifiers": [],
        }

    def _execute_order_carryover(self, record: dict[str, Any], transition: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "carryover_id": record.get("carryover_id"),
            "transition": transition,
            "account_no": record.get("account_no"),
            "code": record.get("code"),
            "executed": False,
            "skipped": False,
            "status": "",
            "reason": "",
            "cancel_result": None,
            "place_result": None,
            "placed_order_identifiers": [],
        }
        if not self._order_carryover_transition_window_active(transition):
            result.update({"skipped": True, "status": "missed", "reason": "transition_window_missed"})
            return result
        account_no = str(record.get("account_no") or "")
        code = str(record.get("code") or "")
        identifiers = {str(value) for value in record.get("current_order_identifiers") or [] if str(value)}
        matches = []
        for item in self._client.get_open_orders(account_no, code):
            if identifiers & set(self._open_order_identifiers(item)):
                matches.append(item)
        if not matches:
            result.update({"skipped": True, "status": "missed", "reason": "open_order_not_found"})
            return result
        if len(matches) > 1:
            result.update({"skipped": True, "status": "missed", "reason": "matched_multiple_open_orders"})
            return result

        item = matches[0]
        if item.filled_quantity > 0:
            result.update(
                {
                    "skipped": True,
                    "status": "chaos",
                    "reason": "partial_fill_chaos",
                    "filled_quantity": item.filled_quantity,
                    "unfilled_quantity": item.unfilled_quantity,
                }
            )
            return result

        reason = self._order_carryover_skip_reason(item, transition) or self._order_carryover_snapshot_skip_reason(record, item)
        if reason is not None:
            result.update({"skipped": True, "status": "missed", "reason": reason})
            return result

        original_order_id = item.sor_order_id or item.raw_order_id or item.order_id
        try:
            cancel_result = self.cancel_order(
                account_no=account_no,
                code=item.code,
                side=item.side,
                quantity=item.unfilled_quantity,
                original_order_id=original_order_id,
                credit_trade_type=item.credit_trade_type or None,
            )
        except Exception as exc:
            result.update(
                {
                    "skipped": True,
                    "status": "missed",
                    "reason": "cancel_exception",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                }
            )
            return result
        result["cancel_result"] = cancel_result
        if not bool(cancel_result.get("accepted")):
            result.update({"skipped": True, "status": "missed", "reason": "cancel_not_accepted"})
            return result

        confirmed_quantity, confirmation = self._confirm_order_carryover_cancelled_quantity(
            account_no,
            item,
        )
        result["cancel_confirmation"] = confirmation
        if confirmed_quantity is None:
            result.update(
                {
                    "skipped": True,
                    "status": str(confirmation.get("status") or "missed"),
                    "reason": confirmation["reason"],
                }
            )
            return result
        if confirmed_quantity <= 0:
            result.update({"skipped": True, "status": "missed", "reason": "no_cancelled_quantity_to_carryover"})
            return result

        if not self._order_carryover_transition_window_active(transition):
            result.update(
                {
                    "skipped": True,
                    "status": "missed",
                    "reason": "transition_window_elapsed_before_reorder",
                    "quantity": confirmed_quantity,
                }
            )
            return result

        try:
            place_result = self.place_order(
                account_no=account_no,
                code=item.code,
                side=item.side,
                quantity=confirmed_quantity,
                price=item.price,
                order_type=item.order_type,
            )
        except Exception as exc:
            result.update(
                {
                    "skipped": True,
                    "status": "missed",
                    "reason": "place_exception",
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                }
            )
            return result
        result["place_result"] = place_result
        result["executed"] = bool(place_result.get("accepted"))
        if not result["executed"]:
            result["skipped"] = True
            result["status"] = "missed"
            result["reason"] = "place_not_accepted"
        else:
            result["status"] = "success"
        result["quantity"] = confirmed_quantity
        result["placed_order_identifiers"] = self._order_result_identifiers(place_result)
        return result

    def _confirm_order_carryover_cancelled_quantity(
        self,
        account_no: str,
        item: OpenOrder,
    ) -> tuple[int | None, dict[str, Any]]:
        identifiers = set(self._open_order_identifiers(item))
        try:
            remaining_matches = [
                current
                for current in self._client.get_open_orders(account_no, item.code)
                if identifiers & set(self._open_order_identifiers(current))
            ]
        except Exception as exc:
            return None, {
                "confirmed": False,
                "reason": "cancel_confirmation_exception",
                "stage": "open_orders",
                "error_type": exc.__class__.__name__,
                "message": str(exc),
            }
        if remaining_matches:
            return None, {
                "confirmed": False,
                "reason": "cancel_unconfirmed_open_order_still_exists",
                "remaining_unfilled_quantity": sum(current.unfilled_quantity for current in remaining_matches),
            }

        try:
            executed_quantity = self._executed_quantity_for_order(account_no, item.code, identifiers)
        except Exception as exc:
            return None, {
                "confirmed": False,
                "reason": "cancel_confirmation_exception",
                "stage": "executions",
                "error_type": exc.__class__.__name__,
                "message": str(exc),
            }
        if executed_quantity is None:
            if item.filled_quantity > 0:
                return None, {
                    "confirmed": False,
                    "status": "chaos",
                    "reason": "partial_fill_chaos",
                    "filled_quantity_before_cancel": item.filled_quantity,
                    "filled_quantity_after_cancel": None,
                }
            return None, {
                "confirmed": False,
                "reason": "cancelled_quantity_unconfirmed",
                "requested_cancel_quantity": item.unfilled_quantity,
                "filled_quantity_before_cancel": item.filled_quantity,
                "filled_quantity_after_cancel": None,
            }

        additional_filled_quantity = max(executed_quantity - item.filled_quantity, 0)
        if additional_filled_quantity > 0:
            return None, {
                "confirmed": False,
                "status": "chaos",
                "reason": "partial_fill_chaos",
                "requested_cancel_quantity": item.unfilled_quantity,
                "filled_quantity_before_cancel": item.filled_quantity,
                "filled_quantity_after_cancel": executed_quantity,
                "additional_filled_quantity": additional_filled_quantity,
            }
        confirmed_quantity = item.unfilled_quantity
        return confirmed_quantity, {
            "confirmed": True,
            "reason": "confirmed",
            "requested_cancel_quantity": item.unfilled_quantity,
            "filled_quantity_before_cancel": item.filled_quantity,
            "filled_quantity_after_cancel": executed_quantity,
            "additional_filled_quantity": additional_filled_quantity,
            "confirmed_cancelled_quantity": confirmed_quantity,
        }

    def _dispatch_order_carryover_callback(self, record: dict[str, Any], result: dict[str, Any]) -> dict[str, Any] | None:
        raw_callback = record.get("httpCallback")
        if not isinstance(raw_callback, dict):
            return None
        try:
            callback = self._http_callback_from_dict(raw_callback)
            payload = self._order_carryover_callback_payload(record, result)
            dispatch_result = self._order_carryover_dispatcher.dispatch(
                self._order_carryover_callback(callback, payload)
            )
        except Exception as exc:
            ops_log(
                LogSource.WEBHOOK,
                f"order carryover callback dispatch failed: {exc.__class__.__name__}: {exc}",
            )
            return {"queued": False, "delivered": None, "error": str(exc)}
        return dispatch_result

    def _order_carryover_callback_payload(self, record: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        status = self._order_carryover_result_status(result)
        status_desc = result.get("status_desc") or self._order_carryover_status_desc(status, result.get("reason"))
        quantity = result.get("quantity") or record.get("unfilled_quantity") or record.get("quantity")
        price = record.get("price")
        side = str(record.get("side") or "")
        target_market = self._order_carryover_target_market(result)
        return {
            "event_type": "order_carryover_transition",
            "carryover_id": record.get("carryover_id"),
            "account_no": record.get("account_no"),
            "code": record.get("code"),
            "name": record.get("name"),
            "side": side,
            "side_label": self._order_carryover_side_label(side),
            "price": price,
            "target_market": target_market,
            "transition": result.get("transition"),
            "status": status,
            "status_desc": status_desc,
            "executed": bool(result.get("executed")),
            "skipped": bool(result.get("skipped")),
            "quantity": quantity,
            "last_status_at": record.get("last_status_at") or self._now_kst().strftime("%Y%m%d%H%M%S"),
            "cancel_result": result.get("cancel_result"),
            "cancel_confirmation": result.get("cancel_confirmation"),
            "place_result": result.get("place_result"),
        }

    @staticmethod
    def _order_carryover_side_label(side: str) -> str:
        if side == "buy":
            return "매수"
        if side == "sell":
            return "매도"
        return side

    @staticmethod
    def _order_carryover_target_market(result: dict[str, Any]) -> str:
        place_result = result.get("place_result") if isinstance(result.get("place_result"), dict) else {}
        raw = place_result.get("raw") if isinstance(place_result.get("raw"), dict) else {}
        method_code = str(raw.get("order_method_code") or "").strip()
        if method_code == "0" or raw.get("sor_order_id"):
            return "SOR"
        if method_code == "1" or raw.get("krx_order_id"):
            return "KRX"
        if method_code == "2" or raw.get("nxt_order_id"):
            return "NXT"
        return "SOR"

    def _order_carryover_callback(self, callback: HttpCallbackSpec, payload: dict[str, Any]) -> HttpCallbackSpec:
        if callback.body is None:
            return HttpCallbackSpec(
                method=callback.method,
                url=callback.url,
                headers=dict(callback.headers),
                body=payload,
                body_format="json",
            )
        replacements = self._order_carryover_callback_replacements(payload)
        return HttpCallbackSpec(
            method=callback.method,
            url=callback.url,
            headers=dict(callback.headers),
            body=self._render_callback_template_value(callback.body, replacements),
            body_format=callback.body_format,
        )

    @staticmethod
    def _order_carryover_callback_replacements(payload: dict[str, Any]) -> dict[str, str]:
        def text(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, bool):
                return "true" if value else "false"
            return str(value)

        trade_price_raw = text(payload.get("price"))
        return {
            "eventType": "order_carryover_transition",
            "carryoverId": text(payload.get("carryover_id")),
            "accountNo": text(payload.get("account_no")),
            "stockName": text(payload.get("name")),
            "stockCode": text(payload.get("code")),
            "side": text(payload.get("side")),
            "sideLabel": text(payload.get("side_label")),
            "tradePrice": format_display_decimal(trade_price_raw) if trade_price_raw else "",
            "tradePriceRaw": trade_price_raw,
            "trade_price": format_display_decimal(trade_price_raw) if trade_price_raw else "",
            "trade_price_raw": trade_price_raw,
            "targetMarket": text(payload.get("target_market")),
            "transition": text(payload.get("transition")),
            "status": text(payload.get("status")),
            "statusDesc": text(payload.get("status_desc")),
            "executed": text(payload.get("executed")),
            "skipped": text(payload.get("skipped")),
            "quantity": format_display_decimal(payload.get("quantity")),
            "quantityRaw": text(payload.get("quantity")),
            "quantity_raw": text(payload.get("quantity")),
            "lastStatusAt": text(payload.get("last_status_at")),
        }

    def _render_callback_template_value(self, value: Any, replacements: dict[str, str]) -> Any:
        if isinstance(value, dict):
            return {key: self._render_callback_template_value(item, replacements) for key, item in value.items()}
        if isinstance(value, list):
            return [self._render_callback_template_value(item, replacements) for item in value]
        if isinstance(value, str):
            return re.sub(
                r"\{\{\s*([^{}]+?)\s*\}\}",
                lambda match: replacements.get(match.group(1).strip(), match.group(0)),
                value,
            )
        return value

    def _executed_quantity_for_order(
        self,
        account_no: str,
        code: str,
        identifiers: set[str],
    ) -> int | None:
        normalized_code = self._normalize_stock_code_for_match(code)
        matched_quantities_by_order_id: dict[str, int] = {}
        for execution in self._client.get_executions(account_no):
            if self._normalize_stock_code_for_match(execution.code) != normalized_code:
                continue
            execution_identifiers = self._execution_identifiers(execution)
            if not (set(execution_identifiers) & identifiers):
                continue
            order_key = next((identifier for identifier in execution_identifiers if identifier), "")
            if not order_key:
                continue
            matched_quantities_by_order_id[order_key] = max(
                matched_quantities_by_order_id.get(order_key, 0),
                int(execution.quantity),
            )
        if not matched_quantities_by_order_id:
            return None
        return sum(matched_quantities_by_order_id.values())

    @staticmethod
    def _execution_identifiers(execution: Execution) -> list[str]:
        candidates = [
            execution.order_id,
            getattr(execution, "raw_order_id", ""),
            getattr(execution, "original_order_id", ""),
            getattr(execution, "sor_order_id", ""),
            getattr(execution, "sor_original_order_id", ""),
        ]
        return [str(candidate).strip() for candidate in candidates if str(candidate).strip()]

    @staticmethod
    def _order_carryover_snapshot_skip_reason(record: dict[str, Any], item: OpenOrder) -> str | None:
        if item.side != record.get("side"):
            return "side_changed"
        if item.order_type != record.get("order_type"):
            return "order_type_changed"
        if int(item.price) != int(record.get("price") or 0):
            return "price_changed"
        return None

    def _order_carryover_common_skip_reason(self, item: OpenOrder) -> str | None:
        if (item.order_method_code or "").strip() != "0":
            return "order_method_is_not_sor"
        if not (item.sor_order_id or item.raw_order_id or item.order_id):
            return "original_order_id_unavailable"
        if not (item.sor_original_order_id or "").strip():
            return "sor_original_order_id_unavailable"
        if (item.credit_trade_type or "").strip() not in {"", "00"}:
            return "credit_order_not_supported"
        if item.order_type != "limit":
            return "only_limit_orders_are_supported"
        if item.price <= 0:
            return "limit_price_unavailable"
        if item.unfilled_quantity <= 0:
            return "unfilled_quantity_unavailable"
        return None

    def _order_carryover_skip_reason(self, item: OpenOrder, transition: str) -> str | None:
        reason = self._order_carryover_common_skip_reason(item)
        if reason is not None:
            return reason
        if transition == "premarket_to_regular" and not self._looks_like_nxt_open_order(item):
            return "order_exchange_is_not_nxt"
        if transition == "regular_to_aftermarket" and not self._looks_like_krx_open_order(item):
            return "order_exchange_is_not_krx"
        return None

    @staticmethod
    def _looks_like_nxt_open_order(item: OpenOrder) -> bool:
        exchange_code = (item.order_exchange_code or "").strip()
        exchange_name = (item.order_exchange_name or "").strip().upper()
        return exchange_code == "2" or "NXT" in exchange_name

    @staticmethod
    def _looks_like_krx_open_order(item: OpenOrder) -> bool:
        exchange_code = (item.order_exchange_code or "").strip()
        exchange_name = (item.order_exchange_name or "").strip().upper()
        return exchange_code == "1" or "KRX" in exchange_name

    def _resolve_stock_order_request(self, request: OrderRequest, action_name: str) -> OrderRequest:
        if not request.original_order_id:
            raise ValueError("original_order_id is required")
        normalized_request_code = self._normalize_stock_code_for_match(request.code)
        matches = []
        for item in self._client.get_open_orders(request.account_no, normalized_request_code):
            candidates = {
                item.order_id,
                item.raw_order_id,
                item.original_raw_order_id,
                item.sor_order_id,
                item.sor_original_order_id,
            }
            if request.original_order_id in {candidate for candidate in candidates if candidate}:
                matches.append(item)
        if not matches:
            raise ValueError(f"open order not found for original_order_id={request.original_order_id}")
        if len(matches) > 1:
            raise ValueError(f"original_order_id={request.original_order_id} matched multiple open orders")

        item = matches[0]
        if self._normalize_stock_code_for_match(item.code) != normalized_request_code:
            raise ValueError("request code does not match the open order")
        if item.side != request.side:
            raise ValueError("request side does not match the open order")
        if action_name == "modify_order" and item.order_type != request.order_type:
            raise ValueError("request order_type does not match the open order")
        if action_name == "cancel_order" and request.quantity > item.unfilled_quantity:
            raise ValueError("cancel quantity exceeds unfilled quantity")

        order_method_code = (item.order_method_code or "").strip()
        if not order_method_code:
            raise ValueError("open order is missing order_method_code")
        raw_order_id = (item.raw_order_id or item.order_id or "").strip()
        if not raw_order_id:
            raise ValueError("open order is missing raw_order_id")

        sor_original_order_id = (item.sor_original_order_id or "").strip()
        if order_method_code == "0" and not sor_original_order_id:
            raise ValueError("SOR open order is missing sor_original_order_id")

        return replace(
            request,
            original_order_id=raw_order_id,
            credit_trade_type=request.credit_trade_type or item.credit_trade_type or "00",
            order_method_code=order_method_code,
            sor_original_order_id=sor_original_order_id,
        )

    def _validate_stock_order_session_policy(self, request: OrderRequest, action_name: str) -> None:
        if action_name == "cancel_order":
            return
        if request.order_type == "limit" and (request.price is None or request.price <= 0):
            raise ValueError("limit order requires a positive price")

        seconds = self._seconds_since_kst_midnight(self._now_kst())
        if self._in_range(seconds, "08:50:00", "09:00:30"):
            raise ValueError("new and modify stock orders are blocked during the 08:50:00-09:00:30 transition")
        if not (
            self._in_range(seconds, "08:00:00", "08:50:00")
            or self._in_range(seconds, "09:00:30", "15:30:00")
            or self._in_range(seconds, "15:30:00", "20:00:00")
        ):
            raise ValueError("new and modify stock orders are blocked outside supported KST sessions")
        if request.order_type == "market" and (
            self._in_range(seconds, "08:00:00", "08:50:00")
            or self._in_range(seconds, "15:30:00", "20:00:00")
        ):
            raise ValueError("market stock orders are blocked during NXT-only SOR sessions")

    @staticmethod
    def _now_kst() -> datetime:
        return datetime.now(timezone(timedelta(hours=9)))

    @classmethod
    def _seconds_since_kst_midnight(cls, value: datetime) -> int:
        if value.tzinfo is not None:
            value = value.astimezone(timezone(timedelta(hours=9)))
        return value.hour * 3600 + value.minute * 60 + value.second

    @classmethod
    def _in_range(cls, value_seconds: int, start: str, end: str) -> bool:
        return cls._time_to_seconds(start) <= value_seconds < cls._time_to_seconds(end)

    @staticmethod
    def _time_to_seconds(value: str) -> int:
        hour, minute, second = (int(part) for part in value.split(":"))
        return hour * 3600 + minute * 60 + second

    @staticmethod
    def _normalize_stock_code_for_match(code: str) -> str:
        normalized = code.strip().upper()
        if len(normalized) == 7 and normalized[0] in {"A", "Q", "J"}:
            return normalized[1:]
        return normalized

    @staticmethod
    def _build_gold_order_request(
        account_no: str,
        code: str,
        side: str,
        quantity: int,
        price: int | None,
        original_order_id: str | None,
        action: str,
    ) -> GoldOrderRequest:
        if side not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        if quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        if action in {"place", "modify"} and (price is None or price <= 0):
            raise ValueError("gold place/modify orders require a positive limit price")
        if action in {"modify", "cancel"} and not original_order_id:
            raise ValueError("original_order_id is required")
        if action not in {"place", "modify", "cancel"}:
            raise ValueError("unsupported gold order action")
        return GoldOrderRequest(
            account_no=account_no,
            code=code,
            side=side,  # type: ignore[arg-type]
            quantity=quantity,
            price=price,
            original_order_id=original_order_id,
            action=action,  # type: ignore[arg-type]
        )

    @staticmethod
    def _gold_guard_request(request: GoldOrderRequest) -> OrderRequest:
        return OrderRequest(
            account_no=request.account_no,
            code=request.code,
            side=request.side,
            quantity=request.quantity,
            price=request.price,
            order_type="limit",
            original_order_id=request.original_order_id,
        )

    @staticmethod
    def _execute_order_action(action: Any, request: OrderRequest, action_name: str) -> dict[str, Any]:
        try:
            return action(request).to_dict()
        except NotImplementedError as exc:
            return OrderResult(
                accepted=False,
                live_order=False,
                order_id=None,
                message=f"{action_name} is not implemented for the active backend: {exc}",
                raw={
                    "account_no": request.account_no,
                    "code": request.code,
                    "side": request.side,
                    "quantity": request.quantity,
                    "price": request.price,
                    "order_type": request.order_type,
                    "original_order_id": request.original_order_id,
                    "credit_trade_type": request.credit_trade_type,
                },
            ).to_dict()

    @staticmethod
    def _execute_gold_order_action(
        action: Any,
        request: GoldOrderRequest,
        action_name: str,
    ) -> dict[str, Any]:
        try:
            return action(request).to_dict()
        except NotImplementedError as exc:
            return OrderResult(
                accepted=False,
                live_order=False,
                order_id=None,
                message=f"{action_name} is not implemented for the active backend: {exc}",
                raw={
                    "account_no": request.account_no,
                    "code": request.code,
                    "side": request.side,
                    "quantity": request.quantity,
                    "price": request.price,
                    "order_type": "limit",
                    "original_order_id": request.original_order_id,
                    "action": request.action,
                },
            ).to_dict()

    @classmethod
    def _expanded_start_date(cls, start_date: str | None, warmup_days: int) -> str | None:
        if start_date is None:
            return None
        parsed = cls._parse_date_value(start_date)
        if parsed is None:
            return start_date
        return (parsed - timedelta(days=warmup_days)).strftime("%Y%m%d")

    @staticmethod
    def _parse_date_value(value: str | None) -> datetime | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            return None
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _today_kst() -> str:
        return datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")

    @classmethod
    def _normalize_date_value(cls, value: str | None) -> str | None:
        parsed = cls._parse_date_value(value)
        if parsed is None:
            return None
        return parsed.strftime("%Y%m%d")

    @classmethod
    def _date_in_range(
        cls,
        value: str,
        normalized_start: str | None,
        normalized_end: str | None,
    ) -> bool:
        normalized_value = cls._normalize_date_value(value)
        if normalized_value is None:
            return False
        if normalized_start is not None and normalized_value < normalized_start:
            return False
        if normalized_end is not None and normalized_value > normalized_end:
            return False
        return True

    @classmethod
    def _filter_daily_prices(
        cls,
        prices: list[DailyPrice],
        start_date: str | None,
        end_date: str | None,
    ) -> list[DailyPrice]:
        normalized_start = cls._normalize_date_value(start_date)
        normalized_end = cls._normalize_date_value(end_date)
        return [
            price
            for price in prices
            if cls._date_in_range(price.date, normalized_start, normalized_end)
        ]

    @classmethod
    def _build_weekly_price_rows(cls, prices: list[DailyPrice]) -> list[dict[str, Any]]:
        ordered = sorted(
            [price for price in prices if cls._parse_date_value(price.date) is not None],
            key=lambda item: cls._parse_date_value(item.date) or datetime.min,
        )
        weekly: dict[tuple[int, int], list[DailyPrice]] = {}
        for price in ordered:
            parsed = cls._parse_date_value(price.date)
            if parsed is None:
                continue
            iso_year, iso_week, _ = parsed.isocalendar()
            weekly.setdefault((iso_year, iso_week), []).append(price)

        rows: list[dict[str, Any]] = []
        for (iso_year, iso_week), items in sorted(weekly.items()):
            rows.append(
                {
                    "week": f"{iso_year}-W{iso_week:02d}",
                    "start_date": items[0].date,
                    "end_date": items[-1].date,
                    "open": items[0].open,
                    "high": max(item.high for item in items),
                    "low": min(item.low for item in items),
                    "close": items[-1].close,
                    "volume": sum(item.volume for item in items),
                    "trading_days": len(items),
                }
            )
        rows.reverse()
        return rows

    @staticmethod
    def _weekly_rows_to_daily_prices(rows: list[dict[str, Any]]) -> list[DailyPrice]:
        return [
            DailyPrice(
                date=str(row["end_date"]),
                open=int(row["open"] or 0),
                high=int(row["high"] or 0),
                low=int(row["low"] or 0),
                close=int(row["close"] or 0),
                volume=int(row["volume"] or 0),
            )
            for row in rows
        ]

    @staticmethod
    def _normalize_time_value(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = re.sub(r"[^0-9]", "", value.strip())
        if not cleaned:
            return None
        if len(cleaned) == 4:
            cleaned = f"{cleaned}00"
        if len(cleaned) != 6 or not cleaned.isdigit():
            raise ValueError("time must be HHMMSS, HH:MM:SS, HHMM, or HH:MM")
        hour = int(cleaned[:2])
        minute = int(cleaned[2:4])
        second = int(cleaned[4:6])
        if hour > 23 or minute > 59 or second > 59:
            raise ValueError("time must be a valid clock time")
        return cleaned

    @staticmethod
    def _filtered_intraday_prices(items: list[IntradayPrice], as_of_time: str | None) -> list[IntradayPrice]:
        filtered = [
            item
            for item in items
            if not as_of_time or item.time <= as_of_time
        ]
        return sorted(filtered, key=lambda item: (item.date, item.time))

    @staticmethod
    def _intraday_vwap_by_timestamp(items: list[IntradayPrice]) -> dict[str, float | None]:
        result: dict[str, float | None] = {}
        total_value = 0.0
        total_volume = 0.0
        for item in sorted(items, key=lambda point: (point.date, point.time)):
            total_value += float(item.close) * float(item.volume)
            total_volume += float(item.volume)
            result[f"{item.date}{item.time}"] = round(total_value / total_volume, 4) if total_volume else None
        return result

    @staticmethod
    def _intraday_volume_ratio_by_timestamp(items: list[IntradayPrice]) -> dict[str, float | None]:
        result: dict[str, float | None] = {}
        total_volume = 0.0
        count = 0
        for item in sorted(items, key=lambda point: (point.date, point.time)):
            total_volume += float(item.volume)
            count += 1
            average = total_volume / count if count else 0.0
            result[f"{item.date}{item.time}"] = round(float(item.volume) / average, 4) if average else None
        return result

    @classmethod
    def _completed_daily_end_date(cls, as_of_date: str, as_of_time: str | None) -> str:
        if as_of_time is None or as_of_time >= "153000":
            return as_of_date
        return (datetime.strptime(as_of_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")

    @staticmethod
    def _is_overseas_etf_config(stock_config: dict[str, Any]) -> bool:
        return bool(stock_config.get("overseas_etf") or stock_config.get("fx_index") or stock_config.get("overseas_index"))

    def _market_environment_context(
        self,
        stock_config: dict[str, Any],
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        if self._is_overseas_etf_config(stock_config):
            return {
                "change_pct": 0,
                "change_series": [],
                "source": "omitted_overseas_etf",
                "index": "kospi200",
                "message": "overseas ETF-like stock; domestic market sync is not used",
            }
        return self._market_index_series_context("kospi200", start_date, end_date)

    def _sector_environment_context(
        self,
        sector_profile: dict[str, Any],
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        sector_code = str(sector_profile.get("sector_code") or "")
        sector_name = str(sector_profile.get("sector_name") or "")
        if not sector_code:
            return {"sector_code": "", "sector_name": "", "change_pct": 0, "change_series": [], "source": "unavailable"}
        try:
            prices = self._client.get_sector_index_prices(sector_code, start_date, end_date, "D")
        except Exception as exc:
            return {
                "sector_code": sector_code,
                "sector_name": sector_name,
                "change_pct": 0,
                "change_series": [],
                "source": "unavailable",
                "message": str(exc),
            }
        context = self._price_point_change_context(prices)
        context.update({"sector_code": sector_code, "sector_name": sector_name})
        return context

    def _market_index_environment_context(self, index_id: str, end_date: str) -> dict[str, Any]:
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=14)).strftime("%Y%m%d")
        context = self._market_index_series_context(index_id, start_date, end_date)
        return {
            "index": index_id.strip().lower(),
            "latest": context.get("latest"),
            "previous": context.get("previous"),
            "change_pct": context.get("change_pct", 0),
            "source": context.get("source", "unavailable"),
        }

    def _market_index_series_context(self, index_id: str, start_date: str, end_date: str) -> dict[str, Any]:
        normalized_index = index_id.strip().lower()
        try:
            prices = self._client.get_market_index_prices(start_date, end_date).get(normalized_index, [])
        except Exception as exc:
            return {
                "index": normalized_index,
                "change_pct": 0,
                "change_series": [],
                "source": "unavailable",
                "message": str(exc),
            }
        context = self._price_point_change_context(prices)
        context["index"] = normalized_index
        return context

    @staticmethod
    def _price_point_change_context(points: list[Any]) -> dict[str, Any]:
        ordered = sorted(points, key=lambda item: item.date)
        if len(ordered) < 2:
            return {"change_pct": 0, "change_series": [], "source": "unavailable"}
        latest, previous = ordered[-1], ordered[-2]
        change = ((latest.close - previous.close) / previous.close * 100.0) if previous.close else 0.0
        change_series = [
            {
                "date": ordered[index].date,
                "change_pct": round(((ordered[index].close - ordered[index - 1].close) / ordered[index - 1].close * 100.0), 6)
                if ordered[index - 1].close
                else 0.0,
            }
            for index in range(1, len(ordered))
        ]
        return {
            "change_pct": round(change, 4),
            "change_series": change_series[-60:],
            "latest": latest.close,
            "previous": previous.close,
            "source": "daily",
        }

    @staticmethod
    def _relative_strength_from_daily(daily: list[DailyPrice], market: dict[str, Any]) -> dict[str, Any]:
        if len(daily) < 2 or market.get("source") == "unavailable":
            return {"rs_ratio": 0, "status": "unavailable"}
        market_series = list(market.get("change_series") or [])
        market_by_date = {str(item.get("date")): float(item.get("change_pct") or 0.0) for item in market_series}
        rs_values: list[float] = []
        for index in range(1, len(daily)):
            previous = daily[index - 1]
            current = daily[index]
            stock_change = ((current.close - previous.close) / previous.close * 100.0) if previous.close else 0.0
            market_change = market_by_date.get(str(current.date).replace("-", ""), float(market.get("change_pct") or 0.0))
            rs_values.append((100.0 + stock_change) / max(100.0 + market_change, 0.0001))
        if not rs_values:
            return {"rs_ratio": 0, "status": "unavailable"}
        latest_rs = rs_values[-1]
        average = sum(rs_values[-60:]) / len(rs_values[-60:])
        rs_ratio = latest_rs / average if average else 1.0
        status = "strong" if rs_ratio > 1.01 else "weak" if rs_ratio < 0.99 else "neutral"
        return {"rs_ratio": round(rs_ratio, 4), "latest_rs": round(latest_rs, 4), "avg_rs_60d": round(average, 4), "status": status}

    @staticmethod
    def _trading_value_from_daily(daily: list[DailyPrice]) -> dict[str, Any]:
        values = [row.close * row.volume for row in daily[-20:]]
        avg20 = int(sum(values) / len(values)) if values else 0
        latest = daily[-1] if daily else None
        today = int(latest.close * latest.volume) if latest is not None else 0
        surge_ratio = round(today / avg20, 4) if avg20 else 0
        return {"surge_ratio": surge_ratio, "avg20": avg20, "latest": today}

    @staticmethod
    def _high_52w_from_daily(daily: list[DailyPrice]) -> dict[str, Any]:
        window = daily[-252:]
        if not window:
            return {"distance_pct": 0, "high": 0, "status": "unavailable"}
        high = max(row.high for row in window)
        latest_close = window[-1].close
        if high <= 0 or latest_close <= 0:
            return {"distance_pct": 0, "high": high, "status": "unavailable"}
        return {
            "distance_pct": round((latest_close - high) / high * 100.0, 4),
            "high": high,
            "latest_close": latest_close,
            "status": "available",
        }

    @staticmethod
    def _context_component(
        data_status: dict[str, Any],
        name: str,
        fetch: Callable[[], Any],
    ) -> Any:
        try:
            result = fetch()
        except Exception as exc:
            data_status[name] = {
                "status": "unavailable",
                "error_type": exc.__class__.__name__,
                "message": str(exc),
            }
            return [] if name.endswith(("prices", "indicators", "candidates", "headlines")) else {}

        count: int | None = None
        if isinstance(result, (list, tuple, dict)):
            count = len(result)
        status = "empty" if count == 0 else "available"
        data_status[name] = {"status": status}
        if count is not None:
            data_status[name]["count"] = count
        return result

    def _execute_fall_safe_order(self, account_no: str, code: str, quantity: int) -> dict[str, Any]:
        request = OrderRequest(
            account_no=account_no,
            code=code,
            side="sell",
            quantity=quantity,
            price=None,
            order_type="market",
        )
        blocked = self._order_guard.block_if_needed(request, "register_fall_safe")
        if blocked is not None:
            return blocked.to_dict()
        return self._execute_order_action(self._client.place_order, request, "register_fall_safe")

    @staticmethod
    def _http_callback_from_dict(raw: dict[str, Any]) -> HttpCallbackSpec:
        return HttpCallbackSpec(
            method=str(raw["method"]),
            url=str(raw["url"]),
            headers={str(key): str(value) for key, value in dict(raw.get("headers") or {}).items()},
            body=raw.get("body"),
            body_format=raw.get("bodyFormat"),
        )

    @staticmethod
    def _parse_http_callback(raw: dict[str, Any]) -> HttpCallbackSpec:
        method = str(raw.get("method", "")).strip().upper()
        url = str(raw.get("url", "")).strip()
        if method != "POST":
            raise ValueError("httpCallback.method must be POST")
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("httpCallback.url must start with http:// or https://")
        headers_raw = raw.get("headers")
        if headers_raw is None:
            headers: dict[str, str] = {}
        elif not isinstance(headers_raw, dict):
            raise ValueError("httpCallback.headers must be an object")
        else:
            headers = {str(key): str(value) for key, value in headers_raw.items()}
        body = raw.get("body")
        body_format_raw = raw.get("bodyFormat")
        if body is None and body_format_raw is not None:
            raise ValueError("httpCallback.bodyFormat is only valid when body is provided")
        if body is not None and not isinstance(body, dict):
            raise ValueError("httpCallback.body must be an object")
        body_format: str | None = None
        if body is not None:
            body_format = str(body_format_raw or "json").strip().lower()
            if body_format not in {"json", "form"}:
                raise ValueError("httpCallback.bodyFormat must be json or form")
        return HttpCallbackSpec(
            method=method,
            url=url,
            headers=headers,
            body=body,
            body_format=body_format,  # type: ignore[arg-type]
        )
