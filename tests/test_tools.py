import unittest
import json
import threading
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from homestock.analysis import build_technical_indicators, detect_chart_patterns
from homestock.config import Settings
from homestock.indi.mock import MockIndiClient
from homestock.indi.real import RealIndiClient
from homestock.indi.threaded import ThreadedIndiClient
from homestock.models import BalanceItem, DailyPrice, Execution, GoldOrderRequest, HealthStatus, HttpCallbackSpec, IntradayPrice, OpenOrder, OrderRequest, OrderResult
from homestock.order_guard import OrderGuard
from homestock.gold_runtime_state import GoldRuntimeStateManager
from homestock.runtime_state import PersistentSubscriptionStore, RuntimeStateManager
from homestock.scripter import InProcessScripter, MockScripter
from homestock.server import create_tools
from homestock.tools import HomestockTools


def make_tools(
    allow_live_orders: bool = False,
    runtime_state_dir: str | None = None,
    scripter=None,
):
    owns_scripter = scripter is None
    if scripter is None:
        scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter"))
    try:
        return create_tools(
            Settings(backend="mock", allow_live_orders=allow_live_orders, runtime_state_dir=runtime_state_dir),
            scripter=scripter,
        )
    except Exception:
        if owns_scripter:
            scripter.close()
        raise


def wait_for_scripter_idle(target, timeout: float = 1.0) -> bool:
    scripter = getattr(target, "_scripter", target)
    wait_for_idle = getattr(scripter, "wait_for_idle", None)
    if not callable(wait_for_idle):
        return True
    return bool(wait_for_idle(timeout))


def make_full_unfilled_open_order(
    order_id: str = "MOCK-OPEN-1",
    sor_order_id: str = "MOCK-SOR-1",
    exchange_code: str = "2",
    exchange_name: str = "NXT",
) -> OpenOrder:
    return OpenOrder(
        order_id=order_id,
        code="005930",
        name="Samsung Electronics",
        side="buy",
        order_type="limit",
        price=70000,
        quantity=100,
        filled_quantity=0,
        unfilled_quantity=100,
        order_time="20260424091000",
        status="pending",
        raw_order_id=order_id,
        original_raw_order_id="MOCK-ORIGINAL-1",
        order_method_code="0",
        order_method_name="SOR",
        order_exchange_code=exchange_code,
        order_exchange_name=exchange_name,
        sor_order_id=sor_order_id,
        sor_original_order_id="MOCK-SOR-ORIGINAL-1",
        credit_trade_type="00",
    )


def make_execution(
    order_id: str = "MOCK-OPEN-1",
    quantity: int = 0,
    sor_order_id: str = "",
) -> Execution:
    return Execution(
        order_id=order_id,
        code="005930",
        side="buy",
        quantity=quantity,
        price=70000,
        status="확인",
        executed_at="20260511090035",
        raw_order_id=order_id,
        sor_order_id=sor_order_id,
    )


class HomestockToolsTest(unittest.TestCase):
    def test_threaded_indi_client_runs_calls_on_worker_thread_and_relays_rt_events(self):
        created: dict[str, MockIndiClient] = {}

        class CapturingClient(MockIndiClient):
            INDI_MAIN_PROCESS_NAME = "FakeIndiMain.exe"

            def __init__(self) -> None:
                super().__init__()
                self.owner_thread_id = threading.get_ident()
                self.list_call_thread_id: int | None = None
                self.pump_count = 0

            def pump_events(self) -> None:
                self.pump_count += 1

            def list_stocks(self):
                self.list_call_thread_id = threading.get_ident()
                return super().list_stocks()

        def create_client() -> CapturingClient:
            client = CapturingClient()
            created["client"] = client
            return client

        threaded_client = ThreadedIndiClient(
            create_client,
            startup_timeout=2.0,
            call_timeout=2.0,
            pump_interval=0.001,
        )
        try:
            main_thread_id = threading.get_ident()
            result = threaded_client.list_stocks()
            worker_client = created["client"]

            self.assertEqual(threaded_client.INDI_MAIN_PROCESS_NAME, "FakeIndiMain.exe")
            self.assertEqual(result[0].code, "005930")
            self.assertNotEqual(worker_client.owner_thread_id, main_thread_id)
            self.assertEqual(worker_client.list_call_thread_id, worker_client.owner_thread_id)

            received = threading.Event()
            received_events: list[dict[str, object]] = []
            listener_thread_ids: list[int] = []

            def listener(event: dict[str, object]) -> None:
                received_events.append(event)
                listener_thread_ids.append(threading.get_ident())
                received.set()

            threaded_client.register_rt_listener(listener)
            threaded_client._call("emit_rt_event", {"rt_type": "N0", "code": "005930"})

            self.assertTrue(received.wait(1.0))
            self.assertEqual(received_events[0]["rt_type"], "N0")
            self.assertNotEqual(listener_thread_ids[0], worker_client.owner_thread_id)
            self.assertGreater(worker_client.pump_count, 0)
            snapshot = threaded_client.event_pump_snapshot()
            self.assertGreater(snapshot["pump_count"], 0)
            self.assertIsInstance(snapshot["last_pump_monotonic"], float)
            self.assertEqual(snapshot["pump_interval_seconds"], 0.001)
        finally:
            threaded_client.close()

    def test_threaded_indi_client_task_lifecycle_logs_are_debug(self):
        with patch("homestock.indi.threaded.ops_log") as startup_log:
            threaded_client = ThreadedIndiClient(
                MockIndiClient,
                startup_timeout=2.0,
                call_timeout=2.0,
                pump_interval=0.001,
            )
            try:
                threaded_client.list_stocks()
            finally:
                threaded_client.close()

        lifecycle_messages = {
            "task queued",
            "task start",
            "task success",
            "task result received",
        }
        matched = [
            call
            for call in startup_log.call_args_list
            if len(call.args) >= 2 and any(message in call.args[1] for message in lifecycle_messages)
        ]

        self.assertEqual(len(matched), 4)
        self.assertTrue(all(call.kwargs.get("level") == "debug" for call in matched))

    def test_threaded_indi_client_rt_dispatch_lifecycle_logs_are_debug(self):
        with patch("homestock.indi.threaded.ops_log") as startup_log:
            threaded_client = ThreadedIndiClient(
                MockIndiClient,
                startup_timeout=2.0,
                call_timeout=2.0,
                pump_interval=0.001,
            )
            try:
                threaded_client._call("emit_rt_event", {"rt_type": "N0", "code": "005930"})
                time.sleep(0.05)
            finally:
                threaded_client.close()

        lifecycle_messages = {
            "RT event queued",
            "RT event dispatch begin",
            "RT event dispatch complete",
        }
        matched = [
            call
            for call in startup_log.call_args_list
            if len(call.args) >= 2 and any(message in call.args[1] for message in lifecycle_messages)
        ]

        self.assertEqual(len(matched), 3)
        self.assertTrue(all(call.kwargs.get("level") == "debug" for call in matched))

    def test_threaded_indi_client_cancels_time_critical_call_before_execution_after_timeout(self):
        worker_blocked = threading.Event()
        release_worker = threading.Event()
        place_order_called = threading.Event()

        class BlockingClient(MockIndiClient):
            def list_stocks(self):
                worker_blocked.set()
                release_worker.wait(1.0)
                return super().list_stocks()

            def place_order(self, request: OrderRequest):
                place_order_called.set()
                return super().place_order(request)

        threaded_client = ThreadedIndiClient(
            BlockingClient,
            startup_timeout=2.0,
            call_timeout=0.05,
            pump_interval=0.001,
        )
        request = OrderRequest(
            account_no="12345678901",
            code="005930",
            side="buy",
            quantity=1,
            price=70000,
        )
        def block_worker() -> None:
            try:
                threaded_client.list_stocks()
            except TimeoutError:
                pass

        worker_call = threading.Thread(target=block_worker)
        try:
            worker_call.start()
            self.assertTrue(worker_blocked.wait(1.0))

            with self.assertRaisesRegex(TimeoutError, "time-critical call timed out before execution"):
                threaded_client.place_order(request)

            release_worker.set()
            worker_call.join(1.0)
            self.assertFalse(place_order_called.wait(0.2))
        finally:
            release_worker.set()
            threaded_client.close()

    def test_threaded_indi_client_waits_for_time_critical_call_after_execution_starts(self):
        place_order_started = threading.Event()

        class SlowOrderClient(MockIndiClient):
            def place_order(self, request: OrderRequest):
                place_order_started.set()
                time.sleep(0.12)
                return super().place_order(request)

        threaded_client = ThreadedIndiClient(
            SlowOrderClient,
            startup_timeout=2.0,
            call_timeout=0.05,
            pump_interval=0.001,
        )
        request = OrderRequest(
            account_no="12345678901",
            code="005930",
            side="buy",
            quantity=1,
            price=70000,
        )
        try:
            result = threaded_client.place_order(request)

            self.assertTrue(place_order_started.is_set())
            self.assertTrue(result.accepted)
            self.assertTrue(result.order_id.startswith("mock-place-"))
        finally:
            threaded_client.close()

    def test_threaded_indi_client_close_rejects_new_calls(self):
        threaded_client = ThreadedIndiClient(
            MockIndiClient,
            startup_timeout=2.0,
            call_timeout=2.0,
            pump_interval=0.001,
        )
        threaded_client.close()

        with self.assertRaisesRegex(RuntimeError, "INDI worker is closing"):
            threaded_client.list_stocks()

    def test_threaded_indi_client_close_cancels_queued_calls(self):
        worker_blocked = threading.Event()
        release_worker = threading.Event()
        queued_done = threading.Event()
        queued_errors: list[BaseException] = []

        class BlockingClient(MockIndiClient):
            def list_stocks(self):
                worker_blocked.set()
                release_worker.wait(2.0)
                return super().list_stocks()

        threaded_client = ThreadedIndiClient(
            BlockingClient,
            startup_timeout=2.0,
            call_timeout=5.0,
            pump_interval=0.001,
        )

        def block_worker() -> None:
            try:
                threaded_client.list_stocks()
            except RuntimeError:
                pass

        def queued_call() -> None:
            try:
                threaded_client.get_accounts()
            except BaseException as exc:
                queued_errors.append(exc)
            finally:
                queued_done.set()

        worker_call = threading.Thread(target=block_worker)
        close_call = threading.Thread(target=threaded_client.close)
        try:
            worker_call.start()
            self.assertTrue(worker_blocked.wait(1.0))

            queued_thread = threading.Thread(target=queued_call)
            queued_thread.start()
            time.sleep(0.05)

            close_call.start()
            self.assertTrue(queued_done.wait(0.5))
            self.assertIsInstance(queued_errors[0], RuntimeError)
            self.assertIn("INDI worker is closing", str(queued_errors[0]))
        finally:
            release_worker.set()
            worker_call.join(1.0)
            close_call.join(1.0)

    def test_threaded_indi_client_close_runs_client_cleanup_on_worker_thread(self):
        cleanup_done = threading.Event()
        created: dict[str, MockIndiClient] = {}

        class ClosingClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.owner_thread_id = threading.get_ident()
                self.close_thread_id: int | None = None
                self.unregister_thread_id: int | None = None

            def unregister_rt_listener(self, listener):
                self.unregister_thread_id = threading.get_ident()
                super().unregister_rt_listener(listener)

            def close(self) -> None:
                self.close_thread_id = threading.get_ident()
                cleanup_done.set()

        def create_client() -> ClosingClient:
            client = ClosingClient()
            created["client"] = client
            return client

        threaded_client = ThreadedIndiClient(
            create_client,
            startup_timeout=2.0,
            call_timeout=2.0,
            pump_interval=0.001,
        )

        threaded_client.close()

        client = created["client"]
        self.assertTrue(cleanup_done.wait(1.0))
        self.assertEqual(client.close_thread_id, client.owner_thread_id)
        self.assertEqual(client.unregister_thread_id, client.owner_thread_id)

    def test_startup_failure_dispatches_system_callback_without_restoring_subscriptions(self):
        class StartupFailingClient(MockIndiClient):
            def health_check(self, live_orders_allowed: bool) -> HealthStatus:
                return HealthStatus(
                    ok=False,
                    backend="real",
                    python_architecture="32bit",
                    ocx_ready=True,
                    login_ready=False,
                    live_orders_allowed=live_orders_allowed,
                    message="real backend probe failed",
                    indi_process_running=True,
                    indi_process_restarted=False,
                    indi_process_message="GiExpertMain.exe unchanged",
                )

        with TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "subscription_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "updated_at": "",
                        "subscriptions": {
                            "disclosures": [
                                {
                                    "subscription_id": "disc_1",
                                    "code": "005930",
                                    "httpCallback": {"method": "POST", "url": "http://localhost:9000/disc"},
                                }
                            ],
                            "news": [
                                {
                                    "subscription_id": "news_1",
                                    "types": ["A"],
                                    "code": None,
                                    "httpCallback": {"method": "POST", "url": "http://localhost:9000/news"},
                                }
                            ],
                        },
                        "system_callbacks": [
                            {
                                "system_callback_id": "sys_1",
                                "httpCallback": {"method": "POST", "url": "http://localhost:9000/system"},
                                "registered_at": "20260428000000",
                                "last_event_at": None,
                                "sent_event_count": 0,
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            client = StartupFailingClient()
            scripter = InProcessScripter()
            try:
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.return_value = {"delivered": True, "error": None}
                    with self.assertRaisesRegex(RuntimeError, "MCP 서버 구동 실패"):
                        HomestockTools(client, OrderGuard(False), runtime_state_dir=tmp, scripter=scripter)
                    self.assertTrue(wait_for_scripter_idle(scripter))
            finally:
                scripter.close()

            self.assertFalse(client._disclosure_feed_active)
            self.assertFalse(client._news_feed_active)
            self.assertEqual(client._rt_listeners, [])
            self.assertEqual(dispatch.call_count, 1)
            callback = dispatch.call_args.args[1]
            self.assertEqual(callback.body["event_type"], "mcp_startup_failed")
            self.assertIn("real backend probe failed", callback.body["message"])

    def test_real_news_register_uses_qvariant_pair_register(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {}
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        calls: list[tuple[str, tuple[object, ...]]] = []

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 1
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        client._register_news_realtime()

        self.assertEqual(
            calls,
            [
                ("RequestRTReg(QVariant, QVariant)", ("N0", "*")),
            ],
        )

    def test_real_news_feed_skips_request_rt_when_already_registered(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {("N0", ""): 1}
        client._rt_news_registered = True
        client._rt_disclosure_registered = False
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        calls: list[tuple[str, tuple[object, ...]]] = []

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 1
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        with patch("homestock.indi.real.ops_log") as startup_log:
            result = client.subscribe_news_feed()

        self.assertTrue(result["already_subscribed"])
        self.assertTrue(result["already_indi_registered"])
        self.assertFalse(result["rt_news_registered_now"])
        self.assertEqual(client._rt_subscription_counts[("N0", "")], 1)
        self.assertEqual(calls, [])

    def test_real_disclosure_feed_skips_request_rt_when_already_registered(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {("N2", ""): 1}
        client._rt_news_registered = False
        client._rt_disclosure_registered = True
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        calls: list[tuple[str, tuple[object, ...]]] = []

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 1
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        result = client.subscribe_disclosure_feed("005930")

        self.assertTrue(result["already_subscribed"])
        self.assertTrue(result["already_indi_registered"])
        self.assertFalse(result["rt_disclosure_registered_now"])
        self.assertEqual(client._rt_subscription_counts[("N2", "")], 1)
        self.assertEqual(calls, [])

    def test_real_news_feed_marks_new_request_rt_success(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {}
        client._rt_news_registered = False
        client._rt_disclosure_registered = False
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        calls: list[tuple[str, tuple[object, ...]]] = []

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 1
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        with patch("homestock.indi.real.ops_log") as startup_log:
            result = client.subscribe_news_feed()

        self.assertFalse(result["already_subscribed"])
        self.assertFalse(result["already_indi_registered"])
        self.assertTrue(result["rt_news_registered_now"])
        self.assertTrue(client._rt_news_registered)
        self.assertEqual(client._rt_subscription_counts[("N0", "")], 1)
        self.assertEqual(
            calls,
            [
                ("RequestRTReg(QVariant, QVariant)", ("N0", "*")),
            ],
        )

    def test_real_disclosure_feed_marks_new_request_rt_success(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {}
        client._rt_news_registered = False
        client._rt_disclosure_registered = False
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        calls: list[tuple[str, tuple[object, ...]]] = []

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 1
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        result = client.subscribe_disclosure_feed("005930")

        self.assertFalse(result["already_subscribed"])
        self.assertFalse(result["already_indi_registered"])
        self.assertTrue(result["rt_disclosure_registered_now"])
        self.assertTrue(client._rt_disclosure_registered)
        self.assertEqual(client._rt_subscription_counts[("N2", "")], 1)
        self.assertEqual(
            calls,
            [
                ("RequestRTReg(QVariant, QVariant)", ("N2", "*")),
            ],
        )

    def test_real_news_feed_accepts_gi005_request_rt_warning(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {}
        client._rt_news_registered = False
        client._rt_disclosure_registered = False
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        def capture_error(rt_type: str, code: str, method_name: str, attempted_signature: str, exception: str | None = None) -> None:
            client._last_rt_error_details = {
                "rt_type": rt_type,
                "code": code,
                "method_name": method_name,
                "attempted_signature": attempted_signature,
                "error_state": 2,
                "error_code": "GI005",
                "error_message": "realtime data parse error",
                "sys_msgs": [],
            }
            if exception:
                client._last_rt_error_details["exception"] = exception

        client._capture_rt_error_details = Mock(side_effect=capture_error)

        def dynamic_call(signature: str, *args: object) -> object:
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 0
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        with patch("homestock.indi.real.ops_log") as startup_log:
            result = client.subscribe_news_feed()

        self.assertFalse(result["already_subscribed"])
        self.assertFalse(result["already_indi_registered"])
        self.assertTrue(result["rt_news_registered_now"])
        self.assertTrue(client._rt_news_registered)
        self.assertEqual(client._rt_subscription_counts[("N0", "")], 1)
        client._capture_rt_error_details.assert_called_once_with("N0", "*", "RequestRTReg", "RequestRTReg(QVariant, QVariant)")
        self.assertTrue(any("GI005 warning accepted" in call.args[1] for call in startup_log.call_args_list))

    def test_real_disclosure_feed_accepts_gi005_request_rt_warning(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {}
        client._rt_news_registered = False
        client._rt_disclosure_registered = False
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        def capture_error(rt_type: str, code: str, method_name: str, attempted_signature: str, exception: str | None = None) -> None:
            client._last_rt_error_details = {
                "rt_type": rt_type,
                "code": code,
                "method_name": method_name,
                "attempted_signature": attempted_signature,
                "error_state": 2,
                "error_code": "GI005",
                "error_message": "realtime data parse error",
                "sys_msgs": [],
            }
            if exception:
                client._last_rt_error_details["exception"] = exception

        client._capture_rt_error_details = Mock(side_effect=capture_error)

        def dynamic_call(signature: str, *args: object) -> object:
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 0
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        with patch("homestock.indi.real.ops_log") as startup_log:
            result = client.subscribe_disclosure_feed("005930")

        self.assertFalse(result["already_subscribed"])
        self.assertFalse(result["already_indi_registered"])
        self.assertTrue(result["rt_disclosure_registered_now"])
        self.assertTrue(client._rt_disclosure_registered)
        self.assertEqual(client._rt_subscription_counts[("N2", "")], 1)
        client._capture_rt_error_details.assert_called_once_with("N2", "*", "RequestRTReg", "RequestRTReg(QVariant, QVariant)")
        self.assertTrue(any("GI005 warning accepted" in call.args[1] for call in startup_log.call_args_list))

    def test_real_price_subscribe_accepts_gi005_request_rt_warning(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {}
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._wait_for_rt_snapshot = Mock()
        client._last_rt_error_details = None

        def capture_error(rt_type: str, code: str, method_name: str, attempted_signature: str, exception: str | None = None) -> None:
            client._last_rt_error_details = {
                "rt_type": rt_type,
                "code": code,
                "method_name": method_name,
                "attempted_signature": attempted_signature,
                "error_state": 2,
                "error_code": "GI005",
                "error_message": "realtime data parse error",
                "sys_msgs": [],
            }
            if exception:
                client._last_rt_error_details["exception"] = exception

        client._capture_rt_error_details = Mock(side_effect=capture_error)

        def dynamic_call(signature: str, *args: object) -> object:
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 0
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        with patch("homestock.indi.real.ops_log") as startup_log:
            result = client.subscribe_realtime_price("000660")

        self.assertEqual(result["code"], "000660")
        self.assertFalse(result["already_subscribed"])
        self.assertEqual(client._rt_subscription_counts[("UC", "000660")], 1)
        client._wait_for_rt_snapshot.assert_not_called()
        client._capture_rt_error_details.assert_called_once_with("UC", "000660", "RequestRTReg", "RequestRTReg(QVariant, QVariant)")
        self.assertTrue(any("GI005 warning accepted" in call.args[1] for call in startup_log.call_args_list))

    def test_real_uc_rt_event_parses_as_integrated_stock_price(self):
        client = RealIndiClient.__new__(RealIndiClient)
        fields = [""] * 27
        fields[1] = "A005930"
        fields[2] = "083000"
        fields[3] = "2"
        fields[4] = "71600"
        fields[5] = "2"
        fields[6] = "1200"
        fields[7] = "1.70"
        fields[8] = "123456"
        fields[26] = "1"

        event = client._build_rt_event("UC", fields)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event["rt_type"], "UC")
        self.assertEqual(event["code"], "005930")
        self.assertEqual(event["current_price"], 71600.0)
        self.assertEqual(event["change_percent"], 1.7)
        self.assertEqual(event["raw_fields"][3], "2")
        self.assertEqual(event["uc_field_count"], 27)
        self.assertEqual(event["uc_info_cls_raw"], "2")
        self.assertTrue(event["uc_info_cls_present"])

    def test_real_uc_rt_event_marks_missing_info_cls_for_26_field_shape(self):
        client = RealIndiClient.__new__(RealIndiClient)
        fields = [""] * 26
        fields[1] = "005930"
        fields[3] = "71600"

        event = client._build_rt_event("UC", fields)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event["uc_field_count"], 26)
        self.assertEqual(event["uc_info_cls_raw"], "")
        self.assertFalse(event["uc_info_cls_present"])

    def test_real_get_accounts_captures_account_list_before_metadata_queries(self):
        client = RealIndiClient.__new__(RealIndiClient)
        active_buffer = {"name": "AccountList"}
        account_rows = [
            ["11111111111", "01"],
            ["22222222222", "10"],
            ["33333333333", "11"],
            ["44444444444", "21"],
        ]

        def fake_multi_text(row: int, col: int) -> str:
            if active_buffer["name"] == "AccountList":
                return account_rows[row][col]
            return "metadata-buffer"

        def fake_metadata(account_no: str, account_password: str) -> dict[str, str]:
            del account_password
            active_buffer["name"] = f"metadata:{account_no}"
            return {}

        client._request = Mock(return_value=SimpleNamespace(multi_row_count=len(account_rows)))
        client._multi_text = Mock(side_effect=fake_multi_text)
        client._get_account_metadata = Mock(side_effect=fake_metadata)

        with patch.dict("os.environ", {client.ACCOUNT_PASSWORD_ENV: "1234"}):
            accounts = client.get_accounts()

        self.assertEqual([account.account_no for account in accounts], [row[0] for row in account_rows])
        self.assertEqual([account.name for account in accounts], [row[1] for row in account_rows])

    def test_real_news_feed_does_not_mark_count_when_request_rt_fails(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {}
        client._rt_news_registered = False
        client._rt_disclosure_registered = False
        client._rt_snapshots = {}
        client._reset_rt_wait_state = Mock()
        client._capture_rt_error_details = Mock()
        client._last_rt_error_details = None

        def dynamic_call(signature: str, *args: object) -> object:
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 0
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        with self.assertRaisesRegex(RuntimeError, r"RequestRTReg failed: N0 \*"):
            client.subscribe_news_feed()

        self.assertNotIn(("N0", ""), client._rt_subscription_counts)
        self.assertFalse(client._rt_news_registered)

    def test_real_news_unsubscribe_uses_qstring_pair_unregister(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {("N0", ""): 1}
        client._rt_news_registered = True
        client._rt_disclosure_registered = False
        client._rt_snapshots = {("N0", ""): ["cached"]}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        calls: list[tuple[str, tuple[object, ...]]] = []

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "SetQueryName(QString)":
                return True
            if signature == "UnRequestRTReg(QString, QString)":
                return 1
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        result = client.unsubscribe_news_feed()

        self.assertTrue(result["was_subscribed"])
        self.assertNotIn(("N0", ""), client._rt_subscription_counts)
        self.assertNotIn(("N0", ""), client._rt_snapshots)
        self.assertFalse(client._rt_news_registered)
        self.assertEqual(
            calls,
            [
                ("SetQueryName(QString)", ("N0",)),
                ("UnRequestRTReg(QString, QString)", ("N0", "")),
            ],
        )

    def test_real_disclosure_unsubscribe_uses_qvariant_unregister(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control_lock = threading.Lock()
        client._rt_subscription_counts = {("N2", ""): 1}
        client._rt_news_registered = False
        client._rt_disclosure_registered = True
        client._rt_snapshots = {("N2", ""): ["cached"]}
        client._reset_rt_wait_state = Mock()
        client._last_rt_error_details = None

        calls: list[tuple[str, tuple[object, ...]]] = []

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "SetQueryName(QString)":
                return True
            if signature == "UnRequestRTReg(QVariant)":
                return 1
            raise AssertionError(f"unexpected signature: {signature} {args}")

        client._rt_control = Mock()
        client._rt_control.dynamicCall.side_effect = dynamic_call

        result = client.unsubscribe_disclosure_feed("005490")

        self.assertTrue(result["was_subscribed"])
        self.assertNotIn(("N2", ""), client._rt_subscription_counts)
        self.assertNotIn(("N2", ""), client._rt_snapshots)
        self.assertFalse(client._rt_disclosure_registered)
        relevant_calls = [
            item
            for item in calls
            if item[0].startswith("SetQueryName")
            or item[0].startswith("UnRequestRTReg")
        ]
        self.assertEqual(
            relevant_calls,
            [
                ("SetQueryName(QString)", ("N2",)),
                ("UnRequestRTReg(QVariant)", ("N2",)),
            ],
        )

    def test_real_indi_client_close_unregisters_rt_and_deletes_controls(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_subscription_counts = {("UC", "005930"): 1, ("N0", ""): 1, ("N2", ""): 1}
        client._rt_news_registered = True
        client._rt_disclosure_registered = True
        client._rt_snapshots = {("UC", "005930"): ["cached"], ("N0", ""): ["cached"], ("N2", ""): ["cached"]}
        client._rt_listeners = [Mock()]
        client._logger = Mock()
        client._app = Mock()
        client._tr_control = Mock()
        client._rt_control = Mock()
        client._unregister_realtime = Mock()
        client._unregister_news_realtime = Mock()
        client._unregister_disclosure_realtime = Mock()

        client.close()

        client._unregister_realtime.assert_called_once_with("UC", "005930")
        client._unregister_news_realtime.assert_called_once()
        client._unregister_disclosure_realtime.assert_called_once()
        self.assertEqual(client._rt_subscription_counts, {})
        self.assertFalse(client._rt_news_registered)
        self.assertFalse(client._rt_disclosure_registered)
        self.assertEqual(client._rt_snapshots, {})
        self.assertEqual(client._rt_listeners, [])
        client._tr_control.deleteLater.assert_called_once()
        client._rt_control.deleteLater.assert_called_once()
        client._app.processEvents.assert_called_once()

    def test_real_giexpert_main_generation_change_is_detected(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._giexpert_main_generation = ((100, 10),)
        client._giexpert_main_current_generation = ((100, 10),)
        client._giexpert_main_restarted = False
        client._giexpert_main_restart_message = ""
        client._capture_giexpert_main_generation = Mock(return_value=((200, 20),))

        with patch("homestock.indi.real.ops_log") as startup_log:
            result = client._check_giexpert_main_generation()

        self.assertTrue(result["running"])
        self.assertTrue(result["restarted"])
        self.assertIn("generation changed", result["message"])
        self.assertIn("pid=100", result["message"])
        self.assertIn("pid=200", result["message"])
        startup_log.assert_called_once()

    def test_real_giexpert_main_generation_restart_flag_is_sticky(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._giexpert_main_generation = ((100, 10),)
        client._giexpert_main_current_generation = ((100, 10),)
        client._giexpert_main_restarted = True
        client._giexpert_main_restart_message = "previous restart"
        client._capture_giexpert_main_generation = Mock(return_value=((100, 10),))

        with patch("homestock.indi.real.ops_log") as startup_log:
            result = client._check_giexpert_main_generation()

        self.assertTrue(result["running"])
        self.assertTrue(result["restarted"])
        self.assertEqual(result["message"], "previous restart")
        startup_log.assert_not_called()

    def test_subscribe_disclosure_persists_unified_state_file(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            result = tools.subscribe_disclosure("005930", {"method": "POST", "url": "http://localhost:9000/hook"})

            self.assertTrue(result["subscribed"])
            state_path = Path(tempdir) / "subscription_state.json"
            self.assertTrue(state_path.exists())
            payload = state_path.read_text(encoding="utf-8")
            self.assertIn('"disclosures"', payload)
            self.assertIn('"005930"', payload)
            self.assertIn('"registered_at"', payload)

    def test_duplicate_subscribe_disclosure_is_noop(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            first = tools.subscribe_disclosure("005930", {"method": "POST", "url": "http://localhost:9000/hook"})
            second = tools.subscribe_disclosure("005930", {"method": "POST", "url": "http://localhost:9000/hook"})

            self.assertFalse(first["already_subscribed"])
            self.assertFalse(first["already_indi_registered"])
            self.assertTrue(first["rt_disclosure_registered_now"])
            self.assertTrue(second["already_subscribed"])
            self.assertTrue(second["already_indi_registered"])
            self.assertFalse(second["rt_disclosure_registered_now"])
            self.assertEqual(first["subscription_id"], second["subscription_id"])
            self.assertTrue(second["rt_subscriptions"]["N2"]["active"])
            self.assertIn("N0", second["rt_subscriptions"])

    def test_duplicate_subscribe_disclosure_reattaches_inactive_feed(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            callback = {"method": "POST", "url": "http://localhost:9000/hook"}
            first = tools.subscribe_disclosure("005930", callback)
            tools._runtime_state._disclosure_feed_active = False
            tools._client._disclosure_feed_active = False

            with patch.object(tools._client, "subscribe_disclosure_feed", wraps=tools._client.subscribe_disclosure_feed) as subscribe_feed:
                second = tools.subscribe_disclosure("005930", callback)

            self.assertTrue(second["already_subscribed"])
            self.assertFalse(second["already_indi_registered"])
            self.assertTrue(second["rt_disclosure_registered_now"])
            self.assertEqual(first["subscription_id"], second["subscription_id"])
            self.assertEqual(subscribe_feed.call_count, 1)
            self.assertTrue(tools._runtime_state._disclosure_feed_active)
            self.assertTrue(second["rt_subscriptions"]["N2"]["active"])

    def test_subscribe_news_normalizes_types(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            result = tools.subscribe_news(["f", "Y", "F"], {"method": "POST", "url": "http://localhost:9000/hook"}, "005930")

            self.assertEqual(result["types"], ["F", "Y"])
            self.assertEqual(result["code"], "005930")
            payload = (Path(tempdir) / "subscription_state.json").read_text(encoding="utf-8")
            self.assertIn('"registered_at"', payload)

    def test_duplicate_subscribe_news_reattaches_inactive_feed(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            callback = {"method": "POST", "url": "http://localhost:9000/hook"}
            first = tools.subscribe_news(["F"], callback)
            tools._runtime_state._news_feed_active = False
            tools._client._news_feed_active = False

            with patch.object(tools._client, "subscribe_news_feed", wraps=tools._client.subscribe_news_feed) as subscribe_feed:
                second = tools.subscribe_news(["F"], callback)

            self.assertTrue(second["already_subscribed"])
            self.assertFalse(second["already_indi_registered"])
            self.assertTrue(second["rt_news_registered_now"])
            self.assertEqual(first["subscription_id"], second["subscription_id"])
            self.assertEqual(subscribe_feed.call_count, 1)
            self.assertTrue(tools._runtime_state._news_feed_active)
            self.assertTrue(second["rt_subscriptions"]["N0"]["active"])

    def test_list_subscription_tools_return_persistent_items(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            tools.subscribe_disclosure("005930", {"method": "POST", "url": "http://localhost:9000/hook"})
            tools.subscribe_news(["F", "Y"], {"method": "POST", "url": "http://localhost:9000/hook"}, "005930")

            disclosures = tools.list_disclosure_subscriptions()
            news = tools.list_news_subscriptions()

            self.assertEqual(disclosures[0]["code"], "005930")
            self.assertEqual(disclosures[0]["name"], "Samsung Electronics")
            self.assertIn("subscription_id", disclosures[0])
            self.assertIn("registered_at", disclosures[0])
            self.assertIn("last_event_at", disclosures[0])
            self.assertEqual(disclosures[0]["evaluated_event_count"], 0)
            self.assertEqual(news[0]["types"], ["F", "Y"])
            self.assertEqual(news[0]["code"], "005930")
            self.assertEqual(news[0]["name"], "Samsung Electronics")
            self.assertIn("subscription_id", news[0])
            self.assertIn("registered_at", news[0])
            self.assertIn("last_event_at", news[0])
            self.assertEqual(news[0]["evaluated_event_count"], 0)

    def test_register_list_and_unregister_system_callback(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            created = tools.register_system_callback({"method": "POST", "url": "http://localhost:9000/hook"})
            items = tools.list_system_callbacks()

            self.assertTrue(created["registered"])
            self.assertIn("system_callback_id", created)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["system_callback_id"], created["system_callback_id"])
            self.assertNotIn("sent_event_count", items[0])
            self.assertNotIn("last_event_at", items[0])
            tools._runtime_state.dispatch_system_event("sample_event", "sample message")
            items_after_dispatch = tools.list_system_callbacks()
            self.assertNotIn("sent_event_count", items_after_dispatch[0])
            self.assertNotIn("last_event_at", items_after_dispatch[0])

            removed = tools.unregister_system_callback(created["system_callback_id"])
            self.assertEqual(removed["removed_callbacks"], 1)
            self.assertEqual(tools.list_system_callbacks(), [])

    def test_register_system_callback_syncs_scripter_config(self):
        with TemporaryDirectory() as tempdir:
            scripter = MockScripter()
            tools = make_tools(runtime_state_dir=tempdir, scripter=scripter)

            created = tools.register_system_callback({"method": "POST", "url": "http://localhost:9000/hook"})

            self.assertEqual(scripter.system_callback_configs[0], [])
            self.assertEqual(len(scripter.system_callback_configs[-1]), 1)
            self.assertEqual(
                scripter.system_callback_configs[-1][0]["system_callback_id"],
                created["system_callback_id"],
            )

    def test_list_system_callbacks_returns_deep_copied_http_callback(self):
        with TemporaryDirectory() as tempdir:
            scripter = MockScripter()
            tools = make_tools(runtime_state_dir=tempdir, scripter=scripter)
            tools.register_system_callback(
                {
                    "method": "POST",
                    "url": "http://localhost:9000/hook",
                    "headers": {"Authorization": "Bearer secret"},
                }
            )

            listed = tools.list_system_callbacks()
            listed[0]["httpCallback"]["url"] = "http://localhost:9000/mutated"
            listed[0]["httpCallback"]["headers"]["Authorization"] = "mutated"

            listed_again = tools.list_system_callbacks()
            self.assertEqual(listed_again[0]["httpCallback"]["url"], "http://localhost:9000/hook")
            self.assertEqual(listed_again[0]["httpCallback"]["headers"]["Authorization"], "Bearer secret")

    def test_register_system_callback_raises_when_scripter_config_sync_fails(self):
        class FailingConfigScripter(MockScripter):
            def __init__(self) -> None:
                super().__init__()
                self.fail_config_sync = False

            def configure_system_callbacks(self, callbacks):
                if self.fail_config_sync:
                    raise RuntimeError("config sync failed")
                super().configure_system_callbacks(callbacks)

        with TemporaryDirectory() as tempdir:
            scripter = FailingConfigScripter()
            tools = make_tools(runtime_state_dir=tempdir, scripter=scripter)
            scripter.fail_config_sync = True

            with self.assertRaisesRegex(RuntimeError, "config sync failed"):
                tools.register_system_callback({"method": "POST", "url": "http://localhost:9000/hook"})

            self.assertEqual(tools.list_system_callbacks(), [])

    def test_register_system_callback_rolls_back_scripter_config_when_persist_fails(self):
        with TemporaryDirectory() as tempdir:
            scripter = MockScripter()
            tools = make_tools(runtime_state_dir=tempdir, scripter=scripter)

            with patch.object(tools._runtime_state, "_persist_subscriptions", side_effect=OSError("disk full")):
                with self.assertRaisesRegex(OSError, "disk full"):
                    tools.register_system_callback({"method": "POST", "url": "http://localhost:9000/hook"})

            self.assertEqual(tools.list_system_callbacks(), [])
            self.assertEqual(scripter.system_callback_configs[-1], [])

    def test_persistent_subscription_store_atomic_save_keeps_old_file_on_replace_failure(self):
        with TemporaryDirectory() as tempdir:
            store = PersistentSubscriptionStore(Path(tempdir))
            old_state = {
                "version": 1,
                "updated_at": "",
                "subscriptions": {"disclosures": [], "news": []},
                "system_callbacks": [
                    {
                        "system_callback_id": "old",
                        "httpCallback": {"method": "POST", "url": "http://localhost:9000/old"},
                    }
                ],
            }
            new_state = {
                "version": 1,
                "updated_at": "",
                "subscriptions": {"disclosures": [], "news": []},
                "system_callbacks": [
                    {
                        "system_callback_id": "new",
                        "httpCallback": {"method": "POST", "url": "http://localhost:9000/new"},
                    }
                ],
            }
            store.save(old_state)

            with patch("homestock.runtime_state.os.replace", side_effect=OSError("replace failed")):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    store.save(new_state)

            loaded = json.loads(store.path().read_text(encoding="utf-8"))
            self.assertEqual(loaded["system_callbacks"][0]["system_callback_id"], "old")

    def test_unregister_system_callback_syncs_empty_scripter_config(self):
        with TemporaryDirectory() as tempdir:
            scripter = MockScripter()
            tools = make_tools(runtime_state_dir=tempdir, scripter=scripter)
            created = tools.register_system_callback({"method": "POST", "url": "http://localhost:9000/hook"})

            removed = tools.unregister_system_callback(created["system_callback_id"])

            self.assertEqual(removed["removed_callbacks"], 1)
            self.assertEqual(scripter.system_callback_configs[-1], [])

    def test_unregister_system_callback_raises_when_scripter_config_sync_fails(self):
        class FailingConfigScripter(MockScripter):
            def __init__(self) -> None:
                super().__init__()
                self.fail_config_sync = False

            def configure_system_callbacks(self, callbacks):
                if self.fail_config_sync:
                    raise RuntimeError("config sync failed")
                super().configure_system_callbacks(callbacks)

        with TemporaryDirectory() as tempdir:
            scripter = FailingConfigScripter()
            tools = make_tools(runtime_state_dir=tempdir, scripter=scripter)
            created = tools.register_system_callback({"method": "POST", "url": "http://localhost:9000/hook"})
            scripter.fail_config_sync = True

            with self.assertRaisesRegex(RuntimeError, "config sync failed"):
                tools.unregister_system_callback(created["system_callback_id"])

            items = tools.list_system_callbacks()
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["system_callback_id"], created["system_callback_id"])
            self.assertEqual(len(scripter.system_callback_configs[-1]), 1)

    def test_unregister_system_callback_rolls_back_scripter_config_when_persist_fails(self):
        with TemporaryDirectory() as tempdir:
            scripter = MockScripter()
            tools = make_tools(runtime_state_dir=tempdir, scripter=scripter)
            created = tools.register_system_callback({"method": "POST", "url": "http://localhost:9000/hook"})

            with patch.object(tools._runtime_state, "_persist_subscriptions", side_effect=OSError("disk full")):
                with self.assertRaisesRegex(OSError, "disk full"):
                    tools.unregister_system_callback(created["system_callback_id"])

            items = tools.list_system_callbacks()
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["system_callback_id"], created["system_callback_id"])
            self.assertEqual(len(scripter.system_callback_configs[-1]), 1)
            self.assertEqual(
                scripter.system_callback_configs[-1][0]["system_callback_id"],
                created["system_callback_id"],
            )

    def test_close_unregisters_runtime_state_realtime_listener(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            client = tools._client
            assert isinstance(client, MockIndiClient)
            self.assertEqual(len(client._rt_listeners), 1)

            tools.close()

            self.assertEqual(client._rt_listeners, [])

    def test_runtime_state_close_retries_realtime_listener_unregister_after_failure(self):
        class FlakyUnregisterClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.fail_once = True

            def unregister_rt_listener(self, listener) -> None:
                if self.fail_once:
                    self.fail_once = False
                    raise RuntimeError("temporary unregister failure")
                super().unregister_rt_listener(listener)

        with TemporaryDirectory() as tempdir:
            client = FlakyUnregisterClient()
            runtime_state = RuntimeStateManager(client, tempdir)
            self.assertEqual(len(client._rt_listeners), 1)

            runtime_state.close()
            self.assertEqual(len(client._rt_listeners), 1)

            runtime_state.close()
            self.assertEqual(client._rt_listeners, [])

    def test_close_continues_cleanup_when_monitor_stop_fails(self):
        class ClosingClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.closed = False

            def close(self) -> None:
                self.closed = True

        with TemporaryDirectory() as tempdir:
            client = ClosingClient()
            scripter = MockScripter()
            tools = HomestockTools(client, OrderGuard(False), scripter, runtime_state_dir=tempdir)

            with patch.object(HomestockTools, "_stop_indi_process_monitor", side_effect=RuntimeError("stop failed")):
                tools.close()

            self.assertEqual(client._rt_listeners, [])
            self.assertTrue(client.closed)
            self.assertTrue(scripter.closed)

    def test_close_stops_indi_process_monitor_thread(self):
        class MonitoringClient(MockIndiClient):
            INDI_MAIN_PROCESS_NAME = "FakeIndiMain.exe"

            def __init__(self) -> None:
                super().__init__()
                self.monitor_called = threading.Event()

            def check_indi_process_status(self) -> dict[str, object]:
                self.monitor_called.set()
                return {"ok": True, "indi_process_restarted": False}

        with TemporaryDirectory() as tempdir:
            client = MonitoringClient()
            with patch.object(HomestockTools, "_indi_process_monitor_interval_seconds", return_value=0.01):
                tools = HomestockTools(
                    client,
                    OrderGuard(False),
                    MockScripter(),
                    runtime_state_dir=tempdir,
                )
                self.assertTrue(client.monitor_called.wait(1.0))
                thread = tools._indi_process_monitor_thread
                self.assertIsNotNone(thread)

                tools.close()

                self.assertFalse(thread.is_alive())

    def test_event_pump_snapshot_drives_scripter_heartbeat(self):
        class PumpSnapshotClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.pump_count = 0

            def event_pump_snapshot(self) -> dict[str, object]:
                self.pump_count += 1
                return {
                    "pump_count": self.pump_count,
                    "last_pump_monotonic": time.monotonic(),
                    "pump_interval_seconds": 0.1,
                    "worker_thread_alive": True,
                    "event_thread_alive": True,
                }

        with TemporaryDirectory() as tempdir:
            client = PumpSnapshotClient()
            scripter = MockScripter()
            with patch.object(HomestockTools, "_indi_event_pump_heartbeat_interval_seconds", return_value=0.01):
                tools = HomestockTools(
                    client,
                    OrderGuard(False),
                    scripter,
                    runtime_state_dir=tempdir,
                )
                try:
                    deadline = time.monotonic() + 1.0
                    while time.monotonic() < deadline and not scripter.heartbeats:
                        time.sleep(0.01)
                finally:
                    tools.close()

        self.assertTrue(scripter.heartbeats)
        self.assertEqual(scripter.heartbeats[0].event_type, "indi_event_pump")
        self.assertGreaterEqual(scripter.heartbeats[0].payload["pump_count"], 1)
        self.assertEqual(scripter.heartbeats[0].payload["pump_interval_seconds"], 0.1)
        self.assertTrue(scripter.heartbeats[0].payload["worker_thread_alive"])
        self.assertTrue(scripter.heartbeats[0].payload["event_thread_alive"])

    def test_indi_process_monitor_does_not_emit_scripter_heartbeat(self):
        class MonitoringClient(MockIndiClient):
            INDI_MAIN_PROCESS_NAME = "FakeIndiMain.exe"

            def __init__(self) -> None:
                super().__init__()
                self.monitor_called = threading.Event()

            def check_indi_process_status(self) -> dict[str, object]:
                self.monitor_called.set()
                return {"ok": True, "indi_process_restarted": False}

        with TemporaryDirectory() as tempdir:
            client = MonitoringClient()
            scripter = MockScripter()
            with patch.object(HomestockTools, "_indi_process_monitor_interval_seconds", return_value=0.01):
                tools = HomestockTools(
                    client,
                    OrderGuard(False),
                    scripter,
                    runtime_state_dir=tempdir,
                )
                try:
                    self.assertTrue(client.monitor_called.wait(1.0))
                    time.sleep(0.05)
                finally:
                    tools.close()

        self.assertEqual(scripter.heartbeats, [])

    def test_blocked_indi_process_monitor_does_not_touch_scripter_after_close(self):
        class SlowMonitoringClient(MockIndiClient):
            INDI_MAIN_PROCESS_NAME = "FakeIndiMain.exe"

            def __init__(self) -> None:
                super().__init__()
                self.monitor_entered = threading.Event()
                self.release_monitor = threading.Event()

            def check_indi_process_status(self) -> dict[str, object]:
                self.monitor_entered.set()
                self.release_monitor.wait(1.0)
                return {"ok": True, "indi_process_restarted": False}

        with TemporaryDirectory() as tempdir:
            client = SlowMonitoringClient()
            scripter = MockScripter()
            with patch.object(HomestockTools, "_indi_process_monitor_interval_seconds", return_value=0.01):
                tools = HomestockTools(
                    client,
                    OrderGuard(False),
                    scripter,
                    runtime_state_dir=tempdir,
                )
                self.assertTrue(client.monitor_entered.wait(1.0))

                tools.close()
                client.release_monitor.set()
                thread = tools._indi_process_monitor_thread
                self.assertIsNotNone(thread)
                thread.join(1.0)

            self.assertEqual(scripter.heartbeats, [])

    def test_system_callback_allows_template_body(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            created = tools.register_system_callback(
                {
                    "method": "POST",
                    "url": "http://localhost:9000/hook",
                    "body": {"tag": "{{tag}}", "name": "{{name}}"},
                }
            )

            self.assertTrue(created["registered"])

    def test_unsubscribe_disclosure_removes_only_target_subscription(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            first = tools.subscribe_disclosure(
                "005930",
                {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "a"}},
            )
            second = tools.subscribe_disclosure(
                "005930",
                {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "b"}},
            )

            result = tools.unsubscribe_disclosure(first["subscription_id"])
            remaining = tools.list_disclosure_subscriptions()

            self.assertEqual(result["removed_subscriptions"], 1)
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0]["subscription_id"], second["subscription_id"])

    def test_unsubscribe_news_removes_only_target_subscription(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            first = tools.subscribe_news(
                ["F"],
                {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "a"}},
                "005930",
            )
            second = tools.subscribe_news(
                ["F"],
                {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "b"}},
                "005930",
            )

            result = tools.unsubscribe_news(first["subscription_id"])
            remaining = tools.list_news_subscriptions()

            self.assertEqual(result["removed_subscriptions"], 1)
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0]["subscription_id"], second["subscription_id"])

    def test_unsubscribe_news_removes_state_without_external_feed_unsubscribe(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            created = tools.subscribe_news(
                ["F"],
                {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "a"}},
                None,
            )
            with patch.object(tools._client, "unsubscribe_news_feed", wraps=tools._client.unsubscribe_news_feed) as unsubscribe_feed:
                result = tools.unsubscribe_news(created["subscription_id"])

            remaining = tools.list_news_subscriptions()
            self.assertEqual(result["removed_subscriptions"], 1)
            self.assertEqual(remaining, [])
            unsubscribe_feed.assert_not_called()

    def test_rt_news_dispatch_serializes_unsubscribe_mutation(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            created = tools.subscribe_news(
                ["F"],
                {"method": "POST", "url": "http://localhost:9000/hook"},
                "005930",
            )
            client = tools._client
            assert isinstance(client, MockIndiClient)
            unsubscribe_started = threading.Event()
            unsubscribe_done = threading.Event()
            unsubscribe_errors: list[BaseException] = []
            unsubscribe_threads: list[threading.Thread] = []

            def unsubscribe_worker() -> None:
                unsubscribe_started.set()
                try:
                    tools.unsubscribe_news(created["subscription_id"])
                except BaseException as exc:
                    unsubscribe_errors.append(exc)
                finally:
                    unsubscribe_done.set()

            def dispatch_side_effect(_self, _callback):
                thread = threading.Thread(target=unsubscribe_worker)
                unsubscribe_threads.append(thread)
                thread.start()
                self.assertTrue(unsubscribe_started.wait(1.0))
                self.assertFalse(unsubscribe_done.wait(0.1))
                return {"delivered": True, "error": None}

            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                dispatch.side_effect = dispatch_side_effect
                client.emit_rt_event(
                    {
                        "rt_type": "N0",
                        "news_type": "F",
                        "news_type_label": "공정공시",
                        "code": "005930",
                        "date": "20260428",
                        "time": "120000",
                        "article_id": "NEWS1",
                        "title": "테스트 뉴스",
                    }
                )

            self.assertTrue(unsubscribe_done.wait(1.0))
            for thread in unsubscribe_threads:
                thread.join(1.0)
            self.assertEqual(unsubscribe_errors, [])
            self.assertEqual(tools.list_news_subscriptions(), [])

    def test_subscribe_news_failure_dispatches_system_callback(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            tools.register_system_callback(
                {
                    "method": "POST",
                    "url": "http://localhost:9000/hook",
                    "body": {
                        "tag": "{{tag}}",
                        "name": "{{name}}",
                        "callstack": "{{callstack}}",
                        "occurredAt": "{{occurred_at}}",
                    },
                }
            )

            dispatched_bodies: list[dict[str, object] | None] = []
            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                dispatch.side_effect = lambda _self, callback: (
                    dispatched_bodies.append(callback.body),
                    {"delivered": True, "error": None},
                )[1]
                with patch.object(tools._client, "subscribe_news_feed", side_effect=RuntimeError("RequestRTReg failed: N0 *")):
                    with patch.object(
                        tools._client,
                        "get_last_rt_error_details",
                        return_value={
                            "method_name": "RequestRTReg",
                            "attempted_signature": "RequestRTReg(QVariant, QVariant)",
                            "error_state": 3,
                            "error_code": "E789",
                            "error_message": "news subscribe failed",
                            "sys_msgs": ["9200"],
                        },
                    ):
                        with self.assertRaisesRegex(RuntimeError, r"RequestRTReg failed: N0 \*"):
                            tools.subscribe_news(
                                ["F"],
                                {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "a"}},
                                None,
                            )
                        self.assertTrue(wait_for_scripter_idle(tools))

            payload = dispatched_bodies[-1] or {}
            self.assertEqual(payload["tag"], "subscription_subscribe_failed")
            self.assertIn("뉴스 구독 등록 실패", payload["name"])
            self.assertIn("subscription_kind=news", payload["callstack"])
            self.assertIn("error=RequestRTReg failed: N0 *", payload["callstack"])
            self.assertIn("method_name=RequestRTReg", payload["callstack"])
            self.assertIn("attempted_signature=RequestRTReg(QVariant, QVariant)", payload["callstack"])
            self.assertIn("error_state=3", payload["callstack"])
            self.assertIn("error_code=E789", payload["callstack"])
            self.assertIn("error_message=news subscribe failed", payload["callstack"])
            self.assertIn("sys_msgs=9200", payload["callstack"])

    def test_unsubscribe_disclosure_removes_state_without_external_feed_unsubscribe(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            created = tools.subscribe_disclosure(
                "005930",
                {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "a"}},
            )
            with patch.object(
                tools._client,
                "unsubscribe_disclosure_feed",
                wraps=tools._client.unsubscribe_disclosure_feed,
            ) as unsubscribe_feed:
                result = tools.unsubscribe_disclosure(created["subscription_id"])

            remaining = tools.list_disclosure_subscriptions()
            self.assertEqual(result["removed_subscriptions"], 1)
            self.assertEqual(remaining, [])
            unsubscribe_feed.assert_not_called()

    def test_restore_news_failure_dispatches_system_callback(self):
        with TemporaryDirectory() as tempdir:
            migration_now = datetime(2026, 4, 27, 17, 45, 0, tzinfo=timezone(timedelta(hours=9)))
            persistent_path = Path(tempdir) / "subscription_state.json"
            persistent_payload = {
                "version": 1,
                "updated_at": "20260427165959",
                "subscriptions": {
                    "disclosures": [],
                    "news": [
                        {
                            "subscription_id": "news_sub_restore_test",
                            "types": ["F"],
                            "code": None,
                            "httpCallback": {"method": "POST", "url": "http://localhost:9000/hook"},
                            "registered_at": "20260427170000",
                            "last_event_at": None,
                            "evaluated_event_count": 0,
                        }
                    ],
                },
                "system_callbacks": [
                    {
                        "system_callback_id": "sys_cb_restore_test",
                        "httpCallback": {
                            "method": "POST",
                            "url": "http://localhost:9000/hook",
                            "body": {
                                "tag": "{{tag}}",
                                "name": "{{name}}",
                                "callstack": "{{callstack}}",
                                "occurredAt": "{{occurred_at}}",
                            },
                            "bodyFormat": "json",
                        },
                        "registered_at": "20260427170000",
                        "last_event_at": None,
                        "sent_event_count": 0,
                    }
                ],
            }
            persistent_path.write_text(json.dumps(persistent_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            dispatched_bodies: list[dict[str, object] | None] = []
            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                dispatch.side_effect = lambda _self, callback: (
                    dispatched_bodies.append(callback.body),
                    {"delivered": True, "error": None},
                )[1]
                with patch.object(MockIndiClient, "subscribe_news_feed", side_effect=RuntimeError("RequestRTReg failed: N0 *")) as subscribe_news_feed:
                    with patch.object(
                        MockIndiClient,
                        "get_last_rt_error_details",
                        return_value={
                            "method_name": "RequestRTReg",
                            "attempted_signature": "RequestRTReg(QVariant, QVariant)",
                            "error_state": 3,
                            "error_code": "E789",
                            "error_message": "news restore failed",
                            "sys_msgs": ["9201"],
                        },
                    ):
                        with (
                            patch("homestock.runtime_state._kst_now", return_value=migration_now),
                            patch("homestock.scripter._kst_now", return_value=migration_now),
                        ):
                            tools = make_tools(runtime_state_dir=tempdir)
                            self.assertTrue(wait_for_scripter_idle(tools))

            subscribe_news_feed.assert_called_once_with(None)
            self.assertEqual(len(dispatched_bodies), 1)
            self.assertEqual(dispatched_bodies[0]["tag"], "subscription_restore_failed")
            self.assertIn("뉴스 구독 복구 실패", dispatched_bodies[0]["name"])
            self.assertIn("RequestRTReg", dispatched_bodies[0]["callstack"])
            self.assertEqual(dispatched_bodies[0]["occurredAt"], "20260427174500")

    def test_restore_price_alert_failure_dispatches_system_callback_and_reraises(self):
        with TemporaryDirectory() as tempdir:
            migration_now = datetime(2026, 4, 27, 9, 15, 0, tzinfo=timezone(timedelta(hours=9)))
            runtime_path = Path(tempdir) / "subscribtion_state_20260427.json"
            runtime_payload = {
                "version": 1,
                "trading_date": "20260427",
                "updated_at": "20260427090000",
                "subscriptions": {"disclosures": [], "news": []},
                "price_alerts": [{"alert_id": "alert_restore_test", "code": "000660"}],
                "fall_safes": [],
            }
            runtime_path.write_text(json.dumps(runtime_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            persistent_path = Path(tempdir) / "subscription_state.json"
            persistent_payload = {
                "version": 1,
                "updated_at": "20260427090000",
                "subscriptions": {"disclosures": [], "news": []},
                "system_callbacks": [
                    {
                        "system_callback_id": "sys_cb_restore_price_test",
                        "httpCallback": {
                            "method": "POST",
                            "url": "http://localhost:9000/hook",
                            "body": {
                                "tag": "{{tag}}",
                                "name": "{{name}}",
                                "callstack": "{{callstack}}",
                                "occurredAt": "{{occurred_at}}",
                            },
                            "bodyFormat": "json",
                        },
                        "registered_at": "20260427090000",
                        "last_event_at": None,
                        "sent_event_count": 0,
                    }
                ],
            }
            persistent_path.write_text(json.dumps(persistent_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            dispatched_bodies: list[dict[str, object] | None] = []
            scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter"))
            try:
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, callback: (
                        dispatched_bodies.append(callback.body),
                        {"delivered": True, "error": None},
                    )[1]
                    with patch.object(MockIndiClient, "subscribe_realtime_price", side_effect=RuntimeError("RequestRTReg failed: SC 000660")) as subscribe_price:
                        with patch.object(
                            MockIndiClient,
                            "get_last_rt_error_details",
                            return_value={
                                "method_name": "RequestRTReg",
                                "attempted_signature": "RequestRTReg(QString, QString)",
                                "error_state": 3,
                                "error_code": "E123",
                                "error_message": "price restore failed",
                                "sys_msgs": ["9301"],
                            },
                        ):
                            with (
                                patch("homestock.runtime_state._kst_now", return_value=migration_now),
                                patch("homestock.scripter._kst_now", return_value=migration_now),
                            ):
                                with self.assertRaisesRegex(RuntimeError, "RequestRTReg failed: SC 000660"):
                                    make_tools(runtime_state_dir=tempdir, scripter=scripter)
                                self.assertTrue(wait_for_scripter_idle(scripter))
            finally:
                scripter.close()

            subscribe_price.assert_called_once_with("000660")
            self.assertEqual(len(dispatched_bodies), 1)
            self.assertEqual(dispatched_bodies[0]["tag"], "subscription_restore_failed")
            self.assertIn("가격 알람 구독 복구 실패", dispatched_bodies[0]["name"])
            self.assertIn("subscription_kind=price_alert", dispatched_bodies[0]["callstack"])
            self.assertIn("RequestRTReg", dispatched_bodies[0]["callstack"])
            self.assertEqual(dispatched_bodies[0]["occurredAt"], "20260427091500")

    def test_restore_stock_price_callback_failure_dispatches_system_callback_and_reraises(self):
        with TemporaryDirectory() as tempdir:
            migration_now = datetime(2026, 4, 27, 9, 15, 0, tzinfo=timezone(timedelta(hours=9)))
            runtime_path = Path(tempdir) / "subscribtion_state_20260427.json"
            runtime_payload = {
                "version": 1,
                "trading_date": "20260427",
                "updated_at": "20260427090000",
                "subscriptions": {"disclosures": [], "news": []},
                "price_alerts": [],
                "fall_safes": [],
                "stock_price_callbacks": [
                    {
                        "stock_price_callback_id": "stock_price_callback_restore_test",
                        "code": "000660",
                        "step": 500.0,
                        "httpCallback": {"method": "POST", "url": "http://localhost:9000/step"},
                        "registered_at": "20260427090000",
                        "last_price": None,
                        "baseline_price": None,
                        "last_direction": None,
                        "fired_count": 0,
                        "last_fired_at": None,
                    }
                ],
            }
            runtime_path.write_text(json.dumps(runtime_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            persistent_path = Path(tempdir) / "subscription_state.json"
            persistent_payload = {
                "version": 1,
                "updated_at": "20260427090000",
                "subscriptions": {"disclosures": [], "news": []},
                "system_callbacks": [
                    {
                        "system_callback_id": "sys_cb_restore_stock_callback_test",
                        "httpCallback": {
                            "method": "POST",
                            "url": "http://localhost:9000/hook",
                            "body": {
                                "tag": "{{tag}}",
                                "name": "{{name}}",
                                "callstack": "{{callstack}}",
                                "occurredAt": "{{occurred_at}}",
                            },
                            "bodyFormat": "json",
                        },
                        "registered_at": "20260427090000",
                        "last_event_at": None,
                        "sent_event_count": 0,
                    }
                ],
            }
            persistent_path.write_text(json.dumps(persistent_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            dispatched_bodies: list[dict[str, object] | None] = []
            scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter"))
            try:
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, callback: (
                        dispatched_bodies.append(callback.body),
                        {"delivered": True, "error": None},
                    )[1]
                    with patch.object(MockIndiClient, "subscribe_realtime_price", side_effect=RuntimeError("RequestRTReg failed: SC 000660")) as subscribe_price:
                        with patch.object(
                            MockIndiClient,
                            "get_last_rt_error_details",
                            return_value={
                                "method_name": "RequestRTReg",
                                "attempted_signature": "RequestRTReg(QString, QString)",
                                "error_state": 3,
                                "error_code": "E124",
                                "error_message": "stock callback restore failed",
                                "sys_msgs": ["9302"],
                            },
                        ):
                            with (
                                patch("homestock.runtime_state._kst_now", return_value=migration_now),
                                patch("homestock.scripter._kst_now", return_value=migration_now),
                            ):
                                with self.assertRaisesRegex(RuntimeError, "RequestRTReg failed: SC 000660"):
                                    make_tools(runtime_state_dir=tempdir, scripter=scripter)
                                self.assertTrue(wait_for_scripter_idle(scripter))
            finally:
                scripter.close()

            subscribe_price.assert_called_once_with("000660")
            self.assertEqual(len(dispatched_bodies), 1)
            self.assertEqual(dispatched_bodies[0]["tag"], "subscription_restore_failed")
            self.assertIn("주가 Step callback 구독 복구 실패", dispatched_bodies[0]["name"])
            self.assertIn("subscription_kind=stock_price_callback", dispatched_bodies[0]["callstack"])
            self.assertIn("RequestRTReg", dispatched_bodies[0]["callstack"])
            self.assertEqual(dispatched_bodies[0]["occurredAt"], "20260427091500")

    def test_restore_registers_disclosure_feed_before_news_feed(self):
        with TemporaryDirectory() as tempdir:
            persistent_path = Path(tempdir) / "subscription_state.json"
            persistent_payload = {
                "version": 1,
                "updated_at": "20260427165959",
                "subscriptions": {
                    "disclosures": [
                        {
                            "subscription_id": "disc_sub_restore_order",
                            "code": "005930",
                            "httpCallback": {"method": "POST", "url": "http://localhost:9000/disclosure"},
                            "registered_at": "20260427170000",
                            "last_event_at": None,
                            "evaluated_event_count": 0,
                        }
                    ],
                    "news": [
                        {
                            "subscription_id": "news_sub_restore_order",
                            "types": ["Y"],
                            "code": None,
                            "httpCallback": {"method": "POST", "url": "http://localhost:9000/news"},
                            "registered_at": "20260427170000",
                            "last_event_at": None,
                            "evaluated_event_count": 0,
                        }
                    ],
                },
                "system_callbacks": [],
            }
            persistent_path.write_text(json.dumps(persistent_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            calls: list[tuple[str, str | None]] = []

            def subscribe_news_feed(_client: MockIndiClient, code: str | None = None) -> dict[str, object]:
                calls.append(("N0", code))
                return {"subscribed": True, "rt_type": "N0"}

            def subscribe_disclosure_feed(_client: MockIndiClient, code: str) -> dict[str, object]:
                calls.append(("N2", code))
                return {"subscribed": True, "rt_type": "N2"}

            with patch.object(MockIndiClient, "subscribe_news_feed", autospec=True, side_effect=subscribe_news_feed):
                with patch.object(MockIndiClient, "subscribe_disclosure_feed", autospec=True, side_effect=subscribe_disclosure_feed):
                    make_tools(runtime_state_dir=tempdir)

            self.assertEqual(calls, [("N2", "005930"), ("N0", None)])

    def test_unsubscribe_disclosure_removes_state_when_rt_error_details_are_missing(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            created = tools.subscribe_disclosure(
                "005930",
                {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "a"}},
            )
            result = tools.unsubscribe_disclosure(created["subscription_id"])

            self.assertEqual(result["removed_subscriptions"], 1)
            self.assertEqual(tools.list_disclosure_subscriptions(), [])

    def test_subscribe_news_callback_body_replaces_supported_fields(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_bodies: list[dict[str, object] | None] = []

            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body)
                tools.subscribe_news(
                    ["F"],
                    {
                        "method": "POST",
                        "url": "http://localhost:9000/hook",
                        "body": {
                            "news_type": "{{news_type}}",
                            "category": "{{news_type_label}}",
                            "code": "{{code}}",
                            "name": "{{name}}",
                            "title": "{{title}}",
                            "status": "{{delete_flag_label}}",
                        },
                    },
                    "005930",
                )
                client = tools._client
                assert isinstance(client, MockIndiClient)
                client.emit_rt_event(
                    {
                        "rt_type": "N0",
                        "news_type": "F",
                        "news_type_label": "시황",
                        "date": "20260427",
                        "time": "153000",
                        "article_id": "356872",
                        "code": "005930",
                        "title": "반도체 업황 개선 기대감 확대",
                        "deleted_flag": "I",
                    }
                )

            self.assertEqual(
                dispatched_bodies,
                [
                    {
                        "news_type": "F",
                        "category": "시황",
                        "code": "005930",
                        "name": "Samsung Electronics",
                        "title": "반도체 업황 개선 기대감 확대",
                        "status": "normal",
                    }
                ],
            )
            subscriptions = tools.list_news_subscriptions()
            self.assertEqual(subscriptions[0]["last_event_at"], "20260427153000")
            self.assertEqual(subscriptions[0]["evaluated_event_count"], 1)

    def test_subscribe_news_dev_callback_reports_queueing_and_uses_test_replacements(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            rendered_bodies: list[dict[str, object] | None] = []

            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                def fake_dispatch(_self, callback):
                    rendered_bodies.append(callback.body)
                    return {"queued": True, "delivered": None, "error": None}

                dispatch.side_effect = fake_dispatch
                result = tools.subscribe_news(
                    ["F"],
                    {
                        "method": "POST",
                        "url": "http://localhost:9000/hook",
                        "body": {
                            "news_type": "{{news_type}}",
                            "category": "{{news_type_label}}",
                            "code": "{{code}}",
                            "name": "{{name}}",
                            "title": "{{title}}",
                            "status": "{{delete_flag_label}}",
                        },
                    },
                    "005930",
                    devCallback=True,
                )

            self.assertEqual(result["dev_callback"], {"attempted": True, "queued": True})
            self.assertEqual(
                rendered_bodies,
                [
                    {
                        "news_type": "F",
                        "category": "테스트",
                        "code": "005930",
                        "name": "Samsung Electronics",
                        "title": "뉴스 구독 테스트",
                        "status": "normal",
                    }
                ],
            )

    def test_subscribe_disclosure_callback_body_replaces_title_with_fallback(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_bodies: list[dict[str, object] | None] = []

            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body)
                tools.subscribe_disclosure(
                    "005930",
                    {
                        "method": "POST",
                        "url": "http://localhost:9000/hook",
                        "body": {
                            "disclosure_type": "{{disclosure_type}}",
                            "category": "{{disclosure_type_label}}",
                            "code": "{{code}}",
                            "name": "{{name}}",
                            "title": "{{title}}",
                            "status": "{{delete_flag_label}}",
                        },
                    },
                )
                client = tools._client
                assert isinstance(client, MockIndiClient)
                client.emit_rt_event(
                    {
                        "rt_type": "N2",
                        "news_type": "P",
                        "news_type_label": "공시",
                        "date": "20260427",
                        "time": "153100",
                        "article_id": "90001",
                        "code": "005930",
                        "title": "",
                        "deleted_flag": "",
                    }
                )

            self.assertEqual(
                dispatched_bodies,
                [
                    {
                        "disclosure_type": "P",
                        "category": "공시",
                        "code": "005930",
                        "name": "Samsung Electronics",
                        "title": "제목 없음",
                        "status": "normal",
                    }
                ],
            )
            subscriptions = tools.list_disclosure_subscriptions()
            self.assertEqual(subscriptions[0]["last_event_at"], "20260427153100")
            self.assertEqual(subscriptions[0]["evaluated_event_count"], 1)

    def test_subscribe_disclosure_dev_callback_reports_queue_failure_and_uses_test_replacements(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            rendered_bodies: list[dict[str, object] | None] = []

            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                def fake_dispatch(_self, callback):
                    rendered_bodies.append(callback.body)
                    return {"queued": False, "delivered": None, "error": "dispatcher closed"}

                dispatch.side_effect = fake_dispatch
                result = tools.subscribe_disclosure(
                    "005930",
                    {
                        "method": "POST",
                        "url": "http://localhost:9000/hook",
                        "body": {
                            "disclosure_type": "{{disclosure_type}}",
                            "category": "{{disclosure_type_label}}",
                            "code": "{{code}}",
                            "name": "{{name}}",
                            "title": "{{title}}",
                            "status": "{{delete_flag_label}}",
                        },
                    },
                    devCallback=True,
                )

            self.assertEqual(
                result["dev_callback"],
                {"attempted": True, "queued": False, "error": "dispatcher closed"},
            )
            self.assertEqual(
                rendered_bodies,
                [
                    {
                        "disclosure_type": "P",
                        "category": "테스트공시",
                        "code": "005930",
                        "name": "Samsung Electronics",
                        "title": "공시 구독 테스트",
                        "status": "normal",
                    }
                ],
            )

    def test_persistent_disclosure_state_restores_on_startup(self):
        with TemporaryDirectory() as tempdir:
            fake_now = datetime(2026, 4, 26, 10, 0, 0, tzinfo=timezone(timedelta(hours=9)))
            with patch("homestock.runtime_state._kst_now", return_value=fake_now):
                first_tools = make_tools(runtime_state_dir=tempdir)
                first_tools.subscribe_disclosure("005930", {"method": "POST", "url": "http://localhost:9000/hook"})

            next_day = datetime(2026, 4, 27, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))
            with patch("homestock.runtime_state._kst_now", return_value=next_day):
                restored_tools = make_tools(runtime_state_dir=tempdir)
                client = restored_tools._client
                assert isinstance(client, MockIndiClient)

                self.assertTrue(client._disclosure_feed_active)

    def test_disclosure_feed_registers_once_and_keeps_feed_active_after_last_listener(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with patch.object(tools._client, "subscribe_disclosure_feed", wraps=tools._client.subscribe_disclosure_feed) as subscribe_feed, patch.object(
                tools._client,
                "unsubscribe_disclosure_feed",
                wraps=tools._client.unsubscribe_disclosure_feed,
            ) as unsubscribe_feed:
                first = tools.subscribe_disclosure(
                    "005930",
                    {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "a"}},
                )
                second = tools.subscribe_disclosure(
                    "000660",
                    {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "b"}},
                )

                self.assertEqual(subscribe_feed.call_count, 1)
                self.assertFalse(first["already_subscribed"])
                self.assertFalse(second["already_subscribed"])
                self.assertFalse(first["already_indi_registered"])
                self.assertTrue(first["rt_disclosure_registered_now"])
                self.assertTrue(second["already_indi_registered"])
                self.assertFalse(second["rt_disclosure_registered_now"])
                self.assertTrue(second["rt_subscriptions"]["N2"]["active"])

                tools.unsubscribe_disclosure(first["subscription_id"])
                self.assertEqual(unsubscribe_feed.call_count, 0)

                tools.unsubscribe_disclosure(second["subscription_id"])
                self.assertEqual(unsubscribe_feed.call_count, 0)
                self.assertTrue(tools._runtime_state._disclosure_feed_active)

    def test_news_feed_registers_once_and_keeps_feed_active_after_last_listener(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with patch.object(tools._client, "subscribe_news_feed", wraps=tools._client.subscribe_news_feed) as subscribe_feed, patch.object(
                tools._client,
                "unsubscribe_news_feed",
                wraps=tools._client.unsubscribe_news_feed,
            ) as unsubscribe_feed:
                first = tools.subscribe_news(
                    ["F"],
                    {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "a"}},
                    "005930",
                )
                second = tools.subscribe_news(
                    ["Y"],
                    {"method": "POST", "url": "http://localhost:9000/hook", "body": {"tag": "b"}},
                    None,
                )

                self.assertEqual(subscribe_feed.call_count, 1)
                self.assertFalse(first["already_subscribed"])
                self.assertFalse(second["already_subscribed"])
                self.assertFalse(first["already_indi_registered"])
                self.assertTrue(first["rt_news_registered_now"])
                self.assertTrue(second["already_indi_registered"])
                self.assertFalse(second["rt_news_registered_now"])
                self.assertTrue(second["rt_subscriptions"]["N0"]["active"])

                tools.unsubscribe_news(first["subscription_id"])
                self.assertEqual(unsubscribe_feed.call_count, 0)

                tools.unsubscribe_news(second["subscription_id"])
                self.assertEqual(unsubscribe_feed.call_count, 0)
                self.assertTrue(tools._runtime_state._news_feed_active)

    def test_legacy_daily_subscription_state_migrates_with_migration_timestamp(self):
        with TemporaryDirectory() as tempdir:
            legacy_path = Path(tempdir) / "subscribtion_state_20260427.json"
            legacy_payload = {
                "version": 1,
                "trading_date": "20260427",
                "updated_at": "20260427165959",
                "subscriptions": {
                    "disclosures": [
                        {
                            "code": "005930",
                            "httpCallback": {"method": "POST", "url": "http://localhost:9000/hook"},
                        }
                    ],
                    "news": [
                        {
                            "types": ["F", "Y"],
                            "code": "005930",
                            "httpCallback": {"method": "POST", "url": "http://localhost:9000/hook"},
                        }
                    ],
                },
                "price_alerts": [],
                "fall_safes": [],
            }
            legacy_path.write_text(json.dumps(legacy_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            migration_now = datetime(2026, 4, 27, 17, 45, 0, tzinfo=timezone(timedelta(hours=9)))
            with patch("homestock.runtime_state._kst_now", return_value=migration_now):
                tools = make_tools(runtime_state_dir=tempdir)

            disclosures = tools.list_disclosure_subscriptions()
            news = tools.list_news_subscriptions()
            self.assertEqual(disclosures[0]["registered_at"], "20260427174500")
            self.assertEqual(news[0]["registered_at"], "20260427174500")
            self.assertTrue(disclosures[0]["subscription_id"].startswith("disc_sub_20260427174500_"))
            self.assertTrue(news[0]["subscription_id"].startswith("news_sub_20260427174500_"))
            persistent_payload = json.loads((Path(tempdir) / "subscription_state.json").read_text(encoding="utf-8"))
            self.assertEqual(
                persistent_payload["subscriptions"]["disclosures"][0]["registered_at"],
                "20260427174500",
            )

    def test_register_price_alert_and_list(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            created = tools.register_price_alert(
                code="005930",
                condition="climb",
                threshold=72000,
                message="삼성전자 돌파",
                httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
            )
            alerts = tools.list_price_alerts()

            self.assertEqual(created["code"], "005930")
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0]["alert_id"], created["alert_id"])
            self.assertEqual(alerts[0]["condition"], "climb")
            self.assertEqual(alerts[0]["current_price"], 71600.0)
            self.assertEqual(alerts[0]["debounce_seconds"], 10.0)
            self.assertFalse(alerts[0]["once_only"])

    def test_register_recovery_fail_alert_and_list(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                created = tools.register_recovery_fail_alert(
                    code="005930",
                    breach_price=1720000,
                    recovery_price=1730000,
                    failure_minutes=3,
                    recovery_minutes=3,
                    valid_after="11:00",
                    httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                )
                alerts = tools.list_price_alerts()
            finally:
                tools.close()

        self.assertEqual(created["code"], "005930")
        self.assertEqual(created["condition"], "recovery_fail")
        self.assertTrue(created["once_only"])
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["alert_id"], created["alert_id"])
        self.assertEqual(alerts[0]["condition"], "recovery_fail")
        self.assertEqual(alerts[0]["breach_price"], 1720000.0)
        self.assertEqual(alerts[0]["recovery_price"], 1730000.0)
        self.assertEqual(alerts[0]["failure_minutes"], 3.0)
        self.assertEqual(alerts[0]["recovery_minutes"], 3.0)
        self.assertEqual(alerts[0]["valid_after"], "11:00")
        self.assertEqual(alerts[0]["recovery_state"], "waiting")

    def test_recovery_fail_alert_resolves_before_removing_once_only(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            now = [datetime(2026, 5, 20, 11, 0, 0, tzinfo=timezone(timedelta(hours=9)))]
            dispatched_bodies: list[dict[str, object] | None] = []
            try:
                with patch("homestock.runtime_state._kst_now", side_effect=lambda: now[0]):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body) or {"queued": True}
                        created = tools.register_recovery_fail_alert(
                            code="005930",
                            breach_price=1720000,
                            recovery_price=1730000,
                            failure_minutes=3,
                            recovery_minutes=3,
                            valid_after="11:00",
                            httpCallback={
                                "method": "POST",
                                "url": "http://localhost:9000/hook",
                                "body": {
                                    "event": "{{event_type}}",
                                    "label": "{{event_type_label}}",
                                    "alert": "{{alert_id}}",
                                    "summary": "{{summary}}",
                                    "current": "{{current_price}}",
                                    "breach": "{{breach_price}}",
                                    "recovery": "{{recovery_price}}",
                                    "breached": "{{breached_at}}",
                                    "triggered": "{{triggered_at}}",
                                },
                            },
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1719000, "time": "110000"})
                        now[0] = datetime(2026, 5, 20, 11, 4, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1718000, "time": "110400"})
                        after_failure_alerts = tools.list_price_alerts()
                        after_failure_owned_codes = dict(tools._runtime_state._owned_price_codes)
                        now[0] = datetime(2026, 5, 20, 11, 5, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1731000, "time": "110500"})
                        now[0] = datetime(2026, 5, 20, 11, 9, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1732000, "time": "110900"})
                alerts = tools.list_price_alerts()
                owned_codes = dict(tools._runtime_state._owned_price_codes)
            finally:
                tools.close()

        self.assertEqual(len(dispatched_bodies), 2)
        fail_body = dispatched_bodies[0]
        resolved_body = dispatched_bodies[1]
        assert isinstance(fail_body, dict)
        assert isinstance(resolved_body, dict)
        self.assertEqual(fail_body["event"], "recovery_fail")
        self.assertEqual(fail_body["label"], "회복 실패")
        self.assertEqual(fail_body["alert"], created["alert_id"])
        self.assertEqual(fail_body["current"], "1718000")
        self.assertEqual(fail_body["breach"], "1720000")
        self.assertEqual(fail_body["recovery"], "1730000")
        self.assertEqual(fail_body["breached"], "20260520110000")
        self.assertEqual(fail_body["triggered"], "20260520110400")
        self.assertIn("회복 실패", str(fail_body["summary"]))
        self.assertEqual(after_failure_alerts[0]["recovery_state"], "failed")
        self.assertEqual(after_failure_owned_codes, {"005930": 1})
        self.assertEqual(resolved_body["event"], "recovery_fail_resolved")
        self.assertEqual(resolved_body["label"], "회복 실패 해소")
        self.assertEqual(resolved_body["alert"], created["alert_id"])
        self.assertEqual(resolved_body["current"], "1732000")
        self.assertEqual(resolved_body["triggered"], "20260520110900")
        self.assertIn("회복 실패 해소", str(resolved_body["summary"]))
        self.assertEqual(alerts, [])
        self.assertEqual(owned_codes, {})

    def test_recovery_fail_alert_success_returns_to_waiting_without_callback(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            now = [datetime(2026, 5, 20, 11, 0, 0, tzinfo=timezone(timedelta(hours=9)))]
            try:
                with patch("homestock.runtime_state._kst_now", side_effect=lambda: now[0]):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, _callback: {"queued": True}
                        tools.register_recovery_fail_alert(
                            code="005930",
                            breach_price=1720000,
                            recovery_price=1730000,
                            failure_minutes=3,
                            recovery_minutes=3,
                            valid_after="11:00",
                            httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1719000, "time": "110000"})
                        now[0] = datetime(2026, 5, 20, 11, 2, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1731000, "time": "110200"})
                        now[0] = datetime(2026, 5, 20, 11, 6, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1732000, "time": "110600"})
                        dispatch_count = dispatch.call_count
                        alerts = tools.list_price_alerts()
            finally:
                tools.close()

        self.assertEqual(dispatch_count, 0)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["recovery_state"], "waiting")
        self.assertIsNone(alerts[0]["breached_at"])
        self.assertIsNone(alerts[0]["recovery_since"])

    def test_recovery_fail_alert_once_false_can_fire_after_full_recovery_and_later_breach(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            now = [datetime(2026, 5, 20, 11, 0, 0, tzinfo=timezone(timedelta(hours=9)))]
            try:
                with patch("homestock.runtime_state._kst_now", side_effect=lambda: now[0]):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, _callback: {"queued": True}
                        tools.register_recovery_fail_alert(
                            code="005930",
                            breach_price=1720000,
                            recovery_price=1730000,
                            failure_minutes=3,
                            recovery_minutes=3,
                            valid_after="11:00",
                            httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                            once_only=False,
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1719000, "time": "110000"})
                        now[0] = datetime(2026, 5, 20, 11, 4, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1718000, "time": "110400"})
                        now[0] = datetime(2026, 5, 20, 11, 5, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1731000, "time": "110500"})
                        now[0] = datetime(2026, 5, 20, 11, 9, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1732000, "time": "110900"})
                        now[0] = datetime(2026, 5, 20, 11, 10, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1719000, "time": "111000"})
                        now[0] = datetime(2026, 5, 20, 11, 14, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 1718000, "time": "111400"})
                        dispatch_count = dispatch.call_count
                        alerts = tools.list_price_alerts()
            finally:
                tools.close()

        self.assertEqual(dispatch_count, 3)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["recovery_state"], "failed")

    def test_register_uptrend_end_alert_and_list(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                created = tools.register_uptrend_end_alert(
                    code="005930",
                    start_price=73000,
                    end_price=72000,
                    end_minutes=3,
                    valid_after="09:00",
                    httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                )
                alerts = tools.list_price_alerts()
            finally:
                tools.close()

        self.assertEqual(created["code"], "005930")
        self.assertEqual(created["condition"], "uptrend_end")
        self.assertTrue(created["once_only"])
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["alert_id"], created["alert_id"])
        self.assertEqual(alerts[0]["condition"], "uptrend_end")
        self.assertEqual(alerts[0]["start_price"], 73000.0)
        self.assertEqual(alerts[0]["end_price"], 72000.0)
        self.assertEqual(alerts[0]["end_minutes"], 3.0)
        self.assertEqual(alerts[0]["valid_after"], "09:00")
        self.assertEqual(alerts[0]["uptrend_state"], "waiting")

    def test_uptrend_end_alert_fires_after_end_line_hold_and_removes_once_only(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            now = [datetime(2026, 5, 20, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))]
            dispatched_bodies: list[dict[str, object] | None] = []
            try:
                with patch("homestock.runtime_state._kst_now", side_effect=lambda: now[0]):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body) or {"queued": True}
                        created = tools.register_uptrend_end_alert(
                            code="005930",
                            start_price=73000,
                            end_price=72000,
                            end_minutes=3,
                            valid_after="09:00",
                            httpCallback={
                                "method": "POST",
                                "url": "http://localhost:9000/hook",
                                "body": {
                                    "event": "{{event_type}}",
                                    "label": "{{event_type_label}}",
                                    "alert": "{{alert_id}}",
                                    "summary": "{{summary}}",
                                    "current": "{{current_price}}",
                                    "start": "{{start_price}}",
                                    "end": "{{end_price}}",
                                    "started": "{{uptrend_started_at}}",
                                    "ending": "{{ending_since}}",
                                    "triggered": "{{triggered_at}}",
                                },
                            },
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 73100, "time": "090000"})
                        now[0] = datetime(2026, 5, 20, 9, 1, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71900, "time": "090100"})
                        ending_alerts = tools.list_price_alerts()
                        now[0] = datetime(2026, 5, 20, 9, 4, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71800, "time": "090400"})
                        alerts = tools.list_price_alerts()
                        owned_codes = dict(tools._runtime_state._owned_price_codes)
            finally:
                tools.close()

        self.assertEqual(len(dispatched_bodies), 1)
        body = dispatched_bodies[0]
        assert isinstance(body, dict)
        self.assertEqual(body["event"], "uptrend_end")
        self.assertEqual(body["label"], "상승세 종료")
        self.assertEqual(body["alert"], created["alert_id"])
        self.assertEqual(body["current"], "71800")
        self.assertEqual(body["start"], "73000")
        self.assertEqual(body["end"], "72000")
        self.assertEqual(body["started"], "20260520090000")
        self.assertEqual(body["ending"], "20260520090100")
        self.assertEqual(body["triggered"], "20260520090400")
        self.assertIn("상승세 종료", str(body["summary"]))
        self.assertEqual(ending_alerts[0]["uptrend_state"], "ending")
        self.assertEqual(alerts, [])
        self.assertEqual(owned_codes, {})

    def test_uptrend_end_alert_cancels_ending_hold_when_price_reclaims_line(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            now = [datetime(2026, 5, 20, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))]
            try:
                with patch("homestock.runtime_state._kst_now", side_effect=lambda: now[0]):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, _callback: {"queued": True}
                        tools.register_uptrend_end_alert(
                            code="005930",
                            start_price=73000,
                            end_price=72000,
                            end_minutes=3,
                            valid_after="09:00",
                            httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 73100, "time": "090000"})
                        now[0] = datetime(2026, 5, 20, 9, 1, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71900, "time": "090100"})
                        now[0] = datetime(2026, 5, 20, 9, 2, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72100, "time": "090200"})
                        now[0] = datetime(2026, 5, 20, 9, 5, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72100, "time": "090500"})
                        dispatch_count = dispatch.call_count
                        alerts = tools.list_price_alerts()
            finally:
                tools.close()

        self.assertEqual(dispatch_count, 0)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["uptrend_state"], "rising")
        self.assertIsNone(alerts[0]["ending_since"])

    def test_uptrend_end_alert_once_false_rearms_after_new_start(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            now = [datetime(2026, 5, 20, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))]
            try:
                with patch("homestock.runtime_state._kst_now", side_effect=lambda: now[0]):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, _callback: {"queued": True}
                        tools.register_uptrend_end_alert(
                            code="005930",
                            start_price=73000,
                            end_price=72000,
                            end_minutes=3,
                            valid_after="09:00",
                            httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                            once_only=False,
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 73100, "time": "090000"})
                        now[0] = datetime(2026, 5, 20, 9, 1, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71900, "time": "090100"})
                        now[0] = datetime(2026, 5, 20, 9, 4, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71800, "time": "090400"})
                        now[0] = datetime(2026, 5, 20, 9, 5, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 73100, "time": "090500"})
                        now[0] = datetime(2026, 5, 20, 9, 6, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71900, "time": "090600"})
                        now[0] = datetime(2026, 5, 20, 9, 9, 0, tzinfo=timezone(timedelta(hours=9)))
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71800, "time": "090900"})
                        dispatch_count = dispatch.call_count
                        alerts = tools.list_price_alerts()
            finally:
                tools.close()

        self.assertEqual(dispatch_count, 2)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["uptrend_state"], "ended")

    def test_register_price_alert_does_not_leave_state_when_rt_subscribe_fails(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with patch.object(tools._client, "subscribe_realtime_price", side_effect=RuntimeError("SC failed")):
                with self.assertRaisesRegex(RuntimeError, "SC failed"):
                    tools.register_price_alert(
                        code="005930",
                        condition="climb",
                        threshold=72000,
                        message="삼성전자 돌파",
                        httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                    )

            self.assertEqual(tools.list_price_alerts(), [])
            self.assertEqual(tools._runtime_state._owned_price_codes, {})

    def test_cancel_price_alert_by_code(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            tools.register_price_alert(
                code="005930",
                condition="climb",
                threshold=72000,
                message="삼성전자 돌파",
                httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
            )

            result = tools.cancel_price_alert(code="005930")

            self.assertTrue(result["canceled"])
            self.assertEqual(result["removed_alerts"], 1)
            self.assertEqual(tools.list_price_alerts(), [])

    def test_register_stock_price_callback_list_and_cancel_by_id(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            created = tools.register_stock_price_callback(
                code="005930",
                step=500,
                price_filter="70000+",
                httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
            )
            items = tools.list_stock_price_callbacks()
            result = tools.cancel_stock_price_callback(created["stock_price_callback_id"])

            self.assertEqual(created["code"], "005930")
            self.assertEqual(created["step"], 500.0)
            self.assertEqual(created["price_filter"], "70000+")
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["stock_price_callback_id"], created["stock_price_callback_id"])
            self.assertEqual(items[0]["price_filter"], "70000+")
            self.assertEqual(items[0]["current_price"], 71600.0)
            self.assertIsNone(items[0]["baseline_price"])
            self.assertEqual(items[0]["fired_count"], 0)
            self.assertTrue(result["canceled"])
            self.assertEqual(result["removed_callbacks"], 1)
            self.assertEqual(tools.list_stock_price_callbacks(), [])

    def test_cancel_stock_price_callbacks_by_code_preserves_other_refs(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            callback = {"method": "POST", "url": "http://localhost:9000/hook"}

            tools.register_stock_price_callback("005930", 500, callback)
            tools.register_stock_price_callback("005930", 700, callback)
            tools.register_stock_price_callback("000660", 500, callback)
            result = tools.cancel_stock_price_callback(code="005930")

            remaining = tools.list_stock_price_callbacks()
            self.assertTrue(result["canceled"])
            self.assertEqual(result["removed_callbacks"], 2)
            self.assertEqual([item["code"] for item in remaining], ["000660"])
            self.assertEqual(tools._runtime_state._owned_price_codes, {"000660": 1})

    def test_stock_price_callback_validates_step_and_callback(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with self.assertRaisesRegex(ValueError, "httpCallback"):
                tools.register_stock_price_callback("005930", 500)
            with self.assertRaisesRegex(ValueError, "step"):
                tools.register_stock_price_callback(
                    "005930",
                    0,
                    {"method": "POST", "url": "http://localhost:9000/hook"},
                )
            with self.assertRaisesRegex(ValueError, "price_filter"):
                tools.register_stock_price_callback(
                    "005930",
                    500,
                    {"method": "POST", "url": "http://localhost:9000/hook"},
                    price_filter="70000",
                )

            self.assertEqual(tools.list_stock_price_callbacks(), [])
            self.assertEqual(tools._runtime_state._owned_price_codes, {})

    def test_register_stock_price_callback_does_not_leave_state_when_rt_subscribe_fails(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with patch.object(tools._client, "subscribe_realtime_price", side_effect=RuntimeError("SC failed")):
                with self.assertRaisesRegex(RuntimeError, "SC failed"):
                    tools.register_stock_price_callback(
                        code="005930",
                        step=500,
                        httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                    )

            self.assertEqual(tools.list_stock_price_callbacks(), [])
            self.assertEqual(tools._runtime_state._owned_price_codes, {})

    def test_stock_price_callback_fires_up_and_down_with_replacements(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_bodies: list[dict[str, object] | None] = []

            with patch.object(RuntimeStateManager, "STOCK_PRICE_CALLBACK_DEBOUNCE_SECONDS", 0):
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body)
                    tools.register_stock_price_callback(
                        code="005930",
                        step=500,
                        httpCallback={
                            "method": "POST",
                            "url": "http://localhost:9000/hook",
                            "body": {
                                "name": "{{name}}",
                                "price": "{{price}}",
                                "price_raw": "{{price_raw}}",
                                "direction": "{{direction}}",
                            },
                        },
                    )
                    client = tools._client
                    assert isinstance(client, MockIndiClient)
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090000"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70499, "time": "090100"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70500, "time": "090200"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090300"})

            self.assertEqual(
                dispatched_bodies,
                [
                    {"name": "Samsung Electronics", "price": "70,500", "price_raw": "70500", "direction": "상향"},
                    {"name": "Samsung Electronics", "price": "70,000", "price_raw": "70000", "direction": "하향"},
                ],
            )
            items = tools.list_stock_price_callbacks()
            self.assertEqual(items[0]["baseline_price"], 70000.0)
            self.assertEqual(items[0]["last_direction"], "하향")
            self.assertEqual(items[0]["fired_count"], 2)

    def test_stock_price_callback_debounce_trailing_fires_latest_price(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_bodies: list[dict[str, object] | None] = []

            try:
                with patch.object(RuntimeStateManager, "STOCK_PRICE_CALLBACK_DEBOUNCE_SECONDS", 0.05):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body)
                        tools.register_stock_price_callback(
                            code="005930",
                            step=500,
                            httpCallback={
                                "method": "POST",
                                "url": "http://localhost:9000/hook",
                                "body": {"price": "{{price}}", "direction": "{{direction}}"},
                            },
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090000"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70500, "time": "090001"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70800, "time": "090002"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71000, "time": "090003"})
                        self.assertEqual(dispatched_bodies, [{"price": "70,500", "direction": "상향"}])
                        time.sleep(0.12)
                self.assertEqual(
                    dispatched_bodies,
                    [
                        {"price": "70,500", "direction": "상향"},
                        {"price": "71,000", "direction": "상향"},
                    ],
                )
                items = tools.list_stock_price_callbacks()
                self.assertEqual(items[0]["baseline_price"], 71000.0)
                self.assertEqual(items[0]["current_price"], 71000.0)
                self.assertEqual(items[0]["fired_count"], 2)
            finally:
                tools.close()

    def test_stock_price_callback_debounce_uses_final_pending_price(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_bodies: list[dict[str, object] | None] = []

            try:
                with patch.object(RuntimeStateManager, "STOCK_PRICE_CALLBACK_DEBOUNCE_SECONDS", 0.05):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body)
                        tools.register_stock_price_callback(
                            code="005930",
                            step=500,
                            httpCallback={
                                "method": "POST",
                                "url": "http://localhost:9000/hook",
                                "body": {"price": "{{price}}", "direction": "{{direction}}"},
                            },
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090000"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70500, "time": "090001"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71000, "time": "090002"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70600, "time": "090003"})
                        time.sleep(0.12)
                self.assertEqual(dispatched_bodies, [{"price": "70,500", "direction": "상향"}])
                items = tools.list_stock_price_callbacks()
                self.assertEqual(items[0]["baseline_price"], 70500.0)
                self.assertEqual(items[0]["current_price"], 70600.0)
                self.assertEqual(items[0]["fired_count"], 1)
            finally:
                tools.close()

    def test_runtime_state_uses_batch_rt_listener_and_eval_uc_extremes(self):
        class BatchClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self._rt_batch_listeners: list = []

            def register_rt_batch_listener(self, listener) -> None:
                if listener not in self._rt_batch_listeners:
                    self._rt_batch_listeners.append(listener)

            def unregister_rt_batch_listener(self, listener) -> None:
                self._rt_batch_listeners = [item for item in self._rt_batch_listeners if item is not listener]

            def emit_rt_batch(self, events: list[dict[str, object]]) -> None:
                for listener in list(self._rt_batch_listeners):
                    listener([dict(event) for event in events])

        with TemporaryDirectory() as tempdir:
            client = BatchClient()
            fall_safe_executor = Mock(return_value={"accepted": False, "message": "test executor"})
            runtime_state = RuntimeStateManager(client, tempdir, fall_safe_executor=fall_safe_executor)
            self.assertEqual(client._rt_listeners, [])
            self.assertEqual(len(client._rt_batch_listeners), 1)

            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                dispatch.side_effect = lambda _self, _callback: {"queued": True}
                runtime_state.register_price_alert(
                    code="005930",
                    condition="climb",
                    threshold=70500,
                    window_minutes=None,
                    message="삼성전자 돌파",
                    http_callback=HttpCallbackSpec(
                        method="POST",
                        url="http://localhost:9000/alert",
                    ),
                )
                runtime_state.register_stock_price_callback(
                    code="005930",
                    step=500,
                    http_callback=HttpCallbackSpec(
                        method="POST",
                        url="http://localhost:9000/hook",
                        body={
                            "name": "{{name}}",
                            "price": "{{price}}",
                            "direction": "{{direction}}",
                        },
                    ),
                )
                runtime_state.register_fall_safe(
                    account_no="12345678901",
                    code="005930",
                    trigger_price=69950,
                    quantity=5,
                )
                client.emit_rt_batch(
                    [
                        {"rt_type": "UC", "code": "005930", "current_price": 70000, "time": "090000"},
                    ]
                )
                client.emit_rt_batch(
                    [
                        {"rt_type": "UC", "code": "005930", "current_price": 70499, "time": "090100"},
                        {"rt_type": "UC", "code": "005930", "current_price": 70500, "time": "090200"},
                        {"rt_type": "UC", "code": "005930", "current_price": 69900, "time": "090250"},
                        {"rt_type": "UC", "code": "005930", "current_price": 70000, "time": "090300"},
                    ]
                )

            self.assertEqual(dispatch.call_count, 2)
            fall_safe_executor.assert_called_once_with("12345678901", "005930", 5)
            alerts = runtime_state.list_price_alerts()
            self.assertEqual(alerts[0]["current_price"], 70000.0)
            items = runtime_state.list_stock_price_callbacks()
            self.assertEqual(items[0]["baseline_price"], 70000.0)
            self.assertEqual(items[0]["current_price"], 70000.0)
            self.assertEqual(items[0]["last_direction"], "상향")
            self.assertEqual(items[0]["fired_count"], 1)
            self.assertEqual(runtime_state.list_fall_safes(), [])

            runtime_state.close()
            self.assertEqual(client._rt_batch_listeners, [])

    def test_stock_price_callback_price_filter_limits_dispatch(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_bodies: list[dict[str, object] | None] = []

            with patch.object(RuntimeStateManager, "STOCK_PRICE_CALLBACK_DEBOUNCE_SECONDS", 0):
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body)
                    tools.register_stock_price_callback(
                        code="005930",
                        step=500,
                        price_filter="70000+",
                        httpCallback={
                            "method": "POST",
                            "url": "http://localhost:9000/hook",
                            "body": {"price": "{{price}}", "direction": "{{direction}}"},
                        },
                    )
                    client = tools._client
                    assert isinstance(client, MockIndiClient)
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 69000, "time": "090000"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 69500, "time": "090100"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090200"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 69400, "time": "090300"})

            self.assertEqual(dispatched_bodies, [{"price": "70,000", "direction": "상향"}])
            items = tools.list_stock_price_callbacks()
            self.assertEqual(items[0]["baseline_price"], 70000.0)
            self.assertEqual(items[0]["current_price"], 69400.0)
            self.assertEqual(items[0]["fired_count"], 1)

        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_bodies = []

            with patch.object(RuntimeStateManager, "STOCK_PRICE_CALLBACK_DEBOUNCE_SECONDS", 0):
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, callback: dispatched_bodies.append(callback.body)
                    tools.register_stock_price_callback(
                        code="005930",
                        step=500,
                        price_filter="70000-",
                        httpCallback={
                            "method": "POST",
                            "url": "http://localhost:9000/hook",
                            "body": {"price": "{{price}}", "direction": "{{direction}}"},
                        },
                    )
                    client = tools._client
                    assert isinstance(client, MockIndiClient)
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71000, "time": "090000"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70500, "time": "090100"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090200"})

            self.assertEqual(dispatched_bodies, [{"price": "70,000", "direction": "하향"}])
            items = tools.list_stock_price_callbacks()
            self.assertEqual(items[0]["baseline_price"], 70000.0)
            self.assertEqual(items[0]["fired_count"], 1)

    def test_stock_price_callback_large_jump_fires_once_and_shares_ref_count(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with patch.object(RuntimeStateManager, "STOCK_PRICE_CALLBACK_DEBOUNCE_SECONDS", 0):
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, _callback: {"queued": True}
                    tools.register_price_alert(
                        code="005930",
                        condition="climb",
                        threshold=80000,
                        message="삼성전자 돌파",
                        httpCallback={"method": "POST", "url": "http://localhost:9000/alert"},
                    )
                    tools.register_fall_safe(
                        account_no="12345678901",
                        code="005930",
                        trigger_price=65000,
                        quantity=1,
                    )
                    created = tools.register_stock_price_callback(
                        code="005930",
                        step=500,
                        httpCallback={"method": "POST", "url": "http://localhost:9000/step"},
                    )
                    client = tools._client
                    assert isinstance(client, MockIndiClient)
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090000"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71500, "time": "090100"})

            self.assertEqual(dispatch.call_count, 1)
            items = tools.list_stock_price_callbacks()
            self.assertEqual(items[0]["baseline_price"], 71500.0)
            self.assertEqual(items[0]["fired_count"], 1)
            self.assertEqual(tools._runtime_state._owned_price_codes, {"005930": 3})
            tools.cancel_stock_price_callback(created["stock_price_callback_id"])
            self.assertEqual(tools._runtime_state._owned_price_codes, {"005930": 2})

    def test_http_callback_body_format_defaults_to_json(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            tools.subscribe_disclosure(
                "005930",
                {
                    "method": "POST",
                    "url": "http://localhost:9000/hook",
                    "body": {"message": "hello"},
                },
            )

            payload = (Path(tempdir) / "subscription_state.json").read_text(encoding="utf-8")
            self.assertIn('"bodyFormat": "json"', payload)

    def test_http_callback_rejects_body_format_without_body(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with self.assertRaisesRegex(ValueError, "bodyFormat"):
                tools.subscribe_disclosure(
                    "005930",
                    {
                        "method": "POST",
                        "url": "http://localhost:9000/hook",
                        "bodyFormat": "form",
                    },
                )

    def test_http_callback_rejects_non_post_method(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with self.assertRaisesRegex(ValueError, "POST"):
                tools.subscribe_news(
                    ["F"],
                    {"method": "GET", "url": "http://localhost:9000/hook"},
                )

    def test_climb_alert_fires_on_upward_recross(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_urls: list[str] = []

            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                dispatch.side_effect = lambda _self, callback: dispatched_urls.append(callback.url)
                tools.register_price_alert(
                    code="005930",
                    condition="climb",
                    threshold=72000,
                    message="삼성전자 돌파",
                    httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                )
                client = tools._client
                assert isinstance(client, MockIndiClient)
                client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71000, "time": "090000"})
                client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72100, "time": "090100"})

            self.assertEqual(dispatched_urls, ["http://localhost:9000/hook"])

    def test_climb_alert_debounce_updates_side_and_waits_for_next_event(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            try:
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, _callback: {"queued": True}
                    tools.register_price_alert(
                        code="005930",
                        condition="climb",
                        threshold=72000,
                        debounce_seconds=10,
                        message="삼성전자 돌파",
                        httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                    )
                    client = tools._client
                    assert isinstance(client, MockIndiClient)
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71000, "time": "090000"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72100, "time": "090001"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71900, "time": "090002"})
                    self.assertEqual(dispatch.call_count, 1)
                    self.assertEqual(tools._runtime_state._state["price_alerts"][0]["last_side"], "below")
                    tools._runtime_state._state["price_alerts"][0]["last_triggered_at"] = "20000101000000"
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72200, "time": "090003"})

                self.assertEqual(dispatch.call_count, 2)
            finally:
                tools.close()

    def test_climb_alert_debounce_consumes_crossing_inside_mute(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            try:
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, _callback: {"queued": True}
                    tools.register_price_alert(
                        code="005930",
                        condition="climb",
                        threshold=72000,
                        debounce_seconds=10,
                        message="삼성전자 돌파",
                        httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                    )
                    client = tools._client
                    assert isinstance(client, MockIndiClient)
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71000, "time": "090000"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72100, "time": "090001"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71900, "time": "090002"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72200, "time": "090003"})
                    self.assertEqual(dispatch.call_count, 1)
                    self.assertEqual(tools._runtime_state._state["price_alerts"][0]["last_side"], "above")
                    tools._runtime_state._state["price_alerts"][0]["last_triggered_at"] = "20000101000000"
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72300, "time": "090004"})

                self.assertEqual(dispatch.call_count, 1)
            finally:
                tools.close()

    def test_price_alert_once_only_removes_after_first_fire(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            try:
                with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    dispatch.side_effect = lambda _self, _callback: {"queued": True}
                    tools.register_price_alert(
                        code="005930",
                        condition="climb",
                        threshold=72000,
                        once_only=True,
                        message="삼성전자 돌파",
                        httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                    )
                    client = tools._client
                    assert isinstance(client, MockIndiClient)
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71000, "time": "090000"})
                    client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72100, "time": "090001"})

                self.assertEqual(dispatch.call_count, 1)
                self.assertEqual(tools.list_price_alerts(), [])
                self.assertEqual(tools._runtime_state._owned_price_codes, {})
            finally:
                tools.close()

    def test_price_alert_validates_debounce_scope(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with self.assertRaisesRegex(ValueError, "debounce_seconds"):
                tools.register_price_alert(
                    code="005930",
                    condition="fastmove",
                    threshold=2.0,
                    window_minutes=5,
                    debounce_seconds=10,
                    message="삼성전자 급등",
                    httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                )
            with self.assertRaisesRegex(ValueError, "debounce_seconds"):
                tools.register_price_alert(
                    code="005930",
                    condition="climb",
                    threshold=72000,
                    debounce_seconds=-1,
                    message="삼성전자 돌파",
                    httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                )

    def test_fastmove_alert_respects_window_cooldown(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatch_count = 0

            try:
                with patch.object(RuntimeStateManager, "PRICE_ALERT_COOLDOWN_SECONDS_PER_MINUTE", 0):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, _callback: None
                        tools.register_price_alert(
                            code="005930",
                            condition="fastmove",
                            threshold=2.0,
                            window_minutes=5,
                            message="삼성전자 급등",
                            httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090000"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71500, "time": "090100"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 73000, "time": "090200"})
                        dispatch_count = dispatch.call_count
            finally:
                tools.close()

            self.assertEqual(dispatch_count, 1)

    def test_fastmove_alert_trailing_cooldown_fires_latest_price(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            try:
                with patch.object(RuntimeStateManager, "PRICE_ALERT_COOLDOWN_SECONDS_PER_MINUTE", 0.05):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, _callback: {"queued": True}
                        tools.register_price_alert(
                            code="005930",
                            condition="fastmove",
                            threshold=2.0,
                            window_minutes=1,
                            message="삼성전자 급등",
                            httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090000"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71400, "time": "090001"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72000, "time": "090002"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 73000, "time": "090003"})
                        self.assertEqual(dispatch.call_count, 1)
                        time.sleep(0.12)
                self.assertEqual(dispatch.call_count, 2)
                alerts = tools._runtime_state._state["price_alerts"]
                self.assertEqual(alerts[0]["last_price"], 73000.0)
                self.assertEqual(alerts[0]["baseline_price"], 73000.0)
            finally:
                tools.close()

    def test_fastmove_alert_trailing_cooldown_uses_final_pending_price(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            try:
                with patch.object(RuntimeStateManager, "PRICE_ALERT_COOLDOWN_SECONDS_PER_MINUTE", 0.05):
                    with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, _callback: {"queued": True}
                        tools.register_price_alert(
                            code="005930",
                            condition="fastmove",
                            threshold=2.0,
                            window_minutes=1,
                            message="삼성전자 급등",
                            httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                        )
                        client = tools._client
                        assert isinstance(client, MockIndiClient)
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090000"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 71400, "time": "090001"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 73000, "time": "090002"})
                        client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 72000, "time": "090003"})
                        time.sleep(0.12)
                self.assertEqual(dispatch.call_count, 1)
                alerts = tools._runtime_state._state["price_alerts"]
                self.assertEqual(alerts[0]["last_price"], 72000.0)
                self.assertEqual(alerts[0]["baseline_price"], 71400.0)
            finally:
                tools.close()

    def test_http_callback_dispatch_adds_json_content_type(self):
        callback = {
            "method": "POST",
            "url": "http://localhost:9000/hook",
            "body": {"message": "hello"},
        }

        tools = make_tools()
        parsed = tools._parse_http_callback(callback)

        from homestock.runtime_state import CallbackDispatcher

        dispatcher = CallbackDispatcher()
        request = dispatcher._build_request(parsed)
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(headers["content-type"], "application/json; charset=utf-8")

    def test_http_callback_dispatch_adds_form_content_type(self):
        callback = {
            "method": "POST",
            "url": "http://localhost:9000/hook",
            "bodyFormat": "form",
            "body": {"message": "hello"},
        }

        tools = make_tools()
        parsed = tools._parse_http_callback(callback)

        from homestock.runtime_state import CallbackDispatcher

        dispatcher = CallbackDispatcher()
        request = dispatcher._build_request(parsed)
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(headers["content-type"], "application/x-www-form-urlencoded; charset=utf-8")

    def test_http_callback_respects_user_content_type_header(self):
        callback = {
            "method": "POST",
            "url": "http://localhost:9000/hook",
            "headers": {"Content-Type": "text/plain"},
            "body": {"message": "hello"},
        }

        tools = make_tools()
        parsed = tools._parse_http_callback(callback)

        from homestock.runtime_state import CallbackDispatcher

        dispatcher = CallbackDispatcher()
        request = dispatcher._build_request(parsed)
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(headers["content-type"], "text/plain")

    def test_http_callback_dispatch_returns_after_queueing(self):
        from homestock.runtime_state import CallbackDispatcher

        callback = make_tools()._parse_http_callback({"method": "POST", "url": "http://localhost:9000/hook"})
        dispatcher = CallbackDispatcher(max_attempts=1, retry_delay_seconds=0.0)
        worker_started = threading.Event()
        release_worker = threading.Event()

        def slow_send(_callback):
            worker_started.set()
            release_worker.wait(1.0)
            return {"delivered": True, "error": None}

        with patch.object(dispatcher, "_send_once", side_effect=slow_send):
            started_at = time.monotonic()
            result = dispatcher.dispatch(callback)
            elapsed = time.monotonic() - started_at

            self.assertEqual(result, {"queued": True, "delivered": None, "error": None})
            self.assertLess(elapsed, 0.2)
            self.assertTrue(worker_started.wait(1.0))
            release_worker.set()
            self.assertTrue(dispatcher.wait_for_idle(1.0))

        dispatcher.close()

    def test_http_callback_dispatch_retries_inside_worker(self):
        from homestock.runtime_state import CallbackDispatcher

        callback = make_tools()._parse_http_callback({"method": "POST", "url": "http://localhost:9000/hook"})
        dispatcher = CallbackDispatcher(max_attempts=3, retry_delay_seconds=0.001)
        attempts: list[str] = []

        def flaky_send(_callback):
            attempts.append(_callback.url)
            if len(attempts) == 1:
                raise RuntimeError("temporary failure")
            return {"delivered": True, "error": None}

        with patch.object(dispatcher, "_send_once", side_effect=flaky_send):
            dispatcher.dispatch(callback)
            self.assertTrue(dispatcher.wait_for_idle(1.0))

        dispatcher.close()
        self.assertEqual(attempts, ["http://localhost:9000/hook", "http://localhost:9000/hook"])

    def test_async_http_callback_does_not_block_runtime_state_mutation(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            created = tools.subscribe_news(
                ["F"],
                {"method": "POST", "url": "http://localhost:9000/hook"},
                "005930",
            )
            client = tools._client
            assert isinstance(client, MockIndiClient)
            worker_started = threading.Event()
            release_worker = threading.Event()

            def slow_send(_self, _callback):
                worker_started.set()
                release_worker.wait(1.0)
                return {"delivered": True, "error": None}

            with patch("homestock.runtime_state.CallbackDispatcher._send_once", autospec=True, side_effect=slow_send):
                client.emit_rt_event(
                    {
                        "rt_type": "N0",
                        "news_type": "F",
                        "news_type_label": "시황",
                        "date": "20260428",
                        "time": "120000",
                        "article_id": "NEWS1",
                        "code": "005930",
                        "title": "테스트 뉴스",
                    }
                )
                self.assertTrue(worker_started.wait(1.0))
                result = tools.unsubscribe_news(created["subscription_id"])
                release_worker.set()
                self.assertTrue(tools._runtime_state._dispatcher.wait_for_idle(1.0))

            tools._runtime_state._dispatcher.close()
            self.assertEqual(result["removed_subscriptions"], 1)
            self.assertEqual(tools.list_news_subscriptions(), [])

    def test_register_fall_safe_and_list(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            created = tools.register_fall_safe(
                account_no="12345678901",
                code="005930",
                trigger_price=70000,
                quantity=5,
                httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
            )
            items = tools.list_fall_safes()

            self.assertEqual(created["code"], "005930")
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["fall_safe_id"], created["fall_safe_id"])
            self.assertEqual(items[0]["trigger_price"], 70000.0)

    def test_register_fall_safe_does_not_leave_state_when_rt_subscribe_fails(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with patch.object(tools._client, "subscribe_realtime_price", side_effect=RuntimeError("SC failed")):
                with self.assertRaisesRegex(RuntimeError, "SC failed"):
                    tools.register_fall_safe(
                        account_no="12345678901",
                        code="005930",
                        trigger_price=70000,
                        quantity=5,
                        httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                    )

            self.assertEqual(tools.list_fall_safes(), [])
            self.assertEqual(tools._runtime_state._owned_price_codes, {})

    def test_cancel_fall_safe(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            created = tools.register_fall_safe(
                account_no="12345678901",
                code="005930",
                trigger_price=70000,
                quantity=5,
            )

            result = tools.cancel_fall_safe(created["fall_safe_id"])

            self.assertTrue(result["canceled"])
            self.assertEqual(result["removed_fall_safes"], 1)
            self.assertEqual(tools.list_fall_safes(), [])

    def test_fall_safe_triggers_once_and_is_removed(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)

            with patch.object(
                tools._runtime_state,
                "_fall_safe_executor",
                wraps=tools._runtime_state._fall_safe_executor,
            ) as execute_order, patch(
                "homestock.runtime_state.CallbackDispatcher.dispatch",
                autospec=True,
            ):
                tools.register_fall_safe(
                    account_no="12345678901",
                    code="005930",
                    trigger_price=70000,
                    quantity=5,
                    httpCallback={"method": "POST", "url": "http://localhost:9000/hook"},
                )
                client = tools._client
                assert isinstance(client, MockIndiClient)
                client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70500, "time": "090000"})
                client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 69900, "time": "090100"})
                client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 69500, "time": "090200"})

            self.assertEqual(execute_order.call_count, 1)
            self.assertEqual(tools.list_fall_safes(), [])

    def test_rt_fall_safe_evaluation_serializes_cancel_mutation(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            created = tools.register_fall_safe(
                account_no="12345678901",
                code="005930",
                trigger_price=70000,
                quantity=5,
            )
            client = tools._client
            assert isinstance(client, MockIndiClient)
            cancel_started = threading.Event()
            cancel_done = threading.Event()
            cancel_errors: list[BaseException] = []
            cancel_results: list[dict[str, object]] = []
            cancel_threads: list[threading.Thread] = []

            def cancel_worker() -> None:
                cancel_started.set()
                try:
                    cancel_results.append(tools.cancel_fall_safe(created["fall_safe_id"]))
                except BaseException as exc:
                    cancel_errors.append(exc)
                finally:
                    cancel_done.set()

            def execute_order(_account_no: str, _code: str, _quantity: int) -> dict[str, object]:
                state_path = next(Path(tempdir).glob("subscribtion_state_*.json"))
                payload = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(payload["fall_safes"], [])
                thread = threading.Thread(target=cancel_worker)
                cancel_threads.append(thread)
                thread.start()
                self.assertTrue(cancel_started.wait(1.0))
                self.assertFalse(cancel_done.wait(0.1))
                return {"accepted": False, "message": "test executor"}

            with patch.object(tools._runtime_state, "_fall_safe_executor", side_effect=execute_order):
                client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70500, "time": "090000"})
                client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 69900, "time": "090100"})

            self.assertTrue(cancel_done.wait(1.0))
            for thread in cancel_threads:
                thread.join(1.0)
            self.assertEqual(cancel_errors, [])
            self.assertEqual(cancel_results[0]["removed_fall_safes"], 0)
            self.assertEqual(tools.list_fall_safes(), [])

    def test_health_check_reports_mock_backend(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            tools.subscribe_news(["F"], {"method": "POST", "url": "http://localhost:9000/news"})
            tools.subscribe_disclosure("005930", {"method": "POST", "url": "http://localhost:9000/disclosure"})
            tools.subscribe_gold_realtime_price("M04020000")
            result = tools.health_check()

        self.assertTrue(result["ok"])
        self.assertEqual(result["backend"], "mock")
        self.assertFalse(result["live_orders_allowed"])
        self.assertTrue(result["rt_news_registered"])
        self.assertTrue(result["rt_disclosure_registered"])
        self.assertEqual(result["gold_rt_subscription_count"], 1)
        self.assertEqual(result["gold_rt_subscriptions"], {"M04020000": 1})
        self.assertTrue(result["gold_runtime"]["available"])
        self.assertEqual(result["gold_runtime"]["active_alert_count"], 0)

    def test_real_health_check_reports_rt_registration_flags(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._ocx_ready = True
        client._rt_news_registered = True
        client._rt_disclosure_registered = False
        client._gold_rt_control = object()
        client._gold_rt_subscription_counts = {("XC", "M04020000"): 2}
        client._current_session_id = Mock(return_value=1)
        client._check_giexpert_main_generation = Mock(
            return_value={
                "running": True,
                "restarted": False,
                "message": "GiExpertMain.exe generation unchanged",
            }
        )
        client._comm_state = Mock(return_value=0)
        client._tr_control = Mock()
        client._tr_control.dynamicCall.side_effect = lambda signature: {
            "GetErrorState()": 0,
            "GetErrorCode()": "",
            "GetErrorMessage()": "",
        }[signature]

        result = client.health_check(live_orders_allowed=False).to_dict()

        self.assertTrue(result["ok"])
        self.assertTrue(result["rt_news_registered"])
        self.assertFalse(result["rt_disclosure_registered"])
        self.assertTrue(result["gold_rt_control_ready"])
        self.assertEqual(result["gold_rt_subscription_count"], 2)
        self.assertEqual(result["gold_rt_subscriptions"], {"XC:M04020000": 2})

    def test_real_place_order_uses_sparse_saba101u1_indices(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._account_password = Mock(return_value="1234")
        client._submit_stock_order = Mock(
            return_value=SimpleNamespace(
                accepted=True,
                live_order=True,
                order_id="1234567890",
                message="ok",
                raw={},
            )
        )

        request = OrderRequest(
            account_no="12345678901",
            code="381170",
            side="sell",
            quantity=1,
            price=33000,
            order_type="limit",
        )

        client.place_order(request)

        client._submit_stock_order.assert_called_once()
        kwargs = client._submit_stock_order.call_args.kwargs
        self.assertEqual(kwargs["query_name"], "SABA101U1")
        self.assertEqual(kwargs["action"], "place_order")
        self.assertEqual(kwargs["inputs"][8], "A381170")
        self.assertEqual(kwargs["inputs"][20], "")
        self.assertEqual(kwargs["inputs"][21], "Y")
        self.assertEqual(kwargs["inputs"][36], "")
        self.assertEqual(kwargs["inputs"][37], "0")

    def test_real_modify_order_uses_saba102u1_order_method_at_35(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._account_password = Mock(return_value="1234")
        client._submit_stock_order = Mock(
            return_value=SimpleNamespace(
                accepted=True,
                live_order=True,
                order_id="1234567890",
                message="ok",
                raw={},
            )
        )

        request = OrderRequest(
            account_no="12345678901",
            code="381170",
            side="sell",
            quantity=1,
            price=33000,
            order_type="limit",
            original_order_id="KRX123456789",
            order_method_code="0",
            sor_original_order_id="SOR123456789",
        )

        client.modify_order(request)

        client._submit_stock_order.assert_called_once()
        kwargs = client._submit_stock_order.call_args.kwargs
        self.assertEqual(kwargs["query_name"], "SABA102U1")
        self.assertEqual(kwargs["action"], "modify_order")
        self.assertEqual(kwargs["inputs"][8], "A381170")
        self.assertEqual(kwargs["inputs"][16], "KRX123456789")
        self.assertEqual(kwargs["inputs"][35], "0")
        self.assertEqual(kwargs["inputs"][36], "")
        self.assertEqual(kwargs["inputs"][37], "SOR123456789")

    def test_real_cancel_order_uses_saba102u1_order_method_at_35(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._account_password = Mock(return_value="1234")
        client._submit_stock_order = Mock(
            return_value=SimpleNamespace(
                accepted=True,
                live_order=True,
                order_id="1234567890",
                message="ok",
                raw={},
            )
        )

        request = OrderRequest(
            account_no="12345678901",
            code="381170",
            side="sell",
            quantity=1,
            price=33000,
            order_type="limit",
            original_order_id="KRX123456789",
            order_method_code="0",
            sor_original_order_id="SOR123456789",
        )

        client.cancel_order(request)

        client._submit_stock_order.assert_called_once()
        kwargs = client._submit_stock_order.call_args.kwargs
        self.assertEqual(kwargs["query_name"], "SABA102U1")
        self.assertEqual(kwargs["action"], "cancel_order")
        self.assertEqual(kwargs["inputs"][8], "A381170")
        self.assertEqual(kwargs["inputs"][16], "KRX123456789")
        self.assertEqual(kwargs["inputs"][35], "0")
        self.assertEqual(kwargs["inputs"][36], "")
        self.assertEqual(kwargs["inputs"][37], "SOR123456789")

    def test_real_stock_order_result_preserves_sor_krx_nxt_orc_ids(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._request = Mock()
        client._single_text = Mock(
            side_effect=lambda index: {
                0: "SOR-1",
                1: "KRX-1",
                2: "NXT-1",
                3: "ORC-1",
                4: "0",
                5: "accepted",
                6: "",
                7: "",
            }.get(index, "")
        )

        result = client._submit_stock_order("SABA101U1", {37: "0"}, "place_order")

        self.assertTrue(result.accepted)
        self.assertEqual(result.order_id, "SOR-1")
        self.assertEqual(result.raw["sor_order_id"], "SOR-1")
        self.assertEqual(result.raw["krx_order_id"], "KRX-1")
        self.assertEqual(result.raw["nxt_order_id"], "NXT-1")
        self.assertEqual(result.raw["orc_order_id"], "ORC-1")
        self.assertEqual(result.raw["submitted_order_method_code"], "0")

    def test_real_stock_order_result_does_not_accept_orc_only(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._request = Mock()
        client._single_text = Mock(
            side_effect=lambda index: {
                0: "",
                1: "0",
                2: "",
                3: "ORC-1",
                4: "0",
                5: "check",
                6: "",
                7: "",
            }.get(index, "")
        )

        result = client._submit_stock_order("SABA101U1", {37: "0"}, "place_order")

        self.assertFalse(result.accepted)
        self.assertIsNone(result.order_id)
        self.assertEqual(result.raw["orc_order_id"], "ORC-1")

    def test_real_stock_order_result_reads_modify_order_method_from_saba102u1_si35(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._request = Mock()
        client._single_text = Mock(
            side_effect=lambda index: {
                0: "SOR-1",
                4: "0",
                5: "accepted",
            }.get(index, "")
        )

        result = client._submit_stock_order("SABA102U1", {35: "0", 37: "SOR-ORIGINAL-1"}, "modify_order")

        self.assertTrue(result.accepted)
        self.assertEqual(result.raw["submitted_order_method_code"], "0")

    def test_real_gold_order_uses_saba871u1_limit_vectors(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._account_password = Mock(return_value="1234")
        client._request = Mock()
        client._single_text = Mock(side_effect=lambda index: {
            0: "GOLD-ORDER-1",
            1: "CH-ORDER-1",
            2: "0",
            3: "ok",
            4: "",
            5: "",
        }.get(index, ""))

        place = GoldOrderRequest(
            account_no="12345678901",
            code="M04020000",
            side="buy",
            quantity=2,
            price=153000,
            action="place",
        )
        modify = GoldOrderRequest(
            account_no="12345678901",
            code="M04020000",
            side="sell",
            quantity=2,
            price=153100,
            original_order_id="GOLD-ORDER-1",
            action="modify",
        )
        cancel = GoldOrderRequest(
            account_no="12345678901",
            code="M04020000",
            side="sell",
            quantity=2,
            price=None,
            original_order_id="GOLD-ORDER-1",
            action="cancel",
        )

        client.place_gold_order(place)
        client.modify_gold_order(modify)
        client.cancel_gold_order(cancel)

        calls = client._request.call_args_list
        self.assertEqual([call.args[0] for call in calls], ["SABA871U1", "SABA871U1", "SABA871U1"])
        place_inputs = calls[0].args[1]
        modify_inputs = calls[1].args[1]
        cancel_inputs = calls[2].args[1]
        self.assertEqual(place_inputs[1], "70")
        self.assertEqual(place_inputs[5], "2")
        self.assertEqual(place_inputs[6], "M04020000")
        self.assertEqual(place_inputs[8], "153000")
        self.assertEqual(place_inputs[10], "2")
        self.assertEqual(place_inputs[14], "Y")
        self.assertEqual(modify_inputs[5], "3")
        self.assertEqual(modify_inputs[8], "153100")
        self.assertEqual(modify_inputs[12], "GOLD-ORDER-1")
        self.assertEqual(modify_inputs[14], "Y")
        self.assertEqual(cancel_inputs[5], "4")
        self.assertEqual(cancel_inputs[8], "0")
        self.assertEqual(cancel_inputs[12], "GOLD-ORDER-1")
        self.assertEqual(cancel_inputs[14], "Y")

    def test_real_gold_order_rejects_zero_order_id(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._account_password = Mock(return_value="1234")
        client._request = Mock()
        client._single_text = Mock(side_effect=lambda index: {
            0: "0",
            3: "rejected",
        }.get(index, ""))

        result = client.place_gold_order(
            GoldOrderRequest(
                account_no="12345678901",
                code="M04020000",
                side="buy",
                quantity=1,
                price=153000,
                action="place",
            )
        )

        self.assertFalse(result.accepted)
        self.assertIsNone(result.order_id)
        self.assertEqual(result.raw["order_id"], None)

    def test_real_gold_code_rejects_stock_normalization(self):
        client = RealIndiClient.__new__(RealIndiClient)

        self.assertEqual(client.normalize_gold_code(None), "M04020000")
        with self.assertRaisesRegex(ValueError, "gold code"):
            client.normalize_gold_code("005930")
        with self.assertRaisesRegex(ValueError, "gold code"):
            client.normalize_gold_code("A005930")

    def test_real_gold_rt_control_is_lazy_and_distinct_from_stock_control(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_control = Mock(name="stock_rt_control")
        client._gold_rt_control = None
        client._gold_rt_control_lock = threading.RLock()
        client._gold_rt_subscription_counts = {}
        client._gold_rt_snapshots = {}
        client._gold_rt_listeners = []
        client._last_rt_error_details = None
        client._reset_gold_rt_wait_state = Mock()

        calls: list[tuple[str, tuple[object, ...]]] = []
        gold_control = Mock()
        gold_control.isNull.return_value = False
        gold_control.ReceiveRTData = Mock()
        gold_control.ReceiveSysMsg = Mock()

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "RequestRTReg(QVariant, QVariant)":
                return 1
            raise AssertionError(f"unexpected signature: {signature} {args}")

        gold_control.dynamicCall.side_effect = dynamic_call
        client._qax_widget_cls = Mock(return_value=gold_control)

        result = client.subscribe_gold_realtime_price("M04020000")

        self.assertTrue(result["subscribed"])
        self.assertEqual(result["rt_type"], "XC")
        self.assertIs(client._gold_rt_control, gold_control)
        self.assertIsNot(client._gold_rt_control, client._rt_control)
        gold_control.ReceiveRTData.connect.assert_called_once_with(client._on_receive_gold_rt_data)
        self.assertEqual(calls, [("RequestRTReg(QVariant, QVariant)", ("XC", "M04020000"))])
        self.assertEqual(client._gold_rt_subscription_counts[("XC", "M04020000")], 1)

    def test_real_gold_product_chart_and_account_queries_use_gold_tr_vectors(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._request = Mock(return_value=SimpleNamespace(multi_row_count=0))
        client._single_text = Mock(return_value="")
        client._single_int = Mock(return_value=0)
        client._single_float = Mock(return_value=0.0)
        client._account_password = Mock(return_value="1234")

        client.list_gold_products()
        client.get_gold_daily_prices("M04020000", "2026-04-01", "2026-04-02")
        client.get_gold_intraday_prices("M04020100", "20260403", 5)
        client.get_gold_account_balance("12345678901")

        self.assertEqual(client._request.call_args_list[0].args[0], "XB")
        self.assertEqual(client._request.call_args_list[0].args[1], ["M04020000"])
        self.assertEqual(client._request.call_args_list[1].args[0], "XB")
        self.assertEqual(client._request.call_args_list[1].args[1], ["M04020100"])
        self.assertEqual(
            client._request.call_args_list[2].args,
            ("TR_GLCHART", ["M04020000", "D", "1", "20260401", "20260402", "9999"]),
        )
        self.assertEqual(
            client._request.call_args_list[3].args,
            ("TR_GLCHART", ["M04020100", "M", "5", "20260403", "20260403", "9999"]),
        )
        self.assertEqual(
            client._request.call_args_list[4].args,
            ("SABA835Q1", ["12345678901", "70", "1234", "1"]),
        )

    def test_real_gold_quote_and_order_book_request_gold_rt_types(self):
        client = RealIndiClient.__new__(RealIndiClient)
        xc_fields = [
            "KRD040200002",
            "M04020000",
            "153000",
            "153000",
            "2",
            "100",
            "0.1",
            "10",
            "1530000",
            "1",
            "152900",
            "153100",
            "152800",
            "090000",
            "100000",
            "091000",
            "0",
            "2",
            "153100",
            "153000",
        ]
        xh_fields = ["KRD040200002", "M04020000", "153001", "1"] + ["0"] * 65
        client._get_gold_rt_snapshot_once = Mock(side_effect=[xc_fields, xh_fields])

        quote = client.get_gold_quote_snapshot("M04020000")
        order_book = client.get_gold_order_book("M04020000")

        self.assertEqual(quote.code, "M04020000")
        self.assertEqual(order_book.source, "XH")
        self.assertEqual(
            client._get_gold_rt_snapshot_once.call_args_list[0].args,
            ("XC", "M04020000"),
        )
        self.assertEqual(
            client._get_gold_rt_snapshot_once.call_args_list[1].args,
            ("XH", "M04020000"),
        )

    def test_real_gold_snapshot_preserves_existing_rt_subscription(self):
        client = RealIndiClient.__new__(RealIndiClient)
        fields = ["KRD040200002", "M04020000", "090000", "153000"] + ["0"] * 16
        client._gold_rt_subscription_counts = {("XC", "M04020000"): 1}
        client._gold_rt_snapshots = {("XC", "M04020000"): fields}
        client._register_gold_realtime = Mock()
        client._unregister_gold_realtime = Mock()

        snapshot = client._get_gold_rt_snapshot_once("XC", "M04020000")

        self.assertIs(snapshot, fields)
        client._register_gold_realtime.assert_not_called()
        client._unregister_gold_realtime.assert_not_called()

    def test_real_gold_snapshot_timeout_preserves_existing_rt_subscription(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._gold_rt_subscription_counts = {("XC", "M04020000"): 1}
        client._gold_rt_snapshots = {}
        client._gold_rt_control_lock = threading.RLock()
        client._ensure_gold_rt_control = Mock()
        client._reset_gold_rt_wait_state = Mock()
        client._wait_for_gold_rt_snapshot = Mock(side_effect=TimeoutError("waiting for live XC"))
        client._register_gold_realtime = Mock()
        client._unregister_gold_realtime = Mock()

        with self.assertRaisesRegex(TimeoutError, "waiting for live XC"):
            client._get_gold_rt_snapshot_once("XC", "M04020000")

        client._register_gold_realtime.assert_not_called()
        client._unregister_gold_realtime.assert_not_called()

    def test_real_gold_order_book_timeout_returns_unavailable(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._get_gold_rt_snapshot_once = Mock(side_effect=TimeoutError("quiet gold book"))

        order_book = client.get_gold_order_book("M04020000")

        self.assertFalse(order_book.available)
        self.assertEqual(order_book.source, "XH")
        self.assertIn("quiet gold book", order_book.message)

    def test_real_gold_order_book_uses_simultaneous_phase_field(self):
        fields = ["KRD040200002", "M04020000", "153001", "1"] + ["0"] * 65
        fields[46] = "1"

        order_book = RealIndiClient._build_gold_order_book("M04020000", fields)

        self.assertEqual(order_book.market_phase, "auction")

    def test_real_request_uses_explicit_sparse_indices_when_dict_inputs_are_given(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._tr_control_lock = threading.RLock()
        client._event_loop_cls = Mock(return_value=SimpleNamespace(exec_=Mock(), exit=Mock()))
        client._reset_request_state = Mock()
        client._format_error = Mock(return_value="error")
        client._tr_data_ready = True
        client._pending_rqid = None
        client._received_rqid = 1
        client._timed_out = False
        client._active_event_loop = None
        client._timer_cls = Mock()
        client._tr_control = Mock()

        calls: list[tuple[str, tuple[object, ...]]] = []

        def dynamic_call(signature: str, *args: object) -> object:
            calls.append((signature, args))
            if signature == "SetQueryName(QString)":
                return True
            if signature == "SetSingleData(int, QString)":
                return True
            if signature == "RequestData()":
                return 1
            if signature == "GetSingleRowCount()":
                return 1
            if signature == "GetMultiRowCount()":
                return 0
            if signature == "GetErrorState()":
                return 0
            if signature == "GetErrorCode()":
                return ""
            if signature == "GetErrorMessage()":
                return ""
            raise AssertionError(f"unexpected dynamicCall: {signature}")

        client._tr_control.dynamicCall.side_effect = dynamic_call

        client._request("SABA101U1", {0: "acct", 20: "", 21: "Y", 36: "", 37: "1"})

        set_single_calls = [args for signature, args in calls if signature == "SetSingleData(int, QString)"]
        self.assertEqual(
            set_single_calls,
            [
                (0, "acct"),
                (20, ""),
                (21, "Y"),
                (36, ""),
                (37, "1"),
            ],
        )

    def test_health_check_dispatches_system_callback_without_restart_on_indi_recreation(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            dispatched_bodies: list[dict[str, object] | None] = []
            tools.register_system_callback({"method": "POST", "url": "http://localhost:9000/system"})
            tools._client.health_check = Mock(
                return_value=HealthStatus(
                    ok=False,
                    backend="real",
                    python_architecture="32bit",
                    ocx_ready=True,
                    login_ready=True,
                    live_orders_allowed=False,
                    message="real backend probe",
                    indi_process_running=True,
                    indi_process_restarted=True,
                    indi_process_message="GiExpertMain.exe generation changed",
                )
            )

            with patch("homestock.runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                dispatch.side_effect = lambda _self, callback: (
                    dispatched_bodies.append(callback.body)
                    or {"delivered": True, "error": None}
                )
                result = tools.health_check()
                self.assertTrue(wait_for_scripter_idle(tools))

            self.assertTrue(result["indi_process_restarted"])
            self.assertEqual(len(dispatched_bodies), 1)
            self.assertEqual(dispatched_bodies[0]["event_type"], "ocx_recreated")
            self.assertEqual(dispatched_bodies[0]["message"], "OCX 재생성 감지. 자동 재시작은 수행하지 않음")

    def test_indi_recreation_system_callback_is_only_sent_once(self):
        tools = make_tools()
        status = {
            "restarted": True,
            "message": "GiExpertMain.exe generation changed",
        }

        with patch.object(tools._scripter, "system_callback") as dispatch:
            first = tools._dispatch_indi_process_recreated_callback(status)
            second = tools._dispatch_indi_process_recreated_callback(status)

        self.assertTrue(first)
        self.assertTrue(second)
        self.assertEqual(dispatch.call_count, 1)

    def test_list_stocks_returns_mock_records(self):
        result = make_tools().list_stocks()

        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["market"], "KOSPI")

    def test_get_market_index_prices_returns_grouped_series(self):
        result = make_tools().get_market_index_prices("20260421", "20260422")

        self.assertEqual(list(result.keys()), ["kospi200", "sp500", "nasdaq", "usdkrw"])
        self.assertEqual(
            result["kospi200"],
            [
                {"date": "20260422", "open": 354.0, "high": 355.2, "low": 352.8, "close": 354.81},
                {"date": "20260421", "open": 352.95, "high": 354.1, "low": 351.88, "close": 353.72},
            ],
        )
        self.assertEqual(
            result["sp500"][0],
            {"date": "20260422", "open": 5269.0, "high": 5280.1, "low": 5258.3, "close": 5274.66},
        )
        self.assertEqual(
            result["sp500"][1],
            {"date": "20260421", "open": 5256.0, "high": 5271.33, "low": 5248.72, "close": 5268.14},
        )
        self.assertEqual(
            result["nasdaq"][1],
            {"date": "20260421", "open": 18296.0, "high": 18388.22, "low": 18240.1, "close": 18340.77},
        )
        self.assertEqual(
            result["usdkrw"][0],
            {"date": "20260422", "open": 1386.8, "high": 1391.2, "low": 1384.5, "close": 1389.7},
        )
        self.assertEqual(
            result["usdkrw"][1],
            {"date": "20260421", "open": 1384.2, "high": 1388.1, "low": 1381.3, "close": 1386.5},
        )

    def test_get_market_index_prices_accepts_hyphenated_dates(self):
        result = make_tools().get_market_index_prices("2026-04-22", "2026-04-22")

        self.assertEqual(len(result["kospi200"]), 1)
        self.assertEqual(result["kospi200"][0]["date"], "20260422")
        self.assertEqual(result["sp500"][0]["date"], "20260422")
        self.assertEqual(result["nasdaq"][0]["date"], "20260422")

    def test_get_market_index_prices_rejects_reversed_dates(self):
        with self.assertRaisesRegex(ValueError, "start_date"):
            make_tools().get_market_index_prices("20260423", "20260421")

    def test_realtime_subscribe_tracks_duplicates(self):
        tools = make_tools()

        first = tools.subscribe_realtime_price("005930")
        second = tools.subscribe_realtime_price("005930")

        self.assertTrue(first["subscribed"])
        self.assertEqual(first["rt_type"], "UC")
        self.assertFalse(first["already_subscribed"])
        self.assertTrue(second["already_subscribed"])

    def test_realtime_unsubscribe_reports_consistent_schema(self):
        tools = make_tools()
        tools.subscribe_realtime_price("005930")

        result = tools.unsubscribe_realtime_price("005930")

        self.assertFalse(result["subscribed"])
        self.assertEqual(result["rt_type"], "UC")
        self.assertTrue(result["was_subscribed"])
        self.assertEqual(result["remaining_subscriptions"], 0)

    def test_gold_realtime_unsubscribe_preserves_duplicate_owner(self):
        tools = make_tools()
        tools.subscribe_gold_realtime_price("M04020000")
        tools.subscribe_gold_realtime_price("M04020000")

        first_unsubscribe = tools.unsubscribe_gold_realtime_price("M04020000")

        self.assertFalse(first_unsubscribe["subscribed"])
        self.assertEqual(first_unsubscribe["rt_type"], "XC")
        self.assertTrue(first_unsubscribe["was_subscribed"])
        self.assertEqual(first_unsubscribe["remaining_subscriptions"], 1)
        client = tools._client
        assert isinstance(client, MockIndiClient)
        self.assertIn("M04020000", client._gold_subscriptions)

    def test_gold_public_tools_return_mock_results(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                products = tools.list_gold_products()
                quote = tools.get_gold_quote_snapshot()
                daily = tools.get_gold_daily_prices("M04020000")
                intraday = tools.get_gold_intraday_prices("M04020000", "20260423")
                order_book = tools.get_gold_order_book("M04020000")
                account_balance = tools.get_gold_account_balance("12345678901")
            finally:
                tools.close()

        self.assertEqual(products[0]["code"], "M04020000")
        self.assertEqual(quote["code"], "M04020000")
        self.assertEqual(daily[0]["date"], "20260420")
        self.assertEqual(intraday[0]["date"], "20260423")
        self.assertEqual(order_book["source"], "XH")
        self.assertEqual(account_balance["account_no"], "12345678901")
        self.assertEqual(account_balance["summary"]["account_no"], "12345678901")
        self.assertEqual(account_balance["balance"]["code"], "M04020000")

    def test_gold_realtime_subscribe_is_separate_from_stock(self):
        tools = make_tools()
        client = tools._client
        assert isinstance(client, MockIndiClient)

        stock = tools.subscribe_realtime_price("005930")
        gold = tools.subscribe_gold_realtime_price("M04020000")

        self.assertEqual(stock["rt_type"], "UC")
        self.assertEqual(gold["rt_type"], "XC")
        self.assertIn("005930", client._subscriptions)
        self.assertIn("M04020000", client._gold_subscriptions)

    def test_gold_runtime_restore_failure_does_not_block_stock_tools(self):
        with TemporaryDirectory() as tempdir:
            migration_now = datetime(2026, 4, 27, 9, 15, 0, tzinfo=timezone(timedelta(hours=9)))
            runtime_path = Path(tempdir) / "gold_runtime_state_20260427.json"
            runtime_payload = {
                "version": 1,
                "trading_date": "20260427",
                "updated_at": "20260427090000",
                "price_alerts": [{"alert_id": "gold_alert_restore_test", "code": "M04020000"}],
                "gold_price_callbacks": [],
            }
            runtime_path.write_text(json.dumps(runtime_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            with patch.object(
                MockIndiClient,
                "subscribe_gold_realtime_price",
                side_effect=RuntimeError("RequestRTReg failed: XC M04020000"),
            ):
                with patch("homestock.gold_runtime_state._kst_now", return_value=migration_now):
                    tools = make_tools(runtime_state_dir=tempdir)
            try:
                self.assertEqual(tools.list_stocks()[0]["code"], "005930")
                health = tools.health_check()
                self.assertFalse(health["gold_runtime"]["available"])
                self.assertIn("RequestRTReg failed", health["gold_runtime"]["message"])
                with self.assertRaisesRegex(RuntimeError, "gold runtime unavailable"):
                    tools.list_gold_price_alerts()
            finally:
                tools.close()

    def test_gold_price_alerts_and_callbacks_use_gold_runtime_only(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            client = tools._client
            assert isinstance(client, MockIndiClient)
            callback = {"method": "POST", "url": "http://localhost:9000/gold"}
            try:
                alert = tools.register_gold_price_alert("M04020000", "climb", 153100, message="gold", httpCallback=callback)
                step = tools.register_gold_price_callback("M04020000", 100, callback)

                self.assertTrue(alert["alert_id"].startswith("gold_alert_"))
                self.assertTrue(step["gold_price_callback_id"].startswith("gold_price_callback_"))
                self.assertEqual(tools._runtime_state._owned_price_codes, {})
                self.assertEqual(tools._gold_runtime_state._owned_price_codes, {"M04020000": 2})
                self.assertIn("M04020000", client._gold_subscriptions)
                self.assertNotIn("M04020000", client._subscriptions)

                before_stock_event = tools.list_gold_price_alerts()[0]["current_price"]
                client.emit_rt_event({"rt_type": "SC", "code": "005930", "current_price": 154000})
                self.assertEqual(tools.list_gold_price_alerts()[0]["current_price"], before_stock_event)

                client.emit_gold_rt_event({"rt_type": "XC", "code": "M04020000", "current_price": 153000, "time": "090000"})
                client.emit_gold_rt_event({"rt_type": "XC", "code": "M04020000", "current_price": 153200, "time": "090100"})
                alerts = tools.list_gold_price_alerts()
                callbacks = tools.list_gold_price_callbacks()

                self.assertEqual(alerts[0]["current_price"], 153200.0)
                self.assertEqual(callbacks[0]["fired_count"], 1)
            finally:
                tools.close()

            state_files = [path.name for path in Path(tempdir).glob("*.json")]
            self.assertTrue(any(name.startswith("gold_runtime_state_") for name in state_files))
            self.assertFalse(any(name.startswith("subscribtion_state_") for name in state_files))

    def test_gold_fastmove_alert_trailing_cooldown_fires_latest_price(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            client = tools._client
            assert isinstance(client, MockIndiClient)

            try:
                with patch.object(GoldRuntimeStateManager, "PRICE_ALERT_COOLDOWN_SECONDS_PER_MINUTE", 0.05):
                    with patch("homestock.gold_runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                        dispatch.side_effect = lambda _self, _callback: {"queued": True}
                        tools.register_gold_price_alert(
                            "M04020000",
                            "fastmove",
                            1.0,
                            window_minutes=1,
                            message="gold fastmove",
                            httpCallback={"method": "POST", "url": "http://localhost:9000/gold"},
                        )
                        client.emit_gold_rt_event(
                            {"rt_type": "XC", "code": "M04020000", "current_price": 153000, "time": "090000"}
                        )
                        client.emit_gold_rt_event(
                            {"rt_type": "XC", "code": "M04020000", "current_price": 154600, "time": "090001"}
                        )
                        client.emit_gold_rt_event(
                            {"rt_type": "XC", "code": "M04020000", "current_price": 155000, "time": "090002"}
                        )
                        client.emit_gold_rt_event(
                            {"rt_type": "XC", "code": "M04020000", "current_price": 156200, "time": "090003"}
                        )
                        self.assertEqual(dispatch.call_count, 1)
                        time.sleep(0.12)
                self.assertEqual(dispatch.call_count, 2)
                alerts = tools._gold_runtime_state._state["price_alerts"]
                self.assertEqual(alerts[0]["last_price"], 156200.0)
                self.assertEqual(alerts[0]["baseline_price"], 156200.0)
            finally:
                tools.close()

    def test_gold_runtime_clears_state_when_trading_date_rolls(self):
        with TemporaryDirectory() as tempdir:
            client = MockIndiClient()
            old_now = datetime(2026, 4, 27, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))
            new_now = datetime(2026, 4, 28, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))
            with patch("homestock.gold_runtime_state._kst_now", return_value=old_now):
                manager = GoldRuntimeStateManager(client, tempdir, restore_realtime=False)
            try:
                manager._state["trading_date"] = "20260427"
                manager._state["price_alerts"] = [{"alert_id": "gold_alert_old", "code": "M04020000"}]
                manager._owned_price_codes = {"M04020000": 1}

                with patch("homestock.gold_runtime_state._kst_now", return_value=new_now):
                    manager._maybe_cleanup_closed_market()

                self.assertEqual(manager._state["price_alerts"], [])
                self.assertEqual(manager._state["gold_price_callbacks"], [])
                self.assertEqual(manager._owned_price_codes, {})
            finally:
                manager.close()

    def test_gold_runtime_restore_failure_dispatches_system_event_and_reraises(self):
        with TemporaryDirectory() as tempdir:
            migration_now = datetime(2026, 4, 27, 9, 15, 0, tzinfo=timezone(timedelta(hours=9)))
            runtime_path = Path(tempdir) / "gold_runtime_state_20260427.json"
            runtime_payload = {
                "version": 1,
                "trading_date": "20260427",
                "updated_at": "20260427090000",
                "price_alerts": [{"alert_id": "gold_alert_restore_test", "code": "M04020000"}],
                "gold_price_callbacks": [],
            }
            runtime_path.write_text(json.dumps(runtime_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            client = MockIndiClient()
            recorder = Mock()

            with patch.object(
                client,
                "subscribe_gold_realtime_price",
                side_effect=RuntimeError("RequestRTReg failed: XC M04020000"),
            ) as subscribe_gold:
                with patch.object(
                    client,
                    "get_last_rt_error_details",
                    return_value={
                        "method_name": "RequestRTReg",
                        "attempted_signature": "RequestRTReg(QString, QString)",
                        "error_state": 3,
                        "error_code": "G123",
                        "error_message": "gold restore failed",
                    },
                ):
                    with patch("homestock.gold_runtime_state._kst_now", return_value=migration_now):
                        with self.assertRaisesRegex(RuntimeError, "RequestRTReg failed: XC M04020000"):
                            GoldRuntimeStateManager(
                                client,
                                tempdir,
                                system_event_recorder=recorder,
                            )

            subscribe_gold.assert_called_once_with("M04020000")
            recorder.assert_called_once()
            event_type, message, details = recorder.call_args.args
            self.assertEqual(event_type, "subscription_restore_failed")
            self.assertIn("금현물 가격 알람 구독 복구 실패", message)
            self.assertEqual(details["subscription_kind"], "gold_price_alert")
            self.assertEqual(details["method_name"], "RequestRTReg")

    def test_gold_runtime_invalid_persisted_filter_skips_only_bad_callback(self):
        with TemporaryDirectory() as tempdir:
            client = MockIndiClient()
            manager = GoldRuntimeStateManager(client, tempdir, restore_realtime=False)
            try:
                callback = {
                    "method": "POST",
                    "url": "http://localhost:9000/gold",
                    "body": {"price": "{{price}}", "price_raw": "{{price_raw}}"},
                }
                manager._state["gold_price_callbacks"] = [
                    {
                        "gold_price_callback_id": "bad_filter",
                        "code": "M04020000",
                        "step": 100.0,
                        "price_filter": "broken",
                        "httpCallback": callback,
                        "registered_at": "20260427090000",
                        "last_price": None,
                        "baseline_price": 153000.0,
                        "last_direction": None,
                        "fired_count": 0,
                        "last_fired_at": None,
                    },
                    {
                        "gold_price_callback_id": "good_filter",
                        "code": "M04020000",
                        "step": 100.0,
                        "price_filter": None,
                        "httpCallback": callback,
                        "registered_at": "20260427090000",
                        "last_price": None,
                        "baseline_price": 153000.0,
                        "last_direction": None,
                        "fired_count": 0,
                        "last_fired_at": None,
                    },
                ]

                with patch("homestock.gold_runtime_state.CallbackDispatcher.dispatch", autospec=True) as dispatch:
                    manager._on_gold_rt_event(
                        {"rt_type": "XC", "code": "M04020000", "current_price": 153200, "time": "090100"}
                    )

                dispatch.assert_called_once()
                self.assertEqual(dispatch.call_args.args[1].body, {"price": "153,200", "price_raw": "153200"})
                self.assertEqual(manager._state["gold_price_callbacks"][0]["fired_count"], 0)
                self.assertEqual(manager._state["gold_price_callbacks"][1]["fired_count"], 1)
            finally:
                manager.close()

    def test_gold_order_is_blocked_by_default(self):
        result = make_tools().place_gold_order(
            account_no="12345678901",
            code="M04020000",
            side="buy",
            quantity=1,
            price=153000,
        )

        self.assertFalse(result["accepted"])
        self.assertFalse(result["live_order"])
        self.assertEqual(result["raw"]["order_type"], "limit")
        self.assertIn("ALLOW_LIVE_ORDERS", result["message"])

    def test_gold_order_reaches_backend_when_live_orders_enabled(self):
        result = make_tools(allow_live_orders=True).place_gold_order(
            account_no="12345678901",
            code="M04020000",
            side="buy",
            quantity=1,
            price=153000,
        )

        self.assertTrue(result["accepted"])
        self.assertFalse(result["live_order"])
        self.assertTrue(result["order_id"].startswith("mock-gold-place"))

    def test_place_order_is_blocked_by_default(self):
        result = make_tools().place_order(
            account_no="12345678901",
            code="005930",
            side="buy",
            quantity=1,
            price=70000,
        )

        self.assertFalse(result["accepted"])
        self.assertFalse(result["live_order"])
        self.assertIn("ALLOW_LIVE_ORDERS", result["message"])

    def test_place_order_reaches_backend_when_live_orders_enabled(self):
        with patch.object(
            HomestockTools,
            "_now_kst",
            return_value=datetime(2026, 5, 11, 10, 0, 0, tzinfo=timezone(timedelta(hours=9))),
        ):
            result = make_tools(allow_live_orders=True).place_order(
                account_no="12345678901",
                code="005930",
                side="buy",
                quantity=1,
                price=70000,
            )

        self.assertTrue(result["accepted"])
        self.assertFalse(result["live_order"])
        self.assertTrue(result["order_id"].startswith("mock-place"))

    def test_place_order_rejects_transition_window(self):
        tools = make_tools(allow_live_orders=True)

        with patch.object(
            HomestockTools,
            "_now_kst",
            return_value=datetime(2026, 5, 11, 8, 55, 0, tzinfo=timezone(timedelta(hours=9))),
        ):
            with self.assertRaisesRegex(ValueError, "08:50:00-09:00:30"):
                tools.place_order(
                    account_no="12345678901",
                    code="005930",
                    side="buy",
                    quantity=1,
                    price=70000,
                )

    def test_place_order_rejects_market_order_during_nxt_only_session(self):
        tools = make_tools(allow_live_orders=True)

        with patch.object(
            HomestockTools,
            "_now_kst",
            return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
        ):
            with self.assertRaisesRegex(ValueError, "market stock orders"):
                tools.place_order(
                    account_no="12345678901",
                    code="005930",
                    side="buy",
                    quantity=1,
                    price=None,
                    order_type="market",
                )

    def test_place_order_rejects_outside_supported_stock_sessions(self):
        tools = make_tools(allow_live_orders=True)

        with patch.object(
            HomestockTools,
            "_now_kst",
            return_value=datetime(2026, 5, 11, 20, 0, 0, tzinfo=timezone(timedelta(hours=9))),
        ):
            with self.assertRaisesRegex(ValueError, "outside supported KST sessions"):
                tools.place_order(
                    account_no="12345678901",
                    code="005930",
                    side="buy",
                    quantity=1,
                    price=70000,
                )

    def test_cancel_order_allows_transition_window_for_existing_open_order(self):
        tools = make_tools(allow_live_orders=True)

        with patch.object(
            HomestockTools,
            "_now_kst",
            return_value=datetime(2026, 5, 11, 8, 55, 0, tzinfo=timezone(timedelta(hours=9))),
        ):
            result = tools.cancel_order(
                account_no="12345678901",
                code="005930",
                side="buy",
                quantity=1,
                original_order_id="MOCK-SOR-1",
            )

        self.assertTrue(result["accepted"])
        self.assertEqual(result["raw"]["original_order_id"], "MOCK-OPEN-1")

    def test_get_accounts_includes_optional_product_metadata(self):
        result = make_tools().get_accounts()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["account_no"], "12345678901")
        self.assertEqual(result[0]["product_code"], "01")
        self.assertEqual(result[0]["product_name"], "종합계좌")
        self.assertIsNone(result[0]["parent_product_code"])

    def test_modify_order_can_pass_credit_trade_type_for_existing_credit_order(self):
        with patch.object(
            HomestockTools,
            "_now_kst",
            return_value=datetime(2026, 5, 11, 10, 0, 0, tzinfo=timezone(timedelta(hours=9))),
        ):
            result = make_tools(allow_live_orders=True).modify_order(
                account_no="12345678901",
                code="005930",
                side="buy",
                quantity=1,
                original_order_id="MOCK-SOR-1",
                price=70000,
                order_type="limit",
                credit_trade_type="01",
            )

        self.assertTrue(result["accepted"])
        self.assertEqual(result["raw"]["credit_trade_type"], "01")
        self.assertEqual(result["raw"]["original_order_id"], "MOCK-OPEN-1")
        self.assertEqual(result["raw"]["order_method_code"], "0")
        self.assertEqual(result["raw"]["sor_original_order_id"], "MOCK-SOR-ORIGINAL-1")

    def test_cancel_order_can_pass_credit_trade_type_for_existing_credit_order(self):
        result = make_tools(allow_live_orders=True).cancel_order(
            account_no="12345678901",
            code="005930",
            side="buy",
            quantity=1,
            original_order_id="MOCK-SOR-1",
            credit_trade_type="01",
        )

        self.assertTrue(result["accepted"])
        self.assertEqual(result["raw"]["credit_trade_type"], "01")
        self.assertEqual(result["raw"]["original_order_id"], "MOCK-OPEN-1")
        self.assertEqual(result["raw"]["order_method_code"], "0")
        self.assertEqual(result["raw"]["sor_original_order_id"], "MOCK-SOR-ORIGINAL-1")

    def test_modify_order_fails_closed_when_open_order_is_not_found(self):
        tools = make_tools(allow_live_orders=True)

        with patch.object(
            HomestockTools,
            "_now_kst",
            return_value=datetime(2026, 5, 11, 10, 0, 0, tzinfo=timezone(timedelta(hours=9))),
        ):
            with self.assertRaisesRegex(ValueError, "open order not found"):
                tools.modify_order(
                    account_no="12345678901",
                    code="005930",
                    side="buy",
                    quantity=1,
                    original_order_id="UNKNOWN",
                    price=70000,
                    order_type="limit",
                )

    def test_cancel_order_fails_closed_when_quantity_exceeds_unfilled_quantity(self):
        tools = make_tools(allow_live_orders=True)

        with self.assertRaisesRegex(ValueError, "cancel quantity exceeds"):
            tools.cancel_order(
                account_no="12345678901",
                code="005930",
                side="buy",
                quantity=71,
                original_order_id="MOCK-SOR-1",
            )

    def test_register_order_carryover_registers_resolved_open_order(self):
        tools = make_tools()
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                result = tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
                listed = tools.list_order_carryovers("12345678901", "005930")
        finally:
            tools.close()

        self.assertTrue(result["carryover_id"].startswith("order_carryover_"))
        self.assertEqual(result["account_no"], "12345678901")
        self.assertEqual(result["code"], "005930")
        self.assertEqual(result["current_order_id"], "MOCK-OPEN-1")
        self.assertTrue(result["premarket_to_regular"])
        self.assertTrue(result["regular_to_aftermarket"])
        self.assertIn("MOCK-SOR-1", result["current_order_identifiers"])
        self.assertEqual(
            result["attempted_dates"],
            {"premarket_to_regular": "", "regular_to_aftermarket": ""},
        )
        self.assertEqual(
            result["transition_statuses"],
            {"premarket_to_regular": "pending", "regular_to_aftermarket": "pending"},
        )
        self.assertEqual(result["last_status"], "pending")
        self.assertNotIn("completed_dates", result)
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["carryover_id"], result["carryover_id"])

    def test_register_order_carryover_rejects_when_all_transitions_are_disabled(self):
        tools = make_tools()
        try:
            with self.assertRaisesRegex(ValueError, "at least one"):
                tools.register_order_carryover(
                    "12345678901",
                    "005930",
                    "MOCK-SOR-1",
                    premarket_to_regular=False,
                    regular_to_aftermarket=False,
                )
        finally:
            tools.close()

    def test_register_order_carryover_fails_when_open_order_is_not_found(self):
        tools = make_tools()
        try:
            with self.assertRaisesRegex(ValueError, "open order not found"):
                tools.register_order_carryover("12345678901", "005930", "UNKNOWN")
        finally:
            tools.close()

    def test_cancel_order_carryover_removes_registration_only(self):
        class TrackingMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.place_requests: list[OrderRequest] = []
                self.cancel_requests: list[OrderRequest] = []

            def place_order(self, request: OrderRequest) -> OrderResult:
                self.place_requests.append(request)
                return super().place_order(request)

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancel_requests.append(request)
                return super().cancel_order(request)

        client = TrackingMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                registered = tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
            result = tools.cancel_order_carryover(carryover_id=registered["carryover_id"])
            listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertTrue(result["cancelled"])
        self.assertEqual(result["cancelled_count"], 1)
        self.assertEqual(listed, [])
        self.assertEqual(client.place_requests, [])
        self.assertEqual(client.cancel_requests, [])

    def test_order_carryover_premarket_to_regular_cancels_and_places_sor_limit_order(self):
        class CarryoverMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.place_requests: list[OrderRequest] = []
                self.cancel_requests: list[OrderRequest] = []
                self.cancelled = False

            def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
                self._require_account(account_no)
                if self.cancelled:
                    if code:
                        self._require_code(code)
                    return []
                if code and code != "005930":
                    self._require_code(code)
                    return []
                return [make_full_unfilled_open_order()]

            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return [make_execution()]

            def place_order(self, request: OrderRequest) -> OrderResult:
                self.place_requests.append(request)
                return OrderResult(
                    accepted=True,
                    live_order=False,
                    order_id="MOCK-SOR-NEW",
                    message="mock SOR order accepted",
                    raw={
                        "account_no": request.account_no,
                        "code": request.code,
                        "side": request.side,
                        "quantity": request.quantity,
                        "price": request.price,
                        "order_type": request.order_type,
                        "sor_order_id": "MOCK-SOR-NEW",
                    },
                )

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancel_requests.append(request)
                self.cancelled = True
                return super().cancel_order(request)

        client = CarryoverMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                registered = tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(len(client.cancel_requests), 1)
        self.assertEqual(client.cancel_requests[0].original_order_id, "MOCK-OPEN-1")
        self.assertEqual(client.cancel_requests[0].quantity, 100)
        self.assertEqual(len(client.place_requests), 1)
        self.assertEqual(client.place_requests[0].quantity, 100)
        self.assertEqual(client.place_requests[0].price, 70000)
        self.assertFalse(client.place_requests[0].sor_original_order_id)
        self.assertEqual(listed[0]["carryover_id"], registered["carryover_id"])
        self.assertTrue(listed[0]["last_result"]["executed"])
        self.assertEqual(listed[0]["last_result"]["status"], "success")
        self.assertEqual(listed[0]["last_result"]["transition"], "premarket_to_regular")
        self.assertEqual(listed[0]["last_result"]["cancel_confirmation"]["confirmed_cancelled_quantity"], 100)
        self.assertEqual(listed[0]["attempted_dates"]["premarket_to_regular"], "20260511")
        self.assertEqual(listed[0]["transition_statuses"]["premarket_to_regular"], "success")
        self.assertEqual(listed[0]["transition_statuses"]["regular_to_aftermarket"], "pending")
        self.assertEqual(listed[0]["last_status"], "success")
        self.assertIn("MOCK-SOR-NEW", listed[0]["current_order_identifiers"])

    def test_order_carryover_does_not_reorder_when_cancelled_quantity_is_unconfirmed(self):
        class UnconfirmedCancelMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.place_requests: list[OrderRequest] = []
                self.cancel_requests: list[OrderRequest] = []
                self.cancelled = False

            def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
                self._require_account(account_no)
                if self.cancelled:
                    if code:
                        self._require_code(code)
                    return []
                if code and code != "005930":
                    self._require_code(code)
                    return []
                return [make_full_unfilled_open_order()]

            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return []

            def place_order(self, request: OrderRequest) -> OrderResult:
                self.place_requests.append(request)
                return super().place_order(request)

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancel_requests.append(request)
                self.cancelled = True
                return super().cancel_order(request)

        client = UnconfirmedCancelMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(len(client.cancel_requests), 1)
        self.assertEqual(client.place_requests, [])
        self.assertEqual(listed[0]["last_result"]["status"], "missed")
        self.assertEqual(listed[0]["last_status"], "missed")
        self.assertEqual(
            listed[0]["last_result"]["status_desc"],
            "취소된 수량을 확정할 수 없어 신규 주문을 넣지 않았습니다.",
        )
        self.assertFalse(listed[0]["last_result"]["cancel_confirmation"]["confirmed"])
        self.assertEqual(
            listed[0]["last_result"]["cancel_confirmation"]["status_desc"],
            "취소된 수량을 확정할 수 없어 신규 주문을 넣지 않았습니다.",
        )
        self.assertNotIn("reason", listed[0]["last_result"])
        self.assertNotIn("reason", listed[0]["last_result"]["cancel_confirmation"])

    def test_order_carryover_sums_distinct_execution_rows_for_registered_order(self):
        class SplitExecutionMockClient(MockIndiClient):
            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return [
                    make_execution(order_id="MOCK-OPEN-1", quantity=3),
                    make_execution(order_id="MOCK-OPEN-1", quantity=2),
                    make_execution(order_id="UNRELATED", quantity=4, sor_order_id="MOCK-SOR-1"),
                ]

        tools = HomestockTools(SplitExecutionMockClient(), OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            result = tools._executed_quantity_for_order("12345678901", "005930", {"MOCK-OPEN-1", "MOCK-SOR-1"})
        finally:
            tools.close()

        self.assertEqual(result, 7)

    def test_order_carryover_skips_reorder_when_window_elapsed_after_cancel_confirmation(self):
        class SlowCarryoverMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.place_requests: list[OrderRequest] = []
                self.cancel_requests: list[OrderRequest] = []
                self.cancelled = False

            def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
                self._require_account(account_no)
                if self.cancelled:
                    if code:
                        self._require_code(code)
                    return []
                if code and code != "005930":
                    self._require_code(code)
                    return []
                return [make_full_unfilled_open_order()]

            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return [make_execution()]

            def place_order(self, request: OrderRequest) -> OrderResult:
                self.place_requests.append(request)
                return super().place_order(request)

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancel_requests.append(request)
                self.cancelled = True
                return super().cancel_order(request)

        client = SlowCarryoverMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                with patch.object(
                    tools,
                    "_order_carryover_transition_window_active",
                    side_effect=[True, False],
                ):
                    tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(len(client.cancel_requests), 1)
        self.assertEqual(client.place_requests, [])
        self.assertEqual(listed[0]["last_result"]["status"], "missed")
        self.assertEqual(
            listed[0]["last_result"]["status_desc"],
            "취소 확인 후 전환 실행창이 지나 신규 주문을 넣지 않았습니다.",
        )
        self.assertEqual(listed[0]["last_status"], "missed")

    def test_order_carryover_callback_sends_transition_result_payload(self):
        class CallbackCarryoverMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.cancelled = False

            def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
                self._require_account(account_no)
                if self.cancelled:
                    if code:
                        self._require_code(code)
                    return []
                if code and code != "005930":
                    self._require_code(code)
                    return []
                return [make_full_unfilled_open_order()]

            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return [make_execution()]

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancelled = True
                return super().cancel_order(request)

            def place_order(self, request: OrderRequest) -> OrderResult:
                return OrderResult(
                    accepted=True,
                    live_order=False,
                    order_id="MOCK-CALLBACK-SOR",
                    message="mock callback SOR order accepted",
                    raw={"sor_order_id": "MOCK-CALLBACK-SOR"},
                )

        client = CallbackCarryoverMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch("homestock.tools.CallbackDispatcher.dispatch", autospec=True, return_value={"queued": True}) as dispatch:
                with patch.object(
                    HomestockTools,
                    "_now_kst",
                    return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
                ):
                    tools.register_order_carryover(
                        "12345678901",
                        "005930",
                        "MOCK-SOR-1",
                        httpCallback={"method": "POST", "url": "http://localhost:9000/carryover"},
                    )
                with patch.object(
                    HomestockTools,
                    "_now_kst",
                    return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
                ):
                    tools._process_due_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(dispatch.call_count, 1)
        callback = dispatch.call_args.args[1]
        self.assertEqual(callback.url, "http://localhost:9000/carryover")
        self.assertEqual(callback.body_format, "json")
        self.assertEqual(callback.body["event_type"], "order_carryover_transition")
        self.assertEqual(callback.body["status"], "success")
        self.assertEqual(callback.body["status_desc"], "자동 이월 주문을 성공적으로 접수했습니다.")
        self.assertNotIn("success", callback.body)
        self.assertNotIn("reason", callback.body)
        self.assertNotIn("failure_reason", callback.body)
        self.assertEqual(callback.body["quantity"], 100)
        self.assertEqual(callback.body["price"], 70000)
        self.assertEqual(callback.body["side_label"], "매수")
        self.assertEqual(callback.body["target_market"], "SOR")

    def test_list_order_carryovers_dispatches_missed_callback_once(self):
        tools = make_tools(allow_live_orders=True)
        try:
            with patch("homestock.tools.CallbackDispatcher.dispatch", autospec=True, return_value={"queued": True}) as dispatch:
                with patch.object(
                    HomestockTools,
                    "_now_kst",
                    return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
                ):
                    tools.register_order_carryover(
                        "12345678901",
                        "005930",
                        "MOCK-SOR-1",
                        premarket_to_regular=True,
                        regular_to_aftermarket=False,
                        httpCallback={"method": "POST", "url": "http://localhost:9000/carryover"},
                    )
                with patch.object(
                    HomestockTools,
                    "_now_kst",
                    return_value=datetime(2026, 5, 11, 9, 2, 0, tzinfo=timezone(timedelta(hours=9))),
                ):
                    first = tools.list_order_carryovers()
                    second = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(dispatch.call_count, 1)
        callback = dispatch.call_args.args[1]
        self.assertEqual(callback.body["status"], "missed")
        self.assertEqual(callback.body["status_desc"], "전환 실행창을 지나 자동 이월을 시도하지 못했습니다.")
        self.assertEqual(first[0]["last_status"], "missed")
        self.assertEqual(second[0]["last_status"], "missed")

    def test_order_carryover_callback_reports_chaos_reason(self):
        tools = make_tools(allow_live_orders=True)
        try:
            with patch("homestock.tools.CallbackDispatcher.dispatch", autospec=True, return_value={"queued": True}) as dispatch:
                with patch.object(
                    HomestockTools,
                    "_now_kst",
                    return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
                ):
                    tools.register_order_carryover(
                        "12345678901",
                        "005930",
                        "MOCK-SOR-1",
                        premarket_to_regular=True,
                        regular_to_aftermarket=False,
                        httpCallback={
                            "method": "POST",
                            "url": "http://localhost:9000/carryover",
                            "body": {
                                "message": "{{stockName}} {{quantity}}주 {{tradePrice}}원 {{sideLabel}} {{targetMarket}} {{status}}",
                                "status_desc": "{{statusDesc}}",
                            },
                        },
                    )
                with patch.object(
                    HomestockTools,
                    "_now_kst",
                    return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
                ):
                    tools._process_due_order_carryovers()
                    listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(dispatch.call_count, 1)
        callback = dispatch.call_args.args[1]
        self.assertEqual(
            callback.body,
            {
                "message": "Samsung Electronics 70주 70,000원 매수 SOR chaos",
                "status_desc": "부분 체결이 확인되어 과주문 방지를 위해 자동 이월을 중단했습니다.",
            },
        )
        self.assertEqual(listed[0]["last_status"], "chaos")

    def test_order_carryover_template_quantity_has_display_and_raw_tokens(self):
        replacements = HomestockTools._order_carryover_callback_replacements(
            {
                "carryover_id": "carryover_test",
                "account_no": "12345678901",
                "name": "Samsung Electronics",
                "code": "005930",
                "side": "buy",
                "side_label": "매수",
                "price": 70000,
                "quantity": 1234,
                "target_market": "SOR",
                "transition": "premarket_to_regular",
                "status": "success",
                "status_desc": "ok",
                "executed": True,
                "skipped": False,
                "last_status_at": "20260511090000",
            }
        )

        self.assertEqual(replacements["quantity"], "1,234")
        self.assertEqual(replacements["quantityRaw"], "1234")
        self.assertEqual(replacements["tradePrice"], "70,000")
        self.assertEqual(replacements["tradePriceRaw"], "70000")

    def test_order_carryover_partial_fill_marks_chaos_without_cancel_or_place(self):
        class PartialFillMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.place_requests: list[OrderRequest] = []
                self.cancel_requests: list[OrderRequest] = []
                self.cancelled = False

            def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
                if self.cancelled:
                    self._require_account(account_no)
                    if code:
                        self._require_code(code)
                    return []
                return super().get_open_orders(account_no, code)

            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return [make_execution()]

            def place_order(self, request: OrderRequest) -> OrderResult:
                self.place_requests.append(request)
                return super().place_order(request)

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancel_requests.append(request)
                self.cancelled = True
                return super().cancel_order(request)

        client = PartialFillMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(client.cancel_requests, [])
        self.assertEqual(client.place_requests, [])
        self.assertEqual(listed[0]["last_result"]["status"], "chaos")
        self.assertEqual(
            listed[0]["last_result"]["status_desc"],
            "부분 체결이 확인되어 과주문 방지를 위해 자동 이월을 중단했습니다.",
        )
        self.assertNotIn("reason", listed[0]["last_result"])
        self.assertEqual(listed[0]["last_status"], "chaos")
        self.assertEqual(listed[0]["last_result"]["filled_quantity"], 30)
        self.assertNotIn("cancel_confirmation", listed[0]["last_result"])

    def test_order_carryover_registered_too_close_to_transition_does_not_execute(self):
        class TrackingMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.place_requests: list[OrderRequest] = []
                self.cancel_requests: list[OrderRequest] = []

            def place_order(self, request: OrderRequest) -> OrderResult:
                self.place_requests.append(request)
                return super().place_order(request)

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancel_requests.append(request)
                return super().cancel_order(request)

        client = TrackingMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 0, 20, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools.register_order_carryover(
                    "12345678901",
                    "005930",
                    "MOCK-SOR-1",
                    premarket_to_regular=True,
                    regular_to_aftermarket=False,
                )
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(client.cancel_requests, [])
        self.assertEqual(client.place_requests, [])
        self.assertEqual(listed[0]["last_result"]["status"], "missed")
        self.assertEqual(
            listed[0]["last_result"]["status_desc"],
            "전환 시작 직전 또는 이후에 등록되어 당일 해당 전환은 실행하지 않았습니다.",
        )
        self.assertNotIn("reason", listed[0]["last_result"])
        self.assertEqual(listed[0]["attempted_dates"]["premarket_to_regular"], "20260511")
        self.assertEqual(listed[0]["transition_statuses"]["premarket_to_regular"], "missed")
        self.assertEqual(listed[0]["last_status"], "missed")

    def test_order_carryover_waits_until_due_transition(self):
        tools = make_tools(allow_live_orders=True)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
                tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertIsNone(listed[0]["last_result"])
        self.assertEqual(listed[0]["last_status"], "pending")
        self.assertEqual(listed[0]["transition_statuses"]["premarket_to_regular"], "pending")

    def test_order_carryover_cancelled_after_claim_does_not_interrupt_inflight_execution(self):
        class TrackingMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.place_requests: list[OrderRequest] = []
                self.cancel_requests: list[OrderRequest] = []
                self.cancelled = False

            def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
                self._require_account(account_no)
                if self.cancelled:
                    if code:
                        self._require_code(code)
                    return []
                if code and code != "005930":
                    self._require_code(code)
                    return []
                return [make_full_unfilled_open_order()]

            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return [make_execution()]

            def place_order(self, request: OrderRequest) -> OrderResult:
                self.place_requests.append(request)
                return OrderResult(
                    accepted=True,
                    live_order=False,
                    order_id="MOCK-SOR-INFLIGHT",
                    message="mock inflight SOR order accepted",
                    raw={
                        "account_no": request.account_no,
                        "code": request.code,
                        "side": request.side,
                        "quantity": request.quantity,
                        "price": request.price,
                        "order_type": request.order_type,
                        "sor_order_id": "MOCK-SOR-INFLIGHT",
                    },
                )

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancel_requests.append(request)
                self.cancelled = True
                return super().cancel_order(request)

        client = TrackingMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                registered = tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                claimed = tools._claim_order_carryover_attempt(
                    registered["carryover_id"],
                    "premarket_to_regular",
                    "20260511",
                )
                cancel_result = tools.cancel_order_carryover(carryover_id=registered["carryover_id"])
                result = tools._execute_order_carryover(claimed, "premarket_to_regular")
                tools._update_order_carryover_after_transition(claimed, "premarket_to_regular", result)
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertFalse(cancel_result["cancelled"])
        self.assertEqual(cancel_result["in_flight_count"], 1)
        self.assertEqual(len(client.cancel_requests), 1)
        self.assertEqual(len(client.place_requests), 1)
        self.assertTrue(result["executed"])
        self.assertEqual(listed[0]["last_status"], "success")

    def test_order_carryover_place_exception_preserves_cancel_result(self):
        class PlaceFailMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.cancelled = False

            def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
                self._require_account(account_no)
                if self.cancelled:
                    if code:
                        self._require_code(code)
                    return []
                if code and code != "005930":
                    self._require_code(code)
                    return []
                return [make_full_unfilled_open_order()]

            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return [make_execution()]

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancelled = True
                return super().cancel_order(request)

            def place_order(self, request: OrderRequest) -> OrderResult:
                raise RuntimeError("place failed")

        client = PlaceFailMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(listed[0]["last_result"]["status"], "missed")
        self.assertEqual(listed[0]["last_result"]["status_desc"], "신규 SOR 주문 전송 중 예외가 발생했습니다.")
        self.assertNotIn("reason", listed[0]["last_result"])
        self.assertEqual(listed[0]["last_status"], "missed")
        self.assertEqual(listed[0]["last_result"]["error_type"], "RuntimeError")
        self.assertTrue(listed[0]["last_result"]["cancel_result"]["accepted"])
        self.assertIsNone(listed[0]["last_result"]["place_result"])

    def test_order_carryover_regular_to_aftermarket_cancels_krx_and_places_sor_limit_order(self):
        class KrxCarryoverMockClient(MockIndiClient):
            def __init__(self) -> None:
                super().__init__()
                self.place_requests: list[OrderRequest] = []
                self.cancel_requests: list[OrderRequest] = []
                self.cancelled = False

            def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
                self._require_account(account_no)
                if self.cancelled:
                    if code:
                        self._require_code(code)
                    return []
                item = OpenOrder(
                    order_id="MOCK-KRX-OPEN-1",
                    code="005930",
                    name="Samsung Electronics",
                    side="buy",
                    order_type="limit",
                    price=70000,
                    quantity=100,
                    filled_quantity=0,
                    unfilled_quantity=100,
                    order_time="20260424140000",
                    status="pending",
                    raw_order_id="MOCK-KRX-OPEN-1",
                    original_raw_order_id="",
                    order_method_code="0",
                    order_method_name="SOR",
                    order_exchange_code="1",
                    order_exchange_name="KRX",
                    sor_order_id="MOCK-KRX-SOR-1",
                    sor_original_order_id="MOCK-KRX-SOR-ORIGINAL-1",
                    credit_trade_type="00",
                )
                if code and code != "005930":
                    return []
                return [item]

            def get_executions(self, account_no: str) -> list[Execution]:
                self._require_account(account_no)
                return [make_execution(order_id="MOCK-KRX-OPEN-1", sor_order_id="MOCK-KRX-SOR-1")]

            def place_order(self, request: OrderRequest) -> OrderResult:
                self.place_requests.append(request)
                return OrderResult(
                    accepted=True,
                    live_order=False,
                    order_id="MOCK-AFTER-SOR-1",
                    message="mock after-hours SOR order accepted",
                    raw={
                        "account_no": request.account_no,
                        "code": request.code,
                        "side": request.side,
                        "quantity": request.quantity,
                        "price": request.price,
                        "order_type": request.order_type,
                        "sor_order_id": "MOCK-AFTER-SOR-1",
                    },
                )

            def cancel_order(self, request: OrderRequest) -> OrderResult:
                self.cancel_requests.append(request)
                self.cancelled = True
                return super().cancel_order(request)

        client = KrxCarryoverMockClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 14, 0, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools.register_order_carryover("12345678901", "005930", "MOCK-KRX-SOR-1")
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 15, 30, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(len(client.cancel_requests), 1)
        self.assertEqual(client.cancel_requests[0].original_order_id, "MOCK-KRX-OPEN-1")
        self.assertEqual(client.cancel_requests[0].quantity, 100)
        self.assertEqual(len(client.place_requests), 1)
        self.assertEqual(client.place_requests[0].quantity, 100)
        self.assertEqual(listed[0]["last_result"]["transition"], "regular_to_aftermarket")
        self.assertEqual(listed[0]["last_result"]["status"], "success")
        self.assertEqual(listed[0]["last_status"], "success")
        self.assertEqual(listed[0]["transition_statuses"]["regular_to_aftermarket"], "success")
        self.assertTrue(listed[0]["last_result"]["executed"])

    def test_order_carryover_registered_after_transition_window_is_marked_missed(self):
        client = MockIndiClient()
        tools = HomestockTools(client, OrderGuard(True), MockScripter(), runtime_state_dir=None)
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 9, 2, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                registered = tools.register_order_carryover(
                    "12345678901",
                    "005930",
                    "MOCK-SOR-1",
                    premarket_to_regular=True,
                    regular_to_aftermarket=False,
                )
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(len(listed), 1)
        self.assertEqual(registered["last_status"], "missed")
        self.assertEqual(registered["last_result"]["status_desc"], "전환 시작 직전 또는 이후에 등록되어 당일 해당 전환은 실행하지 않았습니다.")
        self.assertEqual(listed[0]["last_status"], "missed")
        self.assertEqual(listed[0]["last_result"]["status_desc"], "전환 시작 직전 또는 이후에 등록되어 당일 해당 전환은 실행하지 않았습니다.")
        self.assertNotIn("reason", listed[0]["last_result"])
        self.assertEqual(listed[0]["attempted_dates"]["premarket_to_regular"], "20260511")
        self.assertEqual(listed[0]["transition_statuses"]["premarket_to_regular"], "missed")

    def test_order_carryover_expires_after_registration_day(self):
        tools = make_tools()
        try:
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 11, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools.register_order_carryover("12345678901", "005930", "MOCK-SOR-1")
            with patch.object(
                HomestockTools,
                "_now_kst",
                return_value=datetime(2026, 5, 12, 9, 0, 35, tzinfo=timezone(timedelta(hours=9))),
            ):
                tools._process_due_order_carryovers()
                listed = tools.list_order_carryovers()
        finally:
            tools.close()

        self.assertEqual(listed, [])

    def test_tools_has_no_public_reenter_sor_open_orders_tool(self):
        tools = make_tools()
        try:
            self.assertFalse(hasattr(tools, "reenter_sor_open_orders"))
        finally:
            tools.close()

    def test_get_account_summary_returns_mock_snapshot(self):
        result = make_tools().get_account_summary("12345678901")

        self.assertEqual(result["account_no"], "12345678901")
        self.assertEqual(result["total_deposit"], 12500000)
        self.assertEqual(result["stock_asset_value"], 716000)
        self.assertAlmostEqual(result["total_return_rate"], 2.29)

    def test_get_fundamentals_returns_mock_metrics(self):
        result = make_tools().get_fundamentals("005930")

        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["period_type"], "quarterly")
        self.assertEqual(result[0]["per"], 16.8)
        self.assertEqual(result[0]["eps"], 4200.0)
        self.assertEqual(result[0]["peg"], 0.91)

    def test_get_trade_history_returns_mock_rows(self):
        result = make_tools().get_trade_history("12345678901", "005930", "20250701", "20251231")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["raw_code"], "A005930")
        self.assertEqual(result[0]["trade_type"], "매수")
        self.assertEqual(result[1]["trade_type"], "매도")

    def test_get_account_ledger_returns_mock_rows(self):
        result = make_tools().get_account_ledger("12345678901", "20250401", "20250430")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["transaction_type"], "배당금")
        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["final_amount"], 12240)

    def test_get_account_ledger_filters_dividends(self):
        result = make_tools().get_account_ledger(
            "12345678901",
            "20250401",
            "20250430",
            transaction_type="dividend",
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["transaction_type"], "배당금")
        self.assertEqual(result[0]["summary"], "현금배당")

    def test_get_open_orders_returns_mock_rows(self):
        result = make_tools().get_open_orders("12345678901")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["order_id"], "MOCK-OPEN-1")
        self.assertEqual(result[0]["status"], "partial")
        self.assertEqual(result[0]["raw_order_id"], "MOCK-OPEN-1")
        self.assertEqual(result[0]["order_method_code"], "0")
        self.assertEqual(result[0]["order_exchange_code"], "2")
        self.assertEqual(result[0]["sor_order_id"], "MOCK-SOR-1")
        self.assertEqual(result[0]["sor_original_order_id"], "MOCK-SOR-ORIGINAL-1")
        self.assertEqual(result[1]["status"], "pending")

    def test_get_open_orders_filters_by_code(self):
        result = make_tools().get_open_orders("12345678901", "005930")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["unfilled_quantity"], 70)

    def test_get_stock_technical_indicators_daily_returns_latest_first(self):
        result = make_tools().get_stock_technical_indicators_daily("005930")

        self.assertEqual(result[0]["date"], "2026-04-22")
        self.assertEqual(result[0]["close"], 71600)
        self.assertEqual(result[0]["volume"], 11100000)
        self.assertIn("macd", result[0])
        self.assertIn("rsi", result[0])
        self.assertIn("bollinger_upper", result[0])
        self.assertIn("ichimoku_conversion", result[0])
        self.assertIn("ichimoku_leading_span_a", result[0])
        self.assertIn("ichimoku_cloud_top", result[0])
        self.assertIn("ichimoku_cloud_bias", result[0])
        self.assertIn("atr", result[0])
        self.assertIn("adx", result[0])
        self.assertIn("plus_di", result[0])
        self.assertIn("minus_di", result[0])
        self.assertIn("trend_regime", result[0])
        self.assertIn("obv", result[0])
        self.assertIn("obv_sma", result[0])
        self.assertIn("mfi", result[0])
        self.assertIn("chandelier_exit_long", result[0])
        for key in ("sma5", "sma20", "sma60", "sma120", "ema5", "ema20", "ema60", "ema120"):
            self.assertIn(key, result[0])
        for key in ("volume_ma5", "volume_ma20", "volume_ma60", "volume_ratio5", "volume_ratio20", "volume_ratio60"):
            self.assertIn(key, result[0])

    def test_get_stock_weekly_prices_groups_daily_rows(self):
        result = make_tools().get_stock_weekly_prices("005930")

        self.assertEqual(result[0]["week"], "2026-W17")
        self.assertEqual(result[0]["start_date"], "2026-04-20")
        self.assertEqual(result[0]["end_date"], "2026-04-22")
        self.assertEqual(result[0]["open"], 70000)
        self.assertEqual(result[0]["high"], 72400)
        self.assertEqual(result[0]["low"], 69500)
        self.assertEqual(result[0]["close"], 71600)
        self.assertEqual(result[0]["volume"], 34900000)
        self.assertEqual(result[0]["trading_days"], 3)

    def test_get_stock_technical_indicators_daily_trims_to_requested_range_after_warmup_fetch(self):
        result = make_tools().get_stock_technical_indicators_daily("005930", "2026-04-21", "2026-04-22")

        self.assertEqual([row["date"] for row in result], ["2026-04-22", "2026-04-21"])

    def test_get_stock_technical_indicators_daily_enforces_end_date_before_calculation(self):
        class NonFilteringDailyClient(MockIndiClient):
            def get_daily_prices(self, code, start_date, end_date):
                self._require_code(code)
                return [
                    DailyPrice(date="2026-04-20", open=100, high=110, low=95, close=105, volume=1000),
                    DailyPrice(date="2026-04-21", open=105, high=115, low=100, close=112, volume=1200),
                    DailyPrice(date="2026-04-22", open=112, high=200, low=111, close=190, volume=9900),
                ]

        tools = HomestockTools(NonFilteringDailyClient(), OrderGuard(False), MockScripter(), runtime_state_dir=None)
        try:
            result = tools.get_stock_technical_indicators_daily("005930", end_date="2026-04-21")
        finally:
            tools.close()

        self.assertEqual([row["date"] for row in result], ["2026-04-21", "2026-04-20"])
        self.assertEqual(result[0]["close"], 112)

    def test_get_stock_technical_indicators_weekly_returns_indicator_rows(self):
        result = make_tools().get_stock_technical_indicators_weekly("005930")

        self.assertEqual(result[0]["week"], "2026-W17")
        self.assertEqual(result[0]["end_date"], "2026-04-22")
        self.assertEqual(result[0]["close"], 71600)
        self.assertEqual(result[0]["trading_days"], 3)
        self.assertIn("sma20", result[0])
        self.assertIn("volume_ratio20", result[0])

    def test_get_stock_technical_indicators_intraday_obeys_as_of_time(self):
        result = make_tools().get_stock_technical_indicators_intraday("005930", "2026-04-22", as_of_time="09:10:00")

        self.assertEqual([row["time"] for row in result], ["091000", "090500"])
        self.assertEqual(result[0]["timestamp"], "20260422091000")
        self.assertEqual(result[0]["vwap"], 71363.6364)
        self.assertEqual(result[0]["session_volume_ratio"], 1.0909)
        self.assertEqual(result[0]["as_of_time"], "091000")

    def test_get_stock_chart_patterns_daily_enforces_end_date(self):
        class NonFilteringPatternClient(MockIndiClient):
            def get_daily_prices(self, code, start_date, end_date):
                self._require_code(code)
                prices = [
                    DailyPrice(
                        date=f"2026-04-{index + 1:02d}",
                        open=100,
                        high=105,
                        low=95,
                        close=100,
                        volume=1000,
                    )
                    for index in range(21)
                ]
                prices.append(DailyPrice(date="2026-04-22", open=106, high=130, low=105, close=125, volume=4000))
                return prices

        tools = HomestockTools(NonFilteringPatternClient(), OrderGuard(False), MockScripter(), runtime_state_dir=None)
        try:
            result = tools.get_stock_chart_patterns_daily("005930", end_date="2026-04-21")
        finally:
            tools.close()

        self.assertNotIn("range_breakout", {row["name"] for row in result})

    def test_get_stock_market_environment_indicators_excludes_unfinished_daily_bar_and_quotes(self):
        class BacktestEnvironmentClient(MockIndiClient):
            def get_daily_prices(self, code, start_date, end_date):
                self._require_code(code)
                return [
                    DailyPrice(date="2026-04-21", open=100, high=110, low=95, close=100, volume=1000),
                    DailyPrice(date="2026-04-22", open=100, high=220, low=99, close=200, volume=9000),
                ]

            def get_quote_snapshot(self, code):
                raise AssertionError("market environment indicators must not use quote snapshots")

        tools = HomestockTools(BacktestEnvironmentClient(), OrderGuard(False), MockScripter(), runtime_state_dir=None)
        try:
            result = tools.get_stock_market_environment_indicators("005930", "2026-04-22", "10:00")
        finally:
            tools.close()

        self.assertEqual(result["completed_daily_end_date"], "20260421")
        self.assertEqual(result["high_52w"]["latest_close"], 100)
        self.assertEqual(result["high_52w"]["high"], 110)
        self.assertEqual(result["backtest_policy"]["current_quote_snapshot"], "not used")

    def test_get_stock_technical_analysis_bundle_is_backtest_safe_and_reuses_daily_source(self):
        class BundleBacktestClient(MockIndiClient):
            def __init__(self):
                super().__init__()
                self.daily_calls = 0

            def get_daily_prices(self, code, start_date, end_date):
                self.daily_calls += 1
                self._require_code(code)
                return [
                    DailyPrice(date="2026-04-21", open=100, high=110, low=95, close=100, volume=1000),
                    DailyPrice(date="2026-04-22", open=100, high=220, low=99, close=200, volume=9000),
                ]

            def get_quote_snapshot(self, code):
                raise AssertionError("backtest-safe bundle must not use quote snapshots")

        client = BundleBacktestClient()
        tools = HomestockTools(client, OrderGuard(False), MockScripter(), runtime_state_dir=None)
        try:
            result = tools.get_stock_technical_analysis_bundle(
                "005930",
                end_date="2026-04-22",
                as_of_time="09:10",
            )
        finally:
            tools.close()

        self.assertEqual(client.daily_calls, 1)
        self.assertEqual(result["mode"], "backtest_safe")
        self.assertTrue(result["backtest_policy"]["suitable_for_backtesting"])
        self.assertEqual(result["completed_daily_end_date"], "20260421")
        self.assertEqual([row["date"] for row in result["price_bars"]["daily"]], ["2026-04-21"])
        self.assertEqual([row["time"] for row in result["price_bars"]["intraday"]], ["090500", "091000"])
        self.assertEqual(result["technical_indicators"]["intraday"][0]["time"], "091000")
        self.assertNotIn("live_context", result)

    def test_get_stock_technical_analysis_bundle_live_includes_current_context(self):
        tools = make_tools()
        try:
            result = tools.get_stock_technical_analysis_bundle_live(
                "005930",
                date="2026-04-22",
                news_limit=1,
            )
        finally:
            tools.close()

        self.assertEqual(result["mode"], "live_not_backtest_safe")
        self.assertFalse(result["backtest_policy"]["suitable_for_backtesting"])
        self.assertEqual(result["completed_daily_end_date"], "20260422")
        self.assertEqual(len(result["price_bars"]["intraday"]), 3)
        self.assertEqual(result["live_context"]["quote_snapshot"]["current_price"], 71600)
        self.assertTrue(result["live_context"]["order_book"]["levels"])
        self.assertEqual(len(result["live_context"]["news_headlines"]), 1)
        self.assertTrue(result["live_context"]["investor_flow"])
        self.assertTrue(result["live_context"]["fundamentals"])
        self.assertIn("holding_alert_indicator_context", result["live_context"])

    def test_build_technical_indicators_adds_regime_volume_and_exit_fields(self):
        prices = [
            DailyPrice(
                date=f"202603{index + 1:02d}",
                open=100 + index,
                high=102 + index,
                low=99 + index,
                close=101 + index,
                volume=1000 + (index * 10),
            )
            for index in range(60)
        ]

        result = build_technical_indicators(prices)

        latest = result[0]
        self.assertIn(latest["trend_regime"], {"trending", "ranging", "transitioning"})
        self.assertIsInstance(latest["obv"], float)
        self.assertIsInstance(latest["obv_sma"], float)
        self.assertIsInstance(latest["atr"], float)
        self.assertIsInstance(latest["chandelier_exit_long"], float)
        self.assertIsInstance(latest["sma5"], float)
        self.assertIsInstance(latest["sma20"], float)
        self.assertIsInstance(latest["volume_ma20"], float)
        self.assertIsInstance(latest["volume_ratio20"], float)

    def test_detect_chart_patterns_reports_breakout_candidate(self):
        prices = [
            DailyPrice(
                date=f"202604{index + 1:02d}",
                open=100,
                high=105,
                low=95,
                close=100,
                volume=1000,
            )
            for index in range(21)
        ]
        prices.append(DailyPrice(date="20260422", open=106, high=112, low=105, close=111, volume=2500))

        result = detect_chart_patterns(prices)

        by_name = {item["name"]: item for item in result}
        names = set(by_name)
        self.assertIn("range_breakout", names)
        self.assertIn("prior_20bar_volume_ratio", by_name["range_breakout"]["levels"])
        self.assertNotIn("volume_ratio20", by_name["range_breakout"]["levels"])

    def test_stock_analysis_context_returns_bundled_read_only_sections(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                result = tools.get_stock_analysis_context("005930", "2026-04-22", include_intraday=False)
            finally:
                tools.close()

        self.assertEqual(result["code"], "005930")
        self.assertIn("daily_prices", result)
        self.assertIn("weekly_prices", result)
        self.assertIn("daily_technical_indicators", result)
        self.assertIn("chart_pattern_candidates", result)
        self.assertIn("decision_indicator_context", result)
        self.assertIn("market_index_prices", result)
        self.assertIn("sector_profile", result)
        self.assertIn("sector_index_prices", result)
        self.assertIn("news_headlines", result)
        self.assertEqual(result["data_status"]["intraday_prices"]["status"], "skipped")

    def test_stock_analysis_context_reuses_daily_source_for_derived_sections(self):
        class CountingDailyClient(MockIndiClient):
            def __init__(self):
                super().__init__()
                self.daily_calls = 0

            def get_daily_prices(self, code, start_date, end_date):
                self.daily_calls += 1
                return super().get_daily_prices(code, start_date, end_date)

        with TemporaryDirectory() as tempdir:
            client = CountingDailyClient()
            tools = HomestockTools(client, OrderGuard(False), MockScripter(), runtime_state_dir=tempdir)
            try:
                result = tools.get_stock_analysis_context("005930", "2026-04-22", include_intraday=False)
            finally:
                tools.close()

        self.assertEqual(client.daily_calls, 2)
        self.assertEqual(result["data_status"]["daily_prices"]["source_count"], 3)
        self.assertTrue(result["weekly_prices"])
        self.assertTrue(result["daily_technical_indicators"])

    def test_get_quote_snapshot_returns_mock_snapshot(self):
        result = make_tools().get_quote_snapshot("005930")

        self.assertEqual(result["code"], "005930")
        self.assertEqual(result["current_price"], 71600)
        self.assertEqual(result["previous_close"], 70800)
        self.assertEqual(result["per"], 16.8)

    def test_get_investor_flow_by_stock_returns_mock_points(self):
        result = make_tools().get_investor_flow_by_stock("005930")

        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["date"], "2026-04-20")
        self.assertEqual(result[0]["foreign_net"], 90000)
        self.assertEqual(result[1]["institution_cumulative_net"], 90000)

    def test_get_market_investor_flow_intraday_returns_mock_rows(self):
        result = make_tools().get_market_investor_flow_intraday()

        self.assertEqual(result[0]["time"], "090500")
        self.assertEqual(result[0]["retail"]["net"], -120000)
        self.assertEqual(result[0]["foreign"]["net"], 94000)
        self.assertEqual(result[0]["institution"]["net"], 26000)
        self.assertNotIn("institution_breakdown", result[0])
        self.assertNotIn("source", result[0])
        self.assertNotIn("market", result[0])
        self.assertNotIn("interval_minutes", result[0])
        self.assertNotIn("current_index", result[0])

    def test_get_market_investor_flow_intraday_can_include_institution_breakdown(self):
        result = make_tools().get_market_investor_flow_intraday(include_institution_breakdown=True)

        self.assertEqual(result[0]["institution_breakdown"]["securities"]["net"], 30000)
        self.assertEqual(result[0]["institution_breakdown"]["investment_trust"]["net"], 12000)

    def test_real_get_market_investor_flow_intraday_uses_tr_1202_b(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._request = Mock(return_value=SimpleNamespace(multi_row_count=2))
        rows = [
            {
                0: "090500",
                1: "65000",
                2: "185000",
                3: "-120000",
                4: "136000",
                5: "42000",
                6: "94000",
                7: "49000",
                8: "23000",
                9: "26000",
                10: "47000",
                11: "17000",
                12: "30000",
            },
            {
                0: "091000",
                1: "105000",
                2: "290000",
                3: "-185000",
                4: "198000",
                5: "70000",
                6: "128000",
                7: "92000",
                8: "35000",
                9: "57000",
                10: "67000",
                11: "22000",
                12: "45000",
            },
        ]

        def multi_text(row: int, col: int) -> str:
            return str(rows[row].get(col, ""))

        client._multi_text = Mock(side_effect=multi_text)
        client._multi_int = Mock(side_effect=lambda row, col: int(rows[row].get(col, 0) or 0))
        client._multi_float = Mock(side_effect=lambda row, col: float(rows[row].get(col, 0.0) or 0.0))

        result = client.get_market_investor_flow_intraday()

        client._request.assert_called_once_with("TR_1202_B", ["0001", "01", "1", "010"])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1].time, "091000")
        self.assertEqual(result[1].foreign["net"], 128000)
        self.assertIsNone(result[1].institution_breakdown)

        result_with_breakdown = client.get_market_investor_flow_intraday(include_institution_breakdown=True)
        self.assertEqual(result_with_breakdown[1].institution_breakdown["securities"]["net"], 45000)

    def test_get_foreign_flow_rankings_returns_mock_items(self):
        result = make_tools().get_foreign_flow_rankings()

        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["foreign_ownership_ratio"], 51.2)

    def test_get_top_movers_returns_mock_items(self):
        result = make_tools().get_top_movers()

        self.assertEqual(result[0]["rank"], 1)
        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["trade_strength"], 128.4)

    def test_get_top_movers_respects_limit(self):
        result = make_tools().get_top_movers(limit=0)

        self.assertEqual(result, [])

    def test_get_top_movers_can_filter_to_kospi200(self):
        result = make_tools().get_top_movers(kospi200_only=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["code"], "005930")

    def test_stock_news_round_trip_returns_mock_content(self):
        headlines = make_tools().list_stock_news("005930", "20260423")
        content = make_tools().get_news_content(
            headlines[0]["news_type"],
            headlines[0]["date"],
            headlines[0]["article_id"],
        )

        self.assertEqual(headlines[0]["article_id"], "356872")
        self.assertEqual(content["news_type"], "F")
        self.assertEqual(content["news_type_label"], "시황")
        self.assertIn("005930", content["extracted_codes"])
        self.assertEqual(headlines[0]["title"], "반도체 업황 개선 기대감 확대")
        self.assertEqual(headlines[0]["code"], "005930")
        self.assertEqual(headlines[0]["news_type_label"], "시황")

    def test_list_market_flow_news_filters_mock_time_range(self):
        headlines = make_tools().list_market_flow_news("20260423", "15:00", "15:30")

        self.assertEqual(len(headlines), 1)
        self.assertEqual(headlines[0]["news_type"], "F")
        self.assertEqual(headlines[0]["time"], "153000")
        self.assertEqual(headlines[0]["article_id"], "356872")
        self.assertEqual(headlines[0]["code"], "005930")

    def test_list_market_flow_news_rejects_inverted_time_range(self):
        with self.assertRaisesRegex(ValueError, "from_time must be on or before to_time"):
            make_tools().list_market_flow_news("20260423", "1530", "0900")

    def test_real_list_market_flow_news_uses_fixed_category_tr_and_filters_time(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._request = Mock(return_value=SimpleNamespace(multi_row_count=3))
        rows = [
            {0: "20260512", 1: "090000", 2: "장전 주요 뉴스", 3: "F", 4: "", 5: "356870"},
            {0: "20260512", 1: "1530", 2: "반도체 업황 개선", 3: "F", 4: "005930", 5: "356872"},
            {0: "20260512", 1: "154500", 2: "해외 기술주 강세", 3: "U", 4: "", 5: "356873"},
        ]

        def multi_text(row: int, col: int) -> str:
            return rows[row].get(col, "")

        client._multi_text = Mock(side_effect=multi_text)

        result = client.list_market_flow_news("2026-05-12", "15:00", "15:30")

        client._request.assert_called_once_with("TR_3102_CT", ["09", "20260512"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].article_id, "356872")
        self.assertEqual(result[0].time, "153000")
        self.assertEqual(result[0].news_type_label, "market_commentary")

    def test_get_disclosure_content_does_not_require_api_key(self):
        document = SimpleNamespace(
            content="<html><body>본문</body></html>\n",
            source="dart_viewer",
            viewer_url="https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260424000778",
            dtd="dart4.xsd",
            print_page_break_selector="section[data-ele-id]",
        )
        with patch.dict("os.environ", {}, clear=True):
            with patch("homestock.tools.disclosure_to_html", return_value=document):
                result = make_tools().get_disclosure_content("20260424000778")

        self.assertEqual(result["rcpNo"], "20260424000778")
        self.assertEqual(result["content"], "<html><body>본문</body></html>")
        self.assertEqual(result["content_format"], "html")
        self.assertEqual(result["source"], "dart_viewer")
        self.assertEqual(result["dtd"], "dart4.xsd")
        self.assertEqual(result["print_page_break_selector"], "section[data-ele-id]")
        self.assertNotIn("section_selector", result)
        self.assertNotIn("section_id_attr", result)
        self.assertNotIn("section_title_attr", result)
        self.assertNotIn("section_toc_attr", result)

    def test_get_disclosure_content_from_article_resolves_rcp_no_from_news_article(self):
        tools = make_tools()
        document = SimpleNamespace(
            content="<html><body>본문</body></html>\n",
            source="dart_viewer",
            viewer_url="https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260508000567",
            dtd="HTML",
            print_page_break_selector="",
        )
        with patch.object(tools._client, "get_news_content", return_value=SimpleNamespace(rcpNo="20260508000567")) as get_news_content:
            with patch("homestock.tools.disclosure_to_html", return_value=document):
                result = tools.get_disclosure_content_from_article(date="20260508", article_id="000567")

        get_news_content.assert_called_once_with("5", "20260508", "000567")
        self.assertEqual(result["rcpNo"], "20260508000567")
        self.assertEqual(result["content"], "<html><body>본문</body></html>")
        self.assertEqual(result["content_format"], "html")
        self.assertEqual(result["viewer_url"], "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260508000567")

    def test_get_disclosure_content_from_article_uses_news_type_when_resolving_article(self):
        tools = make_tools()
        document = SimpleNamespace(
            content="<html><body>본문</body></html>",
            source="dart_viewer",
            viewer_url="https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260508000567",
            dtd="HTML",
            print_page_break_selector="",
        )
        with patch.object(tools._client, "get_news_content", return_value=SimpleNamespace(rcpNo="20260508000567")) as get_news_content:
            with patch("homestock.tools.disclosure_to_html", return_value=document):
                tools.get_disclosure_content_from_article(date="2026-05-08", article_id="000567", news_type="5")

        get_news_content.assert_called_once_with("5", "20260508", "000567")

    def test_get_disclosure_content_from_article_prefers_disclosure_body_raw_html(self):
        tools = make_tools()
        raw_html = "<html><body><div class=\"xforms\"><table><tr><td>공시 본문</td></tr></table></div></body></html>"
        with patch.object(
            tools._client,
            "get_news_content",
            return_value=SimpleNamespace(rcpNo="20260511800596", raw_html=raw_html),
        ) as get_news_content:
            with patch("homestock.tools.disclosure_to_html") as disclosure_to_html:
                result = tools.get_disclosure_content_from_article(date="20260511", article_id="800596", news_type="S")

        get_news_content.assert_called_once_with("S", "20260511", "800596")
        disclosure_to_html.assert_not_called()
        self.assertIsNone(result["rcpNo"])
        self.assertEqual(result["content"], raw_html)
        self.assertEqual(result["content_format"], "html")
        self.assertEqual(result["source"], "news_raw_html")
        self.assertEqual(result["viewer_url"], "")
        self.assertEqual(result["dtd"], None)
        self.assertEqual(result["print_page_break_selector"], "")
        self.assertNotIn("section_selector", result)
        self.assertNotIn("section_id_attr", result)
        self.assertNotIn("section_title_attr", result)
        self.assertNotIn("section_toc_attr", result)

    def test_get_disclosure_content_from_article_returns_print_page_break_selector_for_raw_html(self):
        tools = make_tools()
        raw_html = "<html><body><div class=\"xforms\">공시 본문</div><P class='pgbrk'></P></body></html>"
        with patch.object(
            tools._client,
            "get_news_content",
            return_value=SimpleNamespace(rcpNo="20260511800596", raw_html=raw_html),
        ):
            result = tools.get_disclosure_content_from_article(date="20260511", article_id="800596", news_type="S")

        self.assertEqual(result["source"], "news_raw_html")
        self.assertEqual(result["print_page_break_selector"], "p.pgbrk, p.PGBRK")

    def test_get_disclosure_content_from_article_requires_rcp_no_in_news_article(self):
        tools = make_tools()
        with patch.object(tools._client, "get_news_content", return_value=SimpleNamespace(rcpNo=None, raw_html="일반 뉴스")):
            with self.assertRaisesRegex(ValueError, "rcpNo could not be found"):
                tools.get_disclosure_content_from_article(date="20260508", article_id="000567")

    def test_get_volume_surge_returns_mock_items(self):
        result = make_tools().get_volume_surge()

        self.assertEqual(result[0]["code"], "000660")
        self.assertEqual(result[0]["metric_label"], "volume_surge_rate")

    def test_get_volume_surge_respects_limit(self):
        result = make_tools().get_volume_surge(limit=0)

        self.assertEqual(result, [])

    def test_get_volume_surge_can_filter_to_kospi200(self):
        result = make_tools().get_volume_surge(kospi200_only=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["code"], "000660")

    def test_get_new_highs_lows_returns_mock_items(self):
        result = make_tools().get_new_highs_lows(mode="new_high")

        self.assertEqual(result[0]["code"], "005930")
        self.assertEqual(result[0]["metric_label"], "new_high_price")

    def test_get_new_highs_lows_respects_limit(self):
        result = make_tools().get_new_highs_lows(limit=0)

        self.assertEqual(result, [])

    def test_get_new_highs_lows_can_filter_to_kospi200(self):
        result = make_tools().get_new_highs_lows(kospi200_only=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["code"], "005930")

    def test_get_limit_hits_returns_mock_items(self):
        result = make_tools().get_limit_hits(mode="upper")

        self.assertEqual(result[0]["code"], "381170")
        self.assertEqual(result[0]["metric_label"], "consecutive_days")

    def test_get_limit_hits_can_filter_to_kospi200(self):
        result = make_tools().get_limit_hits(kospi200_only=True)

        self.assertEqual(result, [])

    def test_get_order_book_returns_mock_levels(self):
        result = make_tools().get_order_book("005930")

        self.assertEqual(result["code"], "005930")
        self.assertEqual(result["levels"][0]["ask_price"], 71600)
        self.assertEqual(len(result["levels"]), 5)
        self.assertEqual(result["levels"][4]["bid_price"], 71100)

    def test_holding_alert_public_tools_return_mock_results(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                intraday = tools.get_intraday_prices("005930", "20260503")
                cash_book = tools.get_cash_order_book_snapshot("005930")
                baseline = tools.refresh_decision_baselines(code="005930", date="20260503")
                indicator_context = tools.get_alert_indicator_context("005930", "20260503")
                scan = tools.run_holding_alert_scan("12345678901", dry_run=True)
            finally:
                tools.close()

        self.assertEqual(intraday[0]["time"], "090500")
        self.assertEqual(cash_book["source"], "SH")
        self.assertEqual(cash_book["status"], "available")
        self.assertEqual(baseline["refreshed"][0]["code"], "005930")
        self.assertEqual(baseline["refreshed"][0]["status"], "available")
        self.assertIn("vwap", indicator_context)
        self.assertEqual(scan["result_count"], 1)
        self.assertEqual(scan["results"][0]["trade_size"]["recommended_qty"], 0)
        self.assertIn("매매희망가", scan["results"][0]["text"]["summary"])
        self.assertIn("추천 0주", scan["results"][0]["text"]["summary"])
        self.assertTrue(scan["results"][0]["text"]["detail_markdown"].endswith("자동 주문 아님. 수동 판단 필요."))

    def test_holding_alert_runner_register_list_cancel(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                registered = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    heldCode=["005930"],
                    dry_run=True,
                )
                listed = tools.list_holding_alert_runners()
                canceled = tools.cancel_holding_alert_runner(registered["runner_id"])
            finally:
                tools.close()

        self.assertTrue(registered["active"])
        self.assertEqual(registered["accountNo"], "12345678901")
        self.assertEqual(registered["heldCode"], ["005930"])
        self.assertEqual(registered["wannaCode"], [])
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["runner_id"], registered["runner_id"])
        self.assertEqual(listed[0]["heldCode"], ["005930"])
        self.assertEqual(listed[0]["wannaCode"], [])
        self.assertTrue(canceled["canceled"])

    def test_holding_alert_runner_expires_after_registration_day(self):
        day1 = datetime(2026, 5, 11, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))
        day2 = datetime(2026, 5, 12, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))
        with TemporaryDirectory() as tempdir:
            tools = None
            with patch("homestock.holding_alerts._kst_now", return_value=day1):
                tools = make_tools(runtime_state_dir=tempdir)
                registered = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    heldCode=["005930"],
                    dry_run=True,
                )
                manager = getattr(tools, "_holding_alerts")
                self.assertEqual(len(tools.list_holding_alert_runners()), 1)
            try:
                with patch("homestock.holding_alerts._kst_now", return_value=day2):
                    listed = tools.list_holding_alert_runners()
                    runner_threads = dict(manager._runner_threads)
                    owned_price_codes = dict(manager._owned_price_codes)
                    listener_registered = manager._rt_listener_registered
                    listeners = list(tools._client._rt_listeners)
            finally:
                if tools is not None:
                    tools.close()

        self.assertTrue(registered["runner_id"])
        self.assertEqual(listed, [])
        self.assertEqual(runner_threads, {})
        self.assertEqual(owned_price_codes, {})
        self.assertFalse(listener_registered)
        self.assertEqual(len(listeners), 1)

    def test_holding_alert_runner_restore_skips_previous_day_runner(self):
        day2 = datetime(2026, 5, 12, 9, 0, 0, tzinfo=timezone(timedelta(hours=9)))
        with TemporaryDirectory() as tempdir:
            state_path = Path(tempdir) / "holding_alert_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "updated_at": "20260511200000",
                        "runners": [
                            {
                                "runner_id": "holding_runner_previous_day",
                                "account_no": "12345678901",
                                "heldCode": ["005930"],
                                "wannaCode": [],
                                "httpCallback": {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                                "dry_run": True,
                                "registered_at": "20260511090000",
                                "last_scan_at": None,
                                "last_scan_result_count": 0,
                                "active": True,
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            with patch("homestock.holding_alerts._kst_now", return_value=day2):
                tools = make_tools(runtime_state_dir=tempdir)
                try:
                    manager = getattr(tools, "_holding_alerts")
                    listed = tools.list_holding_alert_runners()
                    stored = json.loads(state_path.read_text(encoding="utf-8"))
                    runner_threads = dict(manager._runner_threads)
                    listener_registered = manager._rt_listener_registered
                finally:
                    tools.close()

        self.assertEqual(listed, [])
        self.assertEqual(stored["runners"], [])
        self.assertEqual(runner_threads, {})
        self.assertFalse(listener_registered)

    def test_holding_alert_runner_selected_codes_filter_scan_and_subscriptions(self):
        class MultiBalanceClient(MockIndiClient):
            def get_balance(self, account_no):
                self._require_account(account_no)
                return [
                    BalanceItem("12345678901", "005930", "Samsung Electronics", 10, 70000, 71600),
                    BalanceItem("12345678901", "000660", "SK hynix", 3, 120000, 121000),
                ]

        with TemporaryDirectory() as tempdir:
            client = MultiBalanceClient()
            scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter.selected_runner"))
            tools = HomestockTools(client, OrderGuard(False), scripter, runtime_state_dir=tempdir)
            try:
                registered = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    heldCode=["000660"],
                    dry_run=True,
                )
                manager = getattr(tools, "_holding_alerts")
                scan = manager.run_scan("12345678901", dry_run=True, runner_id=registered["runner_id"])
                owned_codes = set(manager._owned_price_codes)
                tools.cancel_holding_alert_runner(registered["runner_id"])
            finally:
                tools.close()

        self.assertEqual(registered["heldCode"], ["000660"])
        self.assertEqual(scan["result_count"], 1)
        self.assertEqual(scan["results"][0]["code"], "000660")
        self.assertEqual(owned_codes, {"000660"})

    def test_holding_alert_runner_wanna_code_tracks_unheld_buy_only(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                registered = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    wannaCode=["000660"],
                    dry_run=True,
                )
                manager = getattr(tools, "_holding_alerts")
                now = datetime.now(timezone(timedelta(hours=9)))
                with manager._state_lock:
                    manager._state["baseline_cache"]["000660"] = {
                        "code": "000660",
                        "name": "SK hynix",
                        "status": "available",
                        "damage_line": 100000,
                        "recovery_line": 101000,
                        "second_support": {"low": 90000, "high": 91000},
                        "first_support": {"low": 70000, "high": 72000},
                        "buy_reclaim_lines": {
                            "first_rebound": 70000,
                            "second_rebound": 90000,
                            "trend_reclaim": 70000,
                        },
                        "atr14": 1000,
                        "daily_indicators": {},
                    }
                    manager._state["symbol_state"]["000660"] = {
                        "first_support_touched_at": (now - timedelta(minutes=20)).strftime("%Y%m%d%H%M%S"),
                        "first_reclaim_since": (now - timedelta(minutes=11)).strftime("%Y%m%d%H%M%S"),
                    }
                scan = manager.run_scan("12345678901", dry_run=True, runner_id=registered["runner_id"])
                owned_codes = set(manager._owned_price_codes)
                tools.cancel_holding_alert_runner(registered["runner_id"])
            finally:
                tools.close()

        self.assertEqual(registered["heldCode"], [])
        self.assertEqual(registered["wannaCode"], ["000660"])
        self.assertEqual(owned_codes, {"005930", "000660"})
        self.assertEqual([item["code"] for item in scan["results"]], ["005930", "000660"])
        wanna = scan["results"][1]
        self.assertEqual(wanna["watch_mode"], "wanna")
        self.assertEqual(wanna["position"]["status"], "unheld")
        self.assertEqual(wanna["position"]["quantity"], 0)
        self.assertEqual(wanna["alert_type"], "매수 판단")

    def test_holding_alert_runner_rejects_overlapping_held_and_wanna_codes(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                with self.assertRaisesRegex(ValueError, "heldCode and wannaCode"):
                    tools.register_holding_alert_runner(
                        "12345678901",
                        {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                        heldCode=["005930"],
                        wannaCode=["005930"],
                        dry_run=True,
                    )
            finally:
                tools.close()

    def test_holding_alert_runner_rejects_current_holding_as_wanna_when_held_all(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                with self.assertRaisesRegex(ValueError, "currently held"):
                    tools.register_holding_alert_runner(
                        "12345678901",
                        {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                        wannaCode=["005930"],
                        dry_run=True,
                    )
            finally:
                tools.close()

    def test_holding_alert_runner_pauses_wanna_code_after_it_becomes_held(self):
        class MutableBalanceClient(MockIndiClient):
            def __init__(self):
                super().__init__()
                self.balances = [
                    BalanceItem("12345678901", "005930", "Samsung Electronics", 10, 70000, 71600)
                ]

            def get_balance(self, account_no):
                self._require_account(account_no)
                return list(self.balances)

        with TemporaryDirectory() as tempdir:
            client = MutableBalanceClient()
            scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter.wanna_pause"))
            tools = HomestockTools(client, OrderGuard(False), scripter, runtime_state_dir=tempdir)
            try:
                registered = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    wannaCode=["000660"],
                    dry_run=True,
                )
                manager = getattr(tools, "_holding_alerts")
                first_scan = manager.run_scan("12345678901", dry_run=True, runner_id=registered["runner_id"])
                client.balances = [
                    BalanceItem("12345678901", "005930", "Samsung Electronics", 10, 70000, 71600),
                    BalanceItem("12345678901", "000660", "SK hynix", 2, 120000, 121000),
                ]
                manager._tr_cache.pop("balance:12345678901", None)
                second_scan = manager.run_scan("12345678901", dry_run=True, runner_id=registered["runner_id"])
                owned_codes = set(manager._owned_price_codes)
                tools.cancel_holding_alert_runner(registered["runner_id"])
            finally:
                tools.close()

        self.assertEqual([item["code"] for item in first_scan["results"]], ["005930", "000660"])
        self.assertEqual([item["code"] for item in second_scan["results"]], ["005930"])
        self.assertIn("000660", owned_codes)

    def test_holding_alert_cancel_preserves_existing_shared_price_subscription(self):
        class RefCountClient(MockIndiClient):
            def __init__(self):
                super().__init__()
                self.sc_counts: dict[str, int] = {}

            def subscribe_realtime_price(self, code):
                self._require_code(code)
                self.sc_counts[code] = self.sc_counts.get(code, 0) + 1
                self._subscriptions.add(code)
                return {"code": code, "subscribed": True, "remaining_subscriptions": self.sc_counts[code]}

            def unsubscribe_realtime_price(self, code):
                self._require_code(code)
                self.sc_counts[code] = max(self.sc_counts.get(code, 0) - 1, 0)
                if self.sc_counts[code] == 0:
                    self._subscriptions.discard(code)
                return {"code": code, "subscribed": self.sc_counts[code] > 0, "remaining_subscriptions": self.sc_counts[code]}

        with TemporaryDirectory() as tempdir:
            client = RefCountClient()
            scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter.shared_sc"))
            tools = HomestockTools(client, OrderGuard(False), scripter, runtime_state_dir=tempdir)
            try:
                client.subscribe_realtime_price("005930")
                first = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    dry_run=True,
                )
                with self.assertRaisesRegex(ValueError, "already registered"):
                    tools.register_holding_alert_runner(
                        "12345678901",
                        {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                        heldCode=["005930"],
                        dry_run=True,
                    )
                self.assertEqual(client.sc_counts["005930"], 2)

                tools.cancel_holding_alert_runner(first["runner_id"])
                after_cancel = client.sc_counts["005930"]
            finally:
                tools.close()

        self.assertEqual(after_cancel, 1)

    def test_holding_alert_cancel_unregisters_idle_rt_listener_and_clears_queue(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            client = tools._client
            assert isinstance(client, MockIndiClient)
            try:
                registered = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    dry_run=True,
                )
                manager = getattr(tools, "_holding_alerts")
                self.assertEqual(len(client._rt_listeners), 2)
                client.emit_rt_event({"rt_type": "N0", "code": "005930", "title": "news"})
                self.assertEqual(len(manager._raw_event_queue), 1)

                tools.cancel_holding_alert_runner(registered["runner_id"])
                remaining_listeners = list(client._rt_listeners)
                listener_registered = manager._rt_listener_registered
                queued = list(manager._raw_event_queue)
            finally:
                tools.close()

        self.assertEqual(len(remaining_listeners), 1)
        self.assertFalse(listener_registered)
        self.assertEqual(queued, [])

    def test_holding_alert_scan_reuses_tr_cache_inside_tick_loop(self):
        class CountingClient(MockIndiClient):
            def __init__(self):
                super().__init__()
                self.calls = {
                    "balance": 0,
                    "summary": 0,
                    "orders": 0,
                    "intraday": 0,
                    "daily": 0,
                    "quote": 0,
                    "order_book": 0,
                    "market": 0,
                    "sector": 0,
                }

            def get_balance(self, account_no):
                self.calls["balance"] += 1
                return super().get_balance(account_no)

            def get_account_summary(self, account_no):
                self.calls["summary"] += 1
                return super().get_account_summary(account_no)

            def get_open_orders(self, account_no, code=None):
                self.calls["orders"] += 1
                return super().get_open_orders(account_no, code)

            def get_intraday_prices(self, code, date, interval_minutes=5):
                self.calls["intraday"] += 1
                return super().get_intraday_prices(code, date, interval_minutes)

            def get_daily_prices(self, code, start_date, end_date):
                self.calls["daily"] += 1
                return super().get_daily_prices(code, start_date, end_date)

            def get_quote_snapshot(self, code):
                self.calls["quote"] += 1
                return super().get_quote_snapshot(code)

            def get_cash_order_book_snapshot(self, code):
                self.calls["order_book"] += 1
                return super().get_cash_order_book_snapshot(code)

            def get_market_index_prices(self, start_date, end_date):
                self.calls["market"] += 1
                return super().get_market_index_prices(start_date, end_date)

            def get_sector_index_prices(self, sector_code, start_date, end_date, interval="D"):
                self.calls["sector"] += 1
                return super().get_sector_index_prices(sector_code, start_date, end_date, interval)

        with TemporaryDirectory() as tempdir:
            client = CountingClient()
            scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter.cache"))
            tools = HomestockTools(client, OrderGuard(False), scripter, runtime_state_dir=tempdir)
            try:
                tools.run_holding_alert_scan("12345678901", dry_run=True)
                first_counts = dict(client.calls)
                tools.run_holding_alert_scan("12345678901", dry_run=True)
            finally:
                tools.close()

        self.assertEqual(client.calls["balance"], first_counts["balance"])
        self.assertEqual(client.calls["summary"], first_counts["summary"])
        self.assertEqual(client.calls["orders"], first_counts["orders"])
        self.assertEqual(client.calls["intraday"], first_counts["intraday"])
        self.assertEqual(client.calls["quote"], first_counts["quote"])
        self.assertEqual(client.calls["order_book"], first_counts["order_book"])

    def test_holding_alert_balance_diff_handles_new_removed_positions_and_preserves_high(self):
        class MutableBalanceClient(MockIndiClient):
            def __init__(self):
                super().__init__()
                self.balances = [
                    BalanceItem("12345678901", "005930", "Samsung Electronics", 10, 70000, 71600)
                ]

            def get_balance(self, account_no):
                self._require_account(account_no)
                return list(self.balances)

        with TemporaryDirectory() as tempdir:
            client = MutableBalanceClient()
            scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter.balance_diff"))
            tools = HomestockTools(client, OrderGuard(False), scripter, runtime_state_dir=tempdir)
            try:
                registered = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    dry_run=True,
                )
                tools.run_holding_alert_scan("12345678901", dry_run=True)
                manager = getattr(tools, "_holding_alerts")
                first_high = manager._state["symbol_state"]["005930"]["high_since_entry"]
                client.balances = [
                    BalanceItem("12345678901", "005930", "Samsung Electronics", 20, 70500, 70000),
                    BalanceItem("12345678901", "000660", "SK hynix", 3, 120000, 121000),
                ]
                manager._tr_cache.pop("balance:12345678901", None)
                tools.run_holding_alert_scan("12345678901", dry_run=True)
                added_state = dict(manager._state["symbol_state"]["000660"])
                changed_high = manager._state["symbol_state"]["005930"]["high_since_entry"]
                client.balances = [
                    BalanceItem("12345678901", "000660", "SK hynix", 3, 120000, 121000),
                ]
                manager._tr_cache.pop("balance:12345678901", None)
                tools.run_holding_alert_scan("12345678901", dry_run=True)
                removed_state = dict(manager._state["symbol_state"]["005930"])
                owned_codes = set(manager._owned_price_codes)
                subscriptions = set(client._subscriptions)
                tools.cancel_holding_alert_runner(registered["runner_id"])
            finally:
                tools.close()

        self.assertTrue(added_state["active_position"])
        self.assertIn("000660", subscriptions)
        self.assertGreaterEqual(changed_high, first_high)
        self.assertFalse(removed_state["active_position"])
        self.assertNotIn("005930", owned_codes)
        self.assertNotIn("damage_breach_since", removed_state)

    def test_holding_alert_price_input_prefers_uc_integrated_price_cache(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            try:
                with manager._state_lock:
                    manager._rt_listener_registered = True
                manager._on_rt_event({"rt_type": "SC", "code": "005930", "current_price": 70000, "time": "090000"})
                manager._on_rt_event({"rt_type": "UC", "code": "005930", "current_price": 70100, "time": "090001"})
                price = manager._price_input("005930")
            finally:
                tools.close()

        self.assertEqual(price.current_price, 70100.0)
        self.assertEqual(price.source, "UC")

    def test_holding_alert_price_input_prefers_fresh_sc_over_stale_uc(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            try:
                with manager._state_lock:
                    manager._rt_cache["UC"]["005930"] = {
                        "event": {"rt_type": "UC", "code": "005930", "current_price": 70100},
                        "received_at": "20260511090000",
                        "received_at_monotonic": time.monotonic() - manager._price_stale_seconds() - 1,
                    }
                    manager._rt_cache["SC"]["005930"] = {
                        "event": {"rt_type": "SC", "code": "005930", "current_price": 70200},
                        "received_at": "20260511090001",
                        "received_at_monotonic": time.monotonic(),
                    }
                price = manager._price_input("005930")
            finally:
                tools.close()

        self.assertEqual(price.current_price, 70200.0)
        self.assertEqual(price.source, "SC")

    def test_holding_alert_order_book_rt_cache_obeys_refresh_ttl(self):
        class CountingOrderBookClient(MockIndiClient):
            def __init__(self):
                super().__init__()
                self.cash_book_calls = 0

            def get_cash_order_book_snapshot(self, code):
                self.cash_book_calls += 1
                return super().get_cash_order_book_snapshot(code)

        with TemporaryDirectory() as tempdir:
            client = CountingOrderBookClient()
            scripter = InProcessScripter(logger=logging.getLogger("test.homestock.scripter.order_book_ttl"))
            tools = HomestockTools(client, OrderGuard(False), scripter, runtime_state_dir=tempdir)
            try:
                manager = getattr(tools, "_holding_alerts")
                with manager._state_lock:
                    manager._rt_cache["SH"]["005930"] = {
                        "event": {
                            "source": "SH",
                            "received_at": "20260503090000",
                            "levels": [{"ask_price": 1, "bid_price": 1}],
                            "market_phase": "continuous",
                        },
                        "received_at": "20260503090000",
                        "received_at_monotonic": time.monotonic() - 60,
                    }
                snapshot = tools.get_cash_order_book_snapshot("005930")
            finally:
                tools.close()

        self.assertEqual(client.cash_book_calls, 1)
        self.assertNotEqual(snapshot["levels"][0]["ask_price"], 1)

    def test_calculate_trade_size_tolerates_null_containers_and_ignores_stale_order_book(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                result = tools.calculate_trade_size(
                    {
                        "alert_type": "매수 판단",
                        "scenario": "1차 반등형",
                        "current_price": 70000,
                        "position": None,
                        "account": {"orderable_amount": 1000000, "total_asset_value": 10000000},
                        "events": None,
                        "baselines": None,
                        "data_status": {"balance": None, "account_summary": None, "open_orders": None},
                        "indicators": {
                            "trading_value": None,
                            "market": None,
                            "relative_strength": None,
                        },
                        "order_book": {
                            "status": "stale",
                            "levels": [{"ask_price": 1, "bid_price": 1}],
                        },
                    }
                )
            finally:
                tools.close()

        self.assertEqual(result["price_guide"]["reference_source"], "rt_price")
        self.assertEqual(result["price_guide"]["reference_price"], 70000)
        self.assertEqual(result["price_guide"]["status"], "stale")
        self.assertIn("호가 stale", result["warning"])

    def test_calculate_trade_size_caps_non_final_sell_alerts_at_half_position(self):
        tools = make_tools()
        payload = {
            "alert_type": "매도 판단",
            "scenario": "none",
            "current_price": 900,
            "position": {"quantity": 100},
            "account": {"total_asset_value": 1000000, "orderable_amount": 500000},
            "baselines": {
                "daily_indicators": {
                    "obv": 100,
                    "obv_sma": 200,
                    "minus_di": 30,
                    "plus_di": 10,
                    "macd_histogram": -1,
                }
            },
            "indicators": {
                "vwap": {"status": "available", "value": 1000},
                "market": {"source": "daily", "change_pct": -1.5},
                "sector": {"source": "daily", "change_pct": -1.2},
                "relative_strength": {"status": "weak"},
                "trading_value": {"surge_ratio": 2.5},
                "volume_5m": {"status": "available", "ratio": 2.1},
                "high_52w": {"status": "available", "distance_pct": -2},
            },
            "events": {"risk_event_flag": True},
            "data_status": {},
            "order_book": {"levels": [{"bid_price": 900}], "status": "available"},
        }
        try:
            sell = tools.calculate_trade_size(payload)
            final_defense = tools.calculate_trade_size({**payload, "alert_type": "최종 방어선 검토"})
        finally:
            tools.close()

        self.assertEqual(sell["calculated_qty"], 50)
        self.assertEqual(final_defense["calculated_qty"], 100)

    def test_calculate_trade_size_caps_buy_multiplier_at_base_budget(self):
        tools = make_tools()
        try:
            result = tools.calculate_trade_size(
                {
                    "alert_type": "매수 판단",
                    "scenario": "1차 반등형",
                    "current_price": 1000,
                    "position": {"quantity": 0},
                    "account": {"orderable_amount": 100000, "total_asset_value": 1000000},
                    "indicators": {
                        "vwap": {"status": "available", "value": 900},
                        "market": {"source": "daily", "change_pct": 1.5},
                        "sector": {"source": "daily", "change_pct": 1.5},
                        "relative_strength": {"status": "strong"},
                        "trading_value": {"surge_ratio": 1.5},
                        "volume_5m": {"status": "available", "ratio": 2.1},
                        "high_52w": {"status": "available", "distance_pct": -10},
                    },
                    "events": {},
                    "data_status": {},
                    "order_book": {"levels": [{"ask_price": 1000}], "status": "available"},
                }
            )
        finally:
            tools.close()

        self.assertEqual(result["indicator_multiplier"], 1.0)
        self.assertEqual(result["calculated_qty"], 19)

    def test_holding_alert_prior_close_gap_uses_damage_line(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            try:
                candidates = manager._alert_candidates(
                    BalanceItem("12345678901", "005930", "Samsung Electronics", 10, 70000, 96500),
                    {
                        "damage_line": 100000,
                        "recovery_line": 101000,
                        "second_support": {"low": 0, "high": 0},
                        "atr14": 0,
                    },
                    price=SimpleNamespace(current_price=96500, status="available", age_seconds=0),
                    indicators={"vwap": {"value": 0}, "relative_strength": {"status": "neutral"}},
                    symbol_state={
                        "prior_close_damage": {
                            "date": "20260502",
                            "close": 99000,
                            "damage_line": 100000,
                        }
                    },
                    high_since_entry=0,
                    now=datetime(2026, 5, 3, 9, 5, tzinfo=timezone(timedelta(hours=9))),
                )
            finally:
                tools.close()

        self.assertTrue(any(item["alert_type"] == "전일 종가 훼손 후속 판단" for item in candidates))

    def test_holding_alert_dry_run_history_feeds_whipsaw_override(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            try:
                manager._record_dry_run_alert_history(
                    [
                        {
                            "code": "005930",
                            "alert_type": "매도 판단",
                            "scenario": "none",
                            "priority": 5,
                            "triggered_at": "20260501090000",
                        },
                        {
                            "code": "005930",
                            "alert_type": "매수 판단",
                            "scenario": "1차 반등형",
                            "priority": 8,
                            "triggered_at": "20260501100000",
                        },
                        {
                            "code": "005930",
                            "alert_type": "매도 판단",
                            "scenario": "none",
                            "priority": 5,
                            "triggered_at": "20260502090000",
                        },
                    ]
                )
                manager._refresh_whipsaw_overrides(datetime(2026, 5, 3, 9, 0, tzinfo=timezone(timedelta(hours=9))))
                overrides = dict(manager._state["whipsaw_overrides"])
            finally:
                tools.close()

        self.assertIn("005930", overrides)
        self.assertEqual(overrides["005930"]["hold_minutes_add"], 5)

    def test_holding_alert_nonurgent_alerts_are_bundled_for_one_minute(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            manager._dispatcher = Mock()
            manager._dispatcher.dispatch.return_value = {"queued": True, "error": None}
            try:
                with manager._state_lock:
                    manager._state["runners"].append(
                        {
                            "runner_id": "runner-bundle",
                            "account_no": "12345678901",
                            "httpCallback": {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                            "dry_run": False,
                            "active": True,
                            "registered_at": "20260503090000",
                        }
                    )
                payloads = [
                    {
                        "category": "trade_decision",
                        "alert_type": "매도 주의",
                        "priority": 6,
                        "scenario": "none",
                        "code": "005930",
                        "name": "Samsung Electronics",
                        "triggered_at": "20260503090000",
                        "current_price": 70000,
                        "config": {"observe_only": False},
                        "reasons": ["damage_line 하회"],
                        "text": {"summary": "매도 주의", "detail_markdown": "자동 주문 아님. 수동 판단 필요."},
                    },
                    {
                        "category": "trade_decision",
                        "alert_type": "매수 판단",
                        "priority": 8,
                        "scenario": "1차 반등형",
                        "code": "005930",
                        "name": "Samsung Electronics",
                        "triggered_at": "20260503090005",
                        "current_price": 70500,
                        "config": {"observe_only": False},
                        "reasons": ["reclaim 유지"],
                        "text": {"summary": "매수 판단", "detail_markdown": "자동 주문 아님. 수동 판단 필요."},
                    },
                ]
                first_dispatches = manager._dispatch_scan_results("12345678901", payloads, None)
                with manager._state_lock:
                    bundle_key = next(iter(manager._state["pending_alert_bundles"]))
                    manager._state["pending_alert_bundles"][bundle_key]["due_at"] = "20000101000000"
                due_dispatches = manager._dispatch_scan_results("12345678901", [], None)
                callback = manager._dispatcher.dispatch.call_args.args[0]
            finally:
                tools.close()

        self.assertTrue(all(item["reason"] == "bundle_pending" for item in first_dispatches))
        self.assertEqual(due_dispatches[0]["alert_type"], "묶음 알림")
        self.assertEqual(callback.body_format, "text")
        self.assertIn("묶음 알림", callback.body)
        self.assertEqual(len(manager._state["alert_history"]), 2)

    def test_holding_alert_urgent_alert_bypasses_bundler(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            manager._dispatcher = Mock()
            manager._dispatcher.dispatch.return_value = {"queued": True, "error": None}
            try:
                with manager._state_lock:
                    manager._state["runners"].append(
                        {
                            "runner_id": "runner-urgent",
                            "account_no": "12345678901",
                            "httpCallback": {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                            "dry_run": False,
                            "active": True,
                            "registered_at": "20260503090000",
                        }
                    )
                dispatches = manager._dispatch_scan_results(
                    "12345678901",
                    [
                        {
                            "category": "trade_decision",
                            "alert_type": "최종 방어선 검토",
                            "priority": 1,
                            "scenario": "none",
                            "code": "005930",
                            "name": "Samsung Electronics",
                            "triggered_at": "20260503090000",
                            "current_price": 65000,
                            "config": {"observe_only": False},
                            "reasons": ["2차 지지선 이탈"],
                            "text": {"summary": "최종 방어선 검토", "detail_markdown": "자동 주문 아님. 수동 판단 필요."},
                        }
                    ],
                    None,
                )
            finally:
                tools.close()

        self.assertTrue(dispatches[0]["queued"])
        self.assertEqual(manager._state["pending_alert_bundles"], {})

    def test_holding_alert_daily_limit_routes_general_alerts_to_summary(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            manager._dispatcher = Mock()
            manager._dispatcher.dispatch.return_value = {"queued": True, "error": None}
            try:
                with manager._state_lock:
                    manager._state["runners"].append(
                        {
                            "runner_id": "runner-summary",
                            "account_no": "12345678901",
                            "httpCallback": {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                            "dry_run": False,
                            "active": True,
                            "registered_at": "20260503090000",
                        }
                    )
                    manager._state["alert_history"] = [
                        {"code": f"{index:06d}", "alert_type": "매도 주의", "sent_at": f"2026050309{index:02d}00"}
                        for index in range(20)
                    ]
                pending = manager._dispatch_scan_results(
                    "12345678901",
                    [
                        {
                            "category": "trade_decision",
                            "alert_type": "매도 주의",
                            "priority": 6,
                            "scenario": "none",
                            "code": "005930",
                            "name": "Samsung Electronics",
                            "triggered_at": "20260503100500",
                            "current_price": 70000,
                            "config": {"observe_only": False},
                            "reasons": ["일반 알림"],
                            "text": {"summary": "매도 주의", "detail_markdown": "자동 주문 아님. 수동 판단 필요."},
                        }
                    ],
                    None,
                )
                with manager._state_lock:
                    summary_key = next(iter(manager._state["pending_alert_summaries"]))
                    manager._state["pending_alert_summaries"][summary_key]["due_at"] = "20000101000000"
                due = manager._dispatch_scan_results("12345678901", [], None)
                callback = manager._dispatcher.dispatch.call_args.args[0]
            finally:
                tools.close()

        self.assertEqual(pending[0]["reason"], "daily_summary_pending")
        self.assertEqual(due[0]["alert_type"], "30분 요약")
        self.assertEqual(callback.body_format, "text")
        self.assertIn("30분 요약", callback.body)

    def test_holding_alert_indicator_multiplier_and_technical_score_use_full_context(self):
        tools = make_tools()
        try:
            result = tools.calculate_trade_size(
                {
                    "alert_type": "매도 판단",
                    "scenario": "none",
                    "current_price": 900,
                    "position": {"quantity": 100},
                    "account": {"total_asset_value": 1000000, "orderable_amount": 500000},
                    "baselines": {
                        "daily_indicators": {
                            "obv": 100,
                            "obv_sma": 200,
                            "minus_di": 30,
                            "plus_di": 10,
                            "macd_histogram": -1,
                        }
                    },
                    "indicators": {
                        "vwap": {"status": "available", "value": 1000},
                        "market": {"source": "daily", "change_pct": -1.5},
                        "sector": {"source": "daily", "change_pct": -1.2},
                        "relative_strength": {"status": "weak"},
                        "trading_value": {"surge_ratio": 2.5, "avg20": 1000000},
                        "volume_5m": {"status": "available", "ratio": 2.1},
                        "high_52w": {"status": "available", "distance_pct": -2},
                    },
                    "events": {},
                    "data_status": {},
                    "order_book": {"levels": [{"bid_price": 900}], "status": "available"},
                }
            )
        finally:
            tools.close()

        self.assertGreaterEqual(len(result["indicator_components"]), 7)
        self.assertGreaterEqual(len(result["technical_deterioration_components"]), 7)
        self.assertGreaterEqual(result["technical_deterioration_score"], 6)

    def test_holding_alert_overseas_etf_context_includes_fx_and_omits_domestic_market(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                context = tools.get_alert_indicator_context("381170", "20260503")
            finally:
                tools.close()

        self.assertEqual(context["market"]["source"], "omitted_overseas_etf")
        self.assertEqual(context["fx"]["index"], "usdkrw")
        self.assertEqual(context["overseas"]["index"], "nasdaq")

    def test_holding_alert_dry_run_updates_timing_state_without_webhook(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                scan = tools.run_holding_alert_scan("12345678901", dry_run=True)
                state = getattr(tools, "_holding_alerts")._state["symbol_state"]
            finally:
                tools.close()

        self.assertEqual(scan["dispatches"], [])
        self.assertIn("005930", state)
        self.assertIn("last_eval_at", state["005930"])

    def test_holding_alert_default_webhook_body_is_plain_summary(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            try:
                payload = {
                    "category": "trade_decision",
                    "alert_type": "매수 판단",
                    "priority": 8,
                    "scenario": "1차 반등형",
                    "code": "005930",
                    "name": "Samsung Electronics",
                    "triggered_at": "20260503090000",
                    "current_price": 71600,
                    "reasons": ["reclaim 유지", "VWAP 회복"],
                    "trade_size": {
                        "direction": "buy",
                        "recommended_qty": 0,
                        "recommended_amount": 0,
                        "restriction": "현금 부족",
                        "warning": "",
                        "final_text": "자동 주문 아님. 수동 판단 필요.",
                        "price_guide": {"rounded_price": 71600, "status": "available"},
                    },
                }
                payload["text"] = manager._format_alert_text(payload)
                callback = manager._alert_callback(
                    HttpCallbackSpec(method="POST", url="http://localhost:9999/holding-alert"),
                    payload,
                )
            finally:
                tools.close()

        self.assertEqual(callback.body_format, "text")
        self.assertIsInstance(callback.body, str)
        self.assertIn("매수 판단 | Samsung Electronics(005930)", callback.body)
        self.assertIn("매매희망가 71,600원", callback.body)
        self.assertIn("추천 0주", callback.body)
        self.assertIn("제한: 현금 부족", callback.body)
        self.assertNotIn("payload", callback.body)

    def test_holding_alert_template_webhook_uses_public_replacements(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            try:
                payload = {
                    "category": "trade_decision",
                    "alert_type": "매도 판단",
                    "priority": 5,
                    "scenario": "none",
                    "code": "005930",
                    "name": "Samsung Electronics",
                    "triggered_at": "20260503090000",
                    "current_price": 71600,
                    "reasons": ["damage_line 하회", "상대강도 weak"],
                    "trade_size": {
                        "direction": "sell",
                        "calculated_qty": 1234,
                        "recommended_qty": 1234,
                        "recommended_amount": 214800,
                        "restriction": "없음",
                        "warning": "호가 stale",
                        "final_text": "자동 주문 아님. 수동 판단 필요.",
                        "price_guide": {"rounded_price": 71600, "status": "stale"},
                    },
                }
                payload["text"] = manager._format_alert_text(payload)
                callback = manager._alert_callback(
                    HttpCallbackSpec(
                        method="POST",
                        url="http://localhost:9999/holding-alert",
                        body={
                            "content": "{{summary}}",
                            "reason": "{{reasonText}}",
                            "qty": "{{recommendedQty}}",
                            "qty_raw": "{{recommendedQtyRaw}}",
                            "calculated_qty": "{{calculatedQty}}",
                            "price": "{{tradePrice}}",
                            "amount": "{{recommendedAmount}}",
                            "status": "{{priceGuideStatus}}",
                            "raw": "{{payload}}",
                        },
                        body_format="json",
                    ),
                    payload,
                )
            finally:
                tools.close()

        self.assertEqual(callback.body_format, "json")
        self.assertEqual(callback.body["qty"], "1,234")
        self.assertEqual(callback.body["qty_raw"], "1234")
        self.assertEqual(callback.body["calculated_qty"], "1,234")
        self.assertEqual(callback.body["price"], "71,600")
        self.assertEqual(callback.body["amount"], "214,800")
        self.assertEqual(callback.body["status"], "stale")
        self.assertEqual(callback.body["reason"], "damage_line 하회, 상대강도 weak")
        self.assertIn("추천 1,234주", callback.body["content"])
        self.assertEqual(callback.body["raw"], "")

    def test_holding_alert_observe_only_blocks_operational_safety_dispatch(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            manager = getattr(tools, "_holding_alerts")
            try:
                with manager._state_lock:
                    manager._state["runners"].append(
                        {
                            "runner_id": "runner-test",
                            "account_no": "12345678901",
                            "httpCallback": {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                            "dry_run": False,
                            "active": True,
                            "registered_at": "20260503090000",
                        }
                    )
                dispatches = manager._dispatch_scan_results(
                    "12345678901",
                    [
                        {
                            "category": "operational_safety",
                            "alert_type": "운영 안전 알림",
                            "code": "005930",
                            "config": {"observe_only": True},
                            "triggered_at": "20260503090000",
                        }
                    ],
                    None,
                )
            finally:
                tools.close()

        self.assertEqual(dispatches[0]["reason"], "observe_only")

    def test_holding_alert_rt_news_is_flushed_on_scan_not_callback(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                registered = tools.register_holding_alert_runner(
                    "12345678901",
                    {"method": "POST", "url": "http://localhost:9999/holding-alert"},
                    dry_run=True,
                )
                client = getattr(tools, "_client")
                manager = getattr(tools, "_holding_alerts")
                client.emit_rt_event(
                    {
                        "rt_type": "N0",
                        "news_type": "A",
                        "article_id": "NEWS-1",
                        "date": "20260503",
                        "time": "090001",
                        "code": "005930",
                        "title": "거래정지 관련 테스트",
                    }
                )
                queued_count = len(manager._raw_event_queue)
                raw_before_scan = list(manager._state["raw_events"])
                tools.run_holding_alert_scan("12345678901", dry_run=True)
                raw_after_scan = list(manager._state["raw_events"])
                tools.cancel_holding_alert_runner(registered["runner_id"])
            finally:
                tools.close()

        self.assertEqual(queued_count, 1)
        self.assertEqual(raw_before_scan, [])
        self.assertEqual(raw_after_scan[0]["article_id"], "NEWS-1")

    def test_holding_alert_validation_uses_5m_replay_report(self):
        with TemporaryDirectory() as tempdir:
            tools = make_tools(runtime_state_dir=tempdir)
            try:
                result = tools.run_alert_validation("12345678901", lookback_trading_days=3)
            finally:
                tools.close()

        self.assertEqual(result["reports"][0]["validation_method"], "5m_replay")
        self.assertIn("alert_type_counts", result["reports"][0])

    def test_real_intraday_parser_uses_minute_fields(self):
        point = RealIndiClient._build_intraday_price_point(
            ["20260503", "905", "70000", "71000", "69900", "70800", "", "", "", "123456"],
            "20260503",
        )

        self.assertIsNotNone(point)
        assert point is not None
        self.assertEqual(point.date, "20260503")
        self.assertEqual(point.time, "090500")
        self.assertEqual(point.open, 70000)
        self.assertEqual(point.volume, 123456)

    def test_real_intraday_parser_handles_hhmm_and_hhmmss(self):
        self.assertEqual(RealIndiClient._normalize_intraday_time("1530"), "153000")
        self.assertEqual(RealIndiClient._normalize_intraday_time("93005"), "093005")
        self.assertEqual(RealIndiClient._normalize_intraday_time("153012"), "153012")

    def test_real_get_order_book_uses_fresh_uh_without_sh_probe(self):
        client = RealIndiClient.__new__(RealIndiClient)
        fields = [""] * 114
        fields[2] = "153002"
        fields[3] = "1"
        fields[4] = "1"
        fields[5] = "71600"
        fields[8] = "1700"
        fields[45] = "71500"
        fields[48] = "1800"
        client._get_rt_snapshot_once = Mock(return_value=fields)
        client._request = Mock()

        order_book = client.get_order_book("A005930")

        self.assertEqual(order_book.source, "UH")
        self.assertTrue(order_book.available)
        self.assertFalse(order_book.partial)
        self.assertEqual(order_book.levels[0].ask_price, 71600)
        client._get_rt_snapshot_once.assert_called_once_with(
            "UH",
            "005930",
            timeout_ms=client._ORDER_BOOK_UH_TIMEOUT_MS,
        )
        client._request.assert_not_called()

    def test_real_get_order_book_falls_back_to_tr_best_quote(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._get_rt_snapshot_once = Mock(side_effect=TimeoutError("no fresh tick"))
        client._request = Mock(return_value=SimpleNamespace(multi_row_count=2))
        rows = [
            {1: "000660", 2: "153000", 17: "1", 20: "120000", 21: "119900"},
            {1: "005930", 2: "153001", 17: "1", 20: "71600", 21: "71500"},
        ]

        def multi_text(row: int, col: int) -> str:
            return rows[row].get(col, "")

        client._multi_text = Mock(side_effect=multi_text)
        client._multi_int = Mock(side_effect=lambda row, col: int(multi_text(row, col) or 0))

        order_book = client.get_order_book("005930")

        self.assertEqual(order_book.source, "TR_RB002")
        self.assertTrue(order_book.available)
        self.assertTrue(order_book.partial)
        self.assertEqual(order_book.received_at, "153001")
        self.assertEqual(order_book.market_phase, "regular")
        self.assertEqual(len(order_book.levels), 1)
        self.assertEqual(order_book.levels[0].ask_price, 71600)
        self.assertEqual(order_book.levels[0].ask_size, 0)
        self.assertEqual(order_book.levels[0].bid_price, 71500)
        self.assertIn("best bid/ask", order_book.message)
        client._request.assert_called_once_with("TR_RB002", ["0"])

    def test_real_get_order_book_returns_no_quote_when_tr_has_no_prices(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._get_rt_snapshot_once = Mock(side_effect=TimeoutError("no fresh tick"))
        client._request = Mock(return_value=SimpleNamespace(multi_row_count=1))
        rows = [{1: "005930", 2: "153001", 17: "1", 20: "", 21: ""}]

        def multi_text(row: int, col: int) -> str:
            return rows[row].get(col, "")

        client._multi_text = Mock(side_effect=multi_text)
        client._multi_int = Mock(return_value=0)

        order_book = client.get_order_book("005930")

        self.assertEqual(order_book.source, "TR_RB002")
        self.assertFalse(order_book.available)
        self.assertEqual(order_book.levels, [])
        self.assertEqual(order_book.received_at, "153001")
        self.assertEqual(order_book.market_phase, "regular")
        self.assertIn("not trading", order_book.message)

    def test_real_get_order_book_returns_no_quote_during_after_hours_close(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._get_rt_snapshot_once = Mock(side_effect=TimeoutError("no fresh tick"))
        client._request = Mock(return_value=SimpleNamespace(multi_row_count=1))
        rows = [{1: "005930", 2: "155959", 17: "3", 20: "0", 21: "220500"}]

        def multi_text(row: int, col: int) -> str:
            return rows[row].get(col, "")

        client._multi_text = Mock(side_effect=multi_text)
        client._multi_int = Mock(side_effect=lambda row, col: int(multi_text(row, col) or 0))

        order_book = client.get_order_book("005930")

        self.assertEqual(order_book.source, "TR_RB002")
        self.assertFalse(order_book.available)
        self.assertFalse(order_book.partial)
        self.assertEqual(order_book.levels, [])
        self.assertEqual(order_book.received_at, "155959")
        self.assertEqual(order_book.market_phase, "after_hours_close")
        self.assertIn("closing price", order_book.message)

    def test_real_get_order_book_returns_no_quote_when_tr_has_no_row(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._get_rt_snapshot_once = Mock(side_effect=TimeoutError("no fresh tick"))
        client._request = Mock(return_value=SimpleNamespace(multi_row_count=0))

        order_book = client.get_order_book("005930")

        self.assertEqual(order_book.source, "TR_RB002")
        self.assertFalse(order_book.available)
        self.assertEqual(order_book.market_phase, "no_quote")
        self.assertEqual(order_book.levels, [])
        self.assertIn("not trading", order_book.message)

    def test_real_get_order_book_one_shot_unregisters_after_rt_timeout(self):
        client = RealIndiClient.__new__(RealIndiClient)
        client._rt_snapshots = {}
        client._register_realtime = Mock(side_effect=TimeoutError("no fresh tick"))
        client._unregister_realtime = Mock()

        with self.assertRaisesRegex(TimeoutError, "no fresh tick"):
            client._get_rt_snapshot_once("UH", "005930", timeout_ms=1)

        client._unregister_realtime.assert_called_once_with("UH", "005930")

    def test_cash_order_book_parser_uses_top_five_levels(self):
        fields = [""] * 56
        fields[2] = "153001"
        fields[3] = "1"
        fields[4] = "71600"
        fields[5] = "71500"
        fields[6] = "1200"
        fields[7] = "1800"
        fields[8] = "71700"
        fields[9] = "71400"
        fields[10] = "900"
        fields[11] = "1500"
        fields[12] = "71800"
        fields[13] = "71300"
        fields[14] = "800"
        fields[15] = "1400"
        fields[16] = "71900"
        fields[17] = "71200"
        fields[18] = "700"
        fields[19] = "1300"
        fields[20] = "72000"
        fields[21] = "71100"
        fields[22] = "600"
        fields[23] = "1200"

        order_book = RealIndiClient._build_cash_order_book("005930", fields)

        self.assertEqual(order_book.code, "005930")
        self.assertEqual(order_book.received_at, "153001")
        self.assertEqual(order_book.market_phase, "regular")
        self.assertEqual(len(order_book.levels), 5)
        self.assertEqual(order_book.levels[0].ask_price, 71600)
        self.assertEqual(order_book.levels[0].bid_size, 1800)
        self.assertEqual(order_book.levels[4].bid_price, 71100)

    def test_integrated_order_book_parser_prefers_integrated_sizes(self):
        fields = [""] * 114
        fields[2] = "153002"
        fields[3] = "1"
        fields[4] = "1"
        fields[5] = "71600"
        fields[8] = "1700"
        fields[9] = "71700"
        fields[12] = "1600"
        fields[13] = "71800"
        fields[16] = "1500"
        fields[17] = "71900"
        fields[20] = "1400"
        fields[21] = "72000"
        fields[24] = "1300"
        fields[45] = "71500"
        fields[48] = "1800"
        fields[49] = "71400"
        fields[52] = "1750"
        fields[53] = "71300"
        fields[56] = "1650"
        fields[57] = "71200"
        fields[60] = "1550"
        fields[61] = "71100"
        fields[64] = "1450"

        order_book = RealIndiClient._build_integrated_order_book("005930", fields)

        self.assertEqual(order_book.code, "005930")
        self.assertEqual(order_book.received_at, "153002")
        self.assertEqual(order_book.market_phase, "krx:1|nxt:1")
        self.assertEqual(len(order_book.levels), 5)
        self.assertEqual(order_book.levels[0].ask_size, 1700)
        self.assertEqual(order_book.levels[0].bid_price, 71500)
        self.assertEqual(order_book.levels[4].bid_size, 1450)

    def test_kospi200_index_price_parser_uses_current_price_as_close(self):
        point = RealIndiClient._build_kospi200_index_price_point(
            ["20260421", "", "352.95", "354.10", "351.88", "353.72", "176540", "8871200"]
        )

        self.assertIsNotNone(point)
        assert point is not None
        self.assertEqual(point.close, 353.72)

    def test_overseas_index_price_parser_uses_close_price(self):
        point = RealIndiClient._build_overseas_index_price_point(
            ["20260421", "", "5256.00", "5271.33", "5248.72", "5268.14", "0"]
        )

        self.assertIsNotNone(point)
        assert point is not None
        self.assertEqual(point.close, 5268.14)

    def test_news_content_html_is_cleaned_to_plain_text(self):
        raw = "<p>첫 문단&nbsp;입니다.</p><div>둘째<br>줄</div><ul><li>항목1</li><li>항목2</li></ul>"

        cleaned = RealIndiClient._clean_news_content_html(raw)

        self.assertEqual(cleaned, "첫 문단 입니다.\n\n둘째 줄\n\n- 항목1\n- 항목2")

    def test_news_content_hts_anchor_appends_stock_code(self):
        raw = '<p><a href="hts://open?code=A005930">삼성전자</a> 관련 기사</p>'

        cleaned = RealIndiClient._clean_news_content_html(raw)

        self.assertEqual(cleaned, "삼성전자(005930) 관련 기사")

    def test_news_content_extracts_links_and_dart_rcp_no_from_raw_html(self):
        raw = (
            '<p><a href="https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260508000567">'
            "공시 보기(클릭)</a></p>"
        )

        links = RealIndiClient._extract_news_content_links(raw)

        self.assertEqual(
            links,
            [
                {
                    "text": "공시 보기(클릭)",
                    "href": "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260508000567",
                    "rcpNo": "20260508000567",
                }
            ],
        )
        self.assertEqual(RealIndiClient._extract_news_content_rcp_no(raw, links), "20260508000567")

    def test_news_content_extracts_dart_rcp_no_from_onclick(self):
        raw = (
            '<a href="#none" onclick="openReportViewer(\'20260508000567\'); return false;">'
            "공시 보기(클릭)</a>"
        )

        links = RealIndiClient._extract_news_content_links(raw)

        self.assertEqual(links[0]["rcpNo"], "20260508000567")

    def test_limit_order_requires_positive_price_when_live_orders_enabled(self):
        tools = make_tools(allow_live_orders=True)

        with self.assertRaisesRegex(ValueError, "limit order"):
            tools.place_order(
                account_no="12345678901",
                code="005930",
                side="buy",
                quantity=1,
                price=None,
            )


if __name__ == "__main__":
    unittest.main()
