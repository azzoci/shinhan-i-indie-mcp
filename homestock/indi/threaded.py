from __future__ import annotations

import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import count
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
    OrderRequest,
    OrderResult,
    QuoteSnapshot,
    Stock,
    TopMover,
    TradeHistoryItem,
)
from homestock.ops_log import LogSource, ops_log


@dataclass
class _IndiTask:
    task_id: int
    method_name: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    result_queue: queue.Queue[tuple[bool, Any]]
    time_critical: bool = False
    _state_lock: threading.Lock = field(default_factory=threading.Lock)
    _started: bool = False
    _cancelled: bool = False

    def try_mark_started(self) -> bool:
        with self._state_lock:
            if self._cancelled:
                return False
            self._started = True
            return True

    def try_cancel_before_start(self) -> bool:
        with self._state_lock:
            if self._started:
                return False
            self._cancelled = True
            return True

    def has_started(self) -> bool:
        with self._state_lock:
            return self._started


_STOP = object()


class ThreadedIndiClient(IndiClient):
    """Run an IndiClient implementation on one dedicated OS thread."""

    _TIME_CRITICAL_METHODS = {
        "place_order",
        "modify_order",
        "cancel_order",
        "place_gold_order",
        "modify_gold_order",
        "cancel_gold_order",
    }

    def __init__(
        self,
        client_factory: Callable[[], IndiClient],
        *,
        startup_timeout: float = 30.0,
        call_timeout: float = 120.0,
        pump_interval: float = 0.1,
    ) -> None:
        self._client_factory = client_factory
        self._call_timeout = call_timeout
        self._pump_interval = pump_interval
        self._tasks: queue.Queue[_IndiTask | object] = queue.Queue()
        self._rt_events: queue.Queue[tuple[int, dict[str, Any]] | object] = queue.Queue()
        self._gold_rt_events: queue.Queue[tuple[int, dict[str, Any]] | object] = queue.Queue()
        self._listeners: list[Callable[[dict[str, Any]], None]] = []
        self._batch_listeners: list[Callable[[list[dict[str, Any]]], None]] = []
        self._gold_listeners: list[Callable[[dict[str, Any]], None]] = []
        self._listener_lock = threading.RLock()
        self._gold_listener_lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._client: IndiClient | None = None
        self._startup_error: BaseException | None = None
        self._worker_thread_id: int | None = None
        self._task_ids = count(1)
        self._rt_event_ids = count(1)
        self._gold_rt_event_ids = count(1)
        self._event_pump_lock = threading.Lock()
        self._event_pump_count = 0
        self._last_event_pump_monotonic: float | None = None
        self.INDI_MAIN_PROCESS_NAME = str(getattr(client_factory, "INDI_MAIN_PROCESS_NAME", ""))

        self._event_thread = threading.Thread(
            target=self._dispatch_rt_events,
            name="homestock-indi-rt-dispatch",
            daemon=True,
        )
        self._gold_event_thread = threading.Thread(
            target=self._dispatch_gold_rt_events,
            name="homestock-indi-gold-rt-dispatch",
            daemon=True,
        )
        self._worker_thread = threading.Thread(
            target=self._run_worker,
            name="homestock-indi-worker",
            daemon=True,
        )
        ops_log(LogSource.MANAGE,
            "starting INDI worker and RT dispatch threads "
            f"startup_timeout={startup_timeout} call_timeout={call_timeout} pump_interval={pump_interval}",
        )
        self._event_thread.start()
        self._gold_event_thread.start()
        self._worker_thread.start()
        if not self._ready.wait(startup_timeout):
            self.close()
            raise TimeoutError(f"INDI worker did not start within {startup_timeout:.1f}s")
        if self._startup_error is not None:
            error = self._startup_error
            self.close()
            raise RuntimeError(f"INDI worker startup failed: {error}") from error
        ops_log(LogSource.MANAGE,
            f"INDI worker ready thread_id={self._worker_thread_id} "
            f"event_thread_alive={self._event_thread.is_alive()} worker_thread_alive={self._worker_thread.is_alive()}",
        )

    def close(self) -> None:
        ops_log(LogSource.MANAGE,
            f"close requested tasks_queue={self._tasks.qsize()} rt_queue={self._rt_events.qsize()}",
        )
        with self._lifecycle_lock:
            self._stop_requested.set()
            self._cancel_pending_tasks_locked("INDI worker is closing")
            self._tasks.put(_STOP)
            self._rt_events.put(_STOP)
            self._gold_rt_events.put(_STOP)
        if threading.get_ident() != self._worker_thread.ident:
            self._worker_thread.join(timeout=5.0)
        if threading.get_ident() != self._event_thread.ident:
            self._event_thread.join(timeout=5.0)
        if threading.get_ident() != self._gold_event_thread.ident:
            self._gold_event_thread.join(timeout=5.0)
        ops_log(LogSource.MANAGE,
            "close complete "
            f"worker_alive={self._worker_thread.is_alive()} event_thread_alive={self._event_thread.is_alive()} "
            f"gold_event_thread_alive={self._gold_event_thread.is_alive()} "
            f"tasks_queue={self._tasks.qsize()} rt_queue={self._rt_events.qsize()} "
            f"gold_rt_queue={self._gold_rt_events.qsize()}",
        )

    def _cancel_pending_tasks_locked(self, reason: str) -> None:
        cancelled = 0
        while True:
            try:
                item = self._tasks.get_nowait()
            except queue.Empty:
                ops_log(LogSource.MANAGE, f"pending task cancel complete cancelled={cancelled} reason={reason}")
                return
            if item is _STOP:
                continue
            assert isinstance(item, _IndiTask)
            if self._cancel_task_before_start(item, reason):
                cancelled += 1

    def _cancel_task_before_start(self, task: _IndiTask, reason: str) -> bool:
        if not task.try_cancel_before_start():
            return False
        ops_log(LogSource.MANAGE, f"task cancelled before start task_id={task.task_id} method={task.method_name} reason={reason}")
        self._deliver_task_result(task, False, RuntimeError(f"{reason}: {task.method_name}"))
        return True

    @staticmethod
    def _deliver_task_result(task: _IndiTask, ok: bool, result: Any) -> None:
        try:
            task.result_queue.put_nowait((ok, result))
        except queue.Full:
            pass

    def _run_worker(self) -> None:
        self._worker_thread_id = threading.get_ident()
        ops_log(LogSource.MANAGE, f"worker thread entered thread_id={self._worker_thread_id}")
        try:
            ops_log(LogSource.MANAGE, "worker creating wrapped INDI client")
            client = self._client_factory()
            ops_log(LogSource.MANAGE, f"worker wrapped client created class={client.__class__.__name__}")
            client.register_rt_listener(self._enqueue_rt_event)
            ops_log(LogSource.MANAGE, "worker registered RT listener")
            client.register_gold_rt_listener(self._enqueue_gold_rt_event)
            ops_log(LogSource.MANAGE, "worker registered gold RT listener")
            self._client = client
            self.INDI_MAIN_PROCESS_NAME = str(
                getattr(client, "INDI_MAIN_PROCESS_NAME", self.INDI_MAIN_PROCESS_NAME)
            )
        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()
            ops_log(LogSource.MANAGE, f"worker startup failed: {exc.__class__.__name__}: {exc}")
            return
        self._ready.set()

        try:
            while not self._stop_requested.is_set():
                try:
                    task = self._tasks.get(timeout=self._pump_interval)
                except queue.Empty:
                    self._pump_client_events()
                    continue
                if task is _STOP:
                    break
                assert isinstance(task, _IndiTask)
                self._execute_task(task)
                self._pump_client_events()
        finally:
            self._cleanup_client()
        ops_log(LogSource.MANAGE, "worker thread exiting")

    def _cleanup_client(self) -> None:
        client = self._client
        if client is None:
            ops_log(LogSource.MANAGE, "client cleanup skipped reason=no_client")
            return
        ops_log(LogSource.MANAGE, f"client cleanup begin class={client.__class__.__name__}")
        try:
            client.unregister_rt_listener(self._enqueue_rt_event)
            ops_log(LogSource.MANAGE, "client cleanup unregistered RT listener")
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"INDI worker listener cleanup failed: {exc}")
        try:
            client.unregister_gold_rt_listener(self._enqueue_gold_rt_event)
            ops_log(LogSource.MANAGE, "client cleanup unregistered gold RT listener")
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"INDI worker gold listener cleanup failed: {exc}")
        close_client = getattr(client, "close", None)
        if callable(close_client):
            try:
                close_client()
                ops_log(LogSource.MANAGE, "client cleanup close complete")
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"INDI worker client cleanup failed: {exc}")
        self._client = None
        ops_log(LogSource.MANAGE, "client cleanup complete")

    def _execute_task(self, task: _IndiTask) -> None:
        client = self._client
        if client is None:
            ops_log(LogSource.MANAGE, f"task failed task_id={task.task_id} method={task.method_name} reason=client_not_ready")
            self._deliver_task_result(task, False, RuntimeError("INDI worker client is not ready"))
            return
        if self._stop_requested.is_set() and self._cancel_task_before_start(task, "INDI worker is closing"):
            return
        if not task.try_mark_started():
            ops_log(LogSource.MANAGE, f"task skipped stale task_id={task.task_id} method={task.method_name}")
            self._deliver_task_result(
                task,
                False,
                TimeoutError(
                    f"INDI worker skipped stale time-critical call before execution: {task.method_name}"
                ),
            )
            return
        started_at = time.monotonic()
        ops_log(LogSource.MANAGE,
            f"task start task_id={task.task_id} method={task.method_name} time_critical={task.time_critical}",
            level="debug",
        )
        try:
            method = getattr(client, task.method_name)
            result = method(*task.args, **task.kwargs)
            elapsed_ms = int((time.monotonic() - started_at) * 1000)
            ops_log(LogSource.MANAGE,
                f"task success task_id={task.task_id} method={task.method_name} elapsed_ms={elapsed_ms}",
                level="debug",
            )
            self._deliver_task_result(task, True, result)
        except BaseException as exc:
            elapsed_ms = int((time.monotonic() - started_at) * 1000)
            ops_log(LogSource.MANAGE,
                f"task failed task_id={task.task_id} method={task.method_name} "
                f"elapsed_ms={elapsed_ms} error={exc.__class__.__name__}: {exc}",
            )
            self._deliver_task_result(task, False, exc)

    def _pump_client_events(self) -> None:
        client = self._client
        if client is None:
            return
        pump_events = getattr(client, "pump_events", None)
        if pump_events is None:
            return
        try:
            pump_events()
            self._record_event_pump()
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"INDI event pump failed: {exc}")

    def _record_event_pump(self) -> None:
        with self._event_pump_lock:
            self._event_pump_count += 1
            self._last_event_pump_monotonic = time.monotonic()

    def event_pump_snapshot(self) -> dict[str, object]:
        with self._event_pump_lock:
            return {
                "pump_count": self._event_pump_count,
                "last_pump_monotonic": self._last_event_pump_monotonic,
                "pump_interval_seconds": self._pump_interval,
                "worker_thread_alive": self._worker_thread.is_alive(),
                "event_thread_alive": self._event_thread.is_alive(),
            }

    def _call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        if self._stop_requested.is_set():
            raise RuntimeError(f"INDI worker is closing: {method_name}")
        if threading.get_ident() == self._worker_thread_id:
            client = self._client
            if client is None:
                raise RuntimeError("INDI worker client is not ready")
            return getattr(client, method_name)(*args, **kwargs)

        result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)
        task = _IndiTask(
            next(self._task_ids),
            method_name,
            args,
            dict(kwargs),
            result_queue,
            time_critical=method_name in self._TIME_CRITICAL_METHODS,
        )
        with self._lifecycle_lock:
            if self._stop_requested.is_set():
                raise RuntimeError(f"INDI worker is closing: {method_name}")
            self._tasks.put(task)
            queue_size = self._tasks.qsize()
        ops_log(LogSource.MANAGE,
            f"task queued task_id={task.task_id} method={method_name} "
            f"time_critical={task.time_critical} queue_size={queue_size}",
            level="debug",
        )
        if task.time_critical:
            return self._wait_for_time_critical_result(task)
        return self._wait_for_result(task, timeout=self._call_timeout)

    def _wait_for_result(self, task: _IndiTask, timeout: float | None) -> Any:
        try:
            ok, result = task.result_queue.get(timeout=timeout)
        except queue.Empty as exc:
            ops_log(LogSource.MANAGE, f"task wait timeout task_id={task.task_id} method={task.method_name} timeout={timeout}")
            raise TimeoutError(f"INDI worker call timed out: {task.method_name}") from exc
        ops_log(LogSource.MANAGE,
            f"task result received task_id={task.task_id} method={task.method_name} ok={ok}",
            level="debug",
        )
        if ok:
            return result
        raise result

    def _wait_for_time_critical_result(self, task: _IndiTask) -> Any:
        queued_deadline = time.monotonic() + self._call_timeout
        while True:
            if task.has_started():
                return self._wait_for_result(task, timeout=None)
            remaining = queued_deadline - time.monotonic()
            if remaining <= 0:
                if task.try_cancel_before_start():
                    ops_log(LogSource.MANAGE,
                        f"time-critical task timed out before start task_id={task.task_id} method={task.method_name}",
                    )
                    raise TimeoutError(
                        f"INDI worker time-critical call timed out before execution: {task.method_name}"
                    )
                ops_log(LogSource.MANAGE,
                    f"time-critical task started after timeout window task_id={task.task_id} method={task.method_name}; waiting for result",
                )
                return self._wait_for_result(task, timeout=None)
            try:
                return self._wait_for_result(task, timeout=min(remaining, 0.05))
            except TimeoutError as exc:
                if str(exc) != f"INDI worker call timed out: {task.method_name}":
                    raise

    def _enqueue_rt_event(self, event: dict[str, Any]) -> None:
        event_id = next(self._rt_event_ids)
        copied = dict(event)
        self._rt_events.put((event_id, copied))
        ops_log(LogSource.RT_INDI,
            f"RT event queued event_id={event_id} rt_type={copied.get('rt_type') or ''} "
            f"code={copied.get('code') or ''} news_type={copied.get('news_type') or ''} "
            f"queue_size={self._rt_events.qsize()}",
            level="debug",
        )

    def _enqueue_gold_rt_event(self, event: dict[str, Any]) -> None:
        event_id = next(self._gold_rt_event_ids)
        copied = dict(event)
        self._gold_rt_events.put((event_id, copied))
        ops_log(LogSource.RT_INDI,
            f"Gold RT event queued event_id={event_id} rt_type={copied.get('rt_type') or ''} "
            f"code={copied.get('code') or ''} queue_size={self._gold_rt_events.qsize()}",
            level="debug",
        )

    def _dispatch_rt_events(self) -> None:
        ops_log(LogSource.RT_INDI, f"RT dispatch thread entered thread_id={threading.get_ident()}")
        while True:
            item = self._rt_events.get()
            if item is _STOP:
                ops_log(LogSource.RT_INDI, "RT dispatch stop received")
                return
            assert isinstance(item, tuple)
            batch: list[tuple[int, dict[str, Any]]] = [item]
            stop_after_batch = False
            for _ in range(self._rt_events.qsize()):
                try:
                    queued_item = self._rt_events.get_nowait()
                except queue.Empty:
                    break
                if queued_item is _STOP:
                    stop_after_batch = True
                    break
                assert isinstance(queued_item, tuple)
                batch.append(queued_item)
            with self._listener_lock:
                listeners = list(self._listeners)
                batch_listeners = list(self._batch_listeners)
            events = [event for _, event in batch]
            if batch_listeners:
                first_event_id = batch[0][0]
                last_event_id = batch[-1][0]
                ops_log(LogSource.RT_INDI,
                    f"RT batch dispatch begin first_event_id={first_event_id} "
                    f"last_event_id={last_event_id} events={len(events)} "
                    f"batch_listeners={len(batch_listeners)}",
                    level="debug",
                )
                for listener in batch_listeners:
                    try:
                        listener([dict(event) for event in events])
                    except Exception as exc:
                        ops_log(LogSource.RT_INDI, f"RT batch listener failed: {exc}")
                ops_log(LogSource.RT_INDI,
                    f"RT batch dispatch complete first_event_id={first_event_id} "
                    f"last_event_id={last_event_id} batch_listeners={len(batch_listeners)}",
                    level="debug",
                )
            for event_id, event in batch:
                assert isinstance(event, dict)
                ops_log(LogSource.RT_INDI,
                    f"RT event dispatch begin event_id={event_id} rt_type={event.get('rt_type') or ''} "
                    f"code={event.get('code') or ''} listeners={len(listeners)}",
                    level="debug",
                )
                for listener in listeners:
                    try:
                        listener(dict(event))
                    except Exception as exc:
                        ops_log(LogSource.RT_INDI, f"RT listener failed for {event.get('rt_type')}: {exc}")
                ops_log(LogSource.RT_INDI,
                    f"RT event dispatch complete event_id={event_id} listeners={len(listeners)}",
                    level="debug",
                )
            if stop_after_batch:
                ops_log(LogSource.RT_INDI, "RT dispatch stop received after batch")
                return

    def _dispatch_gold_rt_events(self) -> None:
        ops_log(LogSource.RT_INDI, f"Gold RT dispatch thread entered thread_id={threading.get_ident()}")
        while True:
            item = self._gold_rt_events.get()
            if item is _STOP:
                ops_log(LogSource.RT_INDI, "Gold RT dispatch stop received")
                return
            assert isinstance(item, tuple)
            event_id, event = item
            assert isinstance(event, dict)
            with self._gold_listener_lock:
                listeners = list(self._gold_listeners)
            ops_log(LogSource.RT_INDI,
                f"Gold RT event dispatch begin event_id={event_id} rt_type={event.get('rt_type') or ''} "
                f"code={event.get('code') or ''} listeners={len(listeners)}",
                level="debug",
            )
            for listener in listeners:
                try:
                    listener(dict(event))
                except Exception as exc:
                    ops_log(LogSource.RT_INDI, f"Gold RT listener failed for {event.get('rt_type')}: {exc}")
            ops_log(LogSource.RT_INDI,
                f"Gold RT event dispatch complete event_id={event_id} listeners={len(listeners)}",
                level="debug",
            )

    def health_check(self, live_orders_allowed: bool) -> HealthStatus:
        return self._call("health_check", live_orders_allowed)

    def check_indi_process_status(self) -> dict[str, object]:
        return self._call("check_indi_process_status")

    def list_stocks(self) -> list[Stock]:
        return self._call("list_stocks")

    def list_gold_products(self) -> list[GoldProduct]:
        return self._call("list_gold_products")

    def get_daily_prices(self, code: str, start_date: str | None, end_date: str | None) -> list[DailyPrice]:
        return self._call("get_daily_prices", code, start_date, end_date)

    def get_intraday_prices(self, code: str, date: str, interval_minutes: int = 5) -> list[IntradayPrice]:
        return self._call("get_intraday_prices", code, date, interval_minutes)

    def get_gold_daily_prices(
        self,
        code: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[GoldDailyPrice]:
        return self._call("get_gold_daily_prices", code, start_date, end_date)

    def get_gold_intraday_prices(
        self,
        code: str,
        date: str,
        interval_minutes: int = 5,
    ) -> list[GoldIntradayPrice]:
        return self._call("get_gold_intraday_prices", code, date, interval_minutes)

    def get_market_index_prices(
        self,
        start_date: str,
        end_date: str,
    ) -> dict[str, list[MarketIndexPricePoint]]:
        return self._call("get_market_index_prices", start_date, end_date)

    def get_sector_index_prices(
        self,
        sector_code: str,
        start_date: str,
        end_date: str,
        interval: str = "D",
    ) -> list[MarketIndexPricePoint]:
        return self._call("get_sector_index_prices", sector_code, start_date, end_date, interval)

    def get_stock_sector_profile(self, code: str) -> dict[str, Any]:
        return self._call("get_stock_sector_profile", code)

    def subscribe_realtime_price(self, code: str) -> dict[str, object]:
        return self._call("subscribe_realtime_price", code)

    def unsubscribe_realtime_price(self, code: str) -> dict[str, object]:
        return self._call("unsubscribe_realtime_price", code)

    def subscribe_gold_realtime_price(self, code: str) -> dict[str, object]:
        return self._call("subscribe_gold_realtime_price", code)

    def unsubscribe_gold_realtime_price(self, code: str) -> dict[str, object]:
        return self._call("unsubscribe_gold_realtime_price", code)

    def subscribe_disclosure_feed(self, code: str) -> dict[str, object]:
        return self._call("subscribe_disclosure_feed", code)

    def unsubscribe_disclosure_feed(self, code: str) -> dict[str, object]:
        return self._call("unsubscribe_disclosure_feed", code)

    def subscribe_news_feed(self, code: str | None = None) -> dict[str, object]:
        return self._call("subscribe_news_feed", code)

    def unsubscribe_news_feed(self, code: str | None = None) -> dict[str, object]:
        return self._call("unsubscribe_news_feed", code)

    def register_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._listener_lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def unregister_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._listener_lock:
            self._listeners = [item for item in self._listeners if item is not listener]

    def register_rt_batch_listener(self, listener: Callable[[list[dict[str, Any]]], None]) -> None:
        with self._listener_lock:
            if listener not in self._batch_listeners:
                self._batch_listeners.append(listener)

    def unregister_rt_batch_listener(self, listener: Callable[[list[dict[str, Any]]], None]) -> None:
        with self._listener_lock:
            self._batch_listeners = [item for item in self._batch_listeners if item is not listener]

    def register_gold_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._gold_listener_lock:
            if listener not in self._gold_listeners:
                self._gold_listeners.append(listener)

    def unregister_gold_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._gold_listener_lock:
            self._gold_listeners = [item for item in self._gold_listeners if item is not listener]

    def normalize_stock_code(self, code: str | None) -> str:
        return self._call("normalize_stock_code", code)

    def normalize_gold_code(self, code: str | None) -> str:
        return self._call("normalize_gold_code", code)

    def get_last_rt_error_details(self) -> dict[str, Any] | None:
        return self._call("get_last_rt_error_details")

    def get_accounts(self) -> list[Account]:
        return self._call("get_accounts")

    def get_account_summary(self, account_no: str) -> AccountSummary:
        return self._call("get_account_summary", account_no)

    def get_gold_account_summary(self, account_no: str) -> GoldAccountSummary:
        return self._call("get_gold_account_summary", account_no)

    def get_gold_account_balance(self, account_no: str) -> GoldAccountBalance:
        return self._call("get_gold_account_balance", account_no)

    def get_fundamentals(
        self,
        code: str,
        consolidated: bool = True,
        quarterly: bool = True,
    ) -> list[FundamentalPoint]:
        return self._call("get_fundamentals", code, consolidated, quarterly)

    def get_quote_snapshot(self, code: str) -> QuoteSnapshot:
        return self._call("get_quote_snapshot", code)

    def get_gold_quote_snapshot(self, code: str) -> GoldQuoteSnapshot:
        return self._call("get_gold_quote_snapshot", code)

    def get_investor_flow_by_stock(
        self,
        code: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[InvestorFlowPoint]:
        return self._call("get_investor_flow_by_stock", code, start_date, end_date)

    def get_market_investor_flow_intraday(
        self,
        include_institution_breakdown: bool = False,
    ) -> list[MarketInvestorFlowPoint]:
        return self._call("get_market_investor_flow_intraday", include_institution_breakdown)

    def get_foreign_flow_rankings(
        self,
        market: str = "all",
        consecutive_days: int = 3,
        direction: str = "buy",
    ) -> list[ForeignFlowRanking]:
        return self._call("get_foreign_flow_rankings", market, consecutive_days, direction)

    def get_top_movers(
        self,
        market: str = "all",
        direction: str = "up",
        date: str | None = None,
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[TopMover]:
        return self._call("get_top_movers", market, direction, date, limit, kospi200_only)

    def list_stock_news(self, code: str, date: str | None = None) -> list[MarketNewsItem]:
        return self._call("list_stock_news", code, date)

    def list_market_flow_news(
        self,
        date: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> list[MarketNewsItem]:
        return self._call("list_market_flow_news", date, from_time, to_time)

    def get_news_content(self, news_type: str, date: str, article_id: str) -> NewsContent:
        return self._call("get_news_content", news_type, date, article_id)

    def get_volume_surge(
        self,
        market: str = "all",
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        return self._call("get_volume_surge", market, limit, kospi200_only)

    def get_new_highs_lows(
        self,
        market: str = "all",
        mode: str = "new_high",
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        return self._call("get_new_highs_lows", market, mode, limit, kospi200_only)

    def get_limit_hits(
        self,
        market: str = "all",
        mode: str = "upper",
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        return self._call("get_limit_hits", market, mode, kospi200_only)

    def get_order_book(self, code: str) -> OrderBook:
        return self._call("get_order_book", code)

    def get_gold_order_book(self, code: str) -> OrderBook:
        return self._call("get_gold_order_book", code)

    def get_cash_order_book_snapshot(self, code: str) -> OrderBook:
        return self._call("get_cash_order_book_snapshot", code)

    def get_balance(self, account_no: str) -> list[BalanceItem]:
        return self._call("get_balance", account_no)

    def get_gold_balance(self, account_no: str) -> list[GoldBalanceItem]:
        return self._call("get_gold_balance", account_no)

    def get_executions(self, account_no: str) -> list[Execution]:
        return self._call("get_executions", account_no)

    def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
        return self._call("get_open_orders", account_no, code)

    def get_trade_history(
        self,
        account_no: str,
        code: str | None,
        start_date: str,
        end_date: str | None = None,
    ) -> list[TradeHistoryItem]:
        return self._call("get_trade_history", account_no, code, start_date, end_date)

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
        return self._call(
            "get_account_ledger",
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

    def place_order(self, request: OrderRequest) -> OrderResult:
        return self._call("place_order", request)

    def modify_order(self, request: OrderRequest) -> OrderResult:
        return self._call("modify_order", request)

    def cancel_order(self, request: OrderRequest) -> OrderResult:
        return self._call("cancel_order", request)

    def place_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        return self._call("place_gold_order", request)

    def modify_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        return self._call("modify_gold_order", request)

    def cancel_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        return self._call("cancel_gold_order", request)
