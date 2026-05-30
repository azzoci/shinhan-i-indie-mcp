from __future__ import annotations

import copy
import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from homestock.analysis import build_technical_indicators
from homestock.display_format import format_display_decimal
from homestock.indi import IndiClient
from homestock.models import (
    BalanceItem,
    DailyPrice,
    HttpCallbackSpec,
    IntradayPrice,
    OrderBook,
)
from homestock.ops_log import LogSource, ops_log
from homestock.webhook import CallbackDispatcher


try:
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9))


ALERT_PRIORITIES = {
    "최종 방어선 검토": 1,
    "전일 종가 훼손 후속 판단": 2,
    "종가 훼손": 3,
    "이익 보호 알림": 4,
    "매도 판단": 5,
    "매도 주의": 6,
    "회복 판단": 7,
    "매수 판단": 8,
    "수익 실현 검토": 9,
}

URGENT_ALERT_TYPES = {
    "최종 방어선 검토",
    "전일 종가 훼손 후속 판단",
    "종가 훼손",
    "운영 안전 알림",
}


@dataclass(frozen=True)
class PriceInput:
    current_price: float
    status: str
    source: str
    received_at: str
    age_seconds: float | None = None


class HoldingAlertStateStore:
    VERSION = 1
    FILE_NAME = "holding_alert_state.json"

    def __init__(self, state_dir: Path) -> None:
        self._state_dir = state_dir

    def path(self) -> Path:
        return self._state_dir / self.FILE_NAME

    def load(self) -> dict[str, Any]:
        path = self.path()
        if not path.exists():
            return self._default_state()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            ops_log(LogSource.MANAGE, f"Failed to load holding alert state {path}: {exc}")
            return self._default_state()
        return self._merge_defaults(payload)

    def save(self, state: dict[str, Any]) -> Path:
        path = self.path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._merge_defaults(state)
        payload["updated_at"] = _kst_now().strftime("%Y%m%d%H%M%S")
        temp_path = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            os.replace(temp_path, path)
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return path

    def _default_state(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            "updated_at": "",
            "runners": [],
            "baseline_cache": {},
            "symbol_state": {},
            "alert_history": [],
            "raw_events": [],
            "validation": {},
            "whipsaw_overrides": {},
            "balance_snapshots": {},
            "dry_run_alert_history": [],
            "pending_alert_bundles": {},
            "pending_alert_summaries": {},
        }

    def _merge_defaults(self, payload: dict[str, Any]) -> dict[str, Any]:
        base = self._default_state()
        for key in base:
            if key in payload:
                base[key] = payload[key]
        base["runners"] = list(base.get("runners") or [])
        base["baseline_cache"] = dict(base.get("baseline_cache") or {})
        base["symbol_state"] = dict(base.get("symbol_state") or {})
        base["alert_history"] = list(base.get("alert_history") or [])
        base["raw_events"] = list(base.get("raw_events") or [])
        base["validation"] = dict(base.get("validation") or {})
        base["whipsaw_overrides"] = dict(base.get("whipsaw_overrides") or {})
        base["balance_snapshots"] = dict(base.get("balance_snapshots") or {})
        base["dry_run_alert_history"] = list(base.get("dry_run_alert_history") or [])
        base["pending_alert_bundles"] = dict(base.get("pending_alert_bundles") or {})
        base["pending_alert_summaries"] = dict(base.get("pending_alert_summaries") or {})
        return base


class HoldingAlertManager:
    _REPLACEMENT_PATTERN = re.compile(r"\{\{([a-zA-Z0-9_]+)\}\}")

    def __init__(
        self,
        client: IndiClient,
        *,
        state_dir: str | os.PathLike[str] | None = None,
        config_path: str | os.PathLike[str] | None = None,
    ) -> None:
        self._client = client
        self._state_lock = threading.RLock()
        self._state_dir = Path(state_dir) if state_dir is not None else Path(".runtime")
        self._store = HoldingAlertStateStore(self._state_dir)
        self._config_path = Path(config_path) if config_path is not None else self._default_config_path()
        self._config = self._load_config()
        self._state = self._store.load()
        self._normalize_pending_alert_state()
        self._dispatcher = CallbackDispatcher()
        self._closed = False
        self._rt_listener = self._on_rt_event
        self._rt_listener_registered = False
        self._rt_cache: dict[str, dict[str, dict[str, Any]]] = {"SC": {}, "UC": {}, "SH": {}}
        self._tr_cache: dict[str, dict[str, Any]] = {}
        self._raw_event_queue: list[tuple[dict[str, Any], str]] = []
        self._last_state_persist_monotonic = 0.0
        self._runner_threads: dict[str, threading.Thread] = {}
        self._runner_stops: dict[str, threading.Event] = {}
        self._owned_price_codes: dict[str, int] = {}
        self._expire_stale_runners()
        self._restore_runners()

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            stops = list(self._runner_stops.values())
            threads = list(self._runner_threads.values())
        for stop in stops:
            stop.set()
        for thread in threads:
            if threading.get_ident() != thread.ident:
                thread.join(timeout=2.0)
        if self._rt_listener_registered:
            try:
                self._client.unregister_rt_listener(self._rt_listener)
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"holding alert unregister RT listener failed: {exc}")
        for code in list(self._owned_price_codes):
            try:
                self._client.unsubscribe_realtime_price(code)
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"holding alert release price RT failed code={code}: {exc}")
        self._owned_price_codes.clear()
        self._dispatcher.wait_for_idle(timeout=5.0)
        self._dispatcher.close(timeout=5.0)

    def register_runner(
        self,
        account_no: str,
        http_callback: HttpCallbackSpec,
        held_code: Any = None,
        wanna_code: Any = None,
        code: Any = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        self._expire_stale_runners()
        if not account_no.strip():
            raise ValueError("account_no is required")
        if isinstance(held_code, bool):
            dry_run = held_code
            held_code = None
        if held_code is None and code is not None:
            held_code = code
        normalized_account = account_no.strip()
        held_codes = self._normalize_runner_codes(held_code)
        wanna_codes = self._normalize_runner_codes(wanna_code)
        self._validate_runner_code_sets(held_codes, wanna_codes)
        runner_id = f"holding_runner_{_kst_now().strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}"
        record = {
            "runner_id": runner_id,
            "account_no": normalized_account,
            "heldCode": held_codes,
            "wannaCode": wanna_codes,
            "httpCallback": http_callback.to_dict(),
            "dry_run": bool(dry_run),
            "registered_at": _kst_now().strftime("%Y%m%d%H%M%S"),
            "last_scan_at": None,
            "last_scan_result_count": 0,
            "active": True,
        }
        with self._state_lock:
            if self._active_runner_for_account_locked(normalized_account) is not None:
                raise ValueError("holding alert runner already registered for this account; cancel it before registering again")
        warnings = self._retain_runner_price_subscriptions(normalized_account, held_codes, wanna_codes)
        with self._state_lock:
            if self._active_runner_for_account_locked(normalized_account) is not None:
                raise ValueError("holding alert runner already registered for this account; cancel it before registering again")
            self._ensure_rt_listener_locked()
            self._state["runners"].append(record)
            self._persist_state_locked()
            self._start_runner_locked(record)
        return {
            "runner_id": runner_id,
            "accountNo": normalized_account,
            "heldCode": list(held_codes),
            "wannaCode": list(wanna_codes),
            "registered_at": record["registered_at"],
            "active": True,
            "warnings": warnings,
            "message": "holding alert runner registered",
        }

    def list_runners(self) -> list[dict[str, Any]]:
        self._expire_stale_runners()
        with self._state_lock:
            return [
                {
                    "runner_id": raw["runner_id"],
                    "accountNo": raw["account_no"],
                    "heldCode": self._runner_held_codes(raw),
                    "wannaCode": self._runner_wanna_codes(raw),
                    "active": bool(raw.get("active", True)),
                    "registered_at": raw.get("registered_at", ""),
                    "last_scan_at": raw.get("last_scan_at"),
                    "last_scan_result_count": int(raw.get("last_scan_result_count") or 0),
                    "httpCallback": copy.deepcopy(raw["httpCallback"]),
                }
                for raw in self._state["runners"]
            ]

    @staticmethod
    def _runner_held_codes(raw: dict[str, Any]) -> list[str]:
        return list(raw.get("heldCode") or raw.get("code") or [])

    @staticmethod
    def _runner_wanna_codes(raw: dict[str, Any]) -> list[str]:
        return list(raw.get("wannaCode") or [])

    @staticmethod
    def _validate_runner_code_sets(held_codes: list[str], wanna_codes: list[str]) -> None:
        overlap = set(held_codes) & set(wanna_codes)
        if overlap:
            joined = ", ".join(sorted(overlap))
            raise ValueError(f"heldCode and wannaCode must not contain the same stock code: {joined}")

    def _active_runner_for_account_locked(self, account_no: str) -> dict[str, Any] | None:
        return next(
            (
                raw
                for raw in self._state.get("runners", [])
                if raw.get("account_no") == account_no and raw.get("active", True)
            ),
            None,
        )

    def _normalize_runner_codes(self, code: Any = None) -> list[str]:
        if code is None:
            return []
        if isinstance(code, str):
            if code.strip().lower() in {"", "none", "null"}:
                return []
            raw_items = [code]
        else:
            raw_items = list(code or [])
        normalized: list[str] = []
        seen: set[str] = set()
        for item in raw_items:
            text = str(item or "").strip()
            if not text or text.lower() in {"none", "null"}:
                continue
            normalized_code = self._client.normalize_stock_code(text)
            if normalized_code in seen:
                continue
            seen.add(normalized_code)
            normalized.append(normalized_code)
        return normalized

    @staticmethod
    def _filter_balance_items(balances: list[BalanceItem], codes: list[str] | set[str]) -> list[BalanceItem]:
        code_set = set(codes or [])
        if not code_set:
            return list(balances)
        return [item for item in balances if item.code in code_set]

    def _filter_held_balance_items(
        self,
        balances: list[BalanceItem],
        held_codes: list[str] | set[str],
        wanna_codes: list[str] | set[str],
    ) -> list[BalanceItem]:
        filtered = self._filter_balance_items(balances, held_codes)
        wanna_set = set(wanna_codes or [])
        if not wanna_set:
            return filtered
        return [item for item in filtered if item.code not in wanna_set]

    def _runner_watch_config_for_scan(self, runner_id: str | None) -> tuple[list[str], list[str]]:
        if not runner_id:
            return [], []
        with self._state_lock:
            runner = next((raw for raw in self._state.get("runners", []) if raw.get("runner_id") == runner_id), None)
            return (self._runner_held_codes(runner), self._runner_wanna_codes(runner)) if runner else ([], [])

    def cancel_runner(self, runner_id: str) -> dict[str, Any]:
        self._expire_stale_runners()
        with self._state_lock:
            removed = [raw for raw in self._state["runners"] if raw.get("runner_id") == runner_id]
            self._state["runners"] = [
                raw for raw in self._state["runners"] if raw.get("runner_id") != runner_id
            ]
            stop = self._runner_stops.get(runner_id)
            thread = self._runner_threads.get(runner_id)
            if removed:
                self._persist_state_locked()
        if stop is not None:
            stop.set()
        if thread is not None and threading.get_ident() != thread.ident:
            thread.join(timeout=2.0)
        with self._state_lock:
            if thread is None or not thread.is_alive():
                self._runner_stops.pop(runner_id, None)
                self._runner_threads.pop(runner_id, None)
        if removed:
            self._release_unused_price_subscriptions()
            self._release_rt_listener_if_idle()
        return {
            "canceled": bool(removed),
            "removed_runners": len(removed),
            "runner_id": runner_id,
        }

    def refresh_decision_baselines(
        self,
        account_no: str | None = None,
        code: str | None = None,
        date: str | None = None,
    ) -> dict[str, Any]:
        targets = self._baseline_targets(account_no, code)
        trading_date = _normalize_date(date) or _kst_now().strftime("%Y%m%d")
        refreshed: list[dict[str, Any]] = []
        for target_code, target_name in targets:
            baseline = self._build_baseline(target_code, target_name, trading_date)
            with self._state_lock:
                self._state["baseline_cache"][target_code] = baseline
                self._persist_state_locked()
            refreshed.append(baseline)
        return {
            "trading_date": trading_date,
            "refreshed": refreshed,
            "count": len(refreshed),
        }

    def get_decision_baseline_cache(
        self,
        account_no: str | None = None,
        code: str | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if code:
            normalized_code = self._client.normalize_stock_code(code)
            with self._state_lock:
                cached = copy.deepcopy(self._state["baseline_cache"].get(normalized_code))
            return cached or self.refresh_decision_baselines(code=normalized_code)["refreshed"][0]
        if account_no:
            codes = [item.code for item in self._safe_get_balance(account_no)]
            with self._state_lock:
                cached_items = [
                    copy.deepcopy(self._state["baseline_cache"][item])
                    for item in codes
                    if item in self._state["baseline_cache"]
                ]
            if len(cached_items) == len(codes):
                return cached_items
            return self.refresh_decision_baselines(account_no=account_no)["refreshed"]
        with self._state_lock:
            return [copy.deepcopy(item) for item in self._state["baseline_cache"].values()]

    def get_cash_order_book_snapshot(self, code: str) -> dict[str, Any]:
        normalized_code = self._client.normalize_stock_code(code)
        cached = self._cached_order_book(normalized_code, max_age_seconds=self._order_book_ttl_seconds())
        if cached is not None and cached["status"] == "available":
            return cached
        key = f"order_book:{normalized_code}"
        hit, tr_cached, received_at = self._tr_cache_get(key)
        if hit:
            tr_cached["received_at"] = tr_cached.get("received_at") or received_at
            tr_cached["source"] = tr_cached.get("source") or "SH"
            return tr_cached
        try:
            order_book = self._client.get_cash_order_book_snapshot(normalized_code)
            snapshot = self._order_book_to_snapshot(order_book, status="available" if order_book.available else "unavailable")
            return self._tr_cache_set(key, snapshot, self._order_book_ttl_seconds())
        except Exception as exc:
            if cached is not None:
                cached["warning"] = str(exc)
                return cached
            if tr_cached is not None:
                tr_cached["status"] = "stale"
                tr_cached["warning"] = str(exc)
                return tr_cached
            return {
                "code": normalized_code,
                "received_at": "",
                "source": "SH",
                "market_phase": "unavailable",
                "levels": [],
                "status": "unavailable",
                "message": str(exc),
            }

    def get_alert_indicator_context(self, code: str, date: str | None = None) -> dict[str, Any]:
        normalized_code = self._client.normalize_stock_code(code)
        trading_date = _normalize_date(date) or _kst_now().strftime("%Y%m%d")
        intraday = self._safe_intraday_prices(normalized_code, trading_date)
        daily = self._safe_daily_prices(normalized_code, trading_date, lookback_days=300)
        quote = self._safe_quote_snapshot(normalized_code)
        stock_config = self._stock_config(normalized_code)
        vwap = self._vwap_context(intraday, daily)
        market = self._market_context_for_stock(stock_config, trading_date)
        sector = self._sector_context(normalized_code, stock_config, trading_date)
        relative_strength = self._relative_strength_context(daily, market)
        trading_value = self._trading_value_context(daily, quote)
        volume_5m = self._volume_5m_context(intraday)
        high_52w = self._high_52w_context(daily, quote)
        fx = self._fx_context(stock_config, trading_date)
        overseas = self._overseas_market_context(stock_config, trading_date)
        return {
            "vwap": vwap,
            "market": market,
            "sector": sector,
            "relative_strength": relative_strength,
            "trading_value": trading_value,
            "volume_5m": volume_5m,
            "high_52w": high_52w,
            "fx": fx,
            "overseas": overseas,
        }

    def calculate_trade_size(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = dict(payload or {})
        alert_type = str(payload.get("alert_type") or "관찰")
        scenario = str(payload.get("scenario") or "none") or "none"
        current_price = _float(payload.get("current_price")) or 0.0
        position = dict(payload.get("position") or {})
        account = dict(payload.get("account") or {})
        events = dict(payload.get("events") or {})
        data_status = dict(payload.get("data_status") or {})
        code = str(payload.get("code") or "")
        stock_config = self._stock_config(code) if code else self._default_stock_config()
        direction = self._direction_for_alert(alert_type)
        base_ratio = self._base_ratio(alert_type, scenario, stock_config)
        indicator_components = self._indicator_multiplier_components(payload, direction)
        indicator_multiplier = 1.0
        for component in indicator_components:
            indicator_multiplier *= float(component.get("multiplier") or 1.0)
        indicator_multiplier = self._cap_indicator_multiplier(indicator_multiplier, direction)
        technical_components = self._technical_deterioration_components(payload)
        technical_score = len([item for item in technical_components if item.get("active")])
        quantity = int(position.get("quantity") or 0)
        orderable_amount = int(account.get("orderable_amount") or account.get("cash") or 0)
        price_guide = self._price_guide(payload, direction, current_price)
        reference_price = float(price_guide.get("rounded_price") or price_guide.get("reference_price") or current_price or 0)
        calculated_qty = 0
        calculated_amount = 0
        if direction == "sell":
            sell_ratio = base_ratio * indicator_multiplier * self._technical_score_multiplier(technical_score)
            sell_cap = 1.0 if alert_type == "최종 방어선 검토" else 0.5
            sell_ratio = min(sell_ratio, sell_cap)
            calculated_qty = max(int(quantity * sell_ratio), 0)
            calculated_amount = int(calculated_qty * reference_price)
        elif direction == "buy" and reference_price > 0:
            buy_budget = max(max(orderable_amount - self._cash_buffer_amount(account), 0) * base_ratio * indicator_multiplier, 0)
            calculated_qty = int(buy_budget // reference_price)
            calculated_amount = int(calculated_qty * reference_price)

        restrictions: list[str] = []
        warnings: list[str] = []
        recommended_qty = calculated_qty
        if stock_config.get("observe_only") or not stock_config.get("validation_passed"):
            restrictions.append("관찰 전용")
            recommended_qty = 0
        if direction == "buy" and events.get("risk_event_flag"):
            restrictions.append("위험 이벤트 매수 금지")
            recommended_qty = 0
        if events.get("mechanical_event_flag"):
            warnings.append("기계적 가격 조정 이벤트")
            if direction == "buy":
                recommended_qty = 0
        balance_status = dict(data_status.get("balance") or {})
        account_status = dict(data_status.get("account_summary") or {})
        open_orders_status = dict(data_status.get("open_orders") or {})
        if balance_status.get("status") == "stale" and direction == "sell":
            restrictions.append("잔고 stale")
            recommended_qty = 0
        if account_status.get("status") == "stale" and direction == "buy":
            restrictions.append("계좌 요약 stale")
            recommended_qty = 0
        if open_orders_status.get("status") == "stale":
            warnings.append("미체결 조회 stale")
            recommended_qty = min(recommended_qty, calculated_qty // 2)
        if price_guide.get("status") == "stale":
            warnings.append("호가 stale")
        if payload.get("same_direction_open_order"):
            restrictions.append("같은 방향 미체결")
            recommended_qty = 0
        if direction == "buy":
            recommended_qty, limit_reason = self._apply_buy_position_limits(payload, recommended_qty, reference_price)
            if limit_reason:
                warnings.append(limit_reason)
            liquidity_qty = self._liquidity_limited_qty(payload, reference_price)
            if liquidity_qty is not None and recommended_qty > liquidity_qty:
                recommended_qty = max(liquidity_qty, 0)
                warnings.append("유동성 제한")
        recommended_amount = int(recommended_qty * reference_price)
        return {
            "alert_type": alert_type,
            "direction": direction,
            "scenario": scenario,
            "base_ratio": round(base_ratio, 4),
            "indicator_multiplier": round(indicator_multiplier, 4),
            "indicator_components": indicator_components,
            "technical_deterioration_score": technical_score,
            "technical_deterioration_components": technical_components,
            "calculated_qty": calculated_qty,
            "calculated_amount": calculated_amount,
            "recommended_qty": recommended_qty,
            "recommended_amount": recommended_amount,
            "price_guide": price_guide,
            "expected_position_weight_pct": self._expected_weight_pct(payload, direction, recommended_amount),
            "restriction": ", ".join(restrictions) if restrictions else "없음",
            "warning": ", ".join(warnings),
            "final_text": "자동 주문 아님. 수동 판단 필요.",
        }

    def run_scan(
        self,
        account_no: str,
        dry_run: bool = True,
        runner_id: str | None = None,
    ) -> dict[str, Any]:
        self._expire_stale_runners()
        started_at = _kst_now()
        self._flush_raw_event_queue()
        self._refresh_whipsaw_overrides(started_at)
        balances, balance_status = self._balance_snapshot(account_no)
        held_codes, wanna_codes = self._runner_watch_config_for_scan(runner_id)
        held_balances = self._filter_held_balance_items(balances, held_codes, wanna_codes)
        held_code_set = {item.code for item in balances}
        wanna_targets = [code for code in wanna_codes if code not in held_code_set]
        account_summary, account_status = self._account_summary_snapshot(account_no)
        open_orders, open_orders_status = self._open_orders_snapshot(account_no)
        results: list[dict[str, Any]] = []
        for item in held_balances:
            result = self._evaluate_holding(
                account_no,
                item,
                account_summary,
                open_orders,
                balance_status,
                account_status,
                open_orders_status,
                dry_run=dry_run,
            )
            results.append(result)
        for code in wanna_targets:
            result = self._evaluate_wanna_code(
                account_no,
                code,
                account_summary,
                open_orders,
                balance_status,
                account_status,
                open_orders_status,
                dry_run=dry_run,
            )
            results.append(result)
        dispatches: list[dict[str, Any]] = []
        if not dry_run:
            dispatches = self._dispatch_scan_results(account_no, results, runner_id)
        else:
            self._record_dry_run_alert_history(results)
        with self._state_lock:
            for raw in self._state["runners"]:
                if raw.get("account_no") == account_no and (runner_id is None or raw.get("runner_id") == runner_id):
                    raw["last_scan_at"] = started_at.strftime("%Y%m%d%H%M%S")
                    raw["last_scan_result_count"] = len(results)
            if not dry_run:
                self._persist_state_locked()
        return {
            "account_no": account_no,
            "dry_run": bool(dry_run),
            "scanned_at": started_at.strftime("%Y%m%d%H%M%S"),
            "result_count": len(results),
            "results": results,
            "dispatches": dispatches,
        }

    def run_validation(self, account_no: str, lookback_trading_days: int = 60) -> dict[str, Any]:
        balances = self._safe_get_balance(account_no)
        reports: list[dict[str, Any]] = []
        for item in balances:
            daily = self._safe_daily_prices(item.code, _kst_now().strftime("%Y%m%d"), lookback_days=lookback_trading_days + 80)
            recent = sorted(daily, key=lambda row: _normalize_date(row.date) or row.date)[-lookback_trading_days:]
            replay = self._replay_validation(item, daily, recent)
            baseline = self._build_baseline(item.code, item.name, _kst_now().strftime("%Y%m%d"), daily_prices=daily)
            weekly_average = replay["weekly_average_alerts"]
            whipsaw_rate = replay["whipsaw_rate"]
            passed = (
                replay["missing_intraday_days"] == 0
                and weekly_average <= 5
                and whipsaw_rate <= 30.0
                and replay["sell_30m_recoveries"] == 0
            )
            reports.append(
                {
                    "code": item.code,
                    "name": item.name,
                    "lookback_trading_days": lookback_trading_days,
                    "validation_method": "5m_replay",
                    "alert_days": replay["alert_days"],
                    "alert_count": replay["alert_count"],
                    "alert_type_counts": replay["alert_type_counts"],
                    "weekly_average_alerts": weekly_average,
                    "whipsaw_rate": whipsaw_rate,
                    "sell_30m_recoveries": replay["sell_30m_recoveries"],
                    "missing_intraday_days": replay["missing_intraday_days"],
                    "missing_intraday_dates": replay["missing_intraday_dates"],
                    "passed": passed,
                    "status": "passed" if passed else "observe_only",
                    "baseline": baseline,
                }
            )
        overall_passed = all(item["passed"] for item in reports) if reports else False
        with self._state_lock:
            self._state["validation"][account_no] = {
                "validated_at": _kst_now().strftime("%Y%m%d%H%M%S"),
                "lookback_trading_days": lookback_trading_days,
                "passed": overall_passed,
                "reports": reports,
            }
            self._persist_state_locked()
        return {
            "account_no": account_no,
            "lookback_trading_days": lookback_trading_days,
            "passed": overall_passed,
            "reports": reports,
        }

    def _replay_validation(
        self,
        item: BalanceItem,
        daily: list[DailyPrice],
        recent: list[DailyPrice],
    ) -> dict[str, Any]:
        ordered_daily = sorted(daily, key=lambda row: _normalize_date(row.date) or row.date)
        symbol_state: dict[str, Any] = {}
        events: list[dict[str, Any]] = []
        missing_dates: list[str] = []
        high_since_entry = float(item.current_price or 0)
        for row in recent:
            trading_date = _normalize_date(row.date) or row.date.replace("-", "")
            day_daily = [
                daily_row
                for daily_row in ordered_daily
                if (_normalize_date(daily_row.date) or daily_row.date.replace("-", "")) <= trading_date
            ]
            baseline = self._build_baseline(item.code, item.name, trading_date, daily_prices=day_daily)
            intraday = self._safe_intraday_prices(item.code, trading_date)
            if not intraday:
                missing_dates.append(trading_date)
                continue
            indicators = self._validation_indicator_context(item.code, trading_date, intraday, day_daily, row.close)
            for bar in sorted(intraday, key=lambda point: (point.date, point.time)):
                point_time = _intraday_point_datetime(bar)
                if point_time is None:
                    continue
                high_since_entry = max(high_since_entry, float(bar.high), float(bar.close))
                price = PriceInput(
                    current_price=float(bar.close),
                    status="available",
                    source="validation_5m",
                    received_at=point_time.strftime("%Y%m%d%H%M%S"),
                    age_seconds=0,
                )
                candidates = self._alert_candidates(item, baseline, price, indicators, symbol_state, high_since_entry, point_time)
                selected, _ = self._select_candidate(candidates)
                alert_type = str(selected.get("alert_type") or "")
                if alert_type not in {"관찰", "운영 안전 알림"}:
                    events.append(
                        {
                            "code": item.code,
                            "alert_type": alert_type,
                            "direction": self._direction_for_alert(alert_type),
                            "sent_at_dt": point_time,
                            "sent_at": point_time.strftime("%Y%m%d%H%M%S"),
                        }
                    )
                self._update_replay_symbol_state(symbol_state, price, baseline, selected, high_since_entry, point_time)
        alert_dates = {event["sent_at"][:8] for event in events}
        alert_type_counts: dict[str, int] = {}
        for event in events:
            alert_type_counts[event["alert_type"]] = alert_type_counts.get(event["alert_type"], 0) + 1
        whipsaws = 0
        sell_30m_recoveries = 0
        for index, event in enumerate(events):
            direction = event["direction"]
            if direction not in {"buy", "sell"}:
                continue
            opposite = "buy" if direction == "sell" else "sell"
            for other in events[index + 1 :]:
                elapsed = other["sent_at_dt"] - event["sent_at_dt"]
                if elapsed <= timedelta(0):
                    continue
                if elapsed > timedelta(hours=24):
                    break
                if other["direction"] == opposite:
                    whipsaws += 1
                    break
            if direction == "sell" and any(
                other["alert_type"] == "회복 판단" and timedelta(0) < other["sent_at_dt"] - event["sent_at_dt"] <= timedelta(minutes=30)
                for other in events[index + 1 :]
            ):
                sell_30m_recoveries += 1
        alert_count = len(events)
        return {
            "alert_days": len(alert_dates),
            "alert_count": alert_count,
            "alert_type_counts": alert_type_counts,
            "weekly_average_alerts": round(alert_count / max(len(recent) / 5.0, 1.0), 2),
            "whipsaw_rate": round((whipsaws / max(alert_count, 1)) * 100.0, 2),
            "sell_30m_recoveries": sell_30m_recoveries,
            "missing_intraday_days": len(missing_dates),
            "missing_intraday_dates": missing_dates,
        }

    def _validation_indicator_context(
        self,
        code: str,
        trading_date: str,
        intraday: list[IntradayPrice],
        daily: list[DailyPrice],
        close: int,
    ) -> dict[str, Any]:
        stock_config = self._stock_config(code)
        quote = {"current_price": close, "year_high": max([row.high for row in daily[-252:]], default=0)}
        market = self._market_context_for_stock(stock_config, trading_date)
        return {
            "vwap": self._vwap_context(intraday, daily),
            "market": market,
            "sector": self._sector_context(code, stock_config, trading_date),
            "relative_strength": self._relative_strength_context(daily, market),
            "trading_value": self._trading_value_context(daily, quote),
            "volume_5m": self._volume_5m_context(intraday),
            "high_52w": self._high_52w_context(daily, quote),
            "fx": self._fx_context(stock_config, trading_date),
            "overseas": self._overseas_market_context(stock_config, trading_date),
        }

    def _update_replay_symbol_state(
        self,
        symbol_state: dict[str, Any],
        price: PriceInput,
        baseline: dict[str, Any],
        selected: dict[str, Any],
        high_since_entry: float,
        now: datetime,
    ) -> None:
        current = price.current_price
        damage_line = float(baseline.get("damage_line") or 0)
        recovery_line = float(baseline.get("recovery_line") or 0)
        symbol_state["last_price"] = current
        symbol_state["last_eval_at"] = now.strftime("%Y%m%d%H%M%S")
        symbol_state["high_since_entry"] = max(float(symbol_state.get("high_since_entry") or 0), high_since_entry, current)
        if damage_line and current < damage_line:
            symbol_state.setdefault("damage_breach_since", now.strftime("%Y%m%d%H%M%S"))
            symbol_state.pop("recovery_since", None)
        elif recovery_line and current > recovery_line:
            symbol_state.setdefault("recovery_since", now.strftime("%Y%m%d%H%M%S"))
            symbol_state.pop("damage_breach_since", None)
        else:
            symbol_state.pop("damage_breach_since", None)
            symbol_state.pop("recovery_since", None)
        self._update_buy_timing_state(symbol_state, baseline, current, now)
        today = now.strftime("%Y%m%d")
        if damage_line and current < damage_line and (now.hour, now.minute) >= (15, 20):
            symbol_state["prior_close_damage"] = {
                "date": today,
                "close": current,
                "damage_line": damage_line,
                "recorded_at": now.strftime("%Y%m%d%H%M%S"),
            }
        prior_damage = dict(symbol_state.get("prior_close_damage") or {})
        prior_date = str(prior_damage.get("date") or "")
        if prior_date and prior_date < today and selected.get("alert_type") == "전일 종가 훼손 후속 판단":
            symbol_state[f"prior_close_followup_emitted_{today}"] = True
        symbol_state["last_alert_type"] = selected.get("alert_type")

    def _evaluate_holding(
        self,
        account_no: str,
        item: BalanceItem,
        account_summary: dict[str, Any],
        open_orders: list[dict[str, Any]],
        balance_status: dict[str, Any],
        account_status: dict[str, Any],
        open_orders_status: dict[str, Any],
        *,
        dry_run: bool,
    ) -> dict[str, Any]:
        now = _kst_now()
        baseline = self._baseline_for_holding(item)
        price = self._price_input(item.code)
        indicator_context = self.get_alert_indicator_context(item.code, now.strftime("%Y%m%d"))
        order_book = self.get_cash_order_book_snapshot(item.code)
        data_status = {
            "price": {
                "status": price.status,
                "source": price.source,
                "received_at": price.received_at,
                "age_seconds": price.age_seconds,
            },
            "order_book": {
                "status": order_book.get("status", "unavailable"),
                "source": order_book.get("source", "SH"),
                "received_at": order_book.get("received_at", ""),
            },
            "balance": balance_status,
            "account_summary": account_status,
            "open_orders": open_orders_status,
            "intraday_5m": {"status": indicator_context["vwap"]["status"], "received_at": now.strftime("%Y%m%d%H%M%S")},
            "market": {"status": "available" if indicator_context["market"]["source"] != "unavailable" else "unavailable"},
            "sector": {"status": "available" if indicator_context["sector"]["source"] != "unavailable" else "unavailable"},
        }
        symbol_state = self._symbol_state(item.code)
        high_since_entry = self._high_since_entry(item, symbol_state)
        candidates = self._alert_candidates(item, baseline, price, indicator_context, symbol_state, high_since_entry, now)
        selected, merged = self._select_candidate(candidates)
        stock_config = self._stock_config(item.code, item.name)
        events = self._event_flags(item.code)
        payload = {
            "category": "operational_safety" if selected["alert_type"] == "운영 안전 알림" else "trade_decision",
            "alert_type": selected["alert_type"],
            "priority": selected.get("priority"),
            "scenario": selected.get("scenario", "none"),
            "code": item.code,
            "name": item.name,
            "triggered_at": now.strftime("%Y%m%d%H%M%S"),
            "current_price": price.current_price,
            "baselines": baseline,
            "indicators": indicator_context,
            "position": {
                "account_no": account_no,
                "quantity": item.quantity,
                "avg_price": item.avg_price,
                "current_price": item.current_price,
                "high_since_entry": high_since_entry,
            },
            "account": account_summary,
            "events": events,
            "data_status": data_status,
            "order_book": order_book,
            "config": {
                "category": stock_config.get("category"),
                "observe_only": bool(stock_config.get("observe_only")),
                "validation_passed": bool(stock_config.get("validation_passed")),
                "source": stock_config.get("config_source", "configured"),
            },
            "reasons": selected.get("reasons", []),
            "merged_conditions": merged,
            "same_direction_open_order": self._has_same_direction_open_order(open_orders, item.code, selected["alert_type"]),
        }
        payload["trade_size"] = self.calculate_trade_size(payload)
        payload["text"] = self._format_alert_text(payload)
        self._update_symbol_state_after_eval(
            item.code,
            symbol_state,
            price,
            baseline,
            selected,
            high_since_entry,
            now,
            persist=not dry_run,
        )
        return payload

    def _evaluate_wanna_code(
        self,
        account_no: str,
        code: str,
        account_summary: dict[str, Any],
        open_orders: list[dict[str, Any]],
        balance_status: dict[str, Any],
        account_status: dict[str, Any],
        open_orders_status: dict[str, Any],
        *,
        dry_run: bool,
    ) -> dict[str, Any]:
        now = _kst_now()
        name = self._stock_name(code) or self._stock_config(code).get("name") or code
        item = BalanceItem(account_no, code, name, 0, 0, 0)
        baseline = self._baseline_for_holding(item)
        price = self._price_input(code)
        indicator_context = self.get_alert_indicator_context(code, now.strftime("%Y%m%d"))
        order_book = self.get_cash_order_book_snapshot(code)
        data_status = {
            "price": {
                "status": price.status,
                "source": price.source,
                "received_at": price.received_at,
                "age_seconds": price.age_seconds,
            },
            "order_book": {
                "status": order_book.get("status", "unavailable"),
                "source": order_book.get("source", "SH"),
                "received_at": order_book.get("received_at", ""),
            },
            "balance": balance_status,
            "account_summary": account_status,
            "open_orders": open_orders_status,
            "intraday_5m": {"status": indicator_context["vwap"]["status"], "received_at": now.strftime("%Y%m%d%H%M%S")},
            "market": {"status": "available" if indicator_context["market"]["source"] != "unavailable" else "unavailable"},
            "sector": {"status": "available" if indicator_context["sector"]["source"] != "unavailable" else "unavailable"},
        }
        symbol_state = self._symbol_state(code)
        candidates = self._wanna_alert_candidates(item, baseline, price, indicator_context, symbol_state, now)
        selected, merged = self._select_candidate(candidates)
        stock_config = self._stock_config(code, name)
        events = self._event_flags(code)
        payload = {
            "category": "operational_safety" if selected["alert_type"] == "운영 안전 알림" else "trade_decision",
            "alert_type": selected["alert_type"],
            "priority": selected.get("priority"),
            "scenario": selected.get("scenario", "none"),
            "code": code,
            "name": name,
            "watch_mode": "wanna",
            "triggered_at": now.strftime("%Y%m%d%H%M%S"),
            "current_price": price.current_price,
            "baselines": baseline,
            "indicators": indicator_context,
            "position": {
                "account_no": account_no,
                "status": "unheld",
                "quantity": 0,
                "avg_price": 0,
                "current_price": price.current_price,
                "high_since_entry": 0,
            },
            "account": account_summary,
            "events": events,
            "data_status": data_status,
            "order_book": order_book,
            "config": {
                "category": stock_config.get("category"),
                "observe_only": bool(stock_config.get("observe_only")),
                "validation_passed": bool(stock_config.get("validation_passed")),
                "source": stock_config.get("config_source", "configured"),
            },
            "reasons": selected.get("reasons", []),
            "merged_conditions": merged,
            "same_direction_open_order": self._has_same_direction_open_order(open_orders, code, selected["alert_type"]),
        }
        payload["trade_size"] = self.calculate_trade_size(payload)
        payload["text"] = self._format_alert_text(payload)
        self._update_wanna_state_after_eval(code, symbol_state, price, baseline, selected, now, persist=not dry_run)
        return payload

    def _alert_candidates(
        self,
        item: BalanceItem,
        baseline: dict[str, Any],
        price: PriceInput,
        indicators: dict[str, Any],
        symbol_state: dict[str, Any],
        high_since_entry: float,
        now: datetime,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        current = price.current_price
        if price.status == "unavailable":
            candidates.append(
                {
                    "alert_type": "운영 안전 알림",
                    "priority": None,
                    "scenario": "none",
                    "reasons": ["가격 데이터를 확보하지 못했습니다."],
                }
            )
            return candidates
        previous_price_status = str(symbol_state.get("last_price_status") or "")
        if price.status == "available" and previous_price_status in {"stale", "unavailable"}:
            return [
                {
                    "alert_type": "관찰",
                    "priority": None,
                    "scenario": "none",
                    "reasons": ["가격 데이터 복구 직후 5분 재관찰을 시작합니다."],
                }
            ]
        recovered_at = _parse_compact_time(str(symbol_state.get("price_recovered_at") or ""))
        if recovered_at and 0 <= (now - recovered_at).total_seconds() < 300:
            return [
                {
                    "alert_type": "관찰",
                    "priority": None,
                    "scenario": "none",
                    "reasons": ["가격 데이터 복구 후 5분 재관찰 구간입니다."],
                }
            ]
        damage_line = float(baseline.get("damage_line") or 0)
        recovery_line = float(baseline.get("recovery_line") or 0)
        second_support = dict(baseline.get("second_support") or {})
        second_low = float(second_support.get("low") or 0)
        atr = float(baseline.get("atr14") or 0)
        if price.status == "stale" and (price.age_seconds or 0) >= self._price_stale_seconds():
            candidates.append(
                {
                    "alert_type": "운영 안전 알림",
                    "priority": None,
                    "scenario": "none",
                    "reasons": [f"가격 RT가 {int(price.age_seconds or 0)}초 동안 갱신되지 않았습니다."],
                }
            )
        prior_damage = dict(symbol_state.get("prior_close_damage") or {})
        prior_date = str(prior_damage.get("date") or "")
        today = now.strftime("%Y%m%d")
        if prior_date and prior_date < today:
            prior_damage_line = _float(prior_damage.get("damage_line")) or damage_line
            emitted_key = f"prior_close_followup_emitted_{today}"
            if not symbol_state.get(emitted_key):
                if prior_damage_line and current <= prior_damage_line * 0.97 and now.hour == 9:
                    candidates.append(
                        self._trade_candidate(
                            "전일 종가 훼손 후속 판단",
                            ["전일 damage_line 하회 마감 뒤 시초가 3% 이상 갭하락입니다."],
                        )
                    )
                elif (now.hour, now.minute) >= (9, 30) and prior_damage_line and current < prior_damage_line:
                    candidates.append(
                        self._trade_candidate(
                            "전일 종가 훼손 후속 판단",
                            ["전일 damage_line 하회 마감 뒤 09:30 이후에도 훼손이 지속됩니다."],
                        )
                    )
        if second_low and current <= second_low:
            candidates.append(self._trade_candidate("최종 방어선 검토", ["2차 지지선 하단을 이탈했습니다."]))
        if high_since_entry > 0 and atr > 0 and current <= high_since_entry - (atr * 3.0):
            candidates.append(self._trade_candidate("이익 보호 알림", ["진입 이후 고점 대비 ATR x 3 이상 되돌림이 발생했습니다."]))
        if damage_line and current < damage_line:
            breach_since = _parse_compact_time(str(symbol_state.get("damage_breach_since") or ""))
            duration_minutes = ((now - breach_since).total_seconds() / 60.0) if breach_since else 0.0
            sell_judgment_minutes = self._sell_hold_minutes(item.code, "judgment", now)
            sell_caution_minutes = self._sell_hold_minutes(item.code, "caution", now)
            if duration_minutes >= sell_judgment_minutes:
                candidates.append(self._trade_candidate("매도 판단", [f"damage_line 하회가 {duration_minutes:.1f}분 유지됐습니다."]))
            elif duration_minutes >= sell_caution_minutes:
                candidates.append(self._trade_candidate("매도 주의", [f"damage_line 하회가 {duration_minutes:.1f}분 유지됐습니다."]))
        if damage_line and current < damage_line and (now.hour, now.minute) >= (15, 20):
            candidates.append(self._trade_candidate("종가 훼손", ["장마감 평가 구간에서 damage_line 아래입니다."]))
        if recovery_line and current > recovery_line:
            recovery_since = _parse_compact_time(str(symbol_state.get("recovery_since") or ""))
            if recovery_since and (now - recovery_since).total_seconds() >= 600:
                candidates.append(self._trade_candidate("회복 판단", ["recovery_line 상회가 10분 이상 유지됐습니다."]))
        buy_candidate = self._buy_candidate(item, baseline, current, indicators, symbol_state, now)
        if buy_candidate is not None:
            candidates.append(buy_candidate)
        if self._profit_taking_condition(baseline, indicators, current):
            candidates.append(self._trade_candidate("수익 실현 검토", ["Bollinger Upper 위 과열 조건입니다."]))
        if not candidates:
            candidates.append(
                {
                    "alert_type": "관찰",
                    "priority": None,
                    "scenario": "none",
                    "reasons": ["현재 tick에서는 매매 판단 알림 조건이 충족되지 않았습니다."],
                }
            )
        return candidates

    def _wanna_alert_candidates(
        self,
        item: BalanceItem,
        baseline: dict[str, Any],
        price: PriceInput,
        indicators: dict[str, Any],
        symbol_state: dict[str, Any],
        now: datetime,
    ) -> list[dict[str, Any]]:
        current = price.current_price
        if price.status == "unavailable":
            return [
                {
                    "alert_type": "운영 안전 알림",
                    "priority": None,
                    "scenario": "none",
                    "reasons": ["관심 매수 후보 가격 데이터를 확보하지 못했습니다."],
                }
            ]
        previous_price_status = str(symbol_state.get("last_price_status") or "")
        if price.status == "available" and previous_price_status in {"stale", "unavailable"}:
            return [
                {
                    "alert_type": "관찰",
                    "priority": None,
                    "scenario": "none",
                    "reasons": ["가격 데이터 복구 직후 5분 재관찰을 시작합니다."],
                }
            ]
        recovered_at = _parse_compact_time(str(symbol_state.get("price_recovered_at") or ""))
        if recovered_at and 0 <= (now - recovered_at).total_seconds() < 300:
            return [
                {
                    "alert_type": "관찰",
                    "priority": None,
                    "scenario": "none",
                    "reasons": ["가격 데이터 복구 후 5분 재관찰 구간입니다."],
                }
            ]
        candidates: list[dict[str, Any]] = []
        if price.status == "stale" and (price.age_seconds or 0) >= self._price_stale_seconds():
            candidates.append(
                {
                    "alert_type": "운영 안전 알림",
                    "priority": None,
                    "scenario": "none",
                    "reasons": [f"관심 매수 후보 가격 RT가 {int(price.age_seconds or 0)}초 동안 갱신되지 않았습니다."],
                }
            )
        buy_candidate = self._buy_candidate(item, baseline, current, indicators, symbol_state, now)
        if buy_candidate is not None:
            candidates.append(buy_candidate)
        if not candidates:
            candidates.append(
                {
                    "alert_type": "관찰",
                    "priority": None,
                    "scenario": "wanna",
                    "reasons": ["관심 매수 후보 조건이 충족되지 않았습니다."],
                }
            )
        return candidates

    def _select_candidate(self, candidates: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        operational = [item for item in candidates if item["alert_type"] == "운영 안전 알림"]
        if operational:
            return operational[0], [item for item in candidates if item is not operational[0]]
        ordered = sorted(candidates, key=lambda item: item.get("priority") or 999)
        selected = ordered[0]
        return selected, ordered[1:]

    def _dispatch_scan_results(
        self,
        account_no: str,
        results: list[dict[str, Any]],
        runner_id: str | None,
    ) -> list[dict[str, Any]]:
        runners = self._dispatch_target_runners(account_no, runner_id)
        dispatches: list[dict[str, Any]] = self._flush_due_alert_groups(account_no, runner_id)
        for payload in results:
            alert_type = str(payload.get("alert_type") or "")
            if alert_type == "관찰":
                continue
            if payload.get("config", {}).get("observe_only"):
                dispatches.append({"code": payload.get("code"), "alert_type": alert_type, "queued": False, "reason": "observe_only"})
                continue
            self._mark_symbol_fatigue(payload)
            if self._is_duplicate_suppressed(payload):
                dispatches.append({"code": payload.get("code"), "alert_type": alert_type, "queued": False, "reason": "duplicate_suppressed"})
                continue
            for runner in runners:
                if bool(runner.get("dry_run", True)):
                    dispatches.append({"runner_id": runner["runner_id"], "code": payload.get("code"), "alert_type": alert_type, "queued": False, "reason": "runner_dry_run"})
                    continue
                if self._daily_summary_required(payload):
                    dispatches.append(self._enqueue_alert_summary(account_no, runner, payload))
                    continue
                if alert_type in URGENT_ALERT_TYPES:
                    dispatches.append(self._dispatch_payload_to_runner(runner, payload, record_history=True))
                    continue
                dispatches.append(self._enqueue_alert_bundle(account_no, runner, payload))
        return dispatches

    def _format_alert_text(self, payload: dict[str, Any]) -> dict[str, str]:
        trade_size = dict(payload.get("trade_size") or {})
        baselines = dict(payload.get("baselines") or {})
        indicators = dict(payload.get("indicators") or {})
        data_status = dict(payload.get("data_status") or {})
        summary = self._public_alert_summary(payload)
        trade_price = self._trade_price(payload)
        calculated_qty = int(trade_size.get("calculated_qty") or 0)
        recommended_qty = int(trade_size.get("recommended_qty") or 0)
        lines = [
            "## 핵심 요약",
            f"- 알림: {payload.get('alert_type')}",
            f"- 현재가: {format_display_decimal(payload.get('current_price') or 0)}원",
            f"- 매매희망가: {self._format_price_text(trade_price)}",
            f"- 추천 수량: {format_display_decimal(recommended_qty)}주",
            f"- 근거: {', '.join(str(item) for item in payload.get('reasons') or [])}",
            "",
            "## 상세 지표",
            f"- damage_line: {format_display_decimal(baselines.get('damage_line') or 0)}",
            f"- recovery_line: {format_display_decimal(baselines.get('recovery_line') or 0)}",
            f"- VWAP: {indicators.get('vwap', {}).get('value') or 'unavailable'}",
            f"- 상대강도: {indicators.get('relative_strength', {}).get('status')}",
            f"- 시장: {self._format_indicator_change(indicators.get('market', {}))}",
            f"- 섹터: {self._format_indicator_change(indicators.get('sector', {}))}",
            f"- 환율: {self._format_indicator_change(indicators.get('fx', {}))}",
            f"- 해외지수: {self._format_indicator_change(indicators.get('overseas', {}))}",
            f"- 데이터 상태: {self._format_data_status(data_status)}",
            "",
            "## 매매 크기 산정",
            f"- 방향: {trade_size.get('direction')}",
            f"- 계산 수량: {format_display_decimal(calculated_qty)}주",
            f"- 추천 수량: {format_display_decimal(recommended_qty)}주",
            f"- 지표 가중치: {trade_size.get('indicator_multiplier')}",
            f"- 기술 악화 점수: {trade_size.get('technical_deterioration_score')}",
            f"- 제한: {trade_size.get('restriction')}",
            f"- 경고: {trade_size.get('warning') or '없음'}",
            "",
            "자동 주문 아님. 수동 판단 필요.",
        ]
        return {
            "summary": summary,
            "detail_markdown": "\n".join(lines),
        }

    def _public_alert_summary(self, payload: dict[str, Any]) -> str:
        trade_size = dict(payload.get("trade_size") or {})
        trade_price = self._trade_price(payload)
        recommended_qty = int(trade_size.get("recommended_qty") or 0)
        restriction = str(trade_size.get("restriction") or "없음").strip()
        parts = [
            f"{payload.get('alert_type')} | {payload.get('name')}({payload.get('code')})",
            f"매매희망가 {self._format_price_text(trade_price)}",
            f"추천 {format_display_decimal(recommended_qty)}주",
        ]
        if restriction and restriction != "없음":
            parts.append(f"제한: {restriction}")
        parts.append("자동 주문 아님")
        return " | ".join(parts)

    @staticmethod
    def _format_price_text(price: float) -> str:
        if price <= 0:
            return "unavailable"
        return f"{format_display_decimal(price)}원"

    @staticmethod
    def _trade_price(payload: dict[str, Any]) -> float:
        trade_size = dict(payload.get("trade_size") or {})
        price_guide = dict(trade_size.get("price_guide") or {})
        return float(
            _float(price_guide.get("rounded_price"))
            or _float(price_guide.get("reference_price"))
            or _float(payload.get("current_price"))
            or 0.0
        )

    def _on_rt_event(self, event: dict[str, Any]) -> None:
        rt_type = str(event.get("rt_type") or "")
        code = str(event.get("code") or "")
        if rt_type not in {"SC", "UC", "SH", "N0", "N2"}:
            return
        if rt_type in {"SC", "UC", "SH"} and not code:
            return
        received = _kst_now()
        copied = copy.deepcopy(event)
        acquired = self._state_lock.acquire(blocking=False)
        if not acquired:
            return
        try:
            if self._closed or not self._rt_listener_registered:
                return
            if rt_type in {"SC", "UC", "SH"} and code:
                self._rt_cache.setdefault(rt_type, {})[code] = {
                    "event": copied,
                    "received_at": received.strftime("%Y%m%d%H%M%S"),
                    "received_at_monotonic": time.monotonic(),
                }
                return
            if rt_type in {"N0", "N2"}:
                self._raw_event_queue.append((copied, received.strftime("%Y%m%d%H%M%S")))
        finally:
            self._state_lock.release()

    def _restore_runners(self) -> None:
        self._expire_stale_runners()
        with self._state_lock:
            runners = [raw for raw in self._state["runners"] if raw.get("active", True)]
            if runners:
                self._ensure_rt_listener_locked()
            for runner in runners:
                self._start_runner_locked(runner)
        for runner in runners:
            self._retain_runner_price_subscriptions(
                str(runner.get("account_no") or ""),
                self._runner_held_codes(runner),
                self._runner_wanna_codes(runner),
            )

    def _ensure_rt_listener_locked(self) -> None:
        if self._rt_listener_registered:
            return
        self._client.register_rt_listener(self._rt_listener)
        self._rt_listener_registered = True

    def _release_rt_listener_if_idle(self) -> None:
        with self._state_lock:
            active = any(raw.get("active", True) for raw in self._state["runners"])
            if active:
                return
            self._raw_event_queue.clear()
            if not self._rt_listener_registered:
                return
            try:
                self._client.unregister_rt_listener(self._rt_listener)
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"holding alert unregister idle RT listener failed: {exc}")
            else:
                self._rt_listener_registered = False

    def _start_runner_locked(self, record: dict[str, Any]) -> None:
        runner_id = str(record["runner_id"])
        if runner_id in self._runner_threads:
            return
        stop = threading.Event()
        thread = threading.Thread(
            target=self._runner_loop,
            args=(runner_id, stop),
            name=f"homestock-holding-alert-{runner_id[-8:]}",
            daemon=True,
        )
        self._runner_stops[runner_id] = stop
        self._runner_threads[runner_id] = thread
        thread.start()

    def _runner_loop(self, runner_id: str, stop: threading.Event) -> None:
        interval = max(float(self._config.get("tick_interval_seconds") or 5), 1.0)
        while not stop.wait(interval):
            if self._expire_stale_runners() and not self._runner_exists(runner_id):
                return
            with self._state_lock:
                runner = next((raw for raw in self._state["runners"] if raw.get("runner_id") == runner_id), None)
            if runner is None or not bool(runner.get("active", True)):
                return
            try:
                self.run_scan(str(runner["account_no"]), dry_run=bool(runner.get("dry_run", True)), runner_id=runner_id)
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"holding alert runner failed runner_id={runner_id}: {exc.__class__.__name__}: {exc}")

    def _runner_exists(self, runner_id: str) -> bool:
        with self._state_lock:
            return any(raw.get("runner_id") == runner_id for raw in self._state.get("runners", []))

    def _expire_stale_runners(self) -> int:
        current_date = _kst_now().strftime("%Y%m%d")
        with self._state_lock:
            removed = self._remove_stale_runners_locked(current_date)
            if not removed:
                return 0
            removed_ids = {str(raw.get("runner_id") or "") for raw in removed if raw.get("runner_id")}
            self._remove_pending_alert_groups_for_runner_ids_locked(removed_ids)
            thread_pairs = [
                (runner_id, self._runner_stops.get(runner_id), self._runner_threads.get(runner_id))
                for runner_id in removed_ids
            ]
            self._persist_state_locked()

        for _, stop, _ in thread_pairs:
            if stop is not None:
                stop.set()
        for _, _, thread in thread_pairs:
            if thread is not None and threading.get_ident() != thread.ident:
                thread.join(timeout=2.0)
        with self._state_lock:
            for runner_id, _, thread in thread_pairs:
                if thread is None or not thread.is_alive():
                    self._runner_stops.pop(runner_id, None)
                    self._runner_threads.pop(runner_id, None)
        self._release_unused_price_subscriptions()
        self._release_rt_listener_if_idle()
        return len(removed)

    def _remove_stale_runners_locked(self, current_date: str) -> list[dict[str, Any]]:
        removed: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for raw in self._state.get("runners", []):
            if self._runner_registered_date(raw) == current_date:
                retained.append(raw)
            else:
                removed.append(raw)
        self._state["runners"] = retained
        return removed

    @staticmethod
    def _runner_registered_date(raw: dict[str, Any]) -> str:
        registered_at = str(raw.get("registered_at") or "")
        return registered_at[:8] if len(registered_at) >= 8 and registered_at[:8].isdigit() else ""

    def _remove_pending_alert_groups_for_runner_ids_locked(self, runner_ids: set[str]) -> None:
        if not runner_ids:
            return
        for key in ("pending_alert_bundles", "pending_alert_summaries"):
            groups = self._state.setdefault(key, {})
            if not isinstance(groups, dict):
                self._state[key] = {}
                continue
            self._state[key] = {
                group_key: group
                for group_key, group in groups.items()
                if str((group or {}).get("runner_id") or "") not in runner_ids
            }

    def _baseline_targets(self, account_no: str | None, code: str | None) -> list[tuple[str, str]]:
        if code:
            normalized_code = self._client.normalize_stock_code(code)
            return [(normalized_code, self._stock_name(normalized_code))]
        if account_no:
            return [(item.code, item.name) for item in self._safe_get_balance(account_no)]
        raise ValueError("account_no or code is required")

    def _build_baseline(
        self,
        code: str,
        name: str,
        trading_date: str,
        daily_prices: list[DailyPrice] | None = None,
    ) -> dict[str, Any]:
        prices = daily_prices if daily_prices is not None else self._safe_daily_prices(code, trading_date, lookback_days=260)
        stock_config = self._stock_config(code, name)
        if not prices:
            return self._unavailable_baseline(code, name, trading_date, stock_config, "daily price unavailable")
        ordered = sorted(prices, key=lambda item: _normalize_date(item.date) or item.date)
        indicators = build_technical_indicators(ordered)
        latest_indicator = indicators[0] if indicators else {}
        latest_price = ordered[-1]
        close = float(latest_price.close)
        atr = _float(latest_indicator.get("atr")) or self._fallback_atr(ordered) or max(close * 0.03, 1.0)
        atr_multiplier = self._adjusted_atr_multiplier(code, float(stock_config.get("atr_multiplier") or 2.0))
        tolerance_pct = float(stock_config.get("support_tolerance_pct") or 0.5)
        recent_low20 = min(float(item.low) for item in ordered[-20:])
        recent_low60 = min(float(item.low) for item in ordered[-60:])
        chandelier = _float(latest_indicator.get("chandelier_exit_long"))
        if chandelier is None:
            chandelier = max(float(item.high) for item in ordered[-14:]) - (atr * atr_multiplier)
        bollinger_lower = _float(latest_indicator.get("bollinger_lower")) or (close - (atr * atr_multiplier))
        bollinger_lower_adjusted = bollinger_lower * (1.0 - tolerance_pct / 100.0)
        damage_line = round_price_down(max(chandelier, recent_low20, bollinger_lower_adjusted))
        first_support = self._support_zone(
            [
                _float(latest_indicator.get("sma")),
                _float(latest_indicator.get("ema")),
            ],
            tolerance_pct,
            ["sma20", "ema20"],
        )
        second_support = self._support_zone([recent_low60], tolerance_pct, ["recent_low60"])
        recovery_line = round_price_up(max(damage_line + (atr * 0.25), damage_line * 1.01))
        first_reclaim = round_price_up(max(first_support["high"], close * 1.002) if first_support["high"] else close * 1.002)
        second_reclaim = round_price_up(max(second_support["high"], close * 1.004) if second_support["high"] else close * 1.004)
        trend_reclaim = round_price_up(max(_float(latest_indicator.get("ema")) or close, _float(latest_indicator.get("sma")) or close))
        return {
            "code": code,
            "name": name or stock_config.get("name", ""),
            "trading_date": trading_date,
            "generated_at": _kst_now().strftime("%Y%m%d%H%M%S"),
            "category": stock_config.get("category", "대형주"),
            "atr_multiplier": atr_multiplier,
            "support_tolerance_pct": tolerance_pct,
            "atr14": round(atr, 4),
            "latest_close": int(close),
            "damage_line": damage_line,
            "damage_line_components": {
                "chandelier_line": round_price_down(chandelier),
                "recent_low20": round_price_down(recent_low20),
                "bollinger_lower_adjusted": round_price_down(bollinger_lower_adjusted),
            },
            "first_support": first_support,
            "second_support": second_support,
            "recovery_line": recovery_line,
            "buy_reclaim_lines": {
                "first_rebound": first_reclaim,
                "second_rebound": second_reclaim,
                "trend_reclaim": trend_reclaim,
            },
            "daily_indicators": {
                "rsi": latest_indicator.get("rsi"),
                "mfi": latest_indicator.get("mfi"),
                "bollinger_upper": latest_indicator.get("bollinger_upper"),
                "bollinger_lower": latest_indicator.get("bollinger_lower"),
                "adx": latest_indicator.get("adx"),
                "plus_di": latest_indicator.get("plus_di"),
                "minus_di": latest_indicator.get("minus_di"),
                "macd": latest_indicator.get("macd"),
                "macd_signal": latest_indicator.get("macd_signal"),
                "macd_histogram": latest_indicator.get("macd_histogram"),
                "obv": latest_indicator.get("obv"),
                "obv_sma": latest_indicator.get("obv_sma"),
            },
            "status": "available",
        }

    def _baseline_for_holding(self, item: BalanceItem) -> dict[str, Any]:
        with self._state_lock:
            cached = copy.deepcopy(self._state["baseline_cache"].get(item.code))
        if cached and cached.get("status") == "available":
            return cached
        return self.refresh_decision_baselines(code=item.code)["refreshed"][0]

    def _price_input(self, code: str) -> PriceInput:
        fallback: PriceInput | None = None
        for rt_type in ("UC", "SC"):
            with self._state_lock:
                cached = copy.deepcopy(self._rt_cache.get(rt_type, {}).get(code))
            if cached:
                age = max(time.monotonic() - float(cached.get("received_at_monotonic") or 0), 0.0)
                event = dict(cached.get("event") or {})
                status = "available" if age <= self._price_stale_seconds() else "stale"
                current = _float(event.get("current_price")) or 0.0
                if current > 0:
                    candidate = PriceInput(
                        current_price=current,
                        status=status,
                        source=rt_type,
                        received_at=str(cached.get("received_at") or ""),
                        age_seconds=age,
                    )
                    if status == "available":
                        return candidate
                    if fallback is None:
                        fallback = candidate
        if fallback is not None:
            return fallback
        try:
            quote = self._safe_quote_snapshot(code)
            current_price = float(quote.get("current_price") or 0)
            if current_price <= 0:
                raise ValueError("quote current_price unavailable")
            return PriceInput(
                current_price=current_price,
                status="available",
                source="quote_snapshot",
                received_at=_kst_now().strftime("%Y%m%d%H%M%S"),
                age_seconds=None,
            )
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert quote fallback failed code={code}: {exc}")
            return PriceInput(
                current_price=0.0,
                status="unavailable",
                source="unavailable",
                received_at="",
                age_seconds=None,
            )

    def _cached_order_book(self, code: str, max_age_seconds: float | None = None) -> dict[str, Any] | None:
        with self._state_lock:
            cached = copy.deepcopy(self._rt_cache.get("SH", {}).get(code))
        if not cached:
            return None
        age = max(time.monotonic() - float(cached.get("received_at_monotonic") or 0), 0.0)
        event = dict(cached.get("event") or {})
        freshness_limit = float(max_age_seconds if max_age_seconds is not None else self._order_book_stale_seconds())
        status = "available" if age <= freshness_limit else "stale"
        return {
            "code": code,
            "received_at": str(event.get("received_at") or event.get("time") or ""),
            "source": event.get("source") or "SH",
            "market_phase": event.get("market_phase") or "unknown",
            "levels": list(event.get("levels") or []),
            "status": status,
            "age_seconds": age,
        }

    def _stock_config(self, code: str, name: str | None = None) -> dict[str, Any]:
        normalized = str(code or "").strip()
        defaults = self._default_stock_config()
        stock = dict((self._config.get("stocks") or {}).get(normalized) or {})
        if not stock:
            stock = {"name": name or self._stock_name(normalized), "config_source": "default_profile"}
        category_name = stock.get("category") or defaults.get("category") or "대형주"
        category = dict((self._config.get("categories") or {}).get(category_name) or {})
        merged = {**defaults, **category, **stock}
        merged["code"] = normalized
        merged["name"] = merged.get("name") or name or self._stock_name(normalized)
        merged["category"] = category_name
        merged["config_source"] = stock.get("config_source") or ("configured" if normalized in (self._config.get("stocks") or {}) else "default_profile")
        return merged

    def _default_stock_config(self) -> dict[str, Any]:
        return dict(
            self._config.get("default_profile")
            or {
                "category": "대형주",
                "atr_multiplier": 2.0,
                "support_tolerance_pct": 0.5,
                "observe_only": True,
                "validation_passed": False,
            }
        )

    def _event_flags(self, code: str) -> dict[str, Any]:
        risk_keywords = [str(item) for item in self._config.get("risk_keywords", [])]
        mechanical_keywords = [str(item) for item in self._config.get("mechanical_keywords", [])]
        today = _kst_now().strftime("%Y%m%d")
        with self._state_lock:
            events = [dict(item) for item in self._state["raw_events"]]
        matched_risk = []
        matched_mechanical = []
        for event in events:
            if event.get("code") not in {code, "", None}:
                continue
            if event.get("deleted"):
                continue
            event_date = str(event.get("date") or "")
            if event_date and event_date != today:
                continue
            title = str(event.get("title") or "")
            matched_risk.extend([keyword for keyword in risk_keywords if keyword and keyword in title])
            matched_mechanical.extend([keyword for keyword in mechanical_keywords if keyword and keyword in title])
        return {
            "risk_event_flag": bool(matched_risk),
            "risk_keywords": sorted(set(matched_risk)),
            "mechanical_event_flag": bool(matched_mechanical),
            "mechanical_keywords": sorted(set(matched_mechanical)),
        }

    def _flush_raw_event_queue(self) -> None:
        with self._state_lock:
            queued = list(self._raw_event_queue)
            self._raw_event_queue.clear()
            if not queued:
                return
            for event, received_at in queued:
                self._record_raw_event_locked(event, received_at)
            self._persist_state_locked()

    def _record_raw_event_locked(self, event: dict[str, Any], received_at: str) -> None:
        article_id = str(event.get("article_id") or "")
        rt_type = str(event.get("rt_type") or "")
        delete_flag = str(event.get("deleted_flag") or "").upper()
        record = {
            "rt_type": rt_type,
            "news_type": str(event.get("news_type") or ""),
            "date": str(event.get("date") or ""),
            "time": str(event.get("time") or ""),
            "article_id": article_id,
            "code": str(event.get("code") or ""),
            "title": str(event.get("title") or ""),
            "deleted": delete_flag == "D",
            "received_at": received_at,
        }
        replaced = False
        if article_id:
            for index, raw in enumerate(self._state["raw_events"]):
                if raw.get("rt_type") == rt_type and raw.get("article_id") == article_id:
                    self._state["raw_events"][index] = record
                    replaced = True
                    break
        if not replaced:
            self._state["raw_events"].append(record)
        self._state["raw_events"] = self._state["raw_events"][-500:]

    def _symbol_state(self, code: str) -> dict[str, Any]:
        with self._state_lock:
            return copy.deepcopy(self._state["symbol_state"].get(code) or {})

    def _update_symbol_state_after_eval(
        self,
        code: str,
        symbol_state: dict[str, Any],
        price: PriceInput,
        baseline: dict[str, Any],
        selected: dict[str, Any],
        high_since_entry: float,
        now: datetime,
        *,
        persist: bool,
    ) -> None:
        current = price.current_price
        damage_line = float(baseline.get("damage_line") or 0)
        recovery_line = float(baseline.get("recovery_line") or 0)
        updated = dict(symbol_state)
        updated["last_price"] = current
        updated["last_eval_at"] = now.strftime("%Y%m%d%H%M%S")
        updated["high_since_entry"] = max(float(updated.get("high_since_entry") or 0), high_since_entry, current)
        previous_status = str(updated.get("last_price_status") or "")
        updated["last_price_status"] = price.status
        if price.status in {"stale", "unavailable"}:
            updated.setdefault("price_unavailable_since", now.strftime("%Y%m%d%H%M%S"))
        else:
            if previous_status in {"stale", "unavailable"}:
                updated["price_recovered_at"] = now.strftime("%Y%m%d%H%M%S")
            updated.pop("price_unavailable_since", None)
        if damage_line and current < damage_line:
            updated.setdefault("damage_breach_since", now.strftime("%Y%m%d%H%M%S"))
            updated.pop("recovery_since", None)
        elif recovery_line and current > recovery_line:
            updated.setdefault("recovery_since", now.strftime("%Y%m%d%H%M%S"))
            updated.pop("damage_breach_since", None)
        else:
            updated.pop("damage_breach_since", None)
            updated.pop("recovery_since", None)
        self._update_buy_timing_state(updated, baseline, current, now)
        today = now.strftime("%Y%m%d")
        if damage_line and current < damage_line and (now.hour, now.minute) >= (15, 20):
            updated["prior_close_damage"] = {
                "date": today,
                "close": current,
                "damage_line": damage_line,
                "recorded_at": now.strftime("%Y%m%d%H%M%S"),
            }
        prior_damage = dict(updated.get("prior_close_damage") or {})
        prior_date = str(prior_damage.get("date") or "")
        if (
            prior_date
            and prior_date < today
            and selected.get("alert_type") == "전일 종가 훼손 후속 판단"
        ):
            updated[f"prior_close_followup_emitted_{today}"] = True
        if prior_date and prior_date < today and recovery_line and current > recovery_line and (now.hour, now.minute) >= (9, 30):
            updated.pop("prior_close_damage", None)
        updated["last_alert_type"] = selected.get("alert_type")
        with self._state_lock:
            self._state["symbol_state"][code] = updated
            if persist:
                self._persist_state_locked()

    def _update_wanna_state_after_eval(
        self,
        code: str,
        symbol_state: dict[str, Any],
        price: PriceInput,
        baseline: dict[str, Any],
        selected: dict[str, Any],
        now: datetime,
        *,
        persist: bool,
    ) -> None:
        current = price.current_price
        updated = dict(symbol_state)
        updated["watch_mode"] = "wanna"
        updated["active_position"] = False
        updated["last_price"] = current
        updated["last_eval_at"] = now.strftime("%Y%m%d%H%M%S")
        previous_status = str(updated.get("last_price_status") or "")
        updated["last_price_status"] = price.status
        if price.status in {"stale", "unavailable"}:
            updated.setdefault("price_unavailable_since", now.strftime("%Y%m%d%H%M%S"))
        else:
            if previous_status in {"stale", "unavailable"}:
                updated["price_recovered_at"] = now.strftime("%Y%m%d%H%M%S")
            updated.pop("price_unavailable_since", None)
        for key in ("damage_breach_since", "recovery_since", "prior_close_damage"):
            updated.pop(key, None)
        self._update_buy_timing_state(updated, baseline, current, now)
        updated["last_alert_type"] = selected.get("alert_type")
        with self._state_lock:
            self._state["symbol_state"][code] = updated
            if persist:
                self._persist_state_locked()

    def _update_buy_timing_state(
        self,
        symbol_state: dict[str, Any],
        baseline: dict[str, Any],
        current: float,
        now: datetime,
    ) -> None:
        reclaim = dict(baseline.get("buy_reclaim_lines") or {})
        for label, support_key, reclaim_key in (
            ("first", "first_support", "first_rebound"),
            ("second", "second_support", "second_rebound"),
        ):
            support = dict(baseline.get(support_key) or {})
            low = _float(support.get("low")) or 0.0
            high = _float(support.get("high")) or 0.0
            touch_key = f"{label}_support_touched_at"
            reclaim_since_key = f"{label}_reclaim_since"
            touched_at = _parse_compact_time(str(symbol_state.get(touch_key) or ""))
            if low and high and low <= current <= high:
                symbol_state[touch_key] = now.strftime("%Y%m%d%H%M%S")
                touched_at = now
            if touched_at and (now - touched_at).total_seconds() > 1800:
                symbol_state.pop(touch_key, None)
                symbol_state.pop(reclaim_since_key, None)
                continue
            reclaim_price = _float(reclaim.get(reclaim_key)) or 0.0
            if touched_at and reclaim_price and current >= reclaim_price:
                symbol_state.setdefault(reclaim_since_key, now.strftime("%Y%m%d%H%M%S"))
            else:
                symbol_state.pop(reclaim_since_key, None)
        trend_reclaim = _float(reclaim.get("trend_reclaim")) or 0.0
        if trend_reclaim and current >= trend_reclaim:
            symbol_state.setdefault("trend_reclaim_since", now.strftime("%Y%m%d%H%M%S"))
        else:
            symbol_state.pop("trend_reclaim_since", None)

    def _support_reclaim_ready(self, symbol_state: dict[str, Any], label: str, now: datetime) -> bool:
        touched_at = _parse_compact_time(str(symbol_state.get(f"{label}_support_touched_at") or ""))
        reclaim_since = _parse_compact_time(str(symbol_state.get(f"{label}_reclaim_since") or ""))
        if not touched_at or not reclaim_since:
            return False
        if (now - touched_at).total_seconds() > 1800:
            return False
        return (now - reclaim_since).total_seconds() >= 600

    def _high_since_entry(self, item: BalanceItem, symbol_state: dict[str, Any]) -> float:
        state_value = _float(symbol_state.get("high_since_entry"))
        if state_value:
            return max(state_value, float(item.current_price or 0))
        trading_date = _kst_now().strftime("%Y%m%d")
        daily = self._safe_daily_prices(item.code, trading_date, lookback_days=80)
        fallback = max([float(row.high) for row in daily[-60:]], default=float(item.current_price or 0))
        return fallback

    def _buy_candidate(
        self,
        item: BalanceItem,
        baseline: dict[str, Any],
        current: float,
        indicators: dict[str, Any],
        symbol_state: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any] | None:
        if current <= 0:
            return None
        reclaim = dict(baseline.get("buy_reclaim_lines") or {})
        if self._support_reclaim_ready(symbol_state, "first", now) and current >= float(reclaim.get("first_rebound") or 0):
            return self._trade_candidate("매수 판단", ["1차 지지권 접촉 후 반등 후보입니다."], scenario="1차 반등형")
        if self._support_reclaim_ready(symbol_state, "second", now) and current >= float(reclaim.get("second_rebound") or 0):
            return self._trade_candidate("매수 판단", ["2차 지지권 접촉 후 반등 후보입니다."], scenario="2차 반등형")
        vwap_value = _float(indicators.get("vwap", {}).get("value"))
        trend_reclaim = _float(reclaim.get("trend_reclaim"))
        trend_since = _parse_compact_time(str(symbol_state.get("trend_reclaim_since") or ""))
        if (
            vwap_value
            and trend_reclaim
            and current >= max(vwap_value, trend_reclaim)
            and trend_since
            and (now - trend_since).total_seconds() >= 600
        ):
            return self._trade_candidate("매수 판단", ["VWAP과 trend reclaim line을 함께 회복했습니다."], scenario="추세복귀 확인형")
        return None

    def _profit_taking_condition(self, baseline: dict[str, Any], indicators: dict[str, Any], current: float) -> bool:
        daily = dict(baseline.get("daily_indicators") or {})
        upper = _float(daily.get("bollinger_upper"))
        rsi = _float(daily.get("rsi"))
        mfi = _float(daily.get("mfi"))
        if not upper or current <= upper:
            return False
        return (rsi is not None and rsi >= 75) or (mfi is not None and mfi >= 80)

    def _trade_candidate(self, alert_type: str, reasons: list[str], scenario: str = "none") -> dict[str, Any]:
        return {
            "alert_type": alert_type,
            "priority": ALERT_PRIORITIES[alert_type],
            "scenario": scenario,
            "reasons": reasons,
        }

    def _direction_for_alert(self, alert_type: str) -> str:
        if alert_type in {"매도 판단", "매도 주의", "종가 훼손", "전일 종가 훼손 후속 판단", "최종 방어선 검토", "수익 실현 검토", "이익 보호 알림"}:
            return "sell"
        if alert_type == "매수 판단":
            return "buy"
        return "observe"

    def _base_ratio(self, alert_type: str, scenario: str, stock_config: dict[str, Any]) -> float:
        if alert_type == "매도 주의":
            return 0.3
        if alert_type in {"매도 판단", "이익 보호 알림", "수익 실현 검토"}:
            return 0.3
        if alert_type in {"종가 훼손", "전일 종가 훼손 후속 판단"}:
            return 0.5
        if alert_type == "최종 방어선 검토":
            return 1.0
        if alert_type == "매수 판단":
            ratios = dict(stock_config.get("buy_base_ratios") or {})
            return float(ratios.get(scenario, 0.2))
        return 0.0

    def _indicator_multiplier(self, payload: dict[str, Any], direction: str) -> float:
        components = self._indicator_multiplier_components(payload, direction)
        multiplier = 1.0
        for item in components:
            multiplier *= float(item.get("multiplier") or 1.0)
        return self._cap_indicator_multiplier(multiplier, direction)

    @staticmethod
    def _cap_indicator_multiplier(multiplier: float, direction: str) -> float:
        upper = 1.0 if direction == "buy" else 2.0
        return max(min(multiplier, upper), 0.0)

    def _indicator_multiplier_components(self, payload: dict[str, Any], direction: str) -> list[dict[str, Any]]:
        indicators = dict(payload.get("indicators") or {})
        events = dict(payload.get("events") or {})
        current = _float(payload.get("current_price")) or 0.0
        components: list[dict[str, Any]] = []

        vwap = dict(indicators.get("vwap") or {})
        vwap_value = _float(vwap.get("value"))
        if vwap.get("status") == "available" and vwap_value:
            below_vwap = current > 0 and current < vwap_value
            components.append(
                {
                    "name": "vwap",
                    "status": "below" if below_vwap else "above",
                    "multiplier": 0.9 if direction == "buy" and below_vwap else 1.1 if direction == "sell" and below_vwap else 1.0,
                }
            )
        else:
            components.append({"name": "vwap", "status": vwap.get("status") or "unavailable", "multiplier": 0.95 if direction == "buy" else 1.0})

        market = dict(indicators.get("market") or {})
        market_change = _float(market.get("change_pct"))
        if market.get("source") in {"omitted_overseas_etf", "unavailable"} or market_change is None:
            components.append({"name": "market", "status": market.get("source") or "unavailable", "multiplier": 1.0})
        else:
            weak_market = market_change <= -1.0
            strong_market = market_change >= 1.0
            components.append(
                {
                    "name": "market",
                    "status": "weak" if weak_market else "strong" if strong_market else "neutral",
                    "multiplier": 0.9 if direction == "buy" and weak_market else 1.1 if direction == "sell" and weak_market else 1.05 if direction == "buy" and strong_market else 1.0,
                }
            )

        sector = dict(indicators.get("sector") or {})
        sector_change = _float(sector.get("change_pct"))
        if sector.get("source") == "unavailable" or sector_change is None:
            components.append({"name": "sector", "status": "unavailable", "multiplier": 1.0})
        else:
            weak_sector = sector_change <= -1.0
            strong_sector = sector_change >= 1.0
            components.append(
                {
                    "name": "sector",
                    "status": "weak" if weak_sector else "strong" if strong_sector else "neutral",
                    "multiplier": 0.9 if direction == "buy" and weak_sector else 1.1 if direction == "sell" and weak_sector else 1.05 if direction == "buy" and strong_sector else 1.0,
                }
            )

        relative_strength = dict(indicators.get("relative_strength") or {})
        rs_status = str(relative_strength.get("status") or "unavailable")
        components.append(
            {
                "name": "relative_strength",
                "status": rs_status,
                "multiplier": 0.7 if direction == "buy" and rs_status == "weak" else 1.2 if direction == "sell" and rs_status == "weak" else 1.1 if direction == "buy" and rs_status == "strong" else 0.95 if direction == "sell" and rs_status == "strong" else 1.0,
            }
        )

        trading_value = dict(indicators.get("trading_value") or {})
        trading_value_ratio = _float(trading_value.get("surge_ratio")) or 0.0
        components.append(
            {
                "name": "trading_value",
                "status": "surge" if trading_value_ratio >= 2.0 else "normal",
                "multiplier": 1.1 if direction == "sell" and trading_value_ratio >= 2.0 else 1.05 if direction == "buy" and 1.0 <= trading_value_ratio < 2.0 else 1.0,
            }
        )

        volume_5m = dict(indicators.get("volume_5m") or {})
        volume_ratio = _float(volume_5m.get("ratio")) or 0.0
        if volume_5m.get("status") == "unavailable":
            volume_multiplier = 0.8 if direction == "buy" else 1.0
            volume_status = "unavailable"
        elif volume_ratio >= 2.0:
            volume_multiplier = 1.1 if direction == "sell" else 1.05
            volume_status = "surge"
        elif volume_ratio < 0.5:
            volume_multiplier = 0.9 if direction == "buy" else 1.0
            volume_status = "low"
        else:
            volume_multiplier = 1.0
            volume_status = "normal"
        components.append({"name": "volume_5m", "status": volume_status, "multiplier": volume_multiplier})

        high_52w = dict(indicators.get("high_52w") or {})
        distance_pct = _float(high_52w.get("distance_pct"))
        if high_52w.get("status") != "available" or distance_pct is None:
            components.append({"name": "high_52w", "status": "unavailable", "multiplier": 1.0})
        else:
            near_high = distance_pct >= -3.0
            far_from_high = distance_pct <= -20.0
            components.append(
                {
                    "name": "high_52w",
                    "status": "near_high" if near_high else "far" if far_from_high else "normal",
                    "multiplier": 0.85 if direction == "buy" and near_high else 1.1 if direction == "sell" and near_high else 0.95 if direction == "sell" and far_from_high else 1.0,
                }
            )

        if events.get("risk_event_flag") and direction == "sell":
            components.append({"name": "risk_event", "status": "active", "multiplier": 1.5})
        return components

    def _technical_deterioration_score(self, payload: dict[str, Any]) -> int:
        return len([item for item in self._technical_deterioration_components(payload) if item.get("active")])

    @staticmethod
    def _technical_score_multiplier(score: int) -> float:
        if score >= 5:
            return 1.5
        if score >= 3:
            return 1.25
        if score >= 1:
            return 1.1
        return 1.0

    def _technical_deterioration_components(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        indicators = dict(payload.get("indicators") or {})
        events = dict(payload.get("events") or {})
        baselines = dict(payload.get("baselines") or {})
        current = _float(payload.get("current_price")) or 0.0
        daily = dict(baselines.get("daily_indicators") or {})
        vwap = dict(indicators.get("vwap") or {})
        vwap_value = _float(vwap.get("value"))
        trading_value = dict(indicators.get("trading_value") or {})
        market = dict(indicators.get("market") or {})
        relative_strength = dict(indicators.get("relative_strength") or {})
        components = [
            {
                "name": "trading_value",
                "active": (_float(trading_value.get("surge_ratio")) or 0.0) >= 2.0,
            },
            {
                "name": "vwap",
                "active": bool(vwap_value and current > 0 and current < vwap_value),
            },
            {
                "name": "market",
                "active": (_float(market.get("change_pct")) or 0.0) <= -1.0
                and market.get("source") not in {"omitted_overseas_etf", "unavailable"},
            },
            {
                "name": "relative_strength",
                "active": relative_strength.get("status") == "weak",
            },
            {
                "name": "obv",
                "active": (_float(daily.get("obv")) is not None and _float(daily.get("obv_sma")) is not None and float(daily["obv"]) < float(daily["obv_sma"])),
            },
            {
                "name": "di",
                "active": (_float(daily.get("minus_di")) or 0.0) > (_float(daily.get("plus_di")) or 0.0),
            },
            {
                "name": "macd",
                "active": (_float(daily.get("macd_histogram")) or 0.0) < 0,
            },
        ]
        if events.get("risk_event_flag"):
            components.append({"name": "risk_event", "active": True})
        return components

    def _cash_buffer_amount(self, account: dict[str, Any]) -> int:
        orderable = int(account.get("orderable_amount") or account.get("cash") or 0)
        return int(orderable * 0.05)

    def _apply_buy_position_limits(
        self,
        payload: dict[str, Any],
        recommended_qty: int,
        reference_price: float,
    ) -> tuple[int, str | None]:
        if recommended_qty <= 0 or reference_price <= 0:
            return recommended_qty, None
        account = dict(payload.get("account") or {})
        position = dict(payload.get("position") or {})
        total_asset = float(account.get("total_asset_value") or account.get("net_asset_value") or 0)
        if total_asset <= 0:
            return recommended_qty, None
        current_value = float(position.get("quantity") or 0) * float(payload.get("current_price") or 0)
        max_position_value = total_asset * 0.35
        remaining_value = max(max_position_value - current_value, 0.0)
        max_qty = int(remaining_value // reference_price)
        if recommended_qty > max_qty:
            return max(max_qty, 0), "단일 종목 35% 제한"
        return recommended_qty, None

    def _liquidity_limited_qty(self, payload: dict[str, Any], reference_price: float) -> int | None:
        if reference_price <= 0:
            return None
        indicators = dict(payload.get("indicators") or {})
        trading_value = dict(indicators.get("trading_value") or {})
        avg20 = _float(trading_value.get("avg20")) or 0.0
        if avg20 <= 0:
            return None
        max_notional = avg20 * 0.01
        return int(max_notional // reference_price)

    def _price_guide(self, payload: dict[str, Any], direction: str, current_price: float) -> dict[str, Any]:
        order_book = dict(payload.get("order_book") or {})
        levels = list(order_book.get("levels") or [])
        status = str(order_book.get("status") or "unavailable")
        if status == "available" and levels:
            top = dict(levels[0])
            if direction == "buy" and _float(top.get("ask_price")):
                reference = float(top["ask_price"])
                return {
                    "reference_price": reference,
                    "reference_source": "sh_ask1",
                    "rounded_price": round_price_up(reference),
                    "status": status,
                }
            if direction == "sell" and _float(top.get("bid_price")):
                reference = float(top["bid_price"])
                return {
                    "reference_price": reference,
                    "reference_source": "sh_bid1",
                    "rounded_price": round_price_down(reference),
                    "status": status,
                }
        if current_price > 0:
            rounded = round_price_up(current_price) if direction == "buy" else round_price_down(current_price)
            fallback_status = status if status in {"stale", "unavailable"} else "available"
            return {
                "reference_price": current_price,
                "reference_source": "rt_price",
                "rounded_price": rounded,
                "status": fallback_status,
                "warning": order_book.get("warning") or order_book.get("message") or "",
            }
        return {
            "reference_price": 0,
            "reference_source": "unavailable",
            "rounded_price": 0,
            "status": "unavailable",
        }

    def _expected_weight_pct(self, payload: dict[str, Any], direction: str, recommended_amount: int) -> float:
        account = dict(payload.get("account") or {})
        total_asset = float(account.get("total_asset_value") or account.get("net_asset_value") or 0)
        position = dict(payload.get("position") or {})
        current_value = float(position.get("quantity") or 0) * float(payload.get("current_price") or 0)
        if total_asset <= 0:
            return 0.0
        if direction == "buy":
            return round((current_value + recommended_amount) / total_asset * 100.0, 2)
        if direction == "sell":
            return round(max(current_value - recommended_amount, 0) / total_asset * 100.0, 2)
        return round(current_value / total_asset * 100.0, 2)

    def _dispatch_target_runners(self, account_no: str, runner_id: str | None) -> list[dict[str, Any]]:
        with self._state_lock:
            return [
                copy.deepcopy(raw)
                for raw in self._state["runners"]
                if raw.get("account_no") == account_no
                and raw.get("active", True)
                and (runner_id is None or raw.get("runner_id") == runner_id)
            ]

    def _is_duplicate_suppressed(self, payload: dict[str, Any]) -> bool:
        alert_type = str(payload.get("alert_type") or "")
        if alert_type in URGENT_ALERT_TYPES:
            return False
        code = str(payload.get("code") or "")
        now = _parse_compact_time(str(payload.get("triggered_at") or "")) or _kst_now()
        cutoff = now - timedelta(minutes=self._duplicate_minutes())
        with self._state_lock:
            history = [dict(item) for item in self._state["alert_history"]]
        for item in history:
            if item.get("code") != code or item.get("alert_type") != alert_type:
                continue
            sent_at = _parse_compact_time(str(item.get("sent_at") or ""))
            if sent_at and sent_at >= cutoff:
                return True
        return False

    def _fatigue_decision(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        alert_type = str(payload.get("alert_type") or "")
        if alert_type in URGENT_ALERT_TYPES:
            return None
        code = str(payload.get("code") or "")
        now = _parse_compact_time(str(payload.get("triggered_at") or "")) or _kst_now()
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        daily_limit = int(self._config.get("daily_alert_limit") or 20)
        per_symbol_limit = int(self._config.get("per_symbol_fatigue_count") or 5)
        with self._state_lock:
            history = [dict(item) for item in self._state["alert_history"]]
        today_items = [
            item
            for item in history
            if (sent_at := _parse_compact_time(str(item.get("sent_at") or ""))) is not None and sent_at >= start_of_day
        ]
        if daily_limit > 0 and len(today_items) >= daily_limit:
            return {"code": code, "alert_type": alert_type, "queued": False, "reason": "daily_alert_limit"}
        symbol_items = [item for item in today_items if item.get("code") == code]
        if per_symbol_limit > 0 and len(symbol_items) >= per_symbol_limit:
            payload.setdefault("fatigue", {})["per_symbol_count"] = len(symbol_items)
            payload["fatigue"]["label"] = "알림 과다 종목"
            return {"code": code, "alert_type": alert_type, "queued": False, "reason": "per_symbol_fatigue"}
        return None

    def _mark_symbol_fatigue(self, payload: dict[str, Any]) -> None:
        alert_type = str(payload.get("alert_type") or "")
        if alert_type in URGENT_ALERT_TYPES:
            return
        code = str(payload.get("code") or "")
        now = _parse_compact_time(str(payload.get("triggered_at") or "")) or _kst_now()
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        per_symbol_limit = int(self._config.get("per_symbol_fatigue_count") or 5)
        if per_symbol_limit <= 0:
            return
        with self._state_lock:
            history = [dict(item) for item in self._state["alert_history"]]
        symbol_count = 0
        for item in history:
            if item.get("code") != code:
                continue
            sent_at = _parse_compact_time(str(item.get("sent_at") or ""))
            if sent_at and sent_at >= start_of_day:
                symbol_count += 1
        if symbol_count >= per_symbol_limit:
            payload.setdefault("fatigue", {})["per_symbol_count"] = symbol_count
            payload["fatigue"]["label"] = "알림 과다 종목"

    def _daily_summary_required(self, payload: dict[str, Any]) -> bool:
        alert_type = str(payload.get("alert_type") or "")
        if alert_type in URGENT_ALERT_TYPES:
            return False
        now = _parse_compact_time(str(payload.get("triggered_at") or "")) or _kst_now()
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        daily_limit = int(self._config.get("daily_alert_limit") or 20)
        if daily_limit <= 0:
            return False
        with self._state_lock:
            history = [dict(item) for item in self._state["alert_history"]]
        count = 0
        for item in history:
            sent_at = _parse_compact_time(str(item.get("sent_at") or ""))
            if sent_at and sent_at >= start_of_day:
                count += 1
        return count >= daily_limit

    def _dispatch_payload_to_runner(
        self,
        runner: dict[str, Any],
        payload: dict[str, Any],
        *,
        record_history: bool,
    ) -> dict[str, Any]:
        callback = self._alert_callback(self._http_callback_from_dict(runner["httpCallback"]), payload)
        outcome = self._dispatcher.dispatch(callback) or {}
        if record_history:
            self._record_alert_history(payload)
        return {
            "runner_id": runner["runner_id"],
            "code": payload.get("code"),
            "alert_type": payload.get("alert_type"),
            "queued": bool(outcome.get("queued")),
            "error": outcome.get("error"),
        }

    def _enqueue_alert_bundle(self, account_no: str, runner: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        now = _parse_compact_time(str(payload.get("triggered_at") or "")) or _kst_now()
        key = self._alert_group_key(account_no, str(runner["runner_id"]), str(payload.get("code") or ""))
        due_at = (now + timedelta(seconds=self._bundle_window_seconds())).strftime("%Y%m%d%H%M%S")
        with self._state_lock:
            bundles = self._state.setdefault("pending_alert_bundles", {})
            bundle = dict(bundles.get(key) or {})
            payloads = [dict(item) for item in bundle.get("payloads") or []]
            replaced = False
            alert_type = str(payload.get("alert_type") or "")
            for index, existing in enumerate(payloads):
                if existing.get("alert_type") == alert_type:
                    payloads[index] = copy.deepcopy(payload)
                    replaced = True
                    break
            if not replaced:
                payloads.append(copy.deepcopy(payload))
            bundles[key] = {
                "account_no": account_no,
                "runner_id": str(runner["runner_id"]),
                "code": str(payload.get("code") or ""),
                "first_seen_at": bundle.get("first_seen_at") or now.strftime("%Y%m%d%H%M%S"),
                "due_at": bundle.get("due_at") or due_at,
                "payloads": payloads[-10:],
            }
            self._persist_state_locked()
        return {
            "runner_id": runner["runner_id"],
            "code": payload.get("code"),
            "alert_type": payload.get("alert_type"),
            "queued": False,
            "reason": "bundle_pending",
            "due_at": (bundle.get("due_at") if "bundle" in locals() else due_at),
            "bundle_count": len(payloads),
        }

    def _enqueue_alert_summary(self, account_no: str, runner: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        now = _parse_compact_time(str(payload.get("triggered_at") or "")) or _kst_now()
        bucket_start = now.replace(minute=0 if now.minute < 30 else 30, second=0, microsecond=0)
        due_at_dt = bucket_start + timedelta(minutes=30)
        key = self._alert_group_key(account_no, str(runner["runner_id"]), due_at_dt.strftime("%Y%m%d%H%M"))
        with self._state_lock:
            summaries = self._state.setdefault("pending_alert_summaries", {})
            summary = dict(summaries.get(key) or {})
            payloads = [dict(item) for item in summary.get("payloads") or []]
            payloads.append(copy.deepcopy(payload))
            summaries[key] = {
                "account_no": account_no,
                "runner_id": str(runner["runner_id"]),
                "bucket_start": bucket_start.strftime("%Y%m%d%H%M%S"),
                "due_at": due_at_dt.strftime("%Y%m%d%H%M%S"),
                "payloads": payloads[-200:],
            }
            self._persist_state_locked()
        return {
            "runner_id": runner["runner_id"],
            "code": payload.get("code"),
            "alert_type": payload.get("alert_type"),
            "queued": False,
            "reason": "daily_summary_pending",
            "due_at": due_at_dt.strftime("%Y%m%d%H%M%S"),
            "summary_count": len(payloads),
        }

    def _flush_due_alert_groups(self, account_no: str, runner_id: str | None) -> list[dict[str, Any]]:
        now = _kst_now()
        due_bundles: list[dict[str, Any]] = []
        due_summaries: list[dict[str, Any]] = []
        with self._state_lock:
            bundles = self._state.setdefault("pending_alert_bundles", {})
            for key, bundle in list(bundles.items()):
                if bundle.get("account_no") != account_no:
                    continue
                if runner_id is not None and bundle.get("runner_id") != runner_id:
                    continue
                due_at = _parse_compact_time(str(bundle.get("due_at") or ""))
                if due_at and due_at <= now:
                    due_bundles.append(dict(bundle))
                    bundles.pop(key, None)
            summaries = self._state.setdefault("pending_alert_summaries", {})
            for key, summary in list(summaries.items()):
                if summary.get("account_no") != account_no:
                    continue
                if runner_id is not None and summary.get("runner_id") != runner_id:
                    continue
                due_at = _parse_compact_time(str(summary.get("due_at") or ""))
                if due_at and due_at <= now:
                    due_summaries.append(dict(summary))
                    summaries.pop(key, None)
            if due_bundles or due_summaries:
                self._persist_state_locked()
        dispatches: list[dict[str, Any]] = []
        for bundle in due_bundles:
            runner = self._runner_by_id(str(bundle.get("runner_id") or ""))
            if runner is None:
                continue
            payloads = [dict(item) for item in bundle.get("payloads") or []]
            if not payloads:
                continue
            bundle_payload = payloads[0] if len(payloads) == 1 else self._build_bundle_payload(payloads, bundle)
            dispatches.append(self._dispatch_payload_to_runner(runner, bundle_payload, record_history=False))
            for payload in payloads:
                self._record_alert_history(payload)
        for summary in due_summaries:
            runner = self._runner_by_id(str(summary.get("runner_id") or ""))
            if runner is None:
                continue
            payloads = [dict(item) for item in summary.get("payloads") or []]
            if not payloads:
                continue
            summary_payload = self._build_summary_payload(payloads, summary)
            dispatches.append(self._dispatch_payload_to_runner(runner, summary_payload, record_history=False))
            for payload in payloads:
                self._record_alert_history(payload)
        return dispatches

    def _runner_by_id(self, runner_id: str) -> dict[str, Any] | None:
        with self._state_lock:
            runner = next((raw for raw in self._state["runners"] if raw.get("runner_id") == runner_id), None)
            return copy.deepcopy(runner) if runner else None

    def _build_bundle_payload(self, payloads: list[dict[str, Any]], bundle: dict[str, Any]) -> dict[str, Any]:
        ordered = sorted(payloads, key=lambda item: int(item.get("priority") or 999))
        selected = ordered[0]
        lines = [
            "## 핵심 요약",
            f"- 알림: 묶음 알림",
            f"- 종목: {selected.get('name')}({selected.get('code')})",
            f"- 포함 알림: {len(payloads)}건",
            "",
            "## 포함 알림",
        ]
        for item in ordered:
            lines.append(f"- {item.get('alert_type')}: {', '.join(str(reason) for reason in item.get('reasons') or [])}")
        lines.extend(["", "자동 주문 아님. 수동 판단 필요."])
        return {
            "category": "trade_decision_bundle",
            "alert_type": "묶음 알림",
            "priority": selected.get("priority"),
            "scenario": "bundle",
            "code": selected.get("code"),
            "name": selected.get("name"),
            "triggered_at": _kst_now().strftime("%Y%m%d%H%M%S"),
            "bundle": {
                "first_seen_at": bundle.get("first_seen_at"),
                "due_at": bundle.get("due_at"),
                "count": len(payloads),
                "alerts": payloads,
            },
            "text": {
                "summary": f"묶음 알림 | {selected.get('name')}({selected.get('code')}) {len(payloads)}건",
                "detail_markdown": "\n".join(lines),
            },
        }

    def _build_summary_payload(self, payloads: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Any]:
        type_counts: dict[str, int] = {}
        symbol_counts: dict[str, int] = {}
        for payload in payloads:
            type_counts[str(payload.get("alert_type") or "")] = type_counts.get(str(payload.get("alert_type") or ""), 0) + 1
            code = str(payload.get("code") or "")
            symbol_counts[code] = symbol_counts.get(code, 0) + 1
        lines = [
            "## 핵심 요약",
            f"- 알림: 30분 요약",
            f"- 포함 알림: {len(payloads)}건",
            "",
            "## 유형별",
        ]
        lines.extend([f"- {alert_type}: {count}건" for alert_type, count in sorted(type_counts.items())])
        lines.append("")
        lines.append("## 종목별")
        lines.extend([f"- {code}: {count}건" for code, count in sorted(symbol_counts.items())])
        lines.extend(["", "자동 주문 아님. 수동 판단 필요."])
        return {
            "category": "fatigue_summary",
            "alert_type": "30분 요약",
            "priority": None,
            "scenario": "daily_limit_summary",
            "code": "",
            "name": "보유종목 알림 요약",
            "triggered_at": _kst_now().strftime("%Y%m%d%H%M%S"),
            "summary": {
                "bucket_start": summary.get("bucket_start"),
                "due_at": summary.get("due_at"),
                "count": len(payloads),
                "type_counts": type_counts,
                "symbol_counts": symbol_counts,
                "alerts": payloads,
            },
            "text": {
                "summary": f"보유종목 알림 30분 요약 {len(payloads)}건",
                "detail_markdown": "\n".join(lines),
            },
        }

    def _normalize_pending_alert_state(self) -> None:
        with self._state_lock:
            self._state["pending_alert_bundles"] = dict(self._state.get("pending_alert_bundles") or {})
            self._state["pending_alert_summaries"] = dict(self._state.get("pending_alert_summaries") or {})

    def _bundle_window_seconds(self) -> float:
        return max(float(self._config.get("bundle_window_seconds") or 60), 5.0)

    @staticmethod
    def _alert_group_key(account_no: str, runner_id: str, suffix: str) -> str:
        return f"{account_no}:{runner_id}:{suffix}"

    def _record_alert_history(self, payload: dict[str, Any]) -> None:
        self._append_alert_history("alert_history", payload, dry_run=False, persist=True)

    def _record_dry_run_alert_history(self, results: list[dict[str, Any]]) -> None:
        appended = False
        for payload in results:
            alert_type = str(payload.get("alert_type") or "")
            if alert_type in {"", "관찰", "운영 안전 알림"}:
                continue
            if self._is_history_duplicate("dry_run_alert_history", payload):
                continue
            self._append_alert_history("dry_run_alert_history", payload, dry_run=True, persist=False)
            appended = True
        if appended:
            with self._state_lock:
                self._persist_state_locked()

    def _append_alert_history(
        self,
        history_key: str,
        payload: dict[str, Any],
        *,
        dry_run: bool,
        persist: bool,
    ) -> None:
        with self._state_lock:
            self._state.setdefault(history_key, []).append(
                {
                    "code": payload.get("code"),
                    "alert_type": payload.get("alert_type"),
                    "direction": self._direction_for_alert(str(payload.get("alert_type") or "")),
                    "scenario": payload.get("scenario"),
                    "sent_at": str(payload.get("triggered_at") or "") or _kst_now().strftime("%Y%m%d%H%M%S"),
                    "priority": payload.get("priority"),
                    "dry_run": dry_run,
                }
            )
            self._state[history_key] = self._state[history_key][-1000:]
            if persist:
                self._persist_state_locked()

    def _is_history_duplicate(self, history_key: str, payload: dict[str, Any]) -> bool:
        alert_type = str(payload.get("alert_type") or "")
        code = str(payload.get("code") or "")
        now = _parse_compact_time(str(payload.get("triggered_at") or "")) or _kst_now()
        cutoff = now - timedelta(minutes=self._duplicate_minutes())
        with self._state_lock:
            history = [dict(item) for item in self._state.get(history_key, [])]
        for item in history:
            if item.get("code") != code or item.get("alert_type") != alert_type:
                continue
            sent_at = _parse_compact_time(str(item.get("sent_at") or ""))
            if sent_at and sent_at >= cutoff:
                return True
        return False

    def _alert_callback(self, callback: HttpCallbackSpec, payload: dict[str, Any]) -> HttpCallbackSpec:
        text = dict(payload.get("text") or {})
        summary = str(text.get("summary") or self._public_alert_summary(payload))
        if callback.body is None:
            return HttpCallbackSpec(
                method=callback.method,
                url=callback.url,
                headers=dict(callback.headers),
                body=summary,
                body_format="text",
            )
        replacements = self._alert_replacements(payload, summary)
        return HttpCallbackSpec(
            method=callback.method,
            url=callback.url,
            headers=dict(callback.headers),
            body=self._render_template_value(callback.body, replacements),
            body_format=callback.body_format,
        )

    def _alert_replacements(self, payload: dict[str, Any], summary: str) -> dict[str, str]:
        trade_size = dict(payload.get("trade_size") or {})
        price_guide = dict(trade_size.get("price_guide") or {})
        text = dict(payload.get("text") or {})
        reasons = [str(item) for item in payload.get("reasons") or []]
        reason_text = ", ".join(reasons) if reasons else "없음"
        trade_price = self._trade_price(payload)
        current_price_raw = str(int(float(payload.get("current_price") or 0)))
        trade_price_raw = str(int(trade_price)) if trade_price > 0 else ""
        calculated_qty_raw = str(int(trade_size.get("calculated_qty") or 0))
        recommended_qty_raw = str(int(trade_size.get("recommended_qty") or 0))
        recommended_amount_raw = str(int(trade_size.get("recommended_amount") or 0))
        replacements = {
            "eventType": "holding_alert",
            "event_type": "holding_alert",
            "summary": summary,
            "detailMarkdown": str(text.get("detail_markdown") or ""),
            "detail_markdown": str(text.get("detail_markdown") or ""),
            "alertType": str(payload.get("alert_type") or ""),
            "alert_type": str(payload.get("alert_type") or ""),
            "category": str(payload.get("category") or ""),
            "priority": str(payload.get("priority") or ""),
            "direction": str(trade_size.get("direction") or ""),
            "scenario": str(payload.get("scenario") or ""),
            "code": str(payload.get("code") or ""),
            "name": str(payload.get("name") or ""),
            "currentPrice": format_display_decimal(current_price_raw),
            "current_price": format_display_decimal(current_price_raw),
            "currentPriceRaw": current_price_raw,
            "current_price_raw": current_price_raw,
            "tradePrice": format_display_decimal(trade_price_raw) if trade_price_raw else "",
            "trade_price": format_display_decimal(trade_price_raw) if trade_price_raw else "",
            "tradePriceRaw": trade_price_raw,
            "trade_price_raw": trade_price_raw,
            "priceGuidePrice": format_display_decimal(trade_price_raw) if trade_price_raw else "",
            "price_guide_price": format_display_decimal(trade_price_raw) if trade_price_raw else "",
            "priceGuidePriceRaw": trade_price_raw,
            "price_guide_price_raw": trade_price_raw,
            "priceGuideStatus": str(price_guide.get("status") or ""),
            "price_guide_status": str(price_guide.get("status") or ""),
            "calculatedQty": format_display_decimal(calculated_qty_raw),
            "calculated_qty": format_display_decimal(calculated_qty_raw),
            "calculatedQtyRaw": calculated_qty_raw,
            "calculated_qty_raw": calculated_qty_raw,
            "recommendedQty": format_display_decimal(recommended_qty_raw),
            "recommended_qty": format_display_decimal(recommended_qty_raw),
            "recommendedQtyRaw": recommended_qty_raw,
            "recommended_qty_raw": recommended_qty_raw,
            "recommendedAmount": format_display_decimal(recommended_amount_raw),
            "recommended_amount": format_display_decimal(recommended_amount_raw),
            "recommendedAmountRaw": recommended_amount_raw,
            "recommended_amount_raw": recommended_amount_raw,
            "restriction": str(trade_size.get("restriction") or "없음"),
            "warning": str(trade_size.get("warning") or ""),
            "reasonText": reason_text,
            "reason_text": reason_text,
            "finalText": str(trade_size.get("final_text") or "자동 주문 아님. 수동 판단 필요."),
            "final_text": str(trade_size.get("final_text") or "자동 주문 아님. 수동 판단 필요."),
            "triggeredAt": str(payload.get("triggered_at") or ""),
            "triggered_at": str(payload.get("triggered_at") or ""),
        }
        return replacements

    def _render_template_value(self, value: Any, replacements: dict[str, str]) -> Any:
        if isinstance(value, dict):
            return {key: self._render_template_value(item, replacements) for key, item in value.items()}
        if isinstance(value, list):
            return [self._render_template_value(item, replacements) for item in value]
        if isinstance(value, str):
            return self._replace_tokens(value, replacements)
        return value

    def _replace_tokens(self, template: str, replacements: dict[str, str]) -> str:
        def replace_match(match: re.Match[str]) -> str:
            return replacements.get(match.group(1), "")

        return self._REPLACEMENT_PATTERN.sub(replace_match, template)

    def _retain_runner_price_subscriptions(
        self,
        account_no: str,
        held_code: list[str] | set[str] | None = None,
        wanna_code: list[str] | set[str] | None = None,
    ) -> list[str]:
        warnings: list[str] = []
        selected_held_codes = set(held_code or [])
        wanna_codes = set(wanna_code or [])
        try:
            balances = self._client.get_balance(account_no)
        except Exception as exc:
            warnings.append(f"balance query failed: {exc}")
            for code in sorted(wanna_codes):
                try:
                    self._retain_price_subscription(code)
                except Exception as retain_exc:
                    warnings.append(f"{code} price RT subscribe failed: {retain_exc}")
            return warnings
        current_held_codes = {item.code for item in balances}
        if not selected_held_codes:
            overlap = current_held_codes & wanna_codes
            if overlap:
                joined = ", ".join(sorted(overlap))
                raise ValueError(f"wannaCode must not contain currently held stock codes when heldCode watches all holdings: {joined}")
        retained_codes: set[str] = set()
        for item in self._filter_held_balance_items(balances, selected_held_codes, wanna_codes):
            retained_codes.add(item.code)
            try:
                self._retain_price_subscription(item.code)
            except Exception as exc:
                warnings.append(f"{item.code} price RT subscribe failed: {exc}")
        for code in sorted(wanna_codes):
            try:
                self._retain_price_subscription(code)
            except Exception as exc:
                warnings.append(f"{code} price RT subscribe failed: {exc}")
        for missing_code in sorted(selected_held_codes - retained_codes):
            warnings.append(f"{missing_code} is not in current holdings; runner will watch it after it appears in balance")
        return warnings

    def _retain_price_subscription(self, code: str) -> None:
        previous = self._owned_price_codes.get(code, 0)
        if previous == 0:
            self._client.subscribe_realtime_price(code)
        self._owned_price_codes[code] = previous + 1

    def _ensure_price_subscription_for_holding(self, code: str) -> None:
        if self._owned_price_codes.get(code, 0) > 0:
            return
        try:
            self._retain_price_subscription(code)
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert new holding price RT subscribe failed code={code}: {exc}")

    def _warm_baseline_for_new_position(self, code: str, name: str) -> None:
        trading_date = _kst_now().strftime("%Y%m%d")
        try:
            baseline = self._build_baseline(code, name, trading_date)
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert new holding baseline warmup failed code={code}: {exc}")
            return
        with self._state_lock:
            self._state["baseline_cache"][code] = baseline
            self._persist_state_locked()

    def _release_unused_price_subscriptions(self) -> None:
        with self._state_lock:
            active_runners = [
                (str(raw.get("account_no") or ""), self._runner_held_codes(raw), self._runner_wanna_codes(raw))
                for raw in self._state.get("runners", [])
                if raw.get("active", True)
            ]
        wanted: set[str] = set()
        for account_no, held_filter, wanna_filter in active_runners:
            wanted.update(wanna_filter)
            for item in self._filter_held_balance_items(self._safe_get_balance(account_no), held_filter, wanna_filter):
                wanted.add(item.code)
        for code in list(self._owned_price_codes):
            if code in wanted:
                continue
            self._owned_price_codes.pop(code, 0)
            try:
                self._client.unsubscribe_realtime_price(code)
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"holding alert price RT unsubscribe failed code={code}: {exc}")

    def _tr_cache_get(self, key: str) -> tuple[bool, Any, str]:
        now = time.monotonic()
        with self._state_lock:
            entry = self._tr_cache.get(key)
            if not entry:
                return False, None, ""
            value = copy.deepcopy(entry.get("value"))
            received_at = str(entry.get("received_at") or "")
            if now <= float(entry.get("expires_at") or 0):
                return True, value, received_at
            return False, value, received_at

    def _tr_cache_set(self, key: str, value: Any, ttl_seconds: float) -> Any:
        received_at = _kst_now().strftime("%Y%m%d%H%M%S")
        with self._state_lock:
            self._tr_cache[key] = {
                "value": copy.deepcopy(value),
                "received_at": received_at,
                "expires_at": time.monotonic() + max(float(ttl_seconds), 1.0),
            }
        return value

    def _refresh_ttl_seconds(self) -> float:
        return max(float(self._config.get("refresh_interval_seconds") or 45), 5.0)

    def _daily_ttl_seconds(self) -> float:
        return max(float(self._config.get("daily_refresh_interval_seconds") or 3600), 60.0)

    def _market_ttl_seconds(self) -> float:
        return max(float(self._config.get("market_refresh_interval_seconds") or 300), 60.0)

    def _intraday_ttl_seconds(self, now: datetime | None = None) -> float:
        current = now or _kst_now()
        minute_offset = current.minute % 5
        next_boundary = current + timedelta(minutes=5 - minute_offset, seconds=-current.second, microseconds=-current.microsecond)
        if minute_offset == 0 and current.second == 0:
            next_boundary = current + timedelta(minutes=5)
        return min(max((next_boundary - current).total_seconds() + 5.0, 10.0), 305.0)

    def _order_book_ttl_seconds(self) -> float:
        return max(float(self._config.get("order_book_refresh_interval_seconds") or self._refresh_ttl_seconds()), 5.0)

    def _balance_snapshot(self, account_no: str) -> tuple[list[BalanceItem], dict[str, Any]]:
        key = f"balance:{account_no}"
        hit, cached, received_at = self._tr_cache_get(key)
        if hit:
            return cached, {"status": "available", "source": "cache", "received_at": received_at}
        try:
            balances = self._client.get_balance(account_no)
            self._tr_cache_set(key, balances, self._refresh_ttl_seconds())
            self._update_balance_diff_state(account_no, balances)
            return balances, {"status": "available", "received_at": _kst_now().strftime("%Y%m%d%H%M%S")}
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert balance failed account={account_no}: {exc}")
            if cached is not None:
                return cached, {"status": "stale", "received_at": received_at, "message": str(exc)}
            return [], {"status": "unavailable", "received_at": "", "message": str(exc)}

    def _account_summary_snapshot(self, account_no: str) -> tuple[dict[str, Any], dict[str, Any]]:
        key = f"account_summary:{account_no}"
        hit, cached, received_at = self._tr_cache_get(key)
        if hit:
            return cached, {"status": "available", "source": "cache", "received_at": received_at}
        try:
            summary = self._client.get_account_summary(account_no).to_dict()
            self._tr_cache_set(key, summary, self._refresh_ttl_seconds())
            return summary, {"status": "available", "received_at": _kst_now().strftime("%Y%m%d%H%M%S")}
        except Exception as exc:
            if cached is not None:
                return cached, {"status": "stale", "received_at": received_at, "message": str(exc)}
            return {}, {"status": "stale", "received_at": "", "message": str(exc)}

    def _open_orders_snapshot(self, account_no: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        key = f"open_orders:{account_no}"
        hit, cached, received_at = self._tr_cache_get(key)
        if hit:
            return cached, {"status": "available", "source": "cache", "received_at": received_at}
        try:
            orders = [item.to_dict() for item in self._client.get_open_orders(account_no)]
            self._tr_cache_set(key, orders, self._refresh_ttl_seconds())
            return orders, {"status": "available", "received_at": _kst_now().strftime("%Y%m%d%H%M%S")}
        except Exception as exc:
            if cached is not None:
                return cached, {"status": "stale", "received_at": received_at, "message": str(exc)}
            return [], {"status": "stale", "received_at": "", "message": str(exc)}

    def _update_balance_diff_state(self, account_no: str, balances: list[BalanceItem]) -> None:
        snapshot = {
            item.code: {
                "quantity": item.quantity,
                "avg_price": item.avg_price,
                "current_price": item.current_price,
                "name": item.name,
            }
            for item in balances
        }
        now = _kst_now().strftime("%Y%m%d%H%M%S")
        new_positions: dict[str, dict[str, Any]] = {}
        removed_codes: set[str] = set()
        active_runner_filters: list[tuple[set[str], set[str]]] = []
        with self._state_lock:
            account_snapshots = dict(self._state.setdefault("balance_snapshots", {}))
            previous = dict(account_snapshots.get(account_no) or {})
            changed = False
            for code, current in snapshot.items():
                old = dict(previous.get(code) or {})
                if not old:
                    symbol_state = dict(self._state.setdefault("symbol_state", {}).get(code) or {})
                    symbol_state["active_position"] = True
                    symbol_state["position_opened_at"] = symbol_state.get("position_opened_at") or now
                    symbol_state["balance_changed_at"] = now
                    symbol_state["balance_quantity"] = current["quantity"]
                    symbol_state["balance_avg_price"] = current["avg_price"]
                    symbol_state["high_since_entry"] = max(
                        _float(symbol_state.get("high_since_entry")) or 0.0,
                        float(current["current_price"] or 0),
                    )
                    self._state["symbol_state"][code] = symbol_state
                    new_positions[code] = current
                    changed = True
                elif old.get("quantity") != current["quantity"] or old.get("avg_price") != current["avg_price"]:
                    symbol_state = dict(self._state.setdefault("symbol_state", {}).get(code) or {})
                    symbol_state["balance_changed_at"] = now
                    symbol_state["balance_quantity"] = current["quantity"]
                    symbol_state["balance_avg_price"] = current["avg_price"]
                    symbol_state["active_position"] = True
                    symbol_state["high_since_entry"] = max(
                        _float(symbol_state.get("high_since_entry")) or 0.0,
                        float(current["current_price"] or 0),
                    )
                    self._state["symbol_state"][code] = symbol_state
                    changed = True
            removed_codes = set(previous) - set(snapshot)
            for code in removed_codes:
                symbol_state = dict(self._state.setdefault("symbol_state", {}).get(code) or {})
                symbol_state["active_position"] = False
                symbol_state["position_closed_at"] = now
                symbol_state["balance_quantity"] = 0
                for key in (
                    "damage_breach_since",
                    "recovery_since",
                    "price_unavailable_since",
                    "price_recovered_at",
                    "first_support_touched_at",
                    "first_reclaim_since",
                    "second_support_touched_at",
                    "second_reclaim_since",
                    "trend_reclaim_since",
                ):
                    symbol_state.pop(key, None)
                self._state["symbol_state"][code] = symbol_state
                changed = True
            if previous != snapshot:
                account_snapshots[account_no] = snapshot
                self._state["balance_snapshots"] = account_snapshots
                changed = True
            active_runner_filters = [
                (
                    {str(item) for item in self._runner_held_codes(raw)},
                    {str(item) for item in self._runner_wanna_codes(raw)},
                )
                for raw in self._state.get("runners", [])
                if raw.get("account_no") == account_no and raw.get("active", True)
            ]
            if changed:
                self._persist_state_locked()
        if active_runner_filters:
            wanted_new_codes = {
                code
                for code in new_positions
                if any(code not in wanna_filter and (not held_filter or code in held_filter) for held_filter, wanna_filter in active_runner_filters)
            }
            for code in wanted_new_codes:
                self._ensure_price_subscription_for_holding(code)
            if removed_codes:
                self._release_unused_price_subscriptions()
            for code, current in new_positions.items():
                if code in wanted_new_codes:
                    self._warm_baseline_for_new_position(code, str(current.get("name") or ""))

    def _safe_get_balance(self, account_no: str) -> list[BalanceItem]:
        try:
            return self._client.get_balance(account_no)
        except Exception:
            return []

    def _safe_daily_prices(self, code: str, end_date: str, lookback_days: int) -> list[DailyPrice]:
        start = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=lookback_days * 2)).strftime("%Y%m%d")
        key = f"daily:{code}:{start}:{end_date}"
        hit, cached, _ = self._tr_cache_get(key)
        if hit:
            return cached
        try:
            prices = self._client.get_daily_prices(code, start, end_date)
            return self._tr_cache_set(key, prices, self._daily_ttl_seconds())
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert daily prices failed code={code}: {exc}")
            if cached is not None:
                return cached
            return []

    def _safe_intraday_prices(self, code: str, trading_date: str) -> list[IntradayPrice]:
        key = f"intraday:{code}:{trading_date}:5"
        hit, cached, _ = self._tr_cache_get(key)
        if hit:
            return cached
        try:
            prices = self._client.get_intraday_prices(code, trading_date, 5)
            ttl = self._intraday_ttl_seconds() if trading_date == _kst_now().strftime("%Y%m%d") else self._daily_ttl_seconds()
            return self._tr_cache_set(key, prices, ttl)
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert intraday prices failed code={code}: {exc}")
            if cached is not None:
                return cached
            return []

    def _safe_quote_snapshot(self, code: str) -> dict[str, Any]:
        key = f"quote:{code}"
        hit, cached, _ = self._tr_cache_get(key)
        if hit:
            return cached
        try:
            quote = self._client.get_quote_snapshot(code).to_dict()
            return self._tr_cache_set(key, quote, self._refresh_ttl_seconds())
        except Exception:
            if cached is not None:
                return cached
            return {}

    def _market_context_for_stock(self, stock_config: dict[str, Any], trading_date: str) -> dict[str, Any]:
        if self._is_overseas_etf(stock_config):
            return {
                "change_pct": 0,
                "source": "omitted_overseas_etf",
                "index": "kospi200",
                "message": "해외 ETF는 장중 국내 시장 동조성 판단을 생략합니다.",
            }
        return self._market_context(trading_date)

    def _market_context(self, trading_date: str) -> dict[str, Any]:
        key = f"market_context:{trading_date}"
        hit, cached, _ = self._tr_cache_get(key)
        if hit:
            return cached
        start = (datetime.strptime(trading_date, "%Y%m%d") - timedelta(days=120)).strftime("%Y%m%d")
        try:
            prices = self._client.get_market_index_prices(start, trading_date).get("kospi200", [])
            if len(prices) >= 2:
                ordered = sorted(prices, key=lambda item: item.date)
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
                return self._tr_cache_set(
                    key,
                    {"change_pct": round(change, 4), "change_series": change_series[-60:], "source": "daily", "index": "kospi200"},
                    self._market_ttl_seconds(),
                )
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert market context failed: {exc}")
        if cached is not None:
            return cached
        return {"change_pct": 0, "source": "unavailable"}

    def _fx_context(self, stock_config: dict[str, Any], trading_date: str) -> dict[str, Any]:
        if not self._is_overseas_etf(stock_config):
            return {"source": "not_applicable"}
        return self._market_index_context(str(stock_config.get("fx_index") or "usdkrw"), trading_date)

    def _overseas_market_context(self, stock_config: dict[str, Any], trading_date: str) -> dict[str, Any]:
        if not self._is_overseas_etf(stock_config):
            return {"source": "not_applicable"}
        return self._market_index_context(str(stock_config.get("overseas_index") or "nasdaq"), trading_date)

    def _market_index_context(self, index_id: str, trading_date: str) -> dict[str, Any]:
        normalized_index = index_id.strip().lower()
        key = f"market_index_context:{normalized_index}:{trading_date}"
        hit, cached, _ = self._tr_cache_get(key)
        if hit:
            return cached
        start = (datetime.strptime(trading_date, "%Y%m%d") - timedelta(days=14)).strftime("%Y%m%d")
        try:
            prices = self._client.get_market_index_prices(start, trading_date).get(normalized_index, [])
            if len(prices) >= 2:
                ordered = sorted(prices, key=lambda item: item.date)
                latest, previous = ordered[-1], ordered[-2]
                change = ((latest.close - previous.close) / previous.close * 100.0) if previous.close else 0.0
                return self._tr_cache_set(
                    key,
                    {
                        "index": normalized_index,
                        "change_pct": round(change, 4),
                        "latest": latest.close,
                        "previous": previous.close,
                        "source": "daily",
                    },
                    self._market_ttl_seconds(),
                )
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert market index context failed index={normalized_index}: {exc}")
        if cached is not None:
            return cached
        return {"index": normalized_index, "change_pct": 0, "source": "unavailable"}

    @staticmethod
    def _is_overseas_etf(stock_config: dict[str, Any]) -> bool:
        return bool(stock_config.get("overseas_etf") or stock_config.get("fx_index") or stock_config.get("overseas_index"))

    def _sector_context(self, code: str, stock_config: dict[str, Any], trading_date: str) -> dict[str, Any]:
        sector_code = str(stock_config.get("sector_code") or "")
        sector_name = str(stock_config.get("sector_name") or "")
        if not sector_code:
            try:
                profile = self._client.get_stock_sector_profile(code)
                sector_code = str(profile.get("sector_code") or "")
                sector_name = str(profile.get("sector_name") or "")
            except Exception:
                pass
        if not sector_code:
            return {"sector_code": "", "sector_name": "", "change_pct": 0, "source": "unavailable"}
        key = f"sector_context:{sector_code}:{trading_date}"
        hit, cached, _ = self._tr_cache_get(key)
        if hit:
            cached["sector_name"] = cached.get("sector_name") or sector_name
            return cached
        start = (datetime.strptime(trading_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        try:
            prices = self._client.get_sector_index_prices(sector_code, start, trading_date, "D")
            if len(prices) >= 2:
                latest, previous = prices[0], prices[1]
                change = ((latest.close - previous.close) / previous.close * 100.0) if previous.close else 0.0
                return self._tr_cache_set(
                    key,
                    {"sector_code": sector_code, "sector_name": sector_name, "change_pct": round(change, 4), "source": "daily"},
                    self._market_ttl_seconds(),
                )
        except Exception as exc:
            ops_log(LogSource.MANAGE, f"holding alert sector context failed code={code}: {exc}")
        if cached is not None:
            cached["sector_name"] = cached.get("sector_name") or sector_name
            return cached
        return {"sector_code": sector_code, "sector_name": sector_name, "change_pct": 0, "source": "unavailable"}

    def _relative_strength_context(self, daily: list[DailyPrice], market: dict[str, Any]) -> dict[str, Any]:
        if len(daily) < 2 or market.get("source") == "unavailable":
            return {"rs_ratio": 0, "status": "unavailable"}
        ordered = sorted(daily, key=lambda item: _normalize_date(item.date) or item.date)
        market_series = list(market.get("change_series") or [])
        market_by_date = {str(item.get("date")): float(item.get("change_pct") or 0.0) for item in market_series}
        rs_values: list[float] = []
        for index in range(1, len(ordered)):
            previous = ordered[index - 1]
            current = ordered[index]
            stock_change = ((current.close - previous.close) / previous.close * 100.0) if previous.close else 0.0
            market_change = market_by_date.get(str(current.date).replace("-", ""), float(market.get("change_pct") or 0.0))
            rs_values.append((100.0 + stock_change) / max(100.0 + market_change, 0.0001))
        if not rs_values:
            return {"rs_ratio": 0, "status": "unavailable"}
        latest_rs = rs_values[-1]
        avg_window = rs_values[-60:]
        avg_rs = sum(avg_window) / len(avg_window)
        rs_ratio = latest_rs / avg_rs if avg_rs else 1.0
        status = "strong" if rs_ratio > 1.01 else "weak" if rs_ratio < 0.99 else "neutral"
        return {"rs_ratio": round(rs_ratio, 4), "latest_rs": round(latest_rs, 4), "avg_rs_60d": round(avg_rs, 4), "status": status}

    def _trading_value_context(self, daily: list[DailyPrice], quote: dict[str, Any]) -> dict[str, Any]:
        ordered = sorted(daily, key=lambda item: _normalize_date(item.date) or item.date)
        values = [row.close * row.volume for row in ordered[-20:]]
        avg20 = int(sum(values) / len(values)) if values else 0
        latest_volume = ordered[-1].volume if ordered else 0
        current_price = int(quote.get("current_price") or (ordered[-1].close if ordered else 0))
        today = current_price * latest_volume
        surge_ratio = round(today / avg20, 4) if avg20 else 0
        return {"surge_ratio": surge_ratio, "avg20": avg20, "today": today}

    def _volume_5m_context(self, intraday: list[IntradayPrice]) -> dict[str, Any]:
        if not intraday:
            return {"ratio": 0, "status": "unavailable"}
        ordered = sorted(intraday, key=lambda item: (item.date, item.time))
        avg = sum(item.volume for item in ordered) / len(ordered)
        latest = ordered[-1].volume
        return {"ratio": round(latest / avg, 4) if avg else 0, "status": "available"}

    def _high_52w_context(self, daily: list[DailyPrice], quote: dict[str, Any]) -> dict[str, Any]:
        high = int(quote.get("year_high") or 0)
        current = int(quote.get("current_price") or 0)
        if not high and daily:
            high = max(row.high for row in daily[-252:])
            current = daily[-1].close
        if high <= 0 or current <= 0:
            return {"distance_pct": 0, "high": 0, "status": "unavailable"}
        return {"distance_pct": round((current - high) / high * 100.0, 4), "high": high, "status": "available"}

    def _vwap_context(self, intraday: list[IntradayPrice], daily: list[DailyPrice]) -> dict[str, Any]:
        if not intraday:
            return {"value": 0, "status": "unavailable"}
        ordered = sorted(intraday, key=lambda item: (item.date, item.time))
        if ordered[-1].time < "090500":
            return {"value": 0, "status": "unavailable_too_early"}
        total_volume = sum(item.volume for item in intraday)
        avg20_volume = int(sum(item.volume for item in daily[-20:]) / len(daily[-20:])) if daily[-20:] else 0
        if avg20_volume and total_volume < avg20_volume * 0.05:
            return {"value": 0, "status": "unavailable_low_liquidity"}
        if total_volume <= 0:
            return {"value": 0, "status": "unavailable"}
        value = sum(item.close * item.volume for item in intraday) / total_volume
        return {"value": round(value, 4), "status": "available"}

    def _has_same_direction_open_order(self, open_orders: list[dict[str, Any]], code: str, alert_type: str) -> bool:
        direction = self._direction_for_alert(alert_type)
        if direction == "observe":
            return False
        return any(raw.get("code") == code and raw.get("side") == direction for raw in open_orders)

    def _support_zone(self, values: list[float | None], tolerance_pct: float, sources: list[str]) -> dict[str, Any]:
        candidates = [float(item) for item in values if item is not None and item > 0]
        if not candidates:
            return {"low": 0, "high": 0, "sources": []}
        low = min(candidates) * (1.0 - tolerance_pct / 100.0)
        high = max(candidates) * (1.0 + tolerance_pct / 100.0)
        active_sources = [source for source, value in zip(sources, values) if value is not None and value > 0]
        return {"low": round_price_down(low), "high": round_price_up(high), "sources": active_sources}

    def _fallback_atr(self, prices: list[DailyPrice]) -> float | None:
        if len(prices) < 2:
            return None
        true_ranges = []
        ordered = prices[-14:]
        for index, item in enumerate(ordered):
            previous_close = ordered[index - 1].close if index > 0 else item.close
            true_ranges.append(max(item.high - item.low, abs(item.high - previous_close), abs(item.low - previous_close)))
        return sum(true_ranges) / len(true_ranges) if true_ranges else None

    def _unavailable_baseline(
        self,
        code: str,
        name: str,
        trading_date: str,
        stock_config: dict[str, Any],
        message: str,
    ) -> dict[str, Any]:
        return {
            "code": code,
            "name": name,
            "trading_date": trading_date,
            "generated_at": _kst_now().strftime("%Y%m%d%H%M%S"),
            "category": stock_config.get("category", "대형주"),
            "atr_multiplier": stock_config.get("atr_multiplier", 2.0),
            "support_tolerance_pct": stock_config.get("support_tolerance_pct", 0.5),
            "damage_line": 0,
            "damage_line_components": {"chandelier_line": 0, "recent_low20": 0, "bollinger_lower_adjusted": 0},
            "first_support": {"low": 0, "high": 0, "sources": []},
            "second_support": {"low": 0, "high": 0, "sources": []},
            "recovery_line": 0,
            "buy_reclaim_lines": {"first_rebound": 0, "second_rebound": 0, "trend_reclaim": 0},
            "status": "unavailable",
            "message": message,
        }

    def _order_book_to_snapshot(self, order_book: OrderBook, status: str) -> dict[str, Any]:
        return {
            "code": order_book.code,
            "received_at": order_book.received_at,
            "source": order_book.source or "SH",
            "market_phase": order_book.market_phase,
            "levels": [level.to_dict() for level in order_book.levels],
            "status": status,
            "message": order_book.message,
        }

    def _format_data_status(self, data_status: dict[str, Any]) -> str:
        parts = []
        for key in ("price", "order_book", "balance", "account_summary", "open_orders", "intraday_5m", "market", "sector"):
            value = dict(data_status.get(key) or {})
            status = value.get("status")
            if status and status != "available":
                parts.append(f"{key} {status}")
        return ", ".join(parts) if parts else "available"

    @staticmethod
    def _format_indicator_change(raw: Any) -> str:
        value = dict(raw or {})
        source = str(value.get("source") or "")
        if source in {"", "not_applicable"}:
            return "not_applicable"
        if source == "unavailable":
            return "unavailable"
        if source == "omitted_overseas_etf":
            return "omitted_overseas_etf"
        change = _float(value.get("change_pct"))
        label = str(value.get("index") or value.get("sector_name") or source)
        if change is None:
            return f"{label} {source}"
        return f"{label} {change:.2f}%"

    def _stock_name(self, code: str) -> str:
        try:
            for stock in self._client.list_stocks():
                if stock.code == code:
                    return stock.name
        except Exception:
            pass
        return ""

    def _price_stale_seconds(self) -> float:
        return float(self._config.get("price_stale_seconds") or 180)

    def _order_book_stale_seconds(self) -> float:
        return float(self._config.get("order_book_stale_seconds") or 180)

    def _duplicate_minutes(self) -> float:
        return float(self._config.get("duplicate_suppression_minutes") or 30)

    def _sell_hold_minutes(self, code: str, kind: str, now: datetime) -> float:
        base = 15.0 if kind == "judgment" else 10.0
        override = self._active_whipsaw_override(code, now)
        return base + float(override.get("hold_minutes_add") or 0.0)

    def _adjusted_atr_multiplier(self, code: str, base_multiplier: float) -> float:
        override = self._active_whipsaw_override(code, _kst_now())
        step = float(self._config.get("atr_multiplier_step") or 0.2)
        return base_multiplier + (step * float(override.get("atr_multiplier_steps") or 0.0))

    def _active_whipsaw_override(self, code: str, now: datetime) -> dict[str, Any]:
        with self._state_lock:
            override = copy.deepcopy(self._state.get("whipsaw_overrides", {}).get(code) or {})
        effective_from = _parse_compact_time(str(override.get("effective_from") or ""))
        if not effective_from or effective_from > now:
            return {}
        return override

    def _refresh_whipsaw_overrides(self, now: datetime) -> None:
        cutoff = now - timedelta(days=7)
        with self._state_lock:
            history = [dict(item) for item in self._state["alert_history"]]
            history.extend(dict(item) for item in self._state.get("dry_run_alert_history", []))
            previous = copy.deepcopy(self._state.get("whipsaw_overrides") or {})
        by_code: dict[str, list[dict[str, Any]]] = {}
        for item in history:
            sent_at = _parse_compact_time(str(item.get("sent_at") or ""))
            direction = str(item.get("direction") or self._direction_for_alert(str(item.get("alert_type") or "")))
            if not sent_at or sent_at < cutoff or direction not in {"buy", "sell"}:
                continue
            item["sent_at_dt"] = sent_at
            item["direction"] = direction
            by_code.setdefault(str(item.get("code") or ""), []).append(item)
        updated = copy.deepcopy(previous)
        changed = False
        for code, items in by_code.items():
            if len(items) < 3:
                continue
            items.sort(key=lambda item: item["sent_at_dt"])
            whipsaws = 0
            for index, item in enumerate(items):
                sent_at = item["sent_at_dt"]
                opposite = "buy" if item["direction"] == "sell" else "sell"
                if any(
                    other.get("direction") == opposite and timedelta(0) < other["sent_at_dt"] - sent_at <= timedelta(hours=24)
                    for other in items[index + 1 :]
                ):
                    whipsaws += 1
            whipsaw_rate = whipsaws / max(len(items), 1) * 100.0
            current = dict(previous.get(code) or {})
            if whipsaw_rate > 30.0:
                evaluated_week = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
                if current.get("evaluated_week") == evaluated_week:
                    consecutive = int(current.get("consecutive_weeks") or 1)
                else:
                    consecutive = int(current.get("consecutive_weeks") or 0) + 1
                candidate = {
                    "code": code,
                    "evaluated_at": current.get("evaluated_at") if current.get("evaluated_week") == evaluated_week else now.strftime("%Y%m%d%H%M%S"),
                    "evaluated_week": evaluated_week,
                    "effective_from": current.get("effective_from")
                    if current.get("evaluated_week") == evaluated_week
                    else self._next_week_effective_time(now).strftime("%Y%m%d%H%M%S"),
                    "whipsaw_rate": round(whipsaw_rate, 2),
                    "sample_count": len(items),
                    "hold_minutes_add": 5,
                    "atr_multiplier_steps": 1 if consecutive >= 2 else 0,
                    "consecutive_weeks": consecutive,
                }
                if current != candidate:
                    updated[code] = candidate
                    changed = True
            elif code in updated and whipsaw_rate <= 20.0:
                updated.pop(code, None)
                changed = True
        if changed:
            with self._state_lock:
                self._state["whipsaw_overrides"] = updated
                self._persist_state_locked()

    @staticmethod
    def _next_week_effective_time(now: datetime) -> datetime:
        days_until_monday = (7 - now.weekday()) % 7
        if days_until_monday == 0:
            days_until_monday = 7
        next_monday = now + timedelta(days=days_until_monday)
        return next_monday.replace(hour=9, minute=0, second=0, microsecond=0)

    def _load_config(self) -> dict[str, Any]:
        if self._config_path.exists():
            try:
                return json.loads(self._config_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                ops_log(LogSource.MANAGE, f"Failed to load holding alert config {self._config_path}: {exc}")
        return {
            "version": 1,
            "tick_interval_seconds": 5,
            "refresh_interval_seconds": 45,
            "price_stale_seconds": 180,
            "order_book_stale_seconds": 180,
            "daily_refresh_interval_seconds": 3600,
            "market_refresh_interval_seconds": 300,
            "order_book_refresh_interval_seconds": 45,
            "atr_multiplier_step": 0.2,
            "duplicate_suppression_minutes": 30,
            "bundle_window_seconds": 60,
            "daily_alert_limit": 20,
            "per_symbol_fatigue_count": 5,
            "default_profile": {
                "category": "대형주",
                "atr_multiplier": 2.0,
                "support_tolerance_pct": 0.5,
                "observe_only": True,
                "validation_passed": False,
            },
            "stocks": {},
            "categories": {},
            "risk_keywords": [],
            "mechanical_keywords": ["분배락", "배당락", "기준가격"],
        }

    def _persist_state_locked(self) -> None:
        self._store.save(self._state)
        self._last_state_persist_monotonic = time.monotonic()

    @staticmethod
    def _default_config_path() -> Path:
        return Path(__file__).resolve().parent.parent / "config" / "holding_alerts.json"

    @staticmethod
    def _http_callback_from_dict(raw: dict[str, Any]) -> HttpCallbackSpec:
        return HttpCallbackSpec(
            method=str(raw["method"]),
            url=str(raw["url"]),
            headers={str(key): str(value) for key, value in dict(raw.get("headers") or {}).items()},
            body=raw.get("body"),
            body_format=raw.get("bodyFormat"),
        )


def round_price_down(price: float) -> int:
    if price <= 0:
        return 0
    unit = tick_size(price)
    return int(price // unit * unit)


def round_price_up(price: float) -> int:
    if price <= 0:
        return 0
    unit = tick_size(price)
    return int(((int(price) + unit - 1) // unit) * unit)


def tick_size(price: float) -> int:
    value = abs(float(price))
    if value < 2000:
        return 1
    if value < 5000:
        return 5
    if value < 20000:
        return 10
    if value < 50000:
        return 50
    if value < 200000:
        return 100
    if value < 500000:
        return 500
    return 1000


def _kst_now() -> datetime:
    return datetime.now(KST)


def _normalize_date(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if len(cleaned) == 10 and cleaned[4] == "-" and cleaned[7] == "-":
        cleaned = cleaned.replace("-", "")
    if len(cleaned) == 8 and cleaned.isdigit():
        return cleaned
    return None


def _parse_compact_time(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=KST)
    except ValueError:
        return None


def _intraday_point_datetime(point: IntradayPrice) -> datetime | None:
    date = _normalize_date(point.date)
    time_text = str(point.time or "").strip()
    if not date or not time_text.isdigit():
        return None
    if len(time_text) <= 4:
        time_text = time_text.zfill(4) + "00"
    elif len(time_text) == 5:
        time_text = time_text.zfill(6)
    elif len(time_text) > 6:
        time_text = time_text[:6]
    try:
        return datetime.strptime(f"{date}{time_text}", "%Y%m%d%H%M%S").replace(tzinfo=KST)
    except ValueError:
        return None


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
