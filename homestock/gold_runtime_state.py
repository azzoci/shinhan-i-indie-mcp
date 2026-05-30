from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime, time, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from homestock.display_format import format_display_decimal
from homestock.models import (
    HttpCallbackSpec,
    PriceAlertFiredPayload,
    PriceAlertRecord,
)
from homestock.ops_log import LogSource, ops_log
from homestock.webhook import CallbackDispatcher


try:
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9))


def _kst_now() -> datetime:
    return datetime.now(KST)


def _with_state_lock(method: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(method)
    def wrapped(self: "GoldRuntimeStateManager", *args: Any, **kwargs: Any) -> Any:
        with self._state_lock:
            return method(self, *args, **kwargs)

    return wrapped


class GoldRuntimeStateStore:
    VERSION = 1

    def __init__(self, state_dir: Path) -> None:
        self._state_dir = state_dir

    def current_trading_date(self) -> str:
        return _kst_now().strftime("%Y%m%d")

    def current_state_path(self) -> Path:
        return self._state_dir / f"gold_runtime_state_{self.current_trading_date()}.json"

    def load(self) -> dict[str, Any]:
        path = self.current_state_path()
        if not path.exists():
            return self._default_state()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            ops_log(LogSource.MANAGE, f"Failed to load gold runtime state {path}: {exc}")
            return self._default_state()
        if payload.get("trading_date") != self.current_trading_date():
            return self._default_state()
        return self._merge_defaults(payload)

    def save(self, state: dict[str, Any]) -> Path:
        path = self.current_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._merge_defaults(state)
        payload["trading_date"] = self.current_trading_date()
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
        self._cleanup_old_files(path)
        return path

    def clear(self) -> None:
        path = self.current_state_path()
        if path.exists():
            try:
                path.unlink()
            except OSError as exc:
                ops_log(LogSource.MANAGE, f"Failed to remove gold runtime state {path}: {exc}")

    def _cleanup_old_files(self, keep_path: Path) -> None:
        if not self._state_dir.exists():
            return
        for path in self._state_dir.glob("gold_runtime_state_*.json"):
            if path == keep_path:
                continue
            try:
                path.unlink()
            except OSError:
                continue

    def _default_state(self) -> dict[str, Any]:
        return {
            "version": self.VERSION,
            "trading_date": self.current_trading_date(),
            "updated_at": "",
            "price_alerts": [],
            "gold_price_callbacks": [],
        }

    def _merge_defaults(self, payload: dict[str, Any]) -> dict[str, Any]:
        base = self._default_state()
        base.update({key: value for key, value in payload.items() if key in base})
        base["price_alerts"] = list(payload.get("price_alerts", []))
        base["gold_price_callbacks"] = list(payload.get("gold_price_callbacks", []))
        return base


class GoldRuntimeStateManager:
    MARKET_CLOSE_TIME = time(hour=18, minute=0)
    PRICE_ALERT_COOLDOWN_SECONDS_PER_MINUTE = 60.0
    _REPLACEMENT_PATTERN = re.compile(r"\{\{([a-zA-Z0-9_]+)\}\}")

    def __init__(
        self,
        client: Any,
        state_dir: str | os.PathLike[str] | None = None,
        restore_realtime: bool = True,
        system_event_recorder: Callable[[str, str, dict[str, Any] | None], None] | None = None,
    ) -> None:
        self._client = client
        self._state_lock = threading.RLock()
        self._system_event_recorder = system_event_recorder
        resolved_dir = Path(state_dir) if state_dir is not None else Path(".runtime")
        self._store = GoldRuntimeStateStore(resolved_dir)
        self._dispatcher = CallbackDispatcher()
        self._rt_listener = self._on_gold_rt_event
        self._rt_listener_registered = False
        self._closed = False
        self._owned_price_codes: dict[str, int] = {}
        self._price_alert_cooldown_timers: dict[str, threading.Timer] = {}
        self._price_alert_cooldown_pending: dict[str, dict[str, Any]] = {}
        self._state = self._store.load()
        self._restored_from_disk = self._has_runtime_state()
        self._client.register_gold_rt_listener(self._rt_listener)
        self._rt_listener_registered = True
        try:
            if restore_realtime:
                self._restore_same_day_state()
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        drain_timeout = self._dispatcher_drain_timeout()
        with self._state_lock:
            already_closed = self._closed
            self._closed = True
            self._cancel_all_price_alert_cooldowns_locked()
            if self._rt_listener_registered:
                try:
                    self._client.unregister_gold_rt_listener(self._rt_listener)
                except Exception as exc:
                    ops_log(LogSource.RT_RUNTIME, f"unregister gold realtime listener failed: {exc}")
                else:
                    self._rt_listener_registered = False
            if already_closed:
                self._dispatcher.close(timeout=drain_timeout)
                return
        self._dispatcher.wait_for_idle(timeout=drain_timeout)
        self._dispatcher.close(timeout=drain_timeout)

    def _dispatcher_drain_timeout(self) -> float:
        return max(5.0, getattr(self._dispatcher, "drain_timeout_seconds", 5.0))

    @_with_state_lock
    def health_status(self) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        return {
            "available": True,
            "active_alert_count": len(self._state["price_alerts"]),
            "active_callback_count": len(self._state["gold_price_callbacks"]),
            "owned_price_codes": dict(self._owned_price_codes),
            "state_trading_date": str(self._state.get("trading_date") or ""),
        }

    @_with_state_lock
    def register_gold_price_alert(
        self,
        code: str,
        condition: str,
        threshold: float,
        window_minutes: int | None,
        message: str,
        http_callback: HttpCallbackSpec,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        normalized_code = self._client.normalize_gold_code(code)
        normalized_condition = condition.strip().lower()
        if normalized_condition not in {"fastmove", "climb", "fall"}:
            raise ValueError("condition must be one of fastmove, climb, fall")
        if normalized_condition == "fastmove" and (window_minutes is None or window_minutes <= 0):
            raise ValueError("window_minutes is required for fastmove alerts")
        if normalized_condition in {"climb", "fall"} and window_minutes is not None:
            raise ValueError("window_minutes is only supported for fastmove alerts")
        alert_id = f"gold_alert_{_kst_now().strftime('%Y%m%d%H%M%S%f')}"
        record = PriceAlertRecord(
            alert_id=alert_id,
            code=normalized_code,
            condition=normalized_condition,  # type: ignore[arg-type]
            threshold=float(threshold),
            window_minutes=window_minutes,
            message=message,
            http_callback=http_callback,
            created_at=_kst_now().strftime("%Y%m%d%H%M%S"),
        )
        self._retain_price_subscription(normalized_code)
        try:
            self._state["price_alerts"].append(record.to_dict())
            self._persist_state()
        except Exception:
            self._state["price_alerts"] = [
                item for item in self._state["price_alerts"] if item.get("alert_id") != alert_id
            ]
            self._release_price_subscription_safely(normalized_code, alert_id)
            raise
        return {"alert_id": alert_id, "code": normalized_code}

    @_with_state_lock
    def list_gold_price_alerts(self) -> list[dict[str, Any]]:
        self._maybe_cleanup_closed_market()
        prices: dict[str, float] = {}
        items: list[dict[str, Any]] = []
        for raw in self._state["price_alerts"]:
            code = str(raw["code"])
            current_price = self._coerce_float(raw.get("last_price"))
            if current_price is None:
                if code not in prices:
                    try:
                        prices[code] = float(self._client.get_gold_quote_snapshot(code).current_price)
                    except Exception:
                        prices[code] = 0.0
                current_price = prices[code]
            items.append(
                {
                    "alert_id": raw["alert_id"],
                    "code": code,
                    "name": self._gold_name(code),
                    "condition": raw["condition"],
                    "threshold": raw["threshold"],
                    "window_minutes": raw.get("window_minutes"),
                    "message": raw["message"],
                    "httpCallback": raw["httpCallback"],
                    "current_price": current_price,
                    "created_at": raw["created_at"],
                }
            )
        return items

    @_with_state_lock
    def cancel_gold_price_alert(self, alert_id: str | None = None, code: str | None = None) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        if not alert_id and not code:
            raise ValueError("alert_id or code is required")
        normalized_code = self._client.normalize_gold_code(code) if code else None
        removed: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for raw in self._state["price_alerts"]:
            matches_alert = alert_id is not None and raw["alert_id"] == alert_id
            matches_code = normalized_code is not None and raw["code"] == normalized_code
            if matches_alert or matches_code:
                removed.append(raw)
            else:
                retained.append(raw)
        self._state["price_alerts"] = retained
        for raw in removed:
            self._cancel_price_alert_cooldown_locked(str(raw["alert_id"]))
            self._release_price_subscription(str(raw["code"]))
        if removed:
            self._persist_state()
        return {
            "canceled": bool(removed),
            "removed_alerts": len(removed),
            "alert_id": alert_id,
            "code": normalized_code,
        }

    def _price_alert_cooldown_seconds(self, window_minutes: object) -> float:
        try:
            minutes = float(window_minutes)
            seconds_per_minute = float(self.PRICE_ALERT_COOLDOWN_SECONDS_PER_MINUTE)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, minutes * seconds_per_minute)

    def _start_price_alert_cooldown_locked(self, alert_id: str, window_minutes: object) -> None:
        delay = self._price_alert_cooldown_seconds(window_minutes)
        if delay <= 0:
            return
        previous = self._price_alert_cooldown_timers.pop(alert_id, None)
        if previous is not None:
            previous.cancel()
        timer = threading.Timer(delay, self._flush_price_alert_cooldown, args=(alert_id,))
        timer.daemon = True
        self._price_alert_cooldown_timers[alert_id] = timer
        timer.start()

    def _cancel_price_alert_cooldown_locked(self, alert_id: str) -> None:
        timer = self._price_alert_cooldown_timers.pop(alert_id, None)
        if timer is not None:
            timer.cancel()
        self._price_alert_cooldown_pending.pop(alert_id, None)

    def _cancel_all_price_alert_cooldowns_locked(self) -> None:
        for timer in list(self._price_alert_cooldown_timers.values()):
            timer.cancel()
        self._price_alert_cooldown_timers.clear()
        self._price_alert_cooldown_pending.clear()

    @_with_state_lock
    def register_gold_price_callback(
        self,
        code: str,
        step: float,
        http_callback: HttpCallbackSpec,
        price_filter: str | None = None,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        normalized_code = self._client.normalize_gold_code(code)
        normalized_step = float(step)
        if normalized_step <= 0:
            raise ValueError("step must be greater than zero")
        normalized_price_filter = self._normalize_price_callback_filter(price_filter)
        callback_id = f"gold_price_callback_{_kst_now().strftime('%Y%m%d%H%M%S%f')}"
        record = {
            "gold_price_callback_id": callback_id,
            "code": normalized_code,
            "step": normalized_step,
            "price_filter": normalized_price_filter,
            "httpCallback": http_callback.to_dict(),
            "registered_at": _kst_now().strftime("%Y%m%d%H%M%S"),
            "last_price": None,
            "baseline_price": None,
            "last_direction": None,
            "fired_count": 0,
            "last_fired_at": None,
        }
        self._retain_price_subscription(normalized_code)
        try:
            self._state["gold_price_callbacks"].append(record)
            self._persist_state()
        except Exception:
            self._state["gold_price_callbacks"] = [
                item
                for item in self._state["gold_price_callbacks"]
                if item.get("gold_price_callback_id") != callback_id
            ]
            self._release_price_subscription_safely(normalized_code, callback_id)
            raise
        return {
            "gold_price_callback_id": callback_id,
            "code": normalized_code,
            "step": normalized_step,
            "price_filter": normalized_price_filter,
        }

    @_with_state_lock
    def list_gold_price_callbacks(self) -> list[dict[str, Any]]:
        self._maybe_cleanup_closed_market()
        prices: dict[str, float] = {}
        items: list[dict[str, Any]] = []
        for raw in self._state["gold_price_callbacks"]:
            code = str(raw["code"])
            current_price = self._coerce_float(raw.get("last_price"))
            if current_price is None:
                if code not in prices:
                    try:
                        prices[code] = float(self._client.get_gold_quote_snapshot(code).current_price)
                    except Exception:
                        prices[code] = 0.0
                current_price = prices[code]
            items.append(
                {
                    "gold_price_callback_id": raw["gold_price_callback_id"],
                    "code": code,
                    "name": self._gold_name(code),
                    "step": raw["step"],
                    "price_filter": raw.get("price_filter"),
                    "current_price": current_price,
                    "baseline_price": raw.get("baseline_price"),
                    "last_direction": raw.get("last_direction"),
                    "fired_count": int(raw.get("fired_count") or 0),
                    "registered_at": raw["registered_at"],
                    "last_fired_at": raw.get("last_fired_at"),
                    "httpCallback": raw["httpCallback"],
                }
            )
        return items

    @_with_state_lock
    def cancel_gold_price_callback(
        self,
        gold_price_callback_id: str | None = None,
        code: str | None = None,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        if not gold_price_callback_id and not code:
            raise ValueError("gold_price_callback_id or code is required")
        normalized_code = self._client.normalize_gold_code(code) if code else None
        removed: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for raw in self._state["gold_price_callbacks"]:
            matches_callback = (
                gold_price_callback_id is not None
                and raw["gold_price_callback_id"] == gold_price_callback_id
            )
            matches_code = normalized_code is not None and raw["code"] == normalized_code
            if matches_callback or matches_code:
                removed.append(raw)
            else:
                retained.append(raw)
        self._state["gold_price_callbacks"] = retained
        for raw in removed:
            self._release_price_subscription(str(raw["code"]))
        if removed:
            self._persist_state()
        return {
            "canceled": bool(removed),
            "removed_callbacks": len(removed),
            "gold_price_callback_id": gold_price_callback_id,
            "code": normalized_code,
        }

    def _retain_price_subscription(self, code: str, persist: bool = False) -> None:
        previous = self._owned_price_codes.get(code, 0)
        if previous == 0:
            self._client.subscribe_gold_realtime_price(code)
        self._owned_price_codes[code] = previous + 1
        if persist:
            self._persist_state()

    def _release_price_subscription(self, code: str) -> None:
        previous = self._owned_price_codes.get(code, 0)
        if previous > 1:
            self._owned_price_codes[code] = previous - 1
        elif previous == 1:
            self._owned_price_codes.pop(code, None)
            self._client.unsubscribe_gold_realtime_price(code)

    def _release_price_subscription_safely(self, code: str, owner_id: str) -> None:
        try:
            self._release_price_subscription(code)
        except Exception as exc:
            ops_log(LogSource.RT_RUNTIME, f"Failed to roll back gold price RT subscription for {owner_id}: {exc}")

    @_with_state_lock
    def _on_gold_rt_event(self, event: dict[str, Any]) -> None:
        if self._closed:
            return
        self._maybe_cleanup_closed_market()
        rt_type = str(event.get("rt_type") or "")
        if rt_type != "XC":
            return
        self._evaluate_price_alerts(event)
        self._evaluate_gold_price_callbacks(event)

    def _evaluate_price_alerts(self, event: dict[str, Any]) -> None:
        code = str(event.get("code") or "")
        current_price = self._coerce_float(event.get("current_price"))
        if not code or current_price is None or current_price <= 0:
            return
        changed = False
        for index, raw in enumerate(list(self._state["price_alerts"])):
            if raw["code"] != code:
                continue
            updated = dict(raw)
            alert_id = str(updated["alert_id"])
            updated["last_price"] = current_price
            updated["last_eval_at"] = str(event.get("time") or _kst_now().strftime("%H%M%S"))
            if (
                str(updated.get("condition") or "") == "fastmove"
                and alert_id in self._price_alert_cooldown_timers
            ):
                self._price_alert_cooldown_pending[alert_id] = {
                    "code": code,
                    "latest_price": current_price,
                    "latest_eval_at": updated["last_eval_at"],
                }
                self._state["price_alerts"][index] = updated
                changed = True
                continue
            fired = self._check_alert_transition(updated, current_price)
            self._state["price_alerts"][index] = updated
            changed = True
            if fired is not None:
                self._dispatcher.dispatch(self._http_callback_from_dict(updated["httpCallback"]))
                if str(updated.get("condition") or "") == "fastmove":
                    self._start_price_alert_cooldown_locked(alert_id, updated.get("window_minutes"))
        if changed:
            self._persist_state()

    def _flush_price_alert_cooldown(self, alert_id: str) -> None:
        with self._state_lock:
            self._price_alert_cooldown_timers.pop(alert_id, None)
            pending = self._price_alert_cooldown_pending.pop(alert_id, None)
            if self._closed or pending is None:
                return
            self._maybe_cleanup_closed_market()
            for index, raw in enumerate(list(self._state["price_alerts"])):
                if str(raw.get("alert_id") or "") != alert_id:
                    continue
                updated = dict(raw)
                if str(updated.get("condition") or "") != "fastmove":
                    return
                code = str(updated.get("code") or pending.get("code") or "")
                latest_price = self._coerce_float(pending.get("latest_price"))
                if not code or latest_price is None or latest_price <= 0:
                    return
                updated["last_price"] = latest_price
                updated["last_eval_at"] = str(pending.get("latest_eval_at") or _kst_now().strftime("%H%M%S"))
                baseline_price = self._coerce_float(updated.get("baseline_price"))
                if baseline_price is None or not baseline_price:
                    timestamp = _kst_now().strftime("%Y%m%d%H%M%S")
                    updated["baseline_price"] = latest_price
                    updated["baseline_at"] = timestamp
                    self._state["price_alerts"][index] = updated
                    self._persist_state()
                    return
                move_percent = abs((latest_price - baseline_price) / baseline_price * 100.0)
                if move_percent >= float(updated["threshold"]):
                    timestamp = _kst_now().strftime("%Y%m%d%H%M%S")
                    updated["last_triggered_at"] = timestamp
                    updated["baseline_price"] = latest_price
                    updated["baseline_at"] = timestamp
                    dispatch_result = self._dispatcher.dispatch(self._http_callback_from_dict(updated["httpCallback"])) or {}
                    if not dispatch_result.get("queued"):
                        ops_log(LogSource.RT_RUNTIME,
                            f"gold price alert trailing callback queue failed alert_id={alert_id} "
                            f"code={code} error={dispatch_result.get('error')}",
                        )
                    self._start_price_alert_cooldown_locked(alert_id, updated.get("window_minutes"))
                    ops_log(LogSource.RT_RUNTIME,
                        f"gold price alert cooldown trailing fired alert_id={alert_id} "
                        f"code={code} latest_price={latest_price} move_percent={move_percent}",
                    )
                self._state["price_alerts"][index] = updated
                self._persist_state()
                return

    def _evaluate_gold_price_callbacks(self, event: dict[str, Any]) -> None:
        code = str(event.get("code") or "")
        current_price = self._coerce_float(event.get("current_price"))
        if not code or current_price is None or current_price <= 0:
            return
        changed = False
        timestamp = _kst_now().strftime("%Y%m%d%H%M%S")
        for index, raw in enumerate(list(self._state["gold_price_callbacks"])):
            if raw["code"] != code:
                continue
            updated = dict(raw)
            updated["last_price"] = current_price
            baseline_price = self._coerce_float(updated.get("baseline_price"))
            if baseline_price is None:
                updated["baseline_price"] = current_price
                self._state["gold_price_callbacks"][index] = updated
                changed = True
                continue
            step = float(updated["step"])
            delta = current_price - baseline_price
            if abs(delta) >= step:
                direction = "상향" if delta > 0 else "하향"
                try:
                    filter_allows = self._price_callback_filter_allows(updated.get("price_filter"), current_price)
                except ValueError as exc:
                    ops_log(LogSource.RT_RUNTIME,
                        f"gold price callback skipped callback_id={updated.get('gold_price_callback_id', '')} "
                        f"code={code} reason=invalid_price_filter error={exc}",
                    )
                    filter_allows = False
                if filter_allows:
                    updated["baseline_price"] = current_price
                    updated["last_direction"] = direction
                    updated["fired_count"] = int(updated.get("fired_count") or 0) + 1
                    updated["last_fired_at"] = timestamp
                    rendered_callback = self._render_http_callback(
                        self._http_callback_from_dict(updated["httpCallback"]),
                        self._gold_price_callback_replacements(code, current_price, direction),
                    )
                    self._dispatcher.dispatch(rendered_callback)
            self._state["gold_price_callbacks"][index] = updated
            changed = True
        if changed:
            self._persist_state()

    def _check_alert_transition(self, raw: dict[str, Any], current_price: float) -> PriceAlertFiredPayload | None:
        condition = str(raw["condition"])
        threshold = float(raw["threshold"])
        timestamp = _kst_now().strftime("%Y%m%d%H%M%S")
        if condition == "climb":
            previous_side = raw.get("last_side")
            current_side = "above" if current_price >= threshold else "below"
            raw["last_side"] = current_side
            if previous_side == "below" and current_side == "above":
                raw["last_triggered_at"] = timestamp
                return self._build_alert_payload(raw, current_price, timestamp)
            return None
        if condition == "fall":
            previous_side = raw.get("last_side")
            current_side = "below" if current_price <= threshold else "above"
            raw["last_side"] = current_side
            if previous_side == "above" and current_side == "below":
                raw["last_triggered_at"] = timestamp
                return self._build_alert_payload(raw, current_price, timestamp)
            return None
        baseline_price = self._coerce_float(raw.get("baseline_price"))
        baseline_at = str(raw.get("baseline_at") or "")
        window_minutes = int(raw.get("window_minutes") or 0)
        now = _kst_now()
        if baseline_price is None or not baseline_at:
            raw["baseline_price"] = current_price
            raw["baseline_at"] = timestamp
            return None
        baseline_dt = self._parse_compact_kst(baseline_at)
        if baseline_dt is None or (now - baseline_dt).total_seconds() > window_minutes * 60:
            raw["baseline_price"] = current_price
            raw["baseline_at"] = timestamp
            return None
        move_percent = abs((current_price - baseline_price) / baseline_price * 100.0) if baseline_price else 0.0
        last_triggered = self._parse_compact_kst(str(raw.get("last_triggered_at") or ""))
        in_cooldown = last_triggered is not None and (now - last_triggered).total_seconds() <= window_minutes * 60
        if move_percent >= threshold and not in_cooldown:
            raw["last_triggered_at"] = timestamp
            raw["baseline_price"] = current_price
            raw["baseline_at"] = timestamp
            return self._build_alert_payload(raw, current_price, timestamp)
        return None

    def _build_alert_payload(
        self,
        raw: dict[str, Any],
        current_price: float,
        triggered_at: str,
    ) -> PriceAlertFiredPayload:
        return PriceAlertFiredPayload(
            event_type="gold_price_alert",
            alert_id=str(raw["alert_id"]),
            code=str(raw["code"]),
            condition=str(raw["condition"]),  # type: ignore[arg-type]
            threshold=float(raw["threshold"]),
            current_price=current_price,
            message=str(raw["message"]),
            triggered_at=triggered_at,
        )

    def _restore_same_day_state(self) -> None:
        self._maybe_cleanup_closed_market()
        for raw in self._state["price_alerts"]:
            code = str(raw.get("code") or "")
            if not code:
                continue
            try:
                self._retain_price_subscription(code, persist=False)
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME,
                    f"gold price alert restore failed code={code}: {exc.__class__.__name__}: {exc}",
                )
                details = {
                    "subscription_kind": "gold_price_alert",
                    "code": code,
                    "alert_id": str(raw.get("alert_id") or ""),
                    "error": str(exc),
                }
                client_error = self._get_client_rt_error_details()
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_restore_failed",
                    message=f"금현물 가격 알람 구독 복구 실패: code={code}, error={exc}",
                    details=details,
                )
                self._dispatcher.wait_for_idle(timeout=15.0)
                raise
        for raw in self._state["gold_price_callbacks"]:
            code = str(raw.get("code") or "")
            if not code:
                continue
            try:
                self._retain_price_subscription(code, persist=False)
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME,
                    f"gold price callback restore failed code={code}: {exc.__class__.__name__}: {exc}",
                )
                details = {
                    "subscription_kind": "gold_price_callback",
                    "code": code,
                    "gold_price_callback_id": str(raw.get("gold_price_callback_id") or ""),
                    "error": str(exc),
                }
                client_error = self._get_client_rt_error_details()
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_restore_failed",
                    message=f"금현물 Step callback 구독 복구 실패: code={code}, error={exc}",
                    details=details,
                )
                self._dispatcher.wait_for_idle(timeout=15.0)
                raise

    def _maybe_cleanup_closed_market(self) -> None:
        now = _kst_now()
        trading_date = self._state.get("trading_date") or self._store.current_trading_date()
        if trading_date != now.strftime("%Y%m%d"):
            self._clear_runtime_state()
            return
        if self._restored_from_disk and now.time() >= self.MARKET_CLOSE_TIME and self._has_runtime_state():
            self._clear_runtime_state()

    def _has_runtime_state(self) -> bool:
        return bool(self._state["price_alerts"] or self._state["gold_price_callbacks"])

    def _clear_runtime_state(self) -> None:
        if not self._owned_price_codes and not self._has_runtime_state():
            return
        ops_log(LogSource.MANAGE,
            f"clear gold runtime state begin owned_price_codes={len(self._owned_price_codes)} "
            f"price_alerts={len(self._state['price_alerts'])} "
            f"gold_price_callbacks={len(self._state['gold_price_callbacks'])}",
        )
        for code in list(self._owned_price_codes):
            try:
                while self._owned_price_codes.get(code, 0) > 0:
                    self._release_price_subscription(code)
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"Failed to clear gold price subscription for {code}: {exc}")
        self._owned_price_codes.clear()
        self._state = self._store.load()
        self._state["price_alerts"] = []
        self._state["gold_price_callbacks"] = []
        self._cancel_all_price_alert_cooldowns_locked()
        self._restored_from_disk = False
        self._store.clear()
        ops_log(LogSource.MANAGE, "clear gold runtime state complete")

    def _persist_state(self) -> None:
        self._store.save(self._state)

    def _dispatch_system_event(self, event_type: str, message: str, details: dict[str, Any] | None = None) -> None:
        if self._system_event_recorder is None:
            return
        self._system_event_recorder(event_type, message, details)

    def _get_client_rt_error_details(self) -> dict[str, Any] | None:
        getter = getattr(self._client, "get_last_rt_error_details", None)
        if not callable(getter):
            return None
        try:
            details = getter()
        except Exception:
            return None
        return dict(details) if details else None

    @staticmethod
    def _http_callback_from_dict(raw: dict[str, Any]) -> HttpCallbackSpec:
        return HttpCallbackSpec(
            method=str(raw["method"]),
            url=str(raw["url"]),
            headers={str(key): str(value) for key, value in dict(raw.get("headers") or {}).items()},
            body=raw.get("body"),
            body_format=raw.get("bodyFormat"),
        )

    def _gold_price_callback_replacements(self, code: str, price: float, direction: str) -> dict[str, str]:
        raw_price = self._format_compact_decimal(price)
        return {
            "code": code,
            "name": self._gold_name(code),
            "price": format_display_decimal(price),
            "priceRaw": raw_price,
            "price_raw": raw_price,
            "direction": direction,
        }

    def _render_http_callback(
        self,
        callback: HttpCallbackSpec,
        replacements: dict[str, str],
    ) -> HttpCallbackSpec:
        if callback.body is None:
            return callback
        return HttpCallbackSpec(
            method=callback.method,
            url=callback.url,
            headers=dict(callback.headers),
            body=self._render_template_value(callback.body, replacements),
            body_format=callback.body_format,
        )

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

    def _normalize_price_callback_filter(self, price_filter: str | None) -> str | None:
        parsed = self._parse_price_callback_filter(price_filter)
        if parsed is None:
            return None
        threshold, operator = parsed
        return f"{self._format_compact_decimal(threshold)}{operator}"

    def _price_callback_filter_allows(self, price_filter: object, current_price: float) -> bool:
        parsed = self._parse_price_callback_filter(price_filter)
        if parsed is None:
            return True
        threshold, operator = parsed
        if operator == "+":
            return current_price >= threshold
        return current_price <= threshold

    @staticmethod
    def _parse_price_callback_filter(price_filter: object) -> tuple[float, str] | None:
        if price_filter is None:
            return None
        if not isinstance(price_filter, str):
            raise ValueError("price_filter must be like '70000+', '70000-', or null")
        text = price_filter.strip()
        match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([+-])", text)
        if match is None:
            raise ValueError("price_filter must be like '70000+', '70000-', or null")
        threshold = float(match.group(1))
        if threshold <= 0:
            raise ValueError("price_filter threshold must be greater than zero")
        return threshold, match.group(2)

    def _gold_name(self, code: str) -> str:
        try:
            for product in self._client.list_gold_products():
                if product.code == code:
                    return product.name
        except Exception:
            pass
        return ""

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_compact_decimal(value: float) -> str:
        if float(value).is_integer():
            return str(int(value))
        return f"{value:.10f}".rstrip("0").rstrip(".")

    @staticmethod
    def _parse_compact_kst(value: str) -> datetime | None:
        try:
            return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=KST)
        except ValueError:
            return None
