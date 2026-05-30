from __future__ import annotations

import copy
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
    DisclosureSubscriptionRecord,
    FallSafeRecord,
    HttpCallbackSpec,
    NewsSubscriptionRecord,
    PriceAlertFiredPayload,
    PriceAlertRecord,
    RealtimeEventPayload,
    StockPriceCallbackRecord,
    SystemCallbackRecord,
    UnifiedRuntimeState,
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
    def wrapped(self: "RuntimeStateManager", *args: Any, **kwargs: Any) -> Any:
        with self._state_lock:
            return method(self, *args, **kwargs)

    return wrapped


class UnifiedRuntimeStateStore:
    VERSION = 1

    def __init__(self, state_dir: Path) -> None:
        self._state_dir = state_dir

    @property
    def state_dir(self) -> Path:
        return self._state_dir

    def current_trading_date(self) -> str:
        return _kst_now().strftime("%Y%m%d")

    def current_state_path(self) -> Path:
        return self._state_dir / f"subscribtion_state_{self.current_trading_date()}.json"

    def load(self) -> dict[str, Any]:
        path = self.current_state_path()
        if not path.exists():
            return self._default_state()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            ops_log(LogSource.MANAGE, f"Failed to load runtime state {path}: {exc}")
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
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        self._cleanup_old_files(path)
        return path

    def clear(self) -> None:
        path = self.current_state_path()
        if path.exists():
            try:
                path.unlink()
            except OSError as exc:
                ops_log(LogSource.MANAGE, f"Failed to remove runtime state {path}: {exc}")

    def _cleanup_old_files(self, keep_path: Path) -> None:
        if not self._state_dir.exists():
            return
        for path in self._state_dir.glob("subscribtion_state_*.json"):
            if path == keep_path:
                continue
            try:
                path.unlink()
            except OSError:
                continue

    def _default_state(self) -> dict[str, Any]:
        return UnifiedRuntimeState(
            version=self.VERSION,
            trading_date=self.current_trading_date(),
            updated_at="",
            subscriptions={"disclosures": [], "news": []},
            price_alerts=[],
            fall_safes=[],
            stock_price_callbacks=[],
        ).to_dict()

    def _merge_defaults(self, payload: dict[str, Any]) -> dict[str, Any]:
        base = self._default_state()
        base.update({key: value for key, value in payload.items() if key in base})
        subscriptions = payload.get("subscriptions") if isinstance(payload.get("subscriptions"), dict) else {}
        base["subscriptions"] = {
            "disclosures": list(subscriptions.get("disclosures", [])),
            "news": list(subscriptions.get("news", [])),
        }
        base["price_alerts"] = list(payload.get("price_alerts", []))
        base["fall_safes"] = list(payload.get("fall_safes", []))
        base["stock_price_callbacks"] = list(payload.get("stock_price_callbacks", []))
        return base


class PersistentSubscriptionStore:
    VERSION = 1
    FILE_NAME = "subscription_state.json"

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
            ops_log(LogSource.MANAGE, f"Failed to load subscription state {path}: {exc}")
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
            "subscriptions": {"disclosures": [], "news": []},
            "system_callbacks": [],
        }

    def _merge_defaults(self, payload: dict[str, Any]) -> dict[str, Any]:
        base = self._default_state()
        subscriptions = payload.get("subscriptions") if isinstance(payload.get("subscriptions"), dict) else {}
        base["subscriptions"] = {
            "disclosures": list(subscriptions.get("disclosures", [])),
            "news": list(subscriptions.get("news", [])),
        }
        base["system_callbacks"] = list(payload.get("system_callbacks", []))
        if "version" in payload:
            base["version"] = payload["version"]
        if "updated_at" in payload:
            base["updated_at"] = payload["updated_at"]
        return base

    def exists(self) -> bool:
        return self.path().exists()


class RuntimeStateManager:
    MARKET_CLOSE_TIME = time(hour=18, minute=0)
    PRICE_ALERT_DEBOUNCE_SECONDS = 10.0
    PRICE_ALERT_COOLDOWN_SECONDS_PER_MINUTE = 60.0
    STOCK_PRICE_CALLBACK_DEBOUNCE_SECONDS = 10.0
    _REPLACEMENT_PATTERN = re.compile(r"\{\{([a-zA-Z0-9_]+)\}\}")

    def __init__(
        self,
        client: Any,
        state_dir: str | os.PathLike[str] | None = None,
        logger: Any | None = None,
        fall_safe_executor: Callable[[str, str, int], dict[str, Any]] | None = None,
        restore_realtime: bool = True,
        system_event_recorder: Callable[[str, str, dict[str, Any] | None], None] | None = None,
        system_callbacks_configurer: Callable[[list[dict[str, Any]]], None] | None = None,
    ) -> None:
        ops_log(LogSource.STARTUP_RUNTIME, "RuntimeStateManager.__init__ entered")
        self._client = client
        self._state_lock = threading.RLock()
        self._system_event_recorder = system_event_recorder
        self._system_callbacks_configurer = system_callbacks_configurer
        resolved_dir = Path(state_dir) if state_dir is not None else Path(".runtime")
        ops_log(LogSource.STARTUP_RUNTIME, f"resolved_state_dir={resolved_dir}")
        self._store = UnifiedRuntimeStateStore(resolved_dir)
        self._subscription_store = PersistentSubscriptionStore(resolved_dir)
        ops_log(LogSource.STARTUP_RUNTIME, "checking legacy same-day subscription migration")
        self._migrate_legacy_subscriptions_if_needed()
        self._dispatcher = CallbackDispatcher()
        self._rt_listener_registered = False
        self._rt_listener = self._on_rt_event
        self._rt_batch_listener = self._on_rt_events
        self._rt_listener_mode: str | None = None
        self._closed = False
        ops_log(LogSource.STARTUP_RUNTIME, f"loading same-day runtime state path={self._store.current_state_path()}")
        self._state = self._store.load()
        ops_log(LogSource.STARTUP_RUNTIME, f"loading persistent subscription state path={self._subscription_store.path()}")
        self._subscription_state = self._subscription_store.load()
        self._subscription_id_counter = 0
        ops_log(LogSource.STARTUP_RUNTIME, "normalizing persistent subscription ids")
        self._normalize_persistent_subscription_ids()
        self._sync_system_callbacks_to_scripter()
        self._state["subscriptions"] = self._subscription_state["subscriptions"]
        self._restored_from_disk = self._has_runtime_state()
        self._news_feed_active = False
        self._disclosure_feed_active = False
        self._owned_price_codes: dict[str, int] = {}
        self._price_alert_cooldown_timers: dict[str, threading.Timer] = {}
        self._price_alert_cooldown_pending: dict[str, dict[str, Any]] = {}
        self._recovery_fail_timers: dict[str, threading.Timer] = {}
        self._uptrend_end_timers: dict[str, threading.Timer] = {}
        self._stock_price_callback_debounce_timers: dict[str, threading.Timer] = {}
        self._stock_price_callback_debounce_pending: dict[str, dict[str, Any]] = {}
        self._fall_safe_executor = fall_safe_executor
        ops_log(LogSource.STARTUP_RUNTIME, "registering realtime listener")
        self._register_realtime_listener()
        ops_log(LogSource.STARTUP_RUNTIME,
            "state loaded "
            f"disclosures={len(self._state['subscriptions']['disclosures'])} "
            f"news={len(self._state['subscriptions']['news'])} "
            f"price_alerts={len(self._state['price_alerts'])} "
            f"fall_safes={len(self._state['fall_safes'])} "
            f"stock_price_callbacks={len(self._state['stock_price_callbacks'])} "
            f"system_callbacks={len(self._subscription_state['system_callbacks'])} "
            f"restored_from_disk={self._restored_from_disk}",
        )
        if restore_realtime:
            ops_log(LogSource.STARTUP_RUNTIME, "restoring same-day realtime state")
            self._restore_same_day_state()
        else:
            ops_log(LogSource.STARTUP_RUNTIME, "same-day realtime restore skipped by caller")
        ops_log(LogSource.STARTUP_RUNTIME, "RuntimeStateManager.__init__ complete")

    def close(self) -> None:
        drain_timeout = self._dispatcher_drain_timeout()
        with self._state_lock:
            already_closed = self._closed
            self._closed = True
            self._cancel_all_price_alert_cooldowns_locked()
            self._cancel_all_recovery_fail_timers_locked()
            self._cancel_all_uptrend_end_timers_locked()
            self._cancel_all_stock_price_callback_debounces_locked()
            if self._rt_listener_registered:
                self._unregister_realtime_listener()
            if already_closed:
                self._dispatcher.close(timeout=drain_timeout)
                return
        self._dispatcher.wait_for_idle(timeout=drain_timeout)
        self._dispatcher.close(timeout=drain_timeout)

    def _dispatcher_drain_timeout(self) -> float:
        return max(5.0, getattr(self._dispatcher, "drain_timeout_seconds", 5.0))

    def _register_realtime_listener(self) -> None:
        register_batch = getattr(self._client, "register_rt_batch_listener", None)
        if callable(register_batch):
            register_batch(self._rt_batch_listener)
            self._rt_listener_mode = "batch"
        else:
            self._client.register_rt_listener(self._rt_listener)
            self._rt_listener_mode = "single"
        self._rt_listener_registered = True
        ops_log(LogSource.STARTUP_RUNTIME, f"realtime listener registered mode={self._rt_listener_mode}")

    def _unregister_realtime_listener(self) -> None:
        try:
            if self._rt_listener_mode == "batch":
                unregister_batch = getattr(self._client, "unregister_rt_batch_listener", None)
                if not callable(unregister_batch):
                    raise RuntimeError("client has no unregister_rt_batch_listener")
                unregister_batch(self._rt_batch_listener)
            else:
                self._client.unregister_rt_listener(self._rt_listener)
        except Exception as exc:
            ops_log(
                LogSource.STARTUP_RUNTIME,
                f"unregister realtime listener failed during close: {exc.__class__.__name__}: {exc}",
            )
        else:
            self._rt_listener_registered = False
            self._rt_listener_mode = None

    @_with_state_lock
    def subscribe_disclosure(
        self,
        code: str,
        http_callback: HttpCallbackSpec,
        dev_callback: bool = False,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        normalized_code = self._client.normalize_stock_code(code)
        registered_at = _kst_now().strftime("%Y%m%d%H%M%S")
        existing = self._state["subscriptions"]["disclosures"]
        ops_log(LogSource.RT_RUNTIME,
            f"N2 subscribe_disclosure requested code={normalized_code} "
            f"dev_callback={dev_callback} existing_count={len(existing)} feed_active={self._disclosure_feed_active}",
        )
        existing_record = next(
            (
                item
                for item in existing
                if item["code"] == normalized_code and item["httpCallback"] == http_callback.to_dict()
            ),
            None,
        )
        subscription_already_registered = existing_record is not None
        if not subscription_already_registered:
            record = DisclosureSubscriptionRecord(
                subscription_id=self._next_subscription_id("disc_sub"),
                code=normalized_code,
                http_callback=http_callback,
                registered_at=registered_at,
            ).to_dict()
            existing_record = record
        already_indi_registered = self._disclosure_feed_active
        rt_disclosure_registered_now = False
        if not self._disclosure_feed_active:
            ops_log(LogSource.RT_RUNTIME, f"N2 feed retain begin code={normalized_code}")
            try:
                feed_result = self._client.subscribe_disclosure_feed(normalized_code)
                already_indi_registered = (
                    bool(feed_result.get("already_indi_registered", feed_result.get("already_subscribed")))
                    if isinstance(feed_result, dict)
                    else False
                )
                rt_disclosure_registered_now = (
                    bool(feed_result.get("rt_disclosure_registered_now"))
                    if isinstance(feed_result, dict)
                    else False
                )
                self._disclosure_feed_active = True
                ops_log(LogSource.RT_RUNTIME,
                    f"N2 feed retain success code={normalized_code} "
                    f"already_indi_registered={already_indi_registered} "
                    f"rt_disclosure_registered_now={rt_disclosure_registered_now}",
                )
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME,
                    f"N2 feed retain failed code={normalized_code} error={exc.__class__.__name__}: {exc}",
                )
                client_error = self._client.get_last_rt_error_details()
                details = {
                    "subscription_kind": "disclosure",
                    "code": normalized_code,
                    "error": str(exc),
                }
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_subscribe_failed",
                    message=f"공시 구독 등록 실패: code={normalized_code}, error={exc}",
                    details=details,
                )
                raise
        if not subscription_already_registered:
            existing.append(record)
            self._persist_subscriptions()
            ops_log(LogSource.RT_RUNTIME,
                f"N2 logical subscription persisted subscription_id={existing_record['subscription_id']} "
                f"code={normalized_code} total_count={len(existing)}",
            )
        else:
            ops_log(LogSource.RT_RUNTIME,
                f"N2 logical subscription already exists subscription_id={existing_record['subscription_id']} "
                f"code={normalized_code}",
            )
        result = {
            "subscribed": True,
            "rt_type": "N2",
            "subscription_id": existing_record["subscription_id"],
            "code": normalized_code,
            "already_subscribed": subscription_already_registered,
            "already_indi_registered": already_indi_registered,
            "rt_disclosure_registered_now": rt_disclosure_registered_now,
            "rt_subscriptions": self._rt_subscription_statuses(),
            "message": "disclosure subscription registered",
        }
        if dev_callback:
            result["dev_callback"] = self._dispatch_dev_disclosure_callback(normalized_code, http_callback)
        ops_log(LogSource.RT_RUNTIME,
            f"N2 subscribe_disclosure complete subscription_id={existing_record['subscription_id']} "
            f"already_subscribed={subscription_already_registered} feed_active={self._disclosure_feed_active}",
        )
        return result

    @_with_state_lock
    def unsubscribe_disclosure(self, subscription_id: str) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        existing = self._state["subscriptions"]["disclosures"]
        removed: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for item in existing:
            if item.get("subscription_id") == subscription_id:
                removed.append(item)
            else:
                retained.append(item)
        removed_count = len(removed)
        if removed_count:
            self._state["subscriptions"]["disclosures"] = retained
            self._persist_subscriptions()
        ops_log(LogSource.RT_RUNTIME,
            f"N2 unsubscribe_disclosure subscription_id={subscription_id} removed_count={removed_count} "
            f"remaining_count={len(self._state['subscriptions']['disclosures'])} feed_active={self._disclosure_feed_active}",
        )
        return {
            "subscribed": False,
            "rt_type": "N2",
            "subscription_id": subscription_id,
            "removed_subscriptions": removed_count,
            "message": "disclosure subscription removed" if removed_count else "disclosure subscription was not registered",
        }

    @_with_state_lock
    def subscribe_news(
        self,
        types: list[str],
        http_callback: HttpCallbackSpec,
        code: str | None = None,
        dev_callback: bool = False,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        normalized_types = self._normalize_news_types(types)
        normalized_code = self._client.normalize_stock_code(code) if code else None
        registered_at = _kst_now().strftime("%Y%m%d%H%M%S")
        existing = self._state["subscriptions"]["news"]
        ops_log(LogSource.RT_RUNTIME,
            f"N0 subscribe_news requested subject={normalized_code or '*'} types={','.join(normalized_types)} "
            f"dev_callback={dev_callback} existing_count={len(existing)} feed_active={self._news_feed_active}",
        )
        existing_record = next(
            (
                item
                for item in existing
                if item["types"] == normalized_types
                and item.get("code") == normalized_code
                and item["httpCallback"] == http_callback.to_dict()
            ),
            None,
        )
        subscription_already_registered = existing_record is not None
        if not subscription_already_registered:
            record = NewsSubscriptionRecord(
                subscription_id=self._next_subscription_id("news_sub"),
                types=normalized_types,
                code=normalized_code,
                http_callback=http_callback,
                registered_at=registered_at,
            ).to_dict()
            existing_record = record
        already_indi_registered = self._news_feed_active
        rt_news_registered_now = False
        if not self._news_feed_active:
            ops_log(LogSource.RT_RUNTIME, f"N0 feed retain begin subject={normalized_code or '*'}")
            try:
                feed_result = self._client.subscribe_news_feed(normalized_code)
                already_indi_registered = (
                    bool(feed_result.get("already_indi_registered", feed_result.get("already_subscribed")))
                    if isinstance(feed_result, dict)
                    else False
                )
                rt_news_registered_now = (
                    bool(feed_result.get("rt_news_registered_now"))
                    if isinstance(feed_result, dict)
                    else False
                )
                self._news_feed_active = True
                ops_log(LogSource.RT_RUNTIME,
                    f"N0 feed retain success subject={normalized_code or '*'} "
                    f"already_indi_registered={already_indi_registered} "
                    f"rt_news_registered_now={rt_news_registered_now}",
                )
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME,
                    f"N0 feed retain failed subject={normalized_code or '*'} error={exc.__class__.__name__}: {exc}",
                )
                client_error = self._client.get_last_rt_error_details()
                details = {
                    "subscription_kind": "news",
                    "code": normalized_code,
                    "subject": str(normalized_code or "*"),
                    "types": list(normalized_types),
                    "error": str(exc),
                }
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_subscribe_failed",
                    message=f"뉴스 구독 등록 실패: subject={str(normalized_code or '*')}, error={exc}",
                    details=details,
                )
                raise
        if not subscription_already_registered:
            existing.append(record)
            self._persist_subscriptions()
            ops_log(LogSource.RT_RUNTIME,
                f"N0 logical subscription persisted subscription_id={existing_record['subscription_id']} "
                f"subject={normalized_code or '*'} types={','.join(normalized_types)} total_count={len(existing)}",
            )
        else:
            ops_log(LogSource.RT_RUNTIME,
                f"N0 logical subscription already exists subscription_id={existing_record['subscription_id']} "
                f"subject={normalized_code or '*'} types={','.join(normalized_types)}",
            )
        result = {
            "subscribed": True,
            "rt_type": "N0",
            "subscription_id": existing_record["subscription_id"],
            "types": normalized_types,
            "code": normalized_code,
            "already_subscribed": subscription_already_registered,
            "already_indi_registered": already_indi_registered,
            "rt_news_registered_now": rt_news_registered_now,
            "rt_subscriptions": self._rt_subscription_statuses(),
            "message": "news subscription registered",
        }
        if dev_callback:
            result["dev_callback"] = self._dispatch_dev_news_callback(normalized_code, normalized_types[0], http_callback)
        ops_log(LogSource.RT_RUNTIME,
            f"N0 subscribe_news complete subscription_id={existing_record['subscription_id']} "
            f"already_subscribed={subscription_already_registered} feed_active={self._news_feed_active}",
        )
        return result

    def _rt_subscription_statuses(self) -> dict[str, dict[str, bool]]:
        return {
            "N0": {
                "active": self._news_feed_active,
            },
            "N2": {
                "active": self._disclosure_feed_active,
            },
        }

    @_with_state_lock
    def unsubscribe_news(self, subscription_id: str) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        existing = self._state["subscriptions"]["news"]
        removed: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for item in existing:
            if item.get("subscription_id") == subscription_id:
                removed.append(item)
            else:
                retained.append(item)
        removed_count = len(removed)
        if removed_count:
            self._state["subscriptions"]["news"] = retained
            self._persist_subscriptions()
        ops_log(LogSource.RT_RUNTIME,
            f"N0 unsubscribe_news subscription_id={subscription_id} removed_count={removed_count} "
            f"remaining_count={len(self._state['subscriptions']['news'])} feed_active={self._news_feed_active}",
        )
        return {
            "subscribed": False,
            "rt_type": "N0",
            "subscription_id": subscription_id,
            "removed_subscriptions": removed_count,
            "message": "news subscription removed" if removed_count else "news subscription was not registered",
        }

    @_with_state_lock
    def list_disclosure_subscriptions(self) -> list[dict[str, Any]]:
        return [
            {
                "code": raw["code"],
                "name": self._stock_name(str(raw["code"])),
                "subscription_id": raw.get("subscription_id", ""),
                "httpCallback": raw["httpCallback"],
                "registered_at": raw.get("registered_at", ""),
                "last_event_at": raw.get("last_event_at"),
                "evaluated_event_count": int(raw.get("evaluated_event_count") or 0),
            }
            for raw in self._state["subscriptions"]["disclosures"]
        ]

    @_with_state_lock
    def list_news_subscriptions(self) -> list[dict[str, Any]]:
        return [
            {
                "types": list(raw["types"]),
                "code": raw.get("code"),
                "name": self._stock_name(str(raw.get("code") or "")) if raw.get("code") else "",
                "subscription_id": raw.get("subscription_id", ""),
                "httpCallback": raw["httpCallback"],
                "registered_at": raw.get("registered_at", ""),
                "last_event_at": raw.get("last_event_at"),
                "evaluated_event_count": int(raw.get("evaluated_event_count") or 0),
            }
            for raw in self._state["subscriptions"]["news"]
        ]

    @_with_state_lock
    def register_system_callback(self, http_callback: HttpCallbackSpec) -> dict[str, Any]:
        registered_at = _kst_now().strftime("%Y%m%d%H%M%S")
        existing = self._subscription_state["system_callbacks"]
        existing_record = next((item for item in existing if item["httpCallback"] == http_callback.to_dict()), None)
        already_registered = existing_record is not None
        if not already_registered:
            record = SystemCallbackRecord(
                system_callback_id=self._next_subscription_id("sys_cb"),
                http_callback=http_callback,
                registered_at=registered_at,
            ).to_dict()
            candidate = [*existing, record]
            self._replace_system_callback_configs(candidate, existing)
            existing_record = record
        return {
            "registered": True,
            "system_callback_id": existing_record["system_callback_id"],
            "already_registered": already_registered,
            "message": "system callback registered",
        }

    @_with_state_lock
    def list_system_callbacks(self) -> list[dict[str, Any]]:
        return self._system_callback_configs()

    def _system_callback_configs(self, callbacks: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        source = self._subscription_state["system_callbacks"] if callbacks is None else callbacks
        return [
            {
                "system_callback_id": raw.get("system_callback_id", ""),
                "httpCallback": copy.deepcopy(raw["httpCallback"]),
                "registered_at": raw.get("registered_at", ""),
            }
            for raw in source
        ]

    def _sync_system_callbacks_to_scripter(self, callbacks: list[dict[str, Any]] | None = None) -> None:
        if self._system_callbacks_configurer is None:
            return
        configs = callbacks if callbacks is not None else self._system_callback_configs()
        self._system_callbacks_configurer(copy.deepcopy(configs))

    def _replace_system_callback_configs(
        self,
        candidate: list[dict[str, Any]],
        previous: list[dict[str, Any]],
    ) -> None:
        self._subscription_state["system_callbacks"] = candidate
        try:
            self._persist_subscriptions()
        except Exception:
            self._subscription_state["system_callbacks"] = previous
            raise
        try:
            self._sync_system_callbacks_to_scripter(self._system_callback_configs(candidate))
        except Exception:
            self._subscription_state["system_callbacks"] = previous
            self._persist_and_sync_system_callback_rollback()
            raise

    def _persist_and_sync_system_callback_rollback(self) -> None:
        try:
            self._persist_subscriptions()
        except Exception as persist_exc:
            ops_log(LogSource.MANAGE,
                f"system callback config rollback persist failed: {persist_exc.__class__.__name__}: {persist_exc}",
            )
        try:
            self._sync_system_callbacks_to_scripter()
        except Exception as sync_exc:
            ops_log(LogSource.MANAGE,
                f"system callback config rollback sync failed: {sync_exc.__class__.__name__}: {sync_exc}",
            )

    @_with_state_lock
    def unregister_system_callback(self, system_callback_id: str) -> dict[str, Any]:
        existing = self._subscription_state["system_callbacks"]
        removed = [item for item in existing if item.get("system_callback_id") == system_callback_id]
        if removed:
            candidate = [
                item for item in existing if item.get("system_callback_id") != system_callback_id
            ]
            self._replace_system_callback_configs(candidate, existing)
        return {
            "registered": False,
            "system_callback_id": system_callback_id,
            "removed_callbacks": len(removed),
            "message": "system callback removed" if removed else "system callback was not registered",
        }

    @_with_state_lock
    def register_price_alert(
        self,
        code: str,
        condition: str,
        threshold: float,
        window_minutes: int | None,
        message: str,
        http_callback: HttpCallbackSpec,
        debounce_seconds: float | None = None,
        once_only: bool = False,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        normalized_code = self._client.normalize_stock_code(code)
        normalized_condition = condition.strip().lower()
        if normalized_condition not in {"fastmove", "climb", "fall"}:
            raise ValueError("condition must be one of fastmove, climb, fall")
        if normalized_condition == "fastmove" and (window_minutes is None or window_minutes <= 0):
            raise ValueError("window_minutes is required for fastmove alerts")
        if normalized_condition in {"climb", "fall"} and window_minutes is not None:
            raise ValueError("window_minutes is only supported for fastmove alerts")
        normalized_debounce_seconds = self._normalize_price_alert_debounce_seconds(
            normalized_condition,
            debounce_seconds,
        )
        alert_id = f"alert_{_kst_now().strftime('%Y%m%d%H%M%S%f')}"
        ops_log(LogSource.RT_RUNTIME,
            f"register_price_alert requested alert_id={alert_id} code={normalized_code} "
            f"condition={normalized_condition} threshold={float(threshold)} "
            f"existing_count={len(self._state['price_alerts'])}",
        )
        record = PriceAlertRecord(
            alert_id=alert_id,
            code=normalized_code,
            condition=normalized_condition,  # type: ignore[arg-type]
            threshold=float(threshold),
            window_minutes=window_minutes,
            message=message,
            http_callback=http_callback,
            created_at=_kst_now().strftime("%Y%m%d%H%M%S"),
            debounce_seconds=normalized_debounce_seconds,
            once_only=bool(once_only),
        )
        ops_log(LogSource.RT_RUNTIME, f"price alert retain price RT begin alert_id={alert_id} code={normalized_code}")
        self._retain_price_subscription(normalized_code)
        try:
            self._state["price_alerts"].append(record.to_dict())
            self._persist_state()
            ops_log(LogSource.RT_RUNTIME,
                f"price alert persisted alert_id={alert_id} code={normalized_code} "
                f"total_count={len(self._state['price_alerts'])}",
            )
        except Exception:
            ops_log(LogSource.RT_RUNTIME, f"price alert persist failed; rollback begin alert_id={alert_id} code={normalized_code}")
            self._state["price_alerts"] = [
                item for item in self._state["price_alerts"] if item.get("alert_id") != alert_id
            ]
            self._release_price_subscription_safely(normalized_code, alert_id)
            raise
        return {
            "alert_id": alert_id,
            "code": normalized_code,
            "debounce_seconds": normalized_debounce_seconds,
            "once_only": bool(once_only),
        }

    @_with_state_lock
    def register_recovery_fail_alert(
        self,
        code: str,
        breach_price: float,
        recovery_price: float,
        failure_minutes: float,
        recovery_minutes: float,
        valid_after: str,
        http_callback: HttpCallbackSpec,
        once_only: bool = True,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        normalized_code = self._client.normalize_stock_code(code)
        normalized_breach_price = self._normalize_positive_float("breach_price", breach_price)
        normalized_recovery_price = self._normalize_positive_float("recovery_price", recovery_price)
        if normalized_recovery_price <= normalized_breach_price:
            raise ValueError("recovery_price must be greater than breach_price")
        normalized_failure_minutes = self._normalize_positive_float("failure_minutes", failure_minutes)
        normalized_recovery_minutes = self._normalize_positive_float("recovery_minutes", recovery_minutes)
        normalized_valid_after = self._normalize_valid_after(valid_after)
        alert_id = f"alert_{_kst_now().strftime('%Y%m%d%H%M%S%f')}"
        ops_log(
            LogSource.RT_RUNTIME,
            f"register_recovery_fail_alert requested alert_id={alert_id} code={normalized_code} "
            f"breach_price={normalized_breach_price} recovery_price={normalized_recovery_price} "
            f"failure_minutes={normalized_failure_minutes} recovery_minutes={normalized_recovery_minutes} "
            f"valid_after={normalized_valid_after} once_only={bool(once_only)} "
            f"existing_count={len(self._state['price_alerts'])}",
        )
        record = PriceAlertRecord(
            alert_id=alert_id,
            code=normalized_code,
            condition="recovery_fail",
            threshold=normalized_breach_price,
            window_minutes=None,
            message="",
            http_callback=http_callback,
            created_at=_kst_now().strftime("%Y%m%d%H%M%S"),
            once_only=bool(once_only),
            breach_price=normalized_breach_price,
            recovery_price=normalized_recovery_price,
            failure_minutes=normalized_failure_minutes,
            recovery_minutes=normalized_recovery_minutes,
            valid_after=normalized_valid_after,
            recovery_state="waiting",
        )
        ops_log(LogSource.RT_RUNTIME, f"recovery-fail alert retain price RT begin alert_id={alert_id} code={normalized_code}")
        self._retain_price_subscription(normalized_code)
        try:
            self._state["price_alerts"].append(record.to_dict())
            self._persist_state()
            ops_log(
                LogSource.RT_RUNTIME,
                f"recovery-fail alert persisted alert_id={alert_id} code={normalized_code} "
                f"total_count={len(self._state['price_alerts'])}",
            )
        except Exception:
            ops_log(
                LogSource.RT_RUNTIME,
                f"recovery-fail alert persist failed; rollback begin alert_id={alert_id} code={normalized_code}",
            )
            self._state["price_alerts"] = [
                item for item in self._state["price_alerts"] if item.get("alert_id") != alert_id
            ]
            self._release_price_subscription_safely(normalized_code, alert_id)
            raise
        return {
            "alert_id": alert_id,
            "code": normalized_code,
            "condition": "recovery_fail",
            "breach_price": normalized_breach_price,
            "recovery_price": normalized_recovery_price,
            "failure_minutes": normalized_failure_minutes,
            "recovery_minutes": normalized_recovery_minutes,
            "valid_after": normalized_valid_after,
            "once_only": bool(once_only),
        }

    @_with_state_lock
    def register_uptrend_end_alert(
        self,
        code: str,
        start_price: float,
        end_price: float,
        end_minutes: float,
        valid_after: str,
        http_callback: HttpCallbackSpec,
        once_only: bool = True,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        normalized_code = self._client.normalize_stock_code(code)
        normalized_start_price = self._normalize_positive_float("start_price", start_price)
        normalized_end_price = self._normalize_positive_float("end_price", end_price)
        if normalized_start_price <= normalized_end_price:
            raise ValueError("start_price must be greater than end_price")
        normalized_end_minutes = self._normalize_positive_float("end_minutes", end_minutes)
        normalized_valid_after = self._normalize_valid_after(valid_after)
        alert_id = f"alert_{_kst_now().strftime('%Y%m%d%H%M%S%f')}"
        ops_log(
            LogSource.RT_RUNTIME,
            f"register_uptrend_end_alert requested alert_id={alert_id} code={normalized_code} "
            f"start_price={normalized_start_price} end_price={normalized_end_price} "
            f"end_minutes={normalized_end_minutes} valid_after={normalized_valid_after} "
            f"once_only={bool(once_only)} existing_count={len(self._state['price_alerts'])}",
        )
        record = PriceAlertRecord(
            alert_id=alert_id,
            code=normalized_code,
            condition="uptrend_end",
            threshold=normalized_end_price,
            window_minutes=None,
            message="",
            http_callback=http_callback,
            created_at=_kst_now().strftime("%Y%m%d%H%M%S"),
            once_only=bool(once_only),
            valid_after=normalized_valid_after,
            start_price=normalized_start_price,
            end_price=normalized_end_price,
            end_minutes=normalized_end_minutes,
            uptrend_state="waiting",
        )
        ops_log(LogSource.RT_RUNTIME, f"uptrend-end alert retain price RT begin alert_id={alert_id} code={normalized_code}")
        self._retain_price_subscription(normalized_code)
        try:
            self._state["price_alerts"].append(record.to_dict())
            self._persist_state()
            ops_log(
                LogSource.RT_RUNTIME,
                f"uptrend-end alert persisted alert_id={alert_id} code={normalized_code} "
                f"total_count={len(self._state['price_alerts'])}",
            )
        except Exception:
            ops_log(
                LogSource.RT_RUNTIME,
                f"uptrend-end alert persist failed; rollback begin alert_id={alert_id} code={normalized_code}",
            )
            self._state["price_alerts"] = [
                item for item in self._state["price_alerts"] if item.get("alert_id") != alert_id
            ]
            self._release_price_subscription_safely(normalized_code, alert_id)
            raise
        return {
            "alert_id": alert_id,
            "code": normalized_code,
            "condition": "uptrend_end",
            "start_price": normalized_start_price,
            "end_price": normalized_end_price,
            "end_minutes": normalized_end_minutes,
            "valid_after": normalized_valid_after,
            "once_only": bool(once_only),
        }

    @_with_state_lock
    def list_price_alerts(self) -> list[dict[str, Any]]:
        self._maybe_cleanup_closed_market()
        prices: dict[str, float] = {}
        items: list[dict[str, Any]] = []
        for raw in self._state["price_alerts"]:
            code = str(raw["code"])
            current_price = self._coerce_float(raw.get("last_price"))
            if current_price is None:
                if code not in prices:
                    try:
                        prices[code] = float(self._client.get_quote_snapshot(code).current_price)
                    except Exception:
                        prices[code] = 0.0
                current_price = prices[code]
            item = {
                "alert_id": raw["alert_id"],
                "code": code,
                "name": self._stock_name(code),
                "condition": raw["condition"],
                "threshold": raw["threshold"],
                "window_minutes": raw.get("window_minutes"),
                "debounce_seconds": self._price_alert_effective_debounce_seconds(raw),
                "once_only": bool(raw.get("once_only")),
                "message": raw["message"],
                "httpCallback": raw["httpCallback"],
                "current_price": current_price,
                "created_at": raw["created_at"],
            }
            if str(raw.get("condition") or "") == "recovery_fail":
                item.update(
                    {
                        "breach_price": raw.get("breach_price", raw.get("threshold")),
                        "recovery_price": raw.get("recovery_price"),
                        "failure_minutes": raw.get("failure_minutes"),
                        "recovery_minutes": raw.get("recovery_minutes"),
                        "valid_after": raw.get("valid_after"),
                        "recovery_state": raw.get("recovery_state", "waiting"),
                        "breached_at": raw.get("breached_at"),
                        "recovery_since": raw.get("recovery_since"),
                    }
                )
            if str(raw.get("condition") or "") == "uptrend_end":
                item.update(
                    {
                        "start_price": raw.get("start_price"),
                        "end_price": raw.get("end_price", raw.get("threshold")),
                        "end_minutes": raw.get("end_minutes"),
                        "valid_after": raw.get("valid_after"),
                        "uptrend_state": raw.get("uptrend_state", "waiting"),
                        "uptrend_started_at": raw.get("uptrend_started_at"),
                        "ending_since": raw.get("ending_since"),
                    }
                )
            items.append(item)
        return items

    @_with_state_lock
    def cancel_price_alert(self, alert_id: str | None = None, code: str | None = None) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        if not alert_id and not code:
            raise ValueError("alert_id or code is required")
        normalized_code = self._client.normalize_stock_code(code) if code else None
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
            self._cancel_recovery_fail_timer_locked(str(raw["alert_id"]))
            self._cancel_uptrend_end_timer_locked(str(raw["alert_id"]))
            self._release_price_subscription(str(raw["code"]))
        if removed:
            self._persist_state()
        return {
            "canceled": bool(removed),
            "removed_alerts": len(removed),
            "alert_id": alert_id,
            "code": normalized_code,
        }

    @_with_state_lock
    def register_stock_price_callback(
        self,
        code: str,
        step: float,
        http_callback: HttpCallbackSpec,
        price_filter: str | None = None,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        normalized_code = self._client.normalize_stock_code(code)
        normalized_step = float(step)
        if normalized_step <= 0:
            raise ValueError("step must be greater than zero")
        normalized_price_filter = self._normalize_stock_price_callback_filter(price_filter)
        callback_id = f"stock_price_callback_{_kst_now().strftime('%Y%m%d%H%M%S%f')}"
        ops_log(LogSource.RT_RUNTIME,
            f"register_stock_price_callback requested callback_id={callback_id} "
            f"code={normalized_code} step={normalized_step} price_filter={normalized_price_filter} "
            f"existing_count={len(self._state['stock_price_callbacks'])}",
        )
        record = StockPriceCallbackRecord(
            stock_price_callback_id=callback_id,
            code=normalized_code,
            step=normalized_step,
            price_filter=normalized_price_filter,
            http_callback=http_callback,
            registered_at=_kst_now().strftime("%Y%m%d%H%M%S"),
        )
        ops_log(LogSource.RT_RUNTIME, f"stock price callback retain price RT begin callback_id={callback_id} code={normalized_code}")
        self._retain_price_subscription(normalized_code)
        try:
            self._state["stock_price_callbacks"].append(record.to_dict())
            self._persist_state()
            ops_log(LogSource.RT_RUNTIME,
                f"stock price callback persisted callback_id={callback_id} code={normalized_code} "
                f"total_count={len(self._state['stock_price_callbacks'])}",
            )
        except Exception:
            ops_log(LogSource.RT_RUNTIME,
                f"stock price callback persist failed; rollback begin callback_id={callback_id} code={normalized_code}",
            )
            self._state["stock_price_callbacks"] = [
                item
                for item in self._state["stock_price_callbacks"]
                if item.get("stock_price_callback_id") != callback_id
            ]
            self._release_price_subscription_safely(normalized_code, callback_id)
            raise
        return {
            "stock_price_callback_id": callback_id,
            "code": normalized_code,
            "step": normalized_step,
            "price_filter": normalized_price_filter,
        }

    @_with_state_lock
    def list_stock_price_callbacks(self) -> list[dict[str, Any]]:
        self._maybe_cleanup_closed_market()
        prices: dict[str, float] = {}
        items: list[dict[str, Any]] = []
        for raw in self._state["stock_price_callbacks"]:
            code = str(raw["code"])
            current_price = self._coerce_float(raw.get("last_price"))
            if current_price is None:
                if code not in prices:
                    try:
                        prices[code] = float(self._client.get_quote_snapshot(code).current_price)
                    except Exception:
                        prices[code] = 0.0
                current_price = prices[code]
            items.append(
                {
                    "stock_price_callback_id": raw["stock_price_callback_id"],
                    "code": code,
                    "name": self._stock_name(code),
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
    def cancel_stock_price_callback(
        self,
        stock_price_callback_id: str | None = None,
        code: str | None = None,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        if not stock_price_callback_id and not code:
            raise ValueError("stock_price_callback_id or code is required")
        normalized_code = self._client.normalize_stock_code(code) if code else None
        removed: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for raw in self._state["stock_price_callbacks"]:
            matches_callback = (
                stock_price_callback_id is not None
                and raw["stock_price_callback_id"] == stock_price_callback_id
            )
            matches_code = normalized_code is not None and raw["code"] == normalized_code
            if matches_callback or matches_code:
                removed.append(raw)
            else:
                retained.append(raw)
        self._state["stock_price_callbacks"] = retained
        for raw in removed:
            self._cancel_stock_price_callback_debounce_locked(str(raw["stock_price_callback_id"]))
            self._release_price_subscription(str(raw["code"]))
        if removed:
            self._persist_state()
        return {
            "canceled": bool(removed),
            "removed_callbacks": len(removed),
            "stock_price_callback_id": stock_price_callback_id,
            "code": normalized_code,
        }

    def _price_alert_cooldown_seconds(self, window_minutes: object) -> float:
        try:
            minutes = float(window_minutes)
            seconds_per_minute = float(self.PRICE_ALERT_COOLDOWN_SECONDS_PER_MINUTE)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, minutes * seconds_per_minute)

    def _normalize_price_alert_debounce_seconds(
        self,
        condition: str,
        debounce_seconds: float | None,
    ) -> float | None:
        if condition == "fastmove":
            if debounce_seconds is not None:
                raise ValueError("debounce_seconds is only supported for climb and fall alerts")
            return None
        if debounce_seconds is None:
            debounce_seconds = self.PRICE_ALERT_DEBOUNCE_SECONDS
        try:
            normalized = float(debounce_seconds)
        except (TypeError, ValueError):
            raise ValueError("debounce_seconds must be a number") from None
        if normalized < 0:
            raise ValueError("debounce_seconds must be greater than or equal to zero")
        return normalized

    def _price_alert_effective_debounce_seconds(self, raw: dict[str, Any]) -> float | None:
        if str(raw.get("condition") or "") not in {"climb", "fall"}:
            return None
        raw_value = raw.get("debounce_seconds")
        if raw_value is None:
            return float(self.PRICE_ALERT_DEBOUNCE_SECONDS)
        try:
            return max(0.0, float(raw_value))
        except (TypeError, ValueError):
            return float(self.PRICE_ALERT_DEBOUNCE_SECONDS)

    def _price_alert_in_debounce(self, raw: dict[str, Any]) -> bool:
        debounce_seconds = self._price_alert_effective_debounce_seconds(raw)
        if debounce_seconds is None or debounce_seconds <= 0:
            return False
        last_triggered = self._parse_compact_kst(str(raw.get("last_triggered_at") or ""))
        if last_triggered is None:
            return False
        return (_kst_now() - last_triggered).total_seconds() <= debounce_seconds

    def _update_price_alert_side(self, raw: dict[str, Any], current_price: float) -> None:
        condition = str(raw.get("condition") or "")
        threshold = float(raw["threshold"])
        if condition == "climb":
            raw["last_side"] = "above" if current_price >= threshold else "below"
        elif condition == "fall":
            raw["last_side"] = "below" if current_price <= threshold else "above"

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

    @staticmethod
    def _normalize_positive_float(name: str, value: float) -> float:
        try:
            normalized = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be a number") from None
        if normalized <= 0:
            raise ValueError(f"{name} must be greater than zero")
        return normalized

    @staticmethod
    def _normalize_valid_after(value: str) -> str:
        text = str(value or "").strip()
        match = re.fullmatch(r"([01][0-9]|2[0-3]):([0-5][0-9])", text)
        if match is None:
            raise ValueError("valid_after must be HH:MM in KST")
        return f"{match.group(1)}:{match.group(2)}"

    @staticmethod
    def _valid_after_time(value: object) -> time | None:
        text = str(value or "").strip()
        match = re.fullmatch(r"([01][0-9]|2[0-3]):([0-5][0-9])", text)
        if match is None:
            return None
        return time(hour=int(match.group(1)), minute=int(match.group(2)))

    def _recovery_fail_timer_delay_seconds(self, raw: dict[str, Any]) -> float | None:
        state = str(raw.get("recovery_state") or "waiting")
        if state == "breached":
            start_at = self._parse_compact_kst(str(raw.get("breached_at") or ""))
            minutes = self._coerce_float(raw.get("failure_minutes"))
        elif state in {"recovering", "failed_recovering"}:
            start_at = self._parse_compact_kst(str(raw.get("recovery_since") or ""))
            minutes = self._coerce_float(raw.get("recovery_minutes"))
        else:
            return None
        if start_at is None or minutes is None or minutes <= 0:
            return None
        elapsed = (_kst_now() - start_at).total_seconds()
        return max(0.01, (minutes * 60.0) - elapsed)

    def _start_recovery_fail_timer_locked(self, alert_id: str, raw: dict[str, Any]) -> None:
        self._cancel_recovery_fail_timer_locked(alert_id)
        delay = self._recovery_fail_timer_delay_seconds(raw)
        if delay is None:
            return
        timer = threading.Timer(delay, self._flush_recovery_fail_timer, args=(alert_id,))
        timer.daemon = True
        self._recovery_fail_timers[alert_id] = timer
        timer.start()

    def _cancel_recovery_fail_timer_locked(self, alert_id: str) -> None:
        timer = self._recovery_fail_timers.pop(alert_id, None)
        if timer is not None:
            timer.cancel()

    def _cancel_all_recovery_fail_timers_locked(self) -> None:
        for timer in list(self._recovery_fail_timers.values()):
            timer.cancel()
        self._recovery_fail_timers.clear()

    def _uptrend_end_timer_delay_seconds(self, raw: dict[str, Any]) -> float | None:
        if str(raw.get("uptrend_state") or "waiting") != "ending":
            return None
        start_at = self._parse_compact_kst(str(raw.get("ending_since") or ""))
        minutes = self._coerce_float(raw.get("end_minutes"))
        if start_at is None or minutes is None or minutes <= 0:
            return None
        elapsed = (_kst_now() - start_at).total_seconds()
        return max(0.01, (minutes * 60.0) - elapsed)

    def _start_uptrend_end_timer_locked(self, alert_id: str, raw: dict[str, Any]) -> None:
        self._cancel_uptrend_end_timer_locked(alert_id)
        delay = self._uptrend_end_timer_delay_seconds(raw)
        if delay is None:
            return
        timer = threading.Timer(delay, self._flush_uptrend_end_timer, args=(alert_id,))
        timer.daemon = True
        self._uptrend_end_timers[alert_id] = timer
        timer.start()

    def _cancel_uptrend_end_timer_locked(self, alert_id: str) -> None:
        timer = self._uptrend_end_timers.pop(alert_id, None)
        if timer is not None:
            timer.cancel()

    def _cancel_all_uptrend_end_timers_locked(self) -> None:
        for timer in list(self._uptrend_end_timers.values()):
            timer.cancel()
        self._uptrend_end_timers.clear()

    def _stock_price_callback_debounce_seconds(self) -> float:
        try:
            return max(0.0, float(self.STOCK_PRICE_CALLBACK_DEBOUNCE_SECONDS))
        except (TypeError, ValueError):
            return 10.0

    def _start_stock_price_callback_debounce_locked(self, callback_id: str) -> None:
        delay = self._stock_price_callback_debounce_seconds()
        if delay <= 0:
            return
        previous = self._stock_price_callback_debounce_timers.pop(callback_id, None)
        if previous is not None:
            previous.cancel()
        timer = threading.Timer(delay, self._flush_stock_price_callback_debounce, args=(callback_id,))
        timer.daemon = True
        self._stock_price_callback_debounce_timers[callback_id] = timer
        timer.start()

    def _cancel_stock_price_callback_debounce_locked(self, callback_id: str) -> None:
        timer = self._stock_price_callback_debounce_timers.pop(callback_id, None)
        if timer is not None:
            timer.cancel()
        self._stock_price_callback_debounce_pending.pop(callback_id, None)

    def _cancel_all_stock_price_callback_debounces_locked(self) -> None:
        for timer in list(self._stock_price_callback_debounce_timers.values()):
            timer.cancel()
        self._stock_price_callback_debounce_timers.clear()
        self._stock_price_callback_debounce_pending.clear()

    @_with_state_lock
    def register_fall_safe(
        self,
        account_no: str,
        code: str,
        trigger_price: float,
        quantity: int,
        http_callback: HttpCallbackSpec | None = None,
    ) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        if not account_no.strip():
            raise ValueError("account_no is required")
        if trigger_price <= 0:
            raise ValueError("trigger_price must be greater than zero")
        if quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        normalized_code = self._client.normalize_stock_code(code)
        fall_safe_id = f"fall_safe_{_kst_now().strftime('%Y%m%d%H%M%S%f')}"
        ops_log(LogSource.RT_RUNTIME,
            f"register_fall_safe requested fall_safe_id={fall_safe_id} code={normalized_code} "
            f"trigger_price={float(trigger_price)} quantity={int(quantity)} "
            f"existing_count={len(self._state['fall_safes'])}",
        )
        record = FallSafeRecord(
            fall_safe_id=fall_safe_id,
            account_no=account_no.strip(),
            code=normalized_code,
            trigger_price=float(trigger_price),
            quantity=quantity,
            http_callback=http_callback,
            registered_at=_kst_now().strftime("%Y%m%d%H%M%S"),
        )
        ops_log(LogSource.RT_RUNTIME, f"fall-safe retain price RT begin fall_safe_id={fall_safe_id} code={normalized_code}")
        self._retain_price_subscription(normalized_code)
        try:
            self._state["fall_safes"].append(record.to_dict())
            self._persist_state()
            ops_log(LogSource.RT_RUNTIME,
                f"fall-safe persisted fall_safe_id={fall_safe_id} code={normalized_code} "
                f"total_count={len(self._state['fall_safes'])}",
            )
        except Exception:
            ops_log(LogSource.RT_RUNTIME,
                f"fall-safe persist failed; rollback begin fall_safe_id={fall_safe_id} code={normalized_code}",
            )
            self._state["fall_safes"] = [
                item for item in self._state["fall_safes"] if item.get("fall_safe_id") != fall_safe_id
            ]
            self._release_price_subscription_safely(normalized_code, fall_safe_id)
            raise
        return {"fall_safe_id": fall_safe_id, "code": normalized_code}

    @_with_state_lock
    def list_fall_safes(self) -> list[dict[str, Any]]:
        self._maybe_cleanup_closed_market()
        return [
            {
                "fall_safe_id": raw["fall_safe_id"],
                "account_no": raw["account_no"],
                "code": raw["code"],
                "name": self._stock_name(str(raw["code"])),
                "trigger_price": raw["trigger_price"],
                "quantity": raw["quantity"],
                "httpCallback": raw.get("httpCallback"),
                "registered_at": raw["registered_at"],
            }
            for raw in self._state["fall_safes"]
        ]

    @_with_state_lock
    def cancel_fall_safe(self, fall_safe_id: str) -> dict[str, Any]:
        self._maybe_cleanup_closed_market()
        removed: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        for raw in self._state["fall_safes"]:
            if raw["fall_safe_id"] == fall_safe_id:
                removed.append(raw)
            else:
                retained.append(raw)
        self._state["fall_safes"] = retained
        for raw in removed:
            self._release_price_subscription(str(raw["code"]))
        if removed:
            self._persist_state()
        return {
            "canceled": bool(removed),
            "removed_fall_safes": len(removed),
            "fall_safe_id": fall_safe_id,
        }

    @_with_state_lock
    def _restore_same_day_state(self) -> None:
        ops_log(LogSource.STARTUP_RUNTIME, "restore_same_day_state entered")
        self._maybe_cleanup_closed_market()
        disclosures = self._state["subscriptions"]["disclosures"]
        if disclosures and not self._disclosure_feed_active:
            ops_log(LogSource.STARTUP_RUNTIME, f"restoring disclosure feed count={len(disclosures)}")
            try:
                self._client.subscribe_disclosure_feed(str(disclosures[0]["code"]))
                self._disclosure_feed_active = True
                ops_log(LogSource.STARTUP_RUNTIME, "disclosure feed restored")
            except Exception as exc:
                ops_log(LogSource.STARTUP_RUNTIME, f"disclosure feed restore failed: {exc.__class__.__name__}: {exc}")
                client_error = self._client.get_last_rt_error_details()
                details = {
                    "subscription_kind": "disclosure",
                    "code": str(disclosures[0]["code"]),
                    "error": str(exc),
                }
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_restore_failed",
                    message=f"공시 구독 복구 실패: code={str(disclosures[0]['code'])}, error={exc}",
                    details=details,
                )
                ops_log(LogSource.STARTUP_RUNTIME, f"Failed to restore disclosure subscription feed: {exc}")
        news = self._state["subscriptions"]["news"]
        if news and not self._news_feed_active:
            ops_log(LogSource.STARTUP_RUNTIME, f"restoring news feed count={len(news)}")
            try:
                self._client.subscribe_news_feed(news[0].get("code"))
                self._news_feed_active = True
                ops_log(LogSource.STARTUP_RUNTIME, "news feed restored")
            except Exception as exc:
                ops_log(LogSource.STARTUP_RUNTIME, f"news feed restore failed: {exc.__class__.__name__}: {exc}")
                client_error = self._client.get_last_rt_error_details()
                details = {
                    "subscription_kind": "news",
                    "code": news[0].get("code"),
                    "subject": str(news[0].get("code") or "*"),
                    "types": list(news[0].get("types") or []),
                    "error": str(exc),
                }
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_restore_failed",
                    message=f"뉴스 구독 복구 실패: subject={str(news[0].get('code') or '*')}, error={exc}",
                    details=details,
                )
                ops_log(LogSource.STARTUP_RUNTIME, f"Failed to restore news subscription feed: {exc}")
        ops_log(LogSource.STARTUP_RUNTIME, f"restoring price alert subscriptions count={len(self._state['price_alerts'])}")
        for raw in self._state["price_alerts"]:
            code = str(raw["code"])
            try:
                self._retain_price_subscription(code, persist=False)
            except Exception as exc:
                ops_log(LogSource.STARTUP_RUNTIME, f"price alert restore failed code={code}: {exc.__class__.__name__}: {exc}")
                client_error = self._client.get_last_rt_error_details()
                details = {
                    "subscription_kind": "price_alert",
                    "code": code,
                    "alert_id": str(raw.get("alert_id") or ""),
                    "error": str(exc),
                }
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_restore_failed",
                    message=f"가격 알람 구독 복구 실패: code={code}, error={exc}",
                    details=details,
                )
                self._dispatcher.wait_for_idle(timeout=15.0)
                raise
            if str(raw.get("condition") or "") == "recovery_fail":
                self._start_recovery_fail_timer_locked(str(raw.get("alert_id") or ""), raw)
            if str(raw.get("condition") or "") == "uptrend_end":
                self._start_uptrend_end_timer_locked(str(raw.get("alert_id") or ""), raw)
        ops_log(LogSource.STARTUP_RUNTIME,
            f"restoring stock price callback subscriptions count={len(self._state['stock_price_callbacks'])}",
        )
        for raw in self._state["stock_price_callbacks"]:
            code = str(raw["code"])
            try:
                self._retain_price_subscription(code, persist=False)
            except Exception as exc:
                ops_log(LogSource.STARTUP_RUNTIME,
                    f"stock price callback restore failed code={code}: {exc.__class__.__name__}: {exc}",
                )
                client_error = self._client.get_last_rt_error_details()
                details = {
                    "subscription_kind": "stock_price_callback",
                    "code": code,
                    "stock_price_callback_id": str(raw.get("stock_price_callback_id") or ""),
                    "error": str(exc),
                }
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_restore_failed",
                    message=f"주가 Step callback 구독 복구 실패: code={code}, error={exc}",
                    details=details,
                )
                self._dispatcher.wait_for_idle(timeout=15.0)
                raise
        ops_log(LogSource.STARTUP_RUNTIME, f"restoring fall-safe price subscriptions count={len(self._state['fall_safes'])}")
        for raw in self._state["fall_safes"]:
            code = str(raw["code"])
            try:
                self._retain_price_subscription(code, persist=False)
            except Exception as exc:
                ops_log(LogSource.STARTUP_RUNTIME, f"fall-safe restore failed code={code}: {exc.__class__.__name__}: {exc}")
                client_error = self._client.get_last_rt_error_details()
                details = {
                    "subscription_kind": "fall_safe",
                    "code": code,
                    "fall_safe_id": str(raw.get("fall_safe_id") or ""),
                    "error": str(exc),
                }
                if client_error:
                    details.update(client_error)
                self._dispatch_system_event(
                    event_type="subscription_restore_failed",
                    message=f"Fall-safe 구독 복구 실패: code={code}, error={exc}",
                    details=details,
                )
                self._dispatcher.wait_for_idle(timeout=15.0)
                raise
        ops_log(LogSource.STARTUP_RUNTIME, "restore_same_day_state complete")

    def _persist_state(self) -> None:
        payload = dict(self._state)
        payload["subscriptions"] = {"disclosures": [], "news": []}
        self._store.save(payload)

    def _persist_subscriptions(self) -> None:
        self._subscription_state["subscriptions"] = self._state["subscriptions"]
        self._subscription_store.save(self._subscription_state)

    def _normalize_persistent_subscription_ids(self) -> None:
        changed = False
        for prefix, key in (("disc_sub", "disclosures"), ("news_sub", "news")):
            for item in self._subscription_state["subscriptions"][key]:
                if item.get("subscription_id"):
                    continue
                item["subscription_id"] = self._next_subscription_id(prefix)
                changed = True
        for item in self._subscription_state["system_callbacks"]:
            if item.get("system_callback_id"):
                continue
            item["system_callback_id"] = self._next_subscription_id("sys_cb")
            changed = True
        if changed:
            self._subscription_store.save(self._subscription_state)

    def _migrate_legacy_subscriptions_if_needed(self) -> None:
        if self._subscription_store.exists():
            return
        legacy_state = self._store.load()
        legacy_subscriptions = legacy_state.get("subscriptions")
        if not isinstance(legacy_subscriptions, dict):
            return
        disclosures = list(legacy_subscriptions.get("disclosures", []))
        news = list(legacy_subscriptions.get("news", []))
        if not disclosures and not news:
            return

        migrated_at = _kst_now().strftime("%Y%m%d%H%M%S")
        normalized_disclosures = [
            self._migrate_disclosure_record(item, migrated_at, index)
            for index, item in enumerate(disclosures, start=1)
        ]
        normalized_news = [
            self._migrate_news_record(item, migrated_at, index)
            for index, item in enumerate(news, start=1)
        ]
        self._subscription_state = self._subscription_store.load()
        self._subscription_state["subscriptions"] = {
            "disclosures": normalized_disclosures,
            "news": normalized_news,
        }
        self._subscription_store.save(self._subscription_state)
        ops_log(LogSource.STARTUP_RUNTIME,
            "Migrated "
            f"{len(normalized_disclosures)} disclosure and {len(normalized_news)} news subscriptions "
            "to persistent subscription_state.json",
        )

    @staticmethod
    def _migrate_disclosure_record(raw: dict[str, Any], migrated_at: str, index: int) -> dict[str, Any]:
        return {
            "subscription_id": f"disc_sub_{migrated_at}_{index}",
            "code": raw.get("code", ""),
            "httpCallback": dict(raw.get("httpCallback") or {}),
            "registered_at": migrated_at,
            "last_event_at": raw.get("last_event_at"),
            "evaluated_event_count": int(raw.get("evaluated_event_count") or 0),
        }

    @staticmethod
    def _migrate_news_record(raw: dict[str, Any], migrated_at: str, index: int) -> dict[str, Any]:
        return {
            "subscription_id": f"news_sub_{migrated_at}_{index}",
            "types": list(raw.get("types") or []),
            "code": raw.get("code"),
            "httpCallback": dict(raw.get("httpCallback") or {}),
            "registered_at": migrated_at,
            "last_event_at": raw.get("last_event_at"),
            "evaluated_event_count": int(raw.get("evaluated_event_count") or 0),
        }

    def _next_subscription_id(self, prefix: str) -> str:
        self._subscription_id_counter += 1
        return f"{prefix}_{_kst_now().strftime('%Y%m%d%H%M%S%f')}_{self._subscription_id_counter}"

    def _maybe_cleanup_closed_market(self) -> None:
        now = _kst_now()
        trading_date = self._state.get("trading_date") or self._store.current_trading_date()
        if trading_date != now.strftime("%Y%m%d"):
            self._clear_runtime_state()
            return
        if self._restored_from_disk and now.time() >= self.MARKET_CLOSE_TIME and self._has_runtime_state():
            self._clear_runtime_state()

    def _has_runtime_state(self) -> bool:
        return bool(
            self._state["price_alerts"]
            or self._state["fall_safes"]
            or self._state["stock_price_callbacks"]
        )

    def _clear_runtime_state(self) -> None:
        ops_log(LogSource.MANAGE,
            f"clear runtime state begin owned_price_codes={len(self._owned_price_codes)} "
            f"price_alerts={len(self._state['price_alerts'])} "
            f"fall_safes={len(self._state['fall_safes'])} "
            f"stock_price_callbacks={len(self._state['stock_price_callbacks'])}",
        )
        for code in list(self._owned_price_codes.keys()):
            try:
                self._client.unsubscribe_realtime_price(code)
            except Exception as exc:
                ops_log(LogSource.MANAGE, f"Failed to clear price subscription for {code}: {exc}")
        self._owned_price_codes.clear()
        self._state = self._store.load()
        self._state["subscriptions"] = self._subscription_state["subscriptions"]
        self._restored_from_disk = False
        self._state["price_alerts"] = []
        self._state["fall_safes"] = []
        self._state["stock_price_callbacks"] = []
        self._cancel_all_price_alert_cooldowns_locked()
        self._cancel_all_recovery_fail_timers_locked()
        self._cancel_all_uptrend_end_timers_locked()
        self._cancel_all_stock_price_callback_debounces_locked()
        self._store.clear()
        ops_log(LogSource.MANAGE, "clear runtime state complete")

    def _retain_price_subscription(self, code: str, persist: bool = False) -> None:
        previous = self._owned_price_codes.get(code, 0)
        ops_log(LogSource.RT_RUNTIME, f"retain price RT requested code={code} previous_count={previous} persist={persist}")
        if previous == 0:
            ops_log(LogSource.RT_RUNTIME, f"retain price RT external subscribe begin code={code}")
            try:
                self._client.subscribe_realtime_price(code)
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME,
                    f"retain price RT external subscribe failed code={code} error={exc.__class__.__name__}: {exc}",
                )
                raise
            ops_log(LogSource.RT_RUNTIME, f"retain price RT external subscribe success code={code}")
        self._owned_price_codes[code] = previous + 1
        ops_log(LogSource.RT_RUNTIME, f"retain price RT complete code={code} new_count={self._owned_price_codes[code]}")
        if persist:
            self._persist_state()
            ops_log(LogSource.RT_RUNTIME, f"retain price RT state persisted code={code}")

    def _release_price_subscription(self, code: str) -> None:
        previous = self._owned_price_codes.get(code, 0)
        ops_log(LogSource.RT_RUNTIME, f"release price RT requested code={code} previous_count={previous}")
        if previous > 1:
            self._owned_price_codes[code] = previous - 1
            ops_log(LogSource.RT_RUNTIME, f"release price RT decremented code={code} new_count={self._owned_price_codes[code]}")
        elif previous == 1:
            self._owned_price_codes.pop(code, None)
            ops_log(LogSource.RT_RUNTIME, f"release price RT external unsubscribe begin code={code}")
            try:
                self._client.unsubscribe_realtime_price(code)
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME,
                    f"release price RT external unsubscribe failed code={code} error={exc.__class__.__name__}: {exc}",
                )
                raise
            ops_log(LogSource.RT_RUNTIME, f"release price RT external unsubscribe success code={code}")
        else:
            ops_log(LogSource.RT_RUNTIME, f"release price RT skipped code={code} reason=not_owned")

    def _release_price_subscription_safely(self, code: str, owner_id: str) -> None:
        try:
            self._release_price_subscription(code)
            ops_log(LogSource.RT_RUNTIME, f"release price RT rollback complete owner_id={owner_id} code={code}")
        except Exception as exc:
            ops_log(LogSource.RT_RUNTIME,
                f"release price RT rollback failed owner_id={owner_id} code={code} error={exc.__class__.__name__}: {exc}",
            )
            ops_log(LogSource.RT_RUNTIME, f"Failed to roll back price RT subscription for {owner_id}: {exc}")

    @_with_state_lock
    def _on_rt_event(self, event: dict[str, Any]) -> None:
        self._handle_rt_event_locked(event, log_received=True)

    @_with_state_lock
    def _on_rt_events(self, events: list[dict[str, Any]]) -> None:
        if self._closed:
            ops_log(LogSource.RT_RUNTIME, "RT batch ignored reason=runtime_closed")
            return
        normalized_events = [dict(event) for event in events if isinstance(event, dict)]
        if not normalized_events:
            return
        self._maybe_cleanup_closed_market()
        sc_events_by_code: dict[str, list[dict[str, Any]]] = {}
        sc_codes_seen: set[str] = set()
        sc_event_count = 0
        non_sc_count = 0
        unsupported_count = 0

        def flush_sc_events() -> None:
            nonlocal sc_events_by_code
            for code, sc_events in sc_events_by_code.items():
                self._dispatch_sc_events(code, sc_events)
            sc_events_by_code = {}

        for event in normalized_events:
            rt_type = str(event.get("rt_type") or "")
            if self._is_stock_price_rt_type(rt_type):
                code = str(event.get("code") or "")
                if code:
                    sc_event_count += 1
                    sc_codes_seen.add(code)
                    sc_events_by_code.setdefault(code, []).append(event)
                else:
                    unsupported_count += 1
                continue
            flush_sc_events()
            non_sc_count += 1
            if rt_type in {"N0", "N2"}:
                self._handle_rt_event_locked(event, log_received=False)
            else:
                unsupported_count += 1
        flush_sc_events()
        if len(normalized_events) > 1 or unsupported_count:
            ops_log(
                LogSource.RT_RUNTIME,
                f"RT batch processed events={len(normalized_events)} "
                f"sc_codes={len(sc_codes_seen)} sc_events={sc_event_count} "
                f"non_sc_events={non_sc_count} unsupported={unsupported_count}",
                level="debug" if not unsupported_count else "info",
            )

    def _handle_rt_event_locked(self, event: dict[str, Any], *, log_received: bool) -> None:
        if self._closed:
            ops_log(LogSource.RT_RUNTIME, "RT event ignored reason=runtime_closed")
            return
        self._maybe_cleanup_closed_market()
        rt_type = str(event.get("rt_type") or "")
        if log_received:
            ops_log(LogSource.RT_RUNTIME,
                f"RT event received rt_type={rt_type} code={event.get('code') or ''} "
                f"news_type={event.get('news_type') or ''} time={event.get('time') or ''}",
            )
        if rt_type == "N2":
            self._dispatch_disclosure_event(event)
            return
        if rt_type == "N0":
            self._dispatch_news_event(event)
            return
        if self._is_stock_price_rt_type(rt_type):
            code = str(event.get("code") or "")
            self._dispatch_sc_events(code, [event])
            return
        ops_log(LogSource.RT_RUNTIME, f"RT event ignored rt_type={rt_type} reason=unsupported")

    def _dispatch_sc_events(self, code: str, events: list[dict[str, Any]]) -> None:
        if not code or not events:
            return
        valid_events: list[tuple[dict[str, Any], float]] = []
        invalid_count = 0
        for event in events:
            current_price = self._coerce_float(event.get("current_price"))
            if current_price is None or current_price <= 0:
                invalid_count += 1
                continue
            valid_events.append((event, current_price))
        if not valid_events:
            ops_log(
                LogSource.RT_RUNTIME,
                f"stock price RT batch skipped code={code} events={len(events)} invalid_price_events={invalid_count}",
            )
            return
        if len(valid_events) > 1 or invalid_count:
            prices = [current_price for _, current_price in valid_events]
            ops_log(
                LogSource.RT_RUNTIME,
                f"stock price RT batch received code={code} events={len(events)} valid={len(valid_events)} "
                f"invalid_price_events={invalid_count} latest_price={prices[-1]} "
                f"min_price={min(prices)} max_price={max(prices)}",
                level="debug",
            )
        self._evaluate_price_alerts(code, valid_events)
        self._evaluate_stock_price_callbacks(code, valid_events)
        self._evaluate_fall_safes(code, valid_events)

    @staticmethod
    def _is_stock_price_rt_type(rt_type: str) -> bool:
        return rt_type in {"SC", "UC"}

    def _dispatch_disclosure_event(self, event: dict[str, Any]) -> None:
        payload = RealtimeEventPayload(
            rt_type="N2",
            news_type=str(event.get("news_type") or ""),
            news_type_label=str(event.get("news_type_label") or ""),
            date=str(event.get("date") or ""),
            article_id=str(event.get("article_id") or ""),
            deleted_flag=str(event.get("deleted_flag")) if event.get("deleted_flag") is not None else None,
            time=str(event.get("time") or ""),
            code=str(event.get("code") or ""),
            title=event.get("title"),
        ).to_dict()
        replacements = self._disclosure_replacements(payload)
        changed = False
        matched_count = 0
        queued_count = 0
        for raw in self._state["subscriptions"]["disclosures"]:
            if raw["code"] != payload["code"]:
                continue
            matched_count += 1
            raw["last_event_at"] = f"{payload['date']}{payload['time']}" if payload["date"] and payload["time"] else _kst_now().strftime("%Y%m%d%H%M%S")
            raw["evaluated_event_count"] = int(raw.get("evaluated_event_count") or 0) + 1
            changed = True
            callback = self._http_callback_from_dict(raw["httpCallback"])
            dispatch_result = self._dispatcher.dispatch(self._render_http_callback(callback, replacements)) or {}
            if dispatch_result.get("queued"):
                queued_count += 1
            else:
                ops_log(LogSource.RT_RUNTIME,
                    f"N2 callback queue failed subscription_id={raw.get('subscription_id', '')} "
                    f"code={payload['code']} error={dispatch_result.get('error')}",
                )
        if changed:
            self._persist_subscriptions()
        ops_log(LogSource.RT_RUNTIME,
            f"N2 event processed code={payload['code']} article_id={payload['article_id']} "
            f"matched={matched_count} queued={queued_count} persisted={changed}",
        )

    def _dispatch_news_event(self, event: dict[str, Any]) -> None:
        payload = RealtimeEventPayload(
            rt_type="N0",
            news_type=str(event.get("news_type") or ""),
            news_type_label=str(event.get("news_type_label") or ""),
            date=str(event.get("date") or ""),
            article_id=str(event.get("article_id") or ""),
            deleted_flag=str(event.get("deleted_flag")) if event.get("deleted_flag") is not None else None,
            time=str(event.get("time") or ""),
            code=str(event.get("code") or ""),
            title=event.get("title"),
        ).to_dict()
        replacements = self._news_replacements(payload)
        changed = False
        matched_count = 0
        queued_count = 0
        for raw in self._state["subscriptions"]["news"]:
            if payload["news_type"] not in raw["types"]:
                continue
            if raw.get("code") and raw.get("code") != payload["code"]:
                continue
            matched_count += 1
            raw["last_event_at"] = f"{payload['date']}{payload['time']}" if payload["date"] and payload["time"] else _kst_now().strftime("%Y%m%d%H%M%S")
            raw["evaluated_event_count"] = int(raw.get("evaluated_event_count") or 0) + 1
            changed = True
            callback = self._http_callback_from_dict(raw["httpCallback"])
            dispatch_result = self._dispatcher.dispatch(self._render_http_callback(callback, replacements)) or {}
            if dispatch_result.get("queued"):
                queued_count += 1
            else:
                ops_log(LogSource.RT_RUNTIME,
                    f"N0 callback queue failed subscription_id={raw.get('subscription_id', '')} "
                    f"subject={raw.get('code') or '*'} type={payload['news_type']} "
                    f"error={dispatch_result.get('error')}",
                )
        if changed:
            self._persist_subscriptions()
        ops_log(LogSource.RT_RUNTIME,
            f"N0 event processed subject={payload['code'] or '*'} type={payload['news_type']} "
            f"article_id={payload['article_id']} matched={matched_count} queued={queued_count} persisted={changed}",
        )

    def _dispatch_dev_disclosure_callback(self, code: str, callback: HttpCallbackSpec) -> dict[str, Any]:
        timestamp = _kst_now()
        replacements = self._disclosure_replacements(
            {
                "news_type": "P",
                "news_type_label": "테스트공시",
                "date": timestamp.strftime("%Y%m%d"),
                "time": timestamp.strftime("%H%M%S"),
                "article_id": "TEST",
                "code": code,
                "title": "공시 구독 테스트",
                "deleted_flag": "I",
            }
        )
        return self._dispatch_dev_callback(callback, replacements)

    def _dispatch_system_event(self, event_type: str, message: str, details: dict[str, Any] | None = None) -> None:
        if self._system_event_recorder is None:
            return
        self._system_event_recorder(event_type, message, details)

    @_with_state_lock
    def dispatch_system_event(self, event_type: str, message: str, details: dict[str, Any] | None = None) -> None:
        self._dispatch_system_event(event_type, message, details)

    def _dispatch_dev_news_callback(self, code: str | None, news_type: str, callback: HttpCallbackSpec) -> dict[str, Any]:
        timestamp = _kst_now()
        normalized_code = code or ""
        replacements = self._news_replacements(
            {
                "news_type": news_type,
                "news_type_label": "테스트",
                "date": timestamp.strftime("%Y%m%d"),
                "time": timestamp.strftime("%H%M%S"),
                "article_id": "TEST",
                "code": normalized_code,
                "title": "뉴스 구독 테스트",
                "deleted_flag": "I",
            }
        )
        if not replacements["name"]:
            replacements["name"] = "테스트 종목"
        return self._dispatch_dev_callback(callback, replacements)

    def _dispatch_dev_callback(self, callback: HttpCallbackSpec, replacements: dict[str, str]) -> dict[str, Any]:
        rendered = self._render_http_callback(callback, replacements)
        outcome = self._dispatcher.dispatch(rendered) or {}
        result = {
            "attempted": True,
            "queued": bool(outcome.get("queued")),
        }
        if outcome.get("error"):
            result["error"] = str(outcome["error"])
        ops_log(LogSource.RT_RUNTIME,
            f"dev callback queued={result['queued']} error={result.get('error', '')}",
        )
        return result

    @staticmethod
    def _sc_price_batch(events_with_prices: list[tuple[dict[str, Any], float]]) -> dict[str, Any]:
        latest_event, latest_price = events_with_prices[-1]
        min_event, min_price = min(events_with_prices, key=lambda item: item[1])
        max_event, max_price = max(events_with_prices, key=lambda item: item[1])
        return {
            "events": events_with_prices,
            "count": len(events_with_prices),
            "latest_event": latest_event,
            "latest_price": latest_price,
            "min_event": min_event,
            "min_price": min_price,
            "max_event": max_event,
            "max_price": max_price,
        }

    def _evaluate_price_alerts(self, code: str, events_with_prices: list[tuple[dict[str, Any], float]]) -> None:
        if not code or not events_with_prices:
            return
        batch = self._sc_price_batch(events_with_prices)
        latest_event = batch["latest_event"]
        latest_price = float(batch["latest_price"])
        changed = False
        evaluated_count = 0
        fired_count = 0
        queued_count = 0
        once_only_removals: list[tuple[str, str]] = []
        for index, raw in enumerate(list(self._state["price_alerts"])):
            if raw["code"] != code:
                continue
            updated = dict(raw)
            alert_id = str(updated["alert_id"])
            condition = str(updated.get("condition") or "")
            evaluated_count += 1
            updated["last_price"] = latest_price
            updated["last_eval_at"] = str(latest_event.get("time") or _kst_now().strftime("%H%M%S"))
            if (
                condition == "fastmove"
                and alert_id in self._price_alert_cooldown_timers
            ):
                self._price_alert_cooldown_pending[alert_id] = {
                    "code": code,
                    "latest_price": latest_price,
                    "latest_eval_at": updated["last_eval_at"],
                }
                self._state["price_alerts"][index] = updated
                changed = True
                continue
            if condition in {"climb", "fall"} and self._price_alert_in_debounce(updated):
                self._update_price_alert_side(updated, latest_price)
                self._state["price_alerts"][index] = updated
                changed = True
                continue
            if condition == "recovery_fail":
                fired = self._check_recovery_fail_transition(updated, latest_price)
                if fired is not None:
                    fired_count += 1
                    event_type = str(fired.get("event_type") or "recovery_fail")
                    dispatch_result = self._dispatch_recovery_fail_alert(updated, latest_price, event_type) or {}
                    if dispatch_result.get("queued"):
                        queued_count += 1
                    else:
                        ops_log(
                            LogSource.RT_RUNTIME,
                            f"recovery-fail alert callback queue failed alert_id={alert_id} "
                            f"code={code} error={dispatch_result.get('error')}",
                        )
                    self._cancel_recovery_fail_timer_locked(alert_id)
                    if event_type == "recovery_fail_resolved":
                        if bool(updated.get("once_only", True)):
                            once_only_removals.append((alert_id, code))
                        else:
                            self._reset_recovery_fail_state(updated)
                    else:
                        self._mark_recovery_fail_post_failure(updated)
                else:
                    self._start_recovery_fail_timer_locked(alert_id, updated)
                self._state["price_alerts"][index] = updated
                changed = True
                continue
            if condition == "uptrend_end":
                fired = self._check_uptrend_end_transition(updated, latest_price)
                if fired is not None:
                    fired_count += 1
                    dispatch_result = self._dispatch_uptrend_end_alert(updated, latest_price) or {}
                    if dispatch_result.get("queued"):
                        queued_count += 1
                    else:
                        ops_log(
                            LogSource.RT_RUNTIME,
                            f"uptrend-end alert callback queue failed alert_id={alert_id} "
                            f"code={code} error={dispatch_result.get('error')}",
                        )
                    self._cancel_uptrend_end_timer_locked(alert_id)
                    if bool(updated.get("once_only", True)):
                        once_only_removals.append((alert_id, code))
                    else:
                        self._mark_uptrend_end_post_fire(updated)
                else:
                    self._start_uptrend_end_timer_locked(alert_id, updated)
                self._state["price_alerts"][index] = updated
                changed = True
                continue
            fired = self._check_alert_batch_transition(updated, batch)
            if fired is not None:
                fired_count += 1
                dispatch_result = self._dispatcher.dispatch(self._http_callback_from_dict(updated["httpCallback"])) or {}
                if dispatch_result.get("queued"):
                    queued_count += 1
                else:
                    ops_log(LogSource.RT_RUNTIME,
                            f"price alert callback queue failed alert_id={updated.get('alert_id', '')} "
                            f"code={code} error={dispatch_result.get('error')}",
                    )
                if bool(updated.get("once_only")):
                    once_only_removals.append((alert_id, code))
                elif condition == "fastmove":
                    self._start_price_alert_cooldown_locked(alert_id, updated.get("window_minutes"))
            self._state["price_alerts"][index] = updated
            changed = True
        if once_only_removals:
            removal_ids = {alert_id for alert_id, _code in once_only_removals}
            self._state["price_alerts"] = [
                item
                for item in self._state["price_alerts"]
                if str(item.get("alert_id") or "") not in removal_ids
            ]
            for alert_id, removed_code in once_only_removals:
                self._cancel_price_alert_cooldown_locked(alert_id)
                self._cancel_recovery_fail_timer_locked(alert_id)
                self._cancel_uptrend_end_timer_locked(alert_id)
                self._release_price_subscription(removed_code)
            changed = True
        if changed:
            self._persist_state()
        if fired_count:
            ops_log(LogSource.RT_RUNTIME,
                f"price alert batch evaluation complete code={code} events={len(events_with_prices)} "
                f"latest_price={latest_price} min_price={batch['min_price']} max_price={batch['max_price']} "
                f"evaluated={evaluated_count} fired={fired_count} "
                f"queued={queued_count} persisted={changed}",
            )

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
                            f"price alert trailing callback queue failed alert_id={alert_id} "
                            f"code={code} error={dispatch_result.get('error')}",
                        )
                    if bool(updated.get("once_only")):
                        self._state["price_alerts"] = [
                            item
                            for item in self._state["price_alerts"]
                            if str(item.get("alert_id") or "") != alert_id
                        ]
                        self._release_price_subscription(code)
                        self._persist_state()
                        return
                    self._start_price_alert_cooldown_locked(alert_id, updated.get("window_minutes"))
                    ops_log(LogSource.RT_RUNTIME,
                        f"price alert cooldown trailing fired alert_id={alert_id} "
                        f"code={code} latest_price={latest_price} move_percent={move_percent}",
                    )
                self._state["price_alerts"][index] = updated
                self._persist_state()
                return

    def _flush_recovery_fail_timer(self, alert_id: str) -> None:
        with self._state_lock:
            self._recovery_fail_timers.pop(alert_id, None)
            if self._closed:
                return
            self._maybe_cleanup_closed_market()
            for index, raw in enumerate(list(self._state["price_alerts"])):
                if str(raw.get("alert_id") or "") != alert_id:
                    continue
                updated = dict(raw)
                if str(updated.get("condition") or "") != "recovery_fail":
                    return
                code = str(updated.get("code") or "")
                latest_price = self._coerce_float(updated.get("last_price"))
                if not code or latest_price is None or latest_price <= 0:
                    return
                fired = self._check_recovery_fail_transition(updated, latest_price)
                if fired is not None:
                    event_type = str(fired.get("event_type") or "recovery_fail")
                    dispatch_result = self._dispatch_recovery_fail_alert(updated, latest_price, event_type) or {}
                    if not dispatch_result.get("queued"):
                        ops_log(
                            LogSource.RT_RUNTIME,
                            f"recovery-fail alert timer callback queue failed alert_id={alert_id} "
                            f"code={code} error={dispatch_result.get('error')}",
                        )
                    if event_type == "recovery_fail_resolved":
                        if bool(updated.get("once_only", True)):
                            self._state["price_alerts"] = [
                                item
                                for item in self._state["price_alerts"]
                                if str(item.get("alert_id") or "") != alert_id
                            ]
                            self._release_price_subscription(code)
                            self._persist_state()
                            return
                        self._reset_recovery_fail_state(updated)
                    else:
                        self._mark_recovery_fail_post_failure(updated)
                else:
                    self._start_recovery_fail_timer_locked(alert_id, updated)
                self._state["price_alerts"][index] = updated
                self._persist_state()
                return

    def _flush_uptrend_end_timer(self, alert_id: str) -> None:
        with self._state_lock:
            self._uptrend_end_timers.pop(alert_id, None)
            if self._closed:
                return
            self._maybe_cleanup_closed_market()
            for index, raw in enumerate(list(self._state["price_alerts"])):
                if str(raw.get("alert_id") or "") != alert_id:
                    continue
                updated = dict(raw)
                if str(updated.get("condition") or "") != "uptrend_end":
                    return
                code = str(updated.get("code") or "")
                latest_price = self._coerce_float(updated.get("last_price"))
                if not code or latest_price is None or latest_price <= 0:
                    return
                fired = self._check_uptrend_end_transition(updated, latest_price)
                if fired is not None:
                    dispatch_result = self._dispatch_uptrend_end_alert(updated, latest_price) or {}
                    if not dispatch_result.get("queued"):
                        ops_log(
                            LogSource.RT_RUNTIME,
                            f"uptrend-end alert timer callback queue failed alert_id={alert_id} "
                            f"code={code} error={dispatch_result.get('error')}",
                        )
                    if bool(updated.get("once_only", True)):
                        self._state["price_alerts"] = [
                            item
                            for item in self._state["price_alerts"]
                            if str(item.get("alert_id") or "") != alert_id
                        ]
                        self._release_price_subscription(code)
                        self._persist_state()
                        return
                    self._mark_uptrend_end_post_fire(updated)
                else:
                    self._start_uptrend_end_timer_locked(alert_id, updated)
                self._state["price_alerts"][index] = updated
                self._persist_state()
                return

    def _evaluate_fall_safes(self, code: str, events_with_prices: list[tuple[dict[str, Any], float]]) -> None:
        if not code or not events_with_prices:
            return
        batch = self._sc_price_batch(events_with_prices)
        latest_price = float(batch["latest_price"])
        min_price = float(batch["min_price"])
        changed = False
        retained: list[dict[str, Any]] = []
        triggered: list[dict[str, Any]] = []
        evaluated_count = 0
        for raw in self._state["fall_safes"]:
            if raw["code"] != code:
                retained.append(raw)
                continue
            updated = dict(raw)
            evaluated_count += 1
            previous_price = self._coerce_float(updated.get("last_price"))
            updated["last_price"] = latest_price
            changed = True
            if (
                previous_price is not None
                and previous_price >= float(updated["trigger_price"])
                and min_price < float(updated["trigger_price"])
            ):
                ops_log(LogSource.RT_RUNTIME,
                    f"fall-safe triggered fall_safe_id={updated['fall_safe_id']} code={code} "
                    f"previous_price={previous_price} min_price={min_price} latest_price={latest_price} "
                    f"trigger_price={float(updated['trigger_price'])}",
                )
                triggered.append(updated)
            else:
                retained.append(updated)
        if changed:
            self._state["fall_safes"] = retained
            self._persist_state()
            if triggered:
                ops_log(LogSource.RT_RUNTIME,
                    f"fall-safe batch state persisted before execution code={code} "
                    f"events={len(events_with_prices)} latest_price={latest_price} min_price={min_price} "
                    f"evaluated={evaluated_count} "
                    f"triggered={len(triggered)} remaining={len(retained)}",
                )
        elif evaluated_count:
            ops_log(LogSource.RT_RUNTIME,
                f"fall-safe batch evaluation complete code={code} events={len(events_with_prices)} "
                f"evaluated=0 triggered=0 persisted=False",
                level="debug",
            )
        for raw in triggered:
            try:
                self._release_price_subscription(str(raw["code"]))
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME,
                    f"Failed to release fall-safe RT subscription for {raw['fall_safe_id']}: {exc}",
                )
            self._execute_fall_safe(raw)

    def _execute_fall_safe(self, raw: dict[str, Any]) -> None:
        result: dict[str, Any] | None = None
        ops_log(LogSource.RT_RUNTIME,
            f"fall-safe execution begin fall_safe_id={raw['fall_safe_id']} "
            f"code={raw['code']} quantity={raw['quantity']}",
        )
        if self._fall_safe_executor is None:
            ops_log(LogSource.RT_RUNTIME, f"No fall-safe executor configured for {raw['fall_safe_id']}")
        else:
            try:
                result = self._fall_safe_executor(str(raw["account_no"]), str(raw["code"]), int(raw["quantity"]))
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME, f"Fall-safe execution failed for {raw['fall_safe_id']}: {exc}")
                result = {"accepted": False, "message": str(exc)}
        if raw.get("httpCallback") is not None:
            try:
                dispatch_result = self._dispatcher.dispatch(self._http_callback_from_dict(raw["httpCallback"])) or {}
                ops_log(LogSource.RT_RUNTIME,
                    f"fall-safe callback queued fall_safe_id={raw['fall_safe_id']} "
                    f"queued={dispatch_result.get('queued')} error={dispatch_result.get('error') or ''}",
                )
            except Exception as exc:
                ops_log(LogSource.RT_RUNTIME, f"Fall-safe callback failed for {raw['fall_safe_id']}: {exc}")
        if result is not None:
            ops_log(LogSource.RT_RUNTIME,
                f"fall-safe execution complete fall_safe_id={raw['fall_safe_id']} result={result}",
            )
            ops_log(LogSource.RT_RUNTIME, f"Fall-safe {raw['fall_safe_id']} executed with result: {result}")

    def _evaluate_stock_price_callbacks(
        self,
        code: str,
        events_with_prices: list[tuple[dict[str, Any], float]],
    ) -> None:
        if not code or not events_with_prices:
            return
        batch = self._sc_price_batch(events_with_prices)
        latest_price = float(batch["latest_price"])
        min_price = float(batch["min_price"])
        max_price = float(batch["max_price"])
        changed = False
        evaluated_count = 0
        fired_count = 0
        queued_count = 0
        for index, raw in enumerate(list(self._state["stock_price_callbacks"])):
            if raw["code"] != code:
                continue
            updated = dict(raw)
            callback_id = str(updated["stock_price_callback_id"])
            evaluated_count += 1
            updated["last_price"] = latest_price
            baseline_price = self._coerce_float(updated.get("baseline_price"))
            if baseline_price is None:
                updated["baseline_price"] = latest_price
                self._state["stock_price_callbacks"][index] = updated
                changed = True
                continue
            if callback_id in self._stock_price_callback_debounce_timers:
                self._stock_price_callback_debounce_pending[callback_id] = {
                    "code": code,
                    "latest_price": latest_price,
                }
                self._state["stock_price_callbacks"][index] = updated
                changed = True
                continue
            step = float(updated["step"])
            up_delta = max_price - baseline_price
            down_delta = min_price - baseline_price
            trigger_price: float | None = None
            direction: str | None = None
            if up_delta >= step or abs(down_delta) >= step:
                if up_delta >= abs(down_delta):
                    trigger_price = max_price
                    direction = "상향"
                else:
                    trigger_price = min_price
                    direction = "하향"
                try:
                    filter_allows = self._stock_price_callback_filter_allows(
                        updated.get("price_filter"),
                        trigger_price,
                    )
                except ValueError as exc:
                    ops_log(LogSource.RT_RUNTIME,
                        f"stock price callback skipped callback_id={updated.get('stock_price_callback_id', '')} "
                        f"code={code} reason=invalid_price_filter error={exc}",
                    )
                    filter_allows = False
                if filter_allows:
                    timestamp = _kst_now().strftime("%Y%m%d%H%M%S")
                    updated["baseline_price"] = latest_price
                    updated["last_direction"] = direction
                    updated["fired_count"] = int(updated.get("fired_count") or 0) + 1
                    updated["last_fired_at"] = timestamp
                    fired_count += 1
                    dispatch_result = self._dispatch_stock_price_callback(
                        updated,
                        code,
                        trigger_price,
                        direction,
                    )
                    if dispatch_result.get("queued"):
                        queued_count += 1
                    else:
                        ops_log(LogSource.RT_RUNTIME,
                            f"stock price callback queue failed callback_id={updated.get('stock_price_callback_id', '')} "
                            f"code={code} error={dispatch_result.get('error')}",
                        )
                    self._start_stock_price_callback_debounce_locked(callback_id)
            self._state["stock_price_callbacks"][index] = updated
            changed = True
        if changed:
            self._persist_state()
        if fired_count:
            ops_log(LogSource.RT_RUNTIME,
                f"stock price callback batch evaluation complete code={code} events={len(events_with_prices)} "
                f"latest_price={latest_price} min_price={min_price} max_price={max_price} "
                f"evaluated={evaluated_count} fired={fired_count} "
                f"queued={queued_count} persisted={changed}",
            )

    def _flush_stock_price_callback_debounce(self, callback_id: str) -> None:
        with self._state_lock:
            self._stock_price_callback_debounce_timers.pop(callback_id, None)
            pending = self._stock_price_callback_debounce_pending.pop(callback_id, None)
            if self._closed or pending is None:
                return
            self._maybe_cleanup_closed_market()
            for index, raw in enumerate(list(self._state["stock_price_callbacks"])):
                if str(raw.get("stock_price_callback_id") or "") != callback_id:
                    continue
                updated = dict(raw)
                code = str(updated.get("code") or pending.get("code") or "")
                latest_price = self._coerce_float(pending.get("latest_price"))
                if not code or latest_price is None or latest_price <= 0:
                    return
                updated["last_price"] = latest_price
                baseline_price = self._coerce_float(updated.get("baseline_price"))
                if baseline_price is None:
                    updated["baseline_price"] = latest_price
                    self._state["stock_price_callbacks"][index] = updated
                    self._persist_state()
                    return
                step = float(updated["step"])
                delta = latest_price - baseline_price
                if abs(delta) < step:
                    self._state["stock_price_callbacks"][index] = updated
                    self._persist_state()
                    return
                direction = "상향" if delta > 0 else "하향"
                try:
                    filter_allows = self._stock_price_callback_filter_allows(
                        updated.get("price_filter"),
                        latest_price,
                    )
                except ValueError as exc:
                    ops_log(
                        LogSource.RT_RUNTIME,
                        f"stock price callback skipped callback_id={callback_id} "
                        f"code={code} reason=invalid_price_filter error={exc}",
                    )
                    filter_allows = False
                if filter_allows:
                    timestamp = _kst_now().strftime("%Y%m%d%H%M%S")
                    updated["baseline_price"] = latest_price
                    updated["last_direction"] = direction
                    updated["fired_count"] = int(updated.get("fired_count") or 0) + 1
                    updated["last_fired_at"] = timestamp
                    dispatch_result = self._dispatch_stock_price_callback(
                        updated,
                        code,
                        latest_price,
                        direction,
                    )
                    if not dispatch_result.get("queued"):
                        ops_log(
                            LogSource.RT_RUNTIME,
                            f"stock price callback trailing queue failed callback_id={callback_id} "
                            f"code={code} error={dispatch_result.get('error')}",
                        )
                    self._start_stock_price_callback_debounce_locked(callback_id)
                    ops_log(
                        LogSource.RT_RUNTIME,
                        f"stock price callback debounce trailing fired callback_id={callback_id} "
                        f"code={code} latest_price={latest_price} direction={direction}",
                    )
                self._state["stock_price_callbacks"][index] = updated
                self._persist_state()
                return

    def _dispatch_stock_price_callback(
        self,
        raw: dict[str, Any],
        code: str,
        trigger_price: float,
        direction: str,
    ) -> dict[str, Any]:
        rendered_callback = self._render_http_callback(
            self._http_callback_from_dict(raw["httpCallback"]),
            self._stock_price_callback_replacements(code, trigger_price, direction),
        )
        return self._dispatcher.dispatch(rendered_callback) or {}

    def _check_recovery_fail_transition(
        self,
        raw: dict[str, Any],
        current_price: float,
    ) -> dict[str, Any] | None:
        now = _kst_now()
        timestamp = now.strftime("%Y%m%d%H%M%S")
        valid_after = self._valid_after_time(raw.get("valid_after"))
        if valid_after is not None and now.time() < valid_after:
            self._reset_recovery_fail_state(raw)
            return None

        breach_price = self._coerce_float(raw.get("breach_price"))
        recovery_price = self._coerce_float(raw.get("recovery_price"))
        failure_minutes = self._coerce_float(raw.get("failure_minutes"))
        recovery_minutes = self._coerce_float(raw.get("recovery_minutes"))
        if (
            breach_price is None
            or recovery_price is None
            or failure_minutes is None
            or recovery_minutes is None
            or breach_price <= 0
            or recovery_price <= breach_price
            or failure_minutes <= 0
            or recovery_minutes <= 0
        ):
            return None

        state = str(raw.get("recovery_state") or "waiting")
        if state == "failed":
            if current_price >= recovery_price:
                raw["recovery_state"] = "failed_recovering"
                raw["recovery_since"] = timestamp
            return None

        if state == "waiting":
            if current_price <= breach_price:
                raw["recovery_state"] = "breached"
                raw["breached_at"] = timestamp
                raw.pop("recovery_since", None)
            return None

        if state == "breached":
            breached_at = self._parse_compact_kst(str(raw.get("breached_at") or ""))
            if breached_at is None:
                raw["breached_at"] = timestamp
                return None
            if current_price >= recovery_price:
                raw["recovery_state"] = "recovering"
                raw["recovery_since"] = timestamp
                return None
            if (now - breached_at).total_seconds() >= failure_minutes * 60.0:
                raw["last_triggered_at"] = timestamp
                return {"reason": "failure_hold_elapsed", "triggered_at": timestamp}
            return None

        if state == "recovering":
            recovery_since = self._parse_compact_kst(str(raw.get("recovery_since") or ""))
            if recovery_since is None:
                raw["recovery_since"] = timestamp
                return None
            if current_price < recovery_price:
                raw["last_triggered_at"] = timestamp
                return {"reason": "recovery_hold_broken", "triggered_at": timestamp}
            if (now - recovery_since).total_seconds() >= recovery_minutes * 60.0:
                self._reset_recovery_fail_state(raw)
            return None

        if state == "failed_recovering":
            recovery_since = self._parse_compact_kst(str(raw.get("recovery_since") or ""))
            if recovery_since is None:
                raw["recovery_since"] = timestamp
                return None
            if current_price < recovery_price:
                raw["recovery_state"] = "failed"
                raw.pop("recovery_since", None)
                return None
            if (now - recovery_since).total_seconds() >= recovery_minutes * 60.0:
                raw["last_triggered_at"] = timestamp
                return {"event_type": "recovery_fail_resolved", "triggered_at": timestamp}
            return None

        self._reset_recovery_fail_state(raw)
        return None

    @staticmethod
    def _reset_recovery_fail_state(raw: dict[str, Any]) -> None:
        raw["recovery_state"] = "waiting"
        raw.pop("breached_at", None)
        raw.pop("recovery_since", None)

    @staticmethod
    def _mark_recovery_fail_post_failure(raw: dict[str, Any]) -> None:
        raw["recovery_state"] = "failed"
        raw.pop("breached_at", None)
        raw.pop("recovery_since", None)

    def _dispatch_recovery_fail_alert(
        self,
        raw: dict[str, Any],
        current_price: float,
        event_type: str,
    ) -> dict[str, Any]:
        callback = self._http_callback_from_dict(raw["httpCallback"])
        replacements = self._recovery_fail_replacements(raw, current_price, event_type)
        if callback.body is None:
            rendered_callback = HttpCallbackSpec(
                method=callback.method,
                url=callback.url,
                headers=dict(callback.headers),
                body=replacements["summary"],
                body_format="text",
            )
        else:
            rendered_callback = self._render_http_callback(callback, replacements)
        return self._dispatcher.dispatch(rendered_callback) or {}

    def _check_uptrend_end_transition(
        self,
        raw: dict[str, Any],
        current_price: float,
    ) -> dict[str, Any] | None:
        now = _kst_now()
        timestamp = now.strftime("%Y%m%d%H%M%S")
        valid_after = self._valid_after_time(raw.get("valid_after"))
        if valid_after is not None and now.time() < valid_after:
            self._reset_uptrend_end_state(raw)
            return None

        start_price = self._coerce_float(raw.get("start_price"))
        end_price = self._coerce_float(raw.get("end_price", raw.get("threshold")))
        end_minutes = self._coerce_float(raw.get("end_minutes"))
        if (
            start_price is None
            or end_price is None
            or end_minutes is None
            or start_price <= 0
            or end_price <= 0
            or start_price <= end_price
            or end_minutes <= 0
        ):
            return None

        state = str(raw.get("uptrend_state") or "waiting")
        if state == "waiting":
            if current_price >= start_price:
                raw["uptrend_state"] = "rising"
                raw["uptrend_started_at"] = timestamp
                raw.pop("ending_since", None)
            return None

        if state == "rising":
            if current_price <= end_price:
                raw["uptrend_state"] = "ending"
                raw["ending_since"] = timestamp
            return None

        if state == "ending":
            ending_since = self._parse_compact_kst(str(raw.get("ending_since") or ""))
            if ending_since is None:
                raw["ending_since"] = timestamp
                return None
            if current_price > end_price:
                raw["uptrend_state"] = "rising"
                raw.pop("ending_since", None)
                return None
            if (now - ending_since).total_seconds() >= end_minutes * 60.0:
                raw["last_triggered_at"] = timestamp
                return {"event_type": "uptrend_end", "triggered_at": timestamp}
            return None

        if state == "ended":
            if current_price >= start_price:
                raw["uptrend_state"] = "rising"
                raw["uptrend_started_at"] = timestamp
                raw.pop("ending_since", None)
            return None

        self._reset_uptrend_end_state(raw)
        return None

    @staticmethod
    def _reset_uptrend_end_state(raw: dict[str, Any]) -> None:
        raw["uptrend_state"] = "waiting"
        raw.pop("uptrend_started_at", None)
        raw.pop("ending_since", None)

    @staticmethod
    def _mark_uptrend_end_post_fire(raw: dict[str, Any]) -> None:
        raw["uptrend_state"] = "ended"
        raw.pop("ending_since", None)

    def _dispatch_uptrend_end_alert(
        self,
        raw: dict[str, Any],
        current_price: float,
    ) -> dict[str, Any]:
        callback = self._http_callback_from_dict(raw["httpCallback"])
        replacements = self._uptrend_end_replacements(raw, current_price)
        if callback.body is None:
            rendered_callback = HttpCallbackSpec(
                method=callback.method,
                url=callback.url,
                headers=dict(callback.headers),
                body=replacements["summary"],
                body_format="text",
            )
        else:
            rendered_callback = self._render_http_callback(callback, replacements)
        return self._dispatcher.dispatch(rendered_callback) or {}

    def _check_alert_batch_transition(
        self,
        raw: dict[str, Any],
        batch: dict[str, Any],
    ) -> PriceAlertFiredPayload | None:
        condition = str(raw["condition"])
        threshold = float(raw["threshold"])
        timestamp = _kst_now().strftime("%Y%m%d%H%M%S")
        latest_price = float(batch["latest_price"])
        if condition == "climb":
            previous_side = raw.get("last_side")
            current_side = "above" if latest_price >= threshold else "below"
            raw["last_side"] = current_side
            if previous_side == "below" and float(batch["max_price"]) >= threshold:
                raw["last_triggered_at"] = timestamp
                return self._build_alert_payload(raw, float(batch["max_price"]), timestamp)
            return None
        if condition == "fall":
            previous_side = raw.get("last_side")
            current_side = "below" if latest_price <= threshold else "above"
            raw["last_side"] = current_side
            if previous_side == "above" and float(batch["min_price"]) <= threshold:
                raw["last_triggered_at"] = timestamp
                return self._build_alert_payload(raw, float(batch["min_price"]), timestamp)
            return None
        if condition in {"recovery_fail", "uptrend_end"}:
            return None
        baseline_price = self._coerce_float(raw.get("baseline_price"))
        baseline_at = str(raw.get("baseline_at") or "")
        window_minutes = int(raw.get("window_minutes") or 0)
        now = _kst_now()
        if baseline_price is None or not baseline_at:
            raw["baseline_price"] = latest_price
            raw["baseline_at"] = timestamp
            return None
        baseline_dt = self._parse_compact_kst(baseline_at)
        if baseline_dt is None or (now - baseline_dt).total_seconds() > window_minutes * 60:
            raw["baseline_price"] = latest_price
            raw["baseline_at"] = timestamp
            return None
        if not baseline_price:
            return None
        max_move_percent = abs((float(batch["max_price"]) - baseline_price) / baseline_price * 100.0)
        min_move_percent = abs((float(batch["min_price"]) - baseline_price) / baseline_price * 100.0)
        if max_move_percent >= min_move_percent:
            trigger_price = float(batch["max_price"])
            move_percent = max_move_percent
        else:
            trigger_price = float(batch["min_price"])
            move_percent = min_move_percent
        last_triggered = self._parse_compact_kst(str(raw.get("last_triggered_at") or ""))
        in_cooldown = last_triggered is not None and (now - last_triggered).total_seconds() <= window_minutes * 60
        if move_percent >= threshold and not in_cooldown:
            raw["last_triggered_at"] = timestamp
            raw["baseline_price"] = latest_price
            raw["baseline_at"] = timestamp
            return self._build_alert_payload(raw, trigger_price, timestamp)
        return None

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
        if condition in {"recovery_fail", "uptrend_end"}:
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
            event_type="price_alert",
            alert_id=str(raw["alert_id"]),
            code=str(raw["code"]),
            condition=str(raw["condition"]),  # type: ignore[arg-type]
            threshold=float(raw["threshold"]),
            current_price=current_price,
            message=str(raw["message"]),
            triggered_at=triggered_at,
        )

    def _normalize_news_types(self, types: list[str]) -> list[str]:
        normalized = sorted({item.strip().upper() for item in types if item and item.strip()})
        invalid = [item for item in normalized if item not in {"A", "M", "E", "Y", "H", "I", "F", "U"}]
        if not normalized or invalid:
            raise ValueError("types must contain only A, M, E, Y, H, I, F, U")
        return normalized

    @staticmethod
    def _http_callback_from_dict(raw: dict[str, Any]) -> HttpCallbackSpec:
        return HttpCallbackSpec(
            method=str(raw["method"]),
            url=str(raw["url"]),
            headers={str(key): str(value) for key, value in dict(raw.get("headers") or {}).items()},
            body=raw.get("body"),
            body_format=raw.get("bodyFormat"),
        )

    def _news_replacements(self, payload: dict[str, Any]) -> dict[str, str]:
        return {
            "news_type": str(payload.get("news_type") or ""),
            "news_type_label": str(payload.get("news_type_label") or ""),
            "date": str(payload.get("date") or ""),
            "time": str(payload.get("time") or ""),
            "article_id": str(payload.get("article_id") or ""),
            "code": str(payload.get("code") or ""),
            "name": self._stock_name(str(payload.get("code") or "")),
            "title": str(payload.get("title") or ""),
            "delete_flag_label": self._delete_flag_label(payload.get("deleted_flag")),
        }

    def _disclosure_replacements(self, payload: dict[str, Any]) -> dict[str, str]:
        title = str(payload.get("title") or "").strip() or "제목 없음"
        return {
            "disclosure_type": str(payload.get("news_type") or ""),
            "disclosure_type_label": str(payload.get("news_type_label") or ""),
            "date": str(payload.get("date") or ""),
            "time": str(payload.get("time") or ""),
            "article_id": str(payload.get("article_id") or ""),
            "code": str(payload.get("code") or ""),
            "name": self._stock_name(str(payload.get("code") or "")),
            "title": title,
            "delete_flag_label": self._delete_flag_label(payload.get("deleted_flag")),
        }

    def _stock_price_callback_replacements(self, code: str, price: float, direction: str) -> dict[str, str]:
        raw_price = self._format_compact_decimal(price)
        return {
            "name": self._stock_name(code),
            "price": format_display_decimal(price),
            "priceRaw": raw_price,
            "price_raw": raw_price,
            "direction": direction,
        }

    def _recovery_fail_replacements(
        self,
        raw: dict[str, Any],
        current_price: float,
        event_type: str,
    ) -> dict[str, str]:
        def raw_number(value: Any) -> str:
            coerced = self._coerce_float(value)
            if coerced is None:
                return ""
            return self._format_compact_decimal(coerced)

        code = str(raw.get("code") or "")
        name = self._stock_name(code)
        current_price_raw = raw_number(current_price)
        breach_price_raw = raw_number(raw.get("breach_price", raw.get("threshold")))
        recovery_price_raw = raw_number(raw.get("recovery_price"))
        summary_subject = name or code
        if event_type == "recovery_fail_resolved":
            event_type_label = "회복 실패 해소"
            summary = (
                f"{event_type_label}: {summary_subject} "
                f"{format_display_decimal(recovery_price_raw)}원 회복 확인. "
                f"현재가 {format_display_decimal(current_price_raw)}원"
            )
        else:
            event_type = "recovery_fail"
            event_type_label = "회복 실패"
            summary = (
                f"{event_type_label}: {summary_subject} "
                f"{format_display_decimal(breach_price_raw)}원 이탈 후 "
                f"{format_display_decimal(recovery_price_raw)}원 회복 실패. "
                f"현재가 {format_display_decimal(current_price_raw)}원"
            )
        return {
            "event_type": event_type,
            "event_type_label": event_type_label,
            "alert_id": str(raw.get("alert_id") or ""),
            "code": code,
            "name": name,
            "summary": summary,
            "current_price": current_price_raw,
            "breach_price": breach_price_raw,
            "recovery_price": recovery_price_raw,
            "failure_minutes": raw_number(raw.get("failure_minutes")),
            "recovery_minutes": raw_number(raw.get("recovery_minutes")),
            "valid_after": str(raw.get("valid_after") or ""),
            "breached_at": str(raw.get("breached_at") or ""),
            "triggered_at": str(raw.get("last_triggered_at") or _kst_now().strftime("%Y%m%d%H%M%S")),
        }

    def _uptrend_end_replacements(
        self,
        raw: dict[str, Any],
        current_price: float,
    ) -> dict[str, str]:
        def raw_number(value: Any) -> str:
            coerced = self._coerce_float(value)
            if coerced is None:
                return ""
            return self._format_compact_decimal(coerced)

        code = str(raw.get("code") or "")
        name = self._stock_name(code)
        current_price_raw = raw_number(current_price)
        start_price_raw = raw_number(raw.get("start_price"))
        end_price_raw = raw_number(raw.get("end_price", raw.get("threshold")))
        event_type_label = "상승세 종료"
        summary_subject = name or code
        summary = (
            f"{event_type_label}: {summary_subject} "
            f"{format_display_decimal(start_price_raw)}원 돌파 후 "
            f"{format_display_decimal(end_price_raw)}원 이탈 확인. "
            f"현재가 {format_display_decimal(current_price_raw)}원"
        )
        return {
            "event_type": "uptrend_end",
            "event_type_label": event_type_label,
            "alert_id": str(raw.get("alert_id") or ""),
            "code": code,
            "name": name,
            "summary": summary,
            "current_price": current_price_raw,
            "start_price": start_price_raw,
            "end_price": end_price_raw,
            "end_minutes": raw_number(raw.get("end_minutes")),
            "valid_after": str(raw.get("valid_after") or ""),
            "uptrend_started_at": str(raw.get("uptrend_started_at") or ""),
            "ending_since": str(raw.get("ending_since") or ""),
            "triggered_at": str(raw.get("last_triggered_at") or _kst_now().strftime("%Y%m%d%H%M%S")),
        }

    def _normalize_stock_price_callback_filter(self, price_filter: str | None) -> str | None:
        parsed = self._parse_stock_price_callback_filter(price_filter)
        if parsed is None:
            return None
        threshold, operator = parsed
        return f"{self._format_compact_decimal(threshold)}{operator}"

    def _stock_price_callback_filter_allows(self, price_filter: object, current_price: float) -> bool:
        parsed = self._parse_stock_price_callback_filter(price_filter)
        if parsed is None:
            return True
        threshold, operator = parsed
        if operator == "+":
            return current_price >= threshold
        return current_price <= threshold

    @staticmethod
    def _parse_stock_price_callback_filter(price_filter: object) -> tuple[float, str] | None:
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

    @staticmethod
    def _format_compact_decimal(value: float) -> str:
        if float(value).is_integer():
            return str(int(value))
        return f"{value:.10f}".rstrip("0").rstrip(".")

    def _stock_name(self, code: str) -> str:
        try:
            for stock in self._client.list_stocks():
                if stock.code == code:
                    return stock.name
        except Exception:
            pass
        return ""

    @staticmethod
    def _parse_compact_kst(value: str) -> datetime | None:
        try:
            return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=KST)
        except ValueError:
            return None

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _delete_flag_label(value: Any) -> str:
        cleaned = str(value or "").strip().upper()
        if cleaned in {"", "I"}:
            return "normal"
        if cleaned == "D":
            return "deleted"
        if cleaned == "U":
            return "updated"
        return cleaned.lower()
