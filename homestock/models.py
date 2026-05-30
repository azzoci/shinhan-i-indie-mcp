from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


OrderSide = Literal["buy", "sell"]
OrderType = Literal["limit", "market"]


@dataclass(frozen=True)
class HealthStatus:
    ok: bool
    backend: str
    python_architecture: str
    ocx_ready: bool
    login_ready: bool
    live_orders_allowed: bool
    message: str
    indi_process_running: bool = True
    indi_process_restarted: bool = False
    indi_process_message: str = ""
    rt_news_registered: bool = False
    rt_disclosure_registered: bool = False
    gold_rt_control_ready: bool = False
    gold_rt_subscription_count: int = 0
    gold_rt_subscriptions: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Stock:
    code: str
    name: str
    market: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GoldProduct:
    code: str
    standard_code: str
    name: str
    english_name: str = ""
    listed_date: str = ""
    trading_unit: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DailyPrice:
    date: str
    open: int
    high: int
    low: int
    close: int
    volume: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class IntradayPrice:
    date: str
    time: str
    open: int
    high: int
    low: int
    close: int
    volume: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GoldDailyPrice:
    date: str
    open: int
    high: int
    low: int
    close: int
    volume: int
    turnover: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GoldIntradayPrice:
    date: str
    time: str
    open: int
    high: int
    low: int
    close: int
    volume: int
    turnover: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MarketIndexPricePoint:
    date: str
    open: float
    high: float
    low: float
    close: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Account:
    account_no: str
    name: str
    product_code: str | None = None
    product_name: str | None = None
    parent_product_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AccountSummary:
    account_no: str
    total_deposit: int
    withdrawable_amount: int
    orderable_amount: int
    total_purchase_amount: int
    total_evaluation_amount: int
    total_profit_loss: int
    total_return_rate: float
    estimated_total_deposit: int
    net_asset_value: int
    total_asset_value: int
    stock_asset_value: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GoldAccountSummary:
    account_no: str
    total_deposit: int
    orderable_amount: int
    withdrawable_amount: int
    total_purchase_amount: int
    total_evaluation_amount: int
    total_profit_loss: int
    total_asset_value: int
    total_return_rate: float
    total_margin: int
    settlement_buy_amount1: int
    settlement_sell_amount1: int
    buy_settlement_amount: int
    sell_settlement_amount: int
    estimated_total_deposit: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FundamentalPoint:
    code: str
    date: str
    period_type: str
    revenue: int
    operating_income: int
    net_income: int
    operating_margin: float
    net_margin: float
    roe: float
    eps: float
    eps_growth: float
    bps: float
    per: float
    per_ttm: float
    pbr: float
    dps: float
    dividend_yield: float
    ev_ebitda: float
    peg: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class QuoteSnapshot:
    code: str
    current_price: int
    previous_close: int
    change_amount: int
    change_percent: float
    year_high: int
    year_high_date: str
    year_low: int
    year_low_date: str
    upper_limit: int
    lower_limit: int
    market_cap: int
    foreign_ownership_ratio: float
    eps: float
    per: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GoldQuoteSnapshot:
    code: str
    standard_code: str
    trade_time: str
    current_price: float
    change_type: str
    change_amount: float
    change_percent: float
    cumulative_volume: float
    turnover: float
    fill_size: float
    open: float
    high: float
    low: float
    open_time: str
    high_time: str
    low_time: str
    block_volume: float
    trade_side: str
    best_ask: float
    best_bid: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class InvestorFlowPoint:
    code: str
    date: str
    close: int
    volume: int
    retail_net: int
    retail_cumulative_net: int
    foreign_net: int
    foreign_cumulative_net: int
    institution_net: int
    institution_cumulative_net: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MarketInvestorFlowPoint:
    time: str
    retail: dict[str, int]
    foreign: dict[str, int]
    institution: dict[str, int]
    institution_breakdown: dict[str, dict[str, int]] | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        if self.institution_breakdown is None:
            result.pop("institution_breakdown", None)
        return result


@dataclass(frozen=True)
class ForeignFlowRanking:
    code: str
    market: str
    name: str
    current_price: int
    change_percent: float
    foreign_cumulative_volume: int
    foreign_ownership_ratio: float
    institution_cumulative_volume: int
    listed_shares: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TopMover:
    rank: int
    code: str
    name: str
    current_price: int
    change_percent: float
    volume: int
    trade_strength: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MarketScannerItem:
    code: str
    name: str
    current_price: int
    change_percent: float
    volume: int
    metric_value: float
    metric_label: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MarketNewsItem:
    date: str
    time: str
    title: str
    news_type: str
    news_type_label: str
    code: str
    article_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsContent:
    news_type: str
    news_type_label: str
    date: str
    time: str
    extracted_codes: list[str]
    content: str
    raw_html: str = ""
    links: list[dict[str, str]] = field(default_factory=list)
    rcpNo: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DisclosureContent:
    rcpNo: str | None
    content: str
    content_format: Literal["html"] = "html"
    source: str = ""
    viewer_url: str = ""
    dtd: str | None = None
    print_page_break_selector: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


HttpBodyFormat = Literal["json", "form", "text"]
AlertCondition = Literal["fastmove", "climb", "fall", "recovery_fail", "uptrend_end"]


@dataclass(frozen=True)
class HttpCallbackSpec:
    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    body: dict[str, Any] | str | None = None
    body_format: HttpBodyFormat | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "method": self.method,
            "url": self.url,
        }
        if self.headers:
            payload["headers"] = dict(self.headers)
        if self.body is not None:
            payload["body"] = self.body
        if self.body_format is not None:
            payload["bodyFormat"] = self.body_format
        return payload


@dataclass(frozen=True)
class RealtimeEventPayload:
    rt_type: str
    news_type: str
    news_type_label: str
    date: str
    article_id: str
    deleted_flag: str | None
    time: str
    code: str
    title: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DisclosureSubscriptionRecord:
    subscription_id: str
    code: str
    http_callback: HttpCallbackSpec
    registered_at: str
    last_event_at: str | None = None
    evaluated_event_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "subscription_id": self.subscription_id,
            "code": self.code,
            "httpCallback": self.http_callback.to_dict(),
            "registered_at": self.registered_at,
            "last_event_at": self.last_event_at,
            "evaluated_event_count": self.evaluated_event_count,
        }


@dataclass(frozen=True)
class NewsSubscriptionRecord:
    subscription_id: str
    types: list[str]
    code: str | None
    http_callback: HttpCallbackSpec
    registered_at: str
    last_event_at: str | None = None
    evaluated_event_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "subscription_id": self.subscription_id,
            "types": list(self.types),
            "code": self.code,
            "httpCallback": self.http_callback.to_dict(),
            "registered_at": self.registered_at,
            "last_event_at": self.last_event_at,
            "evaluated_event_count": self.evaluated_event_count,
        }


@dataclass(frozen=True)
class SystemCallbackRecord:
    system_callback_id: str
    http_callback: HttpCallbackSpec
    registered_at: str
    last_event_at: str | None = None
    sent_event_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "system_callback_id": self.system_callback_id,
            "httpCallback": self.http_callback.to_dict(),
            "registered_at": self.registered_at,
            "last_event_at": self.last_event_at,
            "sent_event_count": self.sent_event_count,
        }


@dataclass(frozen=True)
class PriceAlertRecord:
    alert_id: str
    code: str
    condition: AlertCondition
    threshold: float
    window_minutes: int | None
    message: str
    http_callback: HttpCallbackSpec
    created_at: str
    last_price: float | None = None
    last_eval_at: str | None = None
    last_triggered_at: str | None = None
    last_side: str | None = None
    baseline_price: float | None = None
    baseline_at: str | None = None
    debounce_seconds: float | None = None
    once_only: bool = False
    breach_price: float | None = None
    recovery_price: float | None = None
    failure_minutes: float | None = None
    recovery_minutes: float | None = None
    valid_after: str | None = None
    recovery_state: str | None = None
    breached_at: str | None = None
    recovery_since: str | None = None
    start_price: float | None = None
    end_price: float | None = None
    end_minutes: float | None = None
    uptrend_state: str | None = None
    uptrend_started_at: str | None = None
    ending_since: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "code": self.code,
            "condition": self.condition,
            "threshold": self.threshold,
            "window_minutes": self.window_minutes,
            "message": self.message,
            "httpCallback": self.http_callback.to_dict(),
            "created_at": self.created_at,
            "last_price": self.last_price,
            "last_eval_at": self.last_eval_at,
            "last_triggered_at": self.last_triggered_at,
            "last_side": self.last_side,
            "baseline_price": self.baseline_price,
            "baseline_at": self.baseline_at,
            "debounce_seconds": self.debounce_seconds,
            "once_only": self.once_only,
            "breach_price": self.breach_price,
            "recovery_price": self.recovery_price,
            "failure_minutes": self.failure_minutes,
            "recovery_minutes": self.recovery_minutes,
            "valid_after": self.valid_after,
            "recovery_state": self.recovery_state,
            "breached_at": self.breached_at,
            "recovery_since": self.recovery_since,
            "start_price": self.start_price,
            "end_price": self.end_price,
            "end_minutes": self.end_minutes,
            "uptrend_state": self.uptrend_state,
            "uptrend_started_at": self.uptrend_started_at,
            "ending_since": self.ending_since,
        }


@dataclass(frozen=True)
class PriceAlertFiredPayload:
    event_type: str
    alert_id: str
    code: str
    condition: AlertCondition
    threshold: float
    current_price: float
    message: str
    triggered_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StockPriceCallbackRecord:
    stock_price_callback_id: str
    code: str
    step: float
    price_filter: str | None
    http_callback: HttpCallbackSpec
    registered_at: str
    last_price: float | None = None
    baseline_price: float | None = None
    last_direction: str | None = None
    fired_count: int = 0
    last_fired_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "stock_price_callback_id": self.stock_price_callback_id,
            "code": self.code,
            "step": self.step,
            "price_filter": self.price_filter,
            "httpCallback": self.http_callback.to_dict(),
            "registered_at": self.registered_at,
            "last_price": self.last_price,
            "baseline_price": self.baseline_price,
            "last_direction": self.last_direction,
            "fired_count": self.fired_count,
            "last_fired_at": self.last_fired_at,
        }


@dataclass(frozen=True)
class FallSafeRecord:
    fall_safe_id: str
    account_no: str
    code: str
    trigger_price: float
    quantity: int
    http_callback: HttpCallbackSpec | None
    registered_at: str
    last_price: float | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "fall_safe_id": self.fall_safe_id,
            "account_no": self.account_no,
            "code": self.code,
            "trigger_price": self.trigger_price,
            "quantity": self.quantity,
            "registered_at": self.registered_at,
            "last_price": self.last_price,
        }
        if self.http_callback is not None:
            payload["httpCallback"] = self.http_callback.to_dict()
        return payload


@dataclass(frozen=True)
class UnifiedRuntimeState:
    version: int
    trading_date: str
    updated_at: str
    subscriptions: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    price_alerts: list[dict[str, Any]] = field(default_factory=list)
    fall_safes: list[dict[str, Any]] = field(default_factory=list)
    stock_price_callbacks: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "trading_date": self.trading_date,
            "updated_at": self.updated_at,
            "subscriptions": self.subscriptions,
            "price_alerts": self.price_alerts,
            "fall_safes": self.fall_safes,
            "stock_price_callbacks": self.stock_price_callbacks,
        }


@dataclass(frozen=True)
class OrderBookLevel:
    ask_price: int
    ask_size: int
    bid_price: int
    bid_size: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrderBook:
    code: str
    received_at: str
    market_phase: str
    levels: list[OrderBookLevel]
    source: str = ""
    partial: bool = False
    available: bool = True
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "received_at": self.received_at,
            "market_phase": self.market_phase,
            "levels": [level.to_dict() for level in self.levels],
            "source": self.source,
            "partial": self.partial,
            "available": self.available,
            "message": self.message,
        }


@dataclass(frozen=True)
class BalanceItem:
    account_no: str
    code: str
    name: str
    quantity: int
    avg_price: int
    current_price: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GoldBalanceItem:
    account_no: str
    code: str
    name: str
    credit_type: str
    quantity: int
    sellable_quantity: int
    restricted_quantity: int
    deliverable_quantity: int
    avg_price: int
    current_price: int
    price_change: int
    purchase_amount: int
    credit_amount: int
    valuation_amount: int
    profit_loss: int
    return_rate: float
    trading_unit: int
    security_type: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class GoldAccountBalance:
    account_no: str
    summary: GoldAccountSummary
    balance: GoldBalanceItem | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Execution:
    order_id: str
    code: str
    side: OrderSide
    quantity: int
    price: int
    status: str
    executed_at: str
    raw_order_id: str = ""
    original_order_id: str = ""
    sor_order_id: str = ""
    sor_original_order_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OpenOrder:
    order_id: str
    code: str
    name: str
    side: OrderSide
    order_type: OrderType
    price: int
    quantity: int
    filled_quantity: int
    unfilled_quantity: int
    order_time: str
    status: str
    raw_order_id: str = ""
    original_raw_order_id: str = ""
    order_method_code: str = ""
    order_method_name: str = ""
    order_exchange_code: str = ""
    order_exchange_name: str = ""
    sor_order_id: str = ""
    sor_original_order_id: str = ""
    credit_trade_type: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TradeHistoryItem:
    date: str
    trade_type: str
    code: str
    raw_code: str
    name: str
    quantity: int
    price: int
    fee: int
    tax: int
    trade_amount: int
    credit_amount: int
    unpaid_repayment: int
    overdue_fee: int
    credit_interest: int
    change_amount: int
    final_amount: int
    order_channel: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AccountLedgerItem:
    date: str
    transaction_type: str
    summary: str
    code: str
    raw_code: str
    name: str
    quantity: int
    price: int
    fee: int
    tax: int
    trade_amount: int
    credit_amount: int
    unpaid_repayment: int
    credit_interest: int
    overdue_fee: int
    substitute_account: str
    change_amount: int
    final_amount: int
    loan_date: str
    maturity_date: str
    product_no: str
    bond_type: str
    transaction_id: str
    taxable_amount: int
    deposit_usage_fee: int
    order_user_id: str
    requester_name: str
    financial_institution_name: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrderRequest:
    account_no: str
    code: str
    side: OrderSide
    quantity: int
    price: int | None = None
    order_type: OrderType = "limit"
    original_order_id: str | None = None
    credit_trade_type: str | None = None
    order_method_code: str | None = None
    sor_original_order_id: str | None = None


@dataclass(frozen=True)
class GoldOrderRequest:
    account_no: str
    code: str
    side: OrderSide
    quantity: int
    price: int | None
    original_order_id: str | None = None
    action: Literal["place", "modify", "cancel"] = "place"


@dataclass(frozen=True)
class OrderResult:
    accepted: bool
    live_order: bool
    order_id: str | None
    message: str
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
