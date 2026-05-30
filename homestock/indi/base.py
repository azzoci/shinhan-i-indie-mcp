from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from homestock.models import (
    Account,
    AccountLedgerItem,
    AccountSummary,
    BalanceItem,
    DailyPrice,
    Execution,
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
    ForeignFlowRanking,
    MarketInvestorFlowPoint,
    MarketIndexPricePoint,
    MarketNewsItem,
    MarketScannerItem,
    NewsContent,
    OrderBook,
    OrderRequest,
    OrderResult,
    OpenOrder,
    QuoteSnapshot,
    Stock,
    TradeHistoryItem,
    TopMover,
)


class IndiClient(ABC):
    @abstractmethod
    def health_check(self, live_orders_allowed: bool) -> HealthStatus:
        raise NotImplementedError

    @abstractmethod
    def list_stocks(self) -> list[Stock]:
        raise NotImplementedError

    @abstractmethod
    def list_gold_products(self) -> list[GoldProduct]:
        raise NotImplementedError

    @abstractmethod
    def get_daily_prices(self, code: str, start_date: str | None, end_date: str | None) -> list[DailyPrice]:
        raise NotImplementedError

    def get_intraday_prices(self, code: str, date: str, interval_minutes: int = 5) -> list[IntradayPrice]:
        raise NotImplementedError

    @abstractmethod
    def get_gold_daily_prices(
        self,
        code: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[GoldDailyPrice]:
        raise NotImplementedError

    @abstractmethod
    def get_gold_intraday_prices(
        self,
        code: str,
        date: str,
        interval_minutes: int = 5,
    ) -> list[GoldIntradayPrice]:
        raise NotImplementedError

    @abstractmethod
    def get_market_index_prices(
        self,
        start_date: str,
        end_date: str,
    ) -> dict[str, list[MarketIndexPricePoint]]:
        raise NotImplementedError

    def get_sector_index_prices(
        self,
        sector_code: str,
        start_date: str,
        end_date: str,
        interval: str = "D",
    ) -> list[MarketIndexPricePoint]:
        raise NotImplementedError

    def get_stock_sector_profile(self, code: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def subscribe_realtime_price(self, code: str) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def unsubscribe_realtime_price(self, code: str) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def subscribe_gold_realtime_price(self, code: str) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def unsubscribe_gold_realtime_price(self, code: str) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def subscribe_disclosure_feed(self, code: str) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def unsubscribe_disclosure_feed(self, code: str) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def subscribe_news_feed(self, code: str | None = None) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def unsubscribe_news_feed(self, code: str | None = None) -> dict[str, object]:
        raise NotImplementedError

    @abstractmethod
    def register_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def unregister_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def register_gold_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def unregister_gold_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def normalize_stock_code(self, code: str | None) -> str:
        raise NotImplementedError

    @abstractmethod
    def normalize_gold_code(self, code: str | None) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_last_rt_error_details(self) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def get_accounts(self) -> list[Account]:
        raise NotImplementedError

    @abstractmethod
    def get_account_summary(self, account_no: str) -> AccountSummary:
        raise NotImplementedError

    @abstractmethod
    def get_gold_account_summary(self, account_no: str) -> GoldAccountSummary:
        raise NotImplementedError

    @abstractmethod
    def get_gold_account_balance(self, account_no: str) -> GoldAccountBalance:
        raise NotImplementedError

    @abstractmethod
    def get_fundamentals(
        self,
        code: str,
        consolidated: bool = True,
        quarterly: bool = True,
    ) -> list[FundamentalPoint]:
        raise NotImplementedError

    @abstractmethod
    def get_quote_snapshot(self, code: str) -> QuoteSnapshot:
        raise NotImplementedError

    @abstractmethod
    def get_gold_quote_snapshot(self, code: str) -> GoldQuoteSnapshot:
        raise NotImplementedError

    @abstractmethod
    def get_investor_flow_by_stock(
        self,
        code: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[InvestorFlowPoint]:
        raise NotImplementedError

    @abstractmethod
    def get_market_investor_flow_intraday(
        self,
        include_institution_breakdown: bool = False,
    ) -> list[MarketInvestorFlowPoint]:
        raise NotImplementedError

    @abstractmethod
    def get_foreign_flow_rankings(
        self,
        market: str = "all",
        consecutive_days: int = 3,
        direction: str = "buy",
    ) -> list[ForeignFlowRanking]:
        raise NotImplementedError

    @abstractmethod
    def get_top_movers(
        self,
        market: str = "all",
        direction: str = "up",
        date: str | None = None,
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[TopMover]:
        raise NotImplementedError

    @abstractmethod
    def list_stock_news(self, code: str, date: str | None = None) -> list[MarketNewsItem]:
        raise NotImplementedError

    @abstractmethod
    def list_market_flow_news(
        self,
        date: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> list[MarketNewsItem]:
        raise NotImplementedError

    @abstractmethod
    def get_news_content(self, news_type: str, date: str, article_id: str) -> NewsContent:
        raise NotImplementedError

    @abstractmethod
    def get_volume_surge(
        self,
        market: str = "all",
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        raise NotImplementedError

    @abstractmethod
    def get_new_highs_lows(
        self,
        market: str = "all",
        mode: str = "new_high",
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        raise NotImplementedError

    @abstractmethod
    def get_limit_hits(
        self,
        market: str = "all",
        mode: str = "upper",
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        raise NotImplementedError

    @abstractmethod
    def get_order_book(self, code: str) -> OrderBook:
        raise NotImplementedError

    @abstractmethod
    def get_gold_order_book(self, code: str) -> OrderBook:
        raise NotImplementedError

    def get_cash_order_book_snapshot(self, code: str) -> OrderBook:
        raise NotImplementedError

    @abstractmethod
    def get_balance(self, account_no: str) -> list[BalanceItem]:
        raise NotImplementedError

    @abstractmethod
    def get_gold_balance(self, account_no: str) -> list[GoldBalanceItem]:
        raise NotImplementedError

    @abstractmethod
    def get_executions(self, account_no: str) -> list[Execution]:
        raise NotImplementedError

    @abstractmethod
    def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
        raise NotImplementedError

    @abstractmethod
    def get_trade_history(
        self,
        account_no: str,
        code: str | None,
        start_date: str,
        end_date: str | None = None,
    ) -> list[TradeHistoryItem]:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def place_order(self, request: OrderRequest) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def modify_order(self, request: OrderRequest) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, request: OrderRequest) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def place_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def modify_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def cancel_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        raise NotImplementedError
