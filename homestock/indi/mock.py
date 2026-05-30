from __future__ import annotations

import platform
from datetime import datetime
from typing import Any
from collections.abc import Callable

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


class MockIndiClient(IndiClient):
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

    def __init__(self) -> None:
        self._subscriptions: set[str] = set()
        self._gold_subscriptions: set[str] = set()
        self._gold_subscription_counts: dict[str, int] = {}
        self._disclosure_feed_active = False
        self._news_feed_active = False
        self._kospi200_codes = {"005930", "000660"}
        self._rt_listeners: list[Callable[[dict[str, Any]], None]] = []
        self._gold_rt_listeners: list[Callable[[dict[str, Any]], None]] = []

    def health_check(self, live_orders_allowed: bool) -> HealthStatus:
        return HealthStatus(
            ok=True,
            backend="mock",
            python_architecture=platform.architecture()[0],
            ocx_ready=False,
            login_ready=True,
            live_orders_allowed=live_orders_allowed,
            message="mock backend ready",
            indi_process_running=False,
            indi_process_restarted=False,
            indi_process_message="mock backend does not monitor GiExpertMain.exe",
            rt_news_registered=self._news_feed_active,
            rt_disclosure_registered=self._disclosure_feed_active,
            gold_rt_control_ready=False,
            gold_rt_subscription_count=sum(self._gold_subscription_counts.values()),
            gold_rt_subscriptions=dict(self._gold_subscription_counts),
        )

    def list_stocks(self) -> list[Stock]:
        return [
            Stock(code="005930", name="Samsung Electronics", market="KOSPI"),
            Stock(code="000660", name="SK hynix", market="KOSPI"),
            Stock(code="035420", name="NAVER", market="KOSPI"),
        ]

    def list_gold_products(self) -> list[GoldProduct]:
        return list(self._GOLD_PRODUCTS.values())

    def get_daily_prices(self, code: str, start_date: str | None, end_date: str | None) -> list[DailyPrice]:
        self._require_code(code)
        return [
            DailyPrice(date="2026-04-20", open=70000, high=71000, low=69500, close=70800, volume=12000000),
            DailyPrice(date="2026-04-21", open=70800, high=72000, low=70500, close=71800, volume=11800000),
            DailyPrice(date="2026-04-22", open=71800, high=72400, low=71200, close=71600, volume=11100000),
        ]

    def get_intraday_prices(self, code: str, date: str, interval_minutes: int = 5) -> list[IntradayPrice]:
        self._require_code(code)
        normalized_date = date.replace("-", "")
        if len(normalized_date) != 8 or not normalized_date.isdigit():
            raise ValueError("date must be YYYYMMDD or YYYY-MM-DD")
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be greater than zero")
        return [
            IntradayPrice(date=normalized_date, time="090500", open=70800, high=71300, low=70700, close=71200, volume=150000),
            IntradayPrice(date=normalized_date, time="091000", open=71200, high=71600, low=71100, close=71500, volume=180000),
            IntradayPrice(date=normalized_date, time="091500", open=71500, high=71700, low=71300, close=71600, volume=160000),
        ]

    def get_gold_daily_prices(
        self,
        code: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[GoldDailyPrice]:
        normalized_code = self.normalize_gold_code(code)
        del normalized_code, start_date, end_date
        return [
            GoldDailyPrice(date="20260420", open=151200, high=152000, low=150800, close=151700, volume=3200, turnover=486000000),
            GoldDailyPrice(date="20260421", open=151700, high=153100, low=151300, close=152800, volume=4100, turnover=626480000),
            GoldDailyPrice(date="20260422", open=152800, high=153500, low=152100, close=153000, volume=3900, turnover=596700000),
        ]

    def get_gold_intraday_prices(
        self,
        code: str,
        date: str,
        interval_minutes: int = 5,
    ) -> list[GoldIntradayPrice]:
        normalized_code = self.normalize_gold_code(code)
        del normalized_code
        normalized_date = date.replace("-", "")
        if len(normalized_date) != 8 or not normalized_date.isdigit():
            raise ValueError("date must be YYYYMMDD or YYYY-MM-DD")
        if interval_minutes <= 0:
            raise ValueError("interval_minutes must be greater than zero")
        return [
            GoldIntradayPrice(date=normalized_date, time="090500", open=152800, high=153000, low=152700, close=153000, volume=120, turnover=18360000),
            GoldIntradayPrice(date=normalized_date, time="091000", open=153000, high=153200, low=152900, close=153100, volume=95, turnover=14544500),
        ]

    def get_market_index_prices(
        self,
        start_date: str,
        end_date: str,
    ) -> dict[str, list[MarketIndexPricePoint]]:
        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required")
        normalized_start = start_date.replace("-", "")
        normalized_end = end_date.replace("-", "")
        if len(normalized_start) != 8 or not normalized_start.isdigit():
            raise ValueError("start_date must be YYYYMMDD or YYYY-MM-DD")
        if len(normalized_end) != 8 or not normalized_end.isdigit():
            raise ValueError("end_date must be YYYYMMDD or YYYY-MM-DD")
        if normalized_start > normalized_end:
            raise ValueError("start_date must be on or before end_date")
        items = {
            "kospi200": [
                MarketIndexPricePoint(date="20260421", open=352.95, high=354.10, low=351.88, close=353.72),
                MarketIndexPricePoint(date="20260422", open=354.00, high=355.20, low=352.80, close=354.81),
            ],
            "sp500": [
                MarketIndexPricePoint(date="20260421", open=5256.00, high=5271.33, low=5248.72, close=5268.14),
                MarketIndexPricePoint(date="20260422", open=5269.00, high=5280.10, low=5258.30, close=5274.66),
                MarketIndexPricePoint(date="20260423", open=5271.00, high=5285.00, low=5265.00, close=5280.01),
            ],
            "nasdaq": [
                MarketIndexPricePoint(date="20260421", open=18296.00, high=18388.22, low=18240.10, close=18340.77),
                MarketIndexPricePoint(date="20260422", open=18341.00, high=18410.55, low=18300.44, close=18395.12),
                MarketIndexPricePoint(date="20260423", open=18396.00, high=18450.00, low=18370.00, close=18430.55),
            ],
            "usdkrw": [
                MarketIndexPricePoint(date="20260421", open=1384.20, high=1388.10, low=1381.30, close=1386.50),
                MarketIndexPricePoint(date="20260422", open=1386.80, high=1391.20, low=1384.50, close=1389.70),
                MarketIndexPricePoint(date="20260423", open=1389.40, high=1393.00, low=1387.90, close=1391.10),
            ],
        }
        return {
            index_id: sorted(
                [item for item in points if normalized_start <= item.date <= normalized_end],
                key=lambda item: item.date,
                reverse=True,
            )
            for index_id, points in items.items()
        }

    def get_sector_index_prices(
        self,
        sector_code: str,
        start_date: str,
        end_date: str,
        interval: str = "D",
    ) -> list[MarketIndexPricePoint]:
        if interval.upper() != "D":
            raise ValueError("mock backend currently supports daily sector index prices only")
        indexes = self.get_market_index_prices(start_date, end_date)
        return [
            MarketIndexPricePoint(
                date=item.date,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
            )
            for item in indexes["kospi200"]
        ]

    def get_stock_sector_profile(self, code: str) -> dict[str, Any]:
        self._require_code(code)
        profiles = {
            "005930": {
                "code": "005930",
                "sector_code": "2155",
                "sector_name": "코스피200 정보기술",
                "source": "mock",
            },
            "000660": {
                "code": "000660",
                "sector_code": "2155",
                "sector_name": "코스피200 정보기술",
                "source": "mock",
            },
        }
        return profiles.get(
            code,
            {
                "code": code,
                "sector_code": "",
                "sector_name": "",
                "source": "unavailable",
            },
        )

    def subscribe_realtime_price(self, code: str) -> dict[str, object]:
        self._require_code(code)
        already_subscribed = code in self._subscriptions
        self._subscriptions.add(code)
        return {
            "code": code,
            "subscribed": True,
            "rt_type": "UC",
            "already_subscribed": already_subscribed,
            "message": "mock realtime price subscription registered",
        }

    def unsubscribe_realtime_price(self, code: str) -> dict[str, object]:
        self._require_code(code)
        was_subscribed = code in self._subscriptions
        self._subscriptions.discard(code)
        return {
            "code": code,
            "subscribed": False,
            "rt_type": "UC",
            "was_subscribed": was_subscribed,
            "remaining_subscriptions": 0,
            "message": "mock realtime price subscription removed" if was_subscribed else "mock realtime price subscription was not registered",
        }

    def subscribe_gold_realtime_price(self, code: str) -> dict[str, object]:
        normalized_code = self.normalize_gold_code(code)
        previous_count = self._gold_subscription_counts.get(normalized_code, 0)
        already_subscribed = previous_count > 0
        self._gold_subscription_counts[normalized_code] = previous_count + 1
        self._gold_subscriptions.add(normalized_code)
        return {
            "code": normalized_code,
            "subscribed": True,
            "rt_type": "XC",
            "already_subscribed": already_subscribed,
            "message": "mock gold realtime price subscription registered",
        }

    def unsubscribe_gold_realtime_price(self, code: str) -> dict[str, object]:
        normalized_code = self.normalize_gold_code(code)
        previous_count = self._gold_subscription_counts.get(normalized_code, 0)
        was_subscribed = previous_count > 0
        remaining_count = max(previous_count - 1, 0)
        if remaining_count > 0:
            self._gold_subscription_counts[normalized_code] = remaining_count
        else:
            self._gold_subscription_counts.pop(normalized_code, None)
            self._gold_subscriptions.discard(normalized_code)
        return {
            "code": normalized_code,
            "subscribed": False,
            "rt_type": "XC",
            "was_subscribed": was_subscribed,
            "remaining_subscriptions": remaining_count,
            "message": "mock gold realtime price subscription removed" if was_subscribed else "mock gold realtime price subscription was not registered",
        }

    def subscribe_disclosure_feed(self, code: str) -> dict[str, object]:
        normalized_code = self.normalize_stock_code(code)
        already_subscribed = self._disclosure_feed_active
        rt_disclosure_registered_now = not already_subscribed
        self._disclosure_feed_active = True
        return {
            "code": normalized_code,
            "subscribed": True,
            "rt_type": "N2",
            "already_subscribed": already_subscribed,
            "already_indi_registered": already_subscribed,
            "rt_disclosure_registered_now": rt_disclosure_registered_now,
            "message": "mock disclosure subscription registered",
        }

    def unsubscribe_disclosure_feed(self, code: str) -> dict[str, object]:
        normalized_code = self.normalize_stock_code(code)
        was_subscribed = self._disclosure_feed_active
        self._disclosure_feed_active = False
        return {
            "code": normalized_code,
            "subscribed": False,
            "rt_type": "N2",
            "was_subscribed": was_subscribed,
            "message": "mock disclosure subscription removed" if was_subscribed else "mock disclosure subscription was not registered",
        }

    def subscribe_news_feed(self, code: str | None = None) -> dict[str, object]:
        normalized_code = self.normalize_stock_code(code) if code else "*"
        already_subscribed = self._news_feed_active
        rt_news_registered_now = not already_subscribed
        self._news_feed_active = True
        return {
            "code": None if normalized_code == "*" else normalized_code,
            "subscribed": True,
            "rt_type": "N0",
            "already_subscribed": already_subscribed,
            "already_indi_registered": already_subscribed,
            "rt_news_registered_now": rt_news_registered_now,
            "message": "mock news subscription registered",
        }

    def unsubscribe_news_feed(self, code: str | None = None) -> dict[str, object]:
        normalized_code = self.normalize_stock_code(code) if code else "*"
        was_subscribed = self._news_feed_active
        self._news_feed_active = False
        return {
            "code": None if normalized_code == "*" else normalized_code,
            "subscribed": False,
            "rt_type": "N0",
            "was_subscribed": was_subscribed,
            "message": "mock news subscription removed" if was_subscribed else "mock news subscription was not registered",
        }

    def register_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        if listener not in self._rt_listeners:
            self._rt_listeners.append(listener)

    def unregister_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        self._rt_listeners = [item for item in self._rt_listeners if item is not listener]

    def register_gold_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        if listener not in self._gold_rt_listeners:
            self._gold_rt_listeners.append(listener)

    def unregister_gold_rt_listener(self, listener: Callable[[dict[str, Any]], None]) -> None:
        self._gold_rt_listeners = [item for item in self._gold_rt_listeners if item is not listener]

    def normalize_stock_code(self, code: str | None) -> str:
        if code is None:
            return ""
        cleaned = code.strip().upper()
        if cleaned.startswith("A") and len(cleaned) == 7:
            cleaned = cleaned[1:]
        self._require_code(cleaned)
        return cleaned

    def normalize_gold_code(self, code: str | None) -> str:
        cleaned = (code or "M04020000").strip().upper()
        if cleaned not in self._GOLD_PRODUCTS:
            raise ValueError("gold code must be one of M04020000 or M04020100")
        return cleaned

    def get_last_rt_error_details(self) -> dict[str, Any] | None:
        return None

    def emit_rt_event(self, event: dict[str, Any]) -> None:
        for listener in list(self._rt_listeners):
            listener(dict(event))

    def emit_gold_rt_event(self, event: dict[str, Any]) -> None:
        for listener in list(self._gold_rt_listeners):
            listener(dict(event))

    def get_accounts(self) -> list[Account]:
        return [
            Account(
                account_no="12345678901",
                name="Mock account",
                product_code="01",
                product_name="종합계좌",
            )
        ]

    def get_account_summary(self, account_no: str) -> AccountSummary:
        self._require_account(account_no)
        return AccountSummary(
            account_no=account_no,
            total_deposit=12500000,
            withdrawable_amount=8200000,
            orderable_amount=9100000,
            total_purchase_amount=700000,
            total_evaluation_amount=716000,
            total_profit_loss=16000,
            total_return_rate=2.29,
            estimated_total_deposit=12550000,
            net_asset_value=13216000,
            total_asset_value=13216000,
            stock_asset_value=716000,
        )

    def get_gold_account_summary(self, account_no: str) -> GoldAccountSummary:
        self._require_account(account_no)
        return GoldAccountSummary(
            account_no=account_no,
            total_deposit=3_000_000,
            orderable_amount=2_100_000,
            withdrawable_amount=1_900_000,
            total_purchase_amount=1_528_000,
            total_evaluation_amount=1_530_000,
            total_profit_loss=2_000,
            total_asset_value=4_530_000,
            total_return_rate=0.13,
            total_margin=0,
            settlement_buy_amount1=0,
            settlement_sell_amount1=0,
            buy_settlement_amount=0,
            sell_settlement_amount=0,
            estimated_total_deposit=3_002_000,
        )

    def get_gold_account_balance(self, account_no: str) -> GoldAccountBalance:
        balances = self.get_gold_balance(account_no)
        if len(balances) > 1:
            raise RuntimeError(f"SABA835Q1 returned multiple gold balance rows: {len(balances)}")
        return GoldAccountBalance(
            account_no=account_no,
            summary=self.get_gold_account_summary(account_no),
            balance=balances[0] if balances else None,
        )

    def get_fundamentals(
        self,
        code: str,
        consolidated: bool = True,
        quarterly: bool = True,
    ) -> list[FundamentalPoint]:
        self._require_code(code)
        period_type = "quarterly" if quarterly else "annual"
        return [
            FundamentalPoint(
                code=code,
                date="2025Q4" if quarterly else "2025",
                period_type=period_type,
                revenue=300000000,
                operating_income=40000000,
                net_income=32000000,
                operating_margin=13.33,
                net_margin=10.67,
                roe=12.4,
                eps=4200.0,
                eps_growth=18.5,
                bps=52000.0,
                per=16.8,
                per_ttm=15.9,
                pbr=1.35,
                dps=1444.0,
                dividend_yield=2.1,
                ev_ebitda=8.2,
                peg=0.91,
            )
        ]

    def get_quote_snapshot(self, code: str) -> QuoteSnapshot:
        self._require_code(code)
        return QuoteSnapshot(
            code=code,
            current_price=71600,
            previous_close=70800,
            change_amount=800,
            change_percent=1.13,
            year_high=86000,
            year_high_date="2025-07-11",
            year_low=64000,
            year_low_date="2025-01-15",
            upper_limit=92000,
            lower_limit=49600,
            market_cap=427000000000000,
            foreign_ownership_ratio=51.2,
            eps=4200.0,
            per=16.8,
        )

    def get_gold_quote_snapshot(self, code: str) -> GoldQuoteSnapshot:
        normalized_code = self.normalize_gold_code(code)
        product = self._GOLD_PRODUCTS[normalized_code]
        return GoldQuoteSnapshot(
            code=normalized_code,
            standard_code=product.standard_code,
            trade_time="153000",
            current_price=153000.0,
            change_type="2",
            change_amount=1200.0,
            change_percent=0.79,
            cumulative_volume=3900.0,
            turnover=596700000.0,
            fill_size=12.0,
            open=152800.0,
            high=153500.0,
            low=152100.0,
            open_time="090000",
            high_time="143000",
            low_time="091500",
            block_volume=0.0,
            trade_side="2",
            best_ask=153100.0,
            best_bid=153000.0,
        )

    def get_investor_flow_by_stock(
        self,
        code: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[InvestorFlowPoint]:
        del start_date, end_date
        self._require_code(code)
        return [
            InvestorFlowPoint(
                code=code,
                date="2026-04-20",
                close=70800,
                volume=12000000,
                retail_net=-150000,
                retail_cumulative_net=-150000,
                foreign_net=90000,
                foreign_cumulative_net=90000,
                institution_net=60000,
                institution_cumulative_net=60000,
            ),
            InvestorFlowPoint(
                code=code,
                date="2026-04-21",
                close=71800,
                volume=11800000,
                retail_net=-50000,
                retail_cumulative_net=-200000,
                foreign_net=20000,
                foreign_cumulative_net=110000,
                institution_net=30000,
                institution_cumulative_net=90000,
            ),
        ]

    def get_market_investor_flow_intraday(
        self,
        include_institution_breakdown: bool = False,
    ) -> list[MarketInvestorFlowPoint]:
        items = [
            self._market_investor_flow_point(
                "090500",
                retail=(65000, 185000, -120000),
                foreign=(136000, 42000, 94000),
                institution=(49000, 23000, 26000),
                include_institution_breakdown=include_institution_breakdown,
            ),
            self._market_investor_flow_point(
                "091000",
                retail=(105000, 290000, -185000),
                foreign=(198000, 70000, 128000),
                institution=(92000, 35000, 57000),
                include_institution_breakdown=include_institution_breakdown,
            ),
        ]
        return items

    @staticmethod
    def _market_investor_flow_point(
        time: str,
        retail: tuple[int, int, int],
        foreign: tuple[int, int, int],
        institution: tuple[int, int, int],
        include_institution_breakdown: bool,
    ) -> MarketInvestorFlowPoint:
        return MarketInvestorFlowPoint(
            time=time,
            retail=MockIndiClient._flow_group(*retail),
            foreign=MockIndiClient._flow_group(*foreign),
            institution=MockIndiClient._flow_group(*institution),
            institution_breakdown=MockIndiClient._mock_institution_breakdown()
            if include_institution_breakdown
            else None,
        )

    @staticmethod
    def _mock_institution_breakdown() -> dict[str, dict[str, int]]:
        return {
            "securities": MockIndiClient._flow_group(47000, 17000, 30000),
            "investment_trust": MockIndiClient._flow_group(16000, 4000, 12000),
            "bank": MockIndiClient._flow_group(2000, 1000, 1000),
            "merchant_bank": MockIndiClient._flow_group(0, 0, 0),
            "insurance": MockIndiClient._flow_group(7000, 2000, 5000),
            "pension_fund": MockIndiClient._flow_group(21000, 1000, 20000),
            "other_corporation": MockIndiClient._flow_group(14000, 5000, 9000),
            "other_foreign": MockIndiClient._flow_group(1100, 1500, -400),
            "futures_dealer": MockIndiClient._flow_group(0, 0, 0),
            "private_fund": MockIndiClient._flow_group(12000, 5000, 7000),
        }

    @staticmethod
    def _flow_group(buy: int, sell: int, net: int) -> dict[str, int]:
        return {"buy": buy, "sell": sell, "net": net}

    def get_foreign_flow_rankings(
        self,
        market: str = "all",
        consecutive_days: int = 3,
        direction: str = "buy",
    ) -> list[ForeignFlowRanking]:
        del consecutive_days, direction
        return [
            ForeignFlowRanking(
                code="005930",
                market=market,
                name="Samsung Electronics",
                current_price=71600,
                change_percent=1.13,
                foreign_cumulative_volume=1234567,
                foreign_ownership_ratio=51.2,
                institution_cumulative_volume=345678,
                listed_shares=5969782550,
            )
        ]

    def get_top_movers(
        self,
        market: str = "all",
        direction: str = "up",
        date: str | None = None,
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[TopMover]:
        del market, direction, date
        items = [
            TopMover(
                rank=1,
                code="005930",
                name="Samsung Electronics",
                current_price=71600,
                change_percent=1.13,
                volume=11100000,
                trade_strength=128.4,
            )
        ]
        if kospi200_only:
            items = [item for item in items if item.code in self._kospi200_codes]
        return self._limit_items(items, limit)

    def list_stock_news(self, code: str, date: str | None = None) -> list[MarketNewsItem]:
        self._require_code(code)
        return [
            MarketNewsItem(
                date=date or "20260423",
                time="153000",
                title="반도체 업황 개선 기대감 확대",
                news_type="F",
                news_type_label="시황",
                code=code,
                article_id="356872",
            )
        ]

    def list_market_flow_news(
        self,
        date: str | None = None,
        from_time: str | None = None,
        to_time: str | None = None,
    ) -> list[MarketNewsItem]:
        normalized_date = date or "20260423"
        start = self._normalize_news_time(from_time)
        end = self._normalize_news_time(to_time)
        if start is not None and end is not None and start > end:
            raise ValueError("from_time must be on or before to_time")
        items = [
            MarketNewsItem(
                date=normalized_date,
                time="090500",
                title="장전 주요 뉴스 요약",
                news_type="F",
                news_type_label="시황",
                code="",
                article_id="356870",
            ),
            MarketNewsItem(
                date=normalized_date,
                time="153000",
                title="반도체 업황 개선 기대감 확대",
                news_type="F",
                news_type_label="시황",
                code="005930",
                article_id="356872",
            ),
            MarketNewsItem(
                date=normalized_date,
                time="154500",
                title="외국인 순매수 전환",
                news_type="F",
                news_type_label="시황",
                code="005930",
                article_id="356873",
            ),
        ]
        filtered = [
            item
            for item in items
            if (start is None or item.time >= start)
            and (end is None or item.time <= end)
        ]
        return filtered

    def get_news_content(self, news_type: str, date: str, article_id: str) -> NewsContent:
        return NewsContent(
            news_type=news_type,
            news_type_label="시황" if news_type == "F" else f"unknown({news_type})",
            date=date,
            time="153000",
            extracted_codes=["005930", "000660"],
            content="반도체 업황 개선 기대감이 확대되며 관련 대형주가 강세를 보였다.",
        )

    @staticmethod
    def _normalize_news_time(value: str | None) -> str | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        digits = "".join(ch for ch in raw if ch.isdigit())
        if not digits:
            raise ValueError("news time must contain digits")
        if len(digits) <= 2:
            return digits.zfill(2) + "0000"
        if len(digits) <= 4:
            return digits.zfill(4) + "00"
        if len(digits) == 5:
            return digits.zfill(6)
        return digits[:6]

    def get_volume_surge(
        self,
        market: str = "all",
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        del market
        items = [
            MarketScannerItem(
                code="000660",
                name="SK hynix",
                current_price=1225000,
                change_percent=2.4,
                volume=33874721,
                metric_value=185.0,
                metric_label="volume_surge_rate",
            )
        ]
        if kospi200_only:
            items = [item for item in items if item.code in self._kospi200_codes]
        return self._limit_items(items, limit)

    def get_new_highs_lows(
        self,
        market: str = "all",
        mode: str = "new_high",
        limit: int | None = None,
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        del market
        label = "new_high_price" if mode in {"new_high", "52w_high"} else "new_low_price"
        items = [
            MarketScannerItem(
                code="005930",
                name="Samsung Electronics",
                current_price=71600,
                change_percent=1.13,
                volume=11100000,
                metric_value=86000.0 if mode == "new_high" else 64000.0,
                metric_label=label,
            )
        ]
        if kospi200_only:
            items = [item for item in items if item.code in self._kospi200_codes]
        return self._limit_items(items, limit)

    @staticmethod
    def _limit_items(items: list[Any], limit: int | None) -> list[Any]:
        if limit is None:
            return items
        return items[: max(limit, 0)]

    def get_limit_hits(
        self,
        market: str = "all",
        mode: str = "upper",
        kospi200_only: bool = False,
    ) -> list[MarketScannerItem]:
        del market
        items = [
            MarketScannerItem(
                code="381170",
                name="TIGER 미국테크TOP10 INDXX",
                current_price=32020,
                change_percent=29.9 if mode == "upper" else -29.9,
                volume=2500000,
                metric_value=3.0,
                metric_label="consecutive_days",
            )
        ]
        if kospi200_only:
            items = [item for item in items if item.code in self._kospi200_codes]
        return items

    def get_order_book(self, code: str) -> OrderBook:
        self._require_code(code)
        return OrderBook(
            code=code,
            received_at="153001",
            market_phase="regular",
            levels=[
                OrderBookLevel(ask_price=71600, ask_size=1200, bid_price=71500, bid_size=1800),
                OrderBookLevel(ask_price=71700, ask_size=900, bid_price=71400, bid_size=1500),
                OrderBookLevel(ask_price=71800, ask_size=800, bid_price=71300, bid_size=1400),
                OrderBookLevel(ask_price=71900, ask_size=700, bid_price=71200, bid_size=1300),
                OrderBookLevel(ask_price=72000, ask_size=600, bid_price=71100, bid_size=1200),
            ],
        )

    def get_gold_order_book(self, code: str) -> OrderBook:
        normalized_code = self.normalize_gold_code(code)
        return OrderBook(
            code=normalized_code,
            received_at="153001",
            market_phase="regular",
            levels=[
                OrderBookLevel(ask_price=153100, ask_size=12, bid_price=153000, bid_size=10),
                OrderBookLevel(ask_price=153200, ask_size=8, bid_price=152900, bid_size=7),
                OrderBookLevel(ask_price=153300, ask_size=6, bid_price=152800, bid_size=5),
            ],
            source="XH",
        )

    def get_cash_order_book_snapshot(self, code: str) -> OrderBook:
        order_book = self.get_order_book(code)
        return OrderBook(
            code=order_book.code,
            received_at=order_book.received_at,
            market_phase=order_book.market_phase,
            levels=order_book.levels,
            source="SH",
            partial=order_book.partial,
            available=order_book.available,
            message=order_book.message,
        )

    def get_balance(self, account_no: str) -> list[BalanceItem]:
        self._require_account(account_no)
        return [
            BalanceItem(
                account_no=account_no,
                code="005930",
                name="Samsung Electronics",
                quantity=10,
                avg_price=70000,
                current_price=71600,
            )
        ]

    def get_gold_balance(self, account_no: str) -> list[GoldBalanceItem]:
        self._require_account(account_no)
        return [
            GoldBalanceItem(
                account_no=account_no,
                code="M04020000",
                name="금 99.99_1kg",
                credit_type="",
                quantity=10,
                sellable_quantity=10,
                restricted_quantity=0,
                deliverable_quantity=10,
                avg_price=152800,
                current_price=153000,
                price_change=200,
                purchase_amount=1_528_000,
                credit_amount=0,
                valuation_amount=1_530_000,
                profit_loss=2_000,
                return_rate=0.13,
                trading_unit=1,
                security_type="70",
            )
        ]

    def get_executions(self, account_no: str) -> list[Execution]:
        self._require_account(account_no)
        return [
            Execution(
                order_id="MOCK-ORDER-1",
                code="005930",
                side="buy",
                quantity=1,
                price=70000,
                status="filled",
                executed_at="2026-04-22T09:10:00+09:00",
            )
        ]

    def get_open_orders(self, account_no: str, code: str | None = None) -> list[OpenOrder]:
        self._require_account(account_no)
        items = [
            OpenOrder(
                order_id="MOCK-OPEN-1",
                code="005930",
                name="Samsung Electronics",
                side="buy",
                order_type="limit",
                price=70000,
                quantity=100,
                filled_quantity=30,
                unfilled_quantity=70,
                order_time="20260424091000",
                status="partial",
                raw_order_id="MOCK-OPEN-1",
                original_raw_order_id="MOCK-ORIGINAL-1",
                order_method_code="0",
                order_method_name="SOR",
                order_exchange_code="2",
                order_exchange_name="NXT",
                sor_order_id="MOCK-SOR-1",
                sor_original_order_id="MOCK-SOR-ORIGINAL-1",
                credit_trade_type="00",
            ),
            OpenOrder(
                order_id="MOCK-OPEN-2",
                code="000660",
                name="SK hynix",
                side="sell",
                order_type="market",
                price=0,
                quantity=5,
                filled_quantity=0,
                unfilled_quantity=5,
                order_time="20260424091500",
                status="pending",
                raw_order_id="MOCK-OPEN-2",
                original_raw_order_id="",
                order_method_code="1",
                order_method_name="KRX",
                order_exchange_code="1",
                order_exchange_name="KRX",
                sor_order_id="",
                sor_original_order_id="",
                credit_trade_type="00",
            ),
        ]
        if code is None:
            return items
        normalized_code = code.strip().upper()
        self._require_code(normalized_code)
        return [item for item in items if item.code == normalized_code]

    def get_trade_history(
        self,
        account_no: str,
        code: str | None,
        start_date: str,
        end_date: str | None = None,
    ) -> list[TradeHistoryItem]:
        del start_date, end_date
        self._require_account(account_no)
        normalized_code = code or "005930"
        self._require_code(normalized_code)
        return [
            TradeHistoryItem(
                date="2025-07-12",
                trade_type="매수",
                code=normalized_code,
                raw_code=f"A{normalized_code}",
                name="Mock stock",
                quantity=10,
                price=70000,
                fee=150,
                tax=0,
                trade_amount=700000,
                credit_amount=0,
                unpaid_repayment=0,
                overdue_fee=0,
                credit_interest=0,
                change_amount=-700150,
                final_amount=-700150,
                order_channel="mock",
            ),
            TradeHistoryItem(
                date="2025-08-01",
                trade_type="매도",
                code=normalized_code,
                raw_code=f"A{normalized_code}",
                name="Mock stock",
                quantity=4,
                price=72500,
                fee=120,
                tax=350,
                trade_amount=290000,
                credit_amount=0,
                unpaid_repayment=0,
                overdue_fee=0,
                credit_interest=0,
                change_amount=289530,
                final_amount=289530,
                order_channel="mock",
            ),
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
    ) -> list[AccountLedgerItem]:
        del start_date, end_date, market, include_mmw, include_rp_details, product_code, admin
        self._require_account(account_no)
        if code is not None:
            self._require_code(code)

        items = [
            AccountLedgerItem(
                date="2025-04-15",
                transaction_type="배당금",
                summary="현금배당",
                code="005930",
                raw_code="A005930",
                name="Mock stock",
                quantity=10,
                price=1444,
                fee=0,
                tax=2200,
                trade_amount=14440,
                credit_amount=0,
                unpaid_repayment=0,
                credit_interest=0,
                overdue_fee=0,
                substitute_account="",
                change_amount=12240,
                final_amount=12240,
                loan_date="",
                maturity_date="",
                product_no="01",
                bond_type="",
                transaction_id="LEDGER-1",
                taxable_amount=14440,
                deposit_usage_fee=0,
                order_user_id="mock-user",
                requester_name="Mock User",
                financial_institution_name="Mock Securities",
            ),
            AccountLedgerItem(
                date="2025-04-16",
                transaction_type="입금",
                summary="계좌입금",
                code="",
                raw_code="",
                name="",
                quantity=0,
                price=0,
                fee=0,
                tax=0,
                trade_amount=500000,
                credit_amount=0,
                unpaid_repayment=0,
                credit_interest=0,
                overdue_fee=0,
                substitute_account="",
                change_amount=500000,
                final_amount=500000,
                loan_date="",
                maturity_date="",
                product_no="01",
                bond_type="",
                transaction_id="LEDGER-2",
                taxable_amount=0,
                deposit_usage_fee=0,
                order_user_id="mock-user",
                requester_name="Mock User",
                financial_institution_name="Mock Bank",
            ),
        ]
        normalized_code = code.strip().upper() if code else None
        if normalized_code is not None:
            items = [item for item in items if item.code == normalized_code]

        normalized_transaction_type = transaction_type.strip().upper()
        if normalized_transaction_type in {"DIVIDEND", "D"}:
            items = [item for item in items if item.transaction_type == "배당금"]
        elif normalized_transaction_type not in {"", "ALL", "0"}:
            items = []
        return items

    def place_order(self, request: OrderRequest) -> OrderResult:
        self._validate_order(request, require_original=False)
        return self._accepted("mock-place", request)

    def modify_order(self, request: OrderRequest) -> OrderResult:
        self._validate_order(request, require_original=True)
        return self._accepted("mock-modify", request)

    def cancel_order(self, request: OrderRequest) -> OrderResult:
        self._validate_order(request, require_original=True)
        return self._accepted("mock-cancel", request)

    def place_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        self._validate_gold_order(request, require_price=True, require_original=False)
        return self._accepted_gold("mock-gold-place", request)

    def modify_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        self._validate_gold_order(request, require_price=True, require_original=True)
        return self._accepted_gold("mock-gold-modify", request)

    def cancel_gold_order(self, request: GoldOrderRequest) -> OrderResult:
        self._validate_gold_order(request, require_price=False, require_original=True)
        return self._accepted_gold("mock-gold-cancel", request)

    def _accepted(self, action: str, request: OrderRequest) -> OrderResult:
        return OrderResult(
            accepted=True,
            live_order=False,
            order_id=f"{action}-{datetime.now().strftime('%H%M%S')}",
            message=f"{action} accepted by mock backend",
            raw={
                "account_no": request.account_no,
                "code": request.code,
                "side": request.side,
                "quantity": request.quantity,
                "price": request.price,
                "order_type": request.order_type,
                "original_order_id": request.original_order_id,
                "credit_trade_type": request.credit_trade_type,
                "order_method_code": request.order_method_code,
                "sor_original_order_id": request.sor_original_order_id,
            },
        )

    def _accepted_gold(self, action: str, request: GoldOrderRequest) -> OrderResult:
        return OrderResult(
            accepted=True,
            live_order=False,
            order_id=f"{action}-{datetime.now().strftime('%H%M%S')}",
            message=f"{action} accepted by mock backend",
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
        )

    @staticmethod
    def _require_code(code: str) -> None:
        if not code or not code.isdigit() or len(code) != 6:
            raise ValueError("code must be a 6 digit stock code")

    @staticmethod
    def _require_account(account_no: str) -> None:
        if not account_no:
            raise ValueError("account_no is required")

    def _validate_order(self, request: OrderRequest, require_original: bool) -> None:
        self._require_account(request.account_no)
        self._require_code(request.code)
        if request.quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        if request.order_type == "limit" and (request.price is None or request.price <= 0):
            raise ValueError("limit order requires a positive price")
        if require_original and not request.original_order_id:
            raise ValueError("original_order_id is required")

    def _validate_gold_order(
        self,
        request: GoldOrderRequest,
        *,
        require_price: bool,
        require_original: bool,
    ) -> None:
        self._require_account(request.account_no)
        self.normalize_gold_code(request.code)
        if request.side not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        if request.quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        if require_price and (request.price is None or request.price <= 0):
            raise ValueError("gold limit orders require a positive price")
        if require_original and not request.original_order_id:
            raise ValueError("original_order_id is required")
