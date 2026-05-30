from __future__ import annotations

import functools
import inspect
import os
import platform
import sys
import time
import traceback
from typing import Any

from homestock.backend import create_indi_client
from homestock.config import Settings
from homestock.order_guard import OrderGuard
from homestock.scripter import IsolateProcessScripter, Scripter, write_crash_log
from homestock.ops_log import LogSource, ops_log
from homestock.tools import HomestockTools


def _settings_summary(settings: Settings) -> str:
    return (
        f"backend={settings.backend}, "
        f"allow_live_orders={settings.allow_live_orders}, "
        f"use_threaded_real_client={settings.use_threaded_real_client}, "
        f"host={settings.host}, "
        f"port={settings.port}, "
        f"runtime_state_dir={settings.runtime_state_dir or '<default>'}, "
        f"holding_alert_config_path={settings.holding_alert_config_path or '<default>'}, "
        f"scripter_log_dir={settings.scripter_log_dir or '<default>'}, "
        f"scripter_log_retention_days={settings.scripter_log_retention_days}, "
        f"scripter_log_level={settings.scripter_log_level}"
    )


def _secret_presence(name: str) -> str:
    return "set" if os.getenv(name, "").strip() else "missing"


def _log_process_context() -> None:
    ops_log(LogSource.STARTUP_SERVER, f"cwd={os.getcwd()}")
    ops_log(LogSource.STARTUP_SERVER, f"pid={os.getpid()}")
    ops_log(LogSource.STARTUP_SERVER, f"python_executable={sys.executable}")
    ops_log(LogSource.STARTUP_SERVER, f"python_version={platform.python_version()}")
    ops_log(LogSource.STARTUP_SERVER, f"python_architecture={platform.architecture()[0]}")
    ops_log(LogSource.STARTUP_SERVER, f"platform={platform.platform()}")
    ops_log(LogSource.STARTUP_SERVER, f"SESSIONNAME={os.getenv('SESSIONNAME', '<unset>')}")
    ops_log(LogSource.STARTUP_SERVER, f"INDI_BACKEND={os.getenv('INDI_BACKEND', '<unset>')}")
    ops_log(LogSource.STARTUP_SERVER, f"ALLOW_LIVE_ORDERS={os.getenv('ALLOW_LIVE_ORDERS', '<unset>')}")
    ops_log(LogSource.STARTUP_SERVER,
        f"HOMESTOCK_USE_THREADED_REAL_CLIENT={os.getenv('HOMESTOCK_USE_THREADED_REAL_CLIENT', '<unset>')}",
    )
    ops_log(LogSource.STARTUP_SERVER, f"HOMESTOCK_HOST={os.getenv('HOMESTOCK_HOST', '<unset>')}")
    ops_log(LogSource.STARTUP_SERVER, f"HOMESTOCK_PORT={os.getenv('HOMESTOCK_PORT', '<unset>')}")
    ops_log(LogSource.STARTUP_SERVER,
        "HOMESTOCK_RUNTIME_STATE_DIR="
        f"{os.getenv('HOMESTOCK_RUNTIME_STATE_DIR', '<unset>')}",
    )
    ops_log(LogSource.STARTUP_SERVER,
        "HOMESTOCK_SCRIPTER_LOG_DIR="
        f"{os.getenv('HOMESTOCK_SCRIPTER_LOG_DIR', '<unset>')}",
    )
    ops_log(LogSource.STARTUP_SERVER,
        "HOMESTOCK_SCRIPTER_LOG_RETENTION_DAYS="
        f"{os.getenv('HOMESTOCK_SCRIPTER_LOG_RETENTION_DAYS', '<unset>')}",
    )
    ops_log(LogSource.STARTUP_SERVER,
        "HOMESTOCK_SCRIPTER_LOG_LEVEL="
        f"{os.getenv('HOMESTOCK_SCRIPTER_LOG_LEVEL', '<unset>')}",
    )
    ops_log(LogSource.STARTUP_SERVER, f"HOMESTOCK_ACCOUNT_PASSWORD={_secret_presence('HOMESTOCK_ACCOUNT_PASSWORD')}")


def create_tools(settings: Settings | None = None, scripter: Scripter | None = None) -> HomestockTools:
    resolved_settings = settings or Settings.from_env()
    owns_scripter = scripter is None
    resolved_scripter: Scripter | None = scripter
    if resolved_scripter is None:
        resolved_scripter = IsolateProcessScripter(
            log_dir=resolved_settings.scripter_log_dir or ".runtime/scripter",
            retention_days=resolved_settings.scripter_log_retention_days,
            log_level=resolved_settings.scripter_log_level,
        )
    try:
        try:
            resolved_scripter.start()
        except Exception as start_exc:
            ops_log(LogSource.SCRIPTER, f"Scripter start failed: {start_exc.__class__.__name__}: {start_exc}")
            write_crash_log(
                role="main",
                source="server.create_tools.scripter_start",
                message="Scripter start failed",
                exc=start_exc,
                log_dir=resolved_settings.scripter_log_dir or ".runtime/scripter",
                extra={"settings": _settings_summary(resolved_settings)},
            )
            raise
        ops_log(LogSource.STARTUP_MANAGE, f"settings resolved: {_settings_summary(resolved_settings)}")
        ops_log(LogSource.STARTUP_MANAGE, f"creating Indi client backend={resolved_settings.backend}")
        client = create_indi_client(resolved_settings)
        ops_log(LogSource.STARTUP_MANAGE, f"Indi client ready: {client.__class__.__name__}")
        ops_log(LogSource.STARTUP_MANAGE,
            f"creating OrderGuard allow_live_orders={resolved_settings.allow_live_orders}",
        )
        order_guard = OrderGuard(resolved_settings.allow_live_orders)
        ops_log(LogSource.STARTUP_MANAGE,
            f"creating HomestockTools runtime_state_dir={resolved_settings.runtime_state_dir or '<default>'}",
        )
        tools = HomestockTools(
            client=client,
            order_guard=order_guard,
            runtime_state_dir=resolved_settings.runtime_state_dir,
            holding_alert_config_path=resolved_settings.holding_alert_config_path,
            scripter=resolved_scripter,
            scripter_log_dir=resolved_settings.scripter_log_dir or ".runtime/scripter",
        )
    except Exception:
        close_client = locals().get("client", None)
        close = getattr(close_client, "close", None)
        if callable(close):
            ops_log(LogSource.STARTUP_MANAGE, "HomestockTools creation failed; closing Indi client")
            try:
                close()
            except Exception as close_exc:
                ops_log(LogSource.MANAGE,
                    f"Indi client close after HomestockTools creation failure failed: "
                    f"{close_exc.__class__.__name__}: {close_exc}",
                )
        if owns_scripter and resolved_scripter is not None:
            try:
                resolved_scripter.close()
            except Exception as close_exc:  # pragma: no cover - defensive startup cleanup
                ops_log(LogSource.MANAGE,
                    f"Scripter close after HomestockTools creation failure failed: "
                    f"{close_exc.__class__.__name__}: {close_exc}",
                )
        raise
    ops_log(LogSource.STARTUP_MANAGE, "HomestockTools ready")
    return tools


def _close_tools(tools: Any) -> None:
    close = getattr(tools, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception as exc:
        ops_log(LogSource.MANAGE, f"HomestockTools close failed: {exc.__class__.__name__}: {exc}")


def close_mcp_server(mcp: Any) -> None:
    tools = getattr(mcp, "_homestock_tools", None)
    if tools is not None:
        _close_tools(tools)


def create_mcp_server(settings: Settings | None = None) -> Any:
    resolved_settings = settings or Settings.from_env()
    tools = create_tools(resolved_settings)
    try:
        ops_log(LogSource.STARTUP_MCP, "importing FastMCP")
        try:
            from mcp.server.fastmcp import FastMCP
        except ImportError as exc:
            ops_log(LogSource.STARTUP_MCP, f"FastMCP import failed: {exc}")
            raise RuntimeError("The 'mcp' package is required to run the MCP server") from exc
        ops_log(LogSource.STARTUP_MCP, "FastMCP import ok")

        ops_log(LogSource.STARTUP_MCP, f"server settings resolved: {_settings_summary(resolved_settings)}")
        ops_log(LogSource.STARTUP_MCP, "constructing FastMCP app")
        mcp = FastMCP(
            "homestock",
            stateless_http=True,
            json_response=True,
            host=resolved_settings.host,
            port=resolved_settings.port,
        )
        ops_log(LogSource.STARTUP_MCP,
            f"FastMCP app ready name=homestock, transport=streamable-http, "
            f"host={resolved_settings.host}, port={resolved_settings.port}, path=/mcp",
        )
        ops_log(LogSource.STARTUP_MCP, "registering MCP tools")

        def _logged_mcp_tool(func: Any) -> Any:
            signature = inspect.signature(func)
            tool_name = getattr(func, "__name__", "<unknown>")

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                started_at = time.perf_counter()
                bound = signature.bind(*args, **kwargs)
                bound.apply_defaults()
                arguments = dict(bound.arguments)
                ops_log(LogSource.MCP_TOOL, f"call begin tool={tool_name}")
                ops_log(
                    LogSource.MCP_TOOL,
                    f"call args tool={tool_name}",
                    level="debug",
                    payload={"tool": tool_name, "args": arguments},
                )
                try:
                    result = func(*args, **kwargs)
                except Exception as exc:
                    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                    ops_log(
                        LogSource.MCP_TOOL,
                        f"call failed tool={tool_name} elapsed_ms={elapsed_ms} "
                        f"exception_type={exc.__class__.__name__}",
                        level="error",
                    )
                    ops_log(
                        LogSource.MCP_TOOL,
                        f"call error tool={tool_name}",
                        level="debug",
                        payload={
                            "tool": tool_name,
                            "args": arguments,
                            "exception_type": exc.__class__.__name__,
                            "error": str(exc),
                            "callstack": traceback.format_exc(),
                        },
                    )
                    raise
                elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                ops_log(
                    LogSource.MCP_TOOL,
                    f"call success tool={tool_name} elapsed_ms={elapsed_ms}",
                )
                ops_log(
                    LogSource.MCP_TOOL,
                    f"call result tool={tool_name}",
                    level="debug",
                    payload={"tool": tool_name, "result": result},
                )
                return result

            wrapper.__signature__ = signature
            return wrapper

        def _mcp_tool(func: Any) -> Any:
            return mcp.tool()(_logged_mcp_tool(func))

        @_mcp_tool
        def health_check() -> dict[str, Any]:
            """EN: Return gateway, backend, OCX, login, live-order, N0/N2 RT registration, gold RT, and gold runtime availability status. | KO: 게이트웨이, 백엔드, OCX, 로그인, 실주문 허용, N0/N2 RT 등록, gold RT, gold runtime 가용 상태를 반환한다."""
            return tools.health_check()

        @_mcp_tool
        def list_stocks() -> list[dict[str, Any]]:
            """EN: Return the available domestic stock master records. | KO: 조회 가능한 국내 주식 종목 마스터 목록을 반환한다."""
            return tools.list_stocks()

        @_mcp_tool
        def get_daily_prices(
            code: str,
            start_date: str | None = None,
            end_date: str | None = None,
        ) -> list[dict[str, Any]]:
            """EN:
    Return daily OHLCV prices for one domestic stock.

    Arguments:
    - `code`: 6-digit stock code. A leading `A` prefix is accepted when backend normalization supports it.
    - `start_date`: optional inclusive start date in `YYYYMMDD` or `YYYY-MM-DD`.
    - `end_date`: optional inclusive end date in `YYYYMMDD` or `YYYY-MM-DD`.

    Response:
    - A list of daily bars with `date`, `open`, `high`, `low`, `close`, and `volume`.
    - This is the raw price series to use for chart structure, support/resistance, moving averages not already returned by indicator tools, and custom pattern checks.
    - Empty list means the backend returned no rows for the requested stock/date range.

    Safety:
    - Read-only data lookup. It does not subscribe to realtime feeds, mutate alert state, send webhooks, or place orders.

    KO:
    한 국내 주식의 일봉 OHLCV 원본 데이터를 조회한다.
    차트 구조, 지지/저항, 별도 이동평균 계산, 패턴 후보 판단의 기본 입력으로 쓰기 위한 tool이다.
    순수 조회용이며 실시간 구독, 알림 상태 변경, webhook 발송, 주문 실행을 하지 않는다."""
            return tools.get_daily_prices(code, start_date, end_date)

        @_mcp_tool
        def get_intraday_prices(
            code: str,
            date: str,
            interval_minutes: int = 5,
        ) -> list[dict[str, Any]]:
            """EN:
    Return intraday OHLCV candle data for one domestic stock.

    Arguments:
    - `code`: 6-digit stock code. The leading `A` prefix is accepted by real backends when supported by code normalization.
    - `date`: trading date in `YYYYMMDD` or `YYYY-MM-DD`.
    - `interval_minutes`: candle interval in minutes. v1 holding-alert workflows use `5`; unsupported intervals may raise a backend validation error.

    Behavior:
    - This is a data lookup tool and does not start a holding-alert runner, send webhooks, or place orders.
    - Real backend data comes from the supported intraday chart TR path and is normalized into complete OHLCV rows.
    - Returned `time` is normalized to `HHMMSS`, so values like `090500` mean 09:05:00 KST.

    Response:
    - A list of candles with `date`, `time`, `open`, `high`, `low`, `close`, and `volume`.
    - Empty list means no candle rows were available for the requested date/code.

    KO:
    한 종목의 장중 분봉 OHLCV candle을 조회한다.
    `code`는 6자리 종목코드이며, `date`는 `YYYYMMDD` 또는 `YYYY-MM-DD`를 받는다.
    `interval_minutes`는 분 단위 candle 간격이고 v1 보유종목 알림은 5분봉을 기준으로 사용한다.
    이 tool은 순수 조회용이며 runner 등록, webhook 발송, 주문 실행을 하지 않는다.
    응답의 `time`은 `HHMMSS`로 정규화된다."""
            return tools.get_intraday_prices(code, date, interval_minutes)

        @_mcp_tool
        def get_market_index_prices(
            start_date: str,
            end_date: str,
        ) -> dict[str, list[dict[str, Any]]]:
            """EN:
    Return daily OHLC series for market indexes used by holding-alert context.

    Arguments:
    - `start_date`: inclusive start date in `YYYYMMDD` or `YYYY-MM-DD`.
    - `end_date`: inclusive end date in `YYYYMMDD` or `YYYY-MM-DD`; must be on or after `start_date`.

    Supported keys:
    - `kospi200`: domestic benchmark.
    - `sp500`: overseas benchmark.
    - `nasdaq`: overseas growth/technology benchmark.
    - `usdkrw`: USD/KRW exchange-rate reference.

    Behavior:
    - This is a read-only TR-backed data lookup.
    - It does not mutate alert state, send webhooks, or place orders.
    - Backend support can vary, but the response shape is always grouped by index id.

    Response:
    - A dict mapping each index id to a list of daily points.
    - Each point includes `date`, `open`, `high`, `low`, and `close`.
    - A supported index may return an empty list if no rows are available in the requested date range.

    KO:
    보유종목 알림 context에서 쓰는 시장 지수 일봉 OHLC를 기간으로 조회한다.
    `start_date`, `end_date`는 `YYYYMMDD` 또는 `YYYY-MM-DD`이며 `end_date`는 시작일 이후여야 한다.
    응답은 `kospi200`, `sp500`, `nasdaq`, `usdkrw` 같은 index id별 list로 묶인다.
    순수 조회용이며 알림 상태 변경, webhook 발송, 주문 실행을 하지 않는다."""
            return tools.get_market_index_prices(start_date, end_date)

        @_mcp_tool
        def get_sector_index_prices(
            sector_code: str,
            start_date: str,
            end_date: str,
            interval: str = "D",
        ) -> list[dict[str, Any]]:
            """EN:
    Return OHLC series for one supported sector index.

    Arguments:
    - `sector_code`: sector/index code such as a KOSPI200 sector code. v1 primarily uses known KOSPI200 sector mappings.
    - `start_date`: inclusive start date in `YYYYMMDD` or `YYYY-MM-DD`.
    - `end_date`: inclusive end date in `YYYYMMDD` or `YYYY-MM-DD`; must be on or after `start_date`.
    - `interval`: currently only daily `D` is supported by the real backend.

    Behavior:
    - This is a read-only TR-backed lookup for sector-relative context.
    - It does not update holding-alert baselines, send webhooks, or place orders.
    - Unsupported interval values raise a validation error.

    Response:
    - A list of OHLC points with `date`, `open`, `high`, `low`, and `close`.
    - Empty list means the backend found no rows for the sector/date range.

    KO:
    특정 업종/섹터 지수의 OHLC series를 조회한다.
    `sector_code`는 KOSPI200 업종 코드 같은 지수 코드를 넣고, 날짜는 `YYYYMMDD` 또는 `YYYY-MM-DD`를 받는다.
    real backend는 현재 일봉 `D`만 지원한다.
    순수 조회용이며 알림 상태 변경, webhook 발송, 주문 실행을 하지 않는다."""
            return tools.get_sector_index_prices(sector_code, start_date, end_date, interval)

        @_mcp_tool
        def get_stock_sector_profile(code: str) -> dict[str, Any]:
            """EN:
    Return sector-mapping metadata for one stock.

    Arguments:
    - `code`: 6-digit stock code. A leading `A` prefix is accepted when backend normalization supports it.

    Behavior:
    - This is metadata lookup used to connect a stock to a sector index for relative context.
    - The v1 real backend uses known KOSPI200/holding-stock mappings first; not every listed stock has a sector profile.
    - This tool is read-only and does not register runners, send webhooks, or place orders.

    Response:
    - `code`: normalized stock code.
    - `sector_code`: sector index code, or empty string when unavailable.
    - `sector_name`: human-readable sector name, or empty string when unavailable.
    - `source`: metadata source such as `known_kospi200_mapping`, `mock`, or `unavailable`.

    KO:
    한 종목을 업종/섹터 지수에 연결하기 위한 metadata를 조회한다.
    응답에는 정규화된 `code`, `sector_code`, `sector_name`, `source`가 포함된다.
    모든 종목의 섹터가 보장되지는 않으며, 모르는 종목은 `source=unavailable`과 빈 섹터 값을 반환할 수 있다.
    순수 조회용이며 runner 등록, webhook 발송, 주문 실행을 하지 않는다."""
            return tools.get_stock_sector_profile(code)

        @_mcp_tool
        def register_holding_alert_runner(
            accountNo: str,
            httpCallback: dict[str, Any],
            heldCode: list[str] | None = None,
            wannaCode: list[str] | None = None,
        ) -> dict[str, Any]:
            """EN:
    Register a same-day periodic holding decision-alert runner for one account.

    Purpose:
    - Watches current holdings, maintains realtime/cache state, and evaluates manual buy/sell decision alerts.
    - The runner never places orders. Every alert is only a manual decision aid.
    - Only one runner can be active for the same account. If a runner is already registered, cancel the existing runner before registering a different watch list.

    Arguments:
    - `accountNo`: account number to monitor for cash, holdings, and sizing context.
    - `heldCode`: optional array of 6-digit stock codes for held-position alerts. Omit it, pass `null`, or pass an empty array to monitor every current holding except stocks listed in `wannaCode`.
    - `wannaCode`: optional array of 6-digit stock codes to track even when not held. These are buy-watch candidates only; held-position sell/profit/recovery alerts are not generated for them.
    - `httpCallback`:
      - `method`: required, currently `POST` only.
      - `url`: required webhook URL.
      - `headers`: optional object.
      - `body`: optional object template. If omitted, the webhook request body is plain text containing only the public one-line summary.
      - `bodyFormat`: optional `json` or `form` when `body` exists; defaults to JSON.
      - Supported template replacements include `{{summary}}`, `{{alertType}}`, `{{code}}`, `{{name}}`, `{{tradePrice}}`, `{{recommendedQty}}`, `{{recommendedAmount}}`, `{{restriction}}`, `{{warning}}`, `{{reasonText}}`, and `{{finalText}}`. Display numeric replacements use thousands separators; raw numeric text is also available through matching `Raw`/`_raw` tokens such as `{{tradePriceRaw}}` and `{{recommendedQtyRaw}}`.

    Runtime behavior:
    - The runner is same-day only. It is not restored on later trading days and expires instead of monitoring a future day's holdings.
    - Uses a short timing tick plus cached TR/RT refresh layers; it is not intended to call every TR on every tick.
    - `heldCode` and `wannaCode` must not contain the same explicit stock code.
    - If `heldCode` is provided, held-position scan/evaluation and runner-owned realtime price subscriptions are limited to those selected holdings. A selected held code that is not currently held is dormant until it appears in the balance.
    - `wannaCode` keeps realtime price subscriptions even while unheld and evaluates only buy-watch conditions. If a wanna stock is currently held, wanna evaluation is paused for that stock.
    - Sends webhooks only when the stock is not observe-only, validation allows dispatch, duplicate/fatigue rules allow it, and an alert condition is selected.
    - The default summary includes alert type, stock, 매매희망가, recommended quantity, optional restriction, and "자동 주문 아님".
    - Internal calculation payloads are not exposed in the webhook body unless the user explicitly templates public replacement fields.
    - Registering a runner retains required realtime price subscriptions for current holdings and releases its own subscriptions on cancel.

    Response:
    - `runner_id`: id required by `cancel_holding_alert_runner`.
    - `accountNo`, `heldCode`, `wannaCode`, `registered_at`, `active`.
    - `warnings`: non-fatal setup warnings such as failed realtime subscription for a holding.

    KO:
    한 계좌의 당일 한정 보유종목 매수·매도 판단 알림 runner를 등록한다.
    runner는 보유 종목을 주기적으로 평가하고 필요하면 webhook으로 판단 알림을 보내지만, 절대 자동 주문을 실행하지 않는다.
    runner는 당일 한정이며 다음 거래일에는 복구되지 않고 만료된다.
    `heldCode`는 보유종목 알림 대상 배열이다. 생략, `null`, 빈 배열이면 `wannaCode`에 들어간 종목을 제외한 계좌 전체 보유종목을 감시한다.
    `wannaCode`는 미보유 상태에서도 추적하는 매수 관심 종목 배열이며 매수 판단만 평가한다.
    `heldCode`와 `wannaCode`에 같은 종목을 동시에 넣으면 등록을 거부한다.
    같은 계좌에 runner가 이미 등록돼 있으면 추가 등록은 거부된다. 기존 runner를 취소한 뒤 다시 등록해야 한다.
    `httpCallback.body`를 생략하면 한 줄 plain text summary만 전송하고, body를 주면 공개 replacement만 치환한다.
    실제 알림 발송은 관찰 전용 아님, 검증 통과, 중복/피로도 제한 통과 조건을 만족해야 한다.
    응답의 `runner_id`는 조회와 취소에 사용한다."""
            return tools.register_holding_alert_runner(accountNo, httpCallback, heldCode=heldCode, wannaCode=wannaCode)

        @_mcp_tool
        def list_holding_alert_runners() -> list[dict[str, Any]]:
            """EN:
    List registered same-day holding decision-alert runners.

    Behavior:
    - Read-only management tool.
    - Does not run a scan, send webhooks, mutate runner state, or place orders.

    Response:
    - A list of current-day runner records. Previous-day runners expire before listing.
    - Each record includes `runner_id`, `accountNo`, `heldCode`, `wannaCode`, `active`, `registered_at`, `last_scan_at`, `last_scan_result_count`, and the configured `httpCallback`.
    - `heldCode` is an empty array when the runner watches every current holding except `wannaCode` stocks.
    - `wannaCode` is an empty array when no unheld buy-watch candidates are configured.
    - Use `runner_id` with `cancel_holding_alert_runner`.

    KO:
    현재 등록된 당일 보유종목 판단 알림 runner 목록을 조회한다.
    읽기 전용 관리 tool이며 scan 실행, webhook 발송, 주문 실행을 하지 않는다.
    전일 이전 runner는 조회 전에 만료된다.
    각 항목에는 `runner_id`, `accountNo`, `heldCode`, `wannaCode`, 활성 여부, 등록 시각, 최근 scan 정보, callback 설정이 포함된다.
    `heldCode`가 빈 배열이면 `wannaCode` 종목을 제외한 계좌 전체 보유종목 감시 runner다."""
            return tools.list_holding_alert_runners()

        @_mcp_tool
        def cancel_holding_alert_runner(runner_id: str) -> dict[str, Any]:
            """EN:
    Cancel one holding decision-alert runner by id.

    Arguments:
    - `runner_id`: id returned by `register_holding_alert_runner` or `list_holding_alert_runners`.

    Behavior:
    - Stops the runner loop for that id and removes the same-day runner record.
    - Releases only realtime price subscriptions retained by the holding-alert runner, preserving subscriptions owned by existing price alerts, stock callbacks, fall-safes, news, or disclosure flows.
    - If this was the last holding runner, the holding-alert realtime listener is unregistered and its inactive raw-event queue is cleared.
    - Does not cancel normal orders, fall-safes, price alerts, stock callbacks, news subscriptions, disclosure subscriptions, or system callbacks.
    - Never places an order.

    Response:
    - `canceled`: whether a matching runner existed.
    - `removed_runners`: number of removed runner records.
    - `runner_id`: requested id.

    KO:
    `runner_id`로 보유종목 판단 알림 runner 한 건을 취소한다.
    해당 runner의 loop와 당일 runner record를 제거하고, holding-alert가 잡은 실시간 시세 구독만 자기 몫만큼 해제한다.
    기존 가격 알림, 주가 step callback, fall-safe, 뉴스/공시 구독, 시스템 callback은 취소하지 않는다.
    주문 취소나 신규 주문도 수행하지 않는다."""
            return tools.cancel_holding_alert_runner(runner_id)

        @_mcp_tool
        def get_stock_technical_indicators_daily(
            code: str,
            start_date: str | None = None,
            end_date: str | None = None,
        ) -> list[dict[str, Any]]:
            """EN:
    Return direct technical indicators calculated from daily stock OHLCV bars.

    Backtest contract:
    - Uses only daily bars on or before `end_date`.
    - If `start_date` is provided, the server fetches warmup history before it, calculates indicators, and then trims the response back to the requested range.
    - Does not use current quote snapshots, realtime state, order state, news, or future bars.

    Indicators:
    - `sma5`, `sma20`, `sma60`, `sma120`, `ema5`, `ema20`, `ema60`, `ema120`.
    - `volume_ma5`, `volume_ma20`, `volume_ma60`, `volume_ratio5`, `volume_ratio20`, `volume_ratio60`.
    - RSI, MACD line/signal/histogram, Bollinger bands, Ichimoku, ATR, ADX, +DI/-DI, trend regime, OBV/OBV SMA, MFI, and Chandelier Exit long.

    Arguments:
    - `code`: 6-digit stock code.
    - `start_date`, `end_date`: optional inclusive daily range in `YYYYMMDD` or `YYYY-MM-DD`.

    Response:
    - Latest daily bar first.
    - Rows include the source daily `date`, `close`, `volume`, and indicator fields; early rows can contain `null` while lookback periods warm up.

    KO:
    종목 일봉 OHLCV에서 직접 계산한 기술지표를 반환한다.
    `end_date` 이후 데이터, 현재가 snapshot, 실시간 상태, 주문 상태, 뉴스는 사용하지 않아 백테스트에 쓸 수 있다."""
            return tools.get_stock_technical_indicators_daily(code, start_date, end_date)

        @_mcp_tool
        def get_stock_technical_indicators_weekly(
            code: str,
            start_date: str | None = None,
            end_date: str | None = None,
        ) -> list[dict[str, Any]]:
            """EN:
    Return direct technical indicators calculated from weekly stock OHLCV bars derived from daily bars.

    Backtest contract:
    - Uses only daily bars on or before `end_date`.
    - Weekly OHLCV bars are derived from the available daily rows in each ISO week; if `end_date` falls mid-week, that final weekly bar is a partial week made only from bars available through `end_date`.
    - Does not use current quote snapshots, realtime state, order state, news, or future bars.

    Response:
    - Latest weekly bar first.
    - Each row includes `week`, `start_date`, `end_date`, weekly OHLCV/trading-day metadata, plus the same direct indicator field family as daily indicators.
    - `date` is also set to the weekly bar `end_date` for compatibility with indicator-row consumers.

    Arguments:
    - `code`: 6-digit stock code.
    - `start_date`, `end_date`: optional inclusive daily range in `YYYYMMDD` or `YYYY-MM-DD`; weekly rows are derived from the daily bars inside that cutoff after warmup.

    KO:
    일봉을 주봉으로 묶은 뒤 주봉 기준 직접 기술지표를 반환한다.
    미완성 주간도 `end_date`까지 확보된 일봉만 사용하므로 백테스트 기준을 지킨다."""
            return tools.get_stock_technical_indicators_weekly(code, start_date, end_date)

        @_mcp_tool
        def get_stock_technical_indicators_intraday(
            code: str,
            date: str,
            interval_minutes: int = 5,
            as_of_time: str | None = None,
        ) -> list[dict[str, Any]]:
            """EN:
    Return direct technical indicators calculated from intraday stock OHLCV bars.

    Backtest contract:
    - Uses only bars from `date`.
    - If `as_of_time` is provided, bars after that time are excluded, preventing same-day lookahead.
    - Does not use current quote snapshots, order state, news, or future bars.

    Direct intraday fields:
    - Standard indicator fields from the intraday bar sequence.
    - `vwap`: cumulative intraday VWAP through each returned bar.
    - `session_volume_ratio`: the current bar's volume divided by the average volume of returned same-day bars through that bar.

    Arguments:
    - `code`: 6-digit stock code.
    - `date`: trading date in `YYYYMMDD` or `YYYY-MM-DD`.
    - `interval_minutes`: candle interval in minutes.
    - `as_of_time`: optional cutoff in `HHMMSS`, `HH:MM:SS`, `HHMM`, or `HH:MM`.

    Response:
    - Latest intraday bar first.
    - Each row includes `date`, `time`, `timestamp`, OHLCV, interval metadata, VWAP, volume ratio, and indicator fields.

    KO:
    특정 거래일의 분봉 OHLCV에서 직접 기술지표를 계산한다.
    `as_of_time` 이후 봉은 제외하므로 장중 백테스트에 사용할 수 있다."""
            return tools.get_stock_technical_indicators_intraday(code, date, interval_minutes, as_of_time)

        @_mcp_tool
        def get_stock_chart_patterns_daily(
            code: str,
            start_date: str | None = None,
            end_date: str | None = None,
            lookback_days: int = 120,
        ) -> list[dict[str, Any]]:
            """EN:
    Return chart-pattern candidates calculated from daily stock OHLCV bars.

    Backtest contract:
    - Uses only daily bars in the requested date range, never bars after `end_date`.
    - Pattern output is a candidate/evidence layer, not a buy/sell signal.

    Candidate families include `uptrend_structure`, `downtrend_structure`, `box_consolidation`, `range_breakout`, `range_breakdown`, `symmetrical_triangle_candidate`, `double_top_candidate`, and `double_bottom_candidate`.
    Breakout candidates use `prior_20bar_volume_ratio`, which compares latest volume to the average of the prior 20 completed bars and is intentionally distinct from indicator-row `volume_ratio20`.

    Arguments:
    - `code`: 6-digit stock code.
    - `start_date`, `end_date`: optional inclusive daily range in `YYYYMMDD` or `YYYY-MM-DD`.
    - `lookback_days`: number of recent daily bars to evaluate after the date cutoff.

    Response:
    - Candidates sorted by `confidence` descending.
    - Each item includes `name`, `direction`, `confidence`, `window_days`, `observed_at`, `levels`, and `evidence`.
    - `observed_at` is the daily bar date where the candidate was observed.

    KO:
    일봉 차트 패턴 후보를 반환한다. `end_date` 이후 데이터는 사용하지 않는다."""
            return tools.get_stock_chart_patterns_daily(code, start_date, end_date, lookback_days)

        @_mcp_tool
        def get_stock_chart_patterns_weekly(
            code: str,
            start_date: str | None = None,
            end_date: str | None = None,
            lookback_weeks: int = 120,
        ) -> list[dict[str, Any]]:
            """EN:
    Return chart-pattern candidates calculated from weekly stock OHLCV bars.

    Backtest contract:
    - Uses only daily bars on or before `end_date`, then derives weekly bars.
    - If `end_date` falls mid-week, the final weekly bar is partial and contains only data through `end_date`.
    - Does not use current quote snapshots, realtime state, news, or future bars.

    Response fields match daily pattern candidates, but `window_days` should be read as a bar-count window over weekly bars.
    Weekly bars follow ISO week grouping and are suitable for weekly backtests where only completed-or-currently-observable weekly data is allowed.

    Arguments:
    - `code`: 6-digit stock code.
    - `start_date`, `end_date`: optional inclusive daily range in `YYYYMMDD` or `YYYY-MM-DD`.
    - `lookback_weeks`: number of recent weekly bars to evaluate after deriving weekly rows.

    Response:
    - Candidates sorted by `confidence` descending.
    - Each item includes `name`, `direction`, `confidence`, `window_days`, `observed_at`, `levels`, and `evidence`.
    - `observed_at` is the weekly bar end date.
    - `window_days` is kept for compatibility; in this weekly tool it means the number of weekly bars used, not calendar days.

    KO:
    주봉 차트 패턴 후보를 반환한다. 주봉은 `end_date`까지의 일봉만 사용해 만든다."""
            return tools.get_stock_chart_patterns_weekly(code, start_date, end_date, lookback_weeks)

        @_mcp_tool
        def get_stock_chart_patterns_intraday(
            code: str,
            date: str,
            interval_minutes: int = 5,
            as_of_time: str | None = None,
            lookback_bars: int = 120,
        ) -> list[dict[str, Any]]:
            """EN:
    Return chart-pattern candidates calculated from intraday stock OHLCV bars.

    Backtest contract:
    - Uses only bars from `date`.
    - If `as_of_time` is provided, bars after that time are excluded.
    - Does not use daily close, current quote snapshots, order state, news, or future bars.

    Arguments:
    - `code`: 6-digit stock code.
    - `date`: trading date in `YYYYMMDD` or `YYYY-MM-DD`.
    - `interval_minutes`: candle interval in minutes.
    - `as_of_time`: optional cutoff in `HHMMSS`, `HH:MM:SS`, `HHMM`, or `HH:MM`; bars with time after it are excluded.
    - `lookback_bars`: number of recent intraday bars to evaluate.

    Response:
    - Latest observed intraday pattern candidates, scored by confidence.
    - Candidate families match the daily/weekly tools, but the pattern window is interpreted as intraday bars.
    - Each item includes `name`, `direction`, `confidence`, `window_days`, `observed_at`, `levels`, and `evidence`.
    - `observed_at` is the intraday timestamp.
    - `window_days` is kept for compatibility; in this intraday tool it means the number of intraday bars used, not calendar days.

    KO:
    특정 거래일의 분봉 차트 패턴 후보를 반환한다. `as_of_time` 이후 봉은 제외한다."""
            return tools.get_stock_chart_patterns_intraday(code, date, interval_minutes, as_of_time, lookback_bars)

        @_mcp_tool
        def get_stock_market_environment_indicators(
            code: str,
            as_of_date: str | None = None,
            as_of_time: str | None = None,
        ) -> dict[str, Any]:
            """EN:
    Return indirect market-environment indicators for one stock.

    Purpose:
    - Separates stock-external or relative indicators from direct chart indicators.
    - Includes market index context, sector context, stock-vs-market relative strength, trading-value environment, 52-week high distance, and overseas ETF macro references when applicable.

    Backtest contract:
    - Uses only completed daily bars through `completed_daily_end_date`.
    - If `as_of_time` is before 15:30 KST, the requested `as_of_date` daily bar is treated as unfinished and excluded.
    - If `as_of_time` is omitted, `as_of_date` is treated as a completed daily bar cutoff; for intraday/as-of backtests, pass `as_of_time` explicitly.
    - Does not use current quote snapshots, realtime state, order state, news, or future bars.

    Arguments:
    - `code`: 6-digit stock code.
    - `as_of_date`: optional cutoff date in `YYYYMMDD` or `YYYY-MM-DD`; defaults to today's KST date.
    - `as_of_time`: optional KST cutoff in `HHMMSS`, `HH:MM:SS`, `HHMM`, or `HH:MM`.

    Response:
    - `completed_daily_end_date` shows the actual daily cutoff used.
    - `backtest_policy` states the data-cut rules.
    - Includes `market`, `sector`, `relative_strength`, `trading_value`, `high_52w`, `fx`, `overseas`, and `sector_profile`.

    KO:
    종목 자체 차트에서 직접 계산한 지표가 아니라 시장/섹터/상대강도/거래대금/52주 위치/환율 같은 간접 환경 지표를 반환한다.
    현재가 snapshot을 섞지 않고 기준 시점 이후 데이터를 사용하지 않는다."""
            return tools.get_stock_market_environment_indicators(code, as_of_date, as_of_time)

        @_mcp_tool
        def get_stock_technical_analysis_bundle(
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
            """EN:
    Return a backtest-safe technical-analysis bundle for one stock.

    Purpose:
    - Convenience entry point for tool users that want the main technical-analysis data in one call.
    - Bundles raw price bars, direct technical indicators, chart-pattern candidates, and market-environment indicators.
    - Reuses the same fetched daily/intraday source rows internally so daily indicators, weekly indicators, patterns, and environment data share one cutoff.

    Backtest contract:
    - `mode` is `backtest_safe`.
    - Daily/weekly data use only daily bars through `completed_daily_end_date`.
    - If `as_of_time` is before 15:30 KST, `end_date` is treated as an unfinished daily bar and excluded from daily/weekly/environment sections.
    - Intraday sections use only bars from `end_date`; if `as_of_time` is provided, bars after it are excluded.
    - Does not use quote snapshots, order books, news, account/order state, realtime subscriptions, or holding-alert state.

    Arguments:
    - `code`: 6-digit stock code.
    - `start_date`, `end_date`: optional inclusive range in `YYYYMMDD` or `YYYY-MM-DD`; when `start_date` is omitted, `lookback_days` chooses the returned daily window.
    - `as_of_time`: optional intraday cutoff in `HHMMSS`, `HH:MM:SS`, `HHMM`, or `HH:MM`.
    - `include_intraday`: whether to include intraday bars, intraday indicators, and intraday chart patterns.
    - `intraday_interval_minutes`: intraday candle interval.
    - `lookback_days`, `lookback_weeks`, `lookback_bars`: pattern/return-window controls for daily, weekly, and intraday sections.

    Response:
    - `price_bars`: `daily`, `weekly`, and optional `intraday` source rows.
    - `technical_indicators`: `daily`, `weekly`, and optional `intraday` indicator rows.
    - `chart_patterns`: `daily`, `weekly`, and optional `intraday` pattern candidates.
    - `market_environment_indicators`: indirect market/sector/relative-strength/trading-value/52-week context.
    - `backtest_policy.suitable_for_backtesting` is `true`.
    - `data_status`: per-section availability and counts.

    KO:
    한 종목의 백테스트 가능한 기술분석 묶음을 반환한다.
    현재가, 호가, 뉴스, 계좌/주문/보유 알림 상태를 섞지 않고 동일 cutoff의 원천 데이터로 일봉/주봉/분봉 지표와 패턴, 시장 환경 지표를 함께 제공한다."""
            return tools.get_stock_technical_analysis_bundle(
                code,
                start_date,
                end_date,
                as_of_time,
                include_intraday,
                intraday_interval_minutes,
                lookback_days,
                lookback_weeks,
                lookback_bars,
            )

        @_mcp_tool
        def get_stock_technical_analysis_bundle_live(
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
            """EN:
    Return a live/current utility bundle for one stock's technical analysis.

    Purpose:
    - Convenience entry point for "what does this stock look like right now?" workflows.
    - Starts with the same technical-analysis sections as `get_stock_technical_analysis_bundle`.
    - Adds live/current context under `live_context`, such as quote snapshot, order book, news headlines, investor flow, fundamentals, and holding-alert indicator context when requested.

    Backtest contract:
    - `mode` is `live_not_backtest_safe`.
    - This tool is intentionally not suitable for backtesting because live/current sections may change between calls.
    - It may use current quote snapshots, latest intraday rows, current order book, current news, and holding-alert context.
    - It remains read-only: it does not subscribe to realtime feeds, send callbacks, mutate alert state, or place/cancel/modify orders.

    Arguments:
    - `code`: 6-digit stock code.
    - `start_date`: optional returned daily/weekly start range in `YYYYMMDD` or `YYYY-MM-DD`; when omitted, `lookback_days` chooses the returned daily window.
    - `date`: optional live date/cutoff in `YYYYMMDD` or `YYYY-MM-DD`; defaults to today's KST date.
    - `include_intraday`: whether to include latest intraday bars, indicators, and patterns.
    - `include_quote_snapshot`, `include_order_book`, `include_news`, `include_investor_flow`, `include_fundamentals`, `include_holding_alert_context`: live-context toggles.
    - `news_limit`: maximum number of news headlines.

    Response:
    - Includes `price_bars`, `technical_indicators`, `chart_patterns`, `market_environment_indicators`, and `data_status` like the backtest-safe bundle.
    - Adds `live_context` with requested current/snapshot sections.
    - `backtest_policy.suitable_for_backtesting` is `false`.

    KO:
    한 종목을 지금 판단하기 편하도록 기술분석 묶음에 현재가, 호가, 뉴스, 수급, 재무, 보유 알림용 지표 문맥 같은 live context를 추가해 반환한다.
    편의용 현재 컨텍스트이므로 백테스트용으로 쓰면 안 되며, 그래도 순수 조회만 수행하고 구독/알림/주문 상태는 변경하지 않는다."""
            return tools.get_stock_technical_analysis_bundle_live(
                code,
                start_date,
                date,
                include_intraday,
                intraday_interval_minutes,
                lookback_days,
                lookback_weeks,
                lookback_bars,
                include_quote_snapshot,
                include_order_book,
                include_news,
                news_limit,
                include_investor_flow,
                include_fundamentals,
                include_holding_alert_context,
            )

        @_mcp_tool
        def subscribe_realtime_price(code: str) -> dict[str, object]:
            """EN: Subscribe to integrated realtime price updates for a stock code. | KO: 종목코드의 통합 실시간 시세 구독을 등록한다."""
            return tools.subscribe_realtime_price(code)

        @_mcp_tool
        def unsubscribe_realtime_price(code: str) -> dict[str, object]:
            """EN: Unsubscribe from integrated realtime price updates for a stock code. | KO: 종목코드의 통합 실시간 시세 구독을 해제한다."""
            return tools.unsubscribe_realtime_price(code)

        @_mcp_tool
        def subscribe_disclosure(code: str, httpCallback: dict[str, Any], devCallback: bool = False) -> dict[str, Any]:
            """EN:
    Subscribe to realtime disclosure events for one 6-digit stock code.

    Arguments:
    - `code`: 6-digit stock code to watch.
    - `httpCallback`:
      - `method`: required, currently `POST` only
      - `url`: required
      - `headers`: optional object
      - `body`: optional object
      - `bodyFormat`: optional, valid only when `body` exists, defaults to `json`
    - `devCallback`: optional boolean. If `true`, the server queues one development test callback immediately after registration and includes the queueing result in the tool response.

    Body behavior:
    - The server queues the configured `body` for asynchronous delivery after replacement.
    - The server does not wrap or merge the RT payload into the callback body.
    - If `body` is omitted, the webhook is called with an empty request body.
    - If `devCallback` is `true`, the server queues one immediate test callback using the same dispatch path as a live event.

    Tool response:
    - The immediate tool response includes `subscription_id`.
    - `already_subscribed` reports whether the same persistent callback subscription row already existed.
    - `already_indi_registered` is scoped to the requested INDI RT feed (`N2`) and is reported only after the RequestRT registration call succeeds.
    - `rt_disclosure_registered_now` is `true` only when this call actually performed and succeeded the N2 RequestRT registration.
    - `rt_subscriptions` reports current `N0`/`N2` feed active status separately.
    - Use that `subscription_id` with `unsubscribe_disclosure`.
    - Use `list_disclosure_subscriptions` to inspect active rows, including `subscription_id`, `registered_at`, `last_event_at`, and `evaluated_event_count`.

    Supported replacements in `httpCallback.body`:
    - `{{disclosure_type}}`
    - `{{disclosure_type_label}}`
    - `{{date}}`
    - `{{time}}`
    - `{{article_id}}`
    - `{{code}}`
    - `{{name}}`
    - `{{title}}`
    - `{{delete_flag_label}}`

    If the disclosure title is unavailable, `{{title}}` becomes `"제목 없음"`.

    KO:
    하나의 6자리 종목코드에 대한 실시간 공시 이벤트를 구독한다.

    인자:
    - `code`: 감시할 6자리 종목코드
    - `httpCallback`:
      - `method`: 필수, 현재는 `POST`만 지원
      - `url`: 필수
      - `headers`: 선택 object
      - `body`: 선택 object
      - `bodyFormat`: 선택값이며 `body`가 있을 때만 유효하고, 생략 시 기본값은 `json`
    - `devCallback`: 선택 bool. `true`이면 등록 직후 개발용 테스트 callback을 1회 큐에 넣고, 큐 등록 결과를 tool 응답에 포함한다.

    본문 전송 규칙:
    - 서버는 사용자가 지정한 `body` 구조를 유지한 채 replacement만 적용해서 비동기 전송 큐에 넣는다.
    - 서버는 RT 이벤트 payload를 callback body에 자동으로 감싸거나 merge하지 않는다.
    - `body`를 생략하면 빈 요청 본문으로 웹훅을 호출한다.
    - `devCallback=true`이면 실제 공시 RT와 같은 dispatch 경로로 즉시 테스트 callback 1회를 큐에 넣는다.

    Tool 응답:
    - 즉시 반환되는 tool 응답에는 `subscription_id`가 포함된다.
    - `already_subscribed`는 같은 persistent callback 구독 행이 이미 있었는지를 나타낸다.
    - `already_indi_registered`는 요청한 INDI RT 피드(`N2`) 기준이며 RequestRT 등록 호출이 성공한 뒤에만 반환된다.
    - `rt_disclosure_registered_now`는 이번 호출에서 N2 RequestRT 등록을 실제 수행하고 성공했을 때만 `true`다.
    - `rt_subscriptions`는 현재 `N0`/`N2` 피드 active 상태를 분리해서 보여준다.
    - 이 `subscription_id`는 `unsubscribe_disclosure`에서 한 건만 정확히 해제할 때 사용한다.
    - `list_disclosure_subscriptions`를 호출하면 `subscription_id`, `registered_at`, `last_event_at`, `evaluated_event_count`를 포함한 현재 구독 목록을 볼 수 있다.

    `httpCallback.body`에서 사용할 수 있는 replacement:
    - `{{disclosure_type}}`
    - `{{disclosure_type_label}}`
    - `{{date}}`
    - `{{time}}`
    - `{{article_id}}`
    - `{{code}}`
    - `{{name}}`
    - `{{title}}`
    - `{{delete_flag_label}}`

    공시 제목을 확보하지 못한 경우 `{{title}}`은 `"제목 없음"`으로 치환된다.
    """
            return tools.subscribe_disclosure(code, httpCallback, devCallback)

        @_mcp_tool
        def unsubscribe_disclosure(subscription_id: str) -> dict[str, Any]:
            """EN: Remove one disclosure RT subscription by `subscription_id`. Use `list_disclosure_subscriptions` to inspect active rows and pick the exact id to remove. | KO: `subscription_id`로 공시 실시간 구독 한 건을 해제한다. 활성 구독 목록과 정확한 id는 `list_disclosure_subscriptions`로 확인한다."""
            return tools.unsubscribe_disclosure(subscription_id)

        @_mcp_tool
        def list_disclosure_subscriptions() -> list[dict[str, Any]]:
            """EN: List persistent disclosure RT subscriptions. Each row includes `subscription_id`, `code`, display `name`, configured `httpCallback`, `registered_at`, `last_event_at`, and `evaluated_event_count`. | KO: 영속 유지되는 공시 RT 구독 목록을 반환한다. 각 행에는 `subscription_id`, `code`, 표시용 `name`, 설정된 `httpCallback`, `registered_at`, `last_event_at`, `evaluated_event_count`가 포함된다."""
            return tools.list_disclosure_subscriptions()

        @_mcp_tool
        def subscribe_news(
            types: list[str],
            httpCallback: dict[str, Any],
            code: str | None = None,
            devCallback: bool = False,
        ) -> dict[str, Any]:
            """EN:
    Subscribe to realtime news events for one or more news type codes.

    Arguments:
    - `types`: supported news type codes.
      - `A`: info
      - `M`: mt
      - `E`: ed
      - `Y`: yonhap
      - `H`: hankyung
      - `I`: internal
      - `F`: market_commentary
      - `U`: overseas
    - `code`: optional 6-digit stock code. If omitted, the subscription watches the wider feed for the requested news types.
    - `httpCallback`:
      - `method`: required, currently `POST` only
      - `url`: required
      - `headers`: optional object
      - `body`: optional object
      - `bodyFormat`: optional, valid only when `body` exists, defaults to `json`
    - `devCallback`: optional boolean. If `true`, the server queues one development test callback immediately after registration and includes the queueing result in the tool response.

    Body behavior:
    - The server queues the configured `body` for asynchronous delivery after replacement.
    - The server does not wrap or merge the RT payload into the callback body.
    - If `body` is omitted, the webhook is called with an empty request body.
    - If `devCallback` is `true`, the server queues one immediate test callback using the same dispatch path as a live event.

    Tool response:
    - The immediate tool response includes `subscription_id`.
    - `already_subscribed` reports whether the same persistent callback subscription row already existed.
    - `already_indi_registered` is scoped to the requested INDI RT feed (`N0`) and is reported only after the RequestRT registration call succeeds.
    - `rt_news_registered_now` is `true` only when this call actually performed and succeeded the N0 RequestRT registration.
    - `rt_subscriptions` reports current `N0`/`N2` feed active status separately.
    - Use that `subscription_id` with `unsubscribe_news`.
    - Use `list_news_subscriptions` to inspect active rows, including `subscription_id`, `registered_at`, `last_event_at`, and `evaluated_event_count`.

    Supported replacements in `httpCallback.body`:
    - `{{news_type}}`
    - `{{news_type_label}}`
    - `{{date}}`
    - `{{time}}`
    - `{{article_id}}`
    - `{{code}}`
    - `{{name}}`
    - `{{title}}`
    - `{{delete_flag_label}}`

    KO:
    하나 이상의 뉴스 타입 코드에 대한 실시간 뉴스 이벤트를 구독한다.

    인자:
    - `types`: 지원 뉴스 타입 코드 배열
      - `A`: 인포
      - `M`: MT
      - `E`: ED
      - `Y`: 연합
      - `H`: 한경
      - `I`: 내부
      - `F`: 시황
      - `U`: 해외
    - `code`: 선택값인 6자리 종목코드. 비우면 요청한 뉴스 타입의 더 넓은 피드를 구독한다.
    - `httpCallback`:
      - `method`: 필수, 현재는 `POST`만 지원
      - `url`: 필수
      - `headers`: 선택 object
      - `body`: 선택 object
      - `bodyFormat`: 선택값이며 `body`가 있을 때만 유효하고, 생략 시 기본값은 `json`
    - `devCallback`: 선택 bool. `true`이면 등록 직후 개발용 테스트 callback을 1회 큐에 넣고, 큐 등록 결과를 tool 응답에 포함한다.

    본문 전송 규칙:
    - 서버는 사용자가 지정한 `body` 구조를 유지한 채 replacement만 적용해서 비동기 전송 큐에 넣는다.
    - 서버는 RT 이벤트 payload를 callback body에 자동으로 감싸거나 merge하지 않는다.
    - `body`를 생략하면 빈 요청 본문으로 웹훅을 호출한다.
    - `devCallback=true`이면 실제 뉴스 RT와 같은 dispatch 경로로 즉시 테스트 callback 1회를 큐에 넣는다.

    Tool 응답:
    - 즉시 반환되는 tool 응답에는 `subscription_id`가 포함된다.
    - `already_subscribed`는 같은 persistent callback 구독 행이 이미 있었는지를 나타낸다.
    - `already_indi_registered`는 요청한 INDI RT 피드(`N0`) 기준이며 RequestRT 등록 호출이 성공한 뒤에만 반환된다.
    - `rt_news_registered_now`는 이번 호출에서 N0 RequestRT 등록을 실제 수행하고 성공했을 때만 `true`다.
    - `rt_subscriptions`는 현재 `N0`/`N2` 피드 active 상태를 분리해서 보여준다.
    - 이 `subscription_id`는 `unsubscribe_news`에서 한 건만 정확히 해제할 때 사용한다.
    - `list_news_subscriptions`를 호출하면 `subscription_id`, `registered_at`, `last_event_at`, `evaluated_event_count`를 포함한 현재 구독 목록을 볼 수 있다.

    `httpCallback.body`에서 사용할 수 있는 replacement:
    - `{{news_type}}`
    - `{{news_type_label}}`
    - `{{date}}`
    - `{{time}}`
    - `{{article_id}}`
    - `{{code}}`
    - `{{name}}`
    - `{{title}}`
    - `{{delete_flag_label}}`
    """
            return tools.subscribe_news(types, httpCallback, code, devCallback)

        @_mcp_tool
        def unsubscribe_news(subscription_id: str) -> dict[str, Any]:
            """EN: Remove one realtime news subscription by `subscription_id`. Use `list_news_subscriptions` to inspect active rows and pick the exact id to remove. | KO: `subscription_id`로 뉴스 실시간 구독 한 건을 해제한다. 활성 구독 목록과 정확한 id는 `list_news_subscriptions`로 확인한다."""
            return tools.unsubscribe_news(subscription_id)

        @_mcp_tool
        def list_news_subscriptions() -> list[dict[str, Any]]:
            """EN: List persistent news RT subscriptions. Each row includes `subscription_id`, `types`, optional `code`, display `name` when one code is pinned, configured `httpCallback`, `registered_at`, `last_event_at`, and `evaluated_event_count`. | KO: 영속 유지되는 뉴스 RT 구독 목록을 반환한다. 각 행에는 `subscription_id`, `types`, 선택 `code`, 단일 종목 고정 구독일 때의 표시용 `name`, 설정된 `httpCallback`, `registered_at`, `last_event_at`, `evaluated_event_count`가 포함된다."""
            return tools.list_news_subscriptions()

        @_mcp_tool
        def register_system_callback(httpCallback: dict[str, Any]) -> dict[str, Any]:
            """EN:
    Register a persistent system-event callback.

    Purpose:
    - Use this callback for internal runtime warnings and operational failures such as realtime unsubscribe errors.

    Arguments:
    - `httpCallback`:
      - `method`: required, currently `POST` only
      - `url`: required
      - `headers`: optional object
      - `body`: optional object
      - `bodyFormat`: optional, valid only when `body` exists, defaults to `json`

    Body behavior:
    - If `body` is omitted, the server queues a default JSON payload with `event_type`, `message`, `occurred_at`, and optional `details`.
    - If `body` is provided, the server keeps the configured body shape, applies replacements, and queues it for asynchronous delivery.

    Supported replacements in `httpCallback.body`:
    - `{{tag}}`
    - `{{name}}`
    - `{{callstack}}`
    - `{{occurred_at}}`

    Tool response:
    - The immediate tool response includes `system_callback_id`.
    - Use that `system_callback_id` with `unregister_system_callback`.
    - Use `list_system_callbacks` to inspect active rows, including `system_callback_id`, `registered_at`, and configured `httpCallback`.

    KO:
    영속 유지되는 시스템 이벤트 callback을 등록한다.

    목적:
    - 이 callback은 실시간 해제 실패 같은 내부 런타임 경고와 운영 오류를 전달하는 데 사용된다.

    인자:
    - `httpCallback`:
      - `method`: 필수, 현재는 `POST`만 지원
      - `url`: 필수
      - `headers`: 선택 object
      - `body`: 선택 object
      - `bodyFormat`: 선택값이며 `body`가 있을 때만 유효하고, 생략 시 기본값은 `json`

    본문 전송 규칙:
    - `body`를 생략하면 서버가 `event_type`, `message`, `occurred_at`, 선택 `details`를 담은 기본 JSON 본문을 비동기 전송 큐에 넣는다.
    - `body`가 있으면 사용자가 지정한 구조를 유지한 채 replacement만 적용해 비동기 전송 큐에 넣는다.

    `httpCallback.body`에서 사용할 수 있는 replacement:
    - `{{tag}}`
    - `{{name}}`
    - `{{callstack}}`
    - `{{occurred_at}}`

    Tool 응답:
    - 즉시 반환값에는 `system_callback_id`가 포함된다.
    - 이 `system_callback_id`는 `unregister_system_callback`에서 한 건만 정확히 해제할 때 사용한다.
    - `list_system_callbacks`를 호출하면 `system_callback_id`, `registered_at`, 설정된 `httpCallback`을 포함한 현재 callback 목록을 볼 수 있다.
    """
            return tools.register_system_callback(httpCallback)

        @_mcp_tool
        def list_system_callbacks() -> list[dict[str, Any]]:
            """EN: List persistent system-event callbacks. Each row includes `system_callback_id`, configured `httpCallback`, and `registered_at`. | KO: 영속 유지되는 시스템 이벤트 callback 목록을 반환한다. 각 행에는 `system_callback_id`, 설정된 `httpCallback`, `registered_at`이 포함된다."""
            return tools.list_system_callbacks()

        @_mcp_tool
        def unregister_system_callback(system_callback_id: str) -> dict[str, Any]:
            """EN: Remove one system-event callback by `system_callback_id`. Use `list_system_callbacks` to inspect active rows and pick the exact id to remove. | KO: `system_callback_id`로 시스템 이벤트 callback 한 건을 해제한다. 활성 callback 목록과 정확한 id는 `list_system_callbacks`로 확인한다."""
            return tools.unregister_system_callback(system_callback_id)

        @_mcp_tool
        def register_price_alert(
            code: str,
            condition: str,
            threshold: float,
            window_minutes: int | None = None,
            message: str = "",
            httpCallback: dict[str, Any] | None = None,
            debounce_seconds: float | None = None,
            once_only: bool = False,
        ) -> dict[str, Any]:
            """EN: Register a same-day realtime price alert for one 6-digit stock code. `condition` must be `climb`, `fall`, or `fastmove`. `threshold` is the trigger price for `climb` and `fall`, and the trigger percentage for `fastmove`. `window_minutes` is only valid for `fastmove`; omit it for `climb` and `fall`. For `climb` and `fall`, optional `debounce_seconds` defaults to 10; events during debounce update side/current state but cannot fire, and firing resumes only on a later event after debounce ends. For `fastmove`, `debounce_seconds` is not supported; after a `fastmove` fires, additional ticks inside `window_minutes` are coalesced and one trailing alert is queued at window end if the latest price still satisfies the threshold. If `once_only` is true, the alert is removed after its first fire. `message` is stored with the alert and returned in list results. `httpCallback` is required and must include `method` and `url`; optional fields are `headers`, `body`, and `bodyFormat`. If `bodyFormat` is omitted while `body` exists, JSON is used by default. Price alerts are same-day only and are restored only within the same trading day. | KO: 하나의 6자리 종목코드에 대한 당일 한정 실시간 가격 알람을 등록한다. `condition`은 `climb`, `fall`, `fastmove` 중 하나여야 한다. `threshold`는 `climb`와 `fall`에서는 기준 가격이고, `fastmove`에서는 기준 변동률이다. `window_minutes`는 `fastmove`에서만 유효하며, `climb`와 `fall`에서는 생략해야 한다. `climb`/`fall`의 선택 `debounce_seconds` 기본값은 10초다. debounce 동안 들어온 이벤트는 현재가/side 상태만 갱신하고 발화하지 않으며, debounce 종료 뒤 새 이벤트에서 crossing이 발생해야 다시 발화한다. `fastmove`에는 `debounce_seconds`를 지원하지 않는다. `fastmove` 발화 후 `window_minutes` 동안 추가 tick은 합쳐서 보관하고, 윈도우가 끝나는 시점에 마지막 현재가가 여전히 기준을 만족하면 trailing 알람을 1회 큐에 넣는다. `once_only`가 true면 첫 발화 후 알람을 제거한다. `message`는 알람과 함께 저장되고 목록 조회 결과에도 반환된다. `httpCallback`은 필수이며 `method`, `url`, 선택 `headers`, `body`, `bodyFormat`을 지원한다. 가격 알람은 당일 한정이며 같은 거래일 안에서만 복구된다."""
            return tools.register_price_alert(
                code,
                condition,
                threshold,
                window_minutes,
                message,
                httpCallback,
                debounce_seconds,
                once_only,
            )

        @_mcp_tool
        def list_price_alerts() -> list[dict[str, Any]]:
            """EN: List active same-day price alerts. Each row includes the `alert_id`, stock `code`, display `name`, `condition`, `threshold`, optional `window_minutes`, `debounce_seconds`, `once_only`, stored `message`, configured `httpCallback`, the most recent `current_price` snapshot, and `created_at`. `recovery_fail` rows also include `breach_price`, `recovery_price`, `failure_minutes`, `recovery_minutes`, `valid_after`, `recovery_state`, `breached_at`, and `recovery_since`. `uptrend_end` rows also include `start_price`, `end_price`, `end_minutes`, `valid_after`, `uptrend_state`, `uptrend_started_at`, and `ending_since`. | KO: 현재 활성화된 당일 가격 알람 목록을 반환한다. 각 행에는 `alert_id`, 종목 `code`, 표시용 `name`, `condition`, `threshold`, 선택 `window_minutes`, `debounce_seconds`, `once_only`, 저장된 `message`, 설정된 `httpCallback`, 최근 `current_price` 스냅샷, `created_at`이 포함된다. `recovery_fail` 행에는 `breach_price`, `recovery_price`, `failure_minutes`, `recovery_minutes`, `valid_after`, `recovery_state`, `breached_at`, `recovery_since`도 포함된다. `uptrend_end` 행에는 `start_price`, `end_price`, `end_minutes`, `valid_after`, `uptrend_state`, `uptrend_started_at`, `ending_since`도 포함된다."""
            return tools.list_price_alerts()

        @_mcp_tool
        def register_recovery_fail_alert(
            code: str,
            breach_price: float,
            recovery_price: float,
            failure_minutes: float = 3,
            recovery_minutes: float = 3,
            valid_after: str = "11:00",
            httpCallback: dict[str, Any] | None = None,
            once_only: bool = True,
        ) -> dict[str, Any]:
            """EN: Register a same-day realtime recovery-failure alert for one 6-digit stock code. This alert is for the pattern: after `valid_after` in KST, if `current_price <= breach_price`, the alert watches whether the stock can recover to `current_price >= recovery_price`. If it fails to recover within `failure_minutes`, or starts recovering but cannot hold `current_price >= recovery_price` for `recovery_minutes`, a `recovery_fail` callback is queued. If recovery succeeds before any failure callback, the alert quietly returns to waiting state. If a failure callback was already sent and the stock later holds `current_price >= recovery_price` for `recovery_minutes`, a `recovery_fail_resolved` callback is queued. This tool never places an order. `valid_after` must be `HH:MM`. `recovery_price` must be greater than `breach_price`. `httpCallback` is required and must include `method` and `url`; optional fields are `headers`, `body`, and `bodyFormat`. If `body` is provided, the server keeps the body shape and applies these snake_case replacements: `{{event_type}}`, `{{event_type_label}}`, `{{alert_id}}`, `{{code}}`, `{{name}}`, `{{summary}}`, `{{current_price}}`, `{{breach_price}}`, `{{recovery_price}}`, `{{failure_minutes}}`, `{{recovery_minutes}}`, `{{valid_after}}`, `{{breached_at}}`, and `{{triggered_at}}`. `event_type_label` is a Korean label such as `회복 실패` or `회복 실패 해소`; `summary` is generated for the specific event. Numeric replacements are raw numeric strings. If `body` is omitted, the callback body is a plain-text summary. Unknown replacement tokens become empty strings. `once_only` defaults to true; when true, the alert remains after the first `recovery_fail` only long enough to send a later `recovery_fail_resolved`, then is removed. When false, the alert returns to waiting after resolution and can alert again on a later breach/recovery-failure cycle. | KO: 하나의 6자리 종목코드에 대해 당일 한정 실시간 회복 실패 알림을 등록한다. KST `valid_after` 이후 `current_price <= breach_price`가 되면 이탈로 보고, 이후 `current_price >= recovery_price` 회복 여부를 감시한다. `failure_minutes` 안에 회복하지 못하거나, 회복 시도 후 `recovery_minutes` 동안 회복선을 유지하지 못하면 `recovery_fail` callback을 큐에 넣는다. 실패 callback 전에 회복에 성공하면 조용히 대기 상태로 돌아간다. 실패 callback이 이미 나간 뒤 `current_price >= recovery_price` 상태가 `recovery_minutes` 동안 유지되면 `recovery_fail_resolved` callback을 큐에 넣는다. 이 tool은 주문을 실행하지 않는다. `valid_after`는 `HH:MM` 형식이며 `recovery_price`는 `breach_price`보다 커야 한다. `httpCallback`은 필수이며 `method`, `url`, 선택 `headers`, `body`, `bodyFormat`을 지원한다. `body`가 있으면 지정한 구조를 유지하고 `{{event_type}}`, `{{event_type_label}}`, `{{alert_id}}`, `{{code}}`, `{{name}}`, `{{summary}}`, `{{current_price}}`, `{{breach_price}}`, `{{recovery_price}}`, `{{failure_minutes}}`, `{{recovery_minutes}}`, `{{valid_after}}`, `{{breached_at}}`, `{{triggered_at}}`를 치환한다. `event_type_label`은 `회복 실패`, `회복 실패 해소` 같은 한국어 라벨이며 `summary`는 이벤트별로 생성된다. 숫자 replacement는 표시 포맷 없는 원시 숫자 문자열이다. `body`가 없으면 plain-text summary를 보낸다. 알 수 없는 replacement는 빈 문자열이 된다. `once_only` 기본값은 true이고, true면 첫 `recovery_fail` 이후에도 추후 `recovery_fail_resolved` 전송까지만 알림을 유지한 뒤 제거한다. false면 해소 후 대기 상태로 돌아가 나중의 재이탈/회복 실패 주기에서 다시 알릴 수 있다."""
            return tools.register_recovery_fail_alert(
                code,
                breach_price,
                recovery_price,
                failure_minutes,
                recovery_minutes,
                valid_after,
                httpCallback,
                once_only,
            )

        @_mcp_tool
        def register_uptrend_end_alert(
            code: str,
            start_price: float,
            end_price: float,
            end_minutes: float = 3,
            valid_after: str = "09:00",
            httpCallback: dict[str, Any] | None = None,
            once_only: bool = True,
        ) -> dict[str, Any]:
            """EN: Register a same-day realtime uptrend-end alert for one 6-digit stock code. This alert watches a completed upward leg: after `valid_after` in KST, `current_price >= start_price` marks the uptrend as active, then `current_price <= end_price` starts an ending check. If the price stays at or below `end_price` for `end_minutes`, an `uptrend_end` callback is queued. If the price moves back above `end_price` before the hold time ends, the ending check is canceled and the alert keeps watching the active uptrend. This tool never places an order. `valid_after` must be `HH:MM`. `start_price` must be greater than `end_price`. `httpCallback` is required and must include `method` and `url`; optional fields are `headers`, `body`, and `bodyFormat`. If `body` is provided, the server keeps the body shape and applies these snake_case replacements: `{{event_type}}`, `{{event_type_label}}`, `{{alert_id}}`, `{{code}}`, `{{name}}`, `{{summary}}`, `{{current_price}}`, `{{start_price}}`, `{{end_price}}`, `{{end_minutes}}`, `{{valid_after}}`, `{{uptrend_started_at}}`, `{{ending_since}}`, and `{{triggered_at}}`. `event_type_label` is `상승세 종료`; numeric replacements are raw numeric strings. If `body` is omitted, the callback body is a plain-text summary. Unknown replacement tokens become empty strings. `once_only` defaults to true; when false, the alert rearms only after price reaches `start_price` again. | KO: 하나의 6자리 종목코드에 대해 당일 한정 실시간 상승세 종료 알림을 등록한다. KST `valid_after` 이후 `current_price >= start_price`가 되면 상승세 활성 상태로 보고, 이후 `current_price <= end_price`가 되면 종료 확인을 시작한다. 가격이 `end_minutes` 동안 `end_price` 이하에 머무르면 `uptrend_end` callback을 큐에 넣는다. 유지 시간이 끝나기 전에 `end_price` 위로 회복하면 종료 확인을 취소하고 활성 상승세 감시를 계속한다. 이 tool은 주문을 실행하지 않는다. `valid_after`는 `HH:MM` 형식이며 `start_price`는 `end_price`보다 커야 한다. `httpCallback`은 필수이며 `method`, `url`, 선택 `headers`, `body`, `bodyFormat`을 지원한다. `body`가 있으면 지정한 구조를 유지하고 `{{event_type}}`, `{{event_type_label}}`, `{{alert_id}}`, `{{code}}`, `{{name}}`, `{{summary}}`, `{{current_price}}`, `{{start_price}}`, `{{end_price}}`, `{{end_minutes}}`, `{{valid_after}}`, `{{uptrend_started_at}}`, `{{ending_since}}`, `{{triggered_at}}`를 치환한다. `event_type_label`은 `상승세 종료`이며 숫자 replacement는 표시 포맷 없는 원시 숫자 문자열이다. `body`가 없으면 plain-text summary를 보낸다. 알 수 없는 replacement는 빈 문자열이 된다. `once_only` 기본값은 true이고, false면 발화 후 가격이 다시 `start_price`에 도달해야 재무장된다."""
            return tools.register_uptrend_end_alert(
                code,
                start_price,
                end_price,
                end_minutes,
                valid_after,
                httpCallback,
                once_only,
            )

        @_mcp_tool
        def cancel_price_alert(alert_id: str | None = None, code: str | None = None) -> dict[str, Any]:
            """EN: Cancel price alerts by one `alert_id`, or cancel every active alert for one 6-digit stock code by passing `code`. At least one of `alert_id` or `code` is required. The response reports whether anything was removed and how many alerts were deleted. | KO: 하나의 `alert_id`로 가격 알람 하나를 취소하거나, `code`를 넣어 해당 6자리 종목코드의 활성 알람을 모두 취소한다. `alert_id`와 `code` 중 최소 하나는 필수다. 응답에는 실제로 삭제가 있었는지와 몇 개의 알람이 제거됐는지가 포함된다."""
            return tools.cancel_price_alert(alert_id, code)

        @_mcp_tool
        def register_stock_price_callback(
            code: str,
            step: float,
            price_filter: str | None = None,
            httpCallback: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            """EN: Register a same-day realtime stock price step callback. The first valid integrated price tick sets the baseline price without sending a callback. Later, when the current price moves up or down by at least `step` from the latest baseline, one HTTP callback is queued and the baseline is reset to the current price. After a callback fires, additional ticks for 10 seconds are coalesced; when that window ends, the latest received price is evaluated and one trailing callback is queued if it still moved by at least `step` from the baseline. `step` must be greater than zero. `price_filter` is optional: use values like `70000+` to send only when the current price is at least 70000, `70000-` to send only when it is at most 70000, or omit/null for all prices. `httpCallback` is required and supports `method`, `url`, optional `headers`, `body`, and `bodyFormat`; body replacements support `{{name}}`, display `{{price}}`, raw `{{priceRaw}}`/`{{price_raw}}`, and `{{direction}}`, where direction is `상향` or `하향`. | KO: 하나의 종목에 대해 당일 한정 주가 step 변동 callback을 등록한다. 첫 유효 통합현재가 tick은 기준 가격만 설정하고 callback을 보내지 않는다. 이후 현재가가 최신 기준 가격에서 `step` 이상 오르거나 내리면 HTTP callback을 1회 큐에 넣고 기준 가격을 현재가로 갱신한다. callback 발화 후 10초 동안 추가 tick은 합쳐서 보관하고, 10초가 끝나는 시점에 마지막으로 들어온 현재가가 기준 가격에서 여전히 `step` 이상 움직였으면 trailing callback을 1회 큐에 넣는다. `step`은 0보다 커야 한다. `price_filter`는 선택값이며 `70000+`는 현재가가 70000원 이상일 때만, `70000-`는 70000원 이하일 때만 보내고, 생략/null이면 모든 가격에서 보낸다. `httpCallback`은 필수이며 `method`, `url`, 선택 `headers`, `body`, `bodyFormat`을 지원한다. body replacement는 `{{name}}`, 표시용 `{{price}}`, 원본 `{{priceRaw}}`/`{{price_raw}}`, `{{direction}}`을 지원하며 방향은 `상향` 또는 `하향`이다."""
            return tools.register_stock_price_callback(code, step, httpCallback, price_filter)

        @_mcp_tool
        def list_stock_price_callbacks() -> list[dict[str, Any]]:
            """EN: List active same-day stock price step callbacks. Each row includes `stock_price_callback_id`, stock `code`, display `name`, `step`, optional `price_filter`, current and baseline prices, last direction, fired count, registered time, last fired time, and configured `httpCallback`. | KO: 현재 활성화된 당일 주가 step 변동 callback 목록을 반환한다. 각 행에는 `stock_price_callback_id`, 종목 `code`, 표시용 `name`, `step`, 선택 `price_filter`, 현재/기준 가격, 마지막 방향, 발화 횟수, 등록 시각, 마지막 발화 시각, 설정된 `httpCallback`이 포함된다."""
            return tools.list_stock_price_callbacks()

        @_mcp_tool
        def cancel_stock_price_callback(
            stock_price_callback_id: str | None = None,
            code: str | None = None,
        ) -> dict[str, Any]:
            """EN: Cancel one stock price step callback by `stock_price_callback_id`, or cancel every active callback for one 6-digit stock code by passing `code`. At least one of `stock_price_callback_id` or `code` is required. | KO: `stock_price_callback_id`로 주가 step 변동 callback 하나를 취소하거나, `code`를 넣어 해당 6자리 종목코드의 활성 callback을 모두 취소한다. 둘 중 최소 하나는 필수다."""
            return tools.cancel_stock_price_callback(stock_price_callback_id, code)

        @_mcp_tool
        def register_fall_safe(
            account_no: str,
            code: str,
            trigger_price: float,
            quantity: int,
            httpCallback: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            """EN: Register a same-day protective fall-safe for one stock position. A fall-safe does not behave like a resting limit order. Instead, it waits until the realtime price breaks downward through `trigger_price`, and only then attempts a one-shot market sell for `quantity`. This makes it suitable for downside protection that ordinary resting orders cannot express cleanly. Before calling this tool, always present the final planned arguments/configuration to the user and receive explicit confirmation for that exact call. `httpCallback` is optional and uses the same webhook shape as other callback tools: `method`, `url`, optional `headers`, optional `body`, optional `bodyFormat`. If configured, the webhook is queued after the order attempt, but the request body is still exactly the configured `body`; delivery is asynchronous and retried by the callback worker. Fall-safes are same-day only and are removed after the first trigger regardless of accepted, failed, or dry-run order outcome. | KO: 하나의 보유 종목에 대한 당일 한정 보호용 fall-safe를 등록한다. fall-safe는 일반 지정가 주문처럼 호가창에 미리 걸어두는 주문이 아니다. 대신 실시간 가격이 `trigger_price`를 위에서 아래로 깨는 순간에만 `quantity` 수량의 시장가 매도를 1회 시도한다. 그래서 일반 대기 주문으로는 깔끔하게 표현하기 어려운 하방 대응 보호 장치에 적합하다. 이 tool을 실제 호출하기 전에는 매번 최종 구성값을 사용자에게 보여주고 해당 호출에 대한 명시 확인을 받은 뒤 호출한다. `httpCallback`은 선택값이며 다른 callback tool과 같은 웹훅 구조를 사용한다: `method`, `url`, 선택 `headers`, 선택 `body`, 선택 `bodyFormat`. 설정하면 주문 시도 이후 웹훅을 비동기 큐에 넣지만, 요청 본문은 여전히 사용자가 지정한 `body` 그대로 전송되며 callback worker가 재시도한다. fall-safe는 당일 한정이고, 주문 결과가 accepted, failed, dry-run 중 무엇이든 첫 발동 후 자동 제거된다."""
            return tools.register_fall_safe(account_no, code, trigger_price, quantity, httpCallback)

        @_mcp_tool
        def list_fall_safes() -> list[dict[str, Any]]:
            """EN: List active same-day fall-safe rules. Each row includes `fall_safe_id`, `account_no`, `code`, stock `name`, `trigger_price`, `quantity`, optional `httpCallback`, and `registered_at`. | KO: 현재 활성화된 당일 fall-safe 규칙 목록을 반환한다. 각 행에는 `fall_safe_id`, `account_no`, `code`, 종목 `name`, `trigger_price`, `quantity`, 선택 `httpCallback`, `registered_at`이 포함된다."""
            return tools.list_fall_safes()

        @_mcp_tool
        def cancel_fall_safe(fall_safe_id: str) -> dict[str, Any]:
            """EN: Cancel one fall-safe rule by `fall_safe_id`. The response reports whether the rule existed and how many fall-safe entries were removed. | KO: `fall_safe_id`로 fall-safe 규칙 하나를 취소한다. 응답에는 해당 규칙이 실제로 존재했는지와 몇 개의 fall-safe 항목이 제거됐는지가 포함된다."""
            return tools.cancel_fall_safe(fall_safe_id)

        @_mcp_tool
        def get_accounts() -> list[dict[str, Any]]:
            """EN: Return accounts available in the logged-in Indi session. | KO: 로그인된 Indi 세션에서 조회 가능한 계좌 목록을 반환한다."""
            return tools.get_accounts()

        @_mcp_tool
        def get_account_summary(account_no: str) -> dict[str, Any]:
            """EN: Return cash, asset, and valuation summary for an account. | KO: 계좌의 예수금, 자산, 평가 요약 정보를 반환한다."""
            return tools.get_account_summary(account_no)

        @_mcp_tool
        def get_fundamentals(
            code: str,
            consolidated: bool = True,
            quarterly: bool = True,
        ) -> list[dict[str, Any]]:
            """EN: Return financial and valuation metrics for a stock code. | KO: 종목코드의 재무 및 밸류에이션 지표를 반환한다."""
            return tools.get_fundamentals(code, consolidated, quarterly)

        @_mcp_tool
        def get_quote_snapshot(code: str) -> dict[str, Any]:
            """EN: Return the latest snapshot metrics for a stock code. | KO: 종목코드의 현재가 핵심 스냅샷 지표를 반환한다."""
            return tools.get_quote_snapshot(code)

        @_mcp_tool
        def get_investor_flow_by_stock(
            code: str,
            start_date: str | None = None,
            end_date: str | None = None,
        ) -> list[dict[str, Any]]:
            """EN: Return investor flow by stock for a date range. | KO: 기간별 종목 투자주체 수급 데이터를 반환한다."""
            return tools.get_investor_flow_by_stock(code, start_date, end_date)

        @_mcp_tool
        def get_market_investor_flow_intraday(
            include_institution_breakdown: bool = False,
        ) -> list[dict[str, Any]]:
            """EN: Return current trading-day intraday KOSPI investor flow by amount from `TR_1202_B` (`업종투자자시간대별-거래대금`). Uses fixed TR inputs `업종코드=0001`, `시장분류=01`, `조회방법=1`, and `시간간격=010`. Each row includes `time` and nested `retail`, `foreign`, and `institution` buy/sell/net amount fields in raw INDI units; set `include_institution_breakdown=true` to also include institution subcategories. | KO: `TR_1202_B`(`업종투자자시간대별-거래대금`) 기반으로 당일 장중 KOSPI 투자주체 수급을 거래대금 기준으로 반환한다. TR 입력은 `업종코드=0001`, `시장분류=01`, `조회방법=1`, `시간간격=010`으로 고정한다. 각 행에는 `time`과 원 INDI 단위의 개인/외국인/기관 buy/sell/net 금액이 들어가며, `include_institution_breakdown=true`를 주면 기관 세부 항목도 포함한다."""
            return tools.get_market_investor_flow_intraday(include_institution_breakdown)

        @_mcp_tool
        def get_foreign_flow_rankings(
            market: str = "all",
            consecutive_days: int = 3,
            direction: str = "buy",
        ) -> list[dict[str, Any]]:
            """EN: Return foreign-flow ranking candidates. | KO: 외국인 순매수/순매도 랭킹 후보를 반환한다."""
            return tools.get_foreign_flow_rankings(market, consecutive_days, direction)

        @_mcp_tool
        def get_top_movers(
            market: str = "all",
            direction: str = "up",
            date: str | None = None,
            limit: int = 100,
            kospi200_only: bool = False,
        ) -> list[dict[str, Any]]:
            """EN: Return top gainers or losers, capped to the requested item count. Set kospi200_only=true to keep only cached KOSPI200 constituents. | KO: 상승률 또는 하락률 상위 종목을 요청한 개수만큼 반환한다. kospi200_only=true를 주면 서버 시작 시 캐시한 KOSPI200 구성 종목만 남긴다."""
            return tools.get_top_movers(market, direction, date, limit, kospi200_only)

        @_mcp_tool
        def list_stock_news(code: str, date: str | None = None) -> list[dict[str, Any]]:
            """EN: Return stock-specific news or disclosure headlines for a 6-digit stock code and date. Each row includes `news_type` and a human-friendly `news_type_label`; undocumented codes fall back to `unknown(<code>)`. | KO: 6자리 숫자 종목코드와 날짜를 받아 해당 종목의 뉴스/공시 헤드라인을 반환한다. 각 행에는 `news_type`과 사람이 읽기 쉬운 `news_type_label`을 함께 담고, 문서에 없는 코드는 `unknown(<code>)`로 내려준다."""
            return tools.list_stock_news(code, date)

        @_mcp_tool
        def list_market_flow_news(
            date: str | None = None,
            from_time: str | None = None,
            to_time: str | None = None,
        ) -> list[dict[str, Any]]:
            """EN: Return market-flow news headlines for one date, optionally bounded by `from_time` and `to_time` (`HHMMSS`, `HH:MM`, or `HH:MM:SS`). Backed by `TR_3102_CT` with its fixed market-flow category (`09`); INDI appears to provide only the latest 20 rows for this query, so treat it as a recent-headlines view rather than a complete intraday archive. Each row includes `date`, normalized `time`, `title`, `news_type`, `news_type_label`, optional stock `code`, and `article_id` for `get_news_content`. | KO: 시장 수급/시황 뉴스 헤드라인을 날짜로 반환하며, `from_time`/`to_time`(`HHMMSS`, `HH:MM`, `HH:MM:SS`)으로 시간대를 좁힐 수 있다. `TR_3102_CT`의 시장 수급 고정 카테고리(`09`) 기반이며 INDI가 이 조회에서 최신 20건만 제공하는 것으로 보이므로 장중 전체 기록이 아니라 최신 헤드라인 뷰로 다룬다. 각 행에는 `date`, 정규화된 `time`, `title`, `news_type`, `news_type_label`, 선택 종목 `code`, `get_news_content`에 넘길 `article_id`가 포함된다."""
            return tools.list_market_flow_news(date, from_time, to_time)

        @_mcp_tool
        def get_news_content(news_type: str, date: str, article_id: str) -> dict[str, Any]:
            """EN: Return full market news content. Includes cleaned plain-text `content`, original `raw_html`, extracted `links`, and `rcpNo` when a DART receipt number can be found in the raw HTML. | KO: 시장 뉴스 본문 전체를 반환한다. 정리된 plain text `content`, 원본 `raw_html`, 추출된 `links`, 그리고 raw HTML에서 DART 접수번호를 찾은 경우 `rcpNo`를 함께 내려준다."""
            return tools.get_news_content(news_type, date, article_id)

        @_mcp_tool
        def get_disclosure_content(rcpNo: str) -> dict[str, Any]:
            """EN: Return DART disclosure body as official viewer-rendered HTML for a receipt number (`rcpNo`). `print_page_break_selector` is the selector to split the returned HTML: `section[data-ele-id]` when server-wrapped sections exist, otherwise `p.pgbrk, p.PGBRK` when actual print page-break tags exist. If this field is empty or absent, do not split the returned HTML into pages/sections. | KO: DART 접수번호(`rcpNo`)로 공시 본문을 DART 공식 viewer 렌더 HTML 형태로 반환한다. `print_page_break_selector`는 반환 HTML을 나누는 selector다. 서버가 감싼 섹션이 있으면 `section[data-ele-id]`, 실제 인쇄용 페이지 구분 태그가 있으면 `p.pgbrk, p.PGBRK`다. 이 필드가 비어 있거나 없으면 반환 HTML을 페이지/섹션으로 나누지 않는다."""
            return tools.get_disclosure_content(rcpNo)

        @_mcp_tool
        def get_disclosure_content_from_article(
            date: str,
            article_id: str,
            news_type: str = "5",
        ) -> dict[str, Any]:
            """EN: Return a DART disclosure body as HTML for a news/disclosure article id. If the article raw HTML already carries DART/KRX disclosure-body markup, it is returned directly; otherwise the server extracts DART `rcpNo` and returns official viewer-rendered HTML. Use `print_page_break_selector` to split the returned HTML only when it is non-empty; if it is empty or absent, do not split the HTML into pages/sections. | KO: 뉴스/공시 기사 ID로 DART 공시 본문을 HTML 형태로 반환한다. 기사 raw HTML이 DART/KRX 공시 본문 마크업을 이미 담고 있으면 그대로 반환하고, 아니면 DART `rcpNo`를 추출해 공식 viewer 렌더 HTML을 반환한다. `print_page_break_selector`가 비어 있지 않을 때만 그 selector로 HTML을 나누고, 비어 있거나 없으면 페이지/섹션으로 나누지 않는다."""
            return tools.get_disclosure_content_from_article(date, article_id, news_type)

        @_mcp_tool
        def get_volume_surge(
            market: str = "all",
            limit: int = 100,
            kospi200_only: bool = False,
        ) -> list[dict[str, Any]]:
            """EN: Return volume-surge scanner results, capped to the requested item count. Set kospi200_only=true to keep only cached KOSPI200 constituents. | KO: 거래량 급증 스캐너 결과를 요청한 개수만큼 반환한다. kospi200_only=true를 주면 서버 시작 시 캐시한 KOSPI200 구성 종목만 남긴다."""
            return tools.get_volume_surge(market, limit, kospi200_only)

        @_mcp_tool
        def get_new_highs_lows(
            market: str = "all",
            mode: str = "new_high",
            limit: int = 100,
            kospi200_only: bool = False,
        ) -> list[dict[str, Any]]:
            """EN: Return new-high or new-low scanner results, capped to the requested item count. Set kospi200_only=true to keep only cached KOSPI200 constituents. | KO: 신고가 또는 신저가 스캐너 결과를 요청한 개수만큼 반환한다. kospi200_only=true를 주면 서버 시작 시 캐시한 KOSPI200 구성 종목만 남긴다."""
            return tools.get_new_highs_lows(market, mode, limit, kospi200_only)

        @_mcp_tool
        def get_limit_hits(
            market: str = "all",
            mode: str = "upper",
            kospi200_only: bool = False,
        ) -> list[dict[str, Any]]:
            """EN: Return upper-limit or lower-limit hit scanner results. Set kospi200_only=true to keep only cached KOSPI200 constituents. | KO: 상한가 또는 하한가 스캐너 결과를 반환한다. kospi200_only=true를 주면 서버 시작 시 캐시한 KOSPI200 구성 종목만 남긴다."""
            return tools.get_limit_hits(market, mode, kospi200_only)

        @_mcp_tool
        def get_order_book(code: str) -> dict[str, Any]:
            """EN: Return order-book information for a stock code. The real backend first waits up to 12 seconds for a fresh integrated `UH` realtime depth tick. If no fresh realtime depth arrives, it falls back to `TR_RB002` and may return only the best ask/bid with `partial=true`; this is common for inactive or thinly traded periods. During after-hours close-price trading, the response is `available=false` because there is no continuous order book. If neither realtime nor TR has a current quote, the response is a normal `available=false` no-quote result rather than a transport failure. | KO: 종목코드의 호가 정보를 반환한다. real backend는 먼저 통합 `UH` 실시간 depth tick을 최대 12초 기다린다. fresh 실시간 depth가 없으면 `TR_RB002`로 fallback하며, 거래가 적거나 비활성 시간대에는 `partial=true`와 함께 매도1호가/매수1호가만 반환될 수 있다. 장종료후 시간외종가 구간은 연속 호가창이 없으므로 `available=false`로 반환한다. 실시간과 TR 모두 현재 호가를 주지 않으면 전송 실패가 아니라 정상적인 `available=false` 호가 없음 결과를 반환한다."""
            return tools.get_order_book(code)

        @_mcp_tool
        def get_balance(account_no: str) -> list[dict[str, Any]]:
            """EN: Return holdings for an account. | KO: 계좌의 보유 종목 잔고를 반환한다."""
            return tools.get_balance(account_no)

        @_mcp_tool
        def get_executions(account_no: str) -> list[dict[str, Any]]:
            """EN: Return execution records for an account. | KO: 계좌의 체결 내역을 반환한다."""
            return tools.get_executions(account_no)

        @_mcp_tool
        def get_open_orders(account_no: str, code: str | None = None) -> list[dict[str, Any]]:
            """EN: Return unfilled or partially filled cash stock orders for an account. Pass a 6-digit stock code to narrow the result to one stock. | KO: 계좌의 미체결 또는 부분체결 현물 주문을 반환한다. code에 6자리 숫자 종목코드를 넣으면 해당 종목만 좁혀서 조회한다."""
            return tools.get_open_orders(account_no, code)

        @_mcp_tool
        def register_order_carryover(
            account_no: str,
            code: str,
            order_id: str,
            premarket_to_regular: bool = True,
            regular_to_aftermarket: bool = True,
            httpCallback: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            """EN: Register an existing open cash stock order for same-day automatic session carryover in the current service process. This does not modify the order immediately, but during the 1-minute transition window the service may cancel the open order, confirm the cancelled quantity, and place an equivalent SOR limit order only for the confirmed cancelled quantity. If the cancelled quantity cannot be positively confirmed from open-order/execution checks, or if the transition window elapses before replacement order submission, the service does not place the replacement order and records a missed result. If the target order is partially filled at execution time, the service does not cancel or place anything and records `status=chaos` with a Korean `status_desc`. Registrations made within 10 seconds before, or after, a transition start do not trigger that transition for the day and are marked missed. A cancellation request does not interrupt a carryover that is already in flight. Optional `httpCallback` uses the normal callback shape; when provided without `body`, the transition result payload is sent as JSON with `status` and `status_desc` as the only outcome fields.

Supported `httpCallback.body` replacements:
- `{{stockName}}`: display name from the resolved open order.
- `{{stockCode}}`: normalized stock code.
- `{{quantity}}`: display carried-over order quantity when a new SOR order is placed; otherwise the registered unfilled quantity used for the transition decision.
- `{{quantityRaw}}`, `{{quantity_raw}}`: same quantity without thousands separators.
- `{{tradePrice}}`: display registered limit price.
- `{{tradePriceRaw}}`, `{{trade_price_raw}}`: same price without thousands separators.
- `{{side}}`: raw side value, `buy` or `sell`.
- `{{sideLabel}}`: Korean side label, `매수` or `매도`.
- `{{targetMarket}}`: destination market for the carryover order, usually `SOR`; may be `KRX` or `NXT` if the placed-order response identifies that market.
- `{{status}}`: final transition status, such as `success`, `missed`, or `chaos`.
- `{{statusDesc}}`: Korean human-readable status detail.
- `{{transition}}`: transition key, `premarket_to_regular` or `regular_to_aftermarket`.
- `{{executed}}`, `{{skipped}}`: boolean text values for execution/skip diagnostics.
- `{{carryoverId}}`: carryover registration id.
- `{{accountNo}}`: account number.
- `{{lastStatusAt}}`: KST compact timestamp when the latest status was recorded.
- `{{eventType}}`: fixed value `order_carryover_transition`.

Before calling this tool, always present the resolved account/code/order_id, transition options, remaining quantity, price, and callback setting to the user and receive explicit confirmation. | KO: 현재 서비스 프로세스 안에서 기존 미체결 현물 주문을 당일 한정 세션 이월 자동화 대상으로 등록한다. 이 호출은 주문을 즉시 정정하거나 취소하지 않지만, 1분 장 전환 실행창 안에서 원 주문을 취소하고 취소 수량을 확인한 뒤 확인된 취소 수량만 동일 조건의 SOR 지정가 주문으로 다시 넣을 수 있다. 미체결/체결 조회로 취소 수량을 확정하지 못했거나 신규 주문 직전 실행창이 지나면 replacement 주문을 넣지 않고 missed로 기록한다. 실행 시점 대상 주문이 부분 체결 상태이면 아무 주문도 취소/신규 실행하지 않고 실패로 보며 `status=chaos`와 한국어 `status_desc`로 기록한다. 전환 시작 10초 전부터 전환 이후에 등록된 건은 당일 해당 전환을 실행하지 않고 missed로 기록한다. 이미 실행 중인 이월은 등록 해제 요청으로 중간 중단하지 않는다. 선택 `httpCallback`은 기존 callback 형식을 사용하며, `body` 없이 지정하면 전환 결과 payload에서 판정 필드는 `status`와 `status_desc`만 보낸다.

지원하는 `httpCallback.body` replacement:
- `{{stockName}}`: 조회로 resolve된 미체결 주문의 종목명.
- `{{stockCode}}`: 정규화된 종목코드.
- `{{quantity}}`: 신규 SOR 주문이 들어간 경우 실제 이월 주문 수량, 그 외에는 전환 판단에 사용한 등록 당시 미체결 수량. 표시용 천단위 쉼표를 포함한다.
- `{{quantityRaw}}`, `{{quantity_raw}}`: 천단위 쉼표 없는 동일 수량.
- `{{tradePrice}}`: 등록 주문의 지정가. 표시용 천단위 쉼표를 포함한다.
- `{{tradePriceRaw}}`, `{{trade_price_raw}}`: 천단위 쉼표 없는 동일 가격.
- `{{side}}`: 원본 방향 값, `buy` 또는 `sell`.
- `{{sideLabel}}`: 한국어 방향 라벨, `매수` 또는 `매도`.
- `{{targetMarket}}`: 이월 주문의 목적 시장. 보통 `SOR`이며, 주문 응답이 시장을 식별하면 `KRX` 또는 `NXT`가 될 수 있다.
- `{{status}}`: 최종 전환 상태. 예: `success`, `missed`, `chaos`.
- `{{statusDesc}}`: 사람이 읽는 한국어 상태 설명.
- `{{transition}}`: 전환 키. `premarket_to_regular` 또는 `regular_to_aftermarket`.
- `{{executed}}`, `{{skipped}}`: 실행/skip 진단용 boolean text.
- `{{carryoverId}}`: 이월 등록 id.
- `{{accountNo}}`: 계좌번호.
- `{{lastStatusAt}}`: 마지막 상태가 기록된 KST compact timestamp.
- `{{eventType}}`: 고정값 `order_carryover_transition`.

이 tool을 호출하기 전에는 계좌/종목/주문번호/전환 옵션/미체결 잔량/가격/callback 설정을 사용자에게 보여주고 명시 확인을 받아야 한다."""
            return tools.register_order_carryover(
                account_no,
                code,
                order_id,
                premarket_to_regular,
                regular_to_aftermarket,
                httpCallback,
            )

        @_mcp_tool
        def list_order_carryovers(account_no: str | None = None, code: str | None = None) -> list[dict[str, Any]]:
            """EN: List current-day order carryovers that have not been manually cancelled. Previous-day registrations are expired and are not returned. `attempted_dates` records one attempt per transition per trading day, `transition_statuses` and `last_status` use values such as `pending`, `success`, `missed`, and `chaos`, and `last_result` keeps `status`, Korean `status_desc`, and execution diagnostics. | KO: 수동 해제되지 않은 당일 주문 이월 자동화를 반환한다. 전일 이전 등록은 만료되어 반환하지 않는다. `attempted_dates`는 전환별 거래일 1회 시도를 기록하고, `transition_statuses`와 `last_status`는 `pending`, `success`, `missed`, `chaos` 같은 상태를 담으며, `last_result`에는 `status`, 한국어 `status_desc`, 실행 진단 정보가 남는다."""
            return tools.list_order_carryovers(account_no, code)

        @_mcp_tool
        def cancel_order_carryover(
            carryover_id: str | None = None,
            account_no: str | None = None,
            code: str | None = None,
            order_id: str | None = None,
        ) -> dict[str, Any]:
            """EN: Cancel an order carryover registration only. This does not cancel, modify, or place a broker order. Pass carryover_id, or filters such as account_no/code/order_id. | KO: 주문 이월 자동화 등록만 해제한다. 이 호출은 증권사 주문을 취소/정정/신규 실행하지 않는다. carryover_id 또는 account_no/code/order_id 같은 필터를 전달한다."""
            return tools.cancel_order_carryover(carryover_id, account_no, code, order_id)

        @_mcp_tool
        def get_trade_history(
            account_no: str,
            code: str | None,
            start_date: str,
            end_date: str | None = None,
        ) -> list[dict[str, Any]]:
            """EN: Return account trade history over a date range. Leave code empty for account-wide history, or pass a 6-digit stock code without the market prefix for stock-specific history. Requests longer than 1 year are split internally and merged automatically. | KO: 계좌의 기간 매매내역을 반환한다. code를 비우면 계좌 전체, 6자리 숫자 종목코드를 넣으면 종목별 내역을 조회한다. 1년을 넘는 기간은 서버 내부에서 자동 분할 조회 후 합쳐서 반환한다."""
            return tools.get_trade_history(account_no, code, start_date, end_date)

        @_mcp_tool
        def get_account_ledger(
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
            """EN: Return account ledger entries over a date range using Indi TR `SACA132Q1`. This is the general ledger view, so it can be used for dividends, deposits/withdrawals, FX, interest, and stock-linked entries. `transaction_type` accepts friendly names like `all`, `sell`, `buy`, `deposit`, `withdraw`, `transfer_in`, `transfer_out`, `buy_sell`, `deposit_withdraw`, `transfer_in_out`, `fx`, `els_dls`, `dividend`, `loan_interest`, `credit_interest`, or the raw Indi codes `0`,`1`,`2`,`3`,`4`,`5`,`6`,`8`,`9`,`A`,`B`,`C`,`D`,`E`,`F`. `market` accepts `all`, `domestic`, `overseas`, or raw codes `0`,`1`,`2`. `include_mmw` maps to the TR's `MMW내역포함` flag, `include_rp_details` maps to `RP상세여부`, and `code` narrows the ledger to one stock code when available. `product_code` and `admin` are optional low-level overrides; in normal use leave them empty and the server will try to resolve account metadata automatically. Requests longer than 1 year are split internally and merged automatically. | KO: Indi TR `SACA132Q1`를 감싼 계좌 원장 조회 도구다. 배당금, 입출금, 환전, 이자, 종목 연계 원장 항목을 함께 조회할 수 있다. `transaction_type`에는 `all`, `sell`, `buy`, `deposit`, `withdraw`, `transfer_in`, `transfer_out`, `buy_sell`, `deposit_withdraw`, `transfer_in_out`, `fx`, `els_dls`, `dividend`, `loan_interest`, `credit_interest` 같은 쉬운 이름이나 원본 Indi 코드 `0`,`1`,`2`,`3`,`4`,`5`,`6`,`8`,`9`,`A`,`B`,`C`,`D`,`E`,`F`를 넣을 수 있다. `market`은 `all`, `domestic`, `overseas` 또는 원본 코드 `0`,`1`,`2`를 받는다. `include_mmw`는 `MMW내역포함`, `include_rp_details`는 `RP상세여부` 입력에 대응하고, `code`를 넣으면 가능한 경우 특정 종목 원장으로 좁힌다. `product_code`와 `admin`은 저수준 override 용도이며 보통은 비워두면 서버가 계좌 메타데이터를 자동으로 맞춰본다. 1년을 넘는 기간 요청은 서버 내부에서 자동 분할 후 합쳐서 반환한다."""
            return tools.get_account_ledger(
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

        @_mcp_tool
        def list_gold_products() -> list[dict[str, Any]]:
            """EN: Return supported KRX gold spot products. v1 supports only `M04020000` and `M04020100`. | KO: 지원되는 금현물 상품 목록을 반환한다. v1은 `M04020000`, `M04020100`만 지원한다."""
            return tools.list_gold_products()

        @_mcp_tool
        def get_gold_quote_snapshot(code: str = "M04020000") -> dict[str, Any]:
            """EN: Return the latest KRX gold spot quote snapshot from `XC`. Only `M04020000` and `M04020100` are accepted; gold codes are not stock-normalized. | KO: `XC` 기반 금현물 현재가 스냅샷을 반환한다. `M04020000`, `M04020100`만 허용하며 주식 6자리 정규화를 적용하지 않는다."""
            return tools.get_gold_quote_snapshot(code)

        @_mcp_tool
        def get_gold_daily_prices(
            code: str = "M04020000",
            start_date: str | None = None,
            end_date: str | None = None,
        ) -> list[dict[str, Any]]:
            """EN: Return daily OHLCV rows for a KRX gold spot product using `TR_GLCHART`. | KO: `TR_GLCHART`로 금현물 일봉 OHLCV 데이터를 반환한다."""
            return tools.get_gold_daily_prices(code, start_date, end_date)

        @_mcp_tool
        def get_gold_intraday_prices(
            date: str,
            code: str = "M04020000",
            interval_minutes: int = 5,
        ) -> list[dict[str, Any]]:
            """EN: Return intraday OHLCV rows for a KRX gold spot product using `TR_GLCHART`. `date` accepts `YYYYMMDD` or `YYYY-MM-DD`. | KO: `TR_GLCHART`로 금현물 분봉 OHLCV 데이터를 반환한다. `date`는 `YYYYMMDD` 또는 `YYYY-MM-DD`를 받는다."""
            return tools.get_gold_intraday_prices(code, date, interval_minutes)

        @_mcp_tool
        def get_gold_order_book(code: str = "M04020000") -> dict[str, Any]:
            """EN: Return KRX gold spot order-book information from `XH`. | KO: `XH` 기반 금현물 호가 정보를 반환한다."""
            return tools.get_gold_order_book(code)

        @_mcp_tool
        def subscribe_gold_realtime_price(code: str = "M04020000") -> dict[str, object]:
            """EN: Subscribe to realtime KRX gold spot price updates from `XC`. This uses the separate gold RT OCX control and does not affect stock price subscriptions. | KO: 별도 gold RT OCX control로 `XC` 금현물 실시간 시세를 구독한다. 주식 가격 구독에는 영향을 주지 않는다."""
            return tools.subscribe_gold_realtime_price(code)

        @_mcp_tool
        def unsubscribe_gold_realtime_price(code: str = "M04020000") -> dict[str, object]:
            """EN: Unsubscribe from realtime KRX gold spot price updates from `XC`. | KO: `XC` 금현물 실시간 시세 구독을 해제한다."""
            return tools.unsubscribe_gold_realtime_price(code)

        @_mcp_tool
        def register_gold_price_alert(
            code: str,
            condition: str,
            threshold: float,
            window_minutes: int | None = None,
            message: str = "",
            httpCallback: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            """EN: Register a same-day realtime price alert for one KRX gold spot product. `condition` must be `climb`, `fall`, or `fastmove`; `fastmove` requires `window_minutes`. After a `fastmove` fires, additional ticks inside `window_minutes` are coalesced and the latest received price is evaluated for one trailing alert when the window ends. The runtime evaluates only `XC` gold events and owns only `XC` subscriptions. | KO: 금현물 상품에 대한 당일 한정 실시간 가격 알람을 등록한다. `condition`은 `climb`, `fall`, `fastmove` 중 하나이며 `fastmove`는 `window_minutes`가 필요하다. `fastmove` 발화 후 `window_minutes` 동안 추가 tick은 합쳐서 보관하고, 윈도우가 끝날 때 마지막 현재가로 trailing 알람을 1회 평가한다. gold runtime은 `XC` 이벤트만 평가하고 `XC` 구독만 소유한다."""
            return tools.register_gold_price_alert(code, condition, threshold, window_minutes, message, httpCallback)

        @_mcp_tool
        def list_gold_price_alerts() -> list[dict[str, Any]]:
            """EN: List active same-day gold price alerts. | KO: 현재 활성화된 당일 금현물 가격 알람 목록을 반환한다."""
            return tools.list_gold_price_alerts()

        @_mcp_tool
        def cancel_gold_price_alert(alert_id: str | None = None, code: str | None = None) -> dict[str, Any]:
            """EN: Cancel one gold price alert by `alert_id`, or all active alerts for a gold product by `code`. | KO: `alert_id`로 금현물 가격 알람 하나를 취소하거나, `code`로 해당 금현물 상품의 활성 알람을 모두 취소한다."""
            return tools.cancel_gold_price_alert(alert_id, code)

        @_mcp_tool
        def register_gold_price_callback(
            code: str,
            step: float,
            price_filter: str | None = None,
            httpCallback: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            """EN: Register a same-day realtime gold price step callback. The first valid `XC` tick sets the baseline; later moves by at least `step` queue one callback and reset the baseline. Body replacements support `{{code}}`, `{{name}}`, display `{{price}}`, raw `{{priceRaw}}`/`{{price_raw}}`, and `{{direction}}`. | KO: 당일 한정 금현물 step 변동 callback을 등록한다. 첫 유효 `XC` tick은 기준 가격만 설정하고, 이후 `step` 이상 움직이면 callback을 큐에 넣고 기준가를 갱신한다. body replacement는 `{{code}}`, `{{name}}`, 표시용 `{{price}}`, 원본 `{{priceRaw}}`/`{{price_raw}}`, `{{direction}}`을 지원한다."""
            return tools.register_gold_price_callback(code, step, httpCallback, price_filter)

        @_mcp_tool
        def list_gold_price_callbacks() -> list[dict[str, Any]]:
            """EN: List active same-day gold price step callbacks. | KO: 현재 활성화된 당일 금현물 step 변동 callback 목록을 반환한다."""
            return tools.list_gold_price_callbacks()

        @_mcp_tool
        def cancel_gold_price_callback(
            gold_price_callback_id: str | None = None,
            code: str | None = None,
        ) -> dict[str, Any]:
            """EN: Cancel one gold price step callback by `gold_price_callback_id`, or every active callback for one gold product by `code`. | KO: `gold_price_callback_id`로 금현물 step callback 하나를 취소하거나, `code`로 해당 금현물 상품의 활성 callback을 모두 취소한다."""
            return tools.cancel_gold_price_callback(gold_price_callback_id, code)

        @_mcp_tool
        def get_gold_account_balance(account_no: str) -> dict[str, Any]:
            """EN: Return KRX gold spot account cash/valuation summary and the single expected balance row using one `SABA835Q1` request with product code `70`. `balance` is null when no gold product is held; multiple balance rows are treated as an error. | KO: `SABA835Q1` 상품코드 `70` 한 번으로 금현물 계좌 예수금/평가 요약과 단일 잔고 행을 함께 반환한다. 보유 금 상품이 없으면 `balance`는 null이며, 여러 잔고 행은 오류로 본다."""
            return tools.get_gold_account_balance(account_no)

        @_mcp_tool
        def place_gold_order(
            account_no: str,
            code: str,
            side: str,
            quantity: int,
            price: int,
        ) -> dict[str, Any]:
            """EN: Place a KRX gold spot limit order using `SABA871U1` product code `70`. Market orders are not supported or exposed. `price` is required and must be positive. Before calling this tool, always present the final planned arguments/configuration to the user and receive explicit confirmation for that exact call. | KO: `SABA871U1` 상품코드 `70`으로 금현물 지정가 주문을 실행한다. 시장가는 지원하거나 노출하지 않는다. `price`는 필수이며 양수여야 한다. 이 tool을 실제 호출하기 전에는 매번 최종 구성값을 사용자에게 보여주고 해당 호출에 대한 명시 확인을 받은 뒤 호출한다."""
            return tools.place_gold_order(account_no, code, side, quantity, price)

        @_mcp_tool
        def modify_gold_order(
            account_no: str,
            code: str,
            side: str,
            quantity: int,
            original_order_id: str,
            price: int,
        ) -> dict[str, Any]:
            """EN: Modify an existing KRX gold spot limit order using `SABA871U1` product code `70`. Market orders are not supported or exposed. `price` is required and must be positive. Before calling this tool, always present the final planned arguments/configuration to the user and receive explicit confirmation for that exact call. | KO: `SABA871U1` 상품코드 `70`으로 기존 금현물 지정가 주문을 정정한다. 시장가는 지원하거나 노출하지 않는다. `price`는 필수이며 양수여야 한다. 이 tool을 실제 호출하기 전에는 매번 최종 구성값을 사용자에게 보여주고 해당 호출에 대한 명시 확인을 받은 뒤 호출한다."""
            return tools.modify_gold_order(account_no, code, side, quantity, original_order_id, price)

        @_mcp_tool
        def cancel_gold_order(
            account_no: str,
            code: str,
            side: str,
            quantity: int,
            original_order_id: str,
        ) -> dict[str, Any]:
            """EN: Cancel an existing KRX gold spot order using `SABA871U1` product code `70`. Market orders are not supported or exposed; cancel sends price `0` internally as required by the TR. Before calling this tool, always present the final planned arguments/configuration to the user and receive explicit confirmation for that exact call. | KO: `SABA871U1` 상품코드 `70`으로 기존 금현물 주문을 취소한다. 시장가는 지원하거나 노출하지 않으며, 취소는 TR 요구에 따라 내부적으로 가격 `0`을 전송한다. 이 tool을 실제 호출하기 전에는 매번 최종 구성값을 사용자에게 보여주고 해당 호출에 대한 명시 확인을 받은 뒤 호출한다."""
            return tools.cancel_gold_order(account_no, code, side, quantity, original_order_id)

        @_mcp_tool
        def place_order(
            account_no: str,
            code: str,
            side: str,
            quantity: int,
            price: int | None = None,
            order_type: str = "limit",
        ) -> dict[str, Any]:
            """EN: Place a cash buy or sell order when live orders are enabled. New credit orders are not supported. Automatic session carryover is managed separately with register_order_carryover. Before calling this tool, always present the final planned arguments/configuration to the user and receive explicit confirmation for that exact call. | KO: 실주문이 허용된 경우 현금 매수 또는 매도 주문을 실행한다. 신규 신용 주문은 지원하지 않는다. 세션 이월 자동화는 register_order_carryover로 별도 관리한다. 이 tool을 실제 호출하기 전에는 매번 최종 구성값을 사용자에게 보여주고 해당 호출에 대한 명시 확인을 받은 뒤 호출한다."""
            return tools.place_order(
                account_no,
                code,
                side,
                quantity,
                price,
                order_type,
            )

        @_mcp_tool
        def modify_order(
            account_no: str,
            code: str,
            side: str,
            quantity: int,
            original_order_id: str,
            price: int | None = None,
            order_type: str = "limit",
            credit_trade_type: str | None = None,
        ) -> dict[str, Any]:
            """EN: Modify an existing order when live orders are enabled. Optional credit_trade_type is only for preserving or clearing an already-existing credit order; it does not enable new credit orders. Before calling this tool, always present the final planned arguments/configuration to the user and receive explicit confirmation for that exact call. | KO: 실주문이 허용된 경우 기존 주문을 정정한다. credit_trade_type은 이미 존재하는 신용 주문의 구분을 유지하거나 비신용으로 정정할 때만 선택적으로 사용하며, 신규 신용 주문을 여는 용도로는 쓸 수 없다. 이 tool을 실제 호출하기 전에는 매번 최종 구성값을 사용자에게 보여주고 해당 호출에 대한 명시 확인을 받은 뒤 호출한다."""
            return tools.modify_order(account_no, code, side, quantity, original_order_id, price, order_type, credit_trade_type)

        @_mcp_tool
        def cancel_order(
            account_no: str,
            code: str,
            side: str,
            quantity: int,
            original_order_id: str,
            credit_trade_type: str | None = None,
        ) -> dict[str, Any]:
            """EN: Cancel an existing order when live orders are enabled. Optional credit_trade_type is only for cancelling an already-existing credit order; it does not enable new credit orders. Before calling this tool, always present the final planned arguments/configuration to the user and receive explicit confirmation for that exact call. | KO: 실주문이 허용된 경우 기존 주문을 취소한다. credit_trade_type은 이미 존재하는 신용 주문을 취소할 때만 선택적으로 사용하며, 신규 신용 주문을 여는 용도로는 쓸 수 없다. 이 tool을 실제 호출하기 전에는 매번 최종 구성값을 사용자에게 보여주고 해당 호출에 대한 명시 확인을 받은 뒤 호출한다."""
            return tools.cancel_order(account_no, code, side, quantity, original_order_id, credit_trade_type)

        ops_log(LogSource.STARTUP_MCP, "MCP tool registration complete")
        setattr(mcp, "_homestock_tools", tools)
        return mcp
    except Exception:
        _close_tools(tools)
        raise


def main() -> None:
    started_at = time.perf_counter()
    ops_log(LogSource.STARTUP_SERVER, "main entered")
    ops_log(LogSource.STARTUP_SERVER, "loading Settings.from_env()")
    try:
        settings = Settings.from_env()
    except Exception as exc:
        ops_log(LogSource.STARTUP_SERVER, f"Settings.from_env() failed: {exc.__class__.__name__}: {exc}")
        write_crash_log(
            role="main",
            source="server.settings",
            message="Settings.from_env() failed",
            exc=exc,
        )
        raise
    ops_log(LogSource.STARTUP_SERVER, "creating MCP server")
    try:
        mcp = create_mcp_server(settings)
    except Exception as exc:
        ops_log(LogSource.STARTUP_SERVER, f"MCP server creation failed: {exc.__class__.__name__}: {exc}")
        write_crash_log(
            role="main",
            source="server.create_mcp_server",
            message="MCP server creation failed",
            exc=exc,
            log_dir=settings.scripter_log_dir or ".runtime/scripter",
            extra={"settings": _settings_summary(settings)},
        )
        raise
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    _log_process_context()
    ops_log(LogSource.STARTUP_SERVER, f"settings loaded: {_settings_summary(settings)}")
    ops_log(LogSource.STARTUP_SERVER, f"MCP server creation complete elapsed_ms={elapsed_ms}")
    ops_log(LogSource.STARTUP_SERVER, "starting FastMCP run loop transport=streamable-http")
    try:
        mcp.run(transport="streamable-http")
    except Exception as exc:
        ops_log(LogSource.STARTUP_SERVER, f"FastMCP run loop failed: {exc.__class__.__name__}: {exc}")
        write_crash_log(
            role="main",
            source="server.run_loop",
            message="FastMCP run loop failed",
            exc=exc,
            log_dir=settings.scripter_log_dir or ".runtime/scripter",
        )
        raise
    finally:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        ops_log(LogSource.STARTUP_SERVER, f"main exiting elapsed_ms={elapsed_ms}")
        close_mcp_server(mcp)


if __name__ == "__main__":
    main()
