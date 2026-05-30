import os
import unittest
from unittest.mock import Mock, patch

import homestock.backend as backend
import homestock.server as server
from homestock.config import Settings
from homestock.indi.base import IndiClient
from homestock.server import create_mcp_server


def create_test_mcp_server(settings: Settings | None = None):
    with patch.object(server, "create_tools", return_value=Mock()):
        return create_mcp_server(settings or Settings())


class ConfigAndServerTest(unittest.TestCase):
    def test_settings_defaults_to_mock_backend_and_safe_orders(self):
        settings = Settings()

        self.assertEqual(settings.backend, "mock")
        self.assertFalse(settings.allow_live_orders)
        self.assertTrue(settings.use_threaded_real_client)
        self.assertEqual(settings.host, "0.0.0.0")
        self.assertEqual(settings.port, 8000)
        self.assertIsNone(settings.scripter_log_dir)
        self.assertEqual(settings.scripter_log_retention_days, 5)
        self.assertEqual(settings.scripter_log_level, "info")

    def test_settings_preserves_positional_threaded_client_argument(self):
        settings = Settings("mock", False, "0.0.0.0", 8000, None, False)

        self.assertFalse(settings.use_threaded_real_client)
        self.assertIsNone(settings.holding_alert_config_path)

    def test_holding_alert_indi_methods_do_not_expand_required_backend_surface(self):
        abstract_methods = set(IndiClient.__abstractmethods__)

        self.assertNotIn("get_intraday_prices", abstract_methods)
        self.assertNotIn("get_sector_index_prices", abstract_methods)
        self.assertNotIn("get_stock_sector_profile", abstract_methods)
        self.assertNotIn("get_cash_order_book_snapshot", abstract_methods)

    def test_settings_can_disable_threaded_real_client_from_env(self):
        with patch.dict(os.environ, {"INDI_BACKEND": "mock", "HOMESTOCK_USE_THREADED_REAL_CLIENT": "false"}):
            settings = Settings.from_env()

        self.assertFalse(settings.use_threaded_real_client)

    def test_settings_reads_scripter_log_options_from_env(self):
        with patch.dict(
            os.environ,
            {
                "INDI_BACKEND": "mock",
                "HOMESTOCK_SCRIPTER_LOG_DIR": "H:\\logs\\homestock-scripter",
                "HOMESTOCK_SCRIPTER_LOG_RETENTION_DAYS": "7",
                "HOMESTOCK_SCRIPTER_LOG_LEVEL": "DEBUG",
            },
        ):
            settings = Settings.from_env()

        self.assertEqual(settings.scripter_log_dir, "H:\\logs\\homestock-scripter")
        self.assertEqual(settings.scripter_log_retention_days, 7)
        self.assertEqual(settings.scripter_log_level, "debug")

    def test_mcp_server_uses_streamable_http_defaults(self):
        server = create_test_mcp_server(Settings())

        self.assertEqual(server.name, "homestock")
        self.assertEqual(server.settings.streamable_http_path, "/mcp")
        self.assertEqual(server.settings.port, 8000)

    def test_mcp_tool_calls_are_logged_with_debug_payloads(self):
        tools = Mock()
        tools.health_check.return_value = {"ok": True}

        with (
            patch.object(server, "create_tools", return_value=tools),
            patch.object(server, "ops_log") as ops_log,
        ):
            mcp = create_mcp_server(Settings())
            tool = next(item for item in mcp._tool_manager.list_tools() if item.name == "health_check")
            result = tool.fn()

        self.assertEqual(result, {"ok": True})
        self.assertTrue(any(call.args[0] == server.LogSource.MCP_TOOL for call in ops_log.call_args_list))
        self.assertTrue(any("call begin tool=health_check" in call.args[1] for call in ops_log.call_args_list))
        self.assertTrue(any("call success tool=health_check" in call.args[1] for call in ops_log.call_args_list))
        info_calls = [
            call
            for call in ops_log.call_args_list
            if call.args[0] == server.LogSource.MCP_TOOL and call.kwargs.get("level", "info") == "info"
        ]
        self.assertTrue(all("payload" not in call.kwargs for call in info_calls))
        debug_calls = [call for call in ops_log.call_args_list if call.kwargs.get("level") == "debug"]
        self.assertTrue(any("call args tool=health_check" in call.args[1] for call in debug_calls))
        self.assertTrue(any(call.kwargs.get("payload", {}).get("result") == {"ok": True} for call in debug_calls))
        tools.health_check.assert_called_once_with()

    def test_holding_alert_mcp_tools_are_registered(self):
        mcp = create_test_mcp_server(Settings())
        tool_names = {item.name for item in mcp._tool_manager.list_tools()}

        for tool_name in {
            "get_intraday_prices",
            "get_market_index_prices",
            "get_sector_index_prices",
            "get_stock_sector_profile",
            "get_stock_technical_indicators_daily",
            "get_stock_technical_indicators_weekly",
            "get_stock_technical_indicators_intraday",
            "get_stock_chart_patterns_daily",
            "get_stock_chart_patterns_weekly",
            "get_stock_chart_patterns_intraday",
            "get_stock_market_environment_indicators",
            "get_stock_technical_analysis_bundle",
            "get_stock_technical_analysis_bundle_live",
            "register_holding_alert_runner",
            "list_holding_alert_runners",
            "cancel_holding_alert_runner",
        }:
            with self.subTest(tool_name=tool_name):
                self.assertIn(tool_name, tool_names)

        for tool_name in {
            "get_cash_order_book_snapshot",
            "get_stock_weekly_prices",
            "get_stock_decision_indicator_context",
            "get_technical_indicators",
            "get_stock_chart_pattern_candidates",
            "get_stock_analysis_context",
            "refresh_decision_baselines",
            "get_decision_baseline_cache",
            "get_alert_indicator_context",
            "calculate_trade_size",
            "run_holding_alert_scan",
            "run_alert_validation",
        }:
            with self.subTest(tool_name=tool_name):
                self.assertNotIn(tool_name, tool_names)

    def test_exposed_holding_alert_tool_descriptions_are_detailed(self):
        mcp = create_test_mcp_server(Settings())
        tools_by_name = {item.name: item for item in mcp._tool_manager.list_tools()}

        expectations = {
            "get_intraday_prices": ["Arguments", "YYYYMMDD", "HHMMSS", "does not start a holding-alert runner"],
            "get_market_index_prices": ["Supported keys", "kospi200", "usdkrw", "read-only TR-backed"],
            "get_sector_index_prices": ["sector_code", "currently only daily `D`", "Unsupported interval"],
            "get_stock_sector_profile": ["sector_code", "source", "unavailable", "read-only"],
            "get_stock_technical_indicators_daily": ["Backtest contract", "sma5", "volume_ratio20", "Does not use current quote snapshots"],
            "get_stock_technical_indicators_weekly": ["weekly OHLCV", "ISO week", "partial week", "future bars"],
            "get_stock_technical_indicators_intraday": ["as_of_time", "VWAP", "session_volume_ratio", "same-day lookahead"],
            "get_stock_chart_patterns_daily": ["range_breakout", "prior_20bar_volume_ratio", "double_bottom", "end_date"],
            "get_stock_chart_patterns_weekly": ["weekly stock OHLCV", "partial", "future bars", "window_days"],
            "get_stock_chart_patterns_intraday": ["lookback_bars", "as_of_time", "bars after that time are excluded"],
            "get_stock_market_environment_indicators": ["indirect market-environment", "completed_daily_end_date", "current quote snapshots", "sector context"],
            "get_stock_technical_analysis_bundle": ["backtest-safe technical-analysis bundle", "price_bars", "technical_indicators", "suitable_for_backtesting"],
            "get_stock_technical_analysis_bundle_live": ["live_not_backtest_safe", "live_context", "quote snapshot", "does not subscribe"],
            "register_holding_alert_runner": ["accountNo", "heldCode", "wannaCode", "never places orders", "runner_id", "warnings", "already registered"],
            "list_holding_alert_runners": ["Read-only management tool", "last_scan_at", "cancel_holding_alert_runner", "heldCode", "wannaCode"],
            "cancel_holding_alert_runner": ["Releases only realtime price subscriptions", "Does not cancel normal orders", "Never places an order"],
        }

        for tool_name, snippets in expectations.items():
            with self.subTest(tool_name=tool_name):
                description = tools_by_name[tool_name].description or ""
                self.assertGreaterEqual(len(description), 500)
                for snippet in snippets:
                    self.assertIn(snippet, description)

    def test_real_backend_uses_threaded_indi_client(self):
        expected_client = object()

        with patch.object(backend, "ThreadedIndiClient", return_value=expected_client) as threaded_client:
            result = backend.create_indi_client(Settings(backend="real"))

        self.assertIs(result, expected_client)
        threaded_client.assert_called_once_with(backend.RealIndiClient)

    def test_real_backend_can_disable_threaded_indi_client(self):
        expected_client = object()

        with (
            patch.object(backend, "ThreadedIndiClient") as threaded_client,
            patch.object(backend, "RealIndiClient", return_value=expected_client) as real_client,
        ):
            result = backend.create_indi_client(
                Settings(backend="real", use_threaded_real_client=False)
            )

        self.assertIs(result, expected_client)
        threaded_client.assert_not_called()
        real_client.assert_called_once_with()

    def test_create_tools_closes_client_when_tool_initialization_fails(self):
        client = Mock()

        with (
            patch.object(server, "create_indi_client", return_value=client),
            patch.object(server, "HomestockTools", side_effect=RuntimeError("startup failed")),
        ):
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                server.create_tools(Settings())

        client.close.assert_called_once_with()

    def test_create_tools_fails_hard_when_scripter_start_fails(self):
        client = Mock()
        scripter = Mock()
        scripter.start.side_effect = RuntimeError("scripter start failed")

        with (
            patch.object(server, "create_indi_client", return_value=client) as create_client,
            patch.object(server, "HomestockTools") as tools_cls,
        ):
            with self.assertRaisesRegex(RuntimeError, "scripter start failed"):
                server.create_tools(Settings(), scripter=scripter)

        scripter.start.assert_called_once_with()
        create_client.assert_not_called()
        tools_cls.assert_not_called()
        client.close.assert_not_called()

    def test_create_tools_passes_scripter_log_settings_to_default_scripter(self):
        client = Mock()
        scripter = Mock()
        created_tools = object()

        with (
            patch.object(server, "create_indi_client", return_value=client),
            patch.object(server, "IsolateProcessScripter", return_value=scripter) as create_scripter,
            patch.object(server, "HomestockTools", return_value=created_tools),
        ):
            result = server.create_tools(
                Settings(
                    scripter_log_dir="H:\\logs\\homestock-scripter",
                    scripter_log_retention_days=7,
                    scripter_log_level="debug",
                )
            )

        self.assertIs(result, created_tools)
        create_scripter.assert_called_once_with(
            log_dir="H:\\logs\\homestock-scripter",
            retention_days=7,
            log_level="debug",
        )
        scripter.start.assert_called_once_with()

    def test_create_mcp_server_closes_tools_when_mcp_construction_fails(self):
        tools = Mock()

        with (
            patch.object(server, "create_tools", return_value=tools),
            patch("mcp.server.fastmcp.FastMCP", side_effect=RuntimeError("mcp construction failed")),
        ):
            with self.assertRaisesRegex(RuntimeError, "mcp construction failed"):
                server.create_mcp_server(Settings())

        tools.close.assert_called_once_with()

    def test_main_closes_mcp_tools_after_run_returns(self):
        tools = Mock()
        mcp = Mock()
        mcp._homestock_tools = tools
        mcp.run.return_value = None

        with (
            patch.object(server.Settings, "from_env", return_value=Settings()),
            patch.object(server, "create_mcp_server", return_value=mcp),
        ):
            server.main()

        tools.close.assert_called_once_with()

    def test_main_closes_mcp_tools_after_run_raises(self):
        tools = Mock()
        mcp = Mock()
        mcp._homestock_tools = tools
        mcp.run.side_effect = RuntimeError("run failed")

        with (
            patch.object(server.Settings, "from_env", return_value=Settings()),
            patch.object(server, "create_mcp_server", return_value=mcp),
        ):
            with self.assertRaisesRegex(RuntimeError, "run failed"):
                server.main()

        tools.close.assert_called_once_with()

    def test_subscribe_news_docstring_describes_http_callback_and_replacements(self):
        server = create_test_mcp_server(Settings())
        tool = next(item for item in server._tool_manager.list_tools() if item.name == "subscribe_news")
        description = tool.description or ""

        self.assertIn("httpCallback", description)
        self.assertIn("bodyFormat", description)
        self.assertIn("subscription_id", description)
        self.assertIn("unsubscribe_news", description)
        self.assertIn("`A`: info", description)
        self.assertIn("`Y`: yonhap", description)
        self.assertIn("`F`: market_commentary", description)
        self.assertIn("`A`: 인포", description)
        self.assertIn("`Y`: 연합", description)
        self.assertIn("`F`: 시황", description)
        self.assertIn("{{news_type}}", description)
        self.assertIn("{{news_type_label}}", description)
        self.assertIn("{{delete_flag_label}}", description)

    def test_list_market_flow_news_tool_is_registered_with_time_filters(self):
        server = create_test_mcp_server(Settings())
        tool_names = {item.name for item in server._tool_manager.list_tools()}
        tool = next(item for item in server._tool_manager.list_tools() if item.name == "list_market_flow_news")
        description = tool.description or ""

        self.assertIn("market-flow", description)
        self.assertIn("from_time", description)
        self.assertIn("to_time", description)
        self.assertIn("TR_3102_CT", description)
        self.assertIn("latest 20", description)
        self.assertIn("09", description)
        self.assertIn("article_id", description)
        self.assertNotIn("list_news_by_type", tool_names)

    def test_get_market_investor_flow_intraday_tool_is_registered(self):
        server = create_test_mcp_server(Settings())
        tool = next(
            item for item in server._tool_manager.list_tools() if item.name == "get_market_investor_flow_intraday"
        )
        description = tool.description or ""

        self.assertIn("TR_1202_B", description)
        self.assertIn("KOSPI", description)
        self.assertIn("조회방법=1", description)
        self.assertIn("시간간격=010", description)
        self.assertIn("Each row includes `time`", description)
        self.assertIn("include_institution_breakdown", description)
        self.assertIn("buy/sell/net", description)

    def test_subscribe_disclosure_docstring_describes_replacements_and_missing_title_rule(self):
        server = create_test_mcp_server(Settings())
        tool = next(item for item in server._tool_manager.list_tools() if item.name == "subscribe_disclosure")
        description = tool.description or ""

        self.assertIn("httpCallback", description)
        self.assertIn("subscription_id", description)
        self.assertIn("unsubscribe_disclosure", description)
        self.assertIn("{{disclosure_type}}", description)
        self.assertIn("{{disclosure_type_label}}", description)
        self.assertIn("{{title}}", description)
        self.assertIn("제목 없음", description)

    def test_register_system_callback_docstring_describes_replacements_and_response_flow(self):
        server = create_test_mcp_server(Settings())
        tool = next(item for item in server._tool_manager.list_tools() if item.name == "register_system_callback")
        description = tool.description or ""

        self.assertIn("httpCallback", description)
        self.assertIn("bodyFormat", description)
        self.assertIn("system_callback_id", description)
        self.assertIn("unregister_system_callback", description)
        self.assertIn("list_system_callbacks", description)
        self.assertIn("{{tag}}", description)
        self.assertIn("{{name}}", description)
        self.assertIn("{{callstack}}", description)
        self.assertIn("{{occurred_at}}", description)

    def test_live_order_tools_require_explicit_user_confirmation_in_descriptions(self):
        server = create_test_mcp_server(Settings())
        tools_by_name = {item.name: item for item in server._tool_manager.list_tools()}

        for tool_name in [
            "place_order",
            "modify_order",
            "cancel_order",
            "register_fall_safe",
            "place_gold_order",
            "modify_gold_order",
            "cancel_gold_order",
        ]:
            with self.subTest(tool_name=tool_name):
                description = tools_by_name[tool_name].description or ""

                self.assertIn("final planned arguments/configuration", description)
                self.assertIn("explicit confirmation", description)
                self.assertIn("매번 최종 구성값", description)
                self.assertIn("명시 확인", description)

    def test_gold_mcp_tools_are_registered(self):
        mcp = create_test_mcp_server(Settings())
        tool_names = {item.name for item in mcp._tool_manager.list_tools()}

        for tool_name in {
            "list_gold_products",
            "get_gold_quote_snapshot",
            "get_gold_daily_prices",
            "get_gold_intraday_prices",
            "get_gold_order_book",
            "subscribe_gold_realtime_price",
            "unsubscribe_gold_realtime_price",
            "register_gold_price_alert",
            "list_gold_price_alerts",
            "cancel_gold_price_alert",
            "register_gold_price_callback",
            "list_gold_price_callbacks",
            "cancel_gold_price_callback",
            "get_gold_account_balance",
            "place_gold_order",
            "modify_gold_order",
            "cancel_gold_order",
        }:
            with self.subTest(tool_name=tool_name):
                self.assertIn(tool_name, tool_names)
        self.assertNotIn("get_gold_account_summary", tool_names)
        self.assertNotIn("get_gold_balance", tool_names)


if __name__ == "__main__":
    unittest.main()
