import io
import json
import logging
import queue
import threading
import unittest
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from homestock.scripter import (
    HeartbeatSilenceMonitor,
    HeartbeatLockMonitor,
    InProcessScripter,
    IsolateProcessScripter,
    MockScripter,
    StdoutScripter,
    HEARTBEAT_LOCK_EVENT_TYPE,
    HEARTBEAT_LOCK_REPEAT_INTERVAL_SECONDS,
    HEARTBEAT_REVIVED_EVENT_TYPE,
    HEARTBEAT_SILENT_ALERT_LIMIT,
    HEARTBEAT_SILENT_EVENT_TYPE,
    HEARTBEAT_SILENT_REPEAT_INTERVAL_SECONDS,
    HEARTBEAT_SILENT_TIMEOUT_SECONDS,
    HEARTBEAT_LOCK_TIMEOUT_SECONDS,
    HEARTBEAT_STATUS_LOG_INTERVAL_SECONDS,
    HEARTBEAT_UNLOCK_EVENT_TYPE,
    SCRIPTER_DEBUG_WRITER_QUEUE_MULTIPLIER,
    SCRIPTER_QUEUE_OVERFLOW_EVENT_TYPE,
    ScripterLogConfig,
    ScripterEvent,
    configure_logger,
    is_heartbeat_silent,
    is_heartbeat_locked,
    write_crash_log,
    _ScripterRuntime,
)
from homestock.models import HttpCallbackSpec
from homestock.ops_log import LogSource, clear_ops_log_sink, ops_log
from homestock.redaction import redact_for_output
from homestock.scripter_child import (
    _utf8_stdin_text_stream,
    main as scripter_child_main,
    process_scripter_event_line,
    run_scripter_child,
)
from homestock.webhook import CallbackDispatcher


class ScripterTest(unittest.TestCase):
    def test_mock_scripter_records_events_heartbeats_and_callback_configs(self):
        scripter = MockScripter()
        callbacks = [
            {
                "system_callback_id": "sys_1",
                "httpCallback": {"method": "POST", "url": "https://example.test/hook"},
            }
        ]

        scripter.start()
        scripter.log("info", "test", "thing happened", {"value": 1})
        scripter.error("test", "thing failed", callstack="stack", payload={"value": 2})
        scripter.system_callback("thing_happened", "thing happened", {"value": 3})
        scripter.heartbeat("indi_monitor", {"ok": True})
        scripter.configure_system_callbacks(callbacks)
        callbacks[0]["system_callback_id"] = "changed"

        self.assertEqual(scripter.logs[0].kind, "log")
        self.assertEqual(scripter.logs[0].event_type, "test")
        self.assertEqual(scripter.logs[0].payload["message"], "thing happened")
        self.assertEqual(scripter.logs[0].payload["payload"], {"value": 1})
        self.assertEqual(scripter.errors[0].kind, "error")
        self.assertEqual(scripter.errors[0].payload["callstack"], "stack")
        self.assertEqual(scripter.system_callbacks[0].kind, "system_callback")
        self.assertEqual(scripter.system_callbacks[0].event_type, "thing_happened")
        self.assertEqual(scripter.system_callbacks[0].payload["message"], "thing happened")
        self.assertEqual(scripter.system_callbacks[0].payload["details"], {"value": 3})
        self.assertEqual(scripter.heartbeats[0].kind, "heartbeat")
        self.assertEqual(scripter.heartbeats[0].event_type, "indi_monitor")
        self.assertEqual(scripter.heartbeats[0].payload, {"ok": True})
        self.assertEqual(scripter.system_callback_configs[0][0]["system_callback_id"], "sys_1")

    def test_ops_log_routes_to_started_scripter(self):
        scripter = MockScripter()
        try:
            scripter.start()
            ops_log(LogSource.STARTUP_SERVER, "startup message", payload={"phase": "boot"})

            self.assertEqual(
                scripter.ops_logs,
                [(LogSource.STARTUP_SERVER, "startup message", "info", {"phase": "boot"})],
            )
        finally:
            scripter.close()

    def test_ops_log_stdout_fallback_redacts_account_password_only(self):
        clear_ops_log_sink()
        stream = io.StringIO()

        with patch("sys.stdout", stream):
            ops_log(
                LogSource.STARTUP_SERVER,
                "url=https://hooks.example.test/system Authorization: Bearer fallback-secret "
                "account_password=trade-secret",
            )

        text = stream.getvalue()
        self.assertIn("[homestock.ops]", text)
        self.assertIn(f"[{LogSource.STARTUP_SERVER}]", text)
        self.assertIn("hooks.example.test", text)
        self.assertIn("fallback-secret", text)
        self.assertIn("account_password=<redacted>", text)
        self.assertNotIn("trade-secret", text)

    def test_ops_log_stdout_fallback_uses_source_verbatim(self):
        clear_ops_log_sink()
        stream = io.StringIO()

        with patch("sys.stdout", stream):
            ops_log(LogSource.WEBHOOK, "callback dispatch queued callback_id=sys_1")
            ops_log(LogSource.MANAGE, "GiExpertMain.exe monitor failed: RuntimeError: boom")

        text = stream.getvalue()
        self.assertIn("[homestock.ops]", text)
        self.assertIn(f"[{LogSource.WEBHOOK}]", text)
        self.assertIn(f"[{LogSource.MANAGE}]", text)
        self.assertNotIn("startup.webhook", text)
        self.assertNotIn("startup.manage", text)

    def test_ops_log_normalizes_legacy_operational_sources(self):
        clear_ops_log_sink()
        stream = io.StringIO()

        with patch("sys.stdout", stream):
            ops_log("startup.webhook", "callback dispatch queued callback_id=sys_1")
            ops_log("startup.indi.rt", "RT event queued rt_type=N0")
            ops_log("startup.runtime.rt", "RT event received rt_type=N0")
            ops_log("startup.runtime.state", "runtime state loaded")

        text = stream.getvalue()
        self.assertIn(f"[{LogSource.WEBHOOK}]", text)
        self.assertIn(f"[{LogSource.RT_INDI}]", text)
        self.assertIn(f"[{LogSource.RT_RUNTIME}]", text)
        self.assertIn(f"[{LogSource.STARTUP_RUNTIME}]", text)
        self.assertNotIn("startup.webhook", text)
        self.assertNotIn("startup.indi.rt", text)
        self.assertNotIn("startup.runtime.rt", text)
        self.assertNotIn("startup.runtime.state", text)

    def test_stdout_scripter_writes_jsonl_and_redacts_account_password_only(self):
        stream = io.StringIO()
        scripter = StdoutScripter(stream)

        scripter.configure_system_callbacks(
            [
                {
                    "system_callback_id": "sys_1",
                    "httpCallback": {
                        "method": "POST",
                        "url": "https://hooks.example.test/system?token=query-secret",
                        "headers": {
                            "Authorization": "Bearer header-secret",
                            "X-Webhook-Secret": "header-secret",
                        },
                        "body": {
                            "visible": "kept",
                            "secret": "body-secret",
                            "accountPassword": "trade-secret",
                        },
                    },
                }
            ]
        )

        line = stream.getvalue().strip()
        payload = json.loads(line)
        callback = payload["payload"]["callbacks"][0]["httpCallback"]

        self.assertEqual(payload["kind"], "config")
        self.assertEqual(payload["event_type"], "system_callbacks")
        self.assertEqual(callback["url"], "https://hooks.example.test/system?token=query-secret")
        self.assertEqual(callback["headers"]["Authorization"], "Bearer header-secret")
        self.assertEqual(callback["headers"]["X-Webhook-Secret"], "header-secret")
        self.assertEqual(callback["body"]["secret"], "body-secret")
        self.assertEqual(callback["body"]["accountPassword"], "<redacted>")
        self.assertEqual(callback["body"]["visible"], "kept")
        self.assertIn("query-secret", line)
        self.assertIn("header-secret", line)
        self.assertIn("body-secret", line)
        self.assertIn("hooks.example.test", line)
        self.assertNotIn("trade-secret", line)

    def test_redact_for_output_redacts_account_password_free_form_strings(self):
        redacted = redact_for_output(
            {
                "message": (
                    "Authorization: Bearer header-secret https://hooks.example.test/hook?token=query-secret "
                    "account_password=trade-secret"
                ),
                "details": {
                    "error": "token=inline-secret",
                    "bearer": "Bearer adjacent-secret",
                    "command": ["tool", "--password", "cli-password", "--token", "cli-secret", "--visible", "kept"],
                    "callback": "https://hooks.example.test/other?token=other-secret",
                    "upper_callback": "HTTPS://hooks.example.test/upper?token=upper-secret",
                    "accountPassword": "nested-password",
                    "visible": "kept",
                },
            }
        )

        text = json.dumps(redacted, ensure_ascii=False)
        self.assertIn("Authorization: Bearer header-secret", text)
        self.assertIn("token=inline-secret", text)
        self.assertIn("https://hooks.example.test/hook?token=query-secret", text)
        self.assertIn("account_password=<redacted>", text)
        self.assertIn("kept", text)
        self.assertIn("header-secret", text)
        self.assertIn("hooks.example.test", text)
        self.assertIn("query-secret", text)
        self.assertIn("inline-secret", text)
        self.assertIn("adjacent-secret", text)
        self.assertIn("cli-secret", text)
        self.assertNotIn("cli-password", text)
        self.assertNotIn("nested-password", text)
        self.assertNotIn("trade-secret", text)
        self.assertIn("other-secret", text)
        self.assertIn("upper-secret", text)

    def test_stdout_scripter_redacts_account_password_in_top_level_event_type(self):
        stream = io.StringIO()
        scripter = StdoutScripter(stream)
        scripter.start()
        scripter.system_callback(
            "token=event-secret account_password=event-password https://hooks.example.test/event",
            "sample message",
        )

        line = stream.getvalue().strip()
        self.assertIn("token=event-secret", line)
        self.assertIn("account_password=<redacted>", line)
        self.assertIn("hooks.example.test", line)
        self.assertNotIn("event-password", line)

    def test_callback_dispatcher_redacts_account_password_delivery_errors(self):
        stream = io.StringIO()
        logger = logging.getLogger("test.webhook.redaction")
        logger.handlers = []
        logger.propagate = False
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
        logger.addHandler(handler)
        dispatcher = CallbackDispatcher(logger=logger, max_attempts=1, retry_delay_seconds=0.0)
        try:
            with patch.object(
                dispatcher,
                "_send_once",
                return_value={
                    "delivered": False,
                    "error": (
                        "Authorization: Bearer logger-secret https://hooks.example.test/hook?token=query-secret "
                        "account_password=delivery-secret"
                    ),
                },
            ):
                dispatcher.dispatch(HttpCallbackSpec(method="POST", url="http://localhost:9000/hook"))
                self.assertTrue(dispatcher.wait_for_idle(1.0))
        finally:
            dispatcher.close(timeout=1.0)
            logger.removeHandler(handler)
            handler.close()
            logger.propagate = True
            logger.setLevel(logging.NOTSET)

        text = stream.getvalue()
        self.assertIn("<redacted-url>", text)
        self.assertIn("Authorization: Bearer logger-secret", text)
        self.assertIn("hooks.example.test", text)
        self.assertIn("query-secret", text)
        self.assertIn("account_password=<redacted>", text)
        self.assertNotIn("delivery-secret", text)

    def test_injected_scripter_logger_redacts_account_password_system_callback_event_type(self):
        stream = io.StringIO()
        logger = logging.getLogger("test.scripter.event_type.redaction")
        logger.handlers = []
        logger.propagate = False
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
        logger.addHandler(handler)
        scripter = InProcessScripter(logger=logger)
        try:
            scripter.configure_system_callbacks(
                [
                    {
                        "system_callback_id": "sys_1",
                        "httpCallback": {"method": "POST", "url": "http://localhost:9000/system"},
                    }
                ]
            )
            with patch("homestock.scripter.CallbackDispatcher.dispatch", autospec=True, return_value={"queued": True}):
                scripter.system_callback(
                    "token=event-secret account_password=event-password https://hooks.example.test/event",
                    "sample message",
                )
                self.assertTrue(scripter.wait_for_idle(1.0))
        finally:
            scripter.close()
            logger.removeHandler(handler)
            handler.close()
            logger.propagate = True
            logger.setLevel(logging.NOTSET)

        text = stream.getvalue()
        self.assertIn("token=event-secret", text)
        self.assertIn("hooks.example.test", text)
        self.assertIn("account_password=<redacted>", text)
        self.assertNotIn("event-password", text)

    def test_write_crash_log_redacts_account_password_output(self):
        with TemporaryDirectory() as tempdir:
            exc = RuntimeError("token=exception-secret account_password=exception-password https://hooks.example.test/crash")

            path = write_crash_log(
                role="main",
                source="test token=source-secret account_password=source-password https://hooks.example.test/source",
                message="failed url=https://hooks.example.test/system token=message-secret password=message-password",
                exc=exc,
                log_dir=tempdir,
                extra={
                    "url": "https://hooks.example.test/extra",
                    "secret": "extra-secret",
                    "accountPassword": "extra-password",
                    "visible": "kept",
                },
            )

            self.assertIsNotNone(path)
            text = Path(path).read_text(encoding="utf-8")
            self.assertIn("<redacted>", text)
            self.assertIn('"visible": "kept"', text)
            self.assertIn("hooks.example.test", text)
            self.assertIn("message-secret", text)
            self.assertIn("exception-secret", text)
            self.assertIn("extra-secret", text)
            self.assertIn("source-secret", text)
            self.assertNotIn("message-password", text)
            self.assertNotIn("exception-password", text)
            self.assertNotIn("extra-password", text)
            self.assertNotIn("source-password", text)

    def test_isolate_process_scripter_writes_jsonl_to_text_stream(self):
        stream = io.StringIO()
        scripter = IsolateProcessScripter(stream)
        scripter.start()

        scripter.system_callback(
            "sample_event",
            "sample message",
            {
                "answer": 42,
                "httpCallback": {
                    "url": "https://hooks.example.test/system?token=query-secret",
                    "secret": "body-secret",
                    "accountPassword": "trade-secret",
                },
            },
        )

        payload = json.loads(stream.getvalue().strip())
        self.assertEqual(payload["kind"], "system_callback")
        self.assertEqual(payload["event_type"], "sample_event")
        self.assertEqual(payload["payload"]["message"], "sample message")
        self.assertEqual(payload["payload"]["details"]["answer"], 42)
        self.assertEqual(
            payload["payload"]["details"]["httpCallback"]["url"],
            "https://hooks.example.test/system?token=query-secret",
        )
        self.assertEqual(payload["payload"]["details"]["httpCallback"]["secret"], "body-secret")
        self.assertEqual(payload["payload"]["details"]["httpCallback"]["accountPassword"], "<redacted>")
        self.assertIn("hooks.example.test", stream.getvalue())
        self.assertIn("query-secret", stream.getvalue())
        self.assertIn("body-secret", stream.getvalue())
        self.assertNotIn("trade-secret", stream.getvalue())

        scripter.error("sample_error", "sample failed", callstack="stack", payload={"value": 7})
        error_payload = json.loads(stream.getvalue().splitlines()[1])
        self.assertEqual(error_payload["kind"], "error")
        self.assertEqual(error_payload["event_type"], "sample_error")
        self.assertEqual(error_payload["payload"]["message"], "sample failed")
        self.assertEqual(error_payload["payload"]["callstack"], "stack")
        self.assertEqual(error_payload["payload"]["payload"], {"value": 7})

    def test_isolate_process_scripter_expands_writer_queue_in_debug_mode(self):
        scripter = IsolateProcessScripter(io.StringIO(), writer_queue_maxsize=7, log_level="debug")

        self.assertEqual(scripter._writer_queue_maxsize, 7 * SCRIPTER_DEBUG_WRITER_QUEUE_MULTIPLIER)

    def test_isolate_process_scripter_keeps_writer_queue_size_in_info_mode(self):
        scripter = IsolateProcessScripter(io.StringIO(), writer_queue_maxsize=7, log_level="info")

        self.assertEqual(scripter._writer_queue_maxsize, 7)

    def test_isolate_process_scripter_starts_child_when_sink_is_not_provided(self):
        class CapturingSink(io.StringIO):
            def __init__(self) -> None:
                super().__init__()
                self.close_called = False

            def close(self) -> None:
                self.close_called = True

        class FakeProcess:
            def __init__(self) -> None:
                self.stdin = CapturingSink()
                self.wait_timeout = None
                self.terminated = False

            def wait(self, timeout=None):
                self.wait_timeout = timeout
                return 0

            def terminate(self) -> None:
                self.terminated = True

            def poll(self):
                return None

        fake_process = FakeProcess()

        def fake_popen(command, **kwargs):
            ready_file = Path(command[command.index("--ready-file") + 1])
            ready_file.write_text("{}", encoding="utf-8")
            return fake_process

        with patch("homestock.scripter.subprocess.Popen", side_effect=fake_popen) as popen:
            scripter = IsolateProcessScripter(
                log_dir="H:\\logs\\homestock-scripter",
                retention_days=5,
                python_executable="py",
                close_timeout=0.5,
            )
            popen.assert_not_called()
            scripter.start()
            scripter.heartbeat("main_process", {"pid": 1004})
            scripter.close()

        command = popen.call_args.args[0]
        self.assertEqual(command[:3], ["py", "-m", "homestock.scripter_child"])
        self.assertIn("H:\\logs\\homestock-scripter", command)
        self.assertIn("--ready-file", command)
        self.assertEqual(popen.call_args.kwargs["stdin"], -1)
        self.assertTrue(popen.call_args.kwargs["text"])
        lines = fake_process.stdin.getvalue().splitlines()
        heartbeat = json.loads(lines[0])
        close = json.loads(lines[1])
        self.assertEqual(heartbeat["kind"], "heartbeat")
        self.assertEqual(heartbeat["event_type"], "main_process")
        self.assertEqual(heartbeat["payload"], {"pid": 1004})
        self.assertEqual(close["kind"], "control")
        self.assertEqual(close["event_type"], "close")
        self.assertEqual(fake_process.wait_timeout, 0.5)
        self.assertTrue(fake_process.stdin.close_called)
        self.assertFalse(fake_process.terminated)

    def test_isolate_process_scripter_fails_when_child_exits_before_ready(self):
        class FakeProcess:
            stdin = io.StringIO()

            def __init__(self) -> None:
                self.terminated = False

            def poll(self):
                return 1

            def terminate(self) -> None:
                self.terminated = True

        fake_process = FakeProcess()
        with patch("homestock.scripter.subprocess.Popen", return_value=fake_process):
            scripter = IsolateProcessScripter(start_timeout=0.1)

            with self.assertRaisesRegex(RuntimeError, "exited before ready"):
                scripter.start()

        self.assertFalse(fake_process.terminated)

    def test_isolate_process_scripter_fails_when_child_never_becomes_ready(self):
        class FakeProcess:
            stdin = io.StringIO()

            def __init__(self) -> None:
                self.terminated = False

            def poll(self):
                return None

            def terminate(self) -> None:
                self.terminated = True

        fake_process = FakeProcess()
        with patch("homestock.scripter.subprocess.Popen", return_value=fake_process):
            scripter = IsolateProcessScripter(start_timeout=0.1)

            with self.assertRaisesRegex(TimeoutError, "did not become ready"):
                scripter.start()

        self.assertTrue(fake_process.terminated)

    def test_isolate_process_writer_error_during_close_does_not_exit_process(self):
        class FailingSink(io.StringIO):
            def write(self, value: str) -> int:
                raise BrokenPipeError("closed during shutdown")

        scripter = IsolateProcessScripter(FailingSink())
        scripter._writer_queue = queue.Queue()
        scripter._closing = True
        writer = threading.Thread(target=scripter._run_writer)
        with patch("homestock.scripter.os._exit") as exit_process:
            writer.start()
            scripter._writer_queue.put("{}")
            writer.join(1.0)

        self.assertFalse(writer.is_alive())
        exit_process.assert_not_called()

    def test_isolate_process_writer_error_during_operation_is_fatal(self):
        class FailingSink(io.StringIO):
            def write(self, value: str) -> int:
                raise BrokenPipeError("child pipe closed")

        scripter = IsolateProcessScripter(FailingSink())
        scripter._writer_queue = queue.Queue()
        writer = threading.Thread(target=scripter._run_writer)
        with (
            patch("homestock.scripter.write_crash_log") as crash_log,
            patch("homestock.scripter.os._exit") as exit_process,
        ):
            writer.start()
            scripter._writer_queue.put("{}")
            for _ in range(100):
                if exit_process.called or not writer.is_alive():
                    break
                threading.Event().wait(0.01)
            scripter._writer_queue.put(scripter._WRITER_STOP)
            writer.join(1.0)

        crash_log.assert_called_once()
        exit_process.assert_called_once_with(1)

    def test_isolate_process_writer_queue_full_is_fatal(self):
        scripter = IsolateProcessScripter(io.StringIO(), writer_queue_maxsize=1)
        scripter._writer_queue = queue.Queue(maxsize=1)
        scripter._writer_queue.put("{}")

        with (
            patch("homestock.scripter.write_crash_log") as crash_log,
            patch("homestock.scripter.os._exit") as exit_process,
        ):
            scripter.heartbeat("main_process", {"pid": 1004})

        crash_log.assert_called_once()
        exit_process.assert_called_once_with(1)

    def test_scripter_child_dispatches_jsonl_to_runtime_and_keeps_running_after_bad_events(self):
        class Runtime:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str, dict[str, object] | list[dict[str, object]]]] = []

            def dispatch_log_now(self, level: str, source: str, message: str, payload: dict[str, object] | None = None) -> None:
                self.calls.append(("log", source, {"level": level, "message": message, "payload": payload or {}}))

            def error(
                self,
                source: str,
                message: str,
                exc=None,
                callstack: str | None = None,
                payload: dict[str, object] | None = None,
            ) -> None:
                self.calls.append(("error", source, {"message": message, "callstack": callstack or "", "payload": payload or {}}))

            def dispatch_error_payload_now(self, event_type: str, payload: dict[str, object]) -> None:
                self.calls.append(("error_payload", event_type, payload))

            def dispatch_system_callback_now(self, event_type: str, message: str, details: dict[str, object] | None = None) -> None:
                self.calls.append(("system_callback", event_type, {"message": message, "details": details or {}}))

            def dispatch_heartbeat_now(self, name: str, payload: dict[str, object]) -> None:
                self.calls.append(("heartbeat", name, payload))

            def configure_system_callbacks(self, callbacks: list[dict[str, object]]) -> None:
                self.calls.append(("config", "system_callbacks", callbacks))

        logger = logging.getLogger("test.scripter.child")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        try:
            runtime = Runtime()

            self.assertTrue(process_scripter_event_line("{not-json}\n", runtime, logger))
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "log",
                            "event_type": "sample",
                            "payload": {"level": "info", "source": "sample", "message": "hello", "payload": {"value": 1}},
                        }
                    ),
                    runtime,
                    logger,
                )
            )
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "error",
                            "event_type": "sample_error",
                            "payload": {
                                "source": "sample",
                                "message": "failed",
                                "exception_type": "RuntimeError",
                                "error": "boom",
                                "callstack": "stack",
                            },
                        }
                    ),
                    runtime,
                    logger,
                )
            )
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "system_callback",
                            "event_type": "sample_event",
                            "payload": {"message": "sample callback", "details": {"value": 2}, "extra": 3},
                        }
                    ),
                    runtime,
                    logger,
                )
            )
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps({"kind": "heartbeat", "event_type": "main", "payload": {"ok": True}}),
                    runtime,
                    logger,
                )
            )
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "config",
                            "event_type": "system_callbacks",
                            "payload": {
                                "callbacks": [
                                    {
                                        "system_callback_id": "sys_1",
                                        "httpCallback": {"method": "POST", "url": "http://localhost:9000/system"},
                                    }
                                ]
                            },
                        }
                    ),
                    runtime,
                    logger,
                )
            )
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "config",
                            "event_type": "system_callbacks",
                            "payload": {"callbacks": "not-a-list"},
                        }
                    ),
                    runtime,
                    logger,
                )
            )
            self.assertFalse(
                process_scripter_event_line(
                    json.dumps({"kind": "control", "event_type": "close", "payload": {}}),
                    runtime,
                    logger,
                )
            )

            self.assertEqual(
                runtime.calls,
                [
                    ("log", "sample", {"level": "info", "message": "hello", "payload": {"value": 1}}),
                    (
                        "error_payload",
                        "sample_error",
                        {
                            "source": "sample",
                            "message": "failed",
                            "exception_type": "RuntimeError",
                            "error": "boom",
                            "callstack": "stack",
                        },
                    ),
                    ("system_callback", "sample_event", {"message": "sample callback", "details": {"value": 2}}),
                    ("heartbeat", "main", {"ok": True}),
                    (
                        "config",
                        "system_callbacks",
                        [
                            {
                                "system_callback_id": "sys_1",
                                "httpCallback": {"method": "POST", "url": "http://localhost:9000/system"},
                            }
                        ],
                    ),
                ],
            )

            class BrokenRuntime(Runtime):
                def dispatch_system_callback_now(self, event_type: str, message: str, details: dict[str, object] | None = None) -> None:
                    raise RuntimeError("boom")

            with self.assertRaisesRegex(RuntimeError, "boom"):
                process_scripter_event_line(
                    json.dumps({"kind": "system_callback", "event_type": "broken", "payload": {}}),
                    BrokenRuntime(),
                    logger,
                )
        finally:
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.propagate = True

    def test_scripter_child_ignores_unknown_top_level_fields_for_business_dispatch(self):
        class Runtime:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str, dict[str, object]]] = []

            def dispatch_log_now(self, level: str, source: str, message: str, payload: dict[str, object] | None = None) -> None:
                self.calls.append(("log", source, {"level": level, "message": message, "payload": payload or {}}))

            def dispatch_system_callback_now(self, event_type: str, message: str, details: dict[str, object] | None = None) -> None:
                self.calls.append(("system_callback", event_type, {"message": message, "details": details or {}}))

        logger = logging.getLogger("test.scripter.child.trace-field")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        try:
            runtime = Runtime()
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "log",
                            "event_type": "sample",
                            "payload": {"level": "info", "source": "sample", "message": "hello", "payload": {"value": 1}},
                            "extra_field": "ignored",
                        }
                    ),
                    runtime,
                    logger,
                )
            )
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "system_callback",
                            "event_type": "sample_event",
                            "payload": {"message": "sample callback", "details": {"value": 2}},
                            "extra_field": "ignored",
                        }
                    ),
                    runtime,
                    logger,
                )
            )
        finally:
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.propagate = True

        self.assertEqual(
            runtime.calls,
            [
                ("log", "sample", {"level": "info", "message": "hello", "payload": {"value": 1}}),
                ("system_callback", "sample_event", {"message": "sample callback", "details": {"value": 2}}),
            ],
        )

    def test_scripter_child_reads_utf8_pipe_without_surrogate_escape(self):
        class BinaryStdin:
            def __init__(self, data: bytes) -> None:
                self.buffer = io.BytesIO(data)

        message = "MCP 서버 구동 실패"
        payload = {
            "kind": "system_callback",
            "event_type": "mcp_startup_failed",
            "payload": {"message": message},
        }
        raw = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        broken = raw.decode("cp949", errors="surrogateescape")

        self.assertTrue(any(0xDC80 <= ord(char) <= 0xDCFF for char in broken))

        line = _utf8_stdin_text_stream(BinaryStdin(raw)).readline()

        self.assertIn(message, line)
        self.assertFalse(any(0xDC80 <= ord(char) <= 0xDCFF for char in line))

    def test_scripter_child_stops_on_stdin_eof_when_configured(self):
        logger = logging.getLogger("test.scripter.child.eof")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        try:
            runtime = InProcessScripter(logger=logger)
            runtime.start()
            run_scripter_child(io.StringIO(""), runtime._runtime_or_start(), logger, stop_on_eof=True)
        finally:
            runtime.close()
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.propagate = True

    def test_scripter_child_ignores_malformed_callback_entries_without_clearing_snapshot(self):
        logger = logging.getLogger("test.scripter.child.malformed-config")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        scripter = InProcessScripter(logger=logger)
        try:
            runtime = scripter._runtime_or_start()
            runtime.configure_system_callbacks(
                [
                    {
                        "system_callback_id": "sys_1",
                        "httpCallback": {"method": "POST", "url": "http://localhost:9000/system"},
                    }
                ]
            )

            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "config",
                            "event_type": "system_callbacks",
                            "payload": {"callbacks": [{}]},
                        }
                    ),
                    runtime,
                    logger,
                )
            )
            self.assertTrue(
                process_scripter_event_line(
                    json.dumps(
                        {
                            "kind": "config",
                            "event_type": "system_callbacks",
                            "payload": {
                                "callbacks": [
                                    {
                                        "system_callback_id": "sys_bad",
                                        "httpCallback": {
                                            "method": "POST",
                                            "url": "http://localhost:9000/system",
                                            "headers": ["bad"],
                                        },
                                    }
                                ]
                            },
                        }
                    ),
                    runtime,
                    logger,
                )
            )

            with runtime._lock:
                callbacks = list(runtime._callbacks)
            self.assertEqual(len(callbacks), 1)
            self.assertEqual(callbacks[0]["system_callback_id"], "sys_1")
        finally:
            scripter.close()
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.propagate = True

    def test_scripter_child_reraises_stdin_read_error_when_stop_on_eof(self):
        class BrokenStream:
            def readline(self) -> str:
                raise OSError("stdin broken")

        logger = logging.getLogger("test.scripter.child.stdin")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        try:
            runtime = InProcessScripter(logger=logger)
            runtime.start()
            with self.assertRaisesRegex(OSError, "stdin broken"):
                run_scripter_child(BrokenStream(), runtime._runtime_or_start(), logger, stop_on_eof=True)
        finally:
            runtime.close()
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.propagate = True

    def test_scripter_child_main_reports_runtime_fatal_and_closes_runtime(self):
        class FakeRuntime:
            instance = None

            def __init__(self, **_kwargs) -> None:
                FakeRuntime.instance = self
                self.system_callbacks: list[tuple[str, str, dict[str, object] | None]] = []
                self.closed = False

            def log_startup(self, stage: str, message: str, level: str = "info") -> None:
                return None

            def log_ops(
                self,
                source: str,
                message: str,
                level: str = "info",
                payload: dict[str, object] | None = None,
            ) -> None:
                return None

            def dispatch_system_callback_now(
                self,
                event_type: str,
                message: str,
                details: dict[str, object] | None = None,
            ) -> None:
                self.system_callbacks.append((event_type, message, details))

            def close(self) -> None:
                self.closed = True

        logger = logging.getLogger("test.scripter.child.main")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        args = SimpleNamespace(
            log_dir=".",
            retention_days=5,
            queue_maxsize=10,
            interval_seconds=60.0,
            ready_file="",
        )

        with (
            patch("homestock.scripter_child._parse_args", return_value=args),
            patch("homestock.scripter_child._configure_child_logger", return_value=logger),
            patch("homestock.scripter_child._ScripterRuntime", FakeRuntime),
            patch("homestock.scripter_child.run_scripter_child", side_effect=RuntimeError("runtime boom")),
            patch("homestock.scripter_child.write_crash_log") as crash_log,
        ):
            with self.assertRaisesRegex(RuntimeError, "runtime boom"):
                scripter_child_main([])

        runtime = FakeRuntime.instance
        self.assertIsNotNone(runtime)
        assert runtime is not None
        self.assertEqual(runtime.system_callbacks[0][0], "scripter_child_fatal")
        self.assertTrue(runtime.closed)
        crash_log.assert_called_once()

    def test_configure_logger_defaults_to_info_for_stdout_and_daily_file(self):
        with TemporaryDirectory() as tempdir:
            stream = io.StringIO()
            logger = configure_logger(
                ScripterLogConfig(log_dir=tempdir, logger_name="test.homestock.scripter.isolate"),
                stream=stream,
            )
            try:
                logger.debug(
                    "debug url=%s token=%s",
                    "https://hooks.example.test/system?token=query-secret",
                    "debug-secret",
                )
                logger.info("info")
                logger.warning("warning secret=warning-secret")
                logger.warning("warning password=warning-password")
                logger.warning("quoted token='quoted-secret' account_password='quoted-password'")
                logger.warning("auth Authorization: Bearer bearer-secret")
                logger.error("error")
                logger.critical("critical")
                for handler in logger.handlers:
                    handler.flush()

                stdout_text = stream.getvalue()
                file_text = (Path(tempdir) / "scripter.log").read_text(encoding="utf-8")
                self.assertEqual(stdout_text, file_text)
                self.assertNotIn("[DEBUG]", stdout_text)
                for level in ("INFO", "WARNING", "ERROR", "CRITICAL"):
                    self.assertIn(f"[{level}]", stdout_text)
                self.assertRegex(
                    stdout_text.splitlines()[0],
                    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4} \[INFO\]",
                )
                self.assertIn("secret=warning-secret", stdout_text)
                self.assertIn("password=<redacted>", stdout_text)
                self.assertIn("token='quoted-secret'", stdout_text)
                self.assertIn("account_password='<redacted>'", stdout_text)
                self.assertIn("Authorization: Bearer bearer-secret", stdout_text)
                self.assertNotIn("hooks.example.test", stdout_text)
                self.assertNotIn("query-secret", stdout_text)
                self.assertNotIn("debug-secret", stdout_text)
                self.assertNotIn("warning-password", stdout_text)
                self.assertNotIn("quoted-password", stdout_text)

                file_handlers = [handler for handler in logger.handlers if isinstance(handler, TimedRotatingFileHandler)]
                self.assertEqual(len(file_handlers), 1)
                self.assertEqual(file_handlers[0].when, "MIDNIGHT")
                self.assertFalse(file_handlers[0].utc)
                self.assertEqual(file_handlers[0].backupCount, 4)
            finally:
                for handler in list(logger.handlers):
                    logger.removeHandler(handler)
                    handler.close()

    def test_configure_logger_can_enable_debug_for_stdout_and_daily_file(self):
        with TemporaryDirectory() as tempdir:
            stream = io.StringIO()
            logger = configure_logger(
                ScripterLogConfig(
                    log_dir=tempdir,
                    logger_name="test.homestock.scripter.debug",
                    log_level="debug",
                ),
                stream=stream,
            )
            try:
                logger.debug("debug detail")
                logger.info("info")
                for handler in logger.handlers:
                    handler.flush()

                stdout_text = stream.getvalue()
                file_text = (Path(tempdir) / "scripter.log").read_text(encoding="utf-8")
                self.assertEqual(stdout_text, file_text)
                self.assertIn("[DEBUG]", stdout_text)
                self.assertIn("debug detail", stdout_text)
                self.assertIn("[INFO]", stdout_text)
            finally:
                for handler in list(logger.handlers):
                    logger.removeHandler(handler)
                    handler.close()

    def test_ops_log_payload_details_are_logged(self):
        with TemporaryDirectory() as tempdir:
            stream = io.StringIO()
            logger = configure_logger(
                ScripterLogConfig(log_dir=tempdir, logger_name="test.homestock.scripter.ops-info"),
                stream=stream,
            )
            scripter = InProcessScripter(logger=logger)
            try:
                scripter.start()
                ops_log(LogSource.STARTUP_SERVER, "startup message", payload={"phase": "boot"})
                self.assertTrue(scripter.wait_for_idle(1.0))

                stdout_text = stream.getvalue()
                self.assertIn(f"{LogSource.STARTUP_SERVER} startup message", stdout_text)
                self.assertIn(f"{LogSource.STARTUP_SERVER} details=", stdout_text)
                self.assertIn('"phase": "boot"', stdout_text)
            finally:
                scripter.close()
                for handler in list(logger.handlers):
                    logger.removeHandler(handler)
                    handler.close()

    def test_ops_log_source_is_not_reclassified(self):
        with TemporaryDirectory() as tempdir:
            stream = io.StringIO()
            logger = configure_logger(
                ScripterLogConfig(log_dir=tempdir, logger_name="test.homestock.scripter.source-log"),
                stream=stream,
            )
            scripter = InProcessScripter(logger=logger)
            try:
                scripter.start()
                ops_log(LogSource.RT_RUNTIME, "RT event received rt_type=N0 code=005930 matched=0 queued=0 persisted=False")
                ops_log(LogSource.WEBHOOK, "callback dispatch queued callback_id=sys_1")
                self.assertTrue(scripter.wait_for_idle(1.0))

                stdout_text = stream.getvalue()
                self.assertIn(f"{LogSource.RT_RUNTIME} RT event received", stdout_text)
                self.assertIn(f"{LogSource.WEBHOOK} callback dispatch queued", stdout_text)
                self.assertNotIn(f"startup.{LogSource.RT_RUNTIME}", stdout_text)
                self.assertNotIn(f"startup.{LogSource.WEBHOOK}", stdout_text)
            finally:
                scripter.close()
                for handler in list(logger.handlers):
                    logger.removeHandler(handler)
                    handler.close()

    def test_ops_log_level_debug_uses_debug_when_enabled(self):
        with TemporaryDirectory() as tempdir:
            stream = io.StringIO()
            logger = configure_logger(
                ScripterLogConfig(
                    log_dir=tempdir,
                    logger_name="test.homestock.scripter.ops-debug",
                    log_level="debug",
                ),
                stream=stream,
            )
            scripter = InProcessScripter(logger=logger)
            try:
                scripter.start()
                ops_log(LogSource.STARTUP_SERVER, "debug startup message", level="debug", payload={"phase": "boot"})
                self.assertTrue(scripter.wait_for_idle(1.0))

                stdout_text = stream.getvalue()
                self.assertIn("[DEBUG]", stdout_text)
                self.assertIn(f"{LogSource.STARTUP_SERVER} debug startup message", stdout_text)
                self.assertIn(f"{LogSource.STARTUP_SERVER} details=", stdout_text)
            finally:
                scripter.close()
                for handler in list(logger.handlers):
                    logger.removeHandler(handler)
                    handler.close()

    def test_in_process_scripter_redacts_json_details_with_plain_injected_logger(self):
        stream = io.StringIO()
        logger = logging.getLogger("test.homestock.scripter.plain-redaction")
        logger.handlers = []
        logger.propagate = False
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        scripter = InProcessScripter(logger=logger)
        try:
            scripter.start()
            scripter.log(
                "info",
                "redaction",
                "Authorization: Bearer message-secret account_password=message-password",
                {
                    "url": "https://hooks.example.test/system?token=query-secret",
                    "secret": "body-secret",
                    "nested": {"token": "nested-token"},
                    "accountPassword": "nested-password",
                },
            )
            self.assertTrue(scripter.wait_for_idle(1.0))

            text = stream.getvalue()
            self.assertIn("<redacted>", text)
            self.assertIn("hooks.example.test", text)
            self.assertIn("query-secret", text)
            self.assertIn("message-secret", text)
            self.assertIn("body-secret", text)
            self.assertIn("nested-token", text)
            self.assertNotIn("message-password", text)
            self.assertNotIn("nested-password", text)
        finally:
            scripter.close()
            logger.removeHandler(handler)
            handler.close()
            logger.setLevel(logging.NOTSET)
            logger.propagate = True

    def test_heartbeat_silent_threshold_is_three_minutes_or_more(self):
        last_heartbeat_at = datetime(2026, 4, 30, 21, 40, 0, tzinfo=timezone(timedelta(hours=9)))

        self.assertEqual(HEARTBEAT_SILENT_TIMEOUT_SECONDS, 180)
        self.assertEqual(HEARTBEAT_LOCK_TIMEOUT_SECONDS, HEARTBEAT_SILENT_TIMEOUT_SECONDS)
        self.assertIs(is_heartbeat_locked, is_heartbeat_silent)
        self.assertIs(HeartbeatLockMonitor, HeartbeatSilenceMonitor)
        self.assertFalse(is_heartbeat_silent(last_heartbeat_at, now=last_heartbeat_at + timedelta(seconds=179)))
        self.assertTrue(is_heartbeat_silent(last_heartbeat_at, now=last_heartbeat_at + timedelta(minutes=3)))
        self.assertTrue(is_heartbeat_silent(last_heartbeat_at, now=last_heartbeat_at + timedelta(minutes=3, seconds=1)))

    def test_heartbeat_silence_monitor_reports_immediately_and_repeats_every_three_minutes(self):
        started_at = datetime(2026, 4, 30, 21, 40, 0, tzinfo=timezone(timedelta(hours=9)))
        scripter = MockScripter()
        monitor = HeartbeatSilenceMonitor("main_process", scripter, started_at=started_at)

        self.assertEqual(HEARTBEAT_SILENT_REPEAT_INTERVAL_SECONDS, 180)
        self.assertEqual(HEARTBEAT_LOCK_REPEAT_INTERVAL_SECONDS, HEARTBEAT_SILENT_REPEAT_INTERVAL_SECONDS)
        self.assertFalse(monitor.check(now=started_at + timedelta(seconds=179)))
        self.assertEqual(scripter.system_callbacks, [])

        self.assertTrue(monitor.check(now=started_at + timedelta(minutes=3)))
        self.assertEqual(len(scripter.system_callbacks), 1)
        self.assertEqual(scripter.system_callbacks[0].event_type, HEARTBEAT_SILENT_EVENT_TYPE)
        self.assertEqual(HEARTBEAT_LOCK_EVENT_TYPE, HEARTBEAT_SILENT_EVENT_TYPE)
        self.assertFalse(scripter.system_callbacks[0].payload["details"]["repeat"])
        self.assertEqual(scripter.system_callbacks[0].payload["details"]["seconds_since_heartbeat"], 180)

        self.assertFalse(monitor.check(now=started_at + timedelta(minutes=5, seconds=59)))
        self.assertEqual(len(scripter.system_callbacks), 1)

        self.assertTrue(monitor.check(now=started_at + timedelta(minutes=6)))
        self.assertEqual(len(scripter.system_callbacks), 2)
        self.assertTrue(scripter.system_callbacks[1].payload["details"]["repeat"])
        self.assertEqual(scripter.system_callbacks[1].payload["details"]["seconds_since_heartbeat"], 360)

        self.assertTrue(monitor.check(now=started_at + timedelta(minutes=9)))
        self.assertEqual(len(scripter.system_callbacks), 3)
        self.assertTrue(scripter.system_callbacks[2].payload["details"]["repeat"])
        self.assertEqual(scripter.system_callbacks[2].payload["details"]["seconds_since_heartbeat"], 540)

    def test_heartbeat_silence_monitor_stops_repeating_after_heartbeat(self):
        started_at = datetime(2026, 4, 30, 21, 40, 0, tzinfo=timezone(timedelta(hours=9)))
        heartbeat_at = started_at + timedelta(minutes=3, seconds=1)
        scripter = MockScripter()
        monitor = HeartbeatSilenceMonitor("main_process", scripter, started_at=started_at)

        self.assertTrue(monitor.check(now=started_at + timedelta(minutes=3)))
        monitor.heartbeat({"pid": 1004}, at=heartbeat_at)

        self.assertEqual(len(scripter.system_callbacks), 2)
        self.assertEqual(scripter.system_callbacks[1].event_type, HEARTBEAT_REVIVED_EVENT_TYPE)
        self.assertEqual(HEARTBEAT_UNLOCK_EVENT_TYPE, HEARTBEAT_REVIVED_EVENT_TYPE)
        self.assertEqual(scripter.system_callbacks[1].payload["details"]["heartbeat_name"], "main_process")
        self.assertEqual(scripter.system_callbacks[1].payload["details"]["seconds_since_heartbeat"], 181)
        self.assertEqual(scripter.system_callbacks[1].payload["details"]["seconds_silent"], 1)
        self.assertEqual(len(scripter.heartbeats), 1)
        self.assertEqual(scripter.heartbeats[0].event_type, "main_process")
        self.assertEqual(scripter.heartbeats[0].payload, {"pid": 1004})

        self.assertFalse(monitor.check(now=heartbeat_at + timedelta(seconds=179)))
        self.assertEqual(len(scripter.system_callbacks), 2)

        self.assertTrue(monitor.check(now=heartbeat_at + timedelta(minutes=3)))
        self.assertEqual(len(scripter.system_callbacks), 3)
        self.assertFalse(scripter.system_callbacks[2].payload["details"]["repeat"])
        self.assertEqual(scripter.system_callbacks[2].payload["details"]["seconds_since_heartbeat"], 180)

    def test_heartbeat_silent_alerts_stop_after_limit_and_reset_after_revive(self):
        started_at = datetime(2026, 4, 30, 21, 40, 0, tzinfo=timezone(timedelta(hours=9)))
        scripter = MockScripter()
        monitor = HeartbeatSilenceMonitor("main_process", scripter, started_at=started_at)

        self.assertEqual(HEARTBEAT_SILENT_ALERT_LIMIT, 5)
        for count in range(1, HEARTBEAT_SILENT_ALERT_LIMIT + 1):
            self.assertTrue(monitor.check(now=started_at + timedelta(minutes=3 * count)))
            self.assertEqual(len(scripter.system_callbacks), count)
            self.assertEqual(scripter.system_callbacks[-1].event_type, HEARTBEAT_SILENT_EVENT_TYPE)
            self.assertEqual(scripter.system_callbacks[-1].payload["details"]["silent_alert_count"], count)
            self.assertEqual(scripter.system_callbacks[-1].payload["details"]["silent_alert_limit"], HEARTBEAT_SILENT_ALERT_LIMIT)

        self.assertFalse(monitor.check(now=started_at + timedelta(minutes=18)))
        self.assertEqual(len(scripter.system_callbacks), HEARTBEAT_SILENT_ALERT_LIMIT)

        revived_at = started_at + timedelta(minutes=18, seconds=1)
        monitor.heartbeat(at=revived_at)
        self.assertEqual(len(scripter.system_callbacks), HEARTBEAT_SILENT_ALERT_LIMIT + 1)
        self.assertEqual(scripter.system_callbacks[-1].event_type, HEARTBEAT_REVIVED_EVENT_TYPE)

        self.assertTrue(monitor.check(now=revived_at + timedelta(minutes=3)))
        self.assertEqual(len(scripter.system_callbacks), HEARTBEAT_SILENT_ALERT_LIMIT + 2)
        self.assertEqual(scripter.system_callbacks[-1].event_type, HEARTBEAT_SILENT_EVENT_TYPE)
        self.assertEqual(scripter.system_callbacks[-1].payload["details"]["silent_alert_count"], 1)

    def test_heartbeat_silence_monitor_logs_status_every_two_minutes(self):
        started_at = datetime(2026, 4, 30, 21, 40, 0, tzinfo=timezone(timedelta(hours=9)))
        stream = io.StringIO()
        logger = logging.getLogger("test.heartbeat.status")
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
        logger.handlers = [handler]
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        try:
            monitor = HeartbeatSilenceMonitor(
                "main_process",
                MockScripter(),
                started_at=started_at,
                timeout=timedelta(minutes=10),
                logger=logger,
            )

            self.assertEqual(HEARTBEAT_STATUS_LOG_INTERVAL_SECONDS, 120)
            monitor.heartbeat(at=started_at + timedelta(seconds=60))
            self.assertFalse(monitor.check(now=started_at + timedelta(seconds=119)))
            self.assertEqual(stream.getvalue(), "")

            self.assertFalse(monitor.check(now=started_at + timedelta(minutes=2)))
            lines = stream.getvalue().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertIn("INFO:Heartbeat beating name=main_process", lines[0])
            self.assertIn("seconds_since_heartbeat=60", lines[0])

            self.assertFalse(monitor.check(now=started_at + timedelta(minutes=3, seconds=59)))
            self.assertEqual(len(stream.getvalue().splitlines()), 1)

            self.assertFalse(monitor.check(now=started_at + timedelta(minutes=4)))
            lines = stream.getvalue().splitlines()
            self.assertEqual(len(lines), 2)
            self.assertIn("WARNING:Heartbeat silent name=main_process", lines[1])
            self.assertIn("seconds_since_heartbeat=180", lines[1])
        finally:
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.setLevel(logging.NOTSET)
            logger.propagate = True

    def test_in_process_scripter_dispatches_configured_system_callbacks(self):
        stream = io.StringIO()
        logger = logging.getLogger("test.scripter.system")
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
        logger.handlers = [handler]
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        scripter = InProcessScripter(logger=logger)
        event_time = datetime(2026, 4, 30, 21, 20, 0, tzinfo=timezone(timedelta(hours=9)))
        try:
            scripter.configure_system_callbacks(
                [
                    {
                        "system_callback_id": "sys_1",
                        "httpCallback": {
                            "method": "POST",
                            "url": "http://localhost:9000/system",
                            "body": {
                                "tag": "{{tag}}",
                                "name": "{{name}}",
                                "callstack": "{{callstack}}",
                                "occurredAt": "{{occurred_at}}",
                            },
                            "bodyFormat": "json",
                        },
                    }
                ]
            )
            self.assertTrue(scripter.wait_for_idle(1.0))

            dispatched_bodies: list[dict[str, object] | None] = []
            with (
                patch("homestock.scripter._kst_now", return_value=event_time),
                patch("homestock.scripter.CallbackDispatcher.dispatch", autospec=True) as dispatch,
            ):
                dispatch.side_effect = lambda _self, callback: (
                    dispatched_bodies.append(callback.body),
                    {"queued": True, "delivered": None, "error": None},
                )[1]

                scripter.system_callback(
                    "subscription_subscribe_failed",
                    "뉴스 구독 등록 실패",
                    {
                        "subscription_kind": "news",
                        "error": "RequestRTReg failed",
                    },
                )
                self.assertTrue(scripter.wait_for_idle(1.0))

            self.assertEqual(dispatched_bodies[0]["tag"], "subscription_subscribe_failed")
            self.assertEqual(dispatched_bodies[0]["name"], "뉴스 구독 등록 실패")
            self.assertIn("subscription_kind=news", dispatched_bodies[0]["callstack"])
            self.assertIn("error=RequestRTReg failed", dispatched_bodies[0]["callstack"])
            self.assertEqual(dispatched_bodies[0]["occurredAt"], "20260430212000")
            system_logs = stream.getvalue()
            self.assertIn("INFO:System callback queued event_type=subscription_subscribe_failed", system_logs)
            self.assertIn("system_callback_id=sys_1", system_logs)
            self.assertIn("queued=True", system_logs)
        finally:
            scripter.close()
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.setLevel(logging.NOTSET)
            logger.propagate = True

    def test_in_process_wait_for_idle_waits_for_real_dispatcher_delivery(self):
        logger = logging.getLogger("test.scripter.dispatcher.drain")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        dispatcher = CallbackDispatcher(logger=logger, max_attempts=1, retry_delay_seconds=0.0)
        scripter = InProcessScripter(logger=logger, dispatcher=dispatcher)
        entered_send = threading.Event()
        release_send = threading.Event()

        def slow_send(_dispatcher, _callback):
            entered_send.set()
            self.assertTrue(release_send.wait(1.0))
            return {"delivered": True, "error": None}

        try:
            scripter.configure_system_callbacks(
                [
                    {
                        "system_callback_id": "sys_1",
                        "httpCallback": {"method": "POST", "url": "http://localhost:9000/system"},
                    }
                ]
            )
            with patch("homestock.webhook.CallbackDispatcher._send_once", autospec=True, side_effect=slow_send):
                scripter.system_callback("sample_event", "sample message")
                self.assertTrue(entered_send.wait(1.0))
                self.assertFalse(scripter.wait_for_idle(0.05))
                release_send.set()
                self.assertTrue(scripter.wait_for_idle(1.0))
        finally:
            release_send.set()
            scripter.close()
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.propagate = True

    def test_in_process_scripter_dispatches_system_callback_when_event_queue_overflows(self):
        scripter = InProcessScripter(queue_maxsize=1)
        worker_entered = threading.Event()
        release_worker = threading.Event()
        try:
            scripter.configure_system_callbacks(
                [
                    {
                        "system_callback_id": "sys_overflow",
                        "httpCallback": {"method": "POST", "url": "http://localhost:9000/system"},
                    }
                ]
            )
            self.assertTrue(scripter.wait_for_idle(1.0))

            dispatched_bodies: list[dict[str, object] | None] = []

            def block_event_handler(_runtime, _event) -> None:
                worker_entered.set()
                self.assertTrue(release_worker.wait(1.0))

            with (
                patch("homestock.scripter._ScripterRuntime._handle_event", block_event_handler),
                patch("homestock.scripter.CallbackDispatcher.dispatch", autospec=True) as dispatch,
            ):
                dispatch.side_effect = lambda _self, callback: (
                    dispatched_bodies.append(callback.body),
                    {"queued": True, "delivered": None, "error": None},
                )[1]

                scripter.log("info", "held_event", "held")
                self.assertTrue(worker_entered.wait(1.0))
                scripter.log("info", "queued_event", "queued")
                scripter.log("info", "dropped_event", "dropped")

                self.assertEqual(len(dispatched_bodies), 1)
                payload = dispatched_bodies[0] or {}
                self.assertEqual(payload["event_type"], SCRIPTER_QUEUE_OVERFLOW_EVENT_TYPE)
                self.assertEqual(payload["message"], "Scripter event queue overflow")
                details = payload["details"]
                self.assertEqual(details["queue_maxsize"], 1)
                self.assertEqual(details["dropped_event_type"], "dropped_event")
                self.assertEqual(details["last_event_action"]["event_type"], "dropped_event")

                release_worker.set()
                self.assertTrue(scripter.wait_for_idle(1.0))
        finally:
            release_worker.set()
            scripter.close()

    def test_runtime_worker_failure_is_fatal_when_configured(self):
        logger = logging.getLogger("test.scripter.runtime.fatal")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        wait_timeouts: list[float] = []

        class Dispatcher:
            drain_timeout_seconds = 12.0

            def dispatch(self, _callback):
                return {"queued": True}

            def wait_for_idle(self, timeout: float = 1.0) -> bool:
                wait_timeouts.append(timeout)
                return True

            def close(self, timeout: float = 1.0) -> None:
                return None

        runtime = _ScripterRuntime(logger=logger, dispatcher=Dispatcher(), fatal_on_worker_error=True)
        try:
            with (
                patch.object(_ScripterRuntime, "_handle_event", side_effect=RuntimeError("worker boom")),
                patch("homestock.scripter.write_crash_log") as crash_log,
                patch("homestock.scripter.os._exit") as exit_process,
            ):
                runtime.log("info", "sample", "hello")
                self.assertTrue(runtime.wait_for_idle(1.0))

            crash_log.assert_called_once()
            exit_process.assert_called_once_with(1)
            self.assertIn(12.0, wait_timeouts)
        finally:
            runtime.close()
            for existing in list(logger.handlers):
                logger.removeHandler(existing)
                existing.close()
            logger.propagate = True


if __name__ == "__main__":
    unittest.main()
