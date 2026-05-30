from __future__ import annotations

from homestock.config import Settings
from homestock.indi import IndiClient, MockIndiClient, RealIndiClient, ThreadedIndiClient
from homestock.ops_log import LogSource, ops_log


def create_indi_client(settings: Settings) -> IndiClient:
    ops_log(LogSource.STARTUP_BACKEND, f"create_indi_client backend={settings.backend}")
    if settings.backend == "mock":
        ops_log(LogSource.STARTUP_BACKEND, "instantiating MockIndiClient")
        client = MockIndiClient()
        ops_log(LogSource.STARTUP_BACKEND, "MockIndiClient instantiated")
        return client
    if settings.backend == "real":
        if settings.use_threaded_real_client:
            ops_log(LogSource.STARTUP_BACKEND, "instantiating ThreadedIndiClient for RealIndiClient")
            client = ThreadedIndiClient(RealIndiClient)
            ops_log(LogSource.STARTUP_BACKEND, "ThreadedIndiClient instantiated")
            return client
        ops_log(LogSource.STARTUP_BACKEND, "instantiating direct RealIndiClient use_threaded_real_client=False")
        client = RealIndiClient()
        ops_log(LogSource.STARTUP_BACKEND, "RealIndiClient instantiated")
        return client
    ops_log(LogSource.STARTUP_BACKEND, f"unsupported backend={settings.backend}")
    raise ValueError(f"unsupported backend: {settings.backend}")
