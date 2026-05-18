"""
examples/web_dashboard_demo.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Extends temperature_monitor.py to add the SignalDashboard web UI.

Every signal and log event is streamed live to a browser and written
to a plain-text log file.

Requirements:
    pip install "arachnite[web]"

Run:
    python examples/web_dashboard_demo.py
    # then open http://localhost:7070 in a browser
"""

from __future__ import annotations

import asyncio
import signal
import sys

# Re-use all the nodes from the temperature monitor example
from examples.temperature_monitor import (
    SimTempSensor,
    WarnInstinct,
    CriticalReflex,
    CoolFan,
    EmergencyStop,
)

from arachnite import (
    ArachniteRuntime,
    SignalBus,
    ContextNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    SenseMasterNode,
    InstinctMasterNode,
    ActionMasterNode,
    LogLevel,
    SignalDashboard,
)


async def main() -> None:
    bus = SignalBus()

    # SignalDashboard acts as both a LogSink and a SignalBus subscriber.
    # Pass it as log_sinks to the runtime and to any nodes whose log
    # events you want captured.
    dashboard = SignalDashboard(
        bus,
        host     = "127.0.0.1",
        port     = 7070,
        log_file = "temperature_monitor.log",   # plain-text file
        level    = LogLevel.DEBUG,
        backlog  = 500,
    )

    log_sinks = [dashboard]

    sense_master    = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    action_master   = ActionMasterNode(bus=bus)

    sense_master.register(SimTempSensor(bus=bus, log_sinks=log_sinks))
    instinct_master.register(WarnInstinct(bus=bus, log_sinks=log_sinks))
    instinct_master.register(CriticalReflex(bus=bus, log_sinks=log_sinks))
    action_master.register(CoolFan(bus=bus, log_sinks=log_sinks))
    action_master.register(EmergencyStop(bus=bus, log_sinks=log_sinks))

    rt = ArachniteRuntime(
        sense_master      = sense_master,
        context           = ContextNode(),
        instinct_master   = instinct_master,
        decision_master   = decision_master,
        action_master     = action_master,
        bus               = bus,
        tick_rate_hz      = 4.0,
        log_sinks         = log_sinks,
        # Wire per-tick Context + Decision snapshots into the details pane.
        context_observers  = [dashboard.submit_context],
        decision_observers = [dashboard.submit_decision],
    )

    # Register OS signal handlers so SIGINT/SIGTERM trigger graceful shutdown.
    # Windows' asyncio doesn't support loop.add_signal_handler, so we skip
    # registration there and rely on the KeyboardInterrupt path below.
    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(rt.stop()))

    await dashboard.start()
    print("Dashboard: http://localhost:7070")
    print("Log file:  temperature_monitor.log")
    print("Press Ctrl+C to stop.")
    print("-" * 50)

    await rt.start()
    try:
        await rt.wait()       # blocks until rt.stop() is called
    except (KeyboardInterrupt, asyncio.CancelledError):
        # On Windows, Ctrl+C surfaces here as KeyboardInterrupt.
        pass
    finally:
        await rt.stop()
        await dashboard.stop()
    print(f"\nStopped after {rt.tick_count} ticks.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
