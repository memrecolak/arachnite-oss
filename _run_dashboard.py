"""Windows-compatible launcher for the web dashboard demo."""
from __future__ import annotations

import asyncio

from examples.temperature_monitor import (
    CoolFan,
    CriticalReflex,
    EmergencyStop,
    SimTempSensor,
    WarnInstinct,
)

from arachnite import (
    ActionMasterNode,
    ArachniteRuntime,
    ContextNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    InstinctMasterNode,
    LogLevel,
    SenseMasterNode,
    SignalBus,
    SignalDashboard,
)


async def main() -> None:
    bus = SignalBus()
    dashboard = SignalDashboard(
        bus,
        host="127.0.0.1",
        port=8765,
        log_file="temperature_monitor.log",
        level=LogLevel.DEBUG,
        backlog=500,
    )
    log_sinks = [dashboard]

    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(SimTempSensor(bus=bus, log_sinks=log_sinks))
    instinct_master.register(WarnInstinct(bus=bus, log_sinks=log_sinks))
    instinct_master.register(CriticalReflex(bus=bus, log_sinks=log_sinks))
    action_master.register(CoolFan(bus=bus, log_sinks=log_sinks))
    action_master.register(EmergencyStop(bus=bus, log_sinks=log_sinks))

    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=4.0,
        log_sinks=log_sinks,
        context_observers=[dashboard.submit_context],
        decision_observers=[dashboard.submit_decision],
    )

    await dashboard.start()
    print("Dashboard: http://127.0.0.1:8765", flush=True)
    await rt.start()
    try:
        await rt.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await rt.stop()
        await dashboard.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
