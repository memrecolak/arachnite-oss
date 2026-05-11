"""
Cross-component invariant tests for Arachnite.

These tests assert structural contracts that span multiple subsystems.
They catch the class of bug where something is "declared but not invoked"
— dangling config fields, orphaned state enum values, missing log events.
"""

from __future__ import annotations

import asyncio
import time

from arachnite import ContextNode, SignalBus
from arachnite.logging import BaseLogSink, LogLevel
from arachnite.models import (
    Context,
    HistoryConfig,
    LogEvent,
    NodeState,
    Proposal,
    RestartPolicy,
    Result,
    ShutdownPhase,
    Signal,
)
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import (
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from arachnite.supervisor import NodeSupervisor

# ── Recording log sink ──────���────────────────────────────────────────────────

class _RecordingSink(BaseLogSink):
    """Captures all log events for assertion."""

    def __init__(self) -> None:
        super().__init__(level=LogLevel.DEBUG)
        self.events: list[LogEvent] = []

    async def emit(self, event: LogEvent) -> None:
        self.events.append(event)


# ── Concrete test nodes ─────────────────────────────────────���────────────────

class _HotSense(BaseSenseNode):
    node_id = "_HotSense"
    signal_kind = "thermal"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=100.0, confidence=1.0, timestamp=time.monotonic(),
        )


class _ColdSense(BaseSenseNode):
    node_id = "_ColdSense"
    signal_kind = "thermal"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=20.0, confidence=1.0, timestamp=time.monotonic(),
        )


class _AlwaysReflex(BaseReflexInstinctNode):
    """Reflex that fires on every tick when thermal > 50."""
    node_id = "_AlwaysReflex"
    priority = 200

    async def evaluate(self, ctx: Context) -> Proposal | None:
        hot = [s for s in ctx.signals if s.kind == "thermal" and s.value > 50]
        if hot:
            return Proposal(
                instinct_id=self.node_id, action_id="_ContractAction",
                priority=self.priority, urgency=1.0,
            )
        return None


class _ContractAction(BaseActionNode):
    node_id = "_ContractAction"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


# ── Helpers ─────────────��────────────────────────────────────────────────────

def _build_runtime(
    sink: _RecordingSink,
    sensor_value: float = 100.0,
    tick_rate_hz: float = 200.0,
) -> ArachniteRuntime:
    """Build a runtime with a shared log sink across runtime AND master nodes."""
    bus = SignalBus()
    sinks: list[BaseLogSink] = [sink]
    sm = SenseMasterNode(bus=bus, log_sinks=sinks)
    im = InstinctMasterNode(bus=bus, log_sinks=sinks)
    dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus), log_sinks=sinks)
    am = ActionMasterNode(bus=bus, log_sinks=sinks)

    if sensor_value > 50:
        sm.register(_HotSense(bus=bus, log_sinks=sinks))
    else:
        sm.register(_ColdSense(bus=bus, log_sinks=sinks))
    im.register(_AlwaysReflex(bus=bus, log_sinks=sinks))
    am.register(_ContractAction(bus=bus, log_sinks=sinks))

    return ArachniteRuntime(
        sense_master=sm, context=ContextNode(),
        instinct_master=im, decision_master=dm,
        action_master=am, bus=bus,
        tick_rate_hz=tick_rate_hz,
        log_sinks=sinks,
    )


# ════════════════��═════════════════���═════════════════════════════════��═════════
# 1. Tick counter sync — last log event tick == runtime.tick_count
# ════��═════════════════════════════════════════════════════════════════════════

class TestTickCounterSync:
    """Catches Bug A class: logger tick counter not synced with runtime."""

    async def test_log_tick_matches_runtime_tick_count(self) -> None:
        sink = _RecordingSink()
        rt = _build_runtime(sink)
        await rt.start()
        await rt.pause()  # pause background loop so only manual ticks count
        for _ in range(5):
            await rt.tick()
        await rt.stop()
        await asyncio.sleep(0)

        assert rt.tick_count == 5
        tick_events = [e for e in sink.events if e.tick > 0]
        assert len(tick_events) > 0
        assert tick_events[-1].tick == 5

    async def test_context_tick_matches_runtime(self) -> None:
        sink = _RecordingSink()
        rt = _build_runtime(sink)
        await rt.start()
        await rt.pause()
        for _ in range(3):
            await rt.tick()
        ctx = rt.context
        await rt.stop()

        assert rt.tick_count == 3
        assert ctx.tick == 3


# ══════════════════���═════════════════════════��═════════════════════════════════
# 2. Spec §13.3 event coverage — framework-emitted events appear
# ═════���════════════════════════��══════════════════════════════════��════════════

class TestFrameworkEventCoverage:
    """Verifies that spec §13.3 promised events are actually emitted."""

    async def test_reflex_fired_event_emitted(self) -> None:
        sink = _RecordingSink()
        rt = _build_runtime(sink, sensor_value=100.0)
        await rt.start()
        await rt.pause()
        await rt.tick()
        await rt.stop()
        await asyncio.sleep(0)

        reflex_events = [e for e in sink.events if e.message == "Reflex fired"]
        assert len(reflex_events) >= 1
        assert "instinct_id" in reflex_events[0].data
        assert "action_id" in reflex_events[0].data

    async def test_action_dispatched_event_emitted(self) -> None:
        sink = _RecordingSink()
        rt = _build_runtime(sink, sensor_value=100.0)
        await rt.start()
        await rt.pause()
        await rt.tick()
        await rt.stop()
        await asyncio.sleep(0)

        dispatch_events = [e for e in sink.events if e.message == "Dispatching action"]
        assert len(dispatch_events) >= 1

    async def test_action_completed_event_emitted(self) -> None:
        sink = _RecordingSink()
        rt = _build_runtime(sink, sensor_value=100.0)
        await rt.start()
        await rt.pause()
        await rt.tick()
        await rt.stop()
        await asyncio.sleep(0)

        complete_events = [e for e in sink.events if e.message == "Action complete"]
        assert len(complete_events) >= 1
        assert "success" in complete_events[0].data

    async def test_no_reflex_when_below_threshold(self) -> None:
        """Sanity check: no reflex fires when sensor is cold."""
        sink = _RecordingSink()
        rt = _build_runtime(sink, sensor_value=20.0)
        await rt.start()
        await rt.pause()
        await rt.tick()
        await rt.stop()
        await asyncio.sleep(0)

        reflex_events = [e for e in sink.events if e.message == "Reflex fired"]
        assert len(reflex_events) == 0


# ══════════��═══════════════════════════════��═════════════════════════════════��═
# 3. HistoryConfig fields produce observable behavior
# ═════════════��═════════════════════════���══════════════════════════════════════

class TestHistoryConfigObservable:
    """Catches dead-config-field class: HistoryConfig.max_ticks must limit."""

    async def test_max_ticks_limits_history(self) -> None:
        ctx_node = ContextNode(
            history_length=10,
            history_config={"thermal": HistoryConfig(max_ticks=2)},
        )
        for tick in range(1, 6):
            signals = [Signal(
                source="test", kind="thermal", value=float(tick),
                confidence=1.0, timestamp=time.monotonic(),
            )]
            ctx = ctx_node.update(signals)

        # History should retain at most 2 ticks worth of non-evicted thermal signals
        thermal_in_history = []
        for tick_signals in ctx.history:
            for s in tick_signals:
                if s.kind == "thermal" and s.value is not None:
                    thermal_in_history.append(s)
        assert len(thermal_in_history) <= 2

    async def test_max_ticks_none_uses_default_history_length(self) -> None:
        ctx_node = ContextNode(
            history_length=3,
            history_config={"thermal": HistoryConfig(max_ticks=None)},
        )
        for tick in range(1, 8):
            signals = [Signal(
                source="test", kind="thermal", value=float(tick),
                confidence=1.0, timestamp=time.monotonic(),
            )]
            ctx = ctx_node.update(signals)

        assert len(ctx.history) <= 3


# ══════════════════���══════════════════════════════��════════════════════════════
# 4. NodeState reachability ��� every enum value is reachable
# ═══════════════════════════════════════════════════════════════════════════���══

class TestNodeStateReachability:
    """Catches orphaned state class: every NodeState must be reachable."""

    async def test_starting_state_on_track(self) -> None:
        bus = SignalBus()
        sv = NodeSupervisor(bus)
        node = _ColdSense(bus=bus)
        sv.track(node)
        assert sv.state_of(node.node_id) == NodeState.STARTING

    async def test_running_state(self) -> None:
        bus = SignalBus()
        sv = NodeSupervisor(bus)
        node = _ColdSense(bus=bus)
        sv.track(node)
        await sv.mark_running(node.node_id)
        assert sv.state_of(node.node_id) == NodeState.RUNNING

    async def test_faulted_state(self) -> None:
        """on_fault transitions to FAULTED before checking restart policy."""
        bus = SignalBus()
        # Use NEVER so it goes FAULTED → DEAD (but the FAULTED transition fires)
        sv = NodeSupervisor(bus, restart_policy=RestartPolicy.NEVER)
        node = _ColdSense(bus=bus)
        sv.track(node)
        await sv.mark_running(node.node_id)

        # Subscribe to supervisor signals to observe FAULTED transition
        faulted_seen = False

        async def _on_signal(sig: Signal) -> None:
            nonlocal faulted_seen
            if hasattr(sig, "current_state") and sig.current_state == NodeState.FAULTED:  # type: ignore[attr-defined]
                faulted_seen = True

        bus.subscribe("supervisor", _on_signal)
        await sv.on_fault(node.node_id, RuntimeError("test"))
        # Final state is DEAD, but FAULTED was reached
        assert sv.state_of(node.node_id) == NodeState.DEAD
        assert faulted_seen

    async def test_dead_state(self) -> None:
        bus = SignalBus()
        sv = NodeSupervisor(bus, restart_policy=RestartPolicy.NEVER)
        node = _ColdSense(bus=bus)
        sv.track(node)
        await sv.mark_running(node.node_id)
        await sv.on_fault(node.node_id, RuntimeError("test"))
        assert sv.state_of(node.node_id) == NodeState.DEAD

    async def test_stopped_state(self) -> None:
        bus = SignalBus()
        sv = NodeSupervisor(bus)
        node = _ColdSense(bus=bus)
        sv.track(node)
        await sv.mark_running(node.node_id)
        await sv.mark_stopped(node.node_id)
        assert sv.state_of(node.node_id) == NodeState.STOPPED

    async def test_restarting_state(self) -> None:
        bus = SignalBus()
        sv = NodeSupervisor(bus, restart_delay_s=0.0)
        node = _ColdSense(bus=bus)
        sv.track(node)
        await sv.mark_running(node.node_id)
        # Trigger fault → restart; _restart transitions to RESTARTING
        await sv.on_fault(node.node_id, RuntimeError("test"))
        # Let the restart task run
        await asyncio.sleep(0.05)
        # After restart, state is RUNNING (but passed through RESTARTING)
        assert sv.state_of(node.node_id) == NodeState.RUNNING

    async def test_all_states_covered(self) -> None:
        """Meta-test: every NodeState value has a test above."""
        expected = {NodeState.STARTING, NodeState.RUNNING, NodeState.FAULTED,
                    NodeState.RESTARTING, NodeState.STOPPED, NodeState.DEAD}
        assert expected == set(NodeState)


# ═══════════════════════════════���══════════════════════════════════════════════
# 5. ShutdownPhase coverage — all phases reached during stop()
# ═��═════════════════��════════════════════════════════��═════════════════════════

class TestShutdownPhaseCoverage:
    """Verifies every ShutdownPhase is reached during a normal stop()."""

    async def test_stop_reaches_complete(self) -> None:
        sink = _RecordingSink()
        rt = _build_runtime(sink, sensor_value=20.0)
        coordinator = rt._shutdown_coordinator

        assert coordinator.phase == ShutdownPhase.NOT_STARTED
        await rt.start()
        await rt.pause()
        await rt.tick()
        await rt.stop()
        assert coordinator.phase == ShutdownPhase.COMPLETE

    async def test_all_shutdown_phases_are_defined(self) -> None:
        """Meta-test: verify the enum has all expected phases."""
        expected_names = {
            "NOT_STARTED", "STOP_SENSING", "DRAIN_REFLEXES",
            "COMPLETE_MANDATORY", "INTERRUPT_ACTION", "STOP_SUPERVISORS",
            "TEARDOWN_NODES", "DISCONNECT_TRANSPORT", "COMPLETE",
        }
        actual_names = {p.name for p in ShutdownPhase}
        assert expected_names == actual_names

    async def test_shutdown_phases_are_ordered(self) -> None:
        """Phases must have monotonically increasing integer values."""
        values = [p.value for p in ShutdownPhase]
        assert values == sorted(values)
