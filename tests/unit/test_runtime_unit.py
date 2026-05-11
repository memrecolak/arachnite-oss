"""Unit tests for ArachniteRuntime — edge cases not exercised by integration tests."""

from __future__ import annotations

import time

import pytest

import arachnite.runtime as _runtime_module
from arachnite import ContextNode, SignalBus
from arachnite.models import (
    ActionStep,
    InterruptPolicy,
    InterruptRequest,
    Proposal,
    Result,
    StepResult,
)
from arachnite.nodes.action import ActionMasterNode, BaseActionNode, MultiStepActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from tests.conftest import ConstantSenseNode, RecordingAction

# ── Helpers ────────────────────────────────────────────────────────────────────

def _build_rt(
    tick_rate_hz: float = 1000.0,
    sense_value: float = 25.0,
) -> tuple[ArachniteRuntime, ActionMasterNode, RecordingAction]:
    bus = SignalBus()
    sm  = SenseMasterNode(bus=bus)
    im  = InstinctMasterNode(bus=bus)
    dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am  = ActionMasterNode(bus=bus)

    sm.register(ConstantSenseNode(bus=bus, value=sense_value))
    action = RecordingAction(bus=bus)
    am.register(action)

    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(),
        instinct_master=im, decision_master=dm,
        action_master=am, bus=bus,
        tick_rate_hz=tick_rate_hz,
    )
    return rt, am, action


# ── bus property ──────────────────────────────────────────────────────────────

class TestRuntimeBusProperty:
    @pytest.mark.asyncio
    async def test_bus_property_returns_signal_bus(self) -> None:
        rt, _, _ = _build_rt()
        await rt.start()
        assert isinstance(rt.bus, SignalBus)
        await rt.stop()


# ── Pause / Resume — idempotence guards ───────────────────────────────────────

class TestPauseResumeIdempotence:
    @pytest.mark.asyncio
    async def test_double_pause_is_noop(self) -> None:
        rt, _, _ = _build_rt()
        await rt.start()
        await rt.pause()
        await rt.pause()          # second call hits early-return guard (line 205)
        assert rt.is_paused
        await rt.stop()

    @pytest.mark.asyncio
    async def test_resume_when_not_paused_is_noop(self) -> None:
        rt, _, _ = _build_rt()
        await rt.start()
        await rt.resume()         # not paused → hits early-return guard (line 216)
        assert not rt.is_paused
        await rt.stop()


# ── Emergency stop ────────────────────────────────────────────────────────────

class _RaisingInterruptAction(BaseActionNode):
    """request_interrupt raises so we exercise the except branch in emergency_stop."""
    node_id = "_RaisingInterruptAction"

    def request_interrupt(self, req: InterruptRequest) -> None:
        raise RuntimeError("simulated interrupt failure")

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class TestEmergencyStop:
    @pytest.mark.asyncio
    async def test_emergency_stop_no_current_action(self) -> None:
        rt, _, _ = _build_rt()
        await rt.start()
        await rt.emergency_stop()
        assert not rt.is_running

    @pytest.mark.asyncio
    async def test_emergency_stop_calls_and_swallows_interrupt_error(self) -> None:
        rt, am, _ = _build_rt()
        await rt.start()
        # Place a node whose request_interrupt raises — emergency_stop must not propagate
        node = _RaisingInterruptAction(bus=rt.bus)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = Proposal(
            instinct_id="test", action_id=node.node_id, priority=100, urgency=0.5,
        )
        await rt.emergency_stop()   # lines 187-189: call + except + pass
        assert not rt.is_running


# ── Tick — action_state path ──────────────────────────────────────────────────

class _IdleMultiStep(MultiStepActionNode):
    node_id          = "_IdleMultiStep"
    interrupt_policy = InterruptPolicy.ALWAYS

    def steps(self) -> list[ActionStep]:
        return [ActionStep("only", interruptible=True)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        return StepResult(step_name=step.name, success=True)


class TestTickActionState:
    @pytest.mark.asyncio
    async def test_tick_reads_execution_state_when_multistep_is_current(self) -> None:
        rt, am, _ = _build_rt()
        await rt.start()
        # Simulate a MultiStepActionNode sitting as the current node
        node = _IdleMultiStep(bus=rt.bus)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = Proposal(
            instinct_id="test", action_id=node.node_id, priority=100, urgency=0.5,
        )
        await rt.tick()   # line 270: execution_state() is called
        await rt.stop()


# ── Tick — interrupt request path ─────────────────────────────────────────────

class _HighPriInstinct(BaseInstinctNode):
    node_id  = "_HighPriInstinct"
    priority = 200

    async def evaluate(self, ctx: object) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="_HighPriAction",
            priority=200, urgency=1.0,
        )


class _HighPriAction(BaseActionNode):
    node_id = "_HighPriAction"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class TestTickInterruptPath:
    @pytest.mark.asyncio
    async def test_interrupt_request_issued_when_new_proposal_outranks_current(
        self,
    ) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus, value=25.0))
        im.register(_HighPriInstinct(bus=bus))
        am.register(_HighPriAction(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()

        # Low-priority current proposal → high-priority instinct outranks → interrupt issued
        am._running_proposals["old_act"] = Proposal(
            instinct_id="old", action_id="old_act", priority=10, urgency=0.1,
        )
        await rt.tick()   # lines 294-295: interrupt_req is not None, request_interrupt called
        await rt.stop()


# ── Loop — tick exception is caught ──────────────────────────────────────────

class TestLoopExceptionHandling:
    @pytest.mark.asyncio
    async def test_loop_catches_exception_and_continues(self) -> None:
        rt, _, _ = _build_rt()
        rt._running = True
        call_count = 0

        async def controlled_tick() -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated tick failure")
            rt._running = False  # stop after second call

        rt.tick = controlled_tick  # type: ignore[method-assign]
        await rt._loop()           # line 236: error is logged, loop continues
        assert call_count == 2


# ── Loop — overrun warning ────────────────────────────────────────────────────

class TestLoopOverrunWarning:
    @pytest.mark.asyncio
    async def test_loop_logs_overrun_when_elapsed_exceeds_interval(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rt, _, _ = _build_rt(tick_rate_hz=10.0)  # 0.1 s interval
        rt._running = True
        t0         = time.monotonic()
        call_count = [0]

        def fake_monotonic() -> float:
            call_count[0] += 1
            # First call: record start; second call: report 10 s elapsed
            return t0 if call_count[0] == 1 else t0 + 10.0

        async def one_shot_tick() -> None:
            rt._running = False

        monkeypatch.setattr(_runtime_module.time, "monotonic", fake_monotonic)
        rt.tick = one_shot_tick  # type: ignore[method-assign]
        await rt._loop()  # lines 247-248: overrun warning emitted


# ── Tick — overrun warning ────────────────────────────────────────────────────

class TestTickOverrunWarning:
    @pytest.mark.asyncio
    async def test_tick_logs_overrun_when_duration_exceeds_interval(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        rt, _, _ = _build_rt(tick_rate_hz=10.0)  # 0.1 s interval
        await rt.start()
        await rt.stop()  # stop the background loop before patching time

        t0         = time.monotonic()
        call_count = [0]

        def fake_monotonic() -> float:
            call_count[0] += 1
            return t0 if call_count[0] == 1 else t0 + 10.0

        monkeypatch.setattr(_runtime_module.time, "monotonic", fake_monotonic)
        await rt.tick()  # line 309: tick overrun warning logged


# ── P0 #6: ActionNotFoundError in reflex dispatch ────────────────────────────

class _BadReflexInstinct(BaseInstinctNode):
    """Reflex that references a non-existent action."""
    node_id  = "_BadReflexInstinct"
    priority = 200
    reflex   = True

    async def evaluate(self, ctx: object) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id,
            action_id="NonExistentAction",
            priority=self.priority,
            urgency=1.0,
        )


class TestReflexActionNotFound:
    @pytest.mark.asyncio
    async def test_tick_survives_action_not_found_in_reflex(self) -> None:
        """Tick loop must not crash when a reflex references a missing action."""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus, value=25.0))
        im.register(_BadReflexInstinct(bus=bus))
        # Intentionally NOT registering "NonExistentAction"

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        # Should not raise — the missing action is logged and skipped
        await rt.tick()
        await rt.tick()  # second tick also works
        await rt.stop()


# ── P0 #13/#19: Double tick increment ────────────────────────────────────────

class _AlwaysFireInstinct(BaseInstinctNode):
    """Instinct that always fires, targeting RecordingAction."""
    node_id  = "_AlwaysFireInstinct"
    priority = 50

    async def evaluate(self, ctx: object) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id,
            action_id="RecordingAction",
            priority=self.priority,
            urgency=0.5,
        )


class TestNoDoubleTick:
    @pytest.mark.asyncio
    async def test_tick_count_matches_context_tick(self) -> None:
        """runtime.tick_count and context.tick must stay in sync."""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus, value=25.0))
        im.register(_AlwaysFireInstinct(bus=bus))
        am.register(RecordingAction(bus=bus))

        ctx = ContextNode()
        rt = ArachniteRuntime(
            sense_master=sm, context=ctx,
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        # Pause the background loop so it does not race with the manual ticks
        # below — ``rt.tick()`` is a testing shim that bypasses ``_paused`` by
        # design (see ``ArachniteRuntime.tick`` docstring).
        await rt.pause()

        for _ in range(5):
            await rt.tick()

        assert rt.tick_count == ctx.tick, (
            f"tick_count={rt.tick_count} != context.tick={ctx.tick}"
        )
        await rt.stop()


# ── audit 2026-04-16 Bug A: logger tick-counter wiring ──────────────────────


class TestLoggerTickWiring:
    """
    LogEvent.tick must equal runtime.tick_count after each tick.

    Regression for the bug where StructuredLogger._set_tick was defined but
    never called by the runtime — every framework log line shipped tick=0.
    """

    @pytest.mark.asyncio
    async def test_runtime_master_and_leaf_loggers_synced(self) -> None:
        from arachnite.logging import BaseLogSink, LogEvent, LogLevel

        captured: list[LogEvent] = []

        class CaptureSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                captured.append(event)

        sink = CaptureSink(level=LogLevel.DEBUG)

        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus, log_sinks=[sink])
        im  = InstinctMasterNode(bus=bus, log_sinks=[sink])
        dm  = DecisionMasterNode(
            bus=bus, log_sinks=[sink], strategy=GreedyDecisionNode(bus=bus),
        )
        am  = ActionMasterNode(bus=bus, log_sinks=[sink])
        leaf = ConstantSenseNode(bus=bus, value=25.0, log_sinks=[sink])
        sm.register(leaf)

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
            log_sinks=[sink],
        )

        # Drive ticks directly (no background loop) so we can measure
        # counter sync independently of the loop.
        for m in (sm, im, dm, am):
            await m.setup()

        for _ in range(3):
            await rt.tick()

        # Emit one log per layer to capture the synced tick on each.
        rt._logger.info("runtime probe")
        sm.logger.info("sense master probe")
        im.logger.info("instinct master probe")
        dm.logger.info("decision master probe")
        am.logger.info("action master probe")
        leaf.logger.info("leaf probe")

        # Allow fire-and-forget tasks to flush.
        import asyncio as _asyncio
        await _asyncio.sleep(0)

        for m in (am, dm, im, sm):
            await m.teardown()

        # The runtime ran 3 ticks; every probe emitted afterwards should
        # carry tick=3.
        probes = [ev for ev in captured if "probe" in ev.message]
        assert probes, "expected at least one probe log event"
        for ev in probes:
            assert ev.tick == rt.tick_count == 3, (
                f"expected tick=3 on {ev.node_id}, got tick={ev.tick}"
            )

    @pytest.mark.asyncio
    async def test_set_tick_called_via_default_on_tick_start(self) -> None:
        """The default BaseNode.on_tick_start must sync the leaf logger."""
        from arachnite.bus import SignalBus as _Bus

        bus = _Bus()
        leaf = ConstantSenseNode(bus=bus, value=1.0)
        assert leaf.logger._tick == 0
        await leaf.on_tick_start(42)
        assert leaf.logger._tick == 42


# ── C1 (audit 2026-04-16 follow-up): reflex.fired LogEvent ──────────────────


class TestReflexFiredEvent:
    """
    Spec §13.3 promises a `reflex.fired` framework event. The runtime must
    emit ``logger.info("Reflex fired", ...)`` once per reflex activation
    *before* dispatch, with `instinct_id`, `action_id`, `priority`, `urgency`
    in the data payload, so safety auditors can reconstruct every reflex-arc
    activation from the log stream alone.
    """

    @pytest.mark.asyncio
    async def test_reflex_fired_event_emitted_with_payload(self) -> None:
        from arachnite.logging import BaseLogSink, LogEvent, LogLevel
        from tests.conftest import EmergencyReflex

        captured: list[LogEvent] = []

        class CaptureSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                captured.append(event)

        sink = CaptureSink(level=LogLevel.INFO)

        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus, log_sinks=[sink])
        im  = InstinctMasterNode(bus=bus, log_sinks=[sink])
        dm  = DecisionMasterNode(
            bus=bus, log_sinks=[sink], strategy=GreedyDecisionNode(bus=bus),
        )
        am  = ActionMasterNode(bus=bus, log_sinks=[sink])

        # Sense feeds a thermal signal (default kind) above the reflex
        # threshold (95).
        sm.register(ConstantSenseNode(bus=bus, value=99.0))
        im.register(EmergencyReflex(bus=bus))

        # The reflex targets "EmergencyStop"; register a matching action.
        class _EmergencyStop(RecordingAction):
            node_id = "EmergencyStop"
        am.register(_EmergencyStop(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
            log_sinks=[sink],
        )

        for m in (sm, im, dm, am):
            await m.setup()
        await rt.tick()

        import asyncio as _asyncio
        await _asyncio.sleep(0)

        for m in (am, dm, im, sm):
            await m.teardown()

        fired = [ev for ev in captured if ev.message == "Reflex fired"]
        assert len(fired) == 1, (
            f"expected exactly one 'Reflex fired' event, got {len(fired)}"
        )
        ev = fired[0]
        assert ev.level == LogLevel.INFO
        assert ev.data["instinct_id"] == "EmergencyReflex"
        assert ev.data["action_id"] == "EmergencyStop"
        assert ev.data["priority"] == 200
        assert ev.data["urgency"] == 1.0


# ── C2 (audit 2026-04-16): runtime overrun warning rate-limit ───────────────


class TestOverrunWarnConsecutive:
    """
    The runtime tick-loop must align with ``TickBudgetMonitor``: only emit
    "Tick overrun" after ``overrun_warn_consecutive`` consecutive overruns,
    and reset the counter on the first non-overrunning tick. Default is 3.
    """

    @staticmethod
    def _make_rt(consecutive: int) -> ArachniteRuntime:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)
        return ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=10.0,
            overrun_warn_consecutive=consecutive,
        )

    def test_default_threshold_is_three(self) -> None:
        rt = self._make_rt(consecutive=3)
        assert rt._overrun_warn_consecutive == 3
        assert rt._consecutive_overruns == 0

    def test_minimum_threshold_clamped_to_one(self) -> None:
        """Passing 0 or negative must be clamped to 1 (at least one overrun
        before emitting), not silently accept ``0`` which would mean "always"."""
        rt = self._make_rt(consecutive=0)
        assert rt._overrun_warn_consecutive == 1
        rt2 = self._make_rt(consecutive=-5)
        assert rt2._overrun_warn_consecutive == 1


# ── P1 #10: Mutable class-level defaults ─────────────────────────────────────

class TestMutableClassDefaults:
    def test_permissions_are_per_instance(self) -> None:
        """Mutating permissions on one node must not affect another (#10)"""
        from arachnite.models import Permission
        bus = SignalBus()
        a = RecordingAction(bus=bus)
        b = RecordingAction(bus=bus)
        a.permissions.add(Permission.NETWORK)
        assert Permission.NETWORK not in b.permissions

    def test_requires_are_per_instance(self) -> None:
        """Mutating requires on one node must not affect another (#10)"""
        bus = SignalBus()
        a = RecordingAction(bus=bus)
        b = RecordingAction(bus=bus)
        a.requires.append("SomeDep")
        assert "SomeDep" not in b.requires


# ── P1 #11: setup() failure tears down already-started masters ───────────────

class _FailingSetupSense(ConstantSenseNode):
    node_id = "_FailingSetupSense"

    async def setup(self) -> None:
        raise RuntimeError("sense setup boom")


class _TrackingTeardownSense(ConstantSenseNode):
    """Sense node that records whether its teardown() was called."""
    node_id = "_TrackingTeardownSense"
    torn_down = False

    async def teardown(self) -> None:
        _TrackingTeardownSense.torn_down = True


class TestSetupFailureTeardown:
    @pytest.mark.asyncio
    async def test_setup_failure_tears_down_succeeded_masters(self) -> None:
        """If one master's setup() fails, already-started masters are torn down (#11).

        Masters set up in order sense → instinct → decision → action. When the
        action master's setup raises, the runtime must call teardown() on the
        three already-started masters in reverse order. Teardown cascades to
        each master's child nodes, so this test registers a tracking sense
        node on the sense master and asserts its teardown ran.
        """
        _TrackingTeardownSense.torn_down = False
        bus = SignalBus()
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))

        sm2  = SenseMasterNode(bus=bus)
        im2  = InstinctMasterNode(bus=bus)
        am2  = ActionMasterNode(bus=bus)
        sm2.register(_TrackingTeardownSense(bus=bus))

        class _FailingSetupAction(BaseActionNode):
            node_id = "_FailingSetupAction"
            async def setup(self) -> None:
                raise RuntimeError("action setup boom")
            async def execute(self, proposal: Proposal) -> Result:
                return Result(action_id=self.node_id, success=True)

        am2.register(_FailingSetupAction(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm2, context=ContextNode(),
            instinct_master=im2, decision_master=dm,
            action_master=am2, bus=bus, tick_rate_hz=1000.0,
        )
        with pytest.raises(RuntimeError, match="action setup boom"):
            await rt.start()
        assert not rt.is_running
        assert _TrackingTeardownSense.torn_down is True, (
            "sense master's teardown must have cascaded to its child sense node "
            "after action master's setup failed"
        )


# ── P1 #28: Live registration setup() failure unregisters ────────────────────

class _FailingSetupActionLive(BaseActionNode):
    node_id = "_FailingSetupActionLive"

    async def setup(self) -> None:
        raise RuntimeError("live setup boom")

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class TestLiveRegistrationSetupFailure:
    @pytest.mark.asyncio
    async def test_failed_live_registration_unregisters_node(self) -> None:
        """If setup() fails during live registration, node is unregistered (#28)"""
        rt, am, _ = _build_rt()
        await rt.start()

        node = _FailingSetupActionLive(bus=rt.bus)
        with pytest.raises(RuntimeError, match="live setup boom"):
            await rt.register_action_live(node)

        # Node must not remain registered
        assert node.node_id not in am._nodes
        await rt.stop()


# ── P1 #17/#18: Pause/resume forwarded to instinct and action children ───────

class _PauseTrackingInstinct(BaseInstinctNode):
    node_id = "_PauseTrackingInstinct"
    priority = 50
    paused = False
    resumed = False

    async def on_pause(self) -> None:
        _PauseTrackingInstinct.paused = True

    async def on_resume(self) -> None:
        _PauseTrackingInstinct.resumed = True

    async def evaluate(self, ctx: object) -> Proposal | None:
        return None


class _PauseTrackingAction(BaseActionNode):
    node_id = "_PauseTrackingAction"
    paused = False
    resumed = False

    async def on_pause(self) -> None:
        _PauseTrackingAction.paused = True

    async def on_resume(self) -> None:
        _PauseTrackingAction.resumed = True

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class TestPauseResumeForwarding:
    @pytest.mark.asyncio
    async def test_pause_resume_forwarded_to_instinct_and_action_children(self) -> None:
        """on_pause/on_resume must reach leaf instinct and action nodes (#17/#18)"""
        _PauseTrackingInstinct.paused = False
        _PauseTrackingInstinct.resumed = False
        _PauseTrackingAction.paused = False
        _PauseTrackingAction.resumed = False

        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus))
        im.register(_PauseTrackingInstinct(bus=bus))
        am.register(_PauseTrackingAction(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        await rt.pause()
        assert _PauseTrackingInstinct.paused
        assert _PauseTrackingAction.paused
        await rt.resume()
        assert _PauseTrackingInstinct.resumed
        assert _PauseTrackingAction.resumed
        await rt.stop()


# ── P1 #7/#29: Reflex results accumulate and merge with normal pipeline ──────

class _ReflexA(BaseInstinctNode):
    node_id = "_ReflexA"
    priority = 210
    reflex = True

    async def evaluate(self, ctx: object) -> Proposal | None:
        return Proposal(instinct_id=self.node_id, action_id="RecordingAction",
                        priority=self.priority, urgency=1.0)


class _ReflexB(BaseInstinctNode):
    node_id = "_ReflexB"
    priority = 200
    reflex = True

    async def evaluate(self, ctx: object) -> Proposal | None:
        return Proposal(instinct_id=self.node_id, action_id="RecordingAction",
                        priority=self.priority, urgency=1.0)


class TestReflexResultAccumulation:
    @pytest.mark.asyncio
    async def test_multiple_reflex_results_all_preserved(self) -> None:
        """All reflex results must be accumulated, not overwritten (#7)"""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus))
        im.register(_ReflexA(bus=bus))
        im.register(_ReflexB(bus=bus))
        action = RecordingAction(bus=bus)
        am.register(action)

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        await rt.pause()  # stop the background loop racing the manual tick
        action.calls.clear()  # clear any calls from background loop
        # Stay paused — ``rt.tick()`` is a testing shim that bypasses
        # ``_paused`` by design (see ``ArachniteRuntime.tick`` docstring).
        # Resuming here would let the background loop reset ``_last_results``
        # mid-assertion (line 519 of runtime.py).
        await rt.tick()

        # Both reflexes fired → both results in _last_results
        assert len(action.calls) >= 2
        assert len(rt._last_results) >= 2
        await rt.stop()


# ── P1 #3: poll_interval_s enforced ──────────────────────────────────────────

class _SlowSense(ConstantSenseNode):
    node_id = "_SlowSense"
    poll_interval_s = 10.0  # very long interval


class TestPollIntervalEnforced:
    @pytest.mark.asyncio
    async def test_slow_sensor_skipped_on_fast_ticks(self) -> None:
        """SenseMasterNode must skip nodes whose poll_interval hasn't elapsed (#3)"""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        slow = _SlowSense(bus=bus)
        sm.register(slow)

        # First read should succeed (last_read_time=0.0)
        signals1 = await sm.read_all()
        assert len(signals1) == 1

        # Immediate second read should be skipped
        signals2 = await sm.read_all()
        assert len(signals2) == 0


# ── P1 #1: on_tick_start/on_tick_end called ──────────────────────────────────

class _TickTrackingSense(ConstantSenseNode):
    node_id = "_TickTrackingSense"
    tick_starts: list[int] = []
    tick_ends: list[int] = []

    async def on_tick_start(self, tick: int) -> None:
        _TickTrackingSense.tick_starts.append(tick)

    async def on_tick_end(self, tick: int, duration_s: float) -> None:
        _TickTrackingSense.tick_ends.append(tick)


class TestTickHooksCalled:
    @pytest.mark.asyncio
    async def test_on_tick_start_and_end_called(self) -> None:
        """on_tick_start/on_tick_end must be called each tick (#1)"""
        _TickTrackingSense.tick_starts = []
        _TickTrackingSense.tick_ends = []

        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)
        sm.register(_TickTrackingSense(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        await rt.pause()
        _TickTrackingSense.tick_starts.clear()
        _TickTrackingSense.tick_ends.clear()
        await rt.resume()
        await rt.tick()
        await rt.tick()
        await rt.stop()

        assert len(_TickTrackingSense.tick_starts) >= 2
        assert len(_TickTrackingSense.tick_ends) >= 2


# ── P1 #2: on_proposal_rejected called ──────────────────────────────────────

class _RejectionTrackingInstinct(BaseInstinctNode):
    node_id = "_RejectionTracker"
    priority = 10  # low priority — will be rejected
    rejected: list[Proposal] = []

    async def evaluate(self, ctx: object) -> Proposal | None:
        return Proposal(instinct_id=self.node_id, action_id="RecordingAction",
                        priority=self.priority, urgency=0.1)

    async def on_proposal_rejected(self, proposal: Proposal) -> None:
        _RejectionTrackingInstinct.rejected.append(proposal)


class TestProposalRejectedCalled:
    @pytest.mark.asyncio
    async def test_on_proposal_rejected_called_for_losing_instinct(self) -> None:
        """on_proposal_rejected must be called when a proposal loses (#2)"""
        _RejectionTrackingInstinct.rejected = []

        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus))
        im.register(_AlwaysFireInstinct(bus=bus))       # priority=50, wins
        im.register(_RejectionTrackingInstinct(bus=bus)) # priority=10, loses
        am.register(RecordingAction(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        await rt.pause()
        _RejectionTrackingInstinct.rejected.clear()
        await rt.resume()
        await rt.tick()
        await rt.stop()

        assert len(_RejectionTrackingInstinct.rejected) >= 1
        assert _RejectionTrackingInstinct.rejected[0].instinct_id == "_RejectionTracker"


# ── A-07: unregister_instinct_live clears pending proposals ─────────────────

class _PersistInstinct(BaseInstinctNode):
    """Instinct that always emits a persist=True proposal."""
    node_id  = "_PersistInstinct"
    priority = 60

    async def evaluate(self, ctx: object) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="RecordingAction",
            priority=self.priority, urgency=0.5, persist=True,
        )


class TestUnregisterInstinctClearsPending:
    @pytest.mark.asyncio
    async def test_unregister_instinct_live_clears_pending_in_decision_master(
        self,
    ) -> None:
        """Unregistering an instinct must clean up _pending and _pending_ages (A-07)"""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus))
        im.register(_PersistInstinct(bus=bus))
        action = RecordingAction(bus=bus)
        am.register(action)

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        await rt.pause()
        action.calls.clear()

        # Manually inject a pending proposal to simulate the persist path
        # (the action may dispatch immediately in a tick, so seed directly)
        dm._pending["_PersistInstinct"] = Proposal(
            instinct_id="_PersistInstinct", action_id="RecordingAction",
            priority=60, urgency=0.5, persist=True,
        )
        dm._pending_ages["_PersistInstinct"] = 3

        assert "_PersistInstinct" in dm.pending_proposals
        assert "_PersistInstinct" in dm._pending_ages

        await rt.unregister_instinct_live("_PersistInstinct")

        assert "_PersistInstinct" not in dm.pending_proposals
        assert "_PersistInstinct" not in dm._pending_ages
        await rt.stop()


# ── A-09: _last_results / _last_result stale across ticks ────────────────────

class _OnceFireInstinct(BaseInstinctNode):
    """Fires on tick 1 only, returns None on all subsequent ticks."""
    node_id  = "_OnceFireInstinct"
    priority = 50

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self._fired = False

    async def evaluate(self, ctx: object) -> Proposal | None:
        if not self._fired:
            self._fired = True
            return Proposal(
                instinct_id=self.node_id,
                action_id="RecordingAction",
                priority=self.priority,
                urgency=0.5,
            )
        return None


class TestStaleResultsCleared:
    @pytest.mark.asyncio
    async def test_last_results_reset_when_no_action_dispatched(self) -> None:
        """_last_results and _last_result must be [] / None on ticks with no dispatch (A-09)"""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus, value=25.0))
        im.register(_OnceFireInstinct(bus=bus))
        action = RecordingAction(bus=bus)
        am.register(action)

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        await rt.pause()
        action.calls.clear()

        # Tick 1: instinct fires, action dispatched — results populated
        await rt.tick()
        assert len(action.calls) == 1, "tick 1 should dispatch exactly one action"
        assert len(rt._last_results) == 1
        assert rt._last_result is not None

        # Tick 2: instinct returns None — no proposals, no dispatch
        await rt.tick()
        assert rt._last_results == [], "_last_results must be empty after idle tick"
        assert rt._last_result is None, "_last_result must be None after idle tick"
        await rt.stop()


# ── A-10: Rejected-instinct notification ignores pending proposals ──────────

class _PersistLowPriInstinct(BaseInstinctNode):
    """Persist=True instinct with low priority, always fires."""
    node_id  = "_PersistLowPriInstinct"
    priority = 30

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.rejected_proposals: list[Proposal] = []
        self._fire_count = 0
        self.max_fires = 1  # only fire on first evaluation

    async def evaluate(self, ctx: object) -> Proposal | None:
        self._fire_count += 1
        if self._fire_count <= self.max_fires:
            return Proposal(
                instinct_id=self.node_id,
                action_id="RecordingAction",
                priority=self.priority,
                urgency=0.3,
                persist=True,
            )
        return None

    async def on_proposal_rejected(self, proposal: Proposal) -> None:
        self.rejected_proposals.append(proposal)


class _HighPriWinnerInstinct(BaseInstinctNode):
    """High-priority instinct that always wins the decision."""
    node_id  = "_HighPriWinnerInstinct"
    priority = 90

    async def evaluate(self, ctx: object) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id,
            action_id="RecordingAction",
            priority=self.priority,
            urgency=0.9,
        )


class TestPendingProposalRejectionNotification:
    @pytest.mark.asyncio
    async def test_pending_proposal_receives_rejection_callback(self) -> None:
        """Pending proposals that lose the decision must receive on_proposal_rejected (A-10)

        Tick 1: PersistLow fires (persist=True, priority=30), HighWinner fires
                (priority=90). HighWinner wins → PersistLow.on_proposal_rejected called.
                PersistLow's proposal enters pending.
        Tick 2: PersistLow does NOT fire (max_fires=1) but is evaluated, so its
                pending entry is cleared because it returned None. However, the
                trigger_interval_s gating is what causes the carry-forward scenario.
                Instead we use signal gating to prevent evaluation.
        """
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus, value=25.0))
        persist_inst = _PersistLowPriInstinct(bus=bus)
        persist_inst.max_fires = 999  # always fire
        im.register(persist_inst)
        im.register(_HighPriWinnerInstinct(bus=bus))
        action = RecordingAction(bus=bus)
        am.register(action)

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        await rt.pause()
        persist_inst.rejected_proposals.clear()

        # Tick 1: both fire, HighWinner wins, PersistLow rejected
        await rt.tick()
        assert len(persist_inst.rejected_proposals) >= 1
        assert persist_inst.rejected_proposals[-1].instinct_id == "_PersistLowPriInstinct"

        await rt.stop()

    @pytest.mark.asyncio
    async def test_carried_forward_pending_proposal_rejected_when_not_dispatched(
        self,
    ) -> None:
        """A pending proposal carried forward from a previous tick must receive
        on_proposal_rejected when it loses the decision again (A-10).

        This is the core bug: before the fix, only this tick's fresh proposals
        were checked for rejection. Pending proposals that were merged in by
        DecisionMasterNode but not dispatched were silently ignored.
        """
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        im  = InstinctMasterNode(bus=bus)
        dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am  = ActionMasterNode(bus=bus)

        sm.register(ConstantSenseNode(bus=bus, value=25.0))
        # PersistLow: fires tick 1 only, then throttled via trigger_interval_s
        persist_inst = _PersistLowPriInstinct(bus=bus)
        persist_inst.max_fires = 1
        persist_inst.trigger_interval_s = 9999.0  # effectively throttled after first fire
        im.register(persist_inst)
        im.register(_HighPriWinnerInstinct(bus=bus))
        action = RecordingAction(bus=bus)
        am.register(action)

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=1000.0,
        )
        await rt.start()
        await rt.pause()
        persist_inst.rejected_proposals.clear()

        # Tick 1: PersistLow fires (persist=True), HighWinner wins.
        # PersistLow's proposal enters pending and is rejected.
        await rt.tick()
        tick1_rejections = len(persist_inst.rejected_proposals)
        assert tick1_rejections >= 1, "tick 1: PersistLow should be rejected"

        # Tick 2: PersistLow is throttled (trigger_interval_s=9999), not evaluated.
        # Its pending proposal carries forward, merges into the decision pool,
        # loses to HighWinner again. The fix ensures on_proposal_rejected is called.
        await rt.tick()
        tick2_rejections = len(persist_inst.rejected_proposals)
        assert tick2_rejections > tick1_rejections, (
            f"tick 2: pending proposal must receive on_proposal_rejected "
            f"(had {tick1_rejections} after tick 1, expected more after tick 2, "
            f"got {tick2_rejections})"
        )
        assert persist_inst.rejected_proposals[-1].instinct_id == "_PersistLowPriInstinct"

        await rt.stop()


# ── Fix 4: emergency_stop log events ─────────────────────────────────────────


class TestEmergencyStopLogEvents:
    """emergency_stop() must emit 'Emergency stop initiated' and per-node
    'Emergency interrupt delivered' log events so benchmarks can timestamp
    externally."""

    @pytest.mark.asyncio
    async def test_emergency_stop_emits_initiated_event(self) -> None:
        from arachnite.logging import BaseLogSink, LogEvent, LogLevel

        captured: list[LogEvent] = []

        class CaptureSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                captured.append(event)

        sink = CaptureSink(level=LogLevel.DEBUG)
        rt, _, _ = _build_rt()
        rt._logger._sinks = [sink]
        await rt.start()
        await rt.emergency_stop()

        import asyncio
        await asyncio.sleep(0)

        messages = [ev.message for ev in captured]
        assert "Emergency stop initiated" in messages

    @pytest.mark.asyncio
    async def test_emergency_stop_emits_interrupt_delivered_per_node(self) -> None:
        from arachnite.logging import BaseLogSink, LogEvent, LogLevel

        captured: list[LogEvent] = []

        class CaptureSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                captured.append(event)

        sink = CaptureSink(level=LogLevel.DEBUG)
        rt, am, _ = _build_rt()
        rt._logger._sinks = [sink]
        await rt.start()

        # Place a MultiStepActionNode (which has request_interrupt) in current_actions
        node = _IdleMultiStep(bus=rt.bus)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = Proposal(
            instinct_id="test", action_id=node.node_id, priority=100, urgency=0.5,
        )

        await rt.emergency_stop()

        import asyncio
        await asyncio.sleep(0)

        delivered = [
            ev for ev in captured
            if ev.message == "Emergency interrupt delivered"
        ]
        assert len(delivered) >= 1
        assert delivered[0].data["action_id"] == "_IdleMultiStep"
