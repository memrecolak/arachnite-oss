"""Integration tests for ArachniteRuntime — full pipeline."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from arachnite import ContextNode, SignalBus
from arachnite.exceptions import MandatoryBlockViolation
from arachnite.logging import BaseLogSink, LogLevel
from arachnite.models import Context, InterruptRequest, LogEvent, Proposal, Result, Signal
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from tests.conftest import (
    ConstantSenseNode,
    EmergencyReflex,
    RecordingAction,
    ThresholdInstinct,
)


def build_runtime(
    sensor_value: float = 25.0,
    threshold: float = 80.0,
    tick_rate_hz: float = 100.0,
) -> tuple[ArachniteRuntime, RecordingAction]:
    bus     = SignalBus()
    context = ContextNode(history_length=5)

    sense_master    = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    action_master   = ActionMasterNode(bus=bus)

    sense_master.register(ConstantSenseNode(bus=bus, value=sensor_value))
    instinct_master.register(ThresholdInstinct(bus=bus, threshold=threshold))
    recording = RecordingAction(bus=bus)
    recording.node_id = "CoolDownAction"  # type: ignore[assignment]
    action_master.register(recording)

    rt = ArachniteRuntime(
        sense_master    = sense_master,
        context         = context,
        instinct_master = instinct_master,
        decision_master = decision_master,
        action_master   = action_master,
        bus             = bus,
        tick_rate_hz    = tick_rate_hz,
    )
    return rt, recording


class TestRuntimePipeline:
    @pytest.mark.asyncio
    async def test_tick_increments_counter(self) -> None:
        rt, _ = build_runtime()
        await rt.start()
        await rt.tick()
        await rt.tick()
        assert rt.tick_count >= 2
        await rt.stop()

    @pytest.mark.asyncio
    async def test_instinct_below_threshold_no_action(self) -> None:
        rt, recording = build_runtime(sensor_value=50.0, threshold=80.0)
        await rt.start()
        await rt.tick()
        assert len(recording.calls) == 0
        await rt.stop()

    @pytest.mark.asyncio
    async def test_instinct_above_threshold_triggers_action(self) -> None:
        rt, recording = build_runtime(sensor_value=90.0, threshold=80.0)
        await rt.start()
        await rt.tick()
        assert len(recording.calls) >= 1
        await rt.stop()

    @pytest.mark.asyncio
    async def test_context_updated_after_tick(self) -> None:
        rt, _ = build_runtime(sensor_value=30.0)
        await rt.start()
        await rt.tick()
        ctx = rt.context.snapshot()
        assert ctx.tick >= 1
        assert any(s.kind == "thermal" for s in ctx.signals)
        await rt.stop()

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self) -> None:
        rt, _ = build_runtime()
        assert not rt.is_running
        await rt.start()
        assert rt.is_running
        await rt.stop()
        assert not rt.is_running

    @pytest.mark.asyncio
    async def test_double_start_is_idempotent(self) -> None:
        rt, _ = build_runtime()
        await rt.start()
        await rt.start()  # should not raise
        assert rt.is_running
        await rt.stop()

    @pytest.mark.asyncio
    async def test_health_monitor_accessible(self) -> None:
        rt, _ = build_runtime()
        await rt.start()
        assert rt.health.system_healthy()
        await rt.stop()

    @pytest.mark.asyncio
    async def test_pause_and_resume(self) -> None:
        """pause() halts the background tick loop; resume() restarts it.

        The `rt.tick()` shim bypasses `_paused` by design (see
        `ArachniteRuntime.tick` docstring) — this test therefore verifies
        the real contract: the background `_loop` stops incrementing
        `tick_count` while paused, and resumes after `resume()`.
        """
        rt, recording = build_runtime(sensor_value=90.0, tick_rate_hz=100.0)
        await rt.start()
        await asyncio.sleep(0.05)
        ticks_before_pause = rt.tick_count
        assert ticks_before_pause >= 1, "background loop should have ticked"

        await rt.pause()
        assert rt.is_paused
        await asyncio.sleep(0.05)
        ticks_paused_start = rt.tick_count
        await asyncio.sleep(0.1)
        assert rt.tick_count == ticks_paused_start, (
            "paused background loop must not start new ticks"
        )

        await rt.resume()
        assert not rt.is_paused
        await asyncio.sleep(0.05)
        assert rt.tick_count > ticks_paused_start, (
            "resumed background loop must start ticking again"
        )
        assert len(recording.calls) >= 1
        await rt.stop()


class TestReflexPipeline:
    @pytest.mark.asyncio
    async def test_reflex_fires_before_decision(self) -> None:
        """Reflex action should be dispatched before any normal action."""
        bus     = SignalBus()
        context = ContextNode()

        sense_master    = SenseMasterNode(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        decision_master = DecisionMasterNode(
            bus=bus, strategy=GreedyDecisionNode(bus=bus)
        )
        action_master = ActionMasterNode(bus=bus)

        # Sensor value above both reflex and normal thresholds
        sense_master.register(ConstantSenseNode(bus=bus, value=98.0))
        instinct_master.register(ThresholdInstinct(bus=bus, threshold=80.0))
        instinct_master.register(EmergencyReflex(bus=bus, critical_threshold=90.0))

        normal_action   = RecordingAction(bus=bus)
        normal_action.node_id = "CoolDownAction"  # type: ignore[assignment]

        class EmergencyStop(BaseActionNode):
            node_id = "EmergencyStop"
            calls: list[Proposal] = []
            async def execute(self, proposal: Proposal) -> Result:
                EmergencyStop.calls.append(proposal)
                return Result(action_id=self.node_id, success=True)

        action_master.register(normal_action)
        action_master.register(EmergencyStop(bus=bus))

        rt = ArachniteRuntime(
            sense_master    = sense_master,
            context         = context,
            instinct_master = instinct_master,
            decision_master = decision_master,
            action_master   = action_master,
            bus             = bus,
            tick_rate_hz    = 100.0,
        )
        await rt.start()
        await rt.tick()

        # Emergency reflex should have fired
        assert len(EmergencyStop.calls) >= 1
        await rt.stop()


class TestRuntimeSignalBus:
    @pytest.mark.asyncio
    async def test_signals_published_to_bus_during_tick(self) -> None:
        bus     = SignalBus()
        context = ContextNode()

        received: list[Signal] = []

        async def collector(sig: Signal) -> None:
            received.append(sig)

        bus.subscribe("thermal", collector)

        sense_master    = SenseMasterNode(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        decision_master = DecisionMasterNode(
            bus=bus, strategy=GreedyDecisionNode(bus=bus)
        )
        action_master = ActionMasterNode(bus=bus)

        sense_master.register(ConstantSenseNode(bus=bus, value=10.0))

        rt = ArachniteRuntime(
            sense_master    = sense_master,
            context         = context,
            instinct_master = instinct_master,
            decision_master = decision_master,
            action_master   = action_master,
            bus             = bus,
            tick_rate_hz    = 100.0,
        )
        await rt.start()
        await rt.tick()
        assert len(received) >= 1
        assert received[0].kind == "thermal"
        await rt.stop()


# ── MandatoryBlockViolation logging ──────────────────────────────────────────


class CaptureSink(BaseLogSink):
    """Collects all emitted LogEvents for test assertions."""

    def __init__(self) -> None:
        super().__init__(level=LogLevel.DEBUG)
        self.events: list[LogEvent] = []

    async def emit(self, event: LogEvent) -> None:
        self.events.append(event)


class TestMandatoryBlockViolationLogging:
    """A-03: MandatoryBlockViolation must be logged, not silently suppressed."""

    @staticmethod
    def _build_runtime_with_sink() -> tuple[
        ArachniteRuntime, ActionMasterNode, DecisionMasterNode, CaptureSink
    ]:
        """Build a minimal runtime wired with a CaptureSink for log assertions."""
        bus     = SignalBus()
        context = ContextNode()
        sink    = CaptureSink()

        sense_master    = SenseMasterNode(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        decision_master = DecisionMasterNode(
            bus=bus, strategy=GreedyDecisionNode(bus=bus),
        )
        action_master = ActionMasterNode(bus=bus)

        action = RecordingAction(bus=bus)
        action.node_id = "RunningAction"  # type: ignore[assignment]
        action_master.register(action)

        sense_master.register(ConstantSenseNode(bus=bus, value=50.0))

        rt = ArachniteRuntime(
            sense_master    = sense_master,
            context         = context,
            instinct_master = instinct_master,
            decision_master = decision_master,
            action_master   = action_master,
            bus             = bus,
            tick_rate_hz    = 100.0,
            log_sinks       = [sink],
        )
        return rt, action_master, decision_master, sink

    @pytest.mark.asyncio
    async def test_mandatory_block_violation_logs_warning(self) -> None:
        """When request_interrupt raises MandatoryBlockViolation the runtime
        logs a warning and the tick completes without crashing."""
        rt, action_master, decision_master, sink = self._build_runtime_with_sink()

        # Craft an interrupt that the runtime will try to issue
        interrupt = InterruptRequest(
            new_proposal=Proposal(
                instinct_id="HighInstinct",
                action_id="RunningAction",
                priority=150,
                urgency=0.9,
            ),
            requesting_instinct_id="HighInstinct",
            reason="higher priority",
        )

        # Patch decision layer to return the interrupt
        original_on_new = decision_master.on_new_proposals_many

        async def fake_on_new(*args, **kwargs):  # type: ignore[no-untyped-def]
            to_dispatch, _ = await original_on_new(*args, **kwargs)
            return to_dispatch, [interrupt]

        await rt.start()

        with patch.object(decision_master, "on_new_proposals_many", side_effect=fake_on_new), \
             patch.object(
                 action_master,
                 "request_interrupt",
                 new=AsyncMock(
                     side_effect=MandatoryBlockViolation("RunningAction", "mandatory_step"),
                 ),
             ):
            await rt.tick()

        # Allow fire-and-forget log tasks to complete
        await asyncio.sleep(0)
        await rt.stop()

        # Tick must have completed (no crash)
        assert rt.tick_count >= 1

        # A warning about the mandatory block must have been logged
        warnings = [
            e for e in sink.events
            if e.level == LogLevel.WARNING
            and "mandatory" in e.message.lower()
        ]
        assert len(warnings) >= 1
        assert warnings[0].data.get("action_id") == "RunningAction"
        assert "detail" in warnings[0].data

    @pytest.mark.asyncio
    async def test_generic_interrupt_error_logs_error(self) -> None:
        """When request_interrupt raises a non-MandatoryBlockViolation exception
        the runtime logs an error and the tick still completes."""
        rt, action_master, decision_master, sink = self._build_runtime_with_sink()

        interrupt = InterruptRequest(
            new_proposal=Proposal(
                instinct_id="HighInstinct",
                action_id="RunningAction",
                priority=150,
                urgency=0.9,
            ),
            requesting_instinct_id="HighInstinct",
            reason="higher priority",
        )

        original_on_new = decision_master.on_new_proposals_many

        async def fake_on_new(*args, **kwargs):  # type: ignore[no-untyped-def]
            to_dispatch, _ = await original_on_new(*args, **kwargs)
            return to_dispatch, [interrupt]

        await rt.start()

        with patch.object(decision_master, "on_new_proposals_many", side_effect=fake_on_new), \
             patch.object(
                 action_master,
                 "request_interrupt",
                 new=AsyncMock(side_effect=RuntimeError("transport down")),
             ):
            await rt.tick()

        await asyncio.sleep(0)
        await rt.stop()

        assert rt.tick_count >= 1

        errors = [
            e for e in sink.events
            if e.level == LogLevel.ERROR
            and "interrupt" in e.message.lower()
        ]
        assert len(errors) >= 1
        assert errors[0].data.get("action_id") == "RunningAction"
        assert "transport down" in errors[0].data.get("error", "")


# ── Result carry-over lifecycle (one-tick) ─────────────────────────────────


class TestLastResultsCarryOver:
    """The runtime carries action ``Result``\\ s into the *next* tick's
    Context so instincts can react to outcomes (per ``Result`` docstring
    "Fed back into the next tick's Context").  Lifetime is exactly one
    tick: after the carry-over snapshot is consumed, the runtime-side
    slots are cleared so an immediately-following idle tick presents
    empty results.

    Supersedes the prior A-09 / A-18 ``test_context_results_fresh_each_tick``
    assertion that ``ctx.last_results`` is empty in the very next tick —
    that behaviour was the bug.  Stale-across-many-idle-ticks (A-09's
    actual concern) is still bounded because the runtime now clears its
    own slots after the carry-over is snapshotted into ctx.
    """

    @pytest.mark.asyncio
    async def test_prior_tick_result_visible_then_cleared(self) -> None:
        """Tick 1 dispatches; tick 2's ctx carries the result; tick 3
        (still idle) sees an empty list because tick 2 had nothing to
        carry forward."""
        rt, recording = build_runtime(sensor_value=90.0, threshold=80.0)
        await rt.start()
        await rt.pause()
        recording.calls.clear()

        # Tick 1: instinct fires, action dispatched.  The runtime stores
        # the result in ``_last_results`` for the next tick to carry over.
        await rt.tick()
        assert len(recording.calls) >= 1, "tick 1 must dispatch an action"
        tick1_action_id = "CoolDownAction"

        # Lower sensor below threshold so instinct no longer fires.
        rt._sense_master._nodes.clear()
        rt._sense_master.register(
            ConstantSenseNode(bus=rt.bus, value=50.0)
        )

        # Tick 2: instinct returns None, no dispatch.  The ctx built at
        # the start of this tick MUST carry tick 1's result.
        await rt.tick()
        ctx_tick2 = rt.context.snapshot()
        assert ctx_tick2.last_result is not None, (
            "tick 2's ctx must carry tick 1's result (one-tick feedback)"
        )
        assert ctx_tick2.last_result.action_id == tick1_action_id
        assert len(ctx_tick2.last_results) == 1
        assert ctx_tick2.last_results[0].action_id == tick1_action_id

        # The runtime-side slots are cleared after the carry-over snapshot
        # so the next idle tick has nothing to carry forward.
        assert rt._last_results == []
        assert rt._last_result is None

        # Tick 3: still idle.  ctx must now be empty — staleness is bounded
        # to exactly one tick (A-09's underlying concern).
        await rt.tick()
        ctx_tick3 = rt.context.snapshot()
        assert ctx_tick3.last_result is None
        assert ctx_tick3.last_results == []

        await rt.stop()

    @pytest.mark.asyncio
    async def test_instinct_observes_prior_result_via_ctx(self) -> None:
        """A normal instinct's ``evaluate(ctx)`` MUST receive a non-empty
        ``ctx.last_results`` on the tick following a dispatch.  This is
        the read path that ``LLMInstinctNode`` and downstream consumers
        rely on; prior to the A1 fix it was always empty."""
        bus = SignalBus()
        context = ContextNode(history_length=5)

        sense_master    = SenseMasterNode(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        decision_master = DecisionMasterNode(
            bus=bus, strategy=GreedyDecisionNode(bus=bus),
        )
        action_master   = ActionMasterNode(bus=bus)

        sensor = ConstantSenseNode(bus=bus, value=90.0)
        sensor.poll_interval_s = 0.0  # don't throttle across rapid ticks
        sense_master.register(sensor)
        instinct_master.register(ThresholdInstinct(bus=bus, threshold=80.0))

        recorded_last_results: list[list[Result]] = []

        class ObservingInstinct(BaseInstinctNode):
            """Read-only instinct: records what it sees in ctx.last_results
            and never produces a proposal, so its presence does not affect
            dispatch."""
            node_id  = "ObservingInstinct"
            priority = 1

            async def evaluate(self, ctx: Context) -> Proposal | None:
                recorded_last_results.append(list(ctx.last_results))
                return None

        observer = ObservingInstinct(bus=bus)
        instinct_master.register(observer)

        recording = RecordingAction(bus=bus)
        recording.node_id = "CoolDownAction"  # type: ignore[assignment]
        action_master.register(recording)

        rt = ArachniteRuntime(
            sense_master    = sense_master,
            context         = context,
            instinct_master = instinct_master,
            decision_master = decision_master,
            action_master   = action_master,
            bus             = bus,
            tick_rate_hz    = 100.0,
        )
        await rt.start()
        await rt.pause()

        # Three ticks: tick 1 dispatches, tick 2 carries it forward, tick 3
        # idle (no carry-over because tick 2 didn't dispatch — observer
        # is read-only and never produces a proposal).
        await rt.tick()
        await rt.tick()
        await rt.tick()

        assert len(recorded_last_results) == 3, "instinct must evaluate each tick"

        # Tick 1: no prior tick, last_results is empty.
        assert recorded_last_results[0] == []

        # Tick 2: tick 1's CoolDownAction result MUST be visible.  This is
        # the assertion the framework was missing — the original ctx.last_results
        # bug bypassed it because no test exercised the runtime path with
        # an observing instinct that actually reads ctx.last_results.
        assert len(recorded_last_results[1]) == 1, (
            "tick 2's ctx.last_results must carry tick 1's dispatch result"
        )
        assert recorded_last_results[1][0].action_id == "CoolDownAction"
        assert recorded_last_results[1][0].success is True

        # Tick 3: tick 2 also dispatched (ThresholdInstinct still fires
        # against sensor=90.0), so ctx.last_results carries tick 2's result.
        assert len(recorded_last_results[2]) == 1
        assert recorded_last_results[2][0].action_id == "CoolDownAction"

        await rt.stop()


# ── A-16: Concurrent reflex + normal dispatch merge ─────────────────────────


class TestReflexNormalMerge:
    """When both a reflex instinct and a normal instinct fire in the same
    tick, the runtime must merge their results as reflex-first, then
    normal. The merged list must be visible in ``_last_results`` and
    propagated to ``ctx.last_results`` on the following tick."""

    @staticmethod
    def _build_merge_runtime() -> tuple[
        ArachniteRuntime, RecordingAction, RecordingAction
    ]:
        """Build a runtime with one reflex instinct and one normal instinct,
        each targeting a distinct action node. Both always fire."""
        bus = SignalBus()
        context = ContextNode(history_length=5)

        sense_master = SenseMasterNode(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        decision_master = DecisionMasterNode(
            bus=bus, strategy=GreedyDecisionNode(bus=bus),
        )
        action_master = ActionMasterNode(bus=bus)

        # Sensor value above both thresholds so both instincts always fire.
        # poll_interval_s=0.0 so sensor is not throttled across rapid ticks.
        sensor = ConstantSenseNode(bus=bus, value=99.0)
        sensor.poll_interval_s = 0.0
        sense_master.register(sensor)
        instinct_master.register(EmergencyReflex(bus=bus, critical_threshold=90.0))
        instinct_master.register(ThresholdInstinct(bus=bus, threshold=80.0))

        reflex_action = RecordingAction(bus=bus)
        reflex_action.node_id = "EmergencyStop"  # type: ignore[assignment]

        normal_action = RecordingAction(bus=bus)
        normal_action.node_id = "CoolDownAction"  # type: ignore[assignment]

        action_master.register(reflex_action)
        action_master.register(normal_action)

        rt = ArachniteRuntime(
            sense_master=sense_master,
            context=context,
            instinct_master=instinct_master,
            decision_master=decision_master,
            action_master=action_master,
            bus=bus,
            tick_rate_hz=100.0,
        )
        return rt, reflex_action, normal_action

    @pytest.mark.asyncio
    async def test_merged_results_ordered_reflex_before_normal(self) -> None:
        """After one tick where both fire, _last_results has reflex first."""
        rt, reflex_action, normal_action = self._build_merge_runtime()
        await rt.start()
        await rt.pause()  # prevent background loop from running extra ticks
        reflex_action.calls.clear()
        normal_action.calls.clear()

        await rt.tick()

        # Both actions must have been called at least once
        assert len(reflex_action.calls) >= 1
        assert len(normal_action.calls) >= 1

        # Merged list: exactly 2 results, reflex first
        assert len(rt._last_results) == 2
        assert rt._last_results[0].action_id == "EmergencyStop"
        assert rt._last_results[1].action_id == "CoolDownAction"
        assert rt._last_results[0].success is True
        assert rt._last_results[1].success is True

        # _last_result is the first (reflex) result
        assert rt._last_result is not None
        assert rt._last_result.action_id == "EmergencyStop"

        await rt.stop()

    @pytest.mark.asyncio
    async def test_merge_is_consistent_across_multiple_ticks(self) -> None:
        """The reflex-first merge must be consistent across successive
        ticks. On each tick where both instincts fire, _last_results
        must contain both results in reflex-before-normal order."""
        bus = SignalBus()
        context = ContextNode(history_length=5)

        sense_master = SenseMasterNode(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        decision_master = DecisionMasterNode(
            bus=bus, strategy=GreedyDecisionNode(bus=bus),
        )
        action_master = ActionMasterNode(bus=bus)

        # poll_interval_s=0.0 so sensor is not throttled across rapid ticks
        sensor = ConstantSenseNode(bus=bus, value=99.0)
        sensor.poll_interval_s = 0.0
        sense_master.register(sensor)
        instinct_master.register(EmergencyReflex(bus=bus, critical_threshold=90.0))
        instinct_master.register(ThresholdInstinct(bus=bus, threshold=80.0))

        reflex_action = RecordingAction(bus=bus)
        reflex_action.node_id = "EmergencyStop"  # type: ignore[assignment]
        normal_action = RecordingAction(bus=bus)
        normal_action.node_id = "CoolDownAction"  # type: ignore[assignment]
        action_master.register(reflex_action)
        action_master.register(normal_action)

        rt = ArachniteRuntime(
            sense_master=sense_master,
            context=context,
            instinct_master=instinct_master,
            decision_master=decision_master,
            action_master=action_master,
            bus=bus,
            tick_rate_hz=100.0,
        )
        await rt.start()
        await rt.pause()  # prevent background loop from running extra ticks
        reflex_action.calls.clear()
        normal_action.calls.clear()

        for tick_num in range(1, 4):
            await rt.tick()

            assert len(rt._last_results) == 2, (
                f"tick {tick_num}: expected 2 merged results"
            )
            assert rt._last_results[0].action_id == "EmergencyStop", (
                f"tick {tick_num}: reflex result must be first"
            )
            assert rt._last_results[1].action_id == "CoolDownAction", (
                f"tick {tick_num}: normal result must be second"
            )

        # Each action was called once per tick (3 explicit ticks)
        assert len(reflex_action.calls) == 3
        assert len(normal_action.calls) == 3

        await rt.stop()
