"""
Stricter versions of existing tests.

These tests tighten weak assertions found during manual code review,
verifying exact outcomes rather than accepting multiple possibilities.
"""

from __future__ import annotations

import asyncio

import pytest

from arachnite import SignalBus
from arachnite.logging import BaseLogSink, StructuredLogger
from arachnite.models import (
    ActionStep,
    InterruptPolicy,
    InterruptRequest,
    LogEvent,
    LogLevel,
    MergePolicy,
    Proposal,
    Signal,
    StepResult,
)
from arachnite.nodes.action import MultiStepActionNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.safety_monitor import (
    MonitorState,
    SafetyMonitorRegistry,
    SafetySeverity,
)
from tests.conftest import make_proposal, make_signal

# ── Helpers ──────────────────────────────────────────────────────────────────


def _state(**overrides: object) -> MonitorState:
    return MonitorState(**overrides)  # type: ignore[arg-type]


# ── MultiStepActionNode: rollback on interrupt ───────────────────────────────


class MandatoryBlockAction(MultiStepActionNode):
    """Three steps: interruptible → mandatory → interruptible."""

    node_id = "StrictMandatoryBlockAction"
    interrupt_policy = InterruptPolicy.ROLLBACK

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.rolled_back: list[str] = []

    async def _undo_step2(self) -> None:
        self.rolled_back.append("step2")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step1", interruptible=True),
            ActionStep("step2", interruptible=False, rollback=self._undo_step2),
            ActionStep("step3", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        return StepResult(step_name=step.name, success=True, output=step.name)


class TestRollbackOnInterruptStrict:
    """Original test used `assert result.interrupted or result.success`.

    This is too weak — it accepts either outcome. The correct behaviour for
    ROLLBACK policy when interrupt arrives during the mandatory step2 is:
    step1 completes, step2 completes (mandatory), interrupt fires before
    step3, rollback runs on step2, result.interrupted is True.
    """

    @pytest.mark.asyncio
    async def test_interrupt_after_mandatory_block_is_interrupted(self, bus: SignalBus) -> None:
        action = MandatoryBlockAction(bus=bus)
        proposal = make_proposal(action_id="StrictMandatoryBlockAction")

        original_execute = action.execute_step
        step_count = [0]

        async def counting_step(
            step: ActionStep, prop: Proposal, completed: list[StepResult]
        ) -> StepResult:
            result = await original_execute(step, prop, completed)
            step_count[0] += 1
            if step_count[0] == 2:  # after step2 (mandatory) completes
                action.request_interrupt(InterruptRequest(
                    new_proposal=make_proposal(priority=200),
                    requesting_instinct_id="test",
                ))
            return result

        action.execute_step = counting_step  # type: ignore[method-assign]
        result = await action.execute(proposal)

        # Must be interrupted, not successful
        assert result.interrupted is True
        assert result.success is False

        # step1 and step2 must have completed (mandatory block honoured)
        assert len(result.step_results) == 2
        completed_names = [sr.step_name for sr in result.step_results]
        assert completed_names == ["step1", "step2"]

        # ROLLBACK policy must have rolled back step2
        assert action.rolled_back == ["step2"]

    @pytest.mark.asyncio
    async def test_interrupt_before_mandatory_block_stops_at_step1(self, bus: SignalBus) -> None:
        """Interrupt requested before step1 should stop at step1 boundary."""
        action = MandatoryBlockAction(bus=bus)
        proposal = make_proposal(action_id="StrictMandatoryBlockAction")

        original_execute = action.execute_step
        step_count = [0]

        async def counting_step(
            step: ActionStep, prop: Proposal, completed: list[StepResult]
        ) -> StepResult:
            result = await original_execute(step, prop, completed)
            step_count[0] += 1
            if step_count[0] == 1:  # after step1 completes
                action.request_interrupt(InterruptRequest(
                    new_proposal=make_proposal(priority=200),
                    requesting_instinct_id="test",
                ))
            return result

        action.execute_step = counting_step  # type: ignore[method-assign]
        result = await action.execute(proposal)

        # step2 is mandatory so interrupt is held; step2 completes,
        # then interrupt fires before step3
        assert result.interrupted is True
        assert len(result.step_results) == 2
        assert [sr.step_name for sr in result.step_results] == ["step1", "step2"]


# ── SafetyMonitorRegistry: verify exact violations ──────────────────────────


class TestSafetyMonitorRegistryStrict:
    """Original test used `assert len(violations) >= 2`.

    This is too loose — it doesn't verify WHICH monitors fired.
    """

    @pytest.mark.asyncio
    async def test_check_all_returns_exact_violations(self, bus: SignalBus) -> None:
        reg = SafetyMonitorRegistry.default(bus)
        violations = await reg.check_all(1, _state(
            reflex_fired=True,
            decision_entered=True,
            reflex_action_dispatched=False,
        ))

        assert len(violations) == 2

        monitor_ids = {v.monitor_id for v in violations}
        assert monitor_ids == {"ReflexBypassMonitor", "ReflexDispatchMonitor"}

        severities = {v.severity for v in violations}
        assert severities == {SafetySeverity.CRITICAL}

        properties = {v.property_name for v in violations}
        assert properties == {"reflex_bypass", "reflex_dispatch_guarantee"}

    @pytest.mark.asyncio
    async def test_only_bypass_fires_when_action_dispatched(self, bus: SignalBus) -> None:
        """When reflex fires AND action dispatches, only bypass should fire."""
        reg = SafetyMonitorRegistry.default(bus)
        violations = await reg.check_all(1, _state(
            reflex_fired=True,
            decision_entered=True,
            reflex_action_dispatched=True,
        ))

        assert len(violations) == 1
        assert violations[0].monitor_id == "ReflexBypassMonitor"

    @pytest.mark.asyncio
    async def test_only_dispatch_fires_when_decision_not_entered(self, bus: SignalBus) -> None:
        """When reflex fires but action not dispatched, only dispatch should fire."""
        reg = SafetyMonitorRegistry.default(bus)
        violations = await reg.check_all(1, _state(
            reflex_fired=True,
            decision_entered=False,
            reflex_action_dispatched=False,
        ))

        assert len(violations) == 1
        assert violations[0].monitor_id == "ReflexDispatchMonitor"


# ── Logging: verify logger respects sink level ───────────────────────────────


class TestLoggerSinkLevelStrict:
    """Original test only checked that the capture list was empty.

    This doesn't distinguish between "logger filtered the event" and
    "sink filtered the event". We verify both paths explicitly.
    """

    @pytest.mark.asyncio
    async def test_debug_event_filtered_by_logger_not_sink(self) -> None:
        """Logger checks accepts() BEFORE calling emit(), so emit() is never called."""
        emit_calls: list[LogEvent] = []

        class InstrumentedSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                emit_calls.append(event)

        sink = InstrumentedSink(level=LogLevel.WARNING)
        logger = StructuredLogger("Node1", sinks=[sink])
        logger.debug("low")
        await asyncio.sleep(0)

        # Logger filters via sink.accepts() at dispatch time — emit() never called
        assert len(emit_calls) == 0

    @pytest.mark.asyncio
    async def test_warning_event_passes_sink_filter(self) -> None:
        accepted: list[LogEvent] = []

        class FilteringSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                if self.accepts(event):
                    accepted.append(event)

        sink = FilteringSink(level=LogLevel.WARNING)
        logger = StructuredLogger("Node1", sinks=[sink])
        logger.warning("high")
        await asyncio.sleep(0)

        assert len(accepted) == 1
        assert accepted[0].level == LogLevel.WARNING

    @pytest.mark.asyncio
    async def test_multiple_sinks_with_different_levels(self) -> None:
        """Each sink independently filters by its own level."""
        debug_events: list[LogEvent] = []
        warning_events: list[LogEvent] = []

        class DebugSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                if self.accepts(event):
                    debug_events.append(event)

        class WarningSink(BaseLogSink):
            async def emit(self, event: LogEvent) -> None:
                if self.accepts(event):
                    warning_events.append(event)

        logger = StructuredLogger("Node1", sinks=[
            DebugSink(level=LogLevel.DEBUG),
            WarningSink(level=LogLevel.WARNING),
        ])
        logger.debug("low")
        logger.info("medium")
        logger.warning("high")
        await asyncio.sleep(0)

        assert len(debug_events) == 3    # DEBUG sink accepts all
        assert len(warning_events) == 1  # WARNING sink only accepts warning+


# ── Merge policy: verify MEAN confidence calculation ─────────────────────────


class TempSense(BaseSenseNode):
    signal_kind = "temperature"

    def __init__(
        self, bus: SignalBus, nid: str, value: float, confidence: float
    ) -> None:
        super().__init__(bus)
        self.node_id = nid  # type: ignore[misc]
        self._value = value
        self._conf = confidence

    async def read(self) -> Signal:
        return make_signal(
            kind="temperature",
            value=self._value,
            confidence=self._conf,
            source=self.node_id,
        )


class TestMergePolicyMeanStrict:
    """Original test asserted confidence == 0.9 without explaining WHY.

    MEAN merge uses arithmetic mean for both value and confidence:
        value = (30 + 40) / 2 = 35.0
        confidence = (0.8 + 1.0) / 2 = 0.9
    We verify the formula explicitly.
    """

    @pytest.mark.asyncio
    async def test_mean_value_is_arithmetic_mean(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(
            bus=bus, merge_policies={"temperature": MergePolicy.MEAN}
        )
        sm.register(TempSense(bus, "A", value=30.0, confidence=0.8))
        sm.register(TempSense(bus, "B", value=40.0, confidence=1.0))
        signals = await sm.read_all()

        assert len(signals) == 1
        sig = signals[0]
        assert sig.value == pytest.approx((30.0 + 40.0) / 2)
        assert sig.metadata["merge_policy"] == "mean"

    @pytest.mark.asyncio
    async def test_mean_confidence_is_arithmetic_mean(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(
            bus=bus, merge_policies={"temperature": MergePolicy.MEAN}
        )
        sm.register(TempSense(bus, "A", value=10.0, confidence=0.6))
        sm.register(TempSense(bus, "B", value=20.0, confidence=0.8))
        sm.register(TempSense(bus, "C", value=30.0, confidence=1.0))
        signals = await sm.read_all()

        assert len(signals) == 1
        sig = signals[0]
        # Arithmetic mean of confidences: (0.6 + 0.8 + 1.0) / 3
        assert sig.confidence == pytest.approx((0.6 + 0.8 + 1.0) / 3)
        assert sig.value == pytest.approx((10.0 + 20.0 + 30.0) / 3)
        assert sig.metadata["sample_count"] == 3

    @pytest.mark.asyncio
    async def test_mean_with_equal_confidences(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(
            bus=bus, merge_policies={"temperature": MergePolicy.MEAN}
        )
        sm.register(TempSense(bus, "A", value=100.0, confidence=0.5))
        sm.register(TempSense(bus, "B", value=200.0, confidence=0.5))
        signals = await sm.read_all()

        assert len(signals) == 1
        sig = signals[0]
        assert sig.value == pytest.approx(150.0)
        assert sig.confidence == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_mean_metadata_contains_merged_sources(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(
            bus=bus, merge_policies={"temperature": MergePolicy.MEAN}
        )
        sm.register(TempSense(bus, "SensorX", value=10.0, confidence=0.9))
        sm.register(TempSense(bus, "SensorY", value=20.0, confidence=0.7))
        signals = await sm.read_all()

        assert len(signals) == 1
        merged_from = set(signals[0].metadata["merged_from"])
        assert merged_from == {"SensorX", "SensorY"}
