"""Unit tests for InstinctMasterNode edge cases."""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque

import pytest

from arachnite import SignalBus
from arachnite.exceptions import NodeRegistrationError, ReflexConflictError
from arachnite.logging import BaseLogSink, LogLevel
from arachnite.models import Context, LogEvent, Proposal, Signal
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _ctx(signals: list[Signal] | None = None) -> Context:
    return Context(
        tick=1,
        signals=signals or [],
        history=deque(),
        state={},
        last_result=None,
        timestamp=time.monotonic(),
    )


def _signal(kind: str) -> Signal:
    return Signal(
        source="TestSource", kind=kind,
        value=1.0, confidence=1.0, timestamp=time.monotonic(),
    )


def _bus() -> SignalBus:
    return SignalBus()


# ── Concrete test nodes ────────────────────────────────────────────────────────

class AlwaysProposesInstinct(BaseInstinctNode):
    node_id  = "AlwaysProposesInstinct"
    priority = 80

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="Act",
            priority=self.priority, urgency=0.5,
        )


class FaceTriggeredInstinct(BaseInstinctNode):
    """Only fires when face or proximity signals are present."""
    node_id  = "FaceTriggeredInstinct"
    priority = 65
    trigger_on_signals = ["face", "proximity"]

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="Greet",
            priority=self.priority, urgency=0.7,
        )


class RaisingInstinct(BaseInstinctNode):
    """Always raises during evaluate()."""
    node_id  = "RaisingInstinct"
    priority = 50

    async def evaluate(self, ctx: Context) -> Proposal | None:
        raise RuntimeError("instinct evaluation failure")


class AlwaysProposesReflex(BaseReflexInstinctNode):
    node_id  = "AlwaysProposesReflex"
    priority = 200

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="EmergencyStop",
            priority=self.priority, urgency=1.0,
        )


class AnotherReflex(BaseReflexInstinctNode):
    """Same priority as AlwaysProposesReflex — triggers conflict."""
    node_id  = "AnotherReflex"
    priority = 200

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="EmergencyStop",
            priority=self.priority, urgency=1.0,
        )


class RaisingReflex(BaseReflexInstinctNode):
    """Always raises during evaluate()."""
    node_id  = "RaisingReflex"
    priority = 200

    async def evaluate(self, ctx: Context) -> Proposal | None:
        raise RuntimeError("reflex evaluation failure")


# ── InstinctMasterNode registration ───────────────────────────────────────────

class TestInstinctMasterNodeRegistration:
    def test_duplicate_normal_instinct_raises(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        with pytest.raises(NodeRegistrationError):
            im.register(AlwaysProposesInstinct(bus=_bus()))

    def test_duplicate_reflex_instinct_raises(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesReflex(bus=_bus()))
        with pytest.raises(NodeRegistrationError):
            im.register(AlwaysProposesReflex(bus=_bus()))

    def test_unregister_normal_node(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.unregister("AlwaysProposesInstinct")
        assert "AlwaysProposesInstinct" not in {n.node_id for n in im.normal_nodes}

    def test_unregister_reflex_node(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesReflex(bus=_bus()))
        im.unregister("AlwaysProposesReflex")
        assert "AlwaysProposesReflex" not in {n.node_id for n in im.reflex_nodes}

    def test_unregister_nonexistent_is_silent(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.unregister("DoesNotExist")   # must not raise

    def test_reflex_with_priority_below_200_raises(self) -> None:
        """Reflex priorities must be >= 200 (priority convention)."""
        class LowPriorityReflex(BaseReflexInstinctNode):
            node_id  = "LowPriorityReflex"
            priority = 75   # invalid: reflexes must have priority >= 200

            async def evaluate(self, ctx: Context) -> Proposal | None:
                return None

        im = InstinctMasterNode(bus=_bus())
        with pytest.raises(NodeRegistrationError, match="priority 75"):
            im.register(LowPriorityReflex(bus=_bus()))

    def test_reflex_with_priority_at_200_registers(self) -> None:
        """The boundary case: priority == 200 is the minimum valid reflex band."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesReflex(bus=_bus()))   # priority defaults to 200
        assert "AlwaysProposesReflex" in {n.node_id for n in im.reflex_nodes}

    def test_normal_instinct_priority_below_200_is_unconstrained(self) -> None:
        """Normal (non-reflex) instincts have no lower priority bound."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))   # priority 80, valid
        assert "AlwaysProposesInstinct" in {n.node_id for n in im.normal_nodes}


# ── InstinctMasterNode.get_node ──────────────────────────────────────────────

class TestInstinctMasterNodeGetNode:
    def test_get_node_returns_normal_node(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        node = AlwaysProposesInstinct(bus=_bus())
        im.register(node)
        assert im.get_node("AlwaysProposesInstinct") is node

    def test_get_node_returns_reflex_node(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        node = AlwaysProposesReflex(bus=_bus())
        im.register(node)
        assert im.get_node("AlwaysProposesReflex") is node

    def test_get_node_returns_none_when_not_registered(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        assert im.get_node("NonExistent") is None


# ── evaluate_all exception handling ───────────────────────────────────────────

class TestEvaluateAllExceptionHandling:
    @pytest.mark.asyncio
    async def test_raising_instinct_excluded_not_propagated(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(RaisingInstinct(bus=_bus()))
        im.register(AlwaysProposesInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx())
        # RaisingInstinct is swallowed; AlwaysProposesInstinct still fires
        assert len(proposals) == 1
        assert proposals[0].instinct_id == "AlwaysProposesInstinct"

    @pytest.mark.asyncio
    async def test_all_raising_returns_empty(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(RaisingInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx())
        assert proposals == []


# ── evaluate_reflexes exception handling ──────────────────────────────────────

class TestEvaluateReflexesExceptionHandling:
    @pytest.mark.asyncio
    async def test_raising_reflex_excluded_not_propagated(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(RaisingReflex(bus=_bus()))
        im.register(AlwaysProposesReflex(bus=_bus()))
        proposals = await im.evaluate_reflexes(_ctx())
        assert len(proposals) == 1
        assert proposals[0].instinct_id == "AlwaysProposesReflex"

    @pytest.mark.asyncio
    async def test_all_raising_reflex_returns_empty(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(RaisingReflex(bus=_bus()))
        proposals = await im.evaluate_reflexes(_ctx())
        assert proposals == []

    @pytest.mark.asyncio
    async def test_empty_reflex_registry_returns_empty(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        assert await im.evaluate_reflexes(_ctx()) == []


# ── Reflex conflict policy ────────────────────────────────────────────────────

class TestReflexConflictPolicy:
    @pytest.mark.asyncio
    async def test_dispatch_all_policy_returns_both(self) -> None:
        im = InstinctMasterNode(bus=_bus(), reflex_conflict="dispatch_all")
        im.register(AlwaysProposesReflex(bus=_bus()))
        im.register(AnotherReflex(bus=_bus()))
        proposals = await im.evaluate_reflexes(_ctx())
        assert len(proposals) == 2

    @pytest.mark.asyncio
    async def test_raise_policy_single_winner_no_error(self) -> None:
        # Only one reflex fires at priority 200 → no conflict, no raise
        im = InstinctMasterNode(bus=_bus(), reflex_conflict="raise")
        im.register(AlwaysProposesReflex(bus=_bus()))
        proposals = await im.evaluate_reflexes(_ctx())
        assert len(proposals) == 1

    @pytest.mark.asyncio
    async def test_raise_policy_conflict_raises(self) -> None:
        im = InstinctMasterNode(bus=_bus(), reflex_conflict="raise")
        im.register(AlwaysProposesReflex(bus=_bus()))
        im.register(AnotherReflex(bus=_bus()))
        with pytest.raises(ReflexConflictError) as exc_info:
            await im.evaluate_reflexes(_ctx())
        assert exc_info.value.priority == 200
        assert len(exc_info.value.node_ids) == 2


# ── trigger_on_signals ───────────────────────────────────────────────────────

class TestTriggerOnSignals:
    @pytest.mark.asyncio
    async def test_fires_when_matching_signal_present(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(FaceTriggeredInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx(signals=[_signal("face")]))
        assert len(proposals) == 1
        assert proposals[0].instinct_id == "FaceTriggeredInstinct"

    @pytest.mark.asyncio
    async def test_fires_on_any_matching_signal(self) -> None:
        """Any signal kind in trigger_on_signals is sufficient."""
        im = InstinctMasterNode(bus=_bus())
        im.register(FaceTriggeredInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx(signals=[_signal("proximity")]))
        assert len(proposals) == 1

    @pytest.mark.asyncio
    async def test_skipped_when_no_matching_signal(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(FaceTriggeredInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx(signals=[_signal("temperature")]))
        assert proposals == []

    @pytest.mark.asyncio
    async def test_skipped_when_no_signals_at_all(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        im.register(FaceTriggeredInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx())
        assert proposals == []

    @pytest.mark.asyncio
    async def test_none_trigger_fires_every_tick(self) -> None:
        """Default trigger_on_signals=None means always evaluate."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx())
        assert len(proposals) == 1

    @pytest.mark.asyncio
    async def test_mixed_triggered_and_untriggered(self) -> None:
        """Untriggered instincts fire normally alongside signal-gated ones."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.register(FaceTriggeredInstinct(bus=_bus()))
        # No face signal → only AlwaysProposesInstinct fires
        proposals = await im.evaluate_all(_ctx(signals=[_signal("temperature")]))
        assert len(proposals) == 1
        assert proposals[0].instinct_id == "AlwaysProposesInstinct"

    @pytest.mark.asyncio
    async def test_mixed_with_matching_signal(self) -> None:
        """Both fire when the trigger signal is present."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.register(FaceTriggeredInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx(signals=[_signal("face")]))
        assert len(proposals) == 2

    @pytest.mark.asyncio
    async def test_no_filter_nodes_with_many_signals(self) -> None:
        """When no instinct uses trigger_on_signals, the signal_kinds set is not built."""
        class SecondAlwaysInstinct(BaseInstinctNode):
            node_id = "SecondAlwaysInstinct"
            priority = 70

            async def evaluate(self, ctx: Context) -> Proposal | None:
                return Proposal(
                    instinct_id=self.node_id, action_id="Act",
                    priority=self.priority, urgency=0.4,
                )

        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.register(SecondAlwaysInstinct(bus=_bus()))
        kinds = [
            "temperature", "proximity", "humidity", "pressure", "face",
            "voice", "motion", "light", "vibration", "battery",
            "temperature", "proximity", "humidity", "pressure", "face",
        ]
        signals = [_signal(k) for k in kinds]
        proposals = await im.evaluate_all(_ctx(signals=signals))
        assert len(proposals) == 2
        ids = {p.instinct_id for p in proposals}
        assert ids == {"AlwaysProposesInstinct", "SecondAlwaysInstinct"}

    @pytest.mark.asyncio
    async def test_filter_node_with_empty_signals_skipped(self) -> None:
        """Filtered instinct is skipped by the gate (not by the outer branch)."""
        im = InstinctMasterNode(bus=_bus())
        im.register(FaceTriggeredInstinct(bus=_bus()))
        im.register(AlwaysProposesInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx(signals=[]))
        assert len(proposals) == 1
        assert proposals[0].instinct_id == "AlwaysProposesInstinct"

    @pytest.mark.asyncio
    async def test_any_probe_respects_iteration_order(self) -> None:
        """any() short-circuit must probe all nodes, not stop at first non-filtered one."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.register(FaceTriggeredInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx(signals=[_signal("temperature")]))
        assert len(proposals) == 1
        assert proposals[0].instinct_id == "AlwaysProposesInstinct"


# ── last_evaluated_ids ──────────────────────────────────────────────────────

class ThrottledInstinct(BaseInstinctNode):
    """Instinct with a long trigger interval for testing throttle."""
    node_id = "ThrottledInstinct"
    priority = 60
    trigger_interval_s = 60.0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="Act",
            priority=self.priority, urgency=0.5,
        )


class TestLastEvaluatedIds:
    @pytest.mark.asyncio
    async def test_tracks_evaluated_instincts(self) -> None:
        """Instincts that pass gate+throttle are tracked."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        await im.evaluate_all(_ctx())
        assert "AlwaysProposesInstinct" in im.last_evaluated_ids

    @pytest.mark.asyncio
    async def test_excludes_gated_instincts(self) -> None:
        """Signal-gated instincts not in evaluated set when gate fails."""
        im = InstinctMasterNode(bus=_bus())
        im.register(FaceTriggeredInstinct(bus=_bus()))
        im.register(AlwaysProposesInstinct(bus=_bus()))
        await im.evaluate_all(_ctx(signals=[_signal("temperature")]))
        assert "AlwaysProposesInstinct" in im.last_evaluated_ids
        assert "FaceTriggeredInstinct" not in im.last_evaluated_ids

    @pytest.mark.asyncio
    async def test_excludes_throttled_instincts(self) -> None:
        """Throttled instincts not in evaluated set on second call."""
        im = InstinctMasterNode(bus=_bus())
        im.register(ThrottledInstinct(bus=_bus()))
        # First call: passes throttle
        await im.evaluate_all(_ctx())
        assert "ThrottledInstinct" in im.last_evaluated_ids
        # Second call: throttled (interval not elapsed)
        await im.evaluate_all(_ctx())
        assert "ThrottledInstinct" not in im.last_evaluated_ids

    @pytest.mark.asyncio
    async def test_empty_when_no_normal_nodes(self) -> None:
        im = InstinctMasterNode(bus=_bus())
        await im.evaluate_all(_ctx())
        assert im.last_evaluated_ids == set()

    @pytest.mark.asyncio
    async def test_includes_raising_instinct(self) -> None:
        """An instinct that raises is still recorded as evaluated."""
        im = InstinctMasterNode(bus=_bus())
        im.register(RaisingInstinct(bus=_bus()))
        await im.evaluate_all(_ctx())
        assert "RaisingInstinct" in im.last_evaluated_ids


# ── Throttle timestamp after evaluate ────────────────────────────────────────

class SlowThrottledInstinct(BaseInstinctNode):
    """Instinct that sleeps briefly during evaluate to simulate slow work"""
    node_id = "SlowThrottledInstinct"
    priority = 60
    trigger_interval_s = 60.0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        await asyncio.sleep(0.05)
        return Proposal(
            instinct_id=self.node_id, action_id="Act",
            priority=self.priority, urgency=0.5,
        )


class TestThrottleTimestampAfterEvaluate:
    @pytest.mark.asyncio
    async def test_last_trigger_set_after_evaluate_completes(self) -> None:
        """Throttle timestamp reflects evaluate completion, not start"""
        im = InstinctMasterNode(bus=_bus())
        node = SlowThrottledInstinct(bus=_bus())
        im.register(node)

        before = time.monotonic()
        await im.evaluate_all(_ctx())

        # The sleep inside evaluate is 0.05s, so _last_trigger_s must be
        # at least 0.04s after the timestamp captured before evaluate_all
        assert node._last_trigger_s >= before + 0.04


# ── First-evaluation must always pass throttle ──────────────────────────────


class HugeIntervalInstinct(BaseInstinctNode):
    """Throttle larger than typical ``time.monotonic()`` values."""
    node_id = "HugeIntervalInstinct"
    priority = 60
    trigger_interval_s = 10_000_000.0  # ~115 days

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="Act",
            priority=self.priority, urgency=0.5,
        )


class TestFirstEvaluationPassesThrottle:
    @pytest.mark.asyncio
    async def test_first_call_not_throttled_regardless_of_interval(self) -> None:
        """``_last_trigger_s`` must initialize so the first evaluation
        always passes the throttle check, regardless of how big
        ``trigger_interval_s`` is or how small ``time.monotonic()`` is on
        this machine. Previously initialized to ``0.0``, which caused
        instincts with intervals larger than the system uptime in seconds
        to skip their first evaluation. Regression test for the throttle
        bug uncovered by the carry-forward rejection test on 2026-04-16.
        """
        im = InstinctMasterNode(bus=_bus())
        node = HugeIntervalInstinct(bus=_bus())
        im.register(node)

        proposals = await im.evaluate_all(_ctx())

        assert "HugeIntervalInstinct" in im.last_evaluated_ids
        assert len(proposals) == 1
        assert proposals[0].instinct_id == "HugeIntervalInstinct"


# ── A-15: Reflex trigger_interval_s warning ──────────────────────────────────


class _CaptureSink(BaseLogSink):
    """Collects all log events for assertion."""

    def __init__(self) -> None:
        super().__init__(level=LogLevel.DEBUG)
        self.events: list[LogEvent] = []

    async def emit(self, event: LogEvent) -> None:
        self.events.append(event)


class ThrottledReflex(BaseReflexInstinctNode):
    """Reflex with trigger_interval_s set (which is ignored for reflexes)."""
    node_id = "ThrottledReflex"
    priority = 200
    trigger_interval_s = 5.0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="EmergencyStop",
            priority=self.priority, urgency=1.0,
        )


class TestReflexTriggerIntervalWarning:
    @pytest.mark.asyncio
    async def test_register_reflex_with_trigger_interval_warns(self) -> None:
        """Registering a reflex with trigger_interval_s emits a warning (A-15)"""
        sink = _CaptureSink()
        im = InstinctMasterNode(bus=_bus(), log_sinks=[sink])
        im.register(ThrottledReflex(bus=_bus()))

        await asyncio.sleep(0)  # let fire-and-forget tasks flush

        warnings = [
            ev for ev in sink.events
            if ev.message == "Reflex trigger_interval_s ignored"
        ]
        assert len(warnings) == 1
        assert warnings[0].data["instinct_node_id"] == "ThrottledReflex"
        assert warnings[0].data["trigger_interval_s"] == 5.0

    @pytest.mark.asyncio
    async def test_register_reflex_without_trigger_interval_no_warning(self) -> None:
        """Registering a normal reflex does not emit the warning."""
        sink = _CaptureSink()
        im = InstinctMasterNode(bus=_bus(), log_sinks=[sink])
        im.register(AlwaysProposesReflex(bus=_bus()))

        await asyncio.sleep(0)

        warnings = [
            ev for ev in sink.events
            if ev.message == "Reflex trigger_interval_s ignored"
        ]
        assert len(warnings) == 0

    @pytest.mark.asyncio
    async def test_register_normal_instinct_with_trigger_interval_no_warning(self) -> None:
        """Normal instincts with trigger_interval_s do NOT emit the warning."""
        sink = _CaptureSink()
        im = InstinctMasterNode(bus=_bus(), log_sinks=[sink])
        im.register(ThrottledInstinct(bus=_bus()))

        await asyncio.sleep(0)

        warnings = [
            ev for ev in sink.events
            if ev.message == "Reflex trigger_interval_s ignored"
        ]
        assert len(warnings) == 0


# ── set_pre_evaluate_gate ────────────────────────────────────────────────────

class _SecondInstinct(BaseInstinctNode):
    """A second always-proposing instinct, distinct node_id from AlwaysProposesInstinct."""
    node_id  = "_SecondInstinct"
    priority = 70

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="Act2",
            priority=self.priority, urgency=0.4,
        )


class TestPreEvaluateGate:
    @pytest.mark.asyncio
    async def test_no_gate_installed_all_nodes_evaluate(self) -> None:
        """Default behavior preserved — no gate means every enabled node evaluates."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.register(_SecondInstinct(bus=_bus()))
        proposals = await im.evaluate_all(_ctx())
        assert len(proposals) == 2
        assert im.last_evaluated_ids == {"AlwaysProposesInstinct", "_SecondInstinct"}

    @pytest.mark.asyncio
    async def test_gate_denying_skips_node(self) -> None:
        """A node the gate denies does not evaluate and is absent from last_evaluated_ids."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.set_pre_evaluate_gate(lambda node, ctx: False)
        proposals = await im.evaluate_all(_ctx())
        assert proposals == []
        assert "AlwaysProposesInstinct" not in im.last_evaluated_ids

    @pytest.mark.asyncio
    async def test_gate_selective_allow_one_deny_other(self) -> None:
        """The InferenceScheduler shape — gate picks one instinct per tick."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.register(_SecondInstinct(bus=_bus()))
        im.set_pre_evaluate_gate(
            lambda node, ctx: node.node_id == "AlwaysProposesInstinct",
        )
        proposals = await im.evaluate_all(_ctx())
        assert [p.instinct_id for p in proposals] == ["AlwaysProposesInstinct"]
        assert im.last_evaluated_ids == {"AlwaysProposesInstinct"}

    @pytest.mark.asyncio
    async def test_passing_none_clears_installed_gate(self) -> None:
        """set_pre_evaluate_gate(None) restores ungated behavior."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.set_pre_evaluate_gate(lambda node, ctx: False)
        assert await im.evaluate_all(_ctx()) == []
        im.set_pre_evaluate_gate(None)
        proposals = await im.evaluate_all(_ctx())
        assert len(proposals) == 1

    @pytest.mark.asyncio
    async def test_gate_runs_after_trigger_on_signals(self) -> None:
        """Signal-gated nodes that fail the signal check never reach the gate."""
        seen: list[str] = []

        def recording_gate(node: BaseInstinctNode, ctx: Context) -> bool:
            seen.append(node.node_id)
            return True

        im = InstinctMasterNode(bus=_bus())
        im.register(FaceTriggeredInstinct(bus=_bus()))   # needs face/proximity
        im.register(AlwaysProposesInstinct(bus=_bus()))
        im.set_pre_evaluate_gate(recording_gate)
        # Signal does not match FaceTriggeredInstinct.trigger_on_signals.
        await im.evaluate_all(_ctx(signals=[_signal("temperature")]))
        assert "FaceTriggeredInstinct" not in seen
        assert "AlwaysProposesInstinct" in seen

    @pytest.mark.asyncio
    async def test_gate_runs_after_trigger_interval_s(self) -> None:
        """Throttled nodes that fail the interval check never reach the gate."""
        seen: list[str] = []

        def recording_gate(node: BaseInstinctNode, ctx: Context) -> bool:
            seen.append(node.node_id)
            return True

        im = InstinctMasterNode(bus=_bus())
        im.register(ThrottledInstinct(bus=_bus()))
        im.set_pre_evaluate_gate(recording_gate)
        # First call: passes throttle, gate is consulted.
        await im.evaluate_all(_ctx())
        assert seen == ["ThrottledInstinct"]
        # Second call: throttled (60s interval) — gate must NOT be consulted.
        await im.evaluate_all(_ctx())
        assert seen == ["ThrottledInstinct"]   # unchanged

    @pytest.mark.asyncio
    async def test_gate_denial_does_not_advance_last_trigger_s(self) -> None:
        """Gate denial is orthogonal to the node's intrinsic throttle clock."""
        im = InstinctMasterNode(bus=_bus())
        node = ThrottledInstinct(bus=_bus())
        im.register(node)
        # Deny the node — its _last_trigger_s must stay at -inf.
        im.set_pre_evaluate_gate(lambda n, ctx: False)
        await im.evaluate_all(_ctx())
        assert node._last_trigger_s == -math.inf
        # Now lift the gate — the throttle clock was not advanced, so the
        # node evaluates on the very next tick (this is the load-bearing
        # invariant: a scheduler denial does not delay an instinct's
        # intrinsic cadence).
        im.set_pre_evaluate_gate(None)
        await im.evaluate_all(_ctx())
        assert "ThrottledInstinct" in im.last_evaluated_ids
        assert node._last_trigger_s > -math.inf

    @pytest.mark.asyncio
    async def test_gate_exception_fails_open(self) -> None:
        """A gate that raises is logged and treated as allowing the node."""
        sink = _CaptureSink()
        im = InstinctMasterNode(bus=_bus(), log_sinks=[sink])
        im.register(AlwaysProposesInstinct(bus=_bus()))

        def bad_gate(node: BaseInstinctNode, ctx: Context) -> bool:
            raise RuntimeError("gate is broken")

        im.set_pre_evaluate_gate(bad_gate)
        proposals = await im.evaluate_all(_ctx())

        assert len(proposals) == 1   # fail-open: node still evaluated
        assert "AlwaysProposesInstinct" in im.last_evaluated_ids

        await asyncio.sleep(0)   # let fire-and-forget log tasks flush
        errors = [
            ev for ev in sink.events
            if ev.message == "Pre-evaluate gate raised; treating as allowed"
        ]
        assert len(errors) == 1
        assert errors[0].data["node_id"] == "AlwaysProposesInstinct"
        assert "gate is broken" in errors[0].data["error"]

    @pytest.mark.asyncio
    async def test_reflex_nodes_are_not_gated(self) -> None:
        """A deny-all gate must not block reflexes (they use evaluate_reflexes)."""
        im = InstinctMasterNode(bus=_bus())
        im.register(AlwaysProposesReflex(bus=_bus()))
        im.set_pre_evaluate_gate(lambda node, ctx: False)
        proposals = await im.evaluate_reflexes(_ctx())
        assert len(proposals) == 1
        assert proposals[0].instinct_id == "AlwaysProposesReflex"

    def test_async_gate_rejected_at_install(self) -> None:
        """Coroutine-function gates are caught at install time, not evaluation."""
        im = InstinctMasterNode(bus=_bus())

        async def async_gate(node: BaseInstinctNode, ctx: Context) -> bool:
            return True

        with pytest.raises(TypeError, match="must be sync"):
            im.set_pre_evaluate_gate(async_gate)   # type: ignore[arg-type]

    def test_sync_gate_returning_coroutine_callable_not_rejected(self) -> None:
        """Sanity: a sync function is accepted; only ``async def`` is rejected."""
        im = InstinctMasterNode(bus=_bus())
        im.set_pre_evaluate_gate(lambda node, ctx: True)   # must not raise
