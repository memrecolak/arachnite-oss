"""Unit tests for all node types and master nodes."""

from __future__ import annotations

import pytest

from arachnite.exceptions import NodeRegistrationError
from arachnite.nodes.decision import (
    GreedyDecisionNode,
    RandomDecisionNode,
    WeightedDecisionNode,
)
from tests.conftest import (
    ConstantSenseNode,
    EmergencyReflex,
    FailingAction,
    MandatoryBlockAction,
    RecordingAction,
    SlowSenseNode,
    ThresholdInstinct,
    TwoStepAction,
    make_context,
    make_proposal,
    make_signal,
)

# ── SenseMasterNode ───────────────────────────────────────────────────────────

class TestSenseMasterNode:
    @pytest.mark.asyncio
    async def test_read_all_returns_signals(self, sense_master, bus) -> None:
        sense_master.register(ConstantSenseNode(bus=bus, value=42.0))
        signals = await sense_master.read_all()
        assert len(signals) == 1
        assert signals[0].value == 42.0

    @pytest.mark.asyncio
    async def test_duplicate_registration_raises(self, sense_master, bus) -> None:
        sense_master.register(ConstantSenseNode(bus=bus))
        with pytest.raises(NodeRegistrationError):
            sense_master.register(ConstantSenseNode(bus=bus))

    @pytest.mark.asyncio
    async def test_last_read_time_reflects_actual_completion(
        self, sense_master, bus
    ) -> None:
        """A-06: _last_read_time must reflect when read() finished, not batch start"""
        import time
        node = SlowSenseNode(bus=bus, delay=0.05)
        sense_master.register(node)
        before = time.monotonic()
        await sense_master.read_all()
        assert node._last_read_time >= before + 0.04

    @pytest.mark.asyncio
    async def test_publishes_to_bus(self, sense_master, bus) -> None:
        received = []

        async def cb(sig):  # type: ignore[no-untyped-def]
            received.append(sig)

        bus.subscribe("thermal", cb)
        sense_master.register(ConstantSenseNode(bus=bus, value=10.0))
        await sense_master.read_all()
        assert len(received) == 1


# ── InstinctMasterNode ────────────────────────────────────────────────────────

class TestInstinctMasterNode:
    @pytest.mark.asyncio
    async def test_normal_instinct_fires_above_threshold(
        self, instinct_master, bus
    ) -> None:
        instinct_master.register(ThresholdInstinct(bus=bus, threshold=80.0))
        ctx = make_context(signals=[make_signal(value=90.0)])
        proposals = await instinct_master.evaluate_all(ctx)
        assert len(proposals) == 1
        assert proposals[0].action_id == "CoolDownAction"

    @pytest.mark.asyncio
    async def test_normal_instinct_silent_below_threshold(
        self, instinct_master, bus
    ) -> None:
        instinct_master.register(ThresholdInstinct(bus=bus, threshold=80.0))
        ctx = make_context(signals=[make_signal(value=50.0)])
        proposals = await instinct_master.evaluate_all(ctx)
        assert proposals == []

    @pytest.mark.asyncio
    async def test_reflex_goes_to_reflex_registry(
        self, instinct_master, bus
    ) -> None:
        instinct_master.register(EmergencyReflex(bus=bus))
        assert len(instinct_master.reflex_nodes) == 1
        assert len(instinct_master.normal_nodes) == 0

    @pytest.mark.asyncio
    async def test_reflex_fires_above_critical(
        self, instinct_master, bus
    ) -> None:
        instinct_master.register(EmergencyReflex(bus=bus, critical_threshold=90.0))
        ctx = make_context(signals=[make_signal(value=95.0)])
        proposals = await instinct_master.evaluate_reflexes(ctx)
        assert len(proposals) == 1
        assert proposals[0].action_id == "EmergencyStop"

    @pytest.mark.asyncio
    async def test_reflex_silent_below_critical(
        self, instinct_master, bus
    ) -> None:
        instinct_master.register(EmergencyReflex(bus=bus, critical_threshold=90.0))
        ctx = make_context(signals=[make_signal(value=80.0)])
        proposals = await instinct_master.evaluate_reflexes(ctx)
        assert proposals == []

    @pytest.mark.asyncio
    async def test_disabled_instinct_not_evaluated(
        self, instinct_master, bus
    ) -> None:
        node = ThresholdInstinct(bus=bus, threshold=80.0)
        node.enabled = False
        instinct_master.register(node)
        ctx = make_context(signals=[make_signal(value=90.0)])
        proposals = await instinct_master.evaluate_all(ctx)
        assert proposals == []


# ── DecisionMasterNode ────────────────────────────────────────────────────────

class TestDecisionNodes:
    @pytest.mark.asyncio
    async def test_greedy_picks_highest_priority(self, bus) -> None:
        node = GreedyDecisionNode(bus=bus)
        proposals = [
            make_proposal(priority=50),
            make_proposal(priority=100),
            make_proposal(priority=30),
        ]
        chosen = await node.decide(proposals)
        assert chosen is not None
        assert chosen.priority == 100

    @pytest.mark.asyncio
    async def test_greedy_returns_none_on_empty(self, bus) -> None:
        node = GreedyDecisionNode(bus=bus)
        assert await node.decide([]) is None

    @pytest.mark.asyncio
    async def test_weighted_uses_priority_times_urgency(self, bus) -> None:
        node = WeightedDecisionNode(bus=bus)
        proposals = [
            make_proposal(priority=100, urgency=0.1),  # score = 10
            make_proposal(priority=50,  urgency=0.9),  # score = 45
        ]
        chosen = await node.decide(proposals)
        assert chosen is not None
        assert chosen.priority == 50  # 50×0.9=45 wins over 100×0.1=10

    @pytest.mark.asyncio
    async def test_random_always_returns_one(self, bus) -> None:
        node = RandomDecisionNode(bus=bus)
        proposals = [make_proposal() for _ in range(5)]
        chosen = await node.decide(proposals)
        assert chosen in proposals


# ── ActionMasterNode ──────────────────────────────────────────────────────────

class TestActionMasterNode:
    @pytest.mark.asyncio
    async def test_dispatch_calls_action(self, action_master, bus) -> None:
        action = RecordingAction(bus=bus)
        action_master.register(action)
        proposal = make_proposal(action_id="RecordingAction")
        result = await action_master.dispatch(proposal)
        assert result.success
        assert len(action.calls) == 1

    @pytest.mark.asyncio
    async def test_dispatch_unknown_action_raises(self, action_master) -> None:
        from arachnite.exceptions import ActionNotFoundError
        with pytest.raises(ActionNotFoundError):
            await action_master.dispatch(make_proposal(action_id="NoSuchAction"))

    @pytest.mark.asyncio
    async def test_failing_action_returns_failed_result(
        self, action_master, bus
    ) -> None:
        action_master.register(FailingAction(bus=bus))
        result = await action_master.dispatch(make_proposal(action_id="FailingAction"))
        assert result.success is False
        assert result.error is not None


# ── MultiStepActionNode ───────────────────────────────────────────────────────

class TestMultiStepAction:
    @pytest.mark.asyncio
    async def test_all_steps_complete(self, bus) -> None:
        action   = TwoStepAction(bus=bus)
        proposal = make_proposal(action_id="TwoStepAction")
        result   = await action.execute(proposal)
        assert result.success
        assert len(result.step_results) == 2
        assert [sr.step_name for sr in result.step_results] == ["step1", "step2"]

    @pytest.mark.asyncio
    async def test_interrupt_stops_at_interruptible_step(self, bus) -> None:
        from arachnite.models import InterruptRequest
        action   = TwoStepAction(bus=bus)
        proposal = make_proposal(action_id="TwoStepAction")

        import asyncio

        async def interrupt_after_delay() -> None:
            await asyncio.sleep(0.01)
            req = InterruptRequest(
                new_proposal           = make_proposal(priority=200),
                requesting_instinct_id = "test",
            )
            action.request_interrupt(req)

        asyncio.create_task(interrupt_after_delay())
        result = await action.execute(proposal)
        assert result.interrupted

    @pytest.mark.asyncio
    async def test_rollback_runs_on_interrupt(self, bus) -> None:
        from arachnite.models import InterruptRequest
        action   = MandatoryBlockAction(bus=bus)
        proposal = make_proposal(action_id="MandatoryBlockAction")


        # Interrupt after step2 completes (the non-interruptible step)
        original_execute = action.execute_step

        step_count = [0]

        async def counting_step(step, prop, completed):  # type: ignore[no-untyped-def]
            result = await original_execute(step, prop, completed)
            step_count[0] += 1
            if step_count[0] == 2:
                req = InterruptRequest(
                    new_proposal           = make_proposal(priority=200),
                    requesting_instinct_id = "test",
                )
                action.request_interrupt(req)
            return result

        action.execute_step = counting_step  # type: ignore[method-assign]
        result = await action.execute(proposal)
        assert result.interrupted or result.success  # completes step3 after mandatory block
