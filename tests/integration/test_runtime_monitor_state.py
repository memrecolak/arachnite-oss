"""ArachniteRuntime.monitor_state() is built from what the runtime observed."""

from __future__ import annotations

import asyncio

import pytest

from arachnite import SignalBus
from arachnite.context import ContextNode
from arachnite.models import ActionStep, Context, InterruptPolicy, Proposal, Result, StepResult
from arachnite.nodes.action import ActionMasterNode, BaseActionNode, MultiStepActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseReflexInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from arachnite.safety_monitor import SafetyMonitorRegistry


class _AlwaysReflex(BaseReflexInstinctNode):
    node_id = "AlwaysReflex"
    priority = 250
    target = "Brace"

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(instinct_id=self.node_id, action_id=self.target,
                        priority=self.priority, urgency=1.0)


class _Brace(BaseActionNode):
    node_id = "Brace"
    resources = frozenset({"arm"})

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class _Hold(MultiStepActionNode):
    node_id = "Hold"
    interrupt_policy = InterruptPolicy.ALWAYS
    timeout_s = 5.0

    def steps(self) -> list[ActionStep]:
        return [ActionStep("a", interruptible=False), ActionStep("b", interruptible=False)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        await asyncio.sleep(0.2)
        return StepResult(step_name=step.name, success=True)


def _runtime(target: str = "Brace") -> tuple[ArachniteRuntime, ActionMasterNode]:
    bus = SignalBus()
    im = InstinctMasterNode(bus=bus)
    reflex = _AlwaysReflex(bus)
    reflex.target = target
    im.register(reflex)
    am = ActionMasterNode(bus=bus)
    am.register(_Brace(bus))
    am.register(_Hold(bus))
    rt = ArachniteRuntime(
        sense_master=SenseMasterNode(bus=bus), context=ContextNode(),
        instinct_master=im,
        decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
        action_master=am, bus=bus, tick_rate_hz=50.0,
    )
    return rt, am


class TestMonitorState:
    @pytest.mark.asyncio
    async def test_reflex_tick_is_healthy(self) -> None:
        rt, _ = _runtime()
        await rt.tick()
        state = rt.monitor_state()
        assert state.reflex_fired is True
        assert state.reflex_action_dispatched is True
        assert state.reflex_routed_through_decision is False
        assert state.total_reflex_nodes == 1 and state.active_reflex_nodes == 1
        assert state.tick_budget_ms == pytest.approx(20.0)
        violations = await SafetyMonitorRegistry.default(rt.bus).check_all(rt.tick_count, state)
        assert violations == []
        await rt.wait_idle(1.0)

    @pytest.mark.asyncio
    async def test_reflex_with_unknown_action_is_a_dead_end(self) -> None:
        rt, _ = _runtime(target="NoSuchAction")
        await rt.tick()
        state = rt.monitor_state()
        assert state.reflex_fired and not state.reflex_action_dispatched
        violations = await SafetyMonitorRegistry.default(rt.bus).check_all(rt.tick_count, state)
        assert [v.property_name for v in violations] == ["reflex_dispatch_guarantee"]

    @pytest.mark.asyncio
    async def test_block_in_progress_is_reported(self) -> None:
        rt, am = _runtime()
        am.launch(Proposal(instinct_id="t", action_id="Hold", priority=50, urgency=0.5))
        await asyncio.sleep(0.05)
        assert rt.monitor_state().mandatory_block_active is True
        await am.cancel_all(timeout=1.0)

    def test_stop_strictly_inside_a_block_is_flagged(self) -> None:
        rt, _ = _runtime()
        inside = Result(action_id="Hold", success=False, interrupted=True, stopped_at_step="b")
        entry = Result(action_id="Hold", success=False, interrupted=True, stopped_at_step="a")
        preempted = Result(action_id="Hold", success=False, interrupted=True,
                           preempted=True, stopped_at_step="b")
        assert rt._stopped_inside_block(inside) is True
        assert rt._stopped_inside_block(entry) is False      # block entry is a stop point
        assert rt._stopped_inside_block(preempted) is False  # declared exception
