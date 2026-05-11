"""
Integration tests for:
  - InterruptPolicy.CHECKPOINT
  - InterruptPolicy.ROLLBACK
  - runtime.emergency_stop()
  - ReflexConflictError
"""

from __future__ import annotations

import asyncio
import time

import pytest

from arachnite import SignalBus
from arachnite.context import ContextNode
from arachnite.exceptions import ReflexConflictError
from arachnite.models import (
    ActionStep,
    Context,
    InterruptPolicy,
    InterruptRequest,
    Proposal,
    Result,
    StepResult,
)
from arachnite.nodes.action import ActionMasterNode, BaseActionNode, MultiStepActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseReflexInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from tests.conftest import make_context, make_proposal, make_signal

# ── Shared helpers ────────────────────────────────────────────────────────────

def _make_interrupt(action_id: str = "Test") -> InterruptRequest:
    return InterruptRequest(
        new_proposal=make_proposal(action_id=action_id),
        requesting_instinct_id="test",
        reason="test interrupt",
    )


# ══════════════════════════════════════════════════════════════════════════════
# CHECKPOINT policy
# ══════════════════════════════════════════════════════════════════════════════

class CheckpointAction(MultiStepActionNode):
    """
    step1 (checkpoint=False, interruptible=True)
    step2 (checkpoint=True,  interruptible=True)
    step3 (checkpoint=False, interruptible=True)
    """
    node_id          = "CheckpointAction"
    interrupt_policy = InterruptPolicy.CHECKPOINT
    completed: list[str]

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.completed = []

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step1", interruptible=True,  checkpoint=False),
            ActionStep("step2", interruptible=True,  checkpoint=True),
            ActionStep("step3", interruptible=True,  checkpoint=False),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        await asyncio.sleep(0.01)
        self.completed.append(step.name)
        return StepResult(step_name=step.name, success=True)


class TestCheckpointPolicy:
    @pytest.mark.asyncio
    async def test_interrupt_at_checkpoint_stops_execution(self) -> None:
        bus    = SignalBus()
        action = CheckpointAction(bus=bus)

        # Start execution; request interrupt while step1 is running.
        # CHECKPOINT policy only stops at a step where checkpoint=True.
        # step1 is NOT a checkpoint → interrupt deferred.
        # step2 IS a checkpoint → execution stops before step2 runs.
        async def _run() -> Result:
            return await action.execute(make_proposal(action_id=action.node_id))

        task = asyncio.create_task(_run())
        await asyncio.sleep(0.003)           # step1 has started
        action.request_interrupt(_make_interrupt())
        result = await task

        assert result.interrupted
        # step1 completes; interrupt fires at step2 (the first checkpoint)
        assert "step1" in action.completed
        assert "step3" not in action.completed

    @pytest.mark.asyncio
    async def test_no_interrupt_runs_all_steps(self) -> None:
        bus    = SignalBus()
        action = CheckpointAction(bus=bus)
        result = await action.execute(make_proposal(action_id=action.node_id))
        assert result.success
        assert action.completed == ["step1", "step2", "step3"]

    @pytest.mark.asyncio
    async def test_interrupt_on_non_checkpoint_step_deferred(self) -> None:
        bus    = SignalBus()
        action = CheckpointAction(bus=bus)

        # Request interrupt mid-execution after step1 (which is not a checkpoint)
        async def _run() -> Result:
            return await action.execute(make_proposal(action_id=action.node_id))

        task = asyncio.create_task(_run())
        await asyncio.sleep(0.005)          # step1 is running
        action.request_interrupt(_make_interrupt())
        result = await task

        # Must stop at step2 (first checkpoint), not mid-step1
        assert result.interrupted
        assert "step3" not in action.completed


# ══════════════════════════════════════════════════════════════════════════════
# ROLLBACK policy
# ══════════════════════════════════════════════════════════════════════════════

class RollbackAction(MultiStepActionNode):
    """
    step1 (interruptible=True)
    step2 (interruptible=False, has rollback)
    step3 (interruptible=True)
    """
    node_id          = "RollbackAction"
    interrupt_policy = InterruptPolicy.ROLLBACK
    rolled_back: list[str]

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.rolled_back = []

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
        await asyncio.sleep(0.01)
        return StepResult(step_name=step.name, success=True)


class TestRollbackPolicy:
    @pytest.mark.asyncio
    async def test_rollback_called_after_mandatory_block(self) -> None:
        bus    = SignalBus()
        action = RollbackAction(bus=bus)

        async def _run() -> Result:
            return await action.execute(make_proposal(action_id=action.node_id))

        task = asyncio.create_task(_run())
        await asyncio.sleep(0.003)          # step1 is running — request interrupt
        action.request_interrupt(_make_interrupt())
        result = await task

        # ROLLBACK policy: interrupt fires at first interruptible step after
        # the mandatory block (step3), then rollback of step2 is invoked.
        assert result.interrupted
        assert result.rolled_back
        assert "step2" in action.rolled_back

    @pytest.mark.asyncio
    async def test_mandatory_block_completes_despite_interrupt(self) -> None:
        """Interrupt requested during step2 (mandatory) — step2 must finish first."""
        bus    = SignalBus()
        action = RollbackAction(bus=bus)
        completed_steps: list[str] = []

        original_execute = action.execute_step

        async def tracked_execute(
            step: ActionStep, proposal: Proposal, completed: list[StepResult]
        ) -> StepResult:
            if step.name == "step2":
                action.request_interrupt(_make_interrupt())
            r = await original_execute(step, proposal, completed)
            completed_steps.append(step.name)
            return r

        action.execute_step = tracked_execute  # type: ignore[method-assign]
        await action.execute(make_proposal(action_id=action.node_id))

        # step2 must have completed even though interrupt was requested during it
        assert "step2" in completed_steps

    @pytest.mark.asyncio
    async def test_no_interrupt_runs_all_steps_without_rollback(self) -> None:
        bus    = SignalBus()
        action = RollbackAction(bus=bus)
        result = await action.execute(make_proposal(action_id=action.node_id))
        assert result.success
        assert not result.rolled_back
        assert action.rolled_back == []


# ══════════════════════════════════════════════════════════════════════════════
# emergency_stop()
# ══════════════════════════════════════════════════════════════════════════════


class SlowAction(BaseActionNode):
    """Simulates a long-running action (0.5 s) to test emergency_stop."""
    node_id   = "SlowAction"
    timeout_s = 5.0
    started   = False
    finished  = False

    async def execute(self, proposal: Proposal) -> Result:
        SlowAction.started = True
        await asyncio.sleep(0.5)
        SlowAction.finished = True
        return Result(action_id=self.node_id, success=True)


class SlowSense(BaseSenseNode):
    node_id     = "SlowSense"
    signal_kind = "thermal"

    async def read(self) -> Signal:  # noqa: F821
        return make_signal(kind=self.signal_kind, value=90.0)


class AlwaysInstinct(BaseReflexInstinctNode):
    node_id  = "AlwaysInstinct"
    priority = 250

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return make_proposal(action_id="SlowAction", instinct_id=self.node_id)


class TestEmergencyStop:
    @pytest.mark.asyncio
    async def test_emergency_stop_halts_runtime(self) -> None:
        SlowAction.started  = False
        SlowAction.finished = False

        bus             = SignalBus()
        sense_master    = SenseMasterNode(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        decision_master = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        action_master   = ActionMasterNode(bus=bus)

        sense_master.register(SlowSense(bus=bus))
        instinct_master.register(AlwaysInstinct(bus=bus))
        action_master.register(SlowAction(bus=bus))

        rt = ArachniteRuntime(
            sense_master    = sense_master,
            context         = ContextNode(),
            instinct_master = instinct_master,
            decision_master = decision_master,
            action_master   = action_master,
            bus             = bus,
            tick_rate_hz    = 20.0,
        )
        await rt.start()
        await asyncio.sleep(0.05)    # let at least one tick start
        t0 = time.monotonic()
        await rt.emergency_stop()
        elapsed = time.monotonic() - t0

        # emergency_stop must return quickly (well under 0.5 s slow-action duration)
        assert elapsed < 0.4
        assert not rt.is_running

    @pytest.mark.asyncio
    async def test_emergency_stop_sets_stop_event(self) -> None:
        bus             = SignalBus()
        sense_master    = SenseMasterNode(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        decision_master = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        action_master   = ActionMasterNode(bus=bus)

        rt = ArachniteRuntime(
            sense_master    = sense_master,
            context         = ContextNode(),
            instinct_master = instinct_master,
            decision_master = decision_master,
            action_master   = action_master,
            bus             = bus,
            tick_rate_hz    = 10.0,
        )
        await rt.start()
        await rt.emergency_stop()
        # wait() should return immediately since the stop event is set
        done, _ = await asyncio.wait([asyncio.create_task(rt.wait())], timeout=0.1)
        assert done


# ══════════════════════════════════════════════════════════════════════════════
# ReflexConflictError
# ══════════════════════════════════════════════════════════════════════════════

class ReflexA(BaseReflexInstinctNode):
    node_id  = "ReflexA"
    priority = 250

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return make_proposal(action_id="ActionA", instinct_id=self.node_id, priority=250)


class ReflexB(BaseReflexInstinctNode):
    node_id  = "ReflexB"
    priority = 250   # same priority as ReflexA → conflict

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return make_proposal(action_id="ActionB", instinct_id=self.node_id, priority=250)


class TestReflexConflictError:
    @pytest.mark.asyncio
    async def test_conflict_raised_when_policy_is_raise(self) -> None:
        bus             = SignalBus()
        instinct_master = InstinctMasterNode(bus=bus, reflex_conflict="raise")
        instinct_master.register(ReflexA(bus=bus))
        instinct_master.register(ReflexB(bus=bus))

        ctx = make_context([make_signal(value=99.0)])

        with pytest.raises(ReflexConflictError) as exc_info:
            await instinct_master.evaluate_reflexes(ctx)

        assert exc_info.value.priority == 250
        assert "ReflexA" in exc_info.value.node_ids
        assert "ReflexB" in exc_info.value.node_ids

    @pytest.mark.asyncio
    async def test_no_conflict_raised_with_dispatch_all_policy(self) -> None:
        bus             = SignalBus()
        instinct_master = InstinctMasterNode(bus=bus, reflex_conflict="dispatch_all")
        instinct_master.register(ReflexA(bus=bus))
        instinct_master.register(ReflexB(bus=bus))

        ctx      = make_context([make_signal(value=99.0)])
        proposals = await instinct_master.evaluate_reflexes(ctx)
        assert len(proposals) == 2

    @pytest.mark.asyncio
    async def test_no_conflict_when_priorities_differ(self) -> None:
        class ReflexC(BaseReflexInstinctNode):
            node_id  = "ReflexC"
            priority = 300

            async def evaluate(self, ctx: Context) -> Proposal | None:
                return make_proposal(instinct_id=self.node_id, priority=300)

        bus             = SignalBus()
        instinct_master = InstinctMasterNode(bus=bus, reflex_conflict="raise")
        instinct_master.register(ReflexA(bus=bus))   # priority 250
        instinct_master.register(ReflexC(bus=bus))   # priority 300 — no conflict

        ctx = make_context([make_signal(value=99.0)])
        # Should not raise since ReflexC has higher priority, no tie
        proposals = await instinct_master.evaluate_reflexes(ctx)
        assert proposals[0].instinct_id == "ReflexC"
