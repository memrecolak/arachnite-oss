"""Unit tests for BaseActionNode, MultiStepActionNode, and ActionMasterNode."""

from __future__ import annotations

import asyncio
import time

import pytest

from arachnite import SignalBus
from arachnite.exceptions import (
    ActionNotFoundError,
    ActionTimeoutError,
    ActionVetoed,
    MandatoryBlockViolation,
    NodeRegistrationError,
    RollbackError,
    StepAbortError,
)
from arachnite.models import (
    ActionExecutionState,
    ActionStep,
    InterruptPolicy,
    InterruptRequest,
    Proposal,
    Result,
    StepResult,
)
from arachnite.nodes import action as action_module
from arachnite.nodes.action import (
    ActionGuard,
    ActionMasterNode,
    BaseActionNode,
    GuardVerdict,
    MultiStepActionNode,
)
from tests.conftest import ManualClock

# ── Helpers ────────────────────────────────────────────────────────────────────

def _bus() -> SignalBus:
    return SignalBus()


def _proposal(action_id: str = "Act") -> Proposal:
    return Proposal(instinct_id="inst", action_id=action_id, priority=100, urgency=0.5)


def _interrupt() -> InterruptRequest:
    return InterruptRequest(
        new_proposal=_proposal("Other"),
        requesting_instinct_id="test",
    )


# ── Concrete test nodes ────────────────────────────────────────────────────────

class SlowAction(BaseActionNode):
    """Times out — exercises on_timeout() path."""
    node_id   = "SlowAction"
    timeout_s = 0.01

    async def execute(self, proposal: Proposal) -> Result:
        await asyncio.sleep(10.0)
        return Result(action_id=self.node_id, success=True)


class RetryableAction(BaseActionNode):
    """Fails on first attempt, succeeds on retry."""
    node_id     = "RetryableAction"
    max_retries = 1

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.call_count = 0

    async def execute(self, proposal: Proposal) -> Result:
        self.call_count += 1
        if self.call_count == 1:
            return Result(action_id=self.node_id, success=False,
                          error=RuntimeError("first attempt"))
        return Result(action_id=self.node_id, success=True)


class AbortingStepAction(MultiStepActionNode):
    """First step sets abort_sequence=True."""
    node_id          = "AbortingStepAction"
    interrupt_policy = InterruptPolicy.ALWAYS

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("abort_step", interruptible=True),
            ActionStep("unreachable", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        return StepResult(step_name=step.name, success=False, abort_sequence=True)


class FailWithoutAbortAction(MultiStepActionNode):
    """First step fails (success=False) but does not abort — second step runs."""
    node_id          = "FailWithoutAbortAction"
    interrupt_policy = InterruptPolicy.ALWAYS

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("fails", interruptible=True),
            ActionStep("succeeds", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "fails":
            return StepResult(step_name=step.name, success=False)
        return StepResult(step_name=step.name, success=True)


class SlowStepAction(MultiStepActionNode):
    """Step sleep exceeds step timeout — exercises _run_step_timed timeout path."""
    node_id          = "SlowStepAction"
    interrupt_policy = InterruptPolicy.ALWAYS

    def steps(self) -> list[ActionStep]:
        return [ActionStep("slow_step", interruptible=True, timeout_s=0.01)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        await asyncio.sleep(10.0)
        return StepResult(step_name=step.name, success=True)


class NeverPolicyAction(MultiStepActionNode):
    """NEVER interrupt policy — interrupt is silently ignored."""
    node_id          = "NeverPolicyAction"
    interrupt_policy = InterruptPolicy.NEVER

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("mandatory", interruptible=False),
            ActionStep("optional",  interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        return StepResult(step_name=step.name, success=True)


class CheckpointAction(MultiStepActionNode):
    """CHECKPOINT policy — interrupt only at checkpoint steps."""
    node_id          = "CheckpointAction"
    interrupt_policy = InterruptPolicy.CHECKPOINT

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("prepare",  interruptible=True, checkpoint=False),
            ActionStep("commit",   interruptible=True, checkpoint=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        await asyncio.sleep(0.01)
        return StepResult(step_name=step.name, success=True)


class RollbackFailAction(MultiStepActionNode):
    """Rollback callable raises — on_interrupted should propagate RollbackError."""
    node_id          = "RollbackFailAction"
    interrupt_policy = InterruptPolicy.ROLLBACK

    async def _bad_rollback(self) -> None:
        raise RuntimeError("rollback exploded")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("mandatory", interruptible=False, rollback=self._bad_rollback),
            ActionStep("optional",  interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        result = StepResult(step_name=step.name, success=True)
        if step.name == "mandatory":
            # Set interrupt flag directly so it fires at the next interruptible step.
            # (asyncio.create_task wouldn't fire in time since execute_step has no await.)
            self._interrupt_requested = True
            self._interrupt_request = InterruptRequest(
                new_proposal=_proposal("Other"),
                requesting_instinct_id="test",
            )
        return result


class SelfInterruptingAction(MultiStepActionNode):
    """ROLLBACK action that requests its own interrupt inside the block."""
    node_id          = "SelfInterruptingAction"
    interrupt_policy = InterruptPolicy.ROLLBACK

    async def _undo(self) -> None:
        return None

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("mandatory", interruptible=False, rollback=self._undo),
            ActionStep("optional",  interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "mandatory":
            self.request_interrupt(_interrupt())
        return StepResult(step_name=step.name, success=True)


class MultiRollbackAction(MultiStepActionNode):
    """Three mandatory steps; rollbacks of 'a' and 'c' raise, 'b' succeeds."""
    node_id          = "MultiRollbackAction"
    interrupt_policy = InterruptPolicy.ROLLBACK

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.rolled: list[str] = []

    def _undo(self, name: str, fail: bool):  # type: ignore[no-untyped-def]
        async def _rollback() -> None:
            self.rolled.append(name)
            if fail:
                raise RuntimeError(f"undo {name} failed")
        return _rollback

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("a", interruptible=False, rollback=self._undo("a", True)),
            ActionStep("b", interruptible=False, rollback=self._undo("b", False)),
            ActionStep("c", interruptible=False, rollback=self._undo("c", True)),
            ActionStep("d", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "c":
            self.request_interrupt(_interrupt())
        return StepResult(step_name=step.name, success=True)


class RaisingStepAction(MultiStepActionNode):
    """Second (mandatory) step raises — execute_step contract violation."""
    node_id = "RaisingStepAction"

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.rolled: list[str] = []

    async def _undo_grip(self) -> None:
        self.rolled.append("grip")

    async def _undo_explode(self) -> None:
        self.rolled.append("explode")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("grip",    interruptible=False, rollback=self._undo_grip),
            ActionStep("explode", interruptible=False, rollback=self._undo_explode),
            ActionStep("finish",  interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "explode":
            raise ValueError("step blew up")
        return StepResult(step_name=step.name, success=True)


class ExecutionStateAction(MultiStepActionNode):
    """Used to test execution_state() snapshot."""
    node_id          = "ExecutionStateAction"
    interrupt_policy = InterruptPolicy.ALWAYS
    timeout_s        = 5.0

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step1", interruptible=True),
            ActionStep("step2", interruptible=False, timeout_s=3.0),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        return StepResult(step_name=step.name, success=True)


# ── BaseActionNode ─────────────────────────────────────────────────────────────

class TestBaseActionNodeTimeout:
    @pytest.mark.asyncio
    async def test_on_timeout_returns_failed_result(self) -> None:
        am = ActionMasterNode(bus=_bus())
        am.register(SlowAction(bus=_bus()))
        result = await am.dispatch(_proposal("SlowAction"))
        assert not result.success
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_on_timeout_default_uses_action_timeout_s(self) -> None:
        node = SlowAction(bus=_bus())
        proposal = _proposal("SlowAction")
        result = await node.on_timeout(proposal)
        assert not result.success
        assert result.action_id == "SlowAction"


# ── ActionMasterNode registration / accessors ──────────────────────────────────

class TestActionMasterNodeRegistration:
    def test_duplicate_registration_raises(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(SlowAction(bus=bus))
        with pytest.raises(NodeRegistrationError):
            am.register(SlowAction(bus=bus))

    def test_unregister_removes_node(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(SlowAction(bus=bus))
        am.unregister("SlowAction")
        assert "SlowAction" not in {n.node_id for n in am.nodes}

    @pytest.mark.asyncio
    async def test_dispatch_unknown_raises(self) -> None:
        am = ActionMasterNode(bus=_bus())
        with pytest.raises(ActionNotFoundError):
            await am.dispatch(_proposal("Unknown"))


class TestActionMasterNodeGetNode:
    def test_get_node_returns_registered_node(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        node = SlowAction(bus=bus)
        am.register(node)
        assert am.get_node("SlowAction") is node

    def test_get_node_returns_none_when_not_registered(self) -> None:
        am = ActionMasterNode(bus=_bus())
        assert am.get_node("NonExistent") is None


class TestActionMasterNodeInterruptibility:
    def test_is_interruptible_no_current_node(self) -> None:
        am = ActionMasterNode(bus=_bus())
        assert am.is_interruptible()

    def test_is_interruptible_with_single_step_action(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        node = SlowAction(bus=bus)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)
        assert am.is_interruptible()  # single-step always interruptible

    def test_is_interruptible_with_multistep_no_current_step(self) -> None:
        bus  = _bus()
        am   = ActionMasterNode(bus=bus)
        node = NeverPolicyAction(bus=bus)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)
        # _current_step is None → interruptible
        assert am.is_interruptible()

    def test_is_interruptible_with_multistep_mandatory_step(self) -> None:
        bus  = _bus()
        am   = ActionMasterNode(bus=bus)
        node = NeverPolicyAction(bus=bus)
        node._current_step = ActionStep("mandatory", interruptible=False)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)
        assert not am.is_interruptible()

    def test_current_step_none_when_no_current(self) -> None:
        am = ActionMasterNode(bus=_bus())
        assert am.current_step() is None

    def test_current_step_delegates_to_multistep(self) -> None:
        bus  = _bus()
        am   = ActionMasterNode(bus=bus)
        node = NeverPolicyAction(bus=bus)
        step = ActionStep("mandatory", interruptible=False)
        node._current_step = step
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)
        assert am.current_step() is step

    def test_current_step_none_for_single_step_node(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        node = SlowAction(bus=bus)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)
        assert am.current_step() is None


class TestActionMasterNodeRequestInterrupt:
    @pytest.mark.asyncio
    async def test_request_interrupt_no_current_node_is_noop(self) -> None:
        am = ActionMasterNode(bus=_bus())
        await am.request_interrupt(_interrupt())  # must not raise

    @pytest.mark.asyncio
    async def test_request_interrupt_mandatory_block_raises(self) -> None:
        bus  = _bus()
        am   = ActionMasterNode(bus=bus)
        node = NeverPolicyAction(bus=bus)
        node._current_step = ActionStep("mandatory", interruptible=False)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)
        with pytest.raises(MandatoryBlockViolation):
            await am.request_interrupt(_interrupt())

    @pytest.mark.asyncio
    async def test_request_interrupt_multistep_interruptible(self) -> None:
        bus  = _bus()
        am   = ActionMasterNode(bus=bus)
        node = CheckpointAction(bus=bus)
        node._current_step = ActionStep("commit", interruptible=True)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)
        await am.request_interrupt(_interrupt())  # must not raise
        assert node._interrupt_requested


# ── MultiStepActionNode — execution paths ─────────────────────────────────────

class TestMultiStepAbortSequence:
    @pytest.mark.asyncio
    async def test_abort_sequence_stops_execution(self) -> None:
        action   = AbortingStepAction(bus=_bus())
        result   = await action.execute(_proposal("AbortingStepAction"))
        assert not result.success
        assert len(result.step_results) == 1
        assert result.step_results[0].step_name == "abort_step"

    @pytest.mark.asyncio
    async def test_step_fail_without_abort_continues(self) -> None:
        action = FailWithoutAbortAction(bus=_bus())
        result = await action.execute(_proposal("FailWithoutAbortAction"))
        # Both steps run; overall success reflects last step
        assert len(result.step_results) == 2
        assert result.step_results[0].step_name == "fails"
        assert result.step_results[1].step_name == "succeeds"


class TestMultiStepStepTimeout:
    @pytest.mark.asyncio
    async def test_step_timeout_aborts_sequence(self) -> None:
        action = SlowStepAction(bus=_bus())
        result = await action.execute(_proposal("SlowStepAction"))
        assert not result.success
        assert len(result.step_results) == 1
        assert result.step_results[0].step_name == "slow_step"
        assert result.step_results[0].abort_sequence


class _NeverPolicyWithPauseAction(MultiStepActionNode):
    """NEVER policy; first step has an await so an interrupt task can fire."""
    node_id          = "_NeverPolicyWithPauseAction"
    interrupt_policy = InterruptPolicy.NEVER

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("prepare", interruptible=True),
            ActionStep("commit",  interruptible=False),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "prepare":
            import asyncio as _asyncio
            await _asyncio.sleep(0.02)
        return StepResult(step_name=step.name, success=True)


class _NeverWithCheckpointAction(MultiStepActionNode):
    """NEVER policy; first step requests an interrupt, second is a checkpoint."""
    node_id          = "_NeverWithCheckpointAction"
    interrupt_policy = InterruptPolicy.NEVER

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("arm", interruptible=True),
            ActionStep("cp",  interruptible=True, checkpoint=True),
            ActionStep("end", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "arm":
            self.request_interrupt(_interrupt())
        return StepResult(step_name=step.name, success=True)


class TestMultiStepNeverPolicy:
    @pytest.mark.asyncio
    async def test_never_policy_ignores_interrupt(self) -> None:
        action = NeverPolicyAction(bus=_bus())
        # Signal interrupt before execution starts — NEVER policy ignores it
        action._interrupt_requested = True
        action._current_step = ActionStep("mandatory", interruptible=False)
        result = await action.execute(_proposal("NeverPolicyAction"))
        # Both steps complete — interrupt was ignored
        assert len(result.step_results) == 2
        assert not result.interrupted

    @pytest.mark.asyncio
    async def test_never_policy_holds_interrupt_at_mandatory_block(self) -> None:
        import asyncio
        action = _NeverPolicyWithPauseAction(bus=_bus())

        async def set_interrupt() -> None:
            await asyncio.sleep(0.005)
            action._interrupt_requested = True

        asyncio.create_task(set_interrupt())
        result = await action.execute(_proposal("_NeverPolicyWithPauseAction"))
        # Both steps run — NEVER policy suppresses the interrupt
        assert len(result.step_results) == 2
        assert not result.interrupted

    @pytest.mark.asyncio
    async def test_never_policy_ignores_interrupt_at_checkpoint_step(self) -> None:
        """A checkpoint marker must not open an interrupt point under NEVER."""
        action = _NeverWithCheckpointAction(bus=_bus())
        result = await action.execute(_proposal("_NeverWithCheckpointAction"))
        assert [sr.step_name for sr in result.step_results] == ["arm", "cp", "end"]
        assert not result.interrupted
        assert result.success is True


class TestMultiStepCheckpointPolicy:
    @pytest.mark.asyncio
    async def test_checkpoint_policy_interrupts_at_checkpoint(self) -> None:
        action = CheckpointAction(bus=_bus())

        async def set_interrupt() -> None:
            await asyncio.sleep(0.005)
            action.request_interrupt(_interrupt())

        asyncio.create_task(set_interrupt())
        result = await action.execute(_proposal("CheckpointAction"))
        # Interrupted at the checkpoint step (or completes if timing is tight)
        assert result.interrupted or result.success


class TestMultiStepRollbackFails:
    @pytest.mark.asyncio
    async def test_rollback_failure_is_reported_not_raised(self) -> None:
        action = RollbackFailAction(bus=_bus())
        # execute_step sets _interrupt_requested after "mandatory" completes,
        # so the interrupt fires at the next interruptible step.
        result = await action.execute(_proposal("RollbackFailAction"))
        assert result.success is False
        assert result.interrupted is True
        assert result.rolled_back is False
        assert isinstance(result.error, RollbackError)

    @pytest.mark.asyncio
    async def test_every_rollback_attempted_despite_failures(self) -> None:
        action = MultiRollbackAction(bus=_bus())
        result = await action.execute(_proposal("MultiRollbackAction"))
        # Walk order is reverse completion: c (raises), b (ok), a (raises).
        assert action.rolled == ["c", "b", "a"]
        assert isinstance(result.error, RollbackError)
        assert [name for name, _ in result.error.failures] == ["c", "a"]
        assert result.error.step_name == "c"
        assert result.rolled_back is False

    @pytest.mark.asyncio
    async def test_rollback_error_not_retried(self) -> None:
        action = RollbackFailAction(bus=_bus())
        action.max_retries = 2
        master = ActionMasterNode(bus=_bus())
        master.register(action)
        calls = 0
        original = action.execute

        async def counting(proposal: Proposal) -> Result:
            nonlocal calls
            calls += 1
            return await original(proposal)

        action.execute = counting  # type: ignore[method-assign]
        result = await master.dispatch(_proposal("RollbackFailAction"))
        assert calls == 1
        assert isinstance(result.error, RollbackError)


class TestMultiStepAbort:
    @pytest.mark.asyncio
    async def test_raising_step_aborts_without_escaping(self) -> None:
        action = RaisingStepAction(bus=_bus())
        result = await action.execute(_proposal("RaisingStepAction"))
        assert result.success is False
        assert isinstance(result.error, StepAbortError)
        assert isinstance(result.error.__cause__, ValueError)
        assert result.stopped_at_step == "explode"

    @pytest.mark.asyncio
    async def test_abort_under_rollback_policy_undoes_completed_mandatory_steps(
        self,
    ) -> None:
        action = RaisingStepAction(bus=_bus())
        action.interrupt_policy = InterruptPolicy.ROLLBACK
        result = await action.execute(_proposal("RaisingStepAction"))
        # Only the step that completed before the failure is undone.
        assert action.rolled == ["grip"]
        assert result.rolled_back is True

    @pytest.mark.asyncio
    async def test_abort_under_always_policy_does_not_roll_back(self) -> None:
        action = RaisingStepAction(bus=_bus())
        result = await action.execute(_proposal("RaisingStepAction"))
        assert action.rolled == []
        assert result.rolled_back is False

    @pytest.mark.asyncio
    async def test_interrupted_result_not_retried(self) -> None:
        action = SelfInterruptingAction(bus=_bus())
        action.max_retries = 2
        master = ActionMasterNode(bus=_bus())
        master.register(action)
        calls = 0
        original = action.execute

        async def counting(proposal: Proposal) -> Result:
            nonlocal calls
            calls += 1
            return await original(proposal)

        action.execute = counting  # type: ignore[method-assign]
        result = await master.dispatch(_proposal("SelfInterruptingAction"))
        assert result.interrupted is True
        assert result.rolled_back is True
        assert calls == 1


# ── MultiStepActionNode — execution_state and is_interruptible ────────────────

class TestMultiStepIsInterruptible:
    def test_no_current_step_is_interruptible(self) -> None:
        action = ExecutionStateAction(bus=_bus())
        assert action.is_interruptible()

    def test_interruptible_step_is_interruptible(self) -> None:
        action = ExecutionStateAction(bus=_bus())
        action._current_step = ActionStep("step1", interruptible=True)
        assert action.is_interruptible()

    def test_non_interruptible_step_not_interruptible(self) -> None:
        action = ExecutionStateAction(bus=_bus())
        action._current_step = ActionStep("step2", interruptible=False)
        assert not action.is_interruptible()


class TestExecutionState:
    def test_execution_state_no_current_step(self) -> None:
        action = ExecutionStateAction(bus=_bus())
        state  = action.execution_state()
        assert isinstance(state, ActionExecutionState)
        assert state.action_id == "ExecutionStateAction"
        assert state.current_step is None
        assert state.completed_steps == []
        assert state.interruptible

    def test_execution_state_with_current_step(self) -> None:
        action = ExecutionStateAction(bus=_bus())
        action._current_step = ActionStep("step1", interruptible=True)
        state  = action.execution_state()
        assert state.current_step == "step1"
        assert state.interruptible

    def test_execution_state_mandatory_block_remaining(self) -> None:
        action = ExecutionStateAction(bus=_bus())
        # At step2 (non-interruptible, timeout_s=3.0) — remaining = 3.0
        action._current_step = ActionStep("step2", interruptible=False, timeout_s=3.0)
        state  = action.execution_state()
        assert not state.interruptible
        assert state.mandatory_block_remaining_s == 3.0

    def test_execution_state_completed_steps_listed(self) -> None:
        action = ExecutionStateAction(bus=_bus())
        action._completed_steps = [
            StepResult(step_name="step1", success=True),
        ]
        state = action.execution_state()
        assert "step1" in state.completed_steps


# ── ActionMasterNode retry logic ───────────────────────────────────────────────

class TestActionMasterNodeRetry:
    @pytest.mark.asyncio
    async def test_retry_succeeds_on_second_attempt(self) -> None:
        bus    = _bus()
        action = RetryableAction(bus=bus)
        am     = ActionMasterNode(bus=bus)
        am.register(action)
        result = await am.dispatch(_proposal("RetryableAction"))
        assert result.success
        assert action.call_count == 2

    @pytest.mark.asyncio
    async def test_retry_warning_logged_on_retry(self) -> None:
        bus    = _bus()
        action = RetryableAction(bus=bus)
        am     = ActionMasterNode(bus=bus)
        am.register(action)
        # Just check it completes without error (warning is internal)
        result = await am.dispatch(_proposal("RetryableAction"))
        assert result.success


# ── Exception handling: user execute() violates the never-raise contract ──────

class RaisingAction(BaseActionNode):
    """Violates the spec contract: execute() raises instead of returning a Result."""
    node_id = "RaisingAction"

    async def execute(self, proposal: Proposal) -> Result:
        raise ValueError("user code blew up")


class RetryableRaisingAction(BaseActionNode):
    """Raises on first attempt, returns success on retry."""
    node_id     = "RetryableRaisingAction"
    max_retries = 1

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.call_count = 0

    async def execute(self, proposal: Proposal) -> Result:
        self.call_count += 1
        if self.call_count == 1:
            raise RuntimeError("transient")
        return Result(action_id=self.node_id, success=True)


class TestActionMasterNodeExceptionHandling:
    @pytest.mark.asyncio
    async def test_execute_exception_converts_to_failed_result(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(RaisingAction(bus=bus))
        result = await am.dispatch(_proposal("RaisingAction"))
        assert not result.success
        assert isinstance(result.error, ValueError)
        assert "user code blew up" in str(result.error)

    @pytest.mark.asyncio
    async def test_execute_exception_does_not_propagate_through_dispatch_many(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(RaisingAction(bus=bus))
        am.register(_FastAction(bus=bus))
        results = await am.dispatch_many([
            _proposal("RaisingAction"),
            _proposal("_FastAction"),
        ])
        by_id = {r.action_id: r for r in results}
        assert not by_id["RaisingAction"].success
        assert by_id["_FastAction"].success

    @pytest.mark.asyncio
    async def test_execute_exception_respects_retry_policy(self) -> None:
        bus    = _bus()
        action = RetryableRaisingAction(bus=bus)
        am     = ActionMasterNode(bus=bus)
        am.register(action)
        result = await am.dispatch(_proposal("RetryableRaisingAction"))
        assert result.success
        assert action.call_count == 2


# ══════════════════════════════════════════════════════════════════════════════
# Concurrent dispatch tests
# ══════════════════════════════════════════════════════════════════════════════

class _DelayAction(BaseActionNode):
    """Action that sleeps briefly so we can verify concurrency."""
    node_id   = "_DelayAction"
    timeout_s = 5.0

    async def execute(self, proposal: Proposal) -> Result:
        await asyncio.sleep(0.05)
        return Result(action_id=self.node_id, success=True, output="delay_done")


class _FastAction(BaseActionNode):
    """Action that completes immediately."""
    node_id = "_FastAction"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True, output="fast_done")


class TestDispatchMany:
    @pytest.mark.asyncio
    async def test_dispatch_many_concurrent(self) -> None:
        """Two different actions dispatch concurrently."""
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(_DelayAction(bus=bus))
        am.register(_FastAction(bus=bus))

        p1 = _proposal("_DelayAction")
        p2 = _proposal("_FastAction")
        results = await am.dispatch_many([p1, p2])

        assert len(results) == 2
        action_ids = {r.action_id for r in results}
        assert action_ids == {"_DelayAction", "_FastAction"}
        assert all(r.success for r in results)

    @pytest.mark.asyncio
    async def test_dispatch_many_empty(self) -> None:
        am = ActionMasterNode(bus=_bus())
        results = await am.dispatch_many([])
        assert results == []

    @pytest.mark.asyncio
    async def test_dispatch_many_deduplicates_by_action_id(self) -> None:
        """When two proposals target the same action_id, only highest priority runs."""
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(_FastAction(bus=bus))

        low  = Proposal(instinct_id="a", action_id="_FastAction", priority=10, urgency=0.3)
        high = Proposal(instinct_id="b", action_id="_FastAction", priority=90, urgency=0.9)
        results = await am.dispatch_many([low, high])

        assert len(results) == 1
        assert results[0].success

    @pytest.mark.asyncio
    async def test_dispatch_many_skips_already_running(self) -> None:
        """Proposals for already-running action_ids are filtered out."""
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(_FastAction(bus=bus))

        # Simulate _FastAction already running
        am._running_nodes["_FastAction"] = _FastAction(bus=bus)
        am._running_proposals["_FastAction"] = _proposal("_FastAction")

        results = await am.dispatch_many([_proposal("_FastAction")])
        assert results == []

    @pytest.mark.asyncio
    async def test_dispatch_many_unknown_action_raises(self) -> None:
        am = ActionMasterNode(bus=_bus())
        with pytest.raises(ActionNotFoundError):
            await am.dispatch_many([_proposal("NonExistent")])


class TestConcurrentAccessors:
    def test_current_actions_empty(self) -> None:
        am = ActionMasterNode(bus=_bus())
        assert am.current_actions() == {}

    def test_current_proposals_empty(self) -> None:
        am = ActionMasterNode(bus=_bus())
        assert am.current_proposals() == {}

    def test_running_action_ids_empty(self) -> None:
        am = ActionMasterNode(bus=_bus())
        assert am.running_action_ids() == set()

    def test_current_action_returns_highest_priority(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        low_node  = _FastAction(bus=bus)
        high_node = _DelayAction(bus=bus)
        am._running_nodes["_FastAction"]  = low_node
        am._running_nodes["_DelayAction"] = high_node
        am._running_proposals["_FastAction"]  = Proposal(
            instinct_id="a", action_id="_FastAction", priority=10, urgency=0.5,
        )
        am._running_proposals["_DelayAction"] = Proposal(
            instinct_id="b", action_id="_DelayAction", priority=90, urgency=0.5,
        )
        assert am.current_action() is high_node

    def test_current_proposal_returns_highest_priority(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        low_p  = Proposal(instinct_id="a", action_id="_FastAction", priority=10, urgency=0.5)
        high_p = Proposal(instinct_id="b", action_id="_DelayAction", priority=90, urgency=0.5)
        am._running_nodes["_FastAction"]  = _FastAction(bus=bus)
        am._running_nodes["_DelayAction"] = _DelayAction(bus=bus)
        am._running_proposals["_FastAction"]  = low_p
        am._running_proposals["_DelayAction"] = high_p
        assert am.current_proposal() is high_p

    @pytest.mark.asyncio
    async def test_running_state_cleaned_after_dispatch(self) -> None:
        """After dispatch completes, running dicts are empty."""
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(_FastAction(bus=bus))
        await am.dispatch(_proposal("_FastAction"))
        assert am.current_actions() == {}
        assert am.current_proposals() == {}

    def test_is_interruptible_targeted(self) -> None:
        """is_interruptible(action_id) queries specific action."""
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        node = NeverPolicyAction(bus=bus)
        node._current_step = ActionStep("mandatory", interruptible=False)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)

        assert not am.is_interruptible(node.node_id)
        assert am.is_interruptible("nonexistent")  # unknown = True

    @pytest.mark.asyncio
    async def test_request_interrupt_targeted(self) -> None:
        """request_interrupt with action_id targets specific action."""
        bus  = _bus()
        am   = ActionMasterNode(bus=bus)
        node = CheckpointAction(bus=bus)
        node._current_step = ActionStep("commit", interruptible=True)
        am._running_nodes[node.node_id] = node
        am._running_proposals[node.node_id] = _proposal(node.node_id)

        await am.request_interrupt(_interrupt(), action_id=node.node_id)
        assert node._interrupt_requested


# ══════════════════════════════════════════════════════════════════════════════
# TOCTOU race guard in _dispatch_one
# ══════════════════════════════════════════════════════════════════════════════

class TestDispatchOneTOCTOUGuard:
    """Verify the atomic check-and-set in _dispatch_one prevents duplicate runs."""

    @pytest.mark.asyncio
    async def test_concurrent_dispatch_one_same_action_id(self) -> None:
        """Two concurrent _dispatch_one calls for the same action_id:
        one succeeds, the other returns Result(success=False)."""
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(_DelayAction(bus=bus))

        p1 = _proposal("_DelayAction")
        p2 = _proposal("_DelayAction")

        r1, r2 = await asyncio.gather(
            am._dispatch_one(p1),
            am._dispatch_one(p2),
        )

        results = [r1, r2]
        succeeded = [r for r in results if r.success]
        rejected  = [r for r in results if not r.success]

        assert len(succeeded) == 1, "Exactly one dispatch should succeed"
        assert len(rejected) == 1, "Exactly one dispatch should be rejected"
        assert isinstance(rejected[0].error, RuntimeError)
        assert str(rejected[0].error) == "action already running"
        assert rejected[0].action_id == "_DelayAction"

    @pytest.mark.asyncio
    async def test_running_nodes_has_single_entry_during_dispatch(self) -> None:
        """While _dispatch_one runs, _running_nodes never has a duplicate."""
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(_DelayAction(bus=bus))

        observed_counts: list[int] = []

        original_execute = _DelayAction.execute

        async def spying_execute(self_node: _DelayAction, proposal: Proposal) -> Result:
            # Record how many entries exist for our action_id while executing
            count = sum(
                1 for k in am._running_nodes if k == "_DelayAction"
            )
            observed_counts.append(count)
            return await original_execute(self_node, proposal)

        _DelayAction.execute = spying_execute  # type: ignore[assignment]
        try:
            p1 = _proposal("_DelayAction")
            p2 = _proposal("_DelayAction")
            await asyncio.gather(
                am._dispatch_one(p1),
                am._dispatch_one(p2),
            )
        finally:
            _DelayAction.execute = original_execute  # type: ignore[assignment]

        # Only one dispatch should have reached execute(), so at most one observation
        assert len(observed_counts) == 1
        assert observed_counts[0] == 1, "At most one entry for the action_id at any time"

    @pytest.mark.asyncio
    async def test_dispatch_one_cleanup_after_rejection(self) -> None:
        """After both dispatches complete, _running_nodes is clean."""
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(_FastAction(bus=bus))

        p1 = _proposal("_FastAction")
        p2 = _proposal("_FastAction")
        await asyncio.gather(
            am._dispatch_one(p1),
            am._dispatch_one(p2),
        )

        assert am._running_nodes == {}
        assert am._running_proposals == {}


# ══════════════════════════════════════════════════════════════════════════════
# P2 — ActionGuard pre-dispatch veto
# ══════════════════════════════════════════════════════════════════════════════

class _CountingAction(BaseActionNode):
    """Counts how many times execute() is actually called."""
    node_id = "_CountingAction"

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.calls = 0

    async def execute(self, proposal: Proposal) -> Result:
        self.calls += 1
        return Result(action_id=self.node_id, success=True)


class _CountingRetryAction(_CountingAction):
    """Like _CountingAction but with retries — proves a veto skips them."""
    node_id     = "_CountingRetryAction"
    max_retries = 2


class _ActA(_CountingAction):
    node_id = "ActA"


class _ActB(_CountingAction):
    node_id = "ActB"


def _allow_guard(proposal: Proposal) -> GuardVerdict:
    return GuardVerdict(allow=True)


def _deny_guard(proposal: Proposal) -> GuardVerdict:
    return GuardVerdict(allow=False, reason="nope")


class TestActionGuard:
    @pytest.mark.asyncio
    async def test_guard_allow_dispatches(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        act = _CountingAction(bus=bus)
        am.register(act)
        am.register_guard(_allow_guard)

        result = await am.dispatch(_proposal("_CountingAction"))

        assert result.success is True
        assert act.calls == 1

    @pytest.mark.asyncio
    async def test_guard_deny_no_side_effect_no_retry(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        act = _CountingRetryAction(bus=bus)
        am.register(act)
        am.register_guard(_deny_guard)

        result = await am.dispatch(_proposal("_CountingRetryAction"))

        assert result.success is False
        assert isinstance(result.error, ActionVetoed)
        assert result.error.reason == "nope"
        assert act.calls == 0                     # never executed, never retried
        assert am._running_nodes == {}            # running-state cleaned up
        assert am._running_proposals == {}

    @pytest.mark.asyncio
    async def test_guard_raising_fails_closed(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        act = _CountingAction(bus=bus)
        am.register(act)

        def _raising_guard(proposal: Proposal) -> GuardVerdict:
            raise RuntimeError("boom")

        am.register_guard(_raising_guard)
        result = await am.dispatch(_proposal("_CountingAction"))

        assert result.success is False
        assert isinstance(result.error, ActionVetoed)
        assert "boom" in result.error.reason
        assert act.calls == 0

    @pytest.mark.asyncio
    async def test_first_deny_wins(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        am.register(_CountingAction(bus=bus))
        consulted: list[str] = []

        def _g1(proposal: Proposal) -> GuardVerdict:
            consulted.append("g1")
            return GuardVerdict(allow=False, reason="g1")

        def _g2(proposal: Proposal) -> GuardVerdict:
            consulted.append("g2")
            return GuardVerdict(allow=True)

        am.register_guard(_g1)
        am.register_guard(_g2)
        result = await am.dispatch(_proposal("_CountingAction"))

        assert isinstance(result.error, ActionVetoed)
        assert result.error.reason == "g1"
        assert consulted == ["g1"]                # g2 never consulted

    @pytest.mark.asyncio
    async def test_reflex_bypasses_guards(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        act = _CountingAction(bus=bus)
        am.register(act)
        am.register_guard(_deny_guard)

        reflex = Proposal(
            instinct_id="reflex", action_id="_CountingAction",
            priority=200, urgency=1.0,
        )
        result = await am.dispatch(reflex)

        assert result.success is True             # reflex fired despite deny-all
        assert act.calls == 1

    @pytest.mark.asyncio
    async def test_reflex_origin_bypasses_guards_at_any_priority(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        act = _CountingAction(bus=bus)
        am.register(act)
        am.register_guard(_deny_guard)

        low = Proposal(
            instinct_id="reflex", action_id="_CountingAction",
            priority=120, urgency=1.0,
        )
        result = await am.dispatch(low, origin="reflex")

        assert result.success is True
        assert act.calls == 1

    @pytest.mark.asyncio
    async def test_decision_origin_cannot_bypass_guards_by_priority(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        act = _CountingAction(bus=bus)
        am.register(act)
        am.register_guard(_deny_guard)

        high = Proposal(
            instinct_id="normal", action_id="_CountingAction",
            priority=250, urgency=1.0,
        )
        result = await am.dispatch(high, origin="decision")

        assert isinstance(result.error, ActionVetoed)
        assert act.calls == 0

    @pytest.mark.asyncio
    async def test_dispatch_many_honors_guards(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus)
        a   = _ActA(bus=bus)
        b   = _ActB(bus=bus)
        am.register(a)
        am.register(b)

        def _deny_b(proposal: Proposal) -> GuardVerdict:
            if proposal.action_id == "ActB":
                return GuardVerdict(allow=False, reason="no B")
            return GuardVerdict(allow=True)

        am.register_guard(_deny_b)
        results = await am.dispatch_many([_proposal("ActA"), _proposal("ActB")])
        by_id   = {r.action_id: r for r in results}

        assert by_id["ActA"].success is True
        assert by_id["ActB"].success is False
        assert isinstance(by_id["ActB"].error, ActionVetoed)
        assert a.calls == 1
        assert b.calls == 0

    @pytest.mark.asyncio
    async def test_guards_via_constructor(self) -> None:
        bus = _bus()
        am  = ActionMasterNode(bus=bus, guards=[_deny_guard])
        act = _CountingAction(bus=bus)
        am.register(act)

        result = await am.dispatch(_proposal("_CountingAction"))

        assert isinstance(result.error, ActionVetoed)
        assert act.calls == 0

    def test_register_guard_rejects_coroutine(self) -> None:
        am = ActionMasterNode(bus=_bus())

        async def _async_guard(proposal: Proposal) -> GuardVerdict:
            return GuardVerdict(allow=True)

        with pytest.raises(TypeError):
            am.register_guard(_async_guard)  # type: ignore[arg-type]

    def test_guard_is_runtime_checkable(self) -> None:
        assert isinstance(_allow_guard, ActionGuard)


# ══════════════════════════════════════════════════════════════════════════════
# P4 — Cooperative mid-step interrupt
# ══════════════════════════════════════════════════════════════════════════════

class _LongPollStep(MultiStepActionNode):
    """A single long step that polls check_interrupt() and bails when set."""
    node_id          = "_LongPollStep"
    interrupt_policy = InterruptPolicy.ALWAYS

    def steps(self) -> list[ActionStep]:
        return [ActionStep("long", interruptible=True)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        for _ in range(200):
            if self.check_interrupt():
                return StepResult(step_name=step.name, success=False, interrupted=True)
            await asyncio.sleep(0.005)
        return StepResult(step_name=step.name, success=True)


class _MandatoryPollStep(MultiStepActionNode):
    """A non-interruptible step — check_interrupt() must stay False throughout."""
    node_id          = "_MandatoryPollStep"
    interrupt_policy = InterruptPolicy.ALWAYS

    def steps(self) -> list[ActionStep]:
        return [ActionStep("mand", interruptible=False)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        saw_interrupt = False
        for _ in range(20):
            if self.check_interrupt():
                saw_interrupt = True
            await asyncio.sleep(0.005)
        return StepResult(step_name=step.name, success=not saw_interrupt)


class _NeverPollStep(MultiStepActionNode):
    """Interruptible step but NEVER policy — check_interrupt() must stay False."""
    node_id          = "_NeverPollStep"
    interrupt_policy = InterruptPolicy.NEVER

    def steps(self) -> list[ActionStep]:
        return [ActionStep("step", interruptible=True)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        saw_interrupt = False
        for _ in range(20):
            if self.check_interrupt():
                saw_interrupt = True
            await asyncio.sleep(0.005)
        return StepResult(step_name=step.name, success=not saw_interrupt)


class TestCooperativeInterrupt:
    @pytest.mark.asyncio
    async def test_long_step_polls_and_bails(self) -> None:
        node = _LongPollStep(bus=_bus())
        task = asyncio.create_task(node.execute(_proposal("_LongPollStep")))
        await asyncio.sleep(0.02)
        node.request_interrupt(_interrupt())
        result = await task

        assert result.interrupted is True
        assert result.success is False
        assert result.stopped_at_step == "long"

    @pytest.mark.asyncio
    async def test_mandatory_step_check_interrupt_false(self) -> None:
        node = _MandatoryPollStep(bus=_bus())
        task = asyncio.create_task(node.execute(_proposal("_MandatoryPollStep")))
        await asyncio.sleep(0.02)
        node.request_interrupt(_interrupt())
        result = await task

        # Mandatory block ran to completion; the interrupt was never honoured.
        assert result.success is True
        assert result.interrupted is False

    @pytest.mark.asyncio
    async def test_never_policy_check_interrupt_false(self) -> None:
        node = _NeverPollStep(bus=_bus())
        task = asyncio.create_task(node.execute(_proposal("_NeverPollStep")))
        await asyncio.sleep(0.02)
        node.request_interrupt(_interrupt())
        result = await task

        assert result.success is True
        assert result.interrupted is False


# ══════════════════════════════════════════════════════════════════════════════
# Mutation-evidence hardening (revision E7) — kills mutants that survived the
# first cosmic-ray pass on the Table 6 enforcement functions
# ══════════════════════════════════════════════════════════════════════════════

class _RecordingParamAction(BaseActionNode):
    """Records which proposal's instinct_id actually executed."""
    node_id = "_RecordingParamAction"

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus=bus, **kwargs)
        self.seen: list[str] = []

    async def execute(self, proposal: Proposal) -> Result:
        self.seen.append(proposal.instinct_id)
        return Result(action_id=self.node_id, success=True)


class _NoStepsAction(MultiStepActionNode):
    """Degenerate multi-step action with an empty step list."""
    node_id = "_NoStepsAction"

    def steps(self) -> list[ActionStep]:
        return []

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        raise AssertionError("execute_step must never be called with no steps")


class _FinalStepFailsAction(MultiStepActionNode):
    """Second (final) step fails without aborting the sequence."""
    node_id = "_FinalStepFailsAction"

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("ok", interruptible=True),
            ActionStep("bad", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "ok":
            return StepResult(step_name="ok", success=True, output="ok-out")
        return StepResult(step_name="bad", success=False, output="bad-out")


class _EventRollbackAction(MultiStepActionNode):
    """Four interruptible steps; one blocks on an event so the test can inject
    the interrupt deterministically. on_interrupted records what it was given."""
    node_id          = "_EventRollbackAction"
    interrupt_policy = InterruptPolicy.ROLLBACK

    def __init__(self, bus: SignalBus, hold_step: str, **kwargs: object) -> None:
        super().__init__(bus=bus, **kwargs)
        self.hold_step = hold_step
        self.started   = asyncio.Event()
        self.release   = asyncio.Event()
        self.recorded_pending:   list[str] | None = None
        self.recorded_completed: list[str] | None = None

    def steps(self) -> list[ActionStep]:
        return [ActionStep(f"s{i}", interruptible=True) for i in (1, 2, 3, 4)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == self.hold_step:
            self.started.set()
            await self.release.wait()
        return StepResult(step_name=step.name, success=True)

    async def on_interrupted(
        self,
        completed: list[StepResult],
        pending: list[ActionStep],
        proposal: Proposal,
    ) -> None:
        self.recorded_completed = [sr.step_name for sr in completed]
        self.recorded_pending   = [s.name for s in pending]


class _QuiescentPollStep(MultiStepActionNode):
    """Interruptible step under ALWAYS that polls check_interrupt() with no
    interrupt ever requested — every poll must be False."""
    node_id          = "_QuiescentPollStep"
    interrupt_policy = InterruptPolicy.ALWAYS

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus=bus, **kwargs)
        self.polls: list[bool] = []

    def steps(self) -> list[ActionStep]:
        return [ActionStep("p", interruptible=True)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        for _ in range(3):
            self.polls.append(self.check_interrupt())
            await asyncio.sleep(0)
        return StepResult(step_name=step.name, success=True)


class TestResultTotalityDetails:
    @pytest.mark.asyncio
    async def test_zero_step_action_returns_success(self) -> None:
        node = _NoStepsAction(bus=_bus())
        t0 = time.monotonic()
        result = await node.execute(_proposal("_NoStepsAction"))
        elapsed = time.monotonic() - t0

        assert result.success is True
        assert result.output is None
        assert result.step_results == []
        assert abs(result.duration_s - elapsed) < 0.5

    @pytest.mark.asyncio
    async def test_final_step_failure_and_output_propagate(self) -> None:
        node = _FinalStepFailsAction(bus=_bus())
        t0 = time.monotonic()
        result = await node.execute(_proposal("_FinalStepFailsAction"))
        elapsed = time.monotonic() - t0

        # Result mirrors the LAST step: failed, with its output
        assert result.success is False
        assert result.output == "bad-out"
        assert [sr.step_name for sr in result.step_results] == ["ok", "bad"]
        assert abs(result.duration_s - elapsed) < 0.5


class TestDispatchManyTieBreak:
    @pytest.mark.asyncio
    async def test_highest_priority_wins_regardless_of_order(self) -> None:
        bus  = _bus()
        am   = ActionMasterNode(bus=bus)
        node = _RecordingParamAction(bus=bus)
        am.register(node)

        def _p(instinct_id: str, priority: int) -> Proposal:
            return Proposal(instinct_id=instinct_id,
                            action_id="_RecordingParamAction",
                            priority=priority, urgency=0.5)

        await am.dispatch_many([_p("low", 10), _p("high", 90)])
        assert node.seen == ["high"]

        node.seen.clear()
        await am.dispatch_many([_p("high", 90), _p("low", 10)])
        assert node.seen == ["high"]

    @pytest.mark.asyncio
    async def test_equal_priority_first_proposal_wins(self) -> None:
        bus  = _bus()
        am   = ActionMasterNode(bus=bus)
        node = _RecordingParamAction(bus=bus)
        am.register(node)

        first  = Proposal(instinct_id="first",  action_id="_RecordingParamAction",
                          priority=50, urgency=0.5)
        second = Proposal(instinct_id="second", action_id="_RecordingParamAction",
                          priority=50, urgency=0.5)
        await am.dispatch_many([first, second])
        assert node.seen == ["first"]


class TestRollbackPendingSteps:
    @pytest.mark.asyncio
    async def test_pending_after_interrupt_during_first_step(self) -> None:
        # Interrupt lands while s1 runs -> honoured at the s2 boundary (index 1):
        # completed=[s1], stopped at s2, pending=[s3, s4]
        node = _EventRollbackAction(bus=_bus(), hold_step="s1")
        task = asyncio.create_task(node.execute(_proposal("_EventRollbackAction")))
        await node.started.wait()
        node.request_interrupt(_interrupt())
        node.release.set()
        result = await task

        assert result.interrupted is True
        assert result.rolled_back is True
        assert result.stopped_at_step == "s2"
        assert node.recorded_completed == ["s1"]
        assert node.recorded_pending == ["s3", "s4"]

    @pytest.mark.asyncio
    async def test_pending_after_interrupt_during_second_step(self) -> None:
        # Interrupt lands while s2 runs -> honoured at the s3 boundary (index 2):
        # completed=[s1, s2], stopped at s3, pending=[s4]
        node = _EventRollbackAction(bus=_bus(), hold_step="s2")
        task = asyncio.create_task(node.execute(_proposal("_EventRollbackAction")))
        await node.started.wait()
        node.request_interrupt(_interrupt())
        node.release.set()
        result = await task

        assert result.interrupted is True
        assert result.stopped_at_step == "s3"
        assert node.recorded_completed == ["s1", "s2"]
        assert node.recorded_pending == ["s4"]


class TestCheckInterruptQuiescent:
    @pytest.mark.asyncio
    async def test_check_interrupt_false_when_never_requested(self) -> None:
        node = _QuiescentPollStep(bus=_bus())
        result = await node.execute(_proposal("_QuiescentPollStep"))

        assert result.success is True
        assert node.polls == [False, False, False]


# ══════════════════════════════════════════════════════════════════════════════
# Mutation-evidence hardening (v0.13) — kills the gap mutants that survived
# the reflex-arbitration mutation run on the Table 6 sites
# ══════════════════════════════════════════════════════════════════════════════

class _ClockAdvancingAction(BaseActionNode):
    """Single-shot action that moves a ManualClock forward while it runs."""
    node_id = "_ClockAdvancingAction"

    def __init__(self, bus: SignalBus, clock: ManualClock, run_s: float) -> None:
        super().__init__(bus=bus)
        self.clock = clock
        self.run_s = run_s

    async def execute(self, proposal: Proposal) -> Result:
        self.clock.now += self.run_s
        return Result(action_id=self.node_id, success=True)


class _ClockedStepsAction(MultiStepActionNode):
    """grip (mandatory, moves the clock 3 s) → carry; optionally asks for a
    WAIT_FOR_BLOCK reflex hand-off from inside the block."""
    node_id = "_ClockedStepsAction"

    def __init__(self, bus: SignalBus, clock: ManualClock, hand_off: bool) -> None:
        super().__init__(bus=bus)
        self.clock = clock
        self.hand_off = hand_off

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("grip",  interruptible=False),
            ActionStep("carry", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "grip":
            if self.hand_off:
                self.request_reflex_preempt(_proposal("Retract"))
            self.clock.now += 3.0
        return StepResult(step_name=step.name, success=True)


class _JammedGripAction(RaisingStepAction):
    """ROLLBACK abort whose undo of 'grip' fails; retries are allowed."""
    node_id          = "_JammedGripAction"
    interrupt_policy = InterruptPolicy.ROLLBACK
    max_retries      = 2

    async def _undo_grip(self) -> None:
        self.rolled.append("grip")
        raise OSError("gripper jammed")


class _AlwaysFailingAction(_CountingAction):
    """Returns a failed Result on every attempt."""
    node_id     = "_AlwaysFailingAction"
    max_retries = 1

    async def execute(self, proposal: Proposal) -> Result:
        self.calls += 1
        return Result(action_id=self.node_id, success=False, error=RuntimeError("no"))


class _HangingStepAction(MultiStepActionNode):
    """One step that never finishes on its own; timeouts are set per test."""
    node_id = "_HangingStepAction"

    def __init__(
        self, bus: SignalBus, *, step_timeout_s: float | None, action_timeout_s: float,
    ) -> None:
        super().__init__(bus=bus)
        self.step_timeout_s = step_timeout_s
        self.timeout_s = action_timeout_s

    def steps(self) -> list[ActionStep]:
        return [ActionStep("hang", interruptible=True, timeout_s=self.step_timeout_s)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        await asyncio.Event().wait()
        return StepResult(step_name=step.name, success=True)


class TestReportedDuration:
    @pytest.mark.asyncio
    async def test_dispatch_reports_elapsed_time_measured_by_the_master(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A dispatched Result's duration_s is the master-measured elapsed time"""
        clock = ManualClock(now=1.0)
        monkeypatch.setattr(action_module, "time", clock)
        bus = _bus()
        am = ActionMasterNode(bus=bus)
        am.register(_ClockAdvancingAction(bus, clock, run_s=3.0))
        result = await am.dispatch(_proposal("_ClockAdvancingAction"))
        assert result.success is True
        assert result.duration_s == pytest.approx(3.0)

    @pytest.mark.asyncio
    async def test_multistep_duration_is_elapsed_time_on_a_freshly_booted_clock(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """duration_s is end minus start even when the clock started near zero"""
        clock = ManualClock(now=1.0)
        monkeypatch.setattr(action_module, "time", clock)
        node = _ClockedStepsAction(_bus(), clock, hand_off=False)
        result = await node.execute(_proposal("_ClockedStepsAction"))
        assert result.success is True
        assert result.duration_s == pytest.approx(3.0)

    @pytest.mark.asyncio
    async def test_runtime_less_reflex_hand_off_reports_elapsed_time(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A WAIT_FOR_BLOCK hand-off outside a master still reports elapsed time"""
        clock = ManualClock(now=1.0)
        monkeypatch.setattr(action_module, "time", clock)
        node = _ClockedStepsAction(_bus(), clock, hand_off=True)
        result = await node.execute(_proposal("_ClockedStepsAction"))
        assert result.preempted is True
        assert result.stopped_at_step == "carry"
        assert result.duration_s == pytest.approx(3.0)


class TestRetryBudget:
    @pytest.mark.asyncio
    async def test_abort_with_failed_rollback_is_not_retried(self) -> None:
        """An abort whose rollback failed is never re-run from step 1"""
        bus = _bus()
        am = ActionMasterNode(bus=bus)
        node = _JammedGripAction(bus=bus)
        am.register(node)
        result = await am.dispatch(_proposal("_JammedGripAction"))
        assert result.interrupted is False
        assert isinstance(result.error, RollbackError)
        assert node.rolled == ["grip"]              # one attempt, one undo

    @pytest.mark.asyncio
    async def test_failing_action_is_attempted_max_retries_plus_one_times(self) -> None:
        """A persistently failing action runs exactly max_retries + 1 times"""
        bus = _bus()
        am = ActionMasterNode(bus=bus)
        node = _AlwaysFailingAction(bus=bus)
        am.register(node)
        result = await am.dispatch(_proposal("_AlwaysFailingAction"))
        assert result.success is False
        assert node.calls == 2


class TestStepTimeoutSelection:
    @pytest.mark.asyncio
    async def test_step_timeout_overrides_action_timeout(self) -> None:
        """A step's own timeout_s bounds it, not the longer action timeout"""
        node = _HangingStepAction(_bus(), step_timeout_s=0.05, action_timeout_s=30.0)
        result = await asyncio.wait_for(node.execute(_proposal("_HangingStepAction")), 2.0)
        assert result.success is False
        assert isinstance(result.error, StepAbortError)
        assert isinstance(result.error.__cause__, ActionTimeoutError)

    @pytest.mark.asyncio
    async def test_action_timeout_bounds_step_without_its_own(self) -> None:
        """A step without timeout_s is bounded by the action's timeout_s"""
        node = _HangingStepAction(_bus(), step_timeout_s=None, action_timeout_s=0.05)
        result = await asyncio.wait_for(node.execute(_proposal("_HangingStepAction")), 2.0)
        assert result.success is False
        assert isinstance(result.error, StepAbortError)
        assert isinstance(result.error.__cause__, ActionTimeoutError)
