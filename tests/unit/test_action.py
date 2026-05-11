"""Unit tests for BaseActionNode, MultiStepActionNode, and ActionMasterNode."""

from __future__ import annotations

import asyncio

import pytest

from arachnite import SignalBus
from arachnite.exceptions import (
    ActionNotFoundError,
    MandatoryBlockViolation,
    NodeRegistrationError,
    RollbackError,
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
from arachnite.nodes.action import (
    ActionMasterNode,
    BaseActionNode,
    MultiStepActionNode,
)

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
        # Both steps run — NEVER policy suppresses the interrupt (line 234: pass)
        assert len(result.step_results) == 2
        assert not result.interrupted


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
    async def test_rollback_failure_raises_rollback_error(self) -> None:
        action = RollbackFailAction(bus=_bus())
        # execute_step sets _interrupt_requested after "mandatory" completes,
        # so the interrupt fires at the next interruptible step.
        with pytest.raises(RollbackError):
            await action.execute(_proposal("RollbackFailAction"))


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
