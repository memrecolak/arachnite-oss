"""
arachnite.nodes.action
~~~~~~~~~~~~~~~~~~~~~~
BaseActionNode, MultiStepActionNode, and ActionMasterNode.
Spec reference: Sections 5.7, 17.
"""

from __future__ import annotations

import asyncio
import time
from abc import abstractmethod
from collections.abc import Sequence

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.exceptions import (
    ActionNotFoundError,
    ActionTimeoutError,
    MandatoryBlockViolation,
    NodeRegistrationError,
    RollbackError,
    StepAbortError,
)
from arachnite.logging import BaseLogSink
from arachnite.models import (
    ActionExecutionState,
    ActionStep,
    InterruptPolicy,
    InterruptRequest,
    Proposal,
    Result,
    StepResult,
)
from arachnite.nodes.base import BaseNode

# ══════════════════════════════════════════════════════════════════════════════
# BaseActionNode
# ══════════════════════════════════════════════════════════════════════════════

class BaseActionNode(BaseNode):
    """
    Knows how to perform a single concrete operation.

    Developer contract:
    - Extend this class and implement execute().
    - Always return a Result — never raise unless unrecoverable.
    - Set node_id to the value InstinctNodes use in Proposal.action_id.
    - Set timeout_s and max_retries to protect against hardware hangs.

    Spec reference: Section 5.7.
    """

    timeout_s:   float = 5.0
    max_retries: int   = 0

    @abstractmethod
    async def execute(self, proposal: Proposal) -> Result:
        """
        Carry out the action described by the proposal.
        Read parameters from proposal.parameters.
        Always return a Result — never raise unless truly unrecoverable.
        """

    async def on_timeout(self, proposal: Proposal) -> Result:
        """
        Called when execute() exceeds timeout_s.
        Default: return a failed Result. Override for custom cleanup.
        """
        return Result(
            action_id = self.node_id,
            success   = False,
            error     = ActionTimeoutError(self.node_id, None, self.timeout_s),
        )


# ══════════════════════════════════════════════════════════════════════════════
# MultiStepActionNode
# ══════════════════════════════════════════════════════════════════════════════

class MultiStepActionNode(BaseActionNode):
    """
    An ActionNode that decomposes behaviour into an ordered sequence of
    ActionSteps with explicit interrupt and rollback semantics.

    Developer contract:
    - Implement steps() returning the ordered ActionStep list.
    - Implement execute_step() with a match/case on step.name.
    - Set interrupt_policy to match safety requirements.
    - Provide rollback callables on non-interruptible steps wherever possible.

    Spec reference: Section 17.4.
    """

    interrupt_policy: InterruptPolicy = InterruptPolicy.ALWAYS

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._interrupt_requested: bool = False
        self._interrupt_request: InterruptRequest | None = None
        self._current_step: ActionStep | None = None
        self._completed_steps: list[StepResult] = []

    @abstractmethod
    def steps(self) -> list[ActionStep]:
        """
        Return the ordered list of ActionSteps for this action.
        Called once at the start of execute(). Steps must not be mutated.
        """

    @abstractmethod
    async def execute_step(
        self,
        step: ActionStep,
        proposal: Proposal,
        completed: list[StepResult],
    ) -> StepResult:
        """
        Execute one step. Receives all already-completed step results so
        later steps can branch on earlier outcomes.
        Must return a StepResult — never raise.
        """

    async def on_interrupted(
        self,
        completed: list[StepResult],
        pending: list[ActionStep],
        proposal: Proposal,
    ) -> None:
        """
        Called after execution stops due to an interrupt request.
        Default: call rollback() on completed non-interruptible steps
        in reverse order.
        """
        non_interruptible_done = [
            sr for sr in reversed(completed)
            if any(
                s.name == sr.step_name and not s.interruptible
                for s in self.steps()
            )
        ]
        steps_by_name = {s.name: s for s in self.steps()}
        for step_result in non_interruptible_done:
            step = steps_by_name.get(step_result.step_name)
            if step and step.rollback:
                try:
                    await step.rollback()
                except Exception as exc:  # noqa: BLE001
                    raise RollbackError(self.node_id, step.name, exc) from exc

    async def on_step_timeout(
        self,
        step: ActionStep,
        proposal: Proposal,
    ) -> StepResult:
        """
        Called when a step exceeds its timeout.
        Default: abort the sequence.
        """
        return StepResult(
            step_name      = step.name,
            success        = False,
            error          = ActionTimeoutError(self.node_id, step.name,
                                                step.timeout_s or self.timeout_s),
            abort_sequence = True,
        )

    def request_interrupt(self, request: InterruptRequest) -> None:
        """Called by ActionMasterNode to signal that a higher-priority action wants control."""
        self._interrupt_requested = True
        self._interrupt_request   = request

    def is_interruptible(self) -> bool:
        """True if execution can be stopped right now (not inside a mandatory block)."""
        if self._current_step is None:
            return True
        return self._current_step.interruptible

    def execution_state(self) -> ActionExecutionState:
        """Return a snapshot of current execution progress for the Context."""
        completed_names = [sr.step_name for sr in self._completed_steps]
        # Compute mandatory block remaining time
        all_steps    = self.steps()
        current_idx  = next(
            (i for i, s in enumerate(all_steps)
             if self._current_step and s.name == self._current_step.name),
            len(all_steps),
        )
        remaining_mandatory = sum(
            (s.timeout_s or self.timeout_s)
            for s in all_steps[current_idx:]
            if not s.interruptible
        )
        return ActionExecutionState(
            action_id                   = self.node_id,
            current_step                = self._current_step.name if self._current_step else None,
            completed_steps             = completed_names,
            interruptible               = self.is_interruptible(),
            mandatory_block_remaining_s = remaining_mandatory,
        )

    async def execute(self, proposal: Proposal) -> Result:
        """Orchestrates step execution with interrupt and rollback handling."""
        self._interrupt_requested = False
        self._interrupt_request   = None
        self._completed_steps     = []

        all_steps  = self.steps()
        start_time = time.monotonic()

        for i, step in enumerate(all_steps):
            self._current_step = step
            pending = all_steps[i + 1:]

            # Check interrupt request before starting this step
            if self._interrupt_requested and step.interruptible:
                should_interrupt = (
                    self.interrupt_policy in (
                        InterruptPolicy.ALWAYS, InterruptPolicy.ROLLBACK
                    )
                    or (
                        self.interrupt_policy == InterruptPolicy.CHECKPOINT
                        and step.checkpoint
                    )
                )
                if should_interrupt:
                    return await self._handle_interrupt(
                        proposal, pending, start_time
                    )

            # Enforce mandatory block — cannot interrupt non-interruptible steps
            if self._interrupt_requested and not step.interruptible:
                if self.interrupt_policy == InterruptPolicy.NEVER:
                    pass  # ignore interrupt for NEVER policy
                # All other policies: hold the interrupt, complete the block
                self.logger.warning(
                    "Interrupt held — inside mandatory block",
                    step=step.name,
                    policy=self.interrupt_policy.value,
                )

            # Execute step with per-step timeout
            step_timeout = step.timeout_s or self.timeout_s
            step_result  = await self._run_step_timed(step, proposal, step_timeout)
            self._completed_steps.append(step_result)

            # Step requested abort
            if step_result.abort_sequence:
                self._current_step = None
                return Result(
                    action_id   = self.node_id,
                    success     = False,
                    error       = StepAbortError(self.node_id, step.name),
                    duration_s  = time.monotonic() - start_time,
                    step_results = self._completed_steps.copy(),
                )

            # Step failed but didn't request abort — log and continue
            if not step_result.success:
                self.logger.warning(
                    "Step failed",
                    step=step.name,
                    error=str(step_result.error),
                )

        # All steps completed
        self._current_step = None
        last = self._completed_steps[-1] if self._completed_steps else None
        return Result(
            action_id    = self.node_id,
            success      = last.success if last else True,
            output       = last.output if last else None,
            duration_s   = time.monotonic() - start_time,
            step_results = self._completed_steps.copy(),
        )

    async def _run_step_timed(
        self,
        step: ActionStep,
        proposal: Proposal,
        timeout: float,
    ) -> StepResult:
        step_start = time.monotonic()
        self.logger.debug("Step starting", step=step.name)
        try:
            result = await asyncio.wait_for(
                self.execute_step(step, proposal, list(self._completed_steps)),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            # See note in _execute_with_retry — required for Python 3.10 support.
            result = await self.on_step_timeout(step, proposal)
        result = StepResult(
            step_name  = result.step_name,
            success    = result.success,
            output     = result.output,
            error      = result.error,
            duration_s = time.monotonic() - step_start,
            abort_sequence = result.abort_sequence,
        )
        self.logger.debug(
            "Step complete",
            step=step.name,
            success=result.success,
            duration_ms=round(result.duration_s * 1000, 2),
        )
        return result

    async def _handle_interrupt(
        self,
        proposal: Proposal,
        pending: list[ActionStep],
        start_time: float,
    ) -> Result:
        self.logger.info(
            "Action interrupted",
            stopped_at=self._current_step.name if self._current_step else "unknown",
            policy=self.interrupt_policy.value,
        )
        stopped_at = self._current_step.name if self._current_step else None
        rolled_back = False

        if self.interrupt_policy == InterruptPolicy.ROLLBACK:
            await self.on_interrupted(self._completed_steps.copy(), pending, proposal)
            rolled_back = True

        self._current_step = None
        return Result(
            action_id       = self.node_id,
            success         = False,
            duration_s      = time.monotonic() - start_time,
            interrupted     = True,
            stopped_at_step = stopped_at,
            step_results    = self._completed_steps.copy(),
            rolled_back     = rolled_back,
        )


# ══════════════════════════════════════════════════════════════════════════════
# ActionMasterNode
# ══════════════════════════════════════════════════════════════════════════════

class ActionMasterNode(BaseNode):
    """
    Routes Proposals to the correct ActionNode, enforces timeouts and
    retries, and manages interrupt requests for running actions.

    Supports concurrent dispatch: different ActionNodes can execute in
    parallel via ``dispatch_many()``.  The same ActionNode cannot run
    twice concurrently — duplicate ``action_id`` proposals are filtered.

    Spec reference: Section 5.7, 17.6.
    """

    node_id = "ActionMasterNode"

    def __init__(
        self,
        bus: SignalBus,
        config: NodeConfig | None = None,
        log_sinks: list[BaseLogSink] | None = None,
        agent_node_id: str = "local",
    ) -> None:
        super().__init__(bus, config, log_sinks, agent_node_id)
        self._nodes: dict[str, BaseActionNode] = {}
        self._running_nodes: dict[str, BaseActionNode] = {}
        self._running_proposals: dict[str, Proposal] = {}

    def register(self, node: BaseActionNode) -> None:
        if node.node_id in self._nodes:
            raise NodeRegistrationError(node.node_id, self.node_id)
        self._nodes[node.node_id] = node
        self.logger.debug("Registered action node", action_node_id=node.node_id)

    def get_node(self, node_id: str) -> BaseActionNode | None:
        """Return a registered action node by ID, or None"""
        return self._nodes.get(node_id)

    def unregister(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)

    @property
    def nodes(self) -> Sequence[BaseActionNode]:
        return list(self._nodes.values())

    # ── Plural accessors (concurrent) ────────────────────────────────────

    def current_actions(self) -> dict[str, BaseActionNode]:
        """Return all currently running action nodes keyed by action_id."""
        return dict(self._running_nodes)

    def current_proposals(self) -> dict[str, Proposal]:
        """Return all currently running proposals keyed by action_id."""
        return dict(self._running_proposals)

    def running_action_ids(self) -> set[str]:
        """Return the set of action_ids currently executing."""
        return set(self._running_nodes)

    # ── Backward-compat singular accessors ───────────────────────────────

    def current_action(self) -> BaseActionNode | None:
        """Return the highest-priority running action, or None."""
        if not self._running_proposals:
            return None
        best_id = max(self._running_proposals, key=lambda k: self._running_proposals[k].priority)
        return self._running_nodes.get(best_id)

    def current_proposal(self) -> Proposal | None:
        """Return the highest-priority running proposal, or None."""
        if not self._running_proposals:
            return None
        return max(self._running_proposals.values(), key=lambda p: p.priority)

    # ── Interrupt queries ────────────────────────────────────────────────

    def is_interruptible(self, action_id: str | None = None) -> bool:
        """
        True if the specified (or all) running action(s) can be stopped.
        With no ``action_id``, returns True only if ALL running actions are
        interruptible (or nothing is running).
        """
        if action_id is not None:
            node = self._running_nodes.get(action_id)
            if node is None:
                return True
            if isinstance(node, MultiStepActionNode):
                return node.is_interruptible()
            return True
        # Check all running actions
        for node in self._running_nodes.values():
            if isinstance(node, MultiStepActionNode) and not node.is_interruptible():
                return False
        return True

    def current_step(self, action_id: str | None = None) -> ActionStep | None:
        """Return the current step of the specified (or highest-priority) action."""
        if action_id is not None:
            node = self._running_nodes.get(action_id)
        else:
            node = self.current_action()
        if isinstance(node, MultiStepActionNode):
            return node._current_step
        return None

    async def request_interrupt(
        self,
        request: InterruptRequest,
        action_id: str | None = None,
    ) -> None:
        """
        Signal a running action to stop at its next safe point.
        If ``action_id`` is None, targets the highest-priority running action.
        """
        if action_id is not None:
            node = self._running_nodes.get(action_id)
        else:
            node = self.current_action()
        if node is None:
            return
        if not self.is_interruptible(node.node_id):
            step = self.current_step(node.node_id)
            raise MandatoryBlockViolation(
                node.node_id,
                step.name if step is not None else "unknown",
            )
        if isinstance(node, MultiStepActionNode):
            node.request_interrupt(request)

    async def setup(self) -> None:
        await asyncio.gather(*(n.setup() for n in self._nodes.values()))

    async def teardown(self) -> None:
        await asyncio.gather(*(n.cancel_background_tasks() for n in self._nodes.values()))
        await asyncio.gather(*(n.teardown() for n in self._nodes.values()))

    async def on_pause(self) -> None:
        await asyncio.gather(*(n.on_pause() for n in self._nodes.values()))

    async def on_resume(self) -> None:
        await asyncio.gather(*(n.on_resume() for n in self._nodes.values()))

    async def notify_tick_start(self, tick: int) -> None:
        await asyncio.gather(*(n.on_tick_start(tick) for n in self._nodes.values()))

    async def notify_tick_end(self, tick: int, duration_s: float) -> None:
        await asyncio.gather(*(n.on_tick_end(tick, duration_s) for n in self._nodes.values()))

    # ── Single dispatch (backward compat) ────────────────────────────────

    async def dispatch(self, proposal: Proposal) -> Result:
        """
        Route a single Proposal to its ActionNode by matching
        ``proposal.action_id``.  For concurrent dispatch of multiple
        proposals, use ``dispatch_many()``.
        """
        return await self._dispatch_one(proposal)

    # ── Concurrent dispatch ──────────────────────────────────────────────

    async def dispatch_many(self, proposals: list[Proposal]) -> list[Result]:
        """
        Dispatch multiple proposals concurrently via ``asyncio.gather()``.

        Filters out proposals whose ``action_id`` is already running or
        appears more than once (keeps highest priority per ``action_id``).
        """
        if not proposals:
            return []

        # Deduplicate: keep highest-priority proposal per action_id
        best: dict[str, Proposal] = {}
        for p in proposals:
            existing = best.get(p.action_id)
            if existing is None or p.priority > existing.priority:
                best[p.action_id] = p

        # Filter out already-running action_ids
        to_dispatch = [
            p for p in best.values()
            if p.action_id not in self._running_nodes
        ]

        if not to_dispatch:
            return []

        results = await asyncio.gather(
            *(self._dispatch_one(p) for p in to_dispatch)
        )
        return list(results)

    async def _dispatch_one(self, proposal: Proposal) -> Result:
        """Execute a single proposal with retry logic and running-state tracking."""
        node = self._nodes.get(proposal.action_id)
        if node is None:
            raise ActionNotFoundError(proposal.action_id)

        # Atomic check-and-set: skip if already running
        if proposal.action_id in self._running_nodes:
            self.logger.debug(
                "Action already running, skipping",
                action_id=proposal.action_id,
            )
            return Result(
                action_id=proposal.action_id,
                success=False,
                error=RuntimeError("action already running"),
            )
        self._running_nodes[proposal.action_id] = node
        self._running_proposals[proposal.action_id] = proposal
        start = time.monotonic()

        self.logger.info(
            "Dispatching action",
            action_id=proposal.action_id,
            priority=proposal.priority,
        )

        try:
            result = await self._execute_with_retry(node, proposal)
            result = Result(
                action_id       = result.action_id,
                success         = result.success,
                output          = result.output,
                error           = result.error,
                duration_s      = time.monotonic() - start,
                interrupted     = result.interrupted,
                stopped_at_step = result.stopped_at_step,
                step_results    = result.step_results,
                rolled_back     = result.rolled_back,
            )
        finally:
            self._running_nodes.pop(proposal.action_id, None)
            self._running_proposals.pop(proposal.action_id, None)

        self.logger.info(
            "Action complete",
            action_id=proposal.action_id,
            success=result.success,
            duration_ms=round(result.duration_s * 1000, 2),
        )
        return result

    async def _execute_with_retry(
        self, node: BaseActionNode, proposal: Proposal
    ) -> Result:
        last_result: Result | None = None
        attempts = node.max_retries + 1

        for attempt in range(attempts):
            if attempt > 0:
                self.logger.warning(
                    "Retrying action",
                    action_id=node.node_id,
                    attempt=attempt + 1,
                    max_attempts=attempts,
                )
            try:
                result = await asyncio.wait_for(
                    node.execute(proposal),
                    timeout=node.timeout_s,
                )
            except asyncio.TimeoutError:
                # Note: ``asyncio.TimeoutError`` is aliased to the builtin
                # ``TimeoutError`` only on Python >= 3.11. On 3.10 the two are
                # distinct classes, so catch the asyncio one explicitly.
                result = await node.on_timeout(proposal)
            except Exception as exc:  # noqa: BLE001
                # Spec contract: execute() must always return a Result, never
                # raise. If a user node violates that contract,
                # log loudly and convert to a failed Result rather than letting
                # the exception escape gather() and crash the tick. Mirrors
                # InstinctMasterNode.evaluate_all's defensive pattern.
                self.logger.error(
                    "Action execute() raised",
                    action_id=node.node_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                result = Result(
                    action_id=node.node_id,
                    success=False,
                    error=exc,
                )
            last_result = result
            if result.success:
                break

        return last_result  # type: ignore[return-value]
