"""
arachnite.nodes.action
~~~~~~~~~~~~~~~~~~~~~~
BaseActionNode, MultiStepActionNode, and ActionMasterNode.
Spec reference: Sections 5.7, 17.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.exceptions import (
    ActionNotFoundError,
    ActionTimeoutError,
    ActionVetoed,
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
from arachnite.nodes.base import BaseNode, FaultReportingMixin

# Proposals at or above this priority are reflex/safety actions. They bypass
# ActionGuards to preserve the framework's "reflexes always fire" guarantee
# (see Proposal.priority: "200+ reflex, 100-199 safety").
REFLEX_PRIORITY_FLOOR = 200


@dataclass(frozen=True, slots=True)
class GuardVerdict:
    """The outcome of an ActionGuard check: allow, or deny with a reason."""

    allow:  bool
    reason: str = ""


@runtime_checkable
class ActionGuard(Protocol):
    """
    A synchronous, bounded, no-I/O pre-dispatch veto.

    Consulted by ActionMasterNode before an action executes. Return
    ``GuardVerdict(allow=False, reason=...)`` to deny a concrete proposal
    based on its parameters (e.g. an out-of-scope path, a destructive
    command). Guards must be deterministic and fast — they run on the
    dispatch path, not in a background task.
    """

    def __call__(self, proposal: Proposal) -> GuardVerdict: ...


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
        in reverse order (see ``_rollback_completed``).
        """
        await self._rollback_completed(completed)

    async def on_aborted(
        self,
        completed: list[StepResult],
        failed_step: ActionStep,
        proposal: Proposal,
    ) -> bool:
        """
        Called when a step aborts the sequence — it returned
        ``abort_sequence=True``, exceeded its timeout, or raised.
        ``completed`` holds the steps that ran before ``failed_step``.

        Default: under ``InterruptPolicy.ROLLBACK`` undo the completed
        non-interruptible steps in reverse order and return True; under every
        other policy do nothing and return False. Return whether a rollback
        ran. May raise ``RollbackError``.
        """
        if self.interrupt_policy != InterruptPolicy.ROLLBACK:
            return False
        await self._rollback_completed(completed)
        return True

    async def _rollback_completed(self, completed: list[StepResult]) -> None:
        """
        Run the rollback callable of every completed non-interruptible step,
        most recent first. Every rollback is attempted even if an earlier one
        raises; the failures are then raised together as one ``RollbackError``
        so no partial walk goes unreported.
        """
        all_steps = self.steps()
        non_interruptible = {s.name for s in all_steps if not s.interruptible}
        steps_by_name = {s.name: s for s in all_steps}
        failures: list[tuple[str, BaseException]] = []
        for step_result in reversed(completed):
            if step_result.step_name not in non_interruptible:
                continue
            step = steps_by_name[step_result.step_name]
            if step.rollback is None:
                continue
            try:
                await step.rollback()
            except Exception as exc:  # noqa: BLE001
                self.logger.error(
                    "Rollback raised",
                    step=step.name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                failures.append((step.name, exc))
        if failures:
            first_step, first_exc = failures[0]
            raise RollbackError(
                self.node_id, first_step, first_exc, failures=failures,
            ) from first_exc

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

    def check_interrupt(self) -> bool:
        """
        Poll from inside a long-running ``execute_step`` to cooperatively bail.

        Returns True only when an interrupt has been requested AND the current
        step may honour it. Returns False inside a non-interruptible (mandatory
        completion) step or under ``InterruptPolicy.NEVER`` — it can never
        bypass a mandatory block (see ``MandatoryBlockMonitor``). Under
        ``CHECKPOINT`` it is True only at a checkpoint step. A step that sees
        True should stop and return ``StepResult(..., interrupted=True)``.
        """
        if not self._interrupt_requested:
            return False
        if self._current_step is None or not self._current_step.interruptible:
            return False
        if self.interrupt_policy == InterruptPolicy.NEVER:
            return False
        if self.interrupt_policy == InterruptPolicy.CHECKPOINT:
            return self._current_step.checkpoint
        return True

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

            # Mandatory block — a pending interrupt is held until the block
            # completes (and ignored altogether under NEVER).
            if self._interrupt_requested and not step.interruptible:
                self.logger.warning(
                    "Interrupt held — inside mandatory block",
                    step=step.name,
                    policy=self.interrupt_policy.value,
                )

            # Execute step with per-step timeout
            step_timeout = step.timeout_s or self.timeout_s
            step_result  = await self._run_step_timed(step, proposal, step_timeout)
            self._completed_steps.append(step_result)

            # Step cooperatively bailed mid-execution on an interrupt — surface
            # it like the between-steps interrupt path (Result.interrupted), not
            # as an abort/failure. Defense-in-depth: never honour it inside a
            # mandatory block (check_interrupt() already returns False there),
            # so a contract-violating step cannot bypass MandatoryBlockMonitor.
            if step_result.interrupted:
                if step.interruptible:
                    return await self._handle_interrupt(proposal, pending, start_time)
                self.logger.warning(
                    "Ignoring interrupted=True from non-interruptible step",
                    step=step.name,
                )

            # Step requested abort (abort_sequence, step timeout, or raised)
            if step_result.abort_sequence:
                return await self._handle_abort(proposal, step, start_time)

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
        except Exception as exc:  # noqa: BLE001
            # execute_step() must never raise; a step that does is treated as
            # an aborting failure so the sequence goes through on_aborted()
            # (and its rollback) instead of escaping execute() half-done.
            self.logger.error(
                "Step raised",
                step=step.name,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            result = StepResult(
                step_name      = step.name,
                success        = False,
                error          = exc,
                abort_sequence = True,
            )
        result = StepResult(
            step_name  = result.step_name,
            success    = result.success,
            output     = result.output,
            error      = result.error,
            duration_s = time.monotonic() - step_start,
            abort_sequence = result.abort_sequence,
            interrupted    = result.interrupted,
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
        error: BaseException | None = None

        if self.interrupt_policy == InterruptPolicy.ROLLBACK:
            try:
                await self.on_interrupted(self._completed_steps.copy(), pending, proposal)
                rolled_back = True
            except RollbackError as exc:
                error = exc

        self._current_step = None
        return Result(
            action_id       = self.node_id,
            success         = False,
            error           = error,
            duration_s      = time.monotonic() - start_time,
            interrupted     = True,
            stopped_at_step = stopped_at,
            step_results    = self._completed_steps.copy(),
            rolled_back     = rolled_back,
        )

    async def _handle_abort(
        self,
        proposal: Proposal,
        step: ActionStep,
        start_time: float,
    ) -> Result:
        """Finish an aborted sequence: run on_aborted() and report the outcome."""
        failed = self._completed_steps[-1]
        error: BaseException = StepAbortError(self.node_id, step.name)
        if failed.error is not None:
            error.__cause__ = failed.error
        rolled_back = False
        try:
            rolled_back = await self.on_aborted(
                self._completed_steps[:-1], step, proposal,
            )
        except RollbackError as exc:
            error = exc

        self._current_step = None
        return Result(
            action_id       = self.node_id,
            success         = False,
            error           = error,
            duration_s      = time.monotonic() - start_time,
            stopped_at_step = step.name,
            step_results    = self._completed_steps.copy(),
            rolled_back     = rolled_back,
        )


# ══════════════════════════════════════════════════════════════════════════════
# ActionMasterNode
# ══════════════════════════════════════════════════════════════════════════════

class ActionMasterNode(FaultReportingMixin, BaseNode):
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
        guards: list[ActionGuard] | None = None,
    ) -> None:
        super().__init__(bus, config, log_sinks, agent_node_id)
        self._nodes: dict[str, BaseActionNode] = {}
        self._running_nodes: dict[str, BaseActionNode] = {}
        self._running_proposals: dict[str, Proposal] = {}
        self._guards: list[ActionGuard] = []
        for guard in guards or ():
            self.register_guard(guard)

    def register(self, node: BaseActionNode) -> None:
        if node.node_id in self._nodes:
            raise NodeRegistrationError(node.node_id, self.node_id)
        self._nodes[node.node_id] = node
        self.logger.debug("Registered action node", action_node_id=node.node_id)

    def register_guard(self, guard: ActionGuard) -> None:
        """
        Install a synchronous, bounded, no-I/O pre-dispatch veto.

        Guards run in registration order inside ``_dispatch_one``, BEFORE the
        action executes and OUTSIDE the retry loop. The first deny wins
        (remaining guards are skipped) and the action's ``execute()`` is never
        called. A guard that raises FAILS CLOSED (treated as a deny) — unlike
        ``InstinctMasterNode.set_pre_evaluate_gate``, which fails open, because
        this is a safety layer.

        Reflex/safety proposals (``priority >= REFLEX_PRIORITY_FLOOR``) bypass
        all guards to preserve the framework's reflex-always-fires guarantee.
        """
        if inspect.iscoroutinefunction(guard):
            raise TypeError(
                "ActionGuard must be synchronous; got coroutine function "
                f"{getattr(guard, '__qualname__', guard)!r}"
            )
        self._guards.append(guard)

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

    async def dispatch(
        self, proposal: Proposal, *, origin: str | None = None,
    ) -> Result:
        """
        Route a single Proposal to its ActionNode by matching
        ``proposal.action_id``.  For concurrent dispatch of multiple
        proposals, use ``dispatch_many()``.

        ``origin`` names the dispatch path: ``"reflex"`` for the reflex arc,
        ``"decision"`` for the deliberative path. Guards are bypassed only for
        ``origin="reflex"``; with ``origin=None`` (runtime-less callers) the
        legacy rule applies — ``priority >= REFLEX_PRIORITY_FLOOR`` bypasses.
        """
        return await self._dispatch_one(proposal, origin=origin)

    # ── Concurrent dispatch ──────────────────────────────────────────────

    async def dispatch_many(
        self, proposals: list[Proposal], *, origin: str | None = None,
    ) -> list[Result]:
        """
        Dispatch multiple proposals concurrently via ``asyncio.gather()``.

        Filters out proposals whose ``action_id`` is already running or
        appears more than once (keeps highest priority per ``action_id``).
        ``origin`` is forwarded to the guard check (see ``dispatch()``).
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
            *(self._dispatch_one(p, origin=origin) for p in to_dispatch)
        )
        return list(results)

    def _bypasses_guards(self, proposal: Proposal, origin: str | None) -> bool:
        """Reflex-origin dispatches skip guards; legacy callers use priority."""
        if origin is not None:
            return origin == "reflex"
        return proposal.priority >= REFLEX_PRIORITY_FLOOR

    async def _dispatch_one(
        self, proposal: Proposal, origin: str | None = None,
    ) -> Result:
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

        # Pre-dispatch guards — deterministic veto BEFORE execution and outside
        # the retry loop. Reflex-arc dispatches bypass guards (always fire).
        if self._guards and not self._bypasses_guards(proposal, origin):
            verdict = self._run_guards(proposal)
            if not verdict.allow:
                self._running_nodes.pop(proposal.action_id, None)
                self._running_proposals.pop(proposal.action_id, None)
                self.logger.info(
                    "Action vetoed by guard",
                    action_id=proposal.action_id,
                    reason=verdict.reason,
                )
                return Result(
                    action_id=proposal.action_id,
                    success=False,
                    error=ActionVetoed(proposal.action_id, verdict.reason),
                )

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

    def _run_guards(self, proposal: Proposal) -> GuardVerdict:
        """Consult guards in order. First deny wins; a raising guard fails closed."""
        for guard in self._guards:
            try:
                verdict = guard(proposal)
            except Exception as exc:  # noqa: BLE001 — fail closed on guard error
                self.logger.error(
                    "ActionGuard raised; failing closed",
                    action_id=proposal.action_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                return GuardVerdict(allow=False, reason=f"guard error: {exc}")
            if not verdict.allow:
                return verdict
        return GuardVerdict(allow=True)

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
                await self._report_fault(node.node_id, exc)
                result = Result(
                    action_id=node.node_id,
                    success=False,
                    error=exc,
                )
            last_result = result
            # Never retry an interrupted/preempted run (the stop was requested,
            # not a fault) or a failed rollback (the plant is in an unknown
            # partial state — re-running from step 1 could compound it).
            if (
                result.success
                or result.interrupted
                or result.preempted
                or isinstance(result.error, RollbackError)
            ):
                break

        return last_result  # type: ignore[return-value]
