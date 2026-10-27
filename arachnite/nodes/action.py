"""
arachnite.nodes.action
~~~~~~~~~~~~~~~~~~~~~~
BaseActionNode, MultiStepActionNode, and ActionMasterNode.
Spec reference: Sections 5.7, 17.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import sys
import time
import weakref
from abc import abstractmethod
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, TypeVar, runtime_checkable

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.exceptions import (
    ActionNotFoundError,
    ActionTimeoutError,
    ActionVetoed,
    MandatoryBlockViolation,
    NodeRegistrationError,
    ResourceConflictError,
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
    ReflexPreemption,
    Result,
    StepResult,
)
from arachnite.nodes.base import BaseNode, FaultReportingMixin, gather_settled

# Proposals at or above this priority are reflex/safety actions. They bypass
# ActionGuards to preserve the framework's "reflexes always fire" guarantee
# (see Proposal.priority: "200+ reflex, 100-199 safety").
REFLEX_PRIORITY_FLOOR = 200

# Results of launched actions kept until a tick drains them. The event lane
# stays live while the runtime is paused, when no tick drains them.
MAX_PENDING_RESULTS = 1024

_T = TypeVar("_T")

# Token of the action run whose code (or a task it started, e.g. the shielded
# safe-stop) is executing; compared with the master's live runs, so a task
# that outlives its run is no longer "inside" it.
_ACTION_RUN: contextvars.ContextVar[object | None] = contextvars.ContextVar(
    "arachnite_action_run", default=None,
)


async def _run_shielded(
    work: Awaitable[_T], *, if_cancelled: Callable[[], _T],
) -> tuple[_T, int]:
    """
    Await ``work`` to completion even if the calling task is cancelled
    meanwhile, and report how many cancels arrived. A safe-stop that has
    started must finish: a second cancel (emergency stop, shutdown, another
    preemption) would otherwise abandon the actuators half-braked. The
    caller decides what to do with the absorbed cancels.

    Nothing outside holds the inner task, so if it ends cancelled the work
    cancelled itself (e.g. a hook awaiting a future its driver cancelled):
    ``if_cancelled()`` supplies the value instead of losing the caller's
    Result. A cancel that lands in the turn the work finishes is absorbed
    too — the finished work's value is returned.
    """
    inner = asyncio.ensure_future(work)
    absorbed = 0
    while True:
        try:
            return await asyncio.shield(inner), absorbed
        except asyncio.CancelledError:
            if inner.cancelled():
                return if_cancelled(), absorbed
            absorbed += 1
            if inner.done():
                return inner.result(), absorbed


if sys.version_info >= (3, 11):

    async def _within(
        work: Awaitable[_T],
        timeout: float | None,
        *,
        stop_requested: Callable[[], bool] | None = None,
    ) -> _T:
        """
        Await ``work`` in the current task, raising ``TimeoutError`` after
        ``timeout`` seconds. Unlike ``asyncio.wait_for`` on Python 3.10 no
        inner task is created, so a cancel can neither be swallowed nor leave
        an orphaned task running. ``stop_requested`` is used on 3.10 only.

        The deadline cancels ``work`` once; clean-up it runs in response is
        not cut short. Work that swallows that cancel and returns after the
        deadline anyway is still a timeout (as on Python 3.10).
        """
        async with asyncio.timeout(timeout) as scope:
            result = await work
        if scope.expired():
            raise TimeoutError
        return result

else:  # pragma: no cover - exercised by the Python 3.10 CI leg
    # Per-task stack of active timeout scopes; each entry is [fired].
    _SCOPES: weakref.WeakKeyDictionary[asyncio.Task[Any], list[list[bool]]] = (
        weakref.WeakKeyDictionary()
    )

    async def _within(
        work: Awaitable[_T],
        timeout: float | None,
        *,
        stop_requested: Callable[[], bool] | None = None,
    ) -> _T:
        """
        Python 3.10 stand-in for ``asyncio.timeout``: the timer cancels the
        current task once, and the resulting CancelledError becomes
        ``asyncio.TimeoutError`` only if this scope fired, no enclosing scope
        fired and nobody asked the action to stop (``stop_requested``).
        Clean-up ``work`` runs in response to the cancel is not cut short,
        and work that returns after the deadline is still a timeout.

        Two Python 3.10 limits (3.11+ has neither): 3.10 does not count
        cancel requests, so a cancel from outside the master (e.g. event-loop
        shutdown) that lands in the same turn as this scope's timer is
        reported as the timeout; and a 3.10 ``asyncio.wait_for`` inside
        ``work`` can swallow the timer's cancel, so the work then ends when
        it next returns (still reported as a timeout).
        """
        task = asyncio.current_task()
        if timeout is None or task is None:
            return await work
        stack = _SCOPES.setdefault(task, [])
        depth = len(stack)
        fired = [False]
        finished = [False]
        stack.append(fired)

        def _fire() -> None:
            if not finished[0]:
                fired[0] = True
                task.cancel()

        handle = asyncio.get_running_loop().call_later(timeout, _fire)
        try:
            result = await work
            if fired[0]:
                # The timer's cancel was swallowed inside ``work``: the
                # deadline still passed, so it is still a timeout.
                raise asyncio.TimeoutError
            return result
        except asyncio.CancelledError:
            outer_fired = any(scope[0] for scope in stack[:depth])
            external = stop_requested is not None and stop_requested()
            if fired[0] and not outer_fired and not external:
                raise asyncio.TimeoutError from None
            raise
        finally:
            finished[0] = True
            handle.cancel()
            stack.pop()


#: Time an overrunning ``on_preempted()`` gets, after the cancel at
#: ``safe_stop_timeout_s``, before it is abandoned (left running, logged).
SAFE_STOP_ABANDON_GRACE_S = 0.1

# Safe-stop hooks abandoned while still running: held so they are not
# garbage-collected mid-flight; each removes itself when it ends.
_ABANDONED_HOOKS: set[asyncio.Future[Any]] = set()


def _master_cancel_overflow(received: int, master: int) -> bool:
    """
    True if the current task got more cancels than the master sent it, i.e.
    one came from outside the master (e.g. event-loop shutdown). Python 3.11+
    counts cancel requests per task; 3.10 cannot, so there ``received`` (the
    deliveries seen) is compared instead, and cancels that coalesced into
    one delivery go unnoticed.
    """
    if sys.version_info >= (3, 11):
        return _cancelling() > master
    return received > master


def _cancelling() -> int:
    """Pending cancel requests of the current task (always 0 on Python 3.10)."""
    current = asyncio.current_task()
    if current is None or not hasattr(current, "cancelling"):
        return 0
    return int(current.cancelling())


def _uncancel(count: int) -> None:
    """Consume ``count`` absorbed cancels (Python 3.11+ counts them per task)."""
    current = asyncio.current_task()
    if current is not None and hasattr(current, "uncancel"):
        for _ in range(count):
            current.uncancel()


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
    - Declare ``resources`` — the actuators this action commands. At most one
      running action owns a resource at any time (ADR 0005); undeclared
      access is invisible to the framework.
    - Override ``on_preempted()`` with a forward safe-stop ("brake, keep the
      grip") if a reflex may take this action's resources mid-execution.
      execute() must be cancellation-safe: preemption arrives as
      ``asyncio.CancelledError`` at an ``await``.

    Spec reference: Section 5.7.
    """

    timeout_s:   float = 5.0
    max_retries: int   = 0

    #: Actuators this action commands (e.g. ``frozenset({"arm", "gripper"})``).
    resources: frozenset[str] = frozenset()

    #: Upper bound on ``on_preempted()``. A hook that overruns it is
    #: cancelled, and abandoned (left running, logged) if it has not ended
    #: ``SAFE_STOP_ABANDON_GRACE_S`` later, so no hook — not even one that
    #: swallows its cancel — can hold a reflex back for longer.
    safe_stop_timeout_s: float = 1.0

    #: Set by ActionMasterNode when it cancels this action (preemption,
    #: cancel, shutdown, a cancelled ``dispatch()`` caller); cleared when the
    #: run is released. MultiStepActionNode also checks it at every step
    #: boundary, so a cancel that lands as a step finishes still stops it.
    _cancel_requested: bool = False

    #: Set by ``ActionMasterNode.cancel_all()`` (shutdown / emergency stop):
    #: a safe-stop that has not started yet runs without the rollback walk.
    _shutdown_requested: bool = False

    #: Cancels ActionMasterNode sent to the current run's task; more than
    #: this many arriving means one came from outside the master.
    _master_cancels: int = 0

    def safe_stop_bound_s(self) -> float:
        """Worst-case duration of this action's safe-stop (hook, then any walk)."""
        return self.safe_stop_timeout_s + SAFE_STOP_ABANDON_GRACE_S

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        """
        Safe-stop hook, called after a reflex (``preempted_by``) cancelled this
        action mid-execution and before the reflex action starts. Bring the
        actuators to a safe hold — do not undo work here: reversing a physical
        operation during a hazard can itself be unsafe. Default: no-op.
        """

    async def _run_safe_stop(
        self,
        proposal: Proposal,
        preempted_by: Proposal,
        *,
        allow_rollback: bool = True,
    ) -> tuple[bool, BaseException | None]:
        """
        Run ``on_preempted()`` bounded by ``safe_stop_timeout_s``. A hook
        that raises or overruns is logged and reported, never propagated —
        the preempting reflex must not be held back by a broken safe-stop.
        An overrunning hook is cancelled; if it has not ended
        ``SAFE_STOP_ABANDON_GRACE_S`` later it is abandoned (left running).
        Returns ``(rolled_back, error)``.
        """
        hook = asyncio.ensure_future(self.on_preempted(proposal, preempted_by))
        timed_out = False
        try:
            done, _ = await asyncio.wait({hook}, timeout=self.safe_stop_timeout_s)
            if not done:
                timed_out = True
                hook.cancel()
                done, _ = await asyncio.wait({hook}, timeout=SAFE_STOP_ABANDON_GRACE_S)
        except asyncio.CancelledError:
            hook.cancel()
            raise
        if not done:
            self.logger.error(
                "Safe-stop hook ignored its cancellation; abandoned",
                timeout_s=self.safe_stop_timeout_s,
            )
            _ABANDONED_HOOKS.add(hook)
            hook.add_done_callback(self._abandoned_hook_done)
            return False, ActionTimeoutError(
                self.node_id, "on_preempted", self.safe_stop_timeout_s,
            )
        if timed_out:
            self.logger.error(
                "Safe-stop hook timed out",
                timeout_s=self.safe_stop_timeout_s,
            )
            return False, ActionTimeoutError(
                self.node_id, "on_preempted", self.safe_stop_timeout_s,
            )
        if hook.cancelled():
            self.logger.error("Safe-stop hook ended with CancelledError")
            return False, RuntimeError(
                f"safe-stop of '{self.node_id}' ended with CancelledError",
            )
        exc = hook.exception()
        if exc is not None:
            self.logger.error(
                "Safe-stop hook raised",
                error=str(exc),
                error_type=type(exc).__name__,
            )
        return False, exc

    def _abandoned_hook_done(self, hook: asyncio.Future[Any]) -> None:
        _ABANDONED_HOOKS.discard(hook)
        exc = None if hook.cancelled() else hook.exception()
        self.logger.warning(
            "Abandoned safe-stop hook ended",
            error=str(exc) if exc is not None else None,
        )

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

    #: Run the rollback walk after ``on_preempted()`` when a reflex preempts
    #: this action. Off by default: undoing work during a hazard can itself
    #: be unsafe (ADR 0005).
    rollback_on_preempt: bool = False

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._interrupt_requested: bool = False
        self._interrupt_request: InterruptRequest | None = None
        self._current_step: ActionStep | None = None
        self._completed_steps: list[StepResult] = []
        # Set by a WAIT_FOR_BLOCK reflex while a mandatory block runs: stop
        # at the first boundary after the block, regardless of policy.
        self._reflex_preempt_by: Proposal | None = None
        # True when ActionMasterNode has already reset the run state at
        # reservation, so an interrupt requested before execute() starts
        # is kept rather than wiped by execute()'s own reset.
        self._run_prepared: bool = False
        # This run's rollback walk (interrupt, abort or rollback_on_preempt):
        # None before one starts, then "running", "done" or "failed". A run
        # walks at most once — a preemption arriving during or after a walk
        # does not start another, or completed steps would be undone twice.
        self._walk: str | None = None
        self._walk_error: BaseException | None = None
        # True while ActionMasterNode runs this action. A WAIT_FOR_BLOCK
        # hand-off then only records the reflex in _handoff_by; the master
        # runs the safe-stop outside the action's timeout and retries.
        self._managed: bool = False
        self._handoff_by: Proposal | None = None

    def _prepare_run(self) -> None:
        """Reset per-run state at reservation time (called by ActionMasterNode)."""
        self._interrupt_requested = False
        self._interrupt_request   = None
        self._reflex_preempt_by   = None
        self._completed_steps     = []
        self._current_step        = None
        self._walk                = None
        self._walk_error          = None
        self._handoff_by          = None
        self._managed             = True
        self._run_prepared        = True

    def request_reflex_preempt(self, by: Proposal) -> None:
        """
        Ask to be preempted at the first step boundary after the current
        block. With several waiting reflexes the highest-priority one is
        recorded (it is admitted first, so its proposal goes to the safe-stop).
        """
        current = self._reflex_preempt_by
        if current is None or by.priority > current.priority:
            self._reflex_preempt_by = by

    def _clear_run_state(self) -> None:
        """Drop per-run state after a cancelled run so nothing looks in-flight."""
        self._current_step      = None
        self._reflex_preempt_by = None
        self._walk              = None
        self._walk_error        = None
        self._handoff_by        = None
        self._managed           = False
        self._run_prepared      = False

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
        so no partial walk goes unreported. Each rollback is bounded by its
        step's ``timeout_s`` (the action's if unset); one that overruns is a
        failure, so a walk — and a safe-stop that includes it — is bounded.
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
            bound = step.timeout_s or self.timeout_s
            try:
                await _within(
                    step.rollback(), bound,
                    stop_requested=lambda: self._cancel_requested,
                )
            except asyncio.TimeoutError:
                self.logger.error("Rollback timed out", step=step.name, timeout_s=bound)
                failures.append((
                    step.name, ActionTimeoutError(self.node_id, step.name, bound),
                ))
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
        # Remaining worst-case time of the mandatory block the action is in
        # now: the current step and the non-interruptible steps contiguous
        # with it. Zero outside a block (later blocks are not counted).
        all_steps    = self.steps()
        current_idx  = next(
            (i for i, s in enumerate(all_steps)
             if self._current_step and s.name == self._current_step.name),
            len(all_steps),
        )
        remaining_mandatory = 0.0
        for s in all_steps[current_idx:]:
            if s.interruptible:
                break
            remaining_mandatory += s.timeout_s or self.timeout_s
        return ActionExecutionState(
            action_id                   = self.node_id,
            current_step                = self._current_step.name if self._current_step else None,
            completed_steps             = completed_names,
            interruptible               = self.is_interruptible(),
            mandatory_block_remaining_s = remaining_mandatory,
        )

    async def execute(self, proposal: Proposal) -> Result:
        """Orchestrates step execution with interrupt and rollback handling."""
        if not self._run_prepared:
            # Runtime-less call: reset here. Under ActionMasterNode the reset
            # already happened at reservation, and an interrupt that arrived
            # since then must survive.
            self._interrupt_requested = False
            self._interrupt_request   = None
            self._reflex_preempt_by   = None
            self._completed_steps     = []
            self._walk                = None
            self._walk_error          = None
            self._managed             = False
            self._handoff_by          = None
            self._cancel_requested    = False
            self._shutdown_requested  = False
        self._run_prepared = False

        all_steps  = self.steps()
        start_time = time.monotonic()

        for i, step in enumerate(all_steps):
            if self._cancel_requested:
                raise asyncio.CancelledError()  # a cancel lost as a step finished
            self._current_step = step
            pending = all_steps[i + 1:]

            # The boundary before this step is a stop point unless it lies
            # strictly inside a mandatory block (previous and this step both
            # non-interruptible). In particular the boundary *before* a block
            # is one: a pending stop is honoured there instead of running the
            # whole block only to undo it afterwards.
            stop_point = step.interruptible or i == 0 or all_steps[i - 1].interruptible

            # A WAIT_FOR_BLOCK reflex is waiting: the first stop point hands
            # the resources over, whatever the interrupt policy (policies
            # govern deliberation, not reflexes).
            if self._reflex_preempt_by is not None and stop_point:
                return await self._handle_reflex_preempt(
                    proposal, self._reflex_preempt_by, start_time,
                )

            # Check interrupt request before starting this step
            if self._interrupt_requested and stop_point:
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
            if self._cancel_requested:
                raise asyncio.CancelledError()  # a cancel lost as a step finished

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
            result = await _within(
                self.execute_step(step, proposal, list(self._completed_steps)),
                timeout,
                stop_requested=lambda: self._cancel_requested,
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
            # Left "running" if a preemption cancels the walk: the safe-stop
            # then does not start it again, and the run is never retried.
            self._walk = "running"
            try:
                await self.on_interrupted(self._completed_steps.copy(), pending, proposal)
                rolled_back = True
            except RollbackError as exc:
                error = exc
            self._end_walk(error)

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

    async def _run_safe_stop(
        self,
        proposal: Proposal,
        preempted_by: Proposal,
        *,
        allow_rollback: bool = True,
    ) -> tuple[bool, BaseException | None]:
        """
        Safe-stop hook, then the rollback walk if ``rollback_on_preempt``.
        ``allow_rollback=False`` (shutdown / emergency_stop) skips the walk,
        and so does a run that has already walked (or started to): walking
        again would undo completed steps twice.
        """
        rolled_back, error = await super()._run_safe_stop(proposal, preempted_by)
        if self.rollback_on_preempt and allow_rollback and self._walk is None:
            self._walk = "running"
            walk_error: RollbackError | None = None
            try:
                await self._rollback_completed(self._completed_steps.copy())
                rolled_back = True
            except RollbackError as exc:
                walk_error = exc
                error = error or exc
            self._end_walk(walk_error)
        return rolled_back, error

    def _end_walk(self, error: BaseException | None) -> None:
        self._walk = "failed" if error is not None else "done"
        self._walk_error = error

    def walk_cut_short(self) -> bool:
        """True if this run's rollback walk started but never reached its end."""
        return self._walk == "running"

    def safe_stop_bound_s(self) -> float:
        """Worst-case safe-stop: the hook, then the walk if ``rollback_on_preempt``."""
        bound = super().safe_stop_bound_s()
        if self.rollback_on_preempt:
            bound += sum(
                s.timeout_s or self.timeout_s
                for s in self.steps()
                if not s.interruptible and s.rollback is not None
            )
        return bound

    async def _handle_reflex_preempt(
        self,
        proposal: Proposal,
        preempted_by: Proposal,
        start_time: float,
    ) -> Result:
        """
        Hand the resources to a waiting reflex at a step boundary.

        Under ActionMasterNode the safe-stop is only recorded here
        (``_handoff_by``) and run by the master after ``execute()`` returns:
        outside the action's timeout, protected from further cancels, and
        exactly once even if a PREEMPT reflex arrives meanwhile. A
        runtime-less ``execute()`` runs it inline.
        """
        stopped_at = self._current_step.name if self._current_step else None
        self.logger.info(
            "Action preempted by reflex at step boundary",
            stopped_at=stopped_at,
            reflex=preempted_by.instinct_id,
        )
        rolled_back, error = False, None
        if self._managed:
            self._handoff_by = preempted_by
        else:
            rolled_back, error = await self._run_safe_stop(proposal, preempted_by)
        self._current_step      = None
        self._reflex_preempt_by = None
        return Result(
            action_id       = self.node_id,
            success         = False,
            error           = error,
            duration_s      = time.monotonic() - start_time,
            interrupted     = True,
            preempted       = True,
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
        self._walk = "running"  # see _handle_interrupt
        walk_error: RollbackError | None = None
        try:
            rolled_back = await self.on_aborted(
                self._completed_steps[:-1], step, proposal,
            )
        except RollbackError as exc:
            error = walk_error = exc
        if rolled_back or walk_error is not None:
            self._end_walk(walk_error)
        else:
            self._walk = None  # on_aborted() undid nothing

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

class LaunchStatus(Enum):
    """Outcome of ``ActionMasterNode.launch()`` / ``launch_reflex()`` (ADR 0005)."""
    LAUNCHED  = "launched"   # execute() started as a tracked task
    BLOCKED   = "blocked"    # conflicts with a running owner; not started
    ARMED     = "armed"      # reflex waits for its owners to release
    DEDUPED   = "deduped"    # reflex is already being handled
    VETOED    = "vetoed"     # an ActionGuard denied the proposal
    NOT_FOUND = "not_found"  # no ActionNode registered under that action_id
    CLOSED    = "closed"     # the master is shutting down; nothing new starts


@dataclass(frozen=True, slots=True)
class LaunchOutcome:
    """What ``launch()`` / ``launch_reflex()`` did with a proposal."""

    status:    LaunchStatus
    action_id: str
    #: Running (or armed) action_ids the proposal conflicted with.
    owners:    frozenset[str] = frozenset()


# Stand-in "preempting" proposal handed to on_preempted() when a run is
# cancelled by shutdown or emergency_stop rather than by a reflex.
_SHUTDOWN_PROPOSAL = Proposal(
    instinct_id = "shutdown",
    action_id   = "__shutdown__",
    priority    = 9999,
    urgency     = 1.0,
    rationale   = "Runtime shutdown cancelled the action",
)


class ActionMasterNode(FaultReportingMixin, BaseNode):
    """
    Routes Proposals to the correct ActionNode, enforces timeouts and
    retries, arbitrates actuator resources, and manages interrupts.

    Two dispatch styles (ADR 0005):

    - ``launch()`` / ``launch_reflex()`` — used by ArachniteRuntime. All
      checks and the reservation happen synchronously, ``execute()`` runs as
      a tracked task, and the call returns at once. Finished Results are
      collected with ``drain_completed()``; ``wait_idle()`` waits for them.
    - ``dispatch()`` / ``dispatch_many()`` — await the Result (runtime-less
      use, ADR 0001).

    An action conflicts with a running one when they share an ``action_id``
    or a declared resource; at most one running action owns a resource at
    any time (actuator exclusivity). A deliberative launch that conflicts is
    BLOCKED. A reflex that conflicts is armed and its owners are preempted
    according to the reflex's ``ReflexPreemption`` mode; the reflex starts the
    moment the owners have run their safe-stop and released.

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
        strict_resources: bool = False,
    ) -> None:
        super().__init__(bus, config, log_sinks, agent_node_id)
        self.strict_resources = strict_resources
        self._nodes: dict[str, BaseActionNode] = {}
        self._running_nodes: dict[str, BaseActionNode] = {}
        self._running_proposals: dict[str, Proposal] = {}
        # action_id -> "reflex" | "decision" | "direct"
        self._origins: dict[str, str] = {}
        # resource -> action_id currently owning it
        self._owners: dict[str, str] = {}
        # reflex action_id -> its proposal, waiting for owners to release.
        # Every armed reflex claims all of its resources (blocks deliberation).
        self._armed: dict[str, Proposal] = {}
        # Set by cancel_all() (shutdown / emergency_stop), cleared by setup():
        # once closed, no launch or admission starts anything new.
        self._closed: bool = False
        # Preempted-before-start Results of awaited (dispatch) runs, by task.
        self._unstarted_results: dict[asyncio.Task[Result], Result] = {}
        # Tokens of the runs in progress (see _ACTION_RUN).
        self._live_runs: set[object] = set()
        # running action_id -> reflex proposal that preempted it
        self._preempted_by: dict[str, Proposal] = {}
        self._tasks: dict[str, asyncio.Task[Result]] = {}
        self._buffered: set[asyncio.Task[Result]] = set()
        self._completed: deque[Result] = deque(maxlen=MAX_PENDING_RESULTS)
        self._results_dropped = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._guards: list[ActionGuard] = []
        for guard in guards or ():
            self.register_guard(guard)

    def register(self, node: BaseActionNode) -> None:
        if node.node_id in self._nodes:
            raise NodeRegistrationError(node.node_id, self.node_id)
        if self.strict_resources and not node.resources:
            raise NodeRegistrationError(
                node.node_id,
                self.node_id,
                reason=(
                    f"Action '{node.node_id}' declares no resources; "
                    "strict_resources requires every action to name the "
                    "actuators it commands."
                ),
            )
        self._nodes[node.node_id] = node
        self.logger.debug("Registered action node", action_node_id=node.node_id)

    def register_guard(self, guard: ActionGuard) -> None:
        """
        Install a synchronous, bounded, no-I/O pre-dispatch veto.

        Guards run in registration order BEFORE the action executes and
        OUTSIDE the retry loop. The first deny wins (remaining guards are
        skipped) and the action's ``execute()`` is never called. A guard that
        raises FAILS CLOSED (treated as a deny) — unlike
        ``InstinctMasterNode.set_pre_evaluate_gate``, which fails open,
        because this is a safety layer.

        Reflex-arc dispatches (``origin="reflex"``) bypass all guards to
        preserve the framework's reflex-always-fires guarantee.
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

    def origin_of(self, action_id: str) -> str | None:
        """Dispatch path of a running action: 'reflex', 'decision' or 'direct'."""
        return self._origins.get(action_id)

    def armed_reflexes(self) -> dict[str, Proposal]:
        """Reflex proposals waiting for their owners to release, by action_id."""
        return dict(self._armed)

    def resource_owners(self) -> dict[str, str]:
        """Current resource -> owning action_id map (a snapshot)."""
        return dict(self._owners)

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

    def remaining_mandatory_s(self) -> float:
        """Longest remaining mandatory-block time across running actions."""
        remaining = 0.0
        for node in self._running_nodes.values():
            if isinstance(node, MultiStepActionNode):
                remaining = max(
                    remaining, node.execution_state().mandatory_block_remaining_s,
                )
        return remaining

    async def request_interrupt(
        self,
        request: InterruptRequest,
        action_id: str | None = None,
    ) -> None:
        """
        Signal a running action to stop at its next safe point (deliberative
        interrupt). If ``action_id`` is None, targets
        ``request.target_action_id`` or else the highest-priority running
        action. Raises ``MandatoryBlockViolation`` inside a mandatory block —
        deliberation never cuts into one.
        """
        target = action_id or request.target_action_id
        node = self._running_nodes.get(target) if target is not None else self.current_action()
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

    def interrupt_all(self, request: InterruptRequest) -> None:
        """
        Deliver ``request`` to every running multi-step action, bypassing the
        mandatory-block gate: the flag is held until the block ends. Used by
        graceful shutdown so ROLLBACK actions stop at a boundary and unwind.
        """
        for node in self._running_nodes.values():
            if isinstance(node, MultiStepActionNode):
                node.request_interrupt(request)

    async def setup(self) -> None:
        self._closed = False
        await asyncio.gather(*(n.setup() for n in self._nodes.values()))

    def close(self) -> None:
        """
        Refuse new work: ``launch()`` / ``launch_reflex()`` return CLOSED,
        ``dispatch()`` returns a failed Result, and armed reflexes are
        dropped (owners waiting to hand over to them carry on). Running
        actions are not touched (``cancel_all()`` stops them). Reopened by
        ``setup()``.
        """
        self._closed = True
        self._drop_armed()

    def _drop_armed(self) -> None:
        """Forget every armed reflex and withdraw the hand-offs waiting for them."""
        self._armed.clear()
        for owner in self._running_nodes.values():
            if isinstance(owner, MultiStepActionNode):
                owner._reflex_preempt_by = None
        self._maybe_idle()

    def _is_idle(self) -> bool:
        """Nothing running or armed, and every launched Result is buffered."""
        return not self._running_nodes and not self._armed and not self._buffered

    def _maybe_idle(self) -> None:
        if self._is_idle():
            self._idle.set()

    async def teardown(self) -> None:
        await self.cancel_all(timeout=self.safe_stop_budget_s())
        await gather_settled(*(n.cancel_background_tasks() for n in self._nodes.values()))
        await gather_settled(*(n.teardown() for n in self._nodes.values()))

    # Hook fan-outs isolate each node: one raising hook is that node's fault
    # (reported to its supervisor), never the whole tick's.

    async def on_pause(self) -> None:
        await self._gather_nodes(
            "on_pause()", [(n.node_id, n.on_pause()) for n in self._nodes.values()],
        )

    async def on_resume(self) -> None:
        await self._gather_nodes(
            "on_resume()", [(n.node_id, n.on_resume()) for n in self._nodes.values()],
        )

    async def notify_tick_start(self, tick: int) -> None:
        await self._gather_nodes(
            "on_tick_start()",
            [(n.node_id, n.on_tick_start(tick)) for n in self._nodes.values()],
        )

    async def notify_tick_end(self, tick: int, duration_s: float) -> None:
        await self._gather_nodes(
            "on_tick_end()",
            [(n.node_id, n.on_tick_end(tick, duration_s)) for n in self._nodes.values()],
        )

    # ── Conflicts ────────────────────────────────────────────────────────

    def _resources_of(self, action_id: str) -> frozenset[str]:
        node = self._nodes.get(action_id)
        return node.resources if node is not None else frozenset()

    def _running_conflicts(self, action_id: str) -> set[str]:
        """Running action_ids sharing ``action_id`` or one of its resources."""
        owners: set[str] = set()
        if action_id in self._running_nodes:
            owners.add(action_id)
        for resource in self._resources_of(action_id):
            owner = self._owners.get(resource)
            if owner is not None:
                owners.add(owner)
        return owners

    def _claim_conflicts(self, action_id: str) -> set[str]:
        """
        Armed reflexes claiming ``action_id`` or one of its resources. Every
        armed reflex claims all of its resources until it is admitted or
        cancelled, so a claim is never lost to an earlier claimant's
        admission.
        """
        resources = self._resources_of(action_id)
        claimants = {
            armed_id for armed_id in self._armed
            if armed_id == action_id or resources & self._resources_of(armed_id)
        }
        return claimants

    def conflicting_owners(self, proposal: Proposal) -> set[str]:
        """
        Action_ids a proposal conflicts with: running owners of its
        action_id or resources, and armed reflexes that claimed them.
        Empty means it can start now.
        """
        return (
            self._running_conflicts(proposal.action_id)
            | self._claim_conflicts(proposal.action_id)
        )

    # ── Launch (non-blocking, used by the runtime) ───────────────────────

    def launch(self, proposal: Proposal, *, origin: str = "decision") -> LaunchOutcome:
        """
        Start ``proposal``'s action as a tracked task and return immediately.

        Synchronous all the way to ``create_task``: lookup, conflict check,
        guards and resource reservation cannot interleave with another
        launch. The Result arrives later via ``drain_completed()``; a guard
        veto is buffered there at once.
        """
        action_id = proposal.action_id
        node = self._nodes.get(action_id)
        if node is None:
            return LaunchOutcome(LaunchStatus.NOT_FOUND, action_id)
        if self._closed:
            return LaunchOutcome(LaunchStatus.CLOSED, action_id)

        owners = self.conflicting_owners(proposal)
        if owners:
            return LaunchOutcome(LaunchStatus.BLOCKED, action_id, frozenset(owners))

        if self._guards and not self._bypasses_guards(proposal, origin):
            verdict = self._run_guards(proposal)
            if not verdict.allow:
                self.logger.info(
                    "Action vetoed by guard",
                    action_id=action_id,
                    reason=verdict.reason,
                )
                self._buffer_result(Result(
                    action_id=action_id,
                    success=False,
                    error=ActionVetoed(action_id, verdict.reason),
                ))
                return LaunchOutcome(LaunchStatus.VETOED, action_id)

        self._start(node, proposal, origin, buffer=True)
        return LaunchOutcome(LaunchStatus.LAUNCHED, action_id)

    def launch_reflex(
        self,
        proposal: Proposal,
        preemption: ReflexPreemption = ReflexPreemption.PREEMPT,
    ) -> LaunchOutcome:
        """
        Launch a reflex action, taking its resources from running owners.

        - No conflict: start now.
        - Already running or armed as a reflex: de-duplicated.
        - Conflict: the reflex is armed and claims its resources (no
          deliberative launch may take them meanwhile). Each deliberative
          owner is preempted — cancelled now (PREEMPT, or WAIT_FOR_BLOCK
          outside a mandatory block), or at the first boundary after its
          mandatory block (WAIT_FOR_BLOCK inside one). A running reflex is
          never preempted; the new reflex waits for it. The armed reflex
          starts as soon as every owner has finished its safe-stop and
          released (see ``_admit_armed``).
        """
        action_id = proposal.action_id
        node = self._nodes.get(action_id)
        if node is None:
            return LaunchOutcome(LaunchStatus.NOT_FOUND, action_id)
        if self._closed:
            return LaunchOutcome(LaunchStatus.CLOSED, action_id)
        if action_id in self._armed or self._origins.get(action_id) == "reflex":
            return LaunchOutcome(LaunchStatus.DEDUPED, action_id)

        owners = self._running_conflicts(action_id)
        claimants = self._claim_conflicts(action_id)
        if not owners and not claimants:
            self._start(node, proposal, "reflex", buffer=True)
            return LaunchOutcome(LaunchStatus.LAUNCHED, action_id)

        self._armed[action_id] = proposal
        self._idle.clear()

        for owner_id in owners:
            if self._origins.get(owner_id) == "reflex":
                continue  # a reflex is never preempted; wait for it
            owner = self._running_nodes[owner_id]
            in_block = (
                isinstance(owner, MultiStepActionNode) and not owner.is_interruptible()
            )
            if preemption is ReflexPreemption.WAIT_FOR_BLOCK and in_block:
                assert isinstance(owner, MultiStepActionNode)
                owner.request_reflex_preempt(proposal)
                self.logger.info(
                    "Reflex armed; owner preempted after its mandatory block",
                    action_id=action_id,
                    owner=owner_id,
                )
            else:
                self._preempt(owner_id, proposal)

        # Waiting only on armed claimants: if they rank lower and nothing
        # running holds the resources, admission starts this reflex now
        # instead of leaving it behind a lower-priority claim.
        if not owners:
            self._admit_armed()
            if action_id not in self._armed:
                return LaunchOutcome(LaunchStatus.LAUNCHED, action_id)

        return LaunchOutcome(
            LaunchStatus.ARMED, action_id, frozenset(owners | claimants),
        )

    def _preempt(self, owner_id: str, by: Proposal) -> None:
        """Cancel a running owner on behalf of reflex ``by`` (once)."""
        if owner_id in self._preempted_by:
            return
        task = self._tasks.get(owner_id)
        if task is None or task.done():
            return
        self._preempted_by[owner_id] = by
        self.logger.info(
            "Preempting action for reflex",
            action_id=owner_id,
            reflex_action=by.action_id,
            reflex=by.instinct_id,
        )
        self._cancel_run(self._running_nodes[owner_id], task)

    def _cancel_run(self, node: BaseActionNode, task: asyncio.Task[Result]) -> None:
        """Cancel a run on the master's behalf; counted, to tell foreign cancels apart."""
        node._cancel_requested = True
        node._master_cancels += 1
        task.cancel()

    def _admit_armed(self) -> None:
        """
        Start armed reflexes whose owners have all released, highest priority
        first. A higher-priority reflex that still waits keeps its resources
        reserved, so a lower-priority one sharing them cannot start ahead of
        it.
        """
        if self._closed:
            return
        reserved: set[str] = set()
        for action_id, proposal in sorted(
            self._armed.items(), key=lambda kv: kv[1].priority, reverse=True,
        ):
            resources = self._resources_of(action_id)
            if self._running_conflicts(action_id) or resources & reserved:
                reserved |= resources
                continue
            del self._armed[action_id]
            node = self._nodes.get(action_id)
            if node is None:
                continue
            self._start(node, proposal, "reflex", buffer=True)

    # ── Task lifecycle ───────────────────────────────────────────────────

    def _start(
        self,
        node: BaseActionNode,
        proposal: Proposal,
        origin: str,
        *,
        buffer: bool,
    ) -> asyncio.Task[Result]:
        """Reserve ``node`` and its resources, then start execute() as a task."""
        action_id = node.node_id
        self._running_nodes[action_id] = node
        self._running_proposals[action_id] = proposal
        self._origins[action_id] = origin
        for resource in node.resources:
            self._owners[resource] = action_id
        node._cancel_requested = False
        node._shutdown_requested = False
        node._master_cancels = 0
        if isinstance(node, MultiStepActionNode):
            node._prepare_run()
        self._idle.clear()

        self.logger.info(
            "Dispatching action",
            action_id=action_id,
            priority=proposal.priority,
            origin=origin,
        )
        task = asyncio.get_running_loop().create_task(
            self._run(node, proposal), name=f"action:{action_id}",
        )
        self._tasks[action_id] = task
        if buffer:
            self._buffered.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    async def _run(self, node: BaseActionNode, proposal: Proposal) -> Result:
        """
        Task body: execute with retries; on cancellation run the safe-stop.

        Every safe-stop runs to completion once started (``_run_shielded``):
        an emergency stop, shutdown or another preemption arriving meanwhile
        waits for it instead of abandoning the actuators half-braked. A run
        the master cancelled (preemption, ``cancel()``, ``cancel_all()``)
        ends with a Result (``interrupted=True``); only a cancel from outside
        the master (e.g. event-loop shutdown) propagates.
        """
        action_id = node.node_id
        start = time.monotonic()
        run_token = object()
        self._live_runs.add(run_token)
        _ACTION_RUN.set(run_token)
        try:
            try:
                result = await self._execute_with_retry(node, proposal)
            except asyncio.CancelledError:
                by = self._preempted_by.get(action_id)
                # Shutdown / emergency_stop cancels run the safe-stop only;
                # rollback_on_preempt applies to reflex preemption, unless a
                # shutdown came before the safe-stop started.
                (rolled_back, error), absorbed = await _run_shielded(
                    node._run_safe_stop(
                        proposal,
                        by if by is not None else _SHUTDOWN_PROPOSAL,
                        allow_rollback=by is not None and not node._shutdown_requested,
                    ),
                    if_cancelled=lambda: (False, self._safe_stop_cancelled(node)),
                )
                # Read after the safe-stop: a master cancel or preemption that
                # arrived during it (after a foreign cancel) still counts.
                preempted = action_id in self._preempted_by
                requested = node._cancel_requested
                foreign = _master_cancel_overflow(1 + absorbed, node._master_cancels)
                stopped_at: str | None = None
                step_results: list[StepResult] = []
                if isinstance(node, MultiStepActionNode):
                    if node._current_step is not None:
                        stopped_at = node._current_step.name
                    step_results = node._completed_steps.copy()
                    # A walk this run already completed (its Result was
                    # swallowed with the cancel) still counts.
                    if node._walk == "done":
                        rolled_back = True
                    elif node._walk == "failed":
                        error = error or node._walk_error
                    node._clear_run_state()
                if not preempted:
                    self.logger.info(
                        "Action cancelled; safe-stop complete",
                        action_id=action_id,
                        stopped_at=stopped_at,
                    )
                if foreign or not (preempted or requested):
                    raise  # a cancel from outside the master: propagate
                # The master's cancels (also any that arrived during the
                # safe-stop) are consumed: the run ends with a Result.
                _uncancel(_cancelling())
                result = Result(
                    action_id       = action_id,
                    success         = False,
                    error           = error,
                    interrupted     = True,
                    preempted       = preempted,
                    stopped_at_step = stopped_at,
                    step_results    = step_results,
                    rolled_back     = rolled_back,
                )
            if isinstance(node, MultiStepActionNode) and node._handoff_by is not None:
                # WAIT_FOR_BLOCK hand-off at a step boundary: the safe-stop
                # (and rollback_on_preempt) runs here, outside the action's
                # timeout and never retried, exactly once.
                by = node._handoff_by
                node._handoff_by = None
                sent_before = node._master_cancels
                (rolled_back, error), absorbed = await _run_shielded(
                    node._run_safe_stop(
                        proposal, by, allow_rollback=not node._shutdown_requested,
                    ),
                    if_cancelled=lambda: (False, self._safe_stop_cancelled(node)),
                )
                if _master_cancel_overflow(absorbed, node._master_cancels - sent_before):
                    raise asyncio.CancelledError()  # a foreign cancel: propagate
                _uncancel(_cancelling())
                result = Result(
                    action_id       = result.action_id,
                    success         = False,
                    error           = result.error or error,
                    interrupted     = True,
                    preempted       = True,
                    stopped_at_step = result.stopped_at_step,
                    step_results    = result.step_results,
                    rolled_back     = rolled_back,
                )
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
                preempted       = result.preempted,
            )
        finally:
            self._live_runs.discard(run_token)
            self._release(action_id)

        self.logger.info(
            "Action complete",
            action_id=action_id,
            success=result.success,
            preempted=result.preempted,
            duration_ms=round(result.duration_s * 1000, 2),
        )
        return result

    def _safe_stop_cancelled(self, node: BaseActionNode) -> BaseException:
        """Report a safe-stop hook that ended in CancelledError (it cancelled itself)."""
        self.logger.error(
            "Safe-stop hook ended with CancelledError",
            action_id=node.node_id,
        )
        return RuntimeError(f"safe-stop of '{node.node_id}' ended with CancelledError")

    def in_action_context(self) -> bool:
        """
        True when called from a running action's code: its task body, or a
        task started from it while the run lasts (such as its safe-stop).
        """
        token = _ACTION_RUN.get()
        return token is not None and token in self._live_runs

    def _release(self, action_id: str) -> None:
        """Free the action's reservation and resources, then admit armed reflexes."""
        node = self._running_nodes.get(action_id)
        if node is not None:
            node._cancel_requested = False
            node._shutdown_requested = False
        self._running_nodes.pop(action_id, None)
        self._running_proposals.pop(action_id, None)
        self._origins.pop(action_id, None)
        self._preempted_by.pop(action_id, None)
        self._tasks.pop(action_id, None)
        for resource, owner in list(self._owners.items()):
            if owner == action_id:
                del self._owners[resource]
        self._admit_armed()

    def _on_task_done(self, task: asyncio.Task[Result]) -> None:
        """Buffer a launched task's Result and signal idleness."""
        buffered = task in self._buffered
        self._buffered.discard(task)
        if task.cancelled():
            self._release_unstarted(task, buffered=buffered)
        if buffered and not task.cancelled():
            exc = task.exception()
            if exc is not None:
                self.logger.error(
                    "Action task failed",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            else:
                self._buffer_result(task.result())
        self._maybe_idle()

    def _release_unstarted(self, task: asyncio.Task[Result], *, buffered: bool) -> None:
        """
        Release an action whose task was cancelled before its first step.

        A cancel that lands before the task body starts never enters
        ``_run``, so its ``finally`` cannot release the reservation. The
        action commanded nothing, so no safe-stop is due; a preemption or a
        master cancel is still reported as an interrupted Result.
        """
        action_id = next(
            (aid for aid, t in self._tasks.items() if t is task), None,
        )
        if action_id is None:
            return  # the body ran and released already
        preempted = action_id in self._preempted_by
        node = self._running_nodes.get(action_id)
        requested = node is not None and node._cancel_requested
        if isinstance(node, MultiStepActionNode):
            node._clear_run_state()
        self.logger.info(
            "Action cancelled before it started",
            action_id=action_id,
            preempted=preempted,
        )
        if preempted or requested:
            result = Result(
                action_id   = action_id,
                success     = False,
                interrupted = True,
                preempted   = preempted,
            )
            if buffered:
                self._buffer_result(result)
            else:
                self._unstarted_results[task] = result  # for the dispatch() caller
        self._release(action_id)

    def drain_completed(self) -> list[Result]:
        """Return (and clear) the Results of launched actions finished so far."""
        completed = list(self._completed)
        self._completed.clear()
        if self._results_dropped:
            self.logger.warning(
                "Action Results dropped before a tick could take them",
                dropped=self._results_dropped,
                kept=len(completed),
            )
            self._results_dropped = 0
        return completed

    def _buffer_result(self, result: Result) -> None:
        """Keep a finished Result for drain_completed() (oldest dropped when full)."""
        if len(self._completed) == self._completed.maxlen:
            self._results_dropped += 1
        self._completed.append(result)

    async def wait_idle(self, timeout: float | None = None) -> bool:
        """
        Wait until no action is running or armed. Returns False on timeout.
        Results of the finished launches are then available from
        ``drain_completed()``.
        """
        if self._is_idle():
            return True
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return True

    async def wait_until_interruptible(self, timeout: float) -> bool:
        """Wait until no running action is inside a mandatory block."""
        deadline = time.monotonic() + timeout
        while not self.is_interruptible():
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.005)
        return True

    async def cancel(self, action_id: str, timeout: float | None = None) -> None:
        """
        Withdraw the reflex armed under ``action_id`` if there is one;
        otherwise cancel the running run of ``action_id`` (its safe-stop runs,
        and its Result reports ``interrupted=True``) and wait for it.

        Withdrawing an armed reflex never touches a running run, even one
        under the same action_id: owners that were waiting to hand over to
        it carry on, unless another armed reflex still needs their resources.
        Call again to cancel the running run as well.
        """
        dropped = self._armed.pop(action_id, None)
        if dropped is not None:
            self._redirect_handoffs(dropped)
            self._admit_armed()
            self._maybe_idle()
            return
        task = self._tasks.get(action_id)
        if task is None or task.done():
            return
        self._cancel_run(self._running_nodes[action_id], task)
        await asyncio.wait({task}, timeout=timeout)

    def _redirect_handoffs(self, dropped: Proposal) -> None:
        """Point WAIT_FOR_BLOCK hand-offs for ``dropped`` at a remaining claimant."""
        for owner in self._running_nodes.values():
            if not isinstance(owner, MultiStepActionNode):
                continue
            if owner._reflex_preempt_by is not dropped:
                continue
            successor = max(
                (p for aid, p in self._armed.items()
                 if self._resources_of(aid) & owner.resources),
                key=lambda p: p.priority,
                default=None,
            )
            owner._reflex_preempt_by = successor

    async def cancel_all(self, timeout: float | None = None) -> None:
        """
        Cancel every running action — each runs its safe-stop, no rollback —
        drop armed reflexes, and close the master so nothing new starts
        until the next ``setup()``. Used by shutdown and emergency_stop. A
        safe-stop already in progress (with its rollback walk) is waited for,
        not cut short; one not yet started skips the walk.
        """
        self._closed = True
        self._drop_armed()
        tasks = []
        for action_id, task in list(self._tasks.items()):
            node = self._running_nodes.get(action_id)
            if task.done() or node is None:
                continue
            node._shutdown_requested = True
            self._cancel_run(node, task)
            tasks.append(task)
        if tasks:
            await asyncio.wait(tasks, timeout=timeout)
        self._maybe_idle()

    def safe_stop_budget_s(self) -> float:
        """
        Time to allow cancelled actions to finish their safe-stop: the
        longest ``safe_stop_bound_s()`` of the running actions (hook, abandon
        grace, and the rollback walk where ``rollback_on_preempt``) + 0.5 s.
        """
        budgets = [n.safe_stop_bound_s() for n in self._running_nodes.values()]
        return max(budgets, default=0.0) + 0.5

    # ── Single dispatch (awaiting; runtime-less use) ─────────────────────

    async def dispatch(
        self, proposal: Proposal, *, origin: str | None = None,
    ) -> Result:
        """
        Route a single Proposal to its ActionNode by matching
        ``proposal.action_id`` and await its Result.  For concurrent dispatch
        of multiple proposals, use ``dispatch_many()``; the runtime uses the
        non-blocking ``launch()`` instead.

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

        # Check every action exists before starting any: raising after some
        # had started would leave them running, unawaited, with no Result.
        for p in proposals:
            if p.action_id not in self._nodes:
                raise ActionNotFoundError(p.action_id)

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
        action_id = proposal.action_id
        node = self._nodes.get(action_id)
        if node is None:
            raise ActionNotFoundError(action_id)

        # Atomic check-and-set (no await until the reservation in _start):
        # skip if the action, or one of its resources, is already taken.
        owners = self.conflicting_owners(proposal)
        if owners:
            self.logger.debug(
                "Action conflicts with a running action, skipping",
                action_id=action_id,
                owners=sorted(owners),
            )
            error: BaseException = (
                RuntimeError("action already running")
                if action_id in owners
                else ResourceConflictError(action_id, sorted(owners))
            )
            return Result(action_id=action_id, success=False, error=error)

        # Pre-dispatch guards — deterministic veto BEFORE execution and outside
        # the retry loop. Reflex-arc dispatches bypass guards (always fire).
        if self._guards and not self._bypasses_guards(proposal, origin):
            verdict = self._run_guards(proposal)
            if not verdict.allow:
                self.logger.info(
                    "Action vetoed by guard",
                    action_id=action_id,
                    reason=verdict.reason,
                )
                return Result(
                    action_id=action_id,
                    success=False,
                    error=ActionVetoed(action_id, verdict.reason),
                )

        if self._closed:
            return Result(
                action_id=action_id,
                success=False,
                error=RuntimeError("action master is shut down"),
            )
        task = self._start(node, proposal, origin or "direct", buffer=False)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # The shield raises only if the caller was cancelled or the run's
            # own task was cancelled; a run that finished normally therefore
            # means the caller was cancelled.
            if task.cancelled():
                # Cancelled before its first step: the run produced no
                # Result of its own, but the caller still gets one.
                stashed = self._unstarted_results.pop(task, None)
                if stashed is not None and _cancelling() == 0:
                    return stashed
                raise
            if not task.done():
                # The caller itself was cancelled: stop the action through
                # the master (safe-stop, flag seen at every step boundary)
                # and wait until it has released — through further cancels
                # of the caller too — then propagate.
                self._cancel_run(node, task)
                while not task.done():
                    with contextlib.suppress(asyncio.CancelledError):
                        await asyncio.wait({task})
                self._unstarted_results.pop(task, None)
            raise

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
                if self._retry_blocked(node):
                    break
                self.logger.warning(
                    "Retrying action",
                    action_id=node.node_id,
                    attempt=attempt + 1,
                    max_attempts=attempts,
                )
                if isinstance(node, MultiStepActionNode):
                    node._prepare_run()  # a fresh, still master-managed run
            try:
                result = await _within(
                    node.execute(proposal),
                    node.timeout_s,
                    stop_requested=lambda: node._cancel_requested,
                )
            except asyncio.TimeoutError:
                # Note: ``asyncio.TimeoutError`` is aliased to the builtin
                # ``TimeoutError`` only on Python >= 3.11. On 3.10 the two are
                # distinct classes, so catch the asyncio one explicitly.
                result = await self._timed_out(node, proposal)
            except Exception as exc:  # noqa: BLE001
                # Spec contract: execute() must always return a Result, never
                # raise. If a user node violates that contract,
                # log loudly and convert to a failed Result rather than letting
                # the exception escape the task and lose the Result.
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
            if node._cancel_requested:
                # The master's cancel never reached a step boundary: it was
                # swallowed (Python 3.10 ``asyncio.wait_for``, or an
                # execute() that turned CancelledError into a Result). The
                # run still ends through the cancel path, with its safe-stop.
                raise asyncio.CancelledError()
            last_result = result
            # Never retry an interrupted/preempted run (the stop was requested,
            # not a fault) or a failed or unfinished rollback walk (e.g. one
            # the action timeout cut): the plant is in an unknown partial
            # state, and re-running from step 1 could compound it.
            if (
                result.success
                or result.interrupted
                or result.preempted
                or isinstance(result.error, RollbackError)
                or (isinstance(node, MultiStepActionNode) and node.walk_cut_short())
            ):
                break

        return last_result  # type: ignore[return-value]

    async def _timed_out(self, node: BaseActionNode, proposal: Proposal) -> Result:
        """``on_timeout()``, with a raising override turned into a failed Result."""
        try:
            return await node.on_timeout(proposal)
        except Exception as exc:  # noqa: BLE001
            self.logger.error(
                "on_timeout() raised",
                action_id=node.node_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            await self._report_fault(node.node_id, exc)
            error = ActionTimeoutError(node.node_id, None, node.timeout_s)
            error.__cause__ = exc
            return Result(action_id=node.node_id, success=False, error=error)

    def _retry_blocked(self, node: BaseActionNode) -> bool:
        """
        True when a failed run must not be retried because something wants the
        action stopped: a cancel or preemption, a pending interrupt or
        WAIT_FOR_BLOCK hand-off, or an armed reflex claiming its resources.
        Re-running from step 1 would take the actuator back from them.
        """
        if node._cancel_requested or node.node_id in self._preempted_by:
            return True
        if isinstance(node, MultiStepActionNode) and (
            node._reflex_preempt_by is not None or node._interrupt_requested
        ):
            return True
        return any(
            self._resources_of(armed_id) & node.resources for armed_id in self._armed
        )
