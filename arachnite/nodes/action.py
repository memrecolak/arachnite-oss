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
from enum import Enum
from typing import Protocol, runtime_checkable

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

    #: Upper bound on ``on_preempted()``. A safe-stop that overruns is
    #: abandoned (logged) so a hung hook cannot hold a reflex back forever.
    safe_stop_timeout_s: float = 1.0

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
        Returns ``(rolled_back, error)``.
        """
        error: BaseException | None = None
        try:
            await asyncio.wait_for(
                self.on_preempted(proposal, preempted_by),
                timeout=self.safe_stop_timeout_s,
            )
        except asyncio.TimeoutError:
            self.logger.error(
                "Safe-stop hook timed out",
                timeout_s=self.safe_stop_timeout_s,
            )
            error = ActionTimeoutError(
                self.node_id, "on_preempted", self.safe_stop_timeout_s,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.error(
                "Safe-stop hook raised",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            error = exc
        return False, error

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
        # True while an interrupt/abort rollback walk runs: a preemption
        # arriving then must not start the walk again (rollback_on_preempt),
        # or completed steps would be undone twice.
        self._unwinding: bool = False

    def _prepare_run(self) -> None:
        """Reset per-run state at reservation time (called by ActionMasterNode)."""
        self._interrupt_requested = False
        self._interrupt_request   = None
        self._reflex_preempt_by   = None
        self._completed_steps     = []
        self._current_step        = None
        self._unwinding           = False
        self._run_prepared        = True

    def request_reflex_preempt(self, by: Proposal) -> None:
        """Ask to be preempted at the first step boundary after the current block."""
        self._reflex_preempt_by = by

    def _clear_run_state(self) -> None:
        """Drop per-run state after a cancelled run so nothing looks in-flight."""
        self._current_step      = None
        self._reflex_preempt_by = None
        self._unwinding         = False
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
        self._run_prepared = False

        all_steps  = self.steps()
        start_time = time.monotonic()

        for i, step in enumerate(all_steps):
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
            # Not reset in a finally: if a preemption cancels the walk, the
            # flag must still be set when the safe-stop runs.
            self._unwinding = True
            try:
                await self.on_interrupted(self._completed_steps.copy(), pending, proposal)
                rolled_back = True
            except RollbackError as exc:
                error = exc
            self._unwinding = False

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
        and so does a preemption that cut into a rollback walk already in
        progress — restarting it would undo completed steps twice.
        """
        rolled_back, error = await super()._run_safe_stop(proposal, preempted_by)
        if self.rollback_on_preempt and allow_rollback and not self._unwinding:
            try:
                await self._rollback_completed(self._completed_steps.copy())
                rolled_back = True
            except RollbackError as exc:
                error = error or exc
        return rolled_back, error

    async def _handle_reflex_preempt(
        self,
        proposal: Proposal,
        preempted_by: Proposal,
        start_time: float,
    ) -> Result:
        """Hand the resources to a waiting reflex at a step boundary."""
        stopped_at = self._current_step.name if self._current_step else None
        self.logger.info(
            "Action preempted by reflex at step boundary",
            stopped_at=stopped_at,
            reflex=preempted_by.instinct_id,
        )
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
        self._unwinding = True  # see _handle_interrupt: not reset on cancel
        try:
            rolled_back = await self.on_aborted(
                self._completed_steps[:-1], step, proposal,
            )
        except RollbackError as exc:
            error = exc
        self._unwinding = False

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
        # resource -> armed reflex action_id waiting for it (blocks deliberation)
        self._claims: dict[str, str] = {}
        # reflex action_id -> its proposal, waiting for owners to release
        self._armed: dict[str, Proposal] = {}
        # running action_id -> reflex proposal that preempted it
        self._preempted_by: dict[str, Proposal] = {}
        self._tasks: dict[str, asyncio.Task[Result]] = {}
        self._buffered: set[asyncio.Task[Result]] = set()
        self._completed: list[Result] = []
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
        await asyncio.gather(*(n.setup() for n in self._nodes.values()))

    async def teardown(self) -> None:
        await self.cancel_all(timeout=self.safe_stop_budget_s())
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
        """Armed reflexes (other than ``action_id``) claiming its resources."""
        claimants: set[str] = set()
        for resource in self._resources_of(action_id):
            claimant = self._claims.get(resource)
            if claimant is not None and claimant != action_id:
                claimants.add(claimant)
        if action_id in self._armed:
            claimants.add(action_id)
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
                self._completed.append(Result(
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
        if action_id in self._armed or self._origins.get(action_id) == "reflex":
            return LaunchOutcome(LaunchStatus.DEDUPED, action_id)

        owners = self._running_conflicts(action_id)
        claimants = self._claim_conflicts(action_id)
        if not owners and not claimants:
            self._start(node, proposal, "reflex", buffer=True)
            return LaunchOutcome(LaunchStatus.LAUNCHED, action_id)

        self._armed[action_id] = proposal
        for resource in node.resources:
            self._claims.setdefault(resource, action_id)
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
        task.cancel()

    def _admit_armed(self) -> None:
        """Start every armed reflex whose owners have all released."""
        for action_id, proposal in sorted(
            self._armed.items(), key=lambda kv: kv[1].priority, reverse=True,
        ):
            if self._running_conflicts(action_id):
                continue
            del self._armed[action_id]
            for resource, claimant in list(self._claims.items()):
                if claimant == action_id:
                    del self._claims[resource]
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
        """Task body: execute with retries; on cancellation run the safe-stop."""
        action_id = node.node_id
        start = time.monotonic()
        try:
            try:
                result = await self._execute_with_retry(node, proposal)
            except asyncio.CancelledError:
                by = self._preempted_by.get(action_id)
                preempted = by is not None
                # Shutdown / emergency_stop cancels run the safe-stop only;
                # rollback_on_preempt applies to reflex preemption.
                rolled_back, error = await node._run_safe_stop(
                    proposal,
                    by if by is not None else _SHUTDOWN_PROPOSAL,
                    allow_rollback=preempted,
                )
                stopped_at: str | None = None
                step_results: list[StepResult] = []
                if isinstance(node, MultiStepActionNode):
                    if node._current_step is not None:
                        stopped_at = node._current_step.name
                    step_results = node._completed_steps.copy()
                    node._clear_run_state()
                if not preempted:
                    self.logger.info(
                        "Action cancelled; safe-stop complete",
                        action_id=action_id,
                        stopped_at=stopped_at,
                    )
                    raise  # shutdown / external cancel: safe-stop done, propagate
                current = asyncio.current_task()
                if current is not None and hasattr(current, "uncancel"):
                    current.uncancel()  # Python 3.11+: the cancel was consumed
                result = Result(
                    action_id       = action_id,
                    success         = False,
                    error           = error,
                    interrupted     = True,
                    preempted       = True,
                    stopped_at_step = stopped_at,
                    step_results    = step_results,
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
            self._release(action_id)

        self.logger.info(
            "Action complete",
            action_id=action_id,
            success=result.success,
            preempted=result.preempted,
            duration_ms=round(result.duration_s * 1000, 2),
        )
        return result

    def _release(self, action_id: str) -> None:
        """Free the action's reservation and resources, then admit armed reflexes."""
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
                self._completed.append(task.result())
        if not self._running_nodes and not self._armed:
            self._idle.set()

    def _release_unstarted(self, task: asyncio.Task[Result], *, buffered: bool) -> None:
        """
        Release an action whose task was cancelled before its first step.

        A cancel that lands before the task body starts never enters
        ``_run``, so its ``finally`` cannot release the reservation. The
        action commanded nothing, so no safe-stop is due; a preemption is
        still reported as a preempted Result.
        """
        action_id = next(
            (aid for aid, t in self._tasks.items() if t is task), None,
        )
        if action_id is None:
            return  # the body ran and released already
        preempted = action_id in self._preempted_by
        node = self._running_nodes.get(action_id)
        if isinstance(node, MultiStepActionNode):
            node._clear_run_state()
        self.logger.info(
            "Action cancelled before it started",
            action_id=action_id,
            preempted=preempted,
        )
        if buffered and preempted:
            self._completed.append(Result(
                action_id   = action_id,
                success     = False,
                interrupted = True,
                preempted   = True,
            ))
        self._release(action_id)

    def drain_completed(self) -> list[Result]:
        """Return (and clear) the Results of launched actions finished so far."""
        completed, self._completed = self._completed, []
        return completed

    async def wait_idle(self, timeout: float | None = None) -> bool:
        """
        Wait until no action is running or armed. Returns False on timeout.
        Results of the finished launches are then available from
        ``drain_completed()``.
        """
        if not self._running_nodes and not self._armed:
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
        """Cancel one running action (safe-stop runs) and wait for it."""
        self._armed.pop(action_id, None)
        for resource, claimant in list(self._claims.items()):
            if claimant == action_id:
                del self._claims[resource]
        task = self._tasks.get(action_id)
        if task is None or task.done():
            return
        task.cancel()
        await asyncio.wait({task}, timeout=timeout)

    async def cancel_all(self, timeout: float | None = None) -> None:
        """
        Cancel every running action — each runs its safe-stop, no rollback —
        and drop armed reflexes. Used by shutdown and emergency_stop.
        """
        self._armed.clear()
        self._claims.clear()
        tasks = [t for t in self._tasks.values() if not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=timeout)
        if not self._running_nodes:
            self._idle.set()

    def safe_stop_budget_s(self) -> float:
        """Time to allow cancelled actions to finish their safe-stop."""
        budgets = [n.safe_stop_timeout_s for n in self._running_nodes.values()]
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

        task = self._start(node, proposal, origin or "direct", buffer=False)
        return await task

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
