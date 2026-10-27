"""Unit tests for non-blocking dispatch, actuator resources, and reflex arbitration (ADR 0005)."""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from collections.abc import Callable

import pytest

from arachnite import BaseSenseNode, RuntimeBuilder, ShutdownPhase, Signal, SignalBus
from arachnite.exceptions import (
    ActionNotFoundError,
    ActionTimeoutError,
    NodeRegistrationError,
    ResourceConflictError,
    RollbackError,
    StepAbortError,
)
from arachnite.logging import BaseLogSink
from arachnite.models import (
    ActionStep,
    InterruptPolicy,
    InterruptRequest,
    LogEvent,
    LogLevel,
    Proposal,
    ReflexPreemption,
    Result,
    StepResult,
)
from arachnite.nodes.action import (
    ActionMasterNode,
    BaseActionNode,
    GuardVerdict,
    LaunchStatus,
    MultiStepActionNode,
)

# ── Instrumented actuator ─────────────────────────────────────────────────────

class Actuator:
    """Records who commands it; flags two writers in motion at once."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []   # (what, who)
        self.in_motion: set[str] = set()
        self.overlaps: list[tuple[str, frozenset[str]]] = []

    async def move(self, who: str, duration_s: float) -> None:
        if self.in_motion:
            self.overlaps.append((who, frozenset(self.in_motion)))
        self.in_motion.add(who)
        self.events.append(("begin", who))
        try:
            await asyncio.sleep(duration_s)
        finally:
            self.in_motion.discard(who)
            self.events.append(("end", who))


# ── Nodes ─────────────────────────────────────────────────────────────────────

STEP_S = 0.05


class PickPlace(MultiStepActionNode):
    """approach → [lower, close, raise] (mandatory) → transport → release."""
    node_id   = "PickPlace"
    resources = frozenset({"arm"})
    timeout_s = 10.0

    def __init__(self, bus: SignalBus, actuator: Actuator) -> None:
        super().__init__(bus)
        self.actuator = actuator
        self.executed: list[str] = []
        self.undone: list[str] = []
        self.safe_stop_at: list[str | None] = []
        self.stopped_by: list[Proposal] = []
        self.order: list[str] | None = None

    def _undo(self, name: str):  # type: ignore[no-untyped-def]
        async def _rollback() -> None:
            self.undone.append(name)
        return _rollback

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("approach",  interruptible=True),
            ActionStep("lower",     interruptible=False, rollback=self._undo("lower")),
            ActionStep("close",     interruptible=False, rollback=self._undo("close")),
            ActionStep("raise",     interruptible=False, rollback=self._undo("raise")),
            ActionStep("transport", interruptible=True),
            ActionStep("release",   interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        self.executed.append(step.name)
        await self.actuator.move(self.node_id, STEP_S)
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.safe_stop_at.append(self._current_step.name if self._current_step else None)
        self.stopped_by.append(preempted_by)
        if self.order is not None:
            self.order.append("safe_stop")


class Retract(BaseActionNode):
    """The reflex action: one short motion on the same actuator."""
    node_id   = "Retract"
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus, actuator: Actuator) -> None:
        super().__init__(bus)
        self.actuator = actuator
        self.entered_at: float | None = None
        self.order: list[str] | None = None

    async def execute(self, proposal: Proposal) -> Result:
        self.entered_at = time.perf_counter()
        if self.order is not None:
            self.order.append("reflex_execute")
        await self.actuator.move(self.node_id, 0.01)
        return Result(action_id=self.node_id, success=True)


class Beep(BaseActionNode):
    """Uses a disjoint resource."""
    node_id   = "Beep"
    resources = frozenset({"buzzer"})

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.entered_at: float | None = None

    async def execute(self, proposal: Proposal) -> Result:
        self.entered_at = time.perf_counter()
        return Result(action_id=self.node_id, success=True)


class MoveArm(BaseActionNode):
    """A deliberative action on the arm."""
    node_id   = "MoveArm"
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus, actuator: Actuator) -> None:
        super().__init__(bus)
        self.actuator = actuator
        self.calls = 0

    async def execute(self, proposal: Proposal) -> Result:
        self.calls += 1
        await self.actuator.move(self.node_id, 0.01)
        return Result(action_id=self.node_id, success=True)


class Gated(BaseActionNode):
    """Holds its resources until the test releases it (``hold=False``: at once)."""

    def __init__(
        self,
        bus: SignalBus,
        node_id: str,
        resources: set[str],
        *,
        hold: bool = True,
        log: list[str] | None = None,
    ) -> None:
        self.node_id = node_id  # type: ignore[misc]
        self.resources = frozenset(resources)  # type: ignore[misc]
        super().__init__(bus)
        self.timeout_s = 10.0
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        if not hold:
            self.release.set()
        self.log = log

    async def execute(self, proposal: Proposal) -> Result:
        if self.log is not None:
            self.log.append(self.node_id)
        self.started.set()
        await self.release.wait()
        return Result(action_id=self.node_id, success=True)


def _gated(
    am: ActionMasterNode,
    node_id: str,
    resources: set[str],
    *,
    hold: bool = True,
    log: list[str] | None = None,
) -> Gated:
    node = Gated(am.bus, node_id, resources, hold=hold, log=log)
    am.register(node)
    return node


def _p(action_id: str, priority: int = 50, instinct_id: str = "inst") -> Proposal:
    return Proposal(instinct_id=instinct_id, action_id=action_id,
                    priority=priority, urgency=0.5)


def _master(
    pick_cls: type[PickPlace] = PickPlace,
) -> tuple[ActionMasterNode, Actuator, PickPlace, Retract]:
    bus = SignalBus()
    am = ActionMasterNode(bus=bus)
    actuator = Actuator()
    pick = pick_cls(bus, actuator)
    retract = Retract(bus, actuator)
    am.register(pick)
    am.register(retract)
    return am, actuator, pick, retract


async def _until(predicate, timeout: float = 2.0) -> None:  # type: ignore[no-untyped-def]
    deadline = time.perf_counter() + timeout
    while not predicate():
        if time.perf_counter() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.002)


def _results_by_id(am: ActionMasterNode) -> dict[str, Result]:
    return {r.action_id: r for r in am.drain_completed()}


# ── Non-blocking launch ───────────────────────────────────────────────────────

class TestLaunch:
    @pytest.mark.asyncio
    async def test_launch_returns_before_action_finishes(self) -> None:
        am, _, pick, _ = _master()
        outcome = am.launch(_p("PickPlace"))
        assert outcome.status is LaunchStatus.LAUNCHED
        assert am.running_action_ids() == {"PickPlace"}
        assert pick.executed == []                     # not even started yet
        assert await am.wait_idle(2.0)
        assert len(pick.executed) == 6
        assert _results_by_id(am)["PickPlace"].success is True

    @pytest.mark.asyncio
    async def test_unknown_action_not_found(self) -> None:
        am, _, _, _ = _master()
        assert am.launch(_p("Nope")).status is LaunchStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_guard_veto_buffered_and_reflex_bypasses(self) -> None:
        am, _, _, retract = _master()
        am.register_guard(lambda p: GuardVerdict(allow=False, reason="no"))
        assert am.launch(_p("Retract")).status is LaunchStatus.VETOED
        assert am.launch_reflex(_p("Retract", 250)).status is LaunchStatus.LAUNCHED
        assert await am.wait_idle(1.0)
        results = am.drain_completed()
        assert [r.success for r in results] == [False, True]
        assert retract.entered_at is not None

    @pytest.mark.asyncio
    async def test_interrupt_before_execute_starts_is_kept(self) -> None:
        am, _, pick, _ = _master()
        am.launch(_p("PickPlace"))
        # Task created but execute() not entered yet: the reservation already
        # reset the run state, so this interrupt must survive execute()'s start.
        pick.request_interrupt(InterruptRequest(
            new_proposal=_p("Other"), requesting_instinct_id="t",
        ))
        assert await am.wait_idle(2.0)
        result = _results_by_id(am)["PickPlace"]
        assert result.interrupted is True
        assert pick.executed == []                     # stopped at the first boundary

    @pytest.mark.asyncio
    async def test_strict_resources_rejects_undeclared_action(self) -> None:
        bus = SignalBus()
        am = ActionMasterNode(bus=bus, strict_resources=True)

        class Bare(BaseActionNode):
            node_id = "Bare"

            async def execute(self, proposal: Proposal) -> Result:
                return Result(action_id=self.node_id, success=True)

        with pytest.raises(NodeRegistrationError, match="declares no resources"):
            am.register(Bare(bus))


# ── Actuator exclusivity for deliberative launches ────────────────────────────

class TestResourceExclusivity:
    @pytest.mark.asyncio
    async def test_deliberative_launch_blocked_while_resource_owned(self) -> None:
        am, actuator, _, _ = _master()
        move = MoveArm(am.bus, actuator)
        am.register(move)
        am.launch(_p("PickPlace"))
        outcome = am.launch(_p("MoveArm", 90))
        assert outcome.status is LaunchStatus.BLOCKED
        assert outcome.owners == frozenset({"PickPlace"})
        assert await am.wait_idle(2.0)
        assert move.calls == 0
        assert actuator.overlaps == []

    @pytest.mark.asyncio
    async def test_dispatch_reports_resource_conflict(self) -> None:
        am, actuator, _, _ = _master()
        am.register(MoveArm(am.bus, actuator))
        am.launch(_p("PickPlace"))
        result = await am.dispatch(_p("MoveArm"))
        assert isinstance(result.error, ResourceConflictError)
        assert result.error.owners == ["PickPlace"]
        await am.wait_idle(2.0)

    @pytest.mark.asyncio
    async def test_disjoint_resources_run_concurrently(self) -> None:
        am, _, pick, _ = _master()
        beep = Beep(am.bus)
        am.register(beep)
        am.launch(_p("PickPlace"))
        await _until(lambda: "lower" in pick.executed)
        assert am.launch(_p("Beep")).status is LaunchStatus.LAUNCHED
        await _until(lambda: beep.entered_at is not None)
        assert am.running_action_ids() >= {"PickPlace"}     # still mid-block
        assert await am.wait_idle(2.0)
        assert len(pick.executed) == 6


# ── Reflex arbitration ────────────────────────────────────────────────────────

class TestReflexPreempt:
    @pytest.mark.asyncio
    async def test_preempt_cuts_into_mandatory_block_after_safe_stop(self) -> None:
        am, actuator, pick, retract = _master()
        order: list[str] = []
        pick.order = order
        retract.order = order
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["close"])   # inside the block

        outcome = am.launch_reflex(_p("Retract", 250, "CollisionReflex"))
        assert outcome.status is LaunchStatus.ARMED
        assert outcome.owners == frozenset({"PickPlace"})
        assert await am.wait_idle(2.0)

        results = _results_by_id(am)
        owner = results["PickPlace"]
        assert owner.preempted is True and owner.interrupted is True
        assert owner.stopped_at_step == "close"
        assert owner.rolled_back is False and pick.undone == []   # no rollback by default
        assert "raise" not in pick.executed                       # block was cut
        assert pick.safe_stop_at == ["close"]
        assert order == ["safe_stop", "reflex_execute"]          # ordering guarantee
        assert results["Retract"].success is True
        assert actuator.overlaps == []                            # exclusivity

    @pytest.mark.asyncio
    async def test_preempt_with_rollback_on_preempt(self) -> None:
        am, _, pick, _ = _master()
        pick.rollback_on_preempt = True
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["close"])
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        owner = _results_by_id(am)["PickPlace"]
        assert owner.rolled_back is True
        assert pick.undone == ["lower"]      # only completed mandatory steps

    @pytest.mark.asyncio
    async def test_reflex_latency_is_independent_of_remaining_action(self) -> None:
        am, _, pick, retract = _master()
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["lower"])
        fired = time.perf_counter()
        am.launch_reflex(_p("Retract", 250))
        await _until(lambda: retract.entered_at is not None)
        assert retract.entered_at is not None
        # Far below the ~4 steps (0.2 s) the action had left to run.
        assert retract.entered_at - fired < STEP_S
        await am.wait_idle(2.0)

    @pytest.mark.asyncio
    async def test_safe_stop_timeout_does_not_hold_reflex_back(self) -> None:
        am, _, pick, retract = _master()
        pick.safe_stop_timeout_s = 0.05

        async def hung(proposal: Proposal, preempted_by: Proposal) -> None:
            await asyncio.sleep(10.0)

        pick.on_preempted = hung  # type: ignore[method-assign]
        am.launch(_p("PickPlace"))
        await _until(lambda: "lower" in pick.executed)
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        results = _results_by_id(am)
        assert isinstance(results["PickPlace"].error, ActionTimeoutError)
        assert results["Retract"].success is True


class TestReflexWaitForBlock:
    @pytest.mark.asyncio
    async def test_wait_for_block_lets_block_finish_then_preempts(self) -> None:
        am, actuator, pick, retract = _master()
        order: list[str] = []
        pick.order = order
        retract.order = order
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["lower"])

        outcome = am.launch_reflex(
            _p("Retract", 250), ReflexPreemption.WAIT_FOR_BLOCK,
        )
        assert outcome.status is LaunchStatus.ARMED
        assert await am.wait_idle(2.0)

        owner = _results_by_id(am)["PickPlace"]
        assert pick.executed == ["approach", "lower", "close", "raise"]  # block completed
        assert owner.preempted is True
        assert owner.stopped_at_step == "transport"   # first boundary after the block
        assert order == ["safe_stop", "reflex_execute"]
        assert actuator.overlaps == []

    @pytest.mark.asyncio
    async def test_wait_for_block_outside_block_preempts_immediately(self) -> None:
        am, _, pick, _ = _master()
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed == ["approach"])
        am.launch_reflex(_p("Retract", 250), ReflexPreemption.WAIT_FOR_BLOCK)
        assert await am.wait_idle(2.0)
        owner = _results_by_id(am)["PickPlace"]
        assert owner.stopped_at_step == "approach"
        assert pick.executed == ["approach"]

    @pytest.mark.asyncio
    async def test_wait_for_block_ignores_never_policy(self) -> None:
        """Policies govern deliberation; a waiting reflex still gets the resource."""
        am, _, pick, _ = _master()
        pick.interrupt_policy = InterruptPolicy.NEVER
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["lower"])
        am.launch_reflex(_p("Retract", 250), ReflexPreemption.WAIT_FOR_BLOCK)
        assert await am.wait_idle(2.0)
        assert _results_by_id(am)["PickPlace"].stopped_at_step == "transport"

    @pytest.mark.asyncio
    async def test_hand_off_result_is_a_preempted_failure(self) -> None:
        """The owner handing over after its block reports success=False, interrupted, preempted"""
        am, _, pick, _ = _master()
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["lower"])
        am.launch_reflex(_p("Retract", 250), ReflexPreemption.WAIT_FOR_BLOCK)
        assert await am.wait_idle(2.0)
        owner = _results_by_id(am)["PickPlace"]
        assert owner.success is False
        assert owner.interrupted is True
        assert owner.preempted is True

    @pytest.mark.asyncio
    async def test_hand_off_honours_rollback_on_preempt(self) -> None:
        """rollback_on_preempt also undoes the block on a WAIT_FOR_BLOCK hand-off"""
        am, _, pick, _ = _master()
        pick.rollback_on_preempt = True
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["lower"])
        am.launch_reflex(_p("Retract", 250), ReflexPreemption.WAIT_FOR_BLOCK)
        assert await am.wait_idle(2.0)
        assert _results_by_id(am)["PickPlace"].rolled_back is True
        assert pick.undone == ["raise", "close", "lower"]


class TestReflexOrdering:
    @pytest.mark.asyncio
    async def test_armed_reflex_claim_blocks_deliberation(self) -> None:
        am, actuator, pick, _ = _master()
        move = MoveArm(am.bus, actuator)
        am.register(move)
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["lower"])
        am.launch_reflex(_p("Retract", 250), ReflexPreemption.WAIT_FOR_BLOCK)
        # While the reflex waits for the block, deliberation cannot take the arm.
        blocked = am.launch(_p("MoveArm", 199))
        assert blocked.status is LaunchStatus.BLOCKED
        assert "Retract" in blocked.owners
        assert await am.wait_idle(2.0)
        assert move.calls == 0
        assert actuator.overlaps == []

    @pytest.mark.asyncio
    async def test_reflex_never_preempts_running_reflex(self) -> None:
        am, actuator, _, retract = _master()

        class Brace(Retract):
            node_id = "Brace"

        brace = Brace(am.bus, actuator)
        am.register(brace)
        am.launch_reflex(_p("Retract", 250))
        outcome = am.launch_reflex(_p("Brace", 260))
        assert outcome.status is LaunchStatus.ARMED
        assert await am.wait_idle(2.0)
        results = _results_by_id(am)
        assert results["Retract"].preempted is False
        assert results["Brace"].success is True
        assert actuator.overlaps == []

    @pytest.mark.asyncio
    async def test_reflex_already_running_is_deduplicated(self) -> None:
        am, _, _, _ = _master()
        am.launch_reflex(_p("Retract", 250))
        assert am.launch_reflex(_p("Retract", 250)).status is LaunchStatus.DEDUPED
        await am.wait_idle(1.0)


class TestExecutionStateBlock:
    def test_remaining_mandatory_counts_only_the_current_block(self) -> None:
        bus = SignalBus()
        pick = PickPlace(bus, Actuator())
        pick._current_step = pick.steps()[2]                 # "close"
        assert pick.execution_state().mandatory_block_remaining_s == pytest.approx(
            2 * pick.timeout_s,                               # close + raise
        )
        pick._current_step = pick.steps()[4]                 # "transport"
        assert pick.execution_state().mandatory_block_remaining_s == 0.0


class _SlowUndoPick(MultiStepActionNode):
    """ROLLBACK action whose rollback callables are slow (preemptable)."""
    node_id             = "_SlowUndoPick"
    interrupt_policy    = InterruptPolicy.ROLLBACK
    rollback_on_preempt = True
    resources           = frozenset({"arm"})
    timeout_s           = 10.0

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.undone: list[str] = []
        self.walk_started = False

    def _undo(self, name: str):  # type: ignore[no-untyped-def]
        async def _rollback() -> None:
            self.walk_started = True
            await asyncio.sleep(0.05)
            self.undone.append(name)
        return _rollback

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("lower", interruptible=False, rollback=self._undo("lower")),
            ActionStep("close", interruptible=False, rollback=self._undo("close")),
            ActionStep("carry", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "close":
            self.request_interrupt(InterruptRequest(
                new_proposal=_p("Other"), requesting_instinct_id="t",
            ))
        await asyncio.sleep(0.01)
        return StepResult(step_name=step.name, success=True)


class TestPreemptDuringRollback:
    @pytest.mark.asyncio
    async def test_preempting_a_rollback_walk_does_not_undo_twice(self) -> None:
        """rollback_on_preempt must not restart a walk already in progress."""
        am, actuator, _, _ = _master()
        pick = _SlowUndoPick(am.bus)
        am.register(pick)
        am.launch(_p("_SlowUndoPick"))
        await _until(lambda: pick.walk_started)      # deliberative walk running
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        assert len(pick.undone) == len(set(pick.undone))   # nothing undone twice
        assert pick.undone in ([], ["close"])              # walk cut short, not restarted
        assert _results_by_id(am)["_SlowUndoPick"].preempted is True


class TestCancelledBeforeStart:
    """A cancel that lands before the task's first step must still release."""

    @pytest.mark.asyncio
    async def test_reflex_preempting_an_unstarted_owner_runs(self) -> None:
        bus = SignalBus()
        am = ActionMasterNode(bus=bus)
        actuator = Actuator()
        move = MoveArm(bus, actuator)
        retract = Retract(bus, actuator)
        am.register(move)
        am.register(retract)
        # Same synchronous span: MoveArm's task has not taken a step yet.
        assert am.launch(_p("MoveArm")).status is LaunchStatus.LAUNCHED
        assert am.launch_reflex(_p("Retract", 250)).status is LaunchStatus.ARMED
        assert await am.wait_idle(1.0)
        assert am.resource_owners() == {}
        assert am.armed_reflexes() == {}
        assert move.calls == 0
        assert retract.entered_at is not None
        results = _results_by_id(am)
        assert results["MoveArm"].preempted is True
        assert results["MoveArm"].success is False
        assert results["Retract"].success is True

    @pytest.mark.asyncio
    async def test_cancel_before_start_releases_the_resource(self) -> None:
        bus = SignalBus()
        am = ActionMasterNode(bus=bus)
        am.register(MoveArm(bus, Actuator()))
        am.launch(_p("MoveArm"))
        await am.cancel("MoveArm", timeout=0.5)
        assert am.running_action_ids() == set()
        assert am.resource_owners() == {}
        assert await am.wait_idle(0.5)


# ── Mutation-evidence hardening (v0.13) ───────────────────────────────────────

class _BrokenBrakePick(PickPlace):
    """PickPlace whose safe-stop hook raises."""
    node_id = "PickPlace"

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        raise RuntimeError("brake fault")


class _JammedUndoPick(PickPlace):
    """PickPlace that rolls back on preemption, but undoing 'lower' fails."""
    node_id             = "PickPlace"
    rollback_on_preempt = True

    def _undo(self, name: str):  # type: ignore[no-untyped-def]
        record = super()._undo(name)

        async def _rollback() -> None:
            await record()
            if name == "lower":
                raise RuntimeError("undo failed")
        return _rollback


class TestSafeStopReporting:
    @pytest.mark.asyncio
    async def test_safe_stop_receives_the_preempting_reflex_proposal(self) -> None:
        """on_preempted() is handed the reflex proposal that preempted the owner"""
        am, _, pick, _ = _master()
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["close"])
        reflex = _p("Retract", 250, "CollisionReflex")
        am.launch_reflex(reflex)
        assert await am.wait_idle(2.0)
        assert pick.stopped_by == [reflex]

    @pytest.mark.asyncio
    async def test_shutdown_safe_stop_receives_the_shutdown_proposal(self) -> None:
        """A shutdown cancel hands on_preempted() the shutdown proposal, never None"""
        am, _, pick, _ = _master()
        am.launch(_p("PickPlace"))
        await _until(lambda: "lower" in pick.executed)
        await am.cancel_all(timeout=2.0)
        assert len(pick.stopped_by) == 1
        by = pick.stopped_by[0]
        assert isinstance(by, Proposal) and by.action_id == "__shutdown__"

    @pytest.mark.asyncio
    async def test_preempted_owner_result_is_a_failure(self) -> None:
        """A reflex-preempted run is reported as success=False"""
        am, _, pick, _ = _master()
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["close"])
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        owner = _results_by_id(am)["PickPlace"]
        assert owner.preempted is True
        assert owner.success is False

    @pytest.mark.asyncio
    async def test_raising_safe_stop_is_reported_not_propagated(self) -> None:
        """A safe-stop hook that raises is reported in the Result; the reflex still runs"""
        am, _, pick, _ = _master(_BrokenBrakePick)
        am.launch(_p("PickPlace"))
        await _until(lambda: "lower" in pick.executed)
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        results = _results_by_id(am)
        assert results["PickPlace"].preempted is True
        assert isinstance(results["PickPlace"].error, RuntimeError)
        assert results["Retract"].success is True

    @pytest.mark.asyncio
    async def test_failed_preempt_rollback_is_reported(self) -> None:
        """A failed rollback_on_preempt walk is reported as a RollbackError"""
        am, _, pick, _ = _master(_JammedUndoPick)
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["close"])
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        owner = _results_by_id(am)["PickPlace"]
        assert owner.preempted is True
        assert isinstance(owner.error, RollbackError)
        assert owner.rolled_back is False
        assert pick.undone == ["lower"]


class TestDispatchBookkeeping:
    @pytest.mark.asyncio
    async def test_awaited_dispatch_result_is_not_buffered(self) -> None:
        """dispatch() returns its Result to the caller only, not via drain_completed()"""
        am, _, _, _ = _master()
        result = await am.dispatch(_p("Retract"))
        assert result.success is True
        assert am.drain_completed() == []

    @pytest.mark.asyncio
    async def test_dispatch_records_its_origin(self) -> None:
        """dispatch() records the given origin, and 'direct' when none is given"""
        am = ActionMasterNode(bus=SignalBus())
        hold = _gated(am, "Hold", {"arm"})
        task = asyncio.ensure_future(am.dispatch(_p("Hold", 250), origin="reflex"))
        await hold.started.wait()
        assert am.origin_of("Hold") == "reflex"
        hold.release.set()
        assert (await task).success is True

        hold.started.clear()
        hold.release.clear()
        task = asyncio.ensure_future(am.dispatch(_p("Hold")))
        await hold.started.wait()
        assert am.origin_of("Hold") == "direct"
        hold.release.set()
        assert (await task).success is True

    @pytest.mark.asyncio
    async def test_origin_is_keyword_only_and_proposal_accepts_a_keyword(self) -> None:
        """launch/dispatch/dispatch_many take proposal(s) by keyword; origin only by keyword"""
        am, _, _, _ = _master()
        assert am.launch(proposal=_p("Retract")).status is LaunchStatus.LAUNCHED
        assert await am.wait_idle(1.0)
        am.drain_completed()
        assert (await am.dispatch(proposal=_p("Retract"))).success is True
        results = await am.dispatch_many(proposals=[_p("Retract")])
        assert [r.success for r in results] == [True]
        with pytest.raises(TypeError):
            am.launch(_p("Retract"), "reflex")  # type: ignore[misc]
        with pytest.raises(TypeError):
            am.dispatch(_p("Retract"), "reflex")  # type: ignore[misc]
        with pytest.raises(TypeError):
            am.dispatch_many([_p("Retract")], "reflex")  # type: ignore[misc]

    @pytest.mark.asyncio
    async def test_launch_with_reflex_origin_bypasses_guards(self) -> None:
        """launch(origin='reflex') never consults the guards"""
        am, _, _, _ = _master()
        am.register_guard(lambda p: GuardVerdict(allow=False, reason="no"))
        assert am.launch(_p("Retract"), origin="reflex").status is LaunchStatus.LAUNCHED
        assert await am.wait_idle(1.0)
        assert _results_by_id(am)["Retract"].success is True


class TestClaimBookkeeping:
    @pytest.mark.asyncio
    async def test_self_preempting_reflex_keeps_the_resource_claimed(self) -> None:
        """A reflex preempting its own deliberative run still blocks the resource"""
        am = ActionMasterNode(bus=SignalBus())
        grip = _gated(am, "Grip", {"arm"})
        _gated(am, "MoveArm", {"arm"}, hold=False)
        _gated(am, "Retract", {"arm"}, hold=False)
        assert am.launch(_p("Grip")).status is LaunchStatus.LAUNCHED
        await grip.started.wait()
        assert am.launch_reflex(_p("Grip", 250)).status is LaunchStatus.ARMED
        # Same synchronous span: Grip is running (safe-stop pending) and armed.
        blocked = am.launch(_p("MoveArm", 90))
        assert blocked.status is LaunchStatus.BLOCKED
        assert blocked.owners == frozenset({"Grip"})
        second = am.launch_reflex(_p("Retract", 260))
        assert second.status is LaunchStatus.ARMED
        assert second.owners == frozenset({"Grip"})
        grip.release.set()
        assert await am.wait_idle(2.0)

    @pytest.mark.asyncio
    async def test_claims_released_after_the_reflex_runs(self) -> None:
        """An admitted reflex's claims are dropped, so deliberation can use the resource again"""
        am, actuator, pick, _ = _master()
        am.register(MoveArm(am.bus, actuator))
        am.launch(_p("PickPlace"))
        await _until(lambda: pick.executed[-1:] == ["close"])
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        assert am.conflicting_owners(_p("MoveArm")) == set()
        assert am.launch(_p("MoveArm")).status is LaunchStatus.LAUNCHED
        assert await am.wait_idle(2.0)

    @pytest.mark.asyncio
    async def test_admitting_one_reflex_keeps_other_armed_claims(self) -> None:
        """Admitting a reflex leaves still-armed reflexes armed and claiming"""
        am = ActionMasterNode(bus=SignalBus())
        deliberate = _gated(am, "Deliberate", {"arm"})
        hold = _gated(am, "ReflexHold", {"leg"})
        _gated(am, "ReflexA", {"leg", "wheel"}, hold=False)
        _gated(am, "ReflexZ", {"leg", "light"}, hold=False)
        reflex_m = _gated(am, "ReflexM", {"arm"}, hold=False)
        _gated(am, "WheelUser", {"wheel"}, hold=False)
        _gated(am, "LightUser", {"light"}, hold=False)
        am.launch(_p("Deliberate"))
        assert am.launch_reflex(_p("ReflexHold", 250)).status is LaunchStatus.LAUNCHED
        await deliberate.started.wait()
        await hold.started.wait()
        # ReflexA and ReflexZ wait for the running reflex; ReflexM preempts
        # Deliberate and is admitted while the other two stay armed.
        assert am.launch_reflex(_p("ReflexA", 250)).status is LaunchStatus.ARMED
        assert am.launch_reflex(_p("ReflexZ", 240)).status is LaunchStatus.ARMED
        assert am.launch_reflex(_p("ReflexM", 230)).status is LaunchStatus.ARMED
        await asyncio.wait_for(reflex_m.started.wait(), 1.0)
        assert set(am.armed_reflexes()) == {"ReflexA", "ReflexZ"}
        assert "ReflexA" in am.conflicting_owners(_p("WheelUser"))
        assert "ReflexZ" in am.conflicting_owners(_p("LightUser"))
        hold.release.set()
        assert await am.wait_idle(2.0)

    @pytest.mark.asyncio
    async def test_release_frees_only_the_finished_actions_resources(self) -> None:
        """A finishing action releases its own resources and no other action's"""
        am = ActionMasterNode(bus=SignalBus())
        alpha = _gated(am, "Alpha", {"arm"})
        bravo = _gated(am, "Bravo", {"leg"})
        charlie = _gated(am, "Charlie", {"wheel"})
        for node in (alpha, bravo, charlie):
            assert am.launch(_p(node.node_id)).status is LaunchStatus.LAUNCHED
        bravo.release.set()
        await _until(lambda: "Bravo" not in am.running_action_ids())
        assert am.resource_owners() == {"arm": "Alpha", "wheel": "Charlie"}
        alpha.release.set()
        charlie.release.set()
        assert await am.wait_idle(2.0)

    @pytest.mark.asyncio
    async def test_claim_on_a_free_resource_blocks_deliberation(self) -> None:
        """An armed reflex's claim on a free resource blocks any other action, whatever its name"""
        am = ActionMasterNode(bus=SignalBus())
        hold = _gated(am, "Hold", {"arm"})
        _gated(am, "Retract", {"arm", "gripper"}, hold=False)
        _gated(am, "Wiggle", {"gripper"}, hold=False)
        assert am.launch_reflex(_p("Hold", 250)).status is LaunchStatus.LAUNCHED
        assert am.launch_reflex(_p("Retract", 260)).status is LaunchStatus.ARMED
        outcome = am.launch(_p("Wiggle"))           # "Wiggle" sorts after "Retract"
        assert outcome.status is LaunchStatus.BLOCKED
        assert outcome.owners == frozenset({"Retract"})
        hold.release.set()
        assert await am.wait_idle(2.0)


async def _preempt_beside_running_reflex(round_no: int) -> None:
    """One round: a reflex whose owners are a running reflex plus eight
    deliberative actions must preempt every deliberative one."""
    am = ActionMasterNode(bus=SignalBus())
    hold = _gated(am, f"Hold{round_no}", {"leg"})
    owners = [_gated(am, f"D{round_no}_{i}", {f"r{round_no}_{i}"}) for i in range(8)]
    _gated(am, f"Big{round_no}", {"leg", *(f"r{round_no}_{i}" for i in range(8))}, hold=False)
    assert am.launch_reflex(_p(f"Hold{round_no}", 250)).status is LaunchStatus.LAUNCHED
    for node in owners:
        am.launch(_p(node.node_id))
    await asyncio.gather(*(node.started.wait() for node in owners))
    assert am.launch_reflex(_p(f"Big{round_no}", 260)).status is LaunchStatus.ARMED
    deliberative = {node.node_id for node in owners}
    await _until(lambda: not deliberative & am.running_action_ids(), 0.5)
    hold.release.set()
    assert await am.wait_idle(2.0)


class TestReflexAdmission:
    @pytest.mark.asyncio
    async def test_reflex_preempts_owner_with_a_custom_origin(self) -> None:
        """Only a reflex-origin owner is exempt from preemption; any other origin is preempted"""
        am = ActionMasterNode(bus=SignalBus())
        teleop = _gated(am, "Teleop", {"arm"})
        _gated(am, "Retract", {"arm"}, hold=False)
        assert am.launch(_p("Teleop"), origin="teleop").status is LaunchStatus.LAUNCHED
        await teleop.started.wait()
        assert am.launch_reflex(_p("Retract", 250)).status is LaunchStatus.ARMED
        assert await am.wait_idle(1.0)
        assert _results_by_id(am)["Teleop"].preempted is True

    @pytest.mark.asyncio
    async def test_higher_priority_armed_reflex_is_admitted_first(self) -> None:
        """Armed reflexes on a released resource start in priority order"""
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        deliberate = _gated(am, "Deliberate", {"arm"})
        _gated(am, "RetractLow", {"arm"}, hold=False, log=log)
        _gated(am, "BraceHigh", {"arm"}, hold=False, log=log)
        am.launch(_p("Deliberate"))
        await deliberate.started.wait()
        assert am.launch_reflex(_p("RetractLow", 210)).status is LaunchStatus.ARMED
        assert am.launch_reflex(_p("BraceHigh", 260)).status is LaunchStatus.ARMED
        assert await am.wait_idle(2.0)
        assert log == ["BraceHigh", "RetractLow"]

    @pytest.mark.asyncio
    async def test_reflex_preempts_every_deliberative_owner_beside_a_running_reflex(
        self,
    ) -> None:
        """A running reflex among the owners does not stop the other owners' preemption"""
        # Owner iteration order is hash-dependent: several rounds with fresh
        # names put the running reflex ahead of a deliberative owner.
        for round_no in range(8):
            await _preempt_beside_running_reflex(round_no)


# ── Reflex-path review regressions (docs/audits 2026-10-26) ───────────────────

class _Braking(Gated):
    """Gated action whose safe-stop takes ``stop_s`` and logs begin/end."""

    def __init__(
        self, bus: SignalBus, node_id: str, resources: set[str], log: list[str],
        stop_s: float = 0.0,
    ) -> None:
        super().__init__(bus, node_id, resources, log=log)
        self.events = log
        self.stop_s = stop_s

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.events.append(f"{self.node_id} brake begin")
        await asyncio.sleep(self.stop_s)
        self.events.append(f"{self.node_id} brake end")


class _BlockOwner(MultiStepActionNode):
    """s0 → [b1, b2] (mandatory, undoable) → s3; logs steps, safe-stops, undos."""
    node_id   = "Owner"
    resources = frozenset({"arm"})
    timeout_s = 10.0

    def __init__(self, bus: SignalBus, log: list[str], step_s: float = 0.05) -> None:
        super().__init__(bus)
        self.log = log
        self.step_s = step_s
        self.run = 0
        self.fail_b2_on_run: int | None = None
        self.brake_s = 0.0
        self.after_step: Callable[[str], None] | None = None

    def _undo(self, name: str):  # type: ignore[no-untyped-def]
        async def _rollback() -> None:
            self.log.append(f"undo {name}")
            await asyncio.sleep(self.step_s)
        return _rollback

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("s0"),
            ActionStep("b1", interruptible=False, rollback=self._undo("b1")),
            ActionStep("b2", interruptible=False, rollback=self._undo("b2")),
            ActionStep("s3"),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        if step.name == "s0":
            self.run += 1
        self.log.append(f"run{self.run} {step.name}")
        await asyncio.sleep(self.step_s)
        if self.after_step is not None:
            self.after_step(step.name)
        if step.name == "b2" and self.run == self.fail_b2_on_run:
            return StepResult(step_name="b2", success=False, abort_sequence=True)
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.log.append(f"brake begin ({preempted_by.action_id})")
        await asyncio.sleep(self.brake_s)
        self.log.append(f"brake end ({preempted_by.action_id})")


def _block_master(step_s: float = 0.05) -> tuple[ActionMasterNode, _BlockOwner, list[str]]:
    am = ActionMasterNode(bus=SignalBus())
    log: list[str] = []
    owner = _BlockOwner(am.bus, log, step_s)
    am.register(owner)
    for node_id in ("R_wait", "R_now"):
        _gated(am, node_id, {"arm"}, hold=False, log=log)
    return am, owner, log


class TestReviewRegressions:
    @pytest.mark.asyncio
    async def test_higher_priority_claim_survives_a_lower_reflex_admission(self) -> None:
        """R01: admitting a lower armed reflex keeps a higher armed reflex's claim and order"""
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        d_arm = _Braking(am.bus, "D_arm", {"arm"}, log)
        d_grip = _Braking(am.bus, "D_grip", {"gripper"}, log, stop_s=0.2)
        am.register(d_arm)
        am.register(d_grip)
        _gated(am, "R_low", {"arm"}, hold=False, log=log)
        _gated(am, "R_high", {"arm", "gripper"}, hold=False, log=log)
        _gated(am, "D3", {"arm"}, hold=False, log=log)
        am.launch(_p("D_arm"))
        am.launch(_p("D_grip"))
        await d_arm.started.wait()
        await d_grip.started.wait()
        assert am.launch_reflex(_p("R_low", 210)).status is LaunchStatus.ARMED
        assert am.launch_reflex(_p("R_high", 250)).status is LaunchStatus.ARMED
        await _until(lambda: "D_arm brake end" in log)
        # D_grip is still braking: R_high waits, R_low must not jump it, and
        # R_high's claim on the arm keeps deliberation off it.
        assert set(am.armed_reflexes()) == {"R_high", "R_low"}
        blocked = am.launch(_p("D3", 60))
        assert blocked.status is LaunchStatus.BLOCKED
        assert "R_high" in blocked.owners
        assert await am.wait_idle(2.0)
        assert "D3" not in log
        assert log.index("R_high") < log.index("R_low")

    @pytest.mark.asyncio
    async def test_preempt_landing_as_a_step_finishes_stops_the_owner(self) -> None:
        """R02: a preempt in the turn a step completes is not swallowed (Python 3.10 wait_for)"""
        am, owner, log = _block_master(step_s=0.02)

        def arm_reflex(step: str) -> None:
            if step == "s0":
                asyncio.get_running_loop().call_soon(
                    lambda: log.append(am.launch_reflex(_p("R_now", 250)).status.name)
                )

        owner.after_step = arm_reflex
        am.launch(_p("Owner"))
        assert await am.wait_idle(2.0)
        armed_at = log.index("ARMED")
        assert [e for e in log[armed_at:] if e.startswith("run")] == []
        assert _results_by_id(am)["Owner"].preempted is True

    @pytest.mark.asyncio
    async def test_preempt_during_a_hand_off_does_not_safe_stop_or_undo_twice(self) -> None:
        """R03: a PREEMPT reflex during a WAIT_FOR_BLOCK hand-off reuses the running safe-stop"""
        am, owner, log = _block_master()
        owner.rollback_on_preempt = True
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        am.launch_reflex(_p("R_wait", 240), ReflexPreemption.WAIT_FOR_BLOCK)
        await _until(lambda: "undo b2" in log)                 # hand-off walk running
        am.launch_reflex(_p("R_now", 250))
        assert await am.wait_idle(2.0)
        assert log.count("undo b1") == 1 and log.count("undo b2") == 1
        assert sum(e.startswith("brake begin") for e in log) == 1
        assert _results_by_id(am)["Owner"].preempted is True

    @pytest.mark.asyncio
    async def test_failed_run_is_not_retried_while_a_reflex_waits(self) -> None:
        """R04: a retry never restarts the owner while a reflex is armed on its resources"""
        am, owner, log = _block_master()
        owner.max_retries = 1
        owner.fail_b2_on_run = 1
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        am.launch_reflex(_p("R_wait", 240), ReflexPreemption.WAIT_FOR_BLOCK)
        assert await am.wait_idle(2.0)
        assert not any(e.startswith("run2") for e in log)
        assert "R_wait" in log

    @pytest.mark.asyncio
    async def test_hand_off_safe_stop_is_outside_the_action_timeout(self) -> None:
        """R05: the hand-off safe-stop is bounded by safe_stop_timeout_s, not timeout_s"""
        am, owner, log = _block_master(step_s=0.1)
        owner.timeout_s = 0.45            # s0+b1+b2 fit; the 0.3 s brake would not
        owner.safe_stop_timeout_s = 1.0
        owner.max_retries = 1
        owner.brake_s = 0.3
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        am.launch_reflex(_p("R_wait", 240), ReflexPreemption.WAIT_FOR_BLOCK)
        assert await am.wait_idle(3.0)
        assert "brake end (R_wait)" in log
        assert not any(e.startswith("run2") for e in log)
        result = _results_by_id(am)["Owner"]
        assert result.preempted is True and result.error is None

    @pytest.mark.asyncio
    async def test_cancel_all_lets_a_running_safe_stop_finish(self) -> None:
        """R06: cancel_all() (emergency_stop, shutdown) does not cut an owner's safe-stop"""
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        owner = _Braking(am.bus, "Owner", {"arm"}, log, stop_s=0.15)
        owner.safe_stop_timeout_s = 1.0
        am.register(owner)
        _gated(am, "Retract", {"arm"}, hold=False, log=log)
        am.launch(_p("Owner"))
        await owner.started.wait()
        am.launch_reflex(_p("Retract", 250))
        await _until(lambda: "Owner brake begin" in log)
        await am.cancel_all(timeout=2.0)
        assert "Owner brake end" in log
        assert log.count("Owner brake begin") == 1
        assert _results_by_id(am)["Owner"].preempted is True

    @pytest.mark.asyncio
    async def test_closed_master_refuses_new_work_until_setup(self) -> None:
        """cancel_all()/close() close the master: launches return CLOSED until setup()"""
        am, _, _, _ = _master()
        await am.cancel_all()
        assert am.launch(_p("Retract")).status is LaunchStatus.CLOSED
        assert am.launch_reflex(_p("Retract", 250)).status is LaunchStatus.CLOSED
        assert (await am.dispatch(_p("Retract"))).success is False
        await am.setup()
        assert am.launch(_p("Retract")).status is LaunchStatus.LAUNCHED
        assert await am.wait_idle(1.0)
        am.close()
        assert am.launch(_p("Retract")).status is LaunchStatus.CLOSED

    @pytest.mark.asyncio
    async def test_dispatch_preempted_before_start_returns_a_result(self) -> None:
        """R08: an awaited dispatch() preempted before its first step returns, not raises"""
        am = ActionMasterNode(bus=SignalBus())
        _gated(am, "Direct", {"arm"})
        _gated(am, "Retract", {"arm"}, hold=False)
        task = asyncio.ensure_future(am.dispatch(_p("Direct"), origin="direct"))
        await asyncio.sleep(0)                 # dispatch awaits the unstarted task
        assert am.launch_reflex(_p("Retract", 250)).status is LaunchStatus.ARMED
        result = await asyncio.wait_for(task, 1.0)
        assert result.preempted is True and result.success is False
        assert await am.wait_idle(1.0)

    @pytest.mark.asyncio
    async def test_cancelling_an_armed_reflex_withdraws_its_hand_off(self) -> None:
        """R11: cancel() of an armed WAIT_FOR_BLOCK reflex lets the owner run on"""
        am, _, log = _block_master()
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        am.launch_reflex(_p("R_wait", 240), ReflexPreemption.WAIT_FOR_BLOCK)
        await am.cancel("R_wait")
        assert am.armed_reflexes() == {}
        assert await am.wait_idle(2.0)
        result = _results_by_id(am)["Owner"]
        assert result.success is True and result.preempted is False
        assert "run1 s3" in log
        assert not any(e.startswith("brake") for e in log)


# ── Second review round (docs/audits 2026-10-26, findings A–I) ────────────────

class _ParkingOwner(BaseActionNode):
    """Moves the arm; on cancellation parks it (a 0.2 s motion), then re-raises."""
    node_id   = "Owner"
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus, actuator: Actuator, timeout_s: float) -> None:
        super().__init__(bus)
        self.actuator = actuator
        self.timeout_s = timeout_s
        self.parking = asyncio.Event()

    async def execute(self, proposal: Proposal) -> Result:
        try:
            await self.actuator.move(self.node_id, 10.0)
        except asyncio.CancelledError:
            self.parking.set()
            await self.actuator.move("Owner-park", 0.2)
            raise
        return Result(action_id=self.node_id, success=True)


class _Seq(MultiStepActionNode):
    """Four plain steps; s0 can be told to overrun the action timeout."""
    node_id   = "Seq"
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus, timeout_s: float) -> None:
        super().__init__(bus)
        self.timeout_s = timeout_s
        self.log: list[str] = []
        self.after_s0: Callable[[], None] | None = None

    def steps(self) -> list[ActionStep]:
        return [ActionStep(n, timeout_s=5.0) for n in ("s0", "s1", "s2", "s3")]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        self.log.append(step.name)
        await asyncio.sleep(0.02)
        if step.name == "s0" and self.after_s0 is not None:
            self.after_s0()
        return StepResult(step_name=step.name, success=True)


class TestSecondReviewRegressions:
    @pytest.mark.asyncio
    async def test_higher_reflex_on_free_resource_starts_past_a_lower_claim(self) -> None:
        """A: a higher reflex whose resources are free is not held behind a lower armed claim"""
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        holder = _gated(am, "Holder", {"a"})
        _gated(am, "R_low", {"a", "b"}, hold=False, log=log)
        high = _gated(am, "R_high", {"b"}, hold=False, log=log)
        assert am.launch_reflex(_p("Holder", 250)).status is LaunchStatus.LAUNCHED
        await holder.started.wait()
        assert am.launch_reflex(_p("R_low", 210)).status is LaunchStatus.ARMED
        assert am.launch_reflex(_p("R_high", 260)).status is LaunchStatus.LAUNCHED
        await asyncio.wait_for(high.started.wait(), 0.5)
        assert set(am.armed_reflexes()) == {"R_low"}
        holder.release.set()
        assert await am.wait_idle(2.0)
        assert log == ["R_high", "R_low"]

    @pytest.mark.asyncio
    async def test_runtime_less_execute_after_a_cancelled_run(self) -> None:
        """B: a cancelled run leaves no cancel flag behind for a later direct execute()"""
        am, owner, log = _block_master()
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        await am.cancel("Owner")
        assert owner._cancel_requested is False
        result = await owner.execute(_p("Owner"))
        assert result.success is True

    @pytest.mark.asyncio
    async def test_raising_on_timeout_still_yields_a_result(self) -> None:
        """C: an on_timeout() override that raises becomes a failed Result (launch and dispatch)"""

        class Slow(BaseActionNode):
            node_id = "Slow"
            timeout_s = 0.05

            async def execute(self, proposal: Proposal) -> Result:
                await asyncio.sleep(1.0)
                return Result(action_id=self.node_id, success=True)

            async def on_timeout(self, proposal: Proposal) -> Result:
                raise RuntimeError("cleanup failed")

        am = ActionMasterNode(bus=SignalBus())
        am.register(Slow(am.bus))
        am.launch(_p("Slow"))
        assert await am.wait_idle(2.0)
        launched = _results_by_id(am)["Slow"]
        dispatched = await am.dispatch(_p("Slow"))
        for result in (launched, dispatched):
            assert result.success is False
            assert isinstance(result.error, ActionTimeoutError)
            assert isinstance(result.error.__cause__, RuntimeError)

    @pytest.mark.asyncio
    async def test_cancellation_cleanup_finishes_before_the_release(self) -> None:
        """D: an action's cancellation clean-up is never orphaned; no second writer meanwhile"""
        actuator = Actuator()
        am = ActionMasterNode(bus=SignalBus())
        owner = _ParkingOwner(am.bus, actuator, timeout_s=0.1)
        am.register(owner)
        am.register(Retract(am.bus, actuator))
        am.launch(_p("Owner"))
        await owner.parking.wait()          # the timeout fired; the arm is parking
        await asyncio.sleep(0.05)
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        await am.cancel_all(timeout=2.0)
        assert actuator.overlaps == []
        assert actuator.in_motion == set()

    @pytest.mark.asyncio
    async def test_cancelled_dispatch_caller_stops_the_action(self) -> None:
        """E: cancelling the caller of an awaited dispatch() stops the action at once"""
        am = ActionMasterNode(bus=SignalBus())
        seq = _Seq(am.bus, timeout_s=5.0)
        am.register(seq)
        caller = asyncio.ensure_future(am.dispatch(_p("Seq")))
        loop = asyncio.get_running_loop()
        seq.after_s0 = lambda: loop.call_soon(caller.cancel)
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert await am.wait_idle(2.0)
        assert seq.log[:2] == ["s0", "s1"] and len(seq.log) <= 2
        assert am.resource_owners() == {}

    @pytest.mark.asyncio
    async def test_emergency_stop_after_a_preempt_skips_the_rollback(self) -> None:
        """F: a shutdown before the preempted owner's safe-stop starts runs no rollback walk"""
        am, owner, log = _block_master()
        owner.rollback_on_preempt = True
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b2" in log)
        am.launch_reflex(_p("R_now", 250))     # same synchronous span as the stop
        await am.cancel_all(timeout=2.0)
        assert not any(e.startswith("undo") for e in log)
        assert sum(e.startswith("brake begin") for e in log) == 1

    @pytest.mark.asyncio
    async def test_safe_stop_ending_in_cancelled_error_keeps_the_result(self) -> None:
        """G: a safe-stop hook that ends in CancelledError is reported, not lost"""
        am, actuator, pick, _ = _master()

        async def driver_cancelled(proposal: Proposal, preempted_by: Proposal) -> None:
            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            fut.cancel()
            await fut

        pick.on_preempted = driver_cancelled  # type: ignore[method-assign]
        am.launch(_p("PickPlace"))
        await _until(lambda: "lower" in pick.executed)
        am.launch_reflex(_p("Retract", 250))
        assert await am.wait_idle(2.0)
        results = _results_by_id(am)
        assert results["PickPlace"].preempted is True
        assert isinstance(results["PickPlace"].error, RuntimeError)
        assert results["Retract"].success is True

    @pytest.mark.asyncio
    async def test_cancel_of_an_armed_reflex_leaves_a_same_id_owner_running(self) -> None:
        """H: withdrawing a reflex armed under the owner's own id does not cancel the owner"""
        am, owner, log = _block_master()
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        # The same action as a reflex: armed under the running owner's id.
        assert am.launch_reflex(
            _p("Owner", 250), ReflexPreemption.WAIT_FOR_BLOCK,
        ).status is LaunchStatus.ARMED
        await am.cancel("Owner")
        assert am.armed_reflexes() == {}
        assert await am.wait_idle(2.0)
        result = _results_by_id(am)["Owner"]
        assert result.success is True
        assert "run1 s3" in log

    @pytest.mark.asyncio
    async def test_master_cancel_gives_an_awaited_dispatch_a_result(self) -> None:
        """I: cancel()/cancel_all() of a dispatched action returns an interrupted Result"""
        am = ActionMasterNode(bus=SignalBus())
        direct = _gated(am, "Direct", {"arm"})
        task = asyncio.ensure_future(am.dispatch(_p("Direct")))
        await direct.started.wait()
        await am.cancel_all(timeout=1.0)
        result = await task
        assert result.success is False and result.interrupted is True
        assert result.preempted is False

        am2 = ActionMasterNode(bus=SignalBus())
        launched = _gated(am2, "Direct", {"arm"})
        am2.launch(_p("Direct"))
        await launched.started.wait()
        await am2.cancel("Direct")
        assert [(r.action_id, r.interrupted) for r in am2.drain_completed()] == [
            ("Direct", True),
        ]


# ── Third review round (docs/audits 2026-10-26, action findings X1–X11) ───────

class _SignalledBrake(Gated):
    """Gated action whose safe-stop ends by setting ``brake_done``."""

    def __init__(self, bus: SignalBus, node_id: str, resources: set[str]) -> None:
        super().__init__(bus, node_id, resources)
        self.brake_done = asyncio.Event()
        self.brakes: list[str] = []

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.brakes.append(preempted_by.action_id)
        await asyncio.sleep(0.01)
        self.brake_done.set()


class _CaptureSink(BaseLogSink):
    def __init__(self) -> None:
        super().__init__(LogLevel.DEBUG)
        self.messages: list[str] = []

    async def emit(self, event: LogEvent) -> None:
        self.messages.append(event.message)


class TestThirdReviewRegressions:
    @pytest.mark.parametrize("how", ["cancel", "cancel_all"])
    @pytest.mark.parametrize("path", ["launch", "dispatch"])
    @pytest.mark.asyncio
    async def test_cancel_landing_as_the_safe_stop_ends_keeps_the_result(
        self, how: str, path: str,
    ) -> None:
        """X1: a master cancel in the turn a safe-stop finishes still yields the Result"""
        am = ActionMasterNode(bus=SignalBus())
        owner = _SignalledBrake(am.bus, "A", {"arm"})
        am.register(owner)
        _gated(am, "R", {"arm"}, hold=False)

        async def cancel_at_brake_end() -> None:
            await owner.brake_done.wait()
            await (am.cancel("A") if how == "cancel" else am.cancel_all())

        killer = asyncio.ensure_future(cancel_at_brake_end())
        if path == "launch":
            am.launch(_p("A"))
            await owner.started.wait()
            am.launch_reflex(_p("R", 250))
            await asyncio.wait_for(killer, 2.0)
            assert await am.wait_idle(2.0)
            result = _results_by_id(am)["A"]
        else:
            caller = asyncio.ensure_future(am.dispatch(_p("A")))
            await owner.started.wait()
            am.launch_reflex(_p("R", 250))
            await asyncio.wait_for(killer, 2.0)
            result = await caller
        assert result.interrupted is True and result.preempted is True
        assert owner.brakes == ["R"]

    @pytest.mark.asyncio
    async def test_preempt_landing_as_the_hand_off_safe_stop_ends(self) -> None:
        """X1: a PREEMPT reflex in the turn a hand-off safe-stop ends keeps the owner's Result"""
        am = ActionMasterNode(bus=SignalBus())

        class Owner(MultiStepActionNode):
            node_id = "O"
            resources = frozenset({"arm"})
            timeout_s = 10.0

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.in_block = asyncio.Event()
                self.go = asyncio.Event()
                self.brake_done = asyncio.Event()

            def steps(self) -> list[ActionStep]:
                return [ActionStep("a"), ActionStep("blk", interruptible=False),
                        ActionStep("c")]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                if step.name == "blk":
                    self.in_block.set()
                    await self.go.wait()
                return StepResult(step_name=step.name, success=True)

            async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
                await asyncio.sleep(0.01)
                self.brake_done.set()

        owner = Owner(am.bus)
        am.register(owner)
        _gated(am, "W", {"arm"}, hold=False)
        _gated(am, "PR", {"arm"}, hold=False)
        am.launch(_p("O"))
        await owner.in_block.wait()
        am.launch_reflex(_p("W", 220), ReflexPreemption.WAIT_FOR_BLOCK)

        async def preempt_at_brake_end() -> None:
            await owner.brake_done.wait()
            am.launch_reflex(_p("PR", 250))

        second = asyncio.ensure_future(preempt_at_brake_end())
        owner.go.set()
        await second
        assert await am.wait_idle(2.0)
        assert _results_by_id(am)["O"].preempted is True

    @pytest.mark.asyncio
    async def test_preempt_reaches_an_action_that_swallowed_the_cancel(self) -> None:
        """X3: an execute() that turns the cancel into a Result still gets its safe-stop"""

        class Swallower(BaseActionNode):
            node_id = "Swallower"
            resources = frozenset({"arm"})

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.started = asyncio.Event()
                self.brakes = 0

            async def execute(self, proposal: Proposal) -> Result:
                self.started.set()
                try:
                    await asyncio.sleep(5.0)
                except asyncio.CancelledError:
                    return Result(action_id=self.node_id, success=False)
                return Result(action_id=self.node_id, success=True)

            async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
                self.brakes += 1

        am = ActionMasterNode(bus=SignalBus())
        node = Swallower(am.bus)
        am.register(node)
        _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("Swallower"))
        await node.started.wait()
        am.launch_reflex(_p("R", 250))
        assert await am.wait_idle(2.0)
        assert node.brakes == 1
        assert _results_by_id(am)["Swallower"].preempted is True

    @pytest.mark.asyncio
    async def test_step_timeout_holds_when_the_step_swallows_its_cancel(self) -> None:
        """
        X4: a step whose inner wait_for swallows the timer's cancel still fails (timeout).
        On 3.11+ it is cut on time. On 3.10 a stdlib ``asyncio.wait_for`` can swallow
        the timer's one cancel (R4-A1: no re-cancel, so clean-up is never cut), so the
        step ends when it next returns — a documented 3.10 limitation.
        """

        class Stepper(MultiStepActionNode):
            node_id = "S"
            resources = frozenset({"arm"})
            timeout_s = 10.0

            def steps(self) -> list[ActionStep]:
                return [ActionStep("s1", timeout_s=0.1)]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                loop = asyncio.get_running_loop()
                ack: asyncio.Future[str] = loop.create_future()
                loop.call_at(loop.time() + 0.1, ack.set_result, "ack")
                await asyncio.wait_for(ack, 5.0)
                await asyncio.sleep(1.0)
                return StepResult(step_name=step.name, success=True)

        am = ActionMasterNode(bus=SignalBus())
        am.register(Stepper(am.bus))
        started = time.perf_counter()
        result = await am.dispatch(_p("S"))
        if sys.version_info >= (3, 11):
            assert time.perf_counter() - started < 0.5
        assert result.success is False

    @pytest.mark.asyncio
    async def test_twice_cancelled_dispatch_caller_raises_after_the_stop(self) -> None:
        """X7: a dispatch() caller cancelled twice raises only once the action has released"""
        am = ActionMasterNode(bus=SignalBus())
        owner = _Braking(am.bus, "A", {"arm"}, [], stop_s=0.1)
        owner.safe_stop_timeout_s = 1.0
        am.register(owner)
        caller = asyncio.ensure_future(am.dispatch(_p("A")))
        await owner.started.wait()
        caller.cancel()
        await asyncio.sleep(0.02)                       # the safe-stop is running
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert am.running_action_ids() == set()
        assert "A brake end" in owner.events

    @pytest.mark.asyncio
    async def test_no_false_safe_stop_error_is_logged(self) -> None:
        """X8: a normal preemption logs no 'Safe-stop hook ended with CancelledError'"""
        sink = _CaptureSink()
        am = ActionMasterNode(bus=SignalBus(), log_sinks=[sink])
        owner = _gated(am, "A", {"arm"})
        _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("A"))
        await owner.started.wait()
        am.launch_reflex(_p("R", 250))
        assert await am.wait_idle(2.0)
        await asyncio.sleep(0.01)
        assert "Action complete" in sink.messages
        assert "Safe-stop hook ended with CancelledError" not in sink.messages

    @pytest.mark.asyncio
    async def test_wait_idle_returns_with_the_result_buffered(self) -> None:
        """X9: once wait_idle() returns, drain_completed() has the finished Result"""
        finishing = asyncio.Event()

        class Fin(BaseActionNode):
            node_id = "F"
            resources = frozenset({"arm"})

            async def execute(self, proposal: Proposal) -> Result:
                await asyncio.sleep(0.01)
                finishing.set()          # wakes the waiter between release and buffering
                return Result(action_id=self.node_id, success=True)

        am = ActionMasterNode(bus=SignalBus())
        am.register(Fin(am.bus))
        am.launch(_p("F"))
        await finishing.wait()
        assert await am.wait_idle(1.0)
        assert [r.action_id for r in am.drain_completed()] == ["F"]

    @pytest.mark.asyncio
    async def test_close_drops_armed_reflexes(self) -> None:
        """X10: close() alone drops armed reflexes: idle again, no stale claim after setup()"""
        am = ActionMasterNode(bus=SignalBus())
        owner = _gated(am, "Owner", {"arm"})
        _gated(am, "R", {"arm"}, hold=False)
        _gated(am, "M", {"arm"}, hold=False)
        am.launch_reflex(_p("Owner", 250))
        await owner.started.wait()
        assert am.launch_reflex(_p("R", 240)).status is LaunchStatus.ARMED
        am.close()
        assert am.armed_reflexes() == {}
        owner.release.set()
        assert await am.wait_idle(1.0)
        await am.setup()
        assert am.launch(_p("M")).status is LaunchStatus.LAUNCHED
        assert await am.wait_idle(1.0)

    @pytest.mark.asyncio
    async def test_dispatch_many_with_an_unknown_action_starts_nothing(self) -> None:
        """X11: dispatch_many() raising for an unknown action leaves no sibling running"""
        am = ActionMasterNode(bus=SignalBus())
        _gated(am, "A", {"arm"})
        with pytest.raises(ActionNotFoundError):
            await am.dispatch_many([_p("A"), _p("Nope")])
        assert am.running_action_ids() == set()
        assert am.resource_owners() == {}


# ── Fourth review round (action findings R4-A1–A8) ────────────────────────────

class _OwnCancelTeardown(BaseActionNode):
    """Teardown ends in its own CancelledError (``bg.cancel(); await bg``)."""
    node_id = "Leaky"

    async def setup(self) -> None:
        self._bg = asyncio.ensure_future(asyncio.sleep(100.0))

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)

    async def teardown(self) -> None:
        self._bg.cancel()
        await self._bg


class _SlowTeardown(BaseActionNode):
    """A sibling whose teardown takes a moment."""
    node_id = "Other"

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.torn_down = False

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)

    async def teardown(self) -> None:
        await asyncio.sleep(0.02)
        self.torn_down = True


class _IdleSense(BaseSenseNode):
    node_id         = "Idle"
    signal_kind     = "idle"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        return Signal(source=self.node_id, kind=self.signal_kind, value=0,
                      confidence=1.0, timestamp=time.monotonic())


class TestFourthReviewRegressions:
    @pytest.mark.asyncio
    async def test_clean_up_on_a_timeout_cancel_is_not_cut_short(self) -> None:
        """
        R4-A1: clean-up run on an action or step timeout's cancel (a park) completes.
        Regression on Python 3.10 only: the old 3.10 timeout re-cancelled every 5 ms
        after the deadline; 3.11+ always cancelled once (passes on old code there).
        """

        class Parker(BaseActionNode):
            node_id   = "Parker"
            resources = frozenset({"arm"})
            timeout_s = 0.05

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.parked = False

            async def execute(self, proposal: Proposal) -> Result:
                try:
                    await asyncio.sleep(10.0)
                except asyncio.CancelledError:
                    await asyncio.sleep(0.05)          # park the arm
                    self.parked = True
                    raise
                return Result(action_id=self.node_id, success=True)

        class StepParker(MultiStepActionNode):
            node_id   = "StepParker"
            resources = frozenset({"gripper"})
            timeout_s = 10.0

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.parked = False

            def steps(self) -> list[ActionStep]:
                return [ActionStep("move", timeout_s=0.05)]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                try:
                    await asyncio.sleep(10.0)
                finally:
                    await asyncio.sleep(0.05)          # stop the motor
                    self.parked = True
                return StepResult(step_name=step.name, success=True)

        am = ActionMasterNode(bus=SignalBus())
        parker, step_parker = Parker(am.bus), StepParker(am.bus)
        am.register(parker)
        am.register(step_parker)
        action_result, step_result = await asyncio.gather(
            am.dispatch(_p("Parker")), am.dispatch(_p("StepParker")),
        )
        assert parker.parked is True
        assert step_parker.parked is True
        assert isinstance(action_result.error, ActionTimeoutError)
        assert step_result.success is False
        assert isinstance(step_result.error, StepAbortError)

    @pytest.mark.asyncio
    async def test_step_returning_after_swallowing_its_timeout_is_a_timeout(self) -> None:
        """
        R4-A2: a step that catches its timeout cancel and returns late fails as a timeout.
        Regression on Python 3.11+ (reported a success); the old 3.10 re-cancel loop cut
        such a step, so the old code passes there.
        """

        class Stubborn(MultiStepActionNode):
            node_id   = "Stubborn"
            resources = frozenset({"arm"})
            timeout_s = 10.0

            def steps(self) -> list[ActionStep]:
                return [ActionStep("s1", timeout_s=0.05)]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.sleep(0.2)           # finishes the motion anyway
                await asyncio.sleep(0.05)
                return StepResult(step_name=step.name, success=True)

        am = ActionMasterNode(bus=SignalBus())
        am.register(Stubborn(am.bus))
        result = await am.dispatch(_p("Stubborn"))
        assert result.success is False
        assert isinstance(result.error, StepAbortError)

    @pytest.mark.asyncio
    async def test_safe_stop_hook_swallowing_its_cancel_is_abandoned(self) -> None:
        """
        R4-A3: a hook that swallows its cancel holds a reflex back for at most
        safe_stop_timeout_s + SAFE_STOP_ABANDON_GRACE_S; it is abandoned and logged.
        """
        from arachnite.nodes import action as action_module

        # The documented grace (0.1 s); read from the module when it exists.
        SAFE_STOP_ABANDON_GRACE_S = getattr(action_module, "SAFE_STOP_ABANDON_GRACE_S", 0.1)

        class Owner(BaseActionNode):
            node_id             = "Owner"
            resources           = frozenset({"arm"})
            timeout_s           = 10.0
            safe_stop_timeout_s = 0.05

            def __init__(self, bus: SignalBus, sink: BaseLogSink) -> None:
                super().__init__(bus, log_sinks=[sink])
                self.started = asyncio.Event()
                self.hard_brake_done = asyncio.Event()

            async def execute(self, proposal: Proposal) -> Result:
                self.started.set()
                await asyncio.sleep(10.0)
                return Result(action_id=self.node_id, success=True)

            async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
                try:
                    await asyncio.sleep(10.0)                  # soft brake
                except asyncio.CancelledError:
                    await self.hard_brake_done.wait()          # brake harder, ignore the cancel

        sink = _CaptureSink()
        am = ActionMasterNode(bus=SignalBus())
        owner = Owner(am.bus, sink)
        am.register(owner)
        reflex = _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("Owner"))
        await owner.started.wait()
        started = time.perf_counter()
        am.launch_reflex(_p("R", 250))
        try:
            await asyncio.wait_for(reflex.started.wait(), 2.0)
            latency = time.perf_counter() - started
        finally:
            owner.hard_brake_done.set()                        # the abandoned hook ends
        assert latency < owner.safe_stop_timeout_s + SAFE_STOP_ABANDON_GRACE_S + 0.15
        assert await am.wait_idle(1.0)
        await _until(lambda: "Abandoned safe-stop hook ended" in sink.messages)
        assert "Safe-stop hook ignored its cancellation; abandoned" in sink.messages
        result = _results_by_id(am)["Owner"]
        assert result.preempted is True
        assert isinstance(result.error, ActionTimeoutError)

    @pytest.mark.asyncio
    async def test_preempt_after_a_walk_that_swallowed_the_cancel_undoes_once(self) -> None:
        """R4-A4: a PREEMPT in an interrupt walk whose undo swallows the cancel: undone once"""

        class Seq(MultiStepActionNode):
            node_id             = "Seq"
            resources           = frozenset({"arm"})
            timeout_s           = 10.0
            interrupt_policy    = InterruptPolicy.ROLLBACK
            rollback_on_preempt = True

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.undone: list[str] = []
                self.in_block = asyncio.Event()
                self.in_walk = asyncio.Event()
                self.ack: asyncio.Future[str] = asyncio.get_running_loop().create_future()

            def _undo(self, name: str):  # type: ignore[no-untyped-def]
                async def _rollback() -> None:
                    self.undone.append(name)
                    if name == "b" and not self.in_walk.is_set():
                        self.in_walk.set()
                        # Motor ack; an undo must finish, so the cancel is swallowed.
                        with contextlib.suppress(asyncio.CancelledError):
                            await self.ack
                return _rollback

            def steps(self) -> list[ActionStep]:
                return [
                    ActionStep("a", interruptible=False, rollback=self._undo("a")),
                    ActionStep("b", interruptible=False, rollback=self._undo("b")),
                    ActionStep("c"),
                ]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                if step.name == "b":
                    self.in_block.set()
                await asyncio.sleep(0.02)
                return StepResult(step_name=step.name, success=True)

        am = ActionMasterNode(bus=SignalBus())
        seq = Seq(am.bus)
        am.register(seq)
        _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("Seq"))
        await seq.in_block.wait()
        am.interrupt_all(InterruptRequest(
            new_proposal=_p("Other", 60), requesting_instinct_id="i",
        ))
        await seq.in_walk.wait()
        seq.ack.set_result("ok")             # the ack and the reflex land in the same turn
        am.launch_reflex(_p("R", 250))
        assert await am.wait_idle(2.0)
        assert seq.undone == ["b", "a"]
        result = _results_by_id(am)["Seq"]
        assert result.preempted is True
        assert result.rolled_back is True

    @pytest.mark.asyncio
    async def test_teardown_waits_for_a_rollback_on_preempt_walk(self) -> None:
        """R4-A5: teardown() during a preemption's rollback walk waits for it to finish"""

        class SlowUndo(MultiStepActionNode):
            node_id             = "Seq"
            resources           = frozenset({"arm"})
            timeout_s           = 10.0
            rollback_on_preempt = True
            safe_stop_timeout_s = 0.05

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.log: list[tuple[str, str, bool]] = []
                self.in_last = asyncio.Event()
                self.torn_down = False

            def _undo(self, name: str):  # type: ignore[no-untyped-def]
                async def _rollback() -> None:
                    self.log.append(("begin", name, self.torn_down))
                    await asyncio.sleep(0.35)          # reverse a physical motion
                    self.log.append(("end", name, self.torn_down))
                return _rollback

            def steps(self) -> list[ActionStep]:
                return [
                    ActionStep("a", interruptible=False, rollback=self._undo("a"), timeout_s=1.0),
                    ActionStep("b", interruptible=False, rollback=self._undo("b"), timeout_s=1.0),
                    ActionStep("c", interruptible=False),
                ]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                if step.name == "c":
                    self.in_last.set()
                    await asyncio.sleep(1.0)
                await asyncio.sleep(0.02)
                return StepResult(step_name=step.name, success=True)

            async def teardown(self) -> None:
                self.torn_down = True                  # e.g. close the motor driver

        am = ActionMasterNode(bus=SignalBus())
        seq = SlowUndo(am.bus)
        am.register(seq)
        _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("Seq"))
        await seq.in_last.wait()
        am.launch_reflex(_p("R", 250))
        await _until(lambda: bool(seq.log))     # the safe-stop's walk runs
        await am.teardown()                     # shutdown / emergency_stop path
        assert am.running_action_ids() == set()
        assert am.resource_owners() == {}
        assert seq.log == [
            ("begin", "b", False), ("end", "b", False),
            ("begin", "a", False), ("end", "a", False),
        ]
        assert seq.torn_down is True

    @pytest.mark.asyncio
    async def test_rollback_overrunning_its_step_timeout_fails_the_walk(self) -> None:
        """R4-A5: an undo that overruns its step's timeout_s is a rollback failure, not a hang"""

        class Jammed(MultiStepActionNode):
            node_id          = "Jammed"
            resources        = frozenset({"arm"})
            timeout_s        = 1.0
            interrupt_policy = InterruptPolicy.ROLLBACK

            async def _jammed_undo(self) -> None:
                await asyncio.Event().wait()           # the undo never ends

            def steps(self) -> list[ActionStep]:
                return [
                    ActionStep("a", interruptible=False, rollback=self._jammed_undo,
                               timeout_s=0.05),
                    ActionStep("b", interruptible=False, timeout_s=1.0),
                ]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                await asyncio.sleep(0.01)
                fail = step.name == "b"
                return StepResult(step_name=step.name, success=not fail, abort_sequence=fail)

        am = ActionMasterNode(bus=SignalBus())
        am.register(Jammed(am.bus))
        started = time.perf_counter()
        result = await am.dispatch(_p("Jammed"))
        assert time.perf_counter() - started < 0.6
        assert result.success is False
        assert isinstance(result.error, RollbackError)
        assert [(name, type(exc)) for name, exc in result.error.failures] == [
            ("a", ActionTimeoutError),
        ]

    @pytest.mark.parametrize("path", ["hand-off", "master-cancel"])
    @pytest.mark.asyncio
    async def test_foreign_cancel_during_a_safe_stop_propagates(self, path: str) -> None:
        """R4-A6: a cancel from outside the master during a safe-stop ends the run cancelled"""

        class Seq(MultiStepActionNode):
            node_id   = "Seq"
            resources = frozenset({"arm"})
            timeout_s = 10.0

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.in_block = asyncio.Event()
                self.in_stop = asyncio.Event()

            def steps(self) -> list[ActionStep]:
                return [ActionStep("a", interruptible=False), ActionStep("b")]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                if step.name == "a":
                    self.in_block.set()
                await asyncio.sleep(0.05)
                return StepResult(step_name=step.name, success=True)

            async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
                self.in_stop.set()
                await asyncio.sleep(0.05)

        am = ActionMasterNode(bus=SignalBus())
        seq = Seq(am.bus)
        am.register(seq)
        _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("Seq"))
        await seq.in_block.wait()
        task = am._tasks["Seq"]
        stopper: asyncio.Future[None] | None = None
        if path == "hand-off":
            am.launch_reflex(_p("R", 250), ReflexPreemption.WAIT_FOR_BLOCK)
        else:
            stopper = asyncio.ensure_future(am.cancel("Seq"))
        await seq.in_stop.wait()
        task.cancel()                            # foreign, e.g. the loop shutting down
        await asyncio.wait({task}, timeout=1.0)
        assert task.cancelled()
        if stopper is not None:
            await stopper
        assert await am.wait_idle(1.0)
        assert am.resource_owners() == {}

    @pytest.mark.asyncio
    async def test_walk_cut_by_the_action_timeout_is_not_retried(self) -> None:
        """R4-A7: an abort walk the action timeout cuts short is never retried from step 1"""

        class Seq(MultiStepActionNode):
            node_id          = "Seq"
            resources        = frozenset({"arm"})
            interrupt_policy = InterruptPolicy.ROLLBACK
            timeout_s        = 0.15
            max_retries      = 1

            def __init__(self, bus: SignalBus) -> None:
                super().__init__(bus)
                self.log: list[str] = []

            async def _undo_a(self) -> None:
                self.log.append("undo a begin")
                await asyncio.sleep(1.0)
                self.log.append("undo a end")

            def steps(self) -> list[ActionStep]:
                return [
                    ActionStep("a", interruptible=False, rollback=self._undo_a, timeout_s=1.0),
                    ActionStep("b", interruptible=False, timeout_s=1.0),
                ]

            async def execute_step(
                self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
            ) -> StepResult:
                self.log.append(f"do {step.name}")
                await asyncio.sleep(0.02)
                fail = step.name == "b"
                return StepResult(step_name=step.name, success=not fail, abort_sequence=fail)

        am = ActionMasterNode(bus=SignalBus())
        seq = Seq(am.bus)
        am.register(seq)
        result = await am.dispatch(_p("Seq"))
        assert seq.log == ["do a", "do b", "undo a begin"]
        assert result.success is False
        assert isinstance(result.error, ActionTimeoutError)

    @pytest.mark.asyncio
    async def test_teardown_ending_in_its_own_cancel_is_an_error(self) -> None:
        """R4-A8: a node teardown ending in its own CancelledError raises RuntimeError"""
        am = ActionMasterNode(bus=SignalBus())
        am.register(_OwnCancelTeardown(am.bus))
        other = _SlowTeardown(am.bus)
        am.register(other)
        await am.setup()
        raised: BaseException | None = None
        try:
            await am.teardown()
        except BaseException as exc:  # the old code raised CancelledError
            raised = exc
        assert isinstance(raised, RuntimeError)
        assert isinstance(raised.__cause__, asyncio.CancelledError)
        assert other.torn_down is True

    @pytest.mark.asyncio
    async def test_stop_completes_when_a_teardown_ends_in_its_own_cancel(self) -> None:
        """R4-A8: runtime.stop() reaches COMPLETE, then re-raises the teardown's RuntimeError"""
        rt = (
            RuntimeBuilder()
            .sense(_IdleSense)
            .action(_OwnCancelTeardown)
            .action(_SlowTeardown)
            .tick_rate(50.0)
            .build()
        )
        await rt.start()
        await asyncio.sleep(0.03)
        with pytest.raises(RuntimeError, match="own CancelledError"):
            await rt.stop()
        assert rt._shutdown_coordinator.phase is ShutdownPhase.COMPLETE
        other = rt._action_master.get_node("Other")
        assert isinstance(other, _SlowTeardown)
        assert other.torn_down is True


# ── Sixth review: action timeout and latched deliberative interrupts ─────────


class _TimedPick(MultiStepActionNode):
    """Approach, a two-step mandatory block, move; one step hangs until cut."""

    node_id          = "TimedPick"
    resources        = frozenset({"arm"})
    interrupt_policy = InterruptPolicy.ROLLBACK
    timeout_s        = 0.3
    max_retries      = 1

    def __init__(self, bus: SignalBus, hang_in: str | None) -> None:
        super().__init__(bus)
        self.log: list[str] = []
        self.hang_in = hang_in
        self.brake_s = 0.0
        self.safe_stop_timeout_s = 1.0
        self.go: dict[str, asyncio.Event] = {}

    async def _undo(self, name: str) -> None:
        self.log.append(f"undo {name}")

    async def _undo_lower(self) -> None:
        await self._undo("lower")

    async def _undo_close(self) -> None:
        await self._undo("close")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("approach", interruptible=True, timeout_s=5.0),
            ActionStep("lower", interruptible=False, timeout_s=5.0, rollback=self._undo_lower),
            ActionStep("close", interruptible=False, timeout_s=5.0, rollback=self._undo_close),
            ActionStep("move", interruptible=True, timeout_s=5.0),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        self.log.append(f"do {step.name}")
        if step.name == self.hang_in:
            await asyncio.Event().wait()
        gate = self.go.get(step.name)
        if gate is not None:
            await gate.wait()
        await asyncio.sleep(0)
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.log.append(f"brake ({preempted_by.instinct_id})")
        await asyncio.sleep(self.brake_s)
        self.log.append("brake end")


class _HangingSingle(BaseActionNode):
    node_id     = "HangingSingle"
    resources   = frozenset({"arm"})
    timeout_s   = 0.1

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.log: list[str] = []

    async def execute(self, proposal: Proposal) -> Result:
        self.log.append("execute")
        await asyncio.Event().wait()
        return Result(action_id=self.node_id, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.log.append(f"brake ({preempted_by.instinct_id})")

    async def on_timeout(self, proposal: Proposal) -> Result:
        self.log.append("on_timeout")
        return await super().on_timeout(proposal)


class TestSixthReviewRegressions:
    @pytest.mark.asyncio
    async def test_action_timeout_in_a_block_runs_safe_stop_and_is_not_retried(self) -> None:
        """R6-F5: a timeout inside a block runs the safe-stop, keeps the steps, no retry"""
        am = ActionMasterNode(bus=SignalBus())
        pick = _TimedPick(am.bus, hang_in="close")
        am.register(pick)
        result = await am.dispatch(_p("TimedPick"))
        # Safe-stop first, then the ROLLBACK abort walk of the completed block
        # step; the run is not re-run from step 1.
        assert pick.log == [
            "do approach", "do lower", "do close",
            "brake (timeout)", "brake end", "undo lower",
        ]
        assert result.success is False
        assert isinstance(result.error, ActionTimeoutError)
        assert result.cancelled is True
        assert result.interrupted is False
        assert result.stopped_at_step == "close"
        assert [s.step_name for s in result.step_results] == ["approach", "lower"]
        assert result.rolled_back is True

    @pytest.mark.asyncio
    async def test_action_timeout_outside_a_block_brakes_each_attempt_then_retries(self) -> None:
        """R6-F5: outside a block the timed-out run safe-stops, then the retry budget applies"""
        am = ActionMasterNode(bus=SignalBus())
        pick = _TimedPick(am.bus, hang_in="approach")
        pick.interrupt_policy = InterruptPolicy.ALWAYS
        am.register(pick)
        result = await am.dispatch(_p("TimedPick"))
        assert pick.log == [
            "do approach", "brake (timeout)", "brake end",
            "do approach", "brake (timeout)", "brake end",
        ]
        assert result.cancelled is True
        assert result.stopped_at_step == "approach"
        assert result.rolled_back is False

    @pytest.mark.asyncio
    async def test_single_step_timeout_runs_safe_stop_then_on_timeout(self) -> None:
        """R6-F5: a single-step action's timeout runs on_preempted(), then on_timeout() as before"""
        am = ActionMasterNode(bus=SignalBus())
        node = _HangingSingle(am.bus)
        am.register(node)
        am.launch(_p("HangingSingle"))
        assert await am.wait_idle(2.0)
        result = _results_by_id(am)["HangingSingle"]
        assert node.log == ["execute", "brake (timeout)", "on_timeout"]
        assert isinstance(result.error, ActionTimeoutError)
        assert result.cancelled is True

    @pytest.mark.asyncio
    async def test_preemption_during_the_timeout_safe_stop_runs_the_hook_once(self) -> None:
        """R6-F5: a reflex during the timeout safe-stop waits for it; one hook, no retry"""
        am = ActionMasterNode(bus=SignalBus())
        pick = _TimedPick(am.bus, hang_in="close")
        pick.brake_s = 0.2
        am.register(pick)
        log = pick.log
        _gated(am, "Retract", {"arm"}, hold=False, log=log)
        am.launch(_p("TimedPick"))
        await _until(lambda: "brake (timeout)" in log)
        assert am.launch_reflex(_p("Retract", 250)).status is LaunchStatus.ARMED
        assert await am.wait_idle(3.0)
        results = _results_by_id(am)
        assert log.count("brake (timeout)") == 1
        assert not any(e.startswith("brake (inst)") for e in log)
        assert log.index("brake end") < log.index("Retract")
        assert "undo lower" not in log            # preempted: no rollback_on_preempt walk
        assert log.count("do approach") == 1
        assert results["TimedPick"].preempted is True
        assert results["TimedPick"].stopped_at_step == "close"

    @pytest.mark.asyncio
    async def test_interrupt_inside_a_block_is_latched_and_honoured_after_it(self) -> None:
        """R6-F6: a deliberative interrupt in a block is held, then honoured at the next stop"""
        am = ActionMasterNode(bus=SignalBus())
        pick = _TimedPick(am.bus, hang_in=None)
        pick.timeout_s = 10.0
        pick.go = {"close": asyncio.Event()}
        am.register(pick)
        am.launch(_p("TimedPick"))
        await _until(lambda: pick.log[-1:] == ["do close"])
        request = InterruptRequest(
            new_proposal=_p("Abort", 150), requesting_instinct_id="inst",
            target_action_id="TimedPick",
        )
        await am.request_interrupt(request)       # inside the block: latched, never refused
        assert pick._interrupt_requested is True
        pick.go["close"].set()
        assert await am.wait_idle(2.0)
        result = _results_by_id(am)["TimedPick"]
        assert "do move" not in pick.log
        assert result.interrupted is True
        assert result.stopped_at_step == "move"
        assert [s.step_name for s in result.step_results] == ["approach", "lower", "close"]
        assert pick.log[-2:] == ["undo close", "undo lower"]
