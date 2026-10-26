"""Unit tests for non-blocking dispatch, actuator resources, and reflex arbitration (ADR 0005)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

import pytest

from arachnite import SignalBus
from arachnite.exceptions import (
    ActionTimeoutError,
    NodeRegistrationError,
    ResourceConflictError,
    RollbackError,
)
from arachnite.models import (
    ActionStep,
    InterruptPolicy,
    InterruptRequest,
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
