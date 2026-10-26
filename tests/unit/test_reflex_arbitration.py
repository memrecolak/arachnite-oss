"""Unit tests for non-blocking dispatch, actuator resources, and reflex arbitration (ADR 0005)."""

from __future__ import annotations

import asyncio
import time

import pytest

from arachnite import SignalBus
from arachnite.exceptions import ActionTimeoutError, NodeRegistrationError, ResourceConflictError
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


def _p(action_id: str, priority: int = 50, instinct_id: str = "inst") -> Proposal:
    return Proposal(instinct_id=instinct_id, action_id=action_id,
                    priority=priority, urgency=0.5)


def _master() -> tuple[ActionMasterNode, Actuator, PickPlace, Retract]:
    bus = SignalBus()
    am = ActionMasterNode(bus=bus)
    actuator = Actuator()
    pick = PickPlace(bus, actuator)
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
