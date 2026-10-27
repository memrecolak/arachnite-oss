"""
Tests that close the test gaps found by classifying the surviving mutants of
three safety sites (actuator exclusivity, preemption safe-stop, mandatory
block atomicity; survivor set of 2026-10-27, classification round B).

Each docstring starts with the survivor ids the test kills
(MUT-AE-nnn / MUT-PS-nnn / MUT-MB-nnn, see
paper/latex/revizyon_iki/deney_betikleri/mutation_v013/
survivor_classification_20261027_b.csv). Every test here was checked to fail
with the named mutant applied and to pass on the original code.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from arachnite import SignalBus
from arachnite.exceptions import ActionTimeoutError, RollbackError
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
    SAFE_STOP_ABANDON_GRACE_S,
    ActionMasterNode,
    BaseActionNode,
    LaunchStatus,
    MultiStepActionNode,
)
from tests.unit.test_reflex_arbitration import (
    Gated,
    _block_master,
    _BlockOwner,
    _gated,
    _p,
    _results_by_id,
    _until,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


async def _result_of(am: ActionMasterNode, action_id: str, timeout: float = 2.0) -> Result:
    """Drain finished Results until ``action_id``'s arrives (others may still run)."""
    found: dict[str, Result] = {}

    async def _collect() -> None:
        while action_id not in found:
            for result in am.drain_completed():
                found[result.action_id] = result
            if action_id not in found:
                await asyncio.sleep(0.002)

    await asyncio.wait_for(_collect(), timeout)
    return found[action_id]


class _HeldBrake(Gated):
    """Gated action whose safe-stop waits until the test lets it go."""

    def __init__(self, bus: SignalBus, node_id: str, resources: set[str]) -> None:
        super().__init__(bus, node_id, resources)
        self.braking = asyncio.Event()
        self.brake_release = asyncio.Event()

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.braking.set()
        await self.brake_release.wait()


class _GatedSeq(MultiStepActionNode):
    """One-step multi-step action that holds its resources until released."""

    def __init__(self, bus: SignalBus, node_id: str, resources: set[str]) -> None:
        self.node_id = node_id  # type: ignore[misc]
        self.resources = frozenset(resources)  # type: ignore[misc]
        super().__init__(bus)
        self.timeout_s = 10.0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def steps(self) -> list[ActionStep]:
        return [ActionStep("hold", timeout_s=10.0)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        self.started.set()
        await self.release.wait()
        return StepResult(step_name=step.name, success=True)


class _JammedBlockOwner(_BlockOwner):
    """_BlockOwner that rolls back on preemption, but undoing b1 fails."""

    node_id             = "Owner"
    rollback_on_preempt = True

    def _undo(self, name: str):  # type: ignore[no-untyped-def]
        record = super()._undo(name)

        async def _rollback() -> None:
            await record()
            if name == "b1":
                raise RuntimeError("undo failed")
        return _rollback


class _Hook(Gated):
    """Owner whose safe-stop misbehaves as ``mode`` says: slow, swallow or self_cancel."""

    def __init__(self, bus: SignalBus, mode: str) -> None:
        super().__init__(bus, "Owner", {"arm"})
        self.safe_stop_timeout_s = 0.05
        self.mode = mode
        self.let_go = asyncio.Event()
        self.hook_ended = asyncio.Event()

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        try:
            if self.mode == "self_cancel":
                fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
                fut.cancel()
                await fut
            try:
                await asyncio.sleep(10.0)                  # overruns safe_stop_timeout_s
            except asyncio.CancelledError:
                if self.mode == "swallow":
                    await self.let_go.wait()               # ignores its cancel: abandoned
                else:
                    raise
        finally:
            self.hook_ended.set()


class _BrakedSingle(BaseActionNode):
    """Single-step action that overruns its timeout; its safe-stop waits for the test."""

    node_id   = "Braked"
    resources = frozenset({"arm"})
    timeout_s = 0.05

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.brakes: list[str] = []
        self.braking = asyncio.Event()
        self.brake_release = asyncio.Event()

    async def execute(self, proposal: Proposal) -> Result:
        await asyncio.Event().wait()
        return Result(action_id=self.node_id, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.brakes.append(preempted_by.instinct_id)
        self.braking.set()
        await self.brake_release.wait()


class _BoundSteps(MultiStepActionNode):
    """Steps mixing interruptible/mandatory and undoable/plain, for the bound sum."""

    node_id             = "BoundSteps"
    timeout_s           = 5.0
    safe_stop_timeout_s = 0.3

    async def _noop(self) -> None:
        return None

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("s0", timeout_s=1.0),
            ActionStep("s1", timeout_s=2.0, rollback=self._noop),
            ActionStep("s2", interruptible=False, timeout_s=3.0, rollback=self._noop),
            ActionStep("s3", interruptible=False, timeout_s=4.0),
            ActionStep("s4", interruptible=False, rollback=self._noop),   # inherits 5.0
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        return StepResult(step_name=step.name, success=True)


class _Handoff(MultiStepActionNode):
    """a → [b, c] (mandatory, undoable) → d; may ask for a reflex hand-off inside the block."""

    node_id             = "Handoff"
    rollback_on_preempt = True
    timeout_s           = 5.0

    def __init__(self, bus: SignalBus, *, hand_off_in: str | None = "b") -> None:
        super().__init__(bus)
        self.log: list[str] = []
        self.hook_calls: list[Proposal] = []
        self.hand_off_in = hand_off_in
        self.reflex = _p("Retract", 250, "CollisionReflex")

    def _undo(self, name: str) -> Callable[[], asyncio.Future[None] | object]:
        async def _rollback() -> None:
            self.log.append(f"undo {name}")
        return _rollback

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("a"),
            ActionStep("b", interruptible=False, rollback=self._undo("b")),
            ActionStep("c", interruptible=False, rollback=self._undo("c")),
            ActionStep("d"),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        self.log.append(step.name)
        if step.name == self.hand_off_in:
            self.request_reflex_preempt(self.reflex)
        await asyncio.sleep(0)
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.hook_calls.append(preempted_by)


class _CancellableHook(_Handoff):
    """_Handoff whose safe-stop hook waits for the test and records its cancellation."""

    node_id = "Handoff"

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.hook_started = asyncio.Event()
        self.hook_gate = asyncio.Event()
        self.hook_cancelled = False

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.hook_started.set()
        try:
            await self.hook_gate.wait()
        except asyncio.CancelledError:
            self.hook_cancelled = True
            raise


class _Walk(MultiStepActionNode):
    """a → [b, c, d] (mandatory; b and d undoable) → e → f; asks for an interrupt inside e."""

    node_id          = "Walk"
    interrupt_policy = InterruptPolicy.ROLLBACK
    timeout_s        = 5.0

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.log: list[str] = []

    def _undo(self, name: str) -> Callable[[], asyncio.Future[None] | object]:
        async def _rollback() -> None:
            self.log.append(f"undo {name}")
        return _rollback

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("a"),
            ActionStep("b", interruptible=False, rollback=self._undo("b")),
            ActionStep("c", interruptible=False),
            ActionStep("d", interruptible=False, rollback=self._undo("d")),
            ActionStep("e"),
            ActionStep("f"),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        self.log.append(step.name)
        if step.name == "e":
            self.request_interrupt(InterruptRequest(
                new_proposal=_p("Other", 150), requesting_instinct_id="inst",
            ))
        await asyncio.sleep(0)
        return StepResult(step_name=step.name, success=True)


# ── Actuator exclusivity ──────────────────────────────────────────────────────


class TestActuatorExclusivityGaps:
    @pytest.mark.asyncio
    async def test_cancel_of_an_idle_action_is_a_no_op(self) -> None:
        """MUT-AE-028: cancel() of an action neither armed nor running returns quietly."""
        am = ActionMasterNode(bus=SignalBus())
        _gated(am, "Idle", {"arm"}, hold=False)
        await am.cancel("Idle")
        await am.cancel("Unknown")
        assert am.launch(_p("Idle")).status is LaunchStatus.LAUNCHED
        assert await am.wait_idle(1.0)
        await am.cancel("Idle")                 # finished: its task is gone, still a no-op
        assert _results_by_id(am)["Idle"].success is True

    @pytest.mark.asyncio
    async def test_disjoint_armed_claim_does_not_block_deliberation(self) -> None:
        """
        MUT-AE-011, -014, -015, -016, -017, -018, -021: an armed reflex claims
        only its own resources; deliberative actions on other actuators (with
        ids sorting before and after the claimant's) start.
        """
        am = ActionMasterNode(bus=SignalBus())
        holder = _gated(am, "Holder", {"buzzer"})
        _gated(am, "M_ref", {"buzzer"}, hold=False)
        _gated(am, "A_move", {"arm"}, hold=False)
        _gated(am, "Z_move", {"wheel"}, hold=False)
        _gated(am, "Horn", {"buzzer"}, hold=False)
        assert am.launch_reflex(_p("Holder", 250)).status is LaunchStatus.LAUNCHED
        await holder.started.wait()
        assert am.launch_reflex(_p("M_ref", 240)).status is LaunchStatus.ARMED
        assert am.conflicting_owners(_p("A_move")) == set()
        assert am.conflicting_owners(_p("Z_move")) == set()
        assert am.launch(_p("A_move")).status is LaunchStatus.LAUNCHED
        assert am.launch(_p("Z_move")).status is LaunchStatus.LAUNCHED
        blocked = am.launch(_p("Horn"))
        assert blocked.status is LaunchStatus.BLOCKED
        assert blocked.owners == frozenset({"Holder", "M_ref"})
        holder.release.set()
        assert await am.wait_idle(2.0)

    @pytest.mark.asyncio
    async def test_lower_reflex_on_a_free_resource_waits_behind_a_higher_claim(self) -> None:
        """
        MUT-AE-024: a reflex whose resources are free but claimed by a
        higher-priority armed reflex is armed behind it, not started.
        """
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        d_arm = _gated(am, "D_arm", {"arm"})
        d_grip = _HeldBrake(am.bus, "D_grip", {"gripper"})
        am.register(d_grip)
        _gated(am, "R_high", {"arm", "gripper"}, hold=False, log=log)
        _gated(am, "R_low", {"arm"}, hold=False, log=log)
        am.launch(_p("D_arm"))
        am.launch(_p("D_grip"))
        await d_arm.started.wait()
        await d_grip.started.wait()
        assert am.launch_reflex(_p("R_high", 250)).status is LaunchStatus.ARMED
        await d_grip.braking.wait()                       # D_grip's safe-stop is in progress
        await _until(lambda: "D_arm" not in am.running_action_ids())   # the arm is free
        outcome = am.launch_reflex(_p("R_low", 210))
        assert outcome.status is LaunchStatus.ARMED
        assert outcome.owners == frozenset({"R_high"})
        assert am.resource_owners().get("arm") is None
        assert set(am.armed_reflexes()) == {"R_high", "R_low"}
        d_grip.brake_release.set()
        assert await am.wait_idle(2.0)
        assert log == ["R_high", "R_low"]

    @pytest.mark.asyncio
    async def test_close_withdraws_pending_hand_offs(self) -> None:
        """MUT-AE-025, -032: close() withdraws a WAIT_FOR_BLOCK hand-off with its reflex."""
        am, _, log = _block_master()
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        outcome = am.launch_reflex(_p("R_wait", 240), ReflexPreemption.WAIT_FOR_BLOCK)
        assert outcome.status is LaunchStatus.ARMED
        am.close()
        assert am.armed_reflexes() == {}
        assert await am.wait_idle(2.0)
        result = _results_by_id(am)["Owner"]
        assert result.success is True and result.preempted is False
        assert "run1 s3" in log
        assert not any(e.startswith("brake") for e in log)

    @pytest.mark.asyncio
    async def test_cancelling_a_hand_off_reflex_skips_unrelated_owners_and_claims(self) -> None:
        """
        MUT-AE-002 to -010, -012, -013, -030, -031: cancelling the reflex an
        owner waits to hand over to re-points the hand-off only at an armed
        reflex sharing the owner's resources — none here — and the search
        passes over a single-step owner and an unrelated multi-step owner.
        """
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        single = _gated(am, "HoldBuzzer", {"buzzer"})
        multi = _GatedSeq(am.bus, "HoldLight", {"light"})
        am.register(multi)
        owner = _BlockOwner(am.bus, log)
        am.register(owner)
        _gated(am, "R_wait", {"arm"}, hold=False, log=log)
        _gated(am, "R_buzz", {"buzzer"}, hold=False, log=log)
        # Running order: a single-step reflex, an unrelated multi-step action, the owner.
        assert am.launch_reflex(_p("HoldBuzzer", 250)).status is LaunchStatus.LAUNCHED
        assert am.launch(_p("HoldLight")).status is LaunchStatus.LAUNCHED
        assert am.launch(_p("Owner")).status is LaunchStatus.LAUNCHED
        await single.started.wait()
        await multi.started.wait()
        await _until(lambda: "run1 b1" in log)
        outcome = am.launch_reflex(_p("R_wait", 240), ReflexPreemption.WAIT_FOR_BLOCK)
        assert outcome.status is LaunchStatus.ARMED
        assert am.launch_reflex(_p("R_buzz", 230)).status is LaunchStatus.ARMED  # behind a reflex
        await am.cancel("R_wait")
        assert set(am.armed_reflexes()) == {"R_buzz"}
        result = await _result_of(am, "Owner")
        assert result.success is True and result.preempted is False
        assert "run1 s3" in log and "R_buzz" not in log
        assert not any(e.startswith("brake") for e in log)
        single.release.set()
        multi.release.set()
        assert await am.wait_idle(2.0)
        assert "R_buzz" in log

    @pytest.mark.asyncio
    async def test_reflex_on_an_owner_cancelled_before_its_first_step(self) -> None:
        """
        MUT-AE-027: a reflex launched after cancel() ended an unstarted owner's
        task, but before its done-callback released it, finds the task done:
        the owner is reported cancelled, not preempted, and the reflex runs.
        """
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        _gated(am, "D", {"arm"})
        _gated(am, "R", {"arm"}, hold=False, log=log)
        outcomes = []
        assert am.launch(_p("D")).status is LaunchStatus.LAUNCHED
        # Queued behind D's first step, ahead of D's done-callback.
        asyncio.get_running_loop().call_soon(
            lambda: outcomes.append(am.launch_reflex(_p("R", 250))),
        )
        await am.cancel("D", timeout=2.0)
        assert outcomes[0].status is LaunchStatus.ARMED
        assert await am.wait_idle(2.0)
        results = _results_by_id(am)
        assert results["D"].cancelled is True and results["D"].interrupted is True
        assert results["D"].preempted is False
        assert results["R"].success is True and log == ["R"]


# ── Preemption safe-stop ──────────────────────────────────────────────────────


class TestPreemptionSafeStopGaps:
    def test_safe_stop_bound_adds_the_abandon_grace(self) -> None:
        """MUT-PS-001, -003, -005, -007, -009, -010: bound = hook timeout + abandon grace."""
        node = Gated(SignalBus(), "A", {"arm"})
        node.safe_stop_timeout_s = 0.3
        assert node.safe_stop_bound_s() == pytest.approx(0.3 + SAFE_STOP_ABANDON_GRACE_S)

    def test_multi_step_bound_adds_undoable_mandatory_steps_with_rollback_on_preempt(self) -> None:
        """
        MUT-PS-023, -034, -036: with rollback_on_preempt the bound adds the
        timeout (the action's where unset) of every mandatory step that has a
        rollback, and nothing else.
        """
        node = _BoundSteps(SignalBus())
        base = 0.3 + SAFE_STOP_ABANDON_GRACE_S
        assert node.safe_stop_bound_s() == pytest.approx(base)
        node.rollback_on_preempt = True
        assert node.safe_stop_bound_s() == pytest.approx(base + 3.0 + 5.0)

    @pytest.mark.asyncio
    async def test_safe_stop_budget_is_the_longest_running_bound_plus_half_a_second(self) -> None:
        """MUT-PS-002, -004, -006, -008, -011, -043, -044, -045, -046."""
        am = ActionMasterNode(bus=SignalBus())
        a = _gated(am, "A", {"arm"})
        a.safe_stop_timeout_s = 0.3
        b = _gated(am, "B", {"wheel"})
        b.safe_stop_timeout_s = 0.6
        assert am.safe_stop_budget_s() == pytest.approx(0.5)
        am.launch(_p("A"))
        am.launch(_p("B"))
        assert am.safe_stop_budget_s() == pytest.approx(0.6 + SAFE_STOP_ABANDON_GRACE_S + 0.5)
        a.release.set()
        b.release.set()
        assert await am.wait_idle(1.0)
        assert am.safe_stop_budget_s() == pytest.approx(0.5)

    @pytest.mark.parametrize(
        "first, second, expected",
        [
            (("R_a", 240), ("R_b", 230), "R_a"),
            (("R_a", 230), ("R_b", 240), "R_b"),
            (("R_a", 240), ("R_b", 240), "R_a"),
        ],
    )
    @pytest.mark.asyncio
    async def test_hand_off_records_the_highest_priority_waiting_reflex(
        self, first: tuple[str, int], second: tuple[str, int], expected: str,
    ) -> None:
        """
        MUT-PS-015 to -021: with two WAIT_FOR_BLOCK reflexes waiting on one
        block, the safe-stop is handed the higher-priority one (the first
        armed on a tie), which is also the one admitted first.
        """
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        owner = _BlockOwner(am.bus, log)
        am.register(owner)
        for reflex_id in ("R_a", "R_b"):
            _gated(am, reflex_id, {"arm"}, hold=False, log=log)
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        for reflex_id, priority in (first, second):
            outcome = am.launch_reflex(_p(reflex_id, priority), ReflexPreemption.WAIT_FOR_BLOCK)
            assert outcome.status is LaunchStatus.ARMED
        assert await am.wait_idle(2.0)
        other = "R_b" if expected == "R_a" else "R_a"
        assert f"brake begin ({expected})" in log
        assert f"brake begin ({other})" not in log
        assert log.index(expected) < log.index(other)
        assert _results_by_id(am)["Owner"].preempted is True

    @pytest.mark.asyncio
    async def test_failed_rollback_in_a_hand_off_is_reported(self) -> None:
        """
        MUT-PS-035: a WAIT_FOR_BLOCK hand-off whose safe-stop hook succeeded
        but whose rollback_on_preempt walk failed reports the RollbackError.
        """
        log: list[str] = []
        am = ActionMasterNode(bus=SignalBus())
        owner = _JammedBlockOwner(am.bus, log)
        am.register(owner)
        _gated(am, "R_wait", {"arm"}, hold=False, log=log)
        am.launch(_p("Owner"))
        await _until(lambda: "run1 b1" in log)
        outcome = am.launch_reflex(_p("R_wait", 240), ReflexPreemption.WAIT_FOR_BLOCK)
        assert outcome.status is LaunchStatus.ARMED
        assert await am.wait_idle(2.0)
        result = _results_by_id(am)["Owner"]
        assert result.preempted is True and result.rolled_back is False
        assert isinstance(result.error, RollbackError)
        assert "undo b2" in log and "undo b1" in log
        assert log.index("undo b1") < log.index("R_wait")

    @pytest.mark.parametrize(
        "mode, error_type",
        [
            ("slow", ActionTimeoutError),
            ("swallow", ActionTimeoutError),
            ("self_cancel", RuntimeError),
        ],
    )
    @pytest.mark.asyncio
    async def test_failed_safe_stop_never_reports_a_rollback(
        self, mode: str, error_type: type[BaseException],
    ) -> None:
        """
        MUT-PS-029, -030, -031: a hook that timed out, was abandoned or ended
        in CancelledError is reported with rolled_back=False and its error.
        """
        am = ActionMasterNode(bus=SignalBus())
        owner = _Hook(am.bus, mode)
        am.register(owner)
        _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("Owner"))
        await owner.started.wait()
        am.launch_reflex(_p("R", 250))
        try:
            assert await am.wait_idle(2.0)
        finally:
            owner.let_go.set()                              # an abandoned hook ends
        await asyncio.wait_for(owner.hook_ended.wait(), 1.0)
        await asyncio.sleep(0)
        result = _results_by_id(am)["Owner"]
        assert result.preempted is True and result.rolled_back is False
        assert isinstance(result.error, error_type)

    @pytest.mark.asyncio
    async def test_second_safe_stop_of_a_run_reports_no_rollback(self) -> None:
        """
        MUT-PS-028: a run preempted during its timeout safe-stop ends through
        the cancel path, which does not run the hook again and reports no
        rollback.
        """
        am = ActionMasterNode(bus=SignalBus())
        owner = _BrakedSingle(am.bus)
        am.register(owner)
        reflex = _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("Braked"))
        await asyncio.wait_for(owner.braking.wait(), 2.0)     # the timeout's safe-stop runs
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        owner.brake_release.set()
        assert await am.wait_idle(2.0)
        assert owner.brakes == ["timeout"]
        assert reflex.started.is_set()
        result = _results_by_id(am)["Braked"]
        assert result.preempted is True and result.cancelled is True
        assert result.rolled_back is False and result.error is None

    @pytest.mark.asyncio
    async def test_cancel_all_skips_a_finished_unstarted_task_and_stops_the_rest(self) -> None:
        """
        MUT-PS-040: cancel_all() passes over a task that ended cancelled
        before its first step (its release is still queued) and still cancels
        every other running action.
        """
        am = ActionMasterNode(bus=SignalBus())
        _gated(am, "D", {"arm"})
        e = _gated(am, "E", {"wheel"})
        _gated(am, "R", {"arm"}, hold=False)
        am.launch(_p("D"))
        am.launch(_p("E"))
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED   # cancels unstarted D
        await asyncio.sleep(0)       # D's task ends cancelled; its release is queued behind us
        assert e.started.is_set() and "D" in am.running_action_ids()
        await am.cancel_all(timeout=2.0)
        assert am.running_action_ids() == set()
        assert await am.wait_idle(2.0)
        results = _results_by_id(am)
        assert results["D"].preempted is True
        assert results["E"].cancelled is True and results["E"].interrupted is True
        assert "R" not in results

    @pytest.mark.asyncio
    async def test_rollback_walk_passes_over_steps_it_cannot_undo(self) -> None:
        """
        MUT-PS-038, -039: the walk skips interruptible steps and mandatory
        steps without a rollback, and carries on to the undoable ones before them.
        """
        node = _Walk(SignalBus())
        result = await node.execute(_p("Walk"))
        assert result.interrupted is True and result.rolled_back is True
        assert result.stopped_at_step == "f"
        assert node.log == ["a", "b", "c", "d", "e", "undo d", "undo b"]

    @pytest.mark.asyncio
    async def test_cancelling_a_runtime_less_hand_off_cancels_its_safe_stop_hook(self) -> None:
        """
        MUT-PS-041: a runtime-less execute() cancelled while its inline
        hand-off safe-stop runs cancels the hook rather than orphaning it.
        """
        node = _CancellableHook(SignalBus())
        task = asyncio.ensure_future(node.execute(_p("Handoff")))
        try:
            await asyncio.wait_for(node.hook_started.wait(), 2.0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.sleep(0)
            assert node.hook_cancelled is True
        finally:
            node.hook_gate.set()                 # an orphaned hook would end here
            await asyncio.sleep(0)


# ── Mandatory block atomicity (runtime-less execute paths) ────────────────────


class TestMandatoryBlockGaps:
    @pytest.mark.asyncio
    async def test_runtime_less_hand_off_runs_the_safe_stop_and_rollback_inline(self) -> None:
        """
        MUT-MB-010, MUT-MB-012, MUT-PS-026, -027, -033: a runtime-less
        execute() hands over at the first boundary after the block, running
        on_preempted() and the rollback_on_preempt walk inline, and reports a
        preempted, interrupted, rolled-back failure.
        """
        node = _Handoff(SignalBus())
        result = await node.execute(_p("Handoff"))
        assert node.hook_calls == [node.reflex]
        assert node.log == ["a", "b", "c", "undo c", "undo b"]
        assert result.success is False and result.error is None
        assert result.interrupted is True and result.preempted is True
        assert result.rolled_back is True and result.stopped_at_step == "d"
        assert [s.step_name for s in result.step_results] == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_runtime_less_clean_run_reports_no_cut_block(self) -> None:
        """MUT-MB-009: after a run that completed every step no block counts as cut."""
        node = _Handoff(SignalBus(), hand_off_in=None)
        result = await node.execute(_p("Handoff"))
        assert result.success is True and node.log == ["a", "b", "c", "d"]
        assert node.block_cut_unrestored() is False
        assert node.walk_cut_short() is False
