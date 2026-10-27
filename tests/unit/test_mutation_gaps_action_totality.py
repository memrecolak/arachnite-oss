"""
Tests written for the surviving mutants of the *action totality* safety site
(R2-1; ``benchmarks/safety_site_mutation.py``, site ``action_totality``).

Each test's docstring starts with the survivor ids it kills (MUT-AT-nnn in
``survivor_classification_20261027_a.csv``). The site is the task body
``ActionMasterNode._run`` and the retry / timeout / cancel bookkeeping under
it (spec §17.12, ADR 0005 and 0009): every run ends in exactly one Result,
except a cancel from outside the master, which propagates.
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from arachnite import SignalBus
from arachnite.exceptions import ActionTimeoutError, RollbackError, StepAbortError
from arachnite.models import (
    ActionStep,
    InterruptPolicy,
    InterruptRequest,
    Proposal,
    ReflexPreemption,
    Result,
    StepResult,
)
from arachnite.nodes import action as action_module
from arachnite.nodes.action import (
    ActionMasterNode,
    BaseActionNode,
    LaunchStatus,
    MultiStepActionNode,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _p(action_id: str, priority: int = 50) -> Proposal:
    return Proposal(instinct_id="inst", action_id=action_id, priority=priority, urgency=0.5)


async def _until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = time.perf_counter() + timeout
    while not predicate():
        if time.perf_counter() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.002)


def _results_by_id(am: ActionMasterNode) -> dict[str, Result]:
    return {r.action_id: r for r in am.drain_completed()}


class _Single(BaseActionNode):
    """
    A single-step action with test-controlled gates: ``release`` lets
    ``execute()`` return, ``brake_release`` lets ``on_preempted()`` return.
    ``fail_first`` attempts return ``success=False``.
    """

    def __init__(
        self,
        bus: SignalBus,
        node_id: str,
        resources: set[str] | None = None,
        *,
        hold: bool = False,
        brake_hold: bool = False,
        fail_first: int = 0,
        interrupted_result: bool = False,
    ) -> None:
        self.node_id = node_id  # type: ignore[misc]
        self.resources = frozenset(resources if resources is not None else {"arm"})  # type: ignore[misc]
        super().__init__(bus)
        self.timeout_s = 10.0
        self.calls = 0
        self.brakes = 0
        self.fail_first = fail_first
        self.interrupted_result = interrupted_result
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.brake_started = asyncio.Event()
        self.brake_release = asyncio.Event()
        if not hold:
            self.release.set()
        if not brake_hold:
            self.brake_release.set()

    async def execute(self, proposal: Proposal) -> Result:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        if self.interrupted_result:
            return Result(action_id=self.node_id, success=False, interrupted=True)
        return Result(action_id=self.node_id, success=self.calls > self.fail_first)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.brakes += 1
        self.brake_started.set()
        await self.brake_release.wait()


class _Seq(MultiStepActionNode):
    """
    A multi-step action on the arm, built from ``(name, interruptible)``
    pairs. ``gates`` hold a step open; ``hang_in`` never returns from a step
    (the action timeout cuts it); ``abort_in`` aborts the sequence at a step.
    Every non-interruptible step has a rollback callable whose behaviour is
    ``undo``: ``"ok"``, ``"raise"`` (a RollbackError walk), ``"cancelled"``
    (ends in its own CancelledError), ``"hold"`` (waits for ``undo_release``)
    or ``"swallow"`` (holds, turns a cancel into a RuntimeError).
    """

    node_id = "Seq"
    resources = frozenset({"arm"})

    def __init__(
        self,
        bus: SignalBus,
        steps: list[tuple[str, bool]],
        *,
        policy: InterruptPolicy = InterruptPolicy.ALWAYS,
        rollback_on_preempt: bool = False,
        max_retries: int = 0,
        timeout_s: float = 10.0,
        step_timeout_s: float | None = None,
        undo: str = "ok",
        abort_in: str | None = None,
        hang_in: str | None = None,
        timeout_hold: bool = False,
    ) -> None:
        super().__init__(bus)
        self.interrupt_policy = policy
        self.rollback_on_preempt = rollback_on_preempt
        self.max_retries = max_retries
        self.timeout_s = timeout_s
        self._spec = steps
        self._step_timeout_s = step_timeout_s
        self.undo = undo
        self.abort_in = abort_in
        self.hang_in = hang_in
        self.log: list[str] = []
        self.runs = 0
        self.gates: dict[str, asyncio.Event] = {}
        self.undo_started = asyncio.Event()
        self.undo_release = asyncio.Event()
        self.timeout_started = asyncio.Event()
        self.timeout_release = asyncio.Event()
        if not timeout_hold:
            self.timeout_release.set()

    def _rollback_of(self, name: str) -> Callable[[], Coroutine[Any, Any, None]]:
        async def _undo() -> None:
            self.log.append(f"undo {name} begin")
            if self.undo == "raise":
                raise RuntimeError(f"undo {name} jammed")
            if self.undo == "cancelled":
                fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
                fut.cancel()
                await fut
            if self.undo in ("hold", "swallow"):
                self.undo_started.set()
                try:
                    await self.undo_release.wait()
                except asyncio.CancelledError:
                    if self.undo == "swallow":
                        raise RuntimeError(f"undo {name} jammed") from None
                    raise
            self.log.append(f"undo {name}")
        return _undo

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep(
                name,
                interruptible=interruptible,
                rollback=None if interruptible else self._rollback_of(name),
                timeout_s=self._step_timeout_s,
            )
            for name, interruptible in self._spec
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        if step.name == self._spec[0][0]:
            self.runs += 1
        self.log.append(f"do {step.name}")
        if step.name == self.hang_in:
            await asyncio.Event().wait()
        gate = self.gates.get(step.name)
        if gate is not None:
            await gate.wait()
        if step.name == self.abort_in:
            return StepResult(step_name=step.name, success=False, abort_sequence=True)
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.log.append(f"brake ({preempted_by.instinct_id})")

    async def on_timeout(self, proposal: Proposal) -> Result:
        self.log.append("on_timeout")
        self.timeout_started.set()
        await self.timeout_release.wait()
        return await super().on_timeout(proposal)


def _master() -> ActionMasterNode:
    return ActionMasterNode(bus=SignalBus())


def _reflex(am: ActionMasterNode, resources: set[str] | None = None) -> _Single:
    """The reflex action ``R``: one short run on ``resources`` (default: the arm)."""
    node = _Single(am.bus, "R", resources)
    am.register(node)
    return node


class _SysView:
    """What the action module sees of ``sys`` under another ``version_info``."""

    def __init__(self, version_info: tuple[int, int, int, str, int]) -> None:
        self.version_info = version_info

    def __getattr__(self, name: str) -> object:
        return getattr(sys, name)


@pytest.fixture
def py310_cancel_accounting(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Make the module account for cancels as it does on CPython 3.10, on any
    interpreter: no ``Task.cancelling()`` (``_cancelling()`` is 0) and the
    version gate selects the delivery-count fallback of
    ``_master_cancel_overflow``. On a real 3.10 this changes nothing.
    """
    monkeypatch.setattr(action_module, "sys", _SysView((3, 10, 0, "final", 0)))
    monkeypatch.setattr(action_module, "_cancelling", lambda: 0)


counted_path = pytest.mark.skipif(
    sys.version_info < (3, 11), reason="Python 3.10 cannot count cancel requests",
)


# ── Foreign cancels: the counted path (3.11+) and the delivery-count fallback ──


class TestCancelAccounting:
    @counted_path
    @pytest.mark.parametrize("version_info", [None, (3, 11, 0, "final", 0)])
    async def test_coalesced_foreign_and_master_cancel_propagates(
        self, monkeypatch: pytest.MonkeyPatch, version_info: tuple | None,
    ) -> None:
        """
        MUT-AT-055, 057, 058, 062, 085, 087: from 3.11 on the master compares
        the task's cancel count, so a foreign cancel that coalesced with a
        preemption into one delivery still propagates after the safe-stop
        (the delivery-count fallback would turn it into a Result).
        """
        if version_info is not None:
            monkeypatch.setattr(action_module, "sys", _SysView(version_info))
        am = _master()
        owner = _Single(am.bus, "Owner", hold=True)
        am.register(owner)
        _reflex(am)
        am.launch(_p("Owner"))
        await owner.started.wait()
        task = am._tasks["Owner"]
        task.cancel()                                                  # foreign
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED  # same turn
        await asyncio.wait({task}, timeout=1.0)
        assert task.cancelled()
        assert owner.brakes == 1
        assert await am.wait_idle(1.0)
        results = _results_by_id(am)
        assert "Owner" not in results
        assert results["R"].success is True

    @pytest.mark.usefixtures("py310_cancel_accounting")
    async def test_fallback_plain_preemption_ends_with_a_result(self) -> None:
        """
        MUT-AT-089, 044, 050, 051, 052: under the delivery-count fallback one
        delivery against one master cancel is not foreign — a preempted run
        ends with a Result.
        """
        am = _master()
        owner = _Single(am.bus, "Owner", hold=True)
        am.register(owner)
        _reflex(am)
        am.launch(_p("Owner"))
        await owner.started.wait()
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        assert await am.wait_idle(1.0)
        result = _results_by_id(am)["Owner"]
        assert result.success is False
        assert result.preempted is True and result.interrupted is True
        assert result.cancelled is True
        assert owner._master_cancels == 1

    @pytest.mark.usefixtures("py310_cancel_accounting")
    async def test_fallback_foreign_cancel_during_the_safe_stop_propagates(self) -> None:
        """
        MUT-AT-001, 003, 008, 010, 014, 016, 018, 090, 048, 056, 086, 088:
        under the fallback the deliveries are 1 + those absorbed by the
        safe-stop; a foreign cancel delivered during it makes two against one
        master cancel, so the run ends cancelled (no Result), and the reflex
        still runs.
        """
        am = _master()
        owner = _Single(am.bus, "Owner", hold=True, brake_hold=True)
        am.register(owner)
        _reflex(am)
        am.launch(_p("Owner"))
        await owner.started.wait()
        task = am._tasks["Owner"]
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        await owner.brake_started.wait()
        task.cancel()                                                  # foreign
        owner.brake_release.set()
        await asyncio.wait({task}, timeout=1.0)
        assert task.cancelled()
        assert await am.wait_idle(1.0)
        results = _results_by_id(am)
        assert "Owner" not in results
        assert results["R"].success is True

    @pytest.mark.usefixtures("py310_cancel_accounting")
    async def test_fallback_master_cancels_during_the_safe_stop_still_end_with_a_result(
        self,
    ) -> None:
        """
        MUT-AT-012: two more master cancels delivered during the safe-stop
        (three in all) are three deliveries against three sent — not foreign;
        the run ends with its Result.
        """
        am = _master()
        owner = _Single(am.bus, "Owner", hold=True, brake_hold=True)
        am.register(owner)
        _reflex(am)
        am.launch(_p("Owner"))
        await owner.started.wait()
        task = am._tasks["Owner"]
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        await owner.brake_started.wait()
        stops = []
        for sent in (2, 3):
            stops.append(asyncio.ensure_future(am.cancel("Owner")))
            await _until(lambda sent=sent: owner._master_cancels == sent)
            await asyncio.sleep(0)                         # deliver it separately
            await asyncio.sleep(0)
        owner.brake_release.set()
        await asyncio.wait({task}, timeout=1.0)
        assert not task.cancelled()
        await asyncio.gather(*stops)
        assert await am.wait_idle(1.0)
        results = _results_by_id(am)
        assert results["Owner"].cancelled is True and results["Owner"].preempted is True
        assert results["R"].success is True

    @pytest.mark.usefixtures("py310_cancel_accounting")
    async def test_fallback_coalesced_master_cancels_end_with_a_result(self) -> None:
        """
        MUT-AT-046, 054: a preemption and a shutdown sent in the same turn
        arrive as one delivery against two master cancels — fewer, not more,
        so not foreign; the run ends with a Result.
        """
        am = _master()
        owner = _Single(am.bus, "Owner", hold=True)
        am.register(owner)
        _reflex(am)
        am.launch(_p("Owner"))
        await owner.started.wait()
        task = am._tasks["Owner"]
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        await am.cancel_all(timeout=1.0)                   # cancels synchronously first
        assert owner._master_cancels == 2
        assert not task.cancelled()
        assert await am.wait_idle(1.0)
        result = _results_by_id(am)["Owner"]
        assert result.cancelled is True and result.preempted is True


# ── Retry decisions ───────────────────────────────────────────────────────────


class TestRetryDecisions:
    async def test_failing_reflex_run_is_not_retried_while_another_reflex_claims_its_resource(
        self,
    ) -> None:
        """
        MUT-AT-030..040, 049, 095: a failed run is not retried while an armed
        reflex claims one of its resources (here the owner is itself a reflex,
        so the claimant waits instead of preempting).
        """
        am = _master()
        owner = _Single(am.bus, "Owner", hold=True, fail_first=1)
        owner.max_retries = 1
        am.register(owner)
        _reflex(am)
        assert am.launch_reflex(_p("Owner", 240)).status is LaunchStatus.LAUNCHED
        await owner.started.wait()
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        owner.release.set()
        assert await am.wait_idle(1.0)
        results = _results_by_id(am)
        assert owner.calls == 1
        assert results["Owner"].success is False
        assert results["R"].success is True

    async def test_failed_run_is_retried_while_an_armed_reflex_claims_a_disjoint_resource(
        self,
    ) -> None:
        """
        MUT-AT-030..040: an armed reflex on other resources does not block the
        retry (the claim check is an intersection, not a union or difference).
        """
        am = _master()
        holder = _Single(am.bus, "Holder", {"buzzer"}, hold=True)
        am.register(holder)
        _reflex(am, {"buzzer"})
        flaky = _Single(am.bus, "Flaky", {"arm"}, fail_first=1)
        flaky.max_retries = 1
        am.register(flaky)
        assert am.launch_reflex(_p("Holder", 240)).status is LaunchStatus.LAUNCHED
        await holder.started.wait()
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        result = await am.dispatch(_p("Flaky"))
        assert flaky.calls == 2
        assert result.success is True
        holder.release.set()
        assert await am.wait_idle(1.0)

    async def test_failed_run_with_a_pending_interrupt_is_not_retried(self) -> None:
        """
        MUT-AT-067, 082: a deliberative interrupt that arrived during the step
        that then aborted blocks the retry — re-running would take the
        actuator back from the requester.
        """
        am = _master()
        seq = _Seq(am.bus, [("work", True)], max_retries=1, abort_in="work")
        seq.gates["work"] = asyncio.Event()
        am.register(seq)
        am.launch(_p("Seq"))
        await _until(lambda: "do work" in seq.log)
        await am.request_interrupt(InterruptRequest(
            new_proposal=_p("Other", 150), requesting_instinct_id="inst",
            target_action_id="Seq",
        ))
        seq.gates["work"].set()
        assert await am.wait_idle(1.0)
        result = _results_by_id(am)["Seq"]
        assert seq.runs == 1
        assert result.success is False
        assert isinstance(result.error, StepAbortError)

    async def test_successful_run_is_not_retried(self) -> None:
        """MUT-AT-078: a successful run is never re-executed, whatever the retry budget."""
        am = _master()
        node = _Single(am.bus, "Once")
        node.max_retries = 2
        am.register(node)
        result = await am.dispatch(_p("Once"))
        assert result.success is True
        assert node.calls == 1

    async def test_interrupted_result_from_execute_is_not_retried(self) -> None:
        """MUT-AT-079, 078: a Result that reports interrupted=True is a stop, not a fault."""
        am = _master()
        node = _Single(am.bus, "Stopped", interrupted_result=True)
        node.max_retries = 1
        am.register(node)
        result = await am.dispatch(_p("Stopped"))
        assert result.success is False and result.interrupted is True
        assert node.calls == 1

    async def test_failed_rollback_outside_a_block_is_not_retried(self) -> None:
        """
        MUT-AT-080: an abort whose rollback walk failed is not retried even
        when the aborting step was interruptible (no block was cut).
        """
        am = _master()
        seq = _Seq(
            am.bus, [("grip", False), ("move", True)],
            policy=InterruptPolicy.ROLLBACK, max_retries=1, abort_in="move", undo="raise",
        )
        am.register(seq)
        result = await am.dispatch(_p("Seq"))
        assert seq.runs == 1
        assert isinstance(result.error, RollbackError)
        assert result.rolled_back is False


# ── The cancel path's Result ──────────────────────────────────────────────────


class TestCancelPathResult:
    async def test_master_cancel_result_reports_cancelled(self) -> None:
        """MUT-AT-065: a run the master cancelled reports cancelled=True."""
        am = _master()
        owner = _Single(am.bus, "Owner", hold=True)
        am.register(owner)
        am.launch(_p("Owner"))
        await owner.started.wait()
        await am.cancel("Owner", timeout=1.0)
        assert await am.wait_idle(1.0)
        result = _results_by_id(am)["Owner"]
        assert result.cancelled is True
        assert result.interrupted is True and result.preempted is False

    async def test_failed_walk_error_survives_a_cancel_landing_during_the_walk(self) -> None:
        """
        MUT-AT-041, 076: a rollback walk that failed, whose Result the cancel
        then swallowed, is still reported — the RollbackError reaches the
        cancel path's Result.
        """
        am = _master()
        seq = _Seq(
            am.bus, [("lower", False), ("move", True)],
            policy=InterruptPolicy.ROLLBACK, undo="swallow",
        )
        seq.gates["lower"] = asyncio.Event()
        am.register(seq)
        _reflex(am)
        am.launch(_p("Seq"))
        await _until(lambda: "do lower" in seq.log)
        await am.request_interrupt(InterruptRequest(
            new_proposal=_p("Other", 150), requesting_instinct_id="inst",
            target_action_id="Seq",
        ))
        seq.gates["lower"].set()                # the block ends: the interrupt walks
        await seq.undo_started.wait()
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        assert await am.wait_idle(1.0)
        results = _results_by_id(am)
        result = results["Seq"]
        assert isinstance(result.error, RollbackError)
        assert result.preempted is True and result.cancelled is True
        assert result.rolled_back is False
        assert results["R"].success is True

    @pytest.mark.parametrize("path", ["preempt", "hand-off"])
    async def test_safe_stop_walk_ending_in_cancelled_error_is_not_rolled_back(
        self, path: str,
    ) -> None:
        """
        MUT-AT-069, 070, 077: a rollback_on_preempt walk that ends in its own
        CancelledError is reported as a RuntimeError, not as a completed
        rollback — on the preempt path and on the WAIT_FOR_BLOCK hand-off.
        """
        am = _master()
        seq = _Seq(
            am.bus, [("grip", False), ("move", True)],
            rollback_on_preempt=True, undo="cancelled",
        )
        am.register(seq)
        _reflex(am)
        if path == "preempt":
            # grip done; the preempt cuts "move", the walk undoes grip.
            seq.gates["move"] = asyncio.Event()
            am.launch(_p("Seq"))
            await _until(lambda: "do move" in seq.log)
            assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        else:
            # armed inside the block; the hand-off at the "move" boundary walks.
            seq.gates["grip"] = asyncio.Event()
            am.launch(_p("Seq"))
            await _until(lambda: "do grip" in seq.log)
            assert am.launch_reflex(
                _p("R", 250), ReflexPreemption.WAIT_FOR_BLOCK,
            ).status is LaunchStatus.ARMED
            seq.gates["grip"].set()
        assert await am.wait_idle(1.0)
        results = _results_by_id(am)
        result = results["Seq"]
        assert "undo grip begin" in seq.log and "undo grip" not in seq.log
        assert result.preempted is True
        assert result.rolled_back is False
        assert isinstance(result.error, RuntimeError)
        assert results["R"].success is True

    @counted_path
    async def test_cancelled_dispatch_caller_is_not_handed_a_stashed_result(self) -> None:
        """
        MUT-AT-043, 074: when a dispatch() caller is itself cancelled while
        its run was preempted before the first step, the caller's own cancel
        wins — it gets CancelledError, not the stashed Result.
        """
        am = _master()
        owner = _Single(am.bus, "Owner", hold=True)
        am.register(owner)
        _reflex(am)
        caller = asyncio.ensure_future(am.dispatch(_p("Owner")))
        await asyncio.sleep(0)                  # the caller awaits; the run is not started
        task = am._tasks["Owner"]
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        await asyncio.sleep(0)                  # the run task is cancelled before its body
        assert task.cancelled()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert owner.calls == 0
        assert await am.wait_idle(1.0)
        assert _results_by_id(am)["R"].success is True


# ── Action timeout (ADR 0009) ─────────────────────────────────────────────────


class TestActionTimeout:
    async def test_timeout_safe_stop_never_runs_the_preempt_walk(self) -> None:
        """
        MUT-AT-071: the timeout's safe-stop runs the hook without the
        rollback_on_preempt walk (that walk is for reflex preemption).
        """
        am = _master()
        seq = _Seq(
            am.bus, [("grip", False), ("move", True)],
            rollback_on_preempt=True, timeout_s=0.05, hang_in="move",
        )
        am.register(seq)
        result = await am.dispatch(_p("Seq"))
        assert seq.log == ["do grip", "do move", "brake (timeout)", "on_timeout"]
        assert isinstance(result.error, ActionTimeoutError)
        assert result.cancelled is True
        assert result.rolled_back is False

    async def test_single_step_timeout_result_is_not_rolled_back(self) -> None:
        """MUT-AT-073: a single-step action's timeout Result has rolled_back=False."""
        am = _master()
        node = _Single(am.bus, "Hang", hold=True)
        node.timeout_s = 0.05
        am.register(node)
        result = await am.dispatch(_p("Hang"))
        assert isinstance(result.error, ActionTimeoutError)
        assert result.cancelled is True
        assert result.rolled_back is False
        assert node.brakes == 1

    async def test_walk_cut_by_the_action_timeout_reports_no_rollback(self) -> None:
        """
        MUT-AT-068: an abort walk the action timeout cut short is reported
        as not rolled back (and the step that was being undone stays listed).
        """
        am = _master()
        seq = _Seq(
            am.bus, [("a", False), ("b", False)],
            policy=InterruptPolicy.ROLLBACK, timeout_s=0.1, step_timeout_s=1.0,
            abort_in="b", undo="hold",
        )
        am.register(seq)
        result = await am.dispatch(_p("Seq"))
        assert seq.log == ["do a", "do b", "undo a begin", "brake (timeout)", "on_timeout"]
        assert result.rolled_back is False
        assert result.stopped_at_step == "b"
        assert [s.step_name for s in result.step_results] == ["a", "b"]
        assert isinstance(result.error, ActionTimeoutError)

    @pytest.mark.parametrize("policy", [InterruptPolicy.ALWAYS, InterruptPolicy.ROLLBACK])
    async def test_preemption_during_on_timeout_undoes_the_block_exactly_once(
        self, policy: InterruptPolicy,
    ) -> None:
        """
        MUT-AT-060, 063 (ALWAYS), 075 (ROLLBACK): a preemption landing in
        on_timeout() runs the rollback_on_preempt walk exactly once — only if
        the abort rules did not already walk.
        """
        am = _master()
        seq = _Seq(
            am.bus, [("grip", False), ("move", True)],
            policy=policy, rollback_on_preempt=True, timeout_s=0.05,
            hang_in="move", timeout_hold=True,
        )
        am.register(seq)
        _reflex(am)
        am.launch(_p("Seq"))
        await seq.timeout_started.wait()
        assert am.launch_reflex(_p("R", 250)).status is LaunchStatus.ARMED
        assert await am.wait_idle(1.0)
        results = _results_by_id(am)
        assert seq.log.count("undo grip") == 1
        assert seq.log.count("brake (timeout)") == 1
        assert results["Seq"].preempted is True
        assert results["Seq"].rolled_back is True
        assert results["R"].success is True

    async def test_failed_abort_walk_after_a_timeout_keeps_the_timeout_result_shape(
        self,
    ) -> None:
        """
        MUT-AT-084: a RollbackError from the timeout's abort walk is carried
        by the timeout Result (cancelled, stopped_at_step, step_results), not
        turned into a bare execute()-raised failure.
        """
        am = _master()
        seq = _Seq(
            am.bus, [("grip", False), ("move", True)],
            policy=InterruptPolicy.ROLLBACK, timeout_s=0.05, hang_in="move", undo="raise",
        )
        am.register(seq)
        result = await am.dispatch(_p("Seq"))
        assert isinstance(result.error, RollbackError)
        assert result.cancelled is True
        assert result.stopped_at_step == "move"
        assert [s.step_name for s in result.step_results] == ["grip"]
        assert result.rolled_back is False
