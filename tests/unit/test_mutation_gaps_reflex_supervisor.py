"""
Tests that close the genuine gaps among the surviving mutants of three
safety sites (R2-1, round of 2026-10-27): reflex responsiveness, supervisor
bounded restart and the co-location invariant.

Each test's docstring starts with the survivor id(s) it kills (MUT-RR-nnn,
MUT-SB-nnn, MUT-CL-nnn); the classification lives in
``paper/latex/revizyon_iki/deney_betikleri/mutation_v013/
survivor_classification_20261027_c.csv`` of the paper repository.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

import pytest

from arachnite import SignalBus
from arachnite.context import ContextNode
from arachnite.exceptions import ReflexConflictError
from arachnite.models import Context, NodeState, Proposal, Result
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.base import BaseNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.sense import SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from arachnite.supervisor import NodeSupervisor
from tests.conftest import make_context, settle

HOLD_ACTION = "HoldUntilReleased"


async def _until_true(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = time.perf_counter() + timeout
    while not predicate():
        assert time.perf_counter() < deadline, "condition not reached"
        await asyncio.sleep(0.002)


# ── Reflex nodes whose Proposal priority differs from the node's own ─────────

class _ReflexA(BaseReflexInstinctNode):
    """Node priority 200; proposes at ``proposal_priority``."""
    node_id = "ReflexA"
    priority = 200

    def __init__(self, bus: SignalBus, proposal_priority: int) -> None:
        super().__init__(bus)
        self.proposal_priority = proposal_priority

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(instinct_id=self.node_id, action_id=HOLD_ACTION,
                        priority=self.proposal_priority, urgency=1.0)


class _ReflexB(_ReflexA):
    """Node priority 250; proposes at ``proposal_priority``."""
    node_id = "ReflexB"
    priority = 250


class _CountingInstinct(BaseInstinctNode):
    """Normal instinct counting its evaluate() calls; always proposes."""
    priority = 50

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.calls = 0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.calls += 1
        return Proposal(instinct_id=self.node_id, action_id=HOLD_ACTION,
                        priority=self.priority, urgency=0.5)


class _LiveInstinct(_CountingInstinct):
    node_id = "LiveInstinct"


class _DeadInstinct(_CountingInstinct):
    node_id = "DeadInstinct"


class _LiveReflex(_ReflexA):
    node_id = "LiveReflex"


class _DeadReflex(_ReflexA):
    node_id = "DeadReflex"


class _SwitchedReflex(BaseReflexInstinctNode):
    """Tick-stage reflex proposing the hold action while ``active``."""
    node_id = "SwitchedReflex"
    priority = 250

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.active = False

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if not self.active:
            return None
        return Proposal(instinct_id=self.node_id, action_id=HOLD_ACTION,
                        priority=self.priority, urgency=1.0)


class _HoldUntilReleased(BaseActionNode):
    """Owns the arm until ``release`` is set; counts its executions."""
    node_id = HOLD_ACTION
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.release = asyncio.Event()
        self.executions = 0

    async def execute(self, proposal: Proposal) -> Result:
        self.executions += 1
        await self.release.wait()
        return Result(action_id=self.node_id, success=True)


# ── Supervisor nodes ─────────────────────────────────────────────────────────

class _Restartable(BaseNode):
    """Minimal node whose setup()/teardown() are counted; setup can fail."""

    def __init__(self, bus: SignalBus, *, fail_setup: bool = False) -> None:
        super().__init__(bus)
        self.setup_count = 0
        self.teardown_count = 0
        self._fail_setup = fail_setup

    async def setup(self) -> None:
        self.setup_count += 1
        if self._fail_setup:
            raise RuntimeError("setup failed")

    async def teardown(self) -> None:
        self.teardown_count += 1


class _NodeA(_Restartable):
    node_id = "NodeA"


class _NodeB(_Restartable):
    node_id = "NodeB"


# ── Reflex responsiveness ────────────────────────────────────────────────────

class TestReflexPairsUseProposalPriority:
    @pytest.mark.asyncio
    async def test_pairs_are_ordered_by_proposal_priority_not_node_priority(self) -> None:
        """MUT-RR-007: reflexes are ordered by the Proposal's priority, not the node's own"""
        bus = SignalBus()
        im = InstinctMasterNode(bus=bus)
        a, b = _ReflexA(bus, proposal_priority=260), _ReflexB(bus, proposal_priority=210)
        im.register(a)
        im.register(b)

        pairs = await im.evaluate_reflex_pairs(make_context())

        assert [node.node_id for node, _ in pairs] == ["ReflexA", "ReflexB"]
        assert [p.priority for _, p in pairs] == [260, 210]
        # Every proposal is paired with the node that produced it.
        assert all(p.instinct_id == node.node_id for node, p in pairs)

    @pytest.mark.asyncio
    async def test_raise_policy_detects_a_tie_on_proposal_priority(self) -> None:
        """MUT-RR-009: with reflex_conflict='raise' the tie is judged on Proposal priorities"""
        bus = SignalBus()
        im = InstinctMasterNode(bus=bus, reflex_conflict="raise")
        im.register(_ReflexA(bus, proposal_priority=260))
        im.register(_ReflexB(bus, proposal_priority=260))

        with pytest.raises(ReflexConflictError) as info:
            await im.evaluate_reflex_pairs(make_context())

        assert info.value.priority == 260
        assert set(info.value.node_ids) == {"ReflexA", "ReflexB"}
        assert [node.node_id for node, _ in info.value.fired] == ["ReflexA", "ReflexB"]


class TestDeadNodesAreSkipped:
    @pytest.mark.asyncio
    async def test_dead_node_call_is_skipped_and_its_siblings_still_run(self) -> None:
        """MUT-RR-028/029/031/032: a DEAD node is never called; its live siblings are"""
        bus = SignalBus()
        im = InstinctMasterNode(bus=bus)
        live, dead = _LiveInstinct(bus), _DeadInstinct(bus)
        im.register(live)
        im.register(dead)
        faults: list[str] = []

        async def _on_fault(node_id: str, error: BaseException) -> None:
            faults.append(node_id)

        im.set_fault_handler(_on_fault)
        im.set_dead_nodes({dead.node_id})

        proposals = await im.evaluate_all(make_context())

        assert [p.instinct_id for p in proposals] == [live.node_id]
        assert (live.calls, dead.calls) == (1, 0)
        assert faults == []

    @pytest.mark.asyncio
    async def test_dead_reflex_is_skipped_and_its_sibling_still_fires(self) -> None:
        """MUT-RR-005 (D-1, fixed): a DEAD reflex is skipped; the sibling keeps its own proposal"""
        bus = SignalBus()
        im = InstinctMasterNode(bus=bus)
        live, dead = _LiveReflex(bus, proposal_priority=260), _DeadReflex(bus, 270)
        im.register(dead)
        im.register(live)
        im.set_dead_nodes({dead.node_id})

        pairs = await im.evaluate_reflex_pairs(make_context())

        assert [(node.node_id, p.instinct_id) for node, p in pairs] == [
            (live.node_id, live.node_id),
        ]


class TestReflexLaunchOutcomes:
    @pytest.mark.asyncio
    async def test_deduplicated_reflex_outcome_does_not_abort_the_tick(self) -> None:
        """MUT-RR-017/018/019/020: a DEDUPED reflex launch is handled, not a tick-aborting error"""
        bus = SignalBus()
        im = InstinctMasterNode(bus=bus)
        reflex = _SwitchedReflex(bus)
        im.register(reflex)
        am = ActionMasterNode(bus=bus)
        hold = _HoldUntilReleased(bus)
        am.register(hold)
        rt = ArachniteRuntime(
            sense_master=SenseMasterNode(bus=bus), context=ContextNode(),
            instinct_master=im,
            decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
            action_master=am, bus=bus, tick_rate_hz=50.0, reflex_mode="tick",
        )
        await rt.start()
        await rt.pause()
        await rt.wait_idle(1.0)
        try:
            reflex.active = True
            await rt.tick()                      # LAUNCHED: the hold starts
            await _until_true(lambda: hold.executions == 1)
            assert rt.monitor_state().reflex_action_dispatched is True

            await rt.tick()                      # DEDUPED: already running as a reflex
            assert rt.monitor_state().reflex_action_dispatched is True
            assert hold.executions == 1
        finally:
            reflex.active = False
            hold.release.set()
            await rt.wait_idle(1.0)
            await rt.stop()


# ── Supervisor bounded restart ───────────────────────────────────────────────

class TestSupervisorRestartTasks:
    @pytest.mark.asyncio
    async def test_untrack_cancels_only_the_nodes_own_pending_restart(self) -> None:
        """MUT-SB-007/008/011/016: untrack() cancels that node's pending restart, nobody else's"""
        bus = SignalBus()
        sv = NodeSupervisor(bus=bus, max_restarts=2, restart_delay_s=0.05)
        node_a, node_b = _NodeA(bus), _NodeB(bus)
        sv.track(node_a)
        sv.track(node_b)
        await sv.on_fault(node_a.node_id, RuntimeError("a"))
        await sv.on_fault(node_b.node_id, RuntimeError("b"))
        assert sv.restart_task_count == 2

        sv.untrack(node_a.node_id)
        await settle()
        assert sv.restart_task_count == 1          # A's restart is gone at once

        await _until_true(lambda: sv.state_of(node_b.node_id) is NodeState.RUNNING)
        assert node_b.setup_count == 1
        assert node_a.setup_count == 0
        assert sv.restart_task_count == 0

    @pytest.mark.asyncio
    async def test_scheduled_restart_exhausting_the_budget_ends_quietly(self) -> None:
        """MUT-SB-013: a scheduled restart that exhausts the budget ends without an exception"""
        bus = SignalBus()
        sv = NodeSupervisor(bus=bus, max_restarts=1, restart_delay_s=0.0)
        node = _NodeA(bus, fail_setup=True)
        sv.track(node)

        await sv.on_fault(node.node_id, RuntimeError("boom"))
        tasks = [
            t for t in asyncio.all_tasks()
            if t.get_name() == f"supervisor_restart_{node.node_id}"
        ]
        assert len(tasks) == 1

        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        assert outcomes == [None]
        assert sv.state_of(node.node_id) is NodeState.DEAD
        assert node.setup_count == 1
        assert sv.restart_task_count == 0
