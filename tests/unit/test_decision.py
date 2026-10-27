"""Unit tests for BaseDecisionNode strategies and DecisionMasterNode."""

from __future__ import annotations

import random

import pytest

from arachnite.bus import SignalBus
from arachnite.context import ContextNode
from arachnite.logging import NullLogSink
from arachnite.models import Context, InterruptRequest, Proposal, Result
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.active_inference import ActiveInferenceDecisionNode
from arachnite.nodes.decision import (
    BaseDecisionNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    RandomDecisionNode,
    WeightedDecisionNode,
)
from arachnite.nodes.instinct import BaseInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import SenseMasterNode
from arachnite.runtime import ArachniteRuntime


def _proposal(
    action_id: str = "Act",
    priority: int = 100,
    urgency: float = 0.5,
    instinct_id: str = "Instinct",
) -> Proposal:
    return Proposal(
        instinct_id=instinct_id,
        action_id=action_id,
        priority=priority,
        urgency=urgency,
    )


def _bus() -> SignalBus:
    return SignalBus()


# ── GreedyDecisionNode ────────────────────────────────────────────────────────

class TestGreedyDecisionNode:
    @pytest.mark.asyncio
    async def test_empty_returns_none(self) -> None:
        node = GreedyDecisionNode(bus=_bus())
        assert await node.decide([]) is None

    @pytest.mark.asyncio
    async def test_single_proposal_returned(self) -> None:
        node = GreedyDecisionNode(bus=_bus())
        p = _proposal("A", priority=50)
        assert await node.decide([p]) is p

    @pytest.mark.asyncio
    async def test_highest_priority_wins(self) -> None:
        node = GreedyDecisionNode(bus=_bus())
        low  = _proposal("Low",  priority=10, urgency=0.9)
        high = _proposal("High", priority=90, urgency=0.1)
        assert await node.decide([low, high]) is high

    @pytest.mark.asyncio
    async def test_tie_broken_by_urgency(self) -> None:
        node = GreedyDecisionNode(bus=_bus())
        a = _proposal("A", priority=100, urgency=0.3)
        b = _proposal("B", priority=100, urgency=0.8)
        assert await node.decide([a, b]) is b


# ── WeightedDecisionNode ──────────────────────────────────────────────────────

class TestWeightedDecisionNode:
    @pytest.mark.asyncio
    async def test_empty_returns_none(self) -> None:
        node = WeightedDecisionNode(bus=_bus())
        assert await node.decide([]) is None

    @pytest.mark.asyncio
    async def test_highest_score_wins(self) -> None:
        node = WeightedDecisionNode(bus=_bus())
        # 100 * 0.2 = 20, 50 * 1.0 = 50  → b wins
        a = _proposal("A", priority=100, urgency=0.2)
        b = _proposal("B", priority=50,  urgency=1.0)
        assert await node.decide([a, b]) is b

    @pytest.mark.asyncio
    async def test_single_proposal(self) -> None:
        node = WeightedDecisionNode(bus=_bus())
        p = _proposal("Only", priority=80, urgency=0.5)
        assert await node.decide([p]) is p


# ── RandomDecisionNode ────────────────────────────────────────────────────────

class TestRandomDecisionNode:
    @pytest.mark.asyncio
    async def test_empty_returns_none(self) -> None:
        node = RandomDecisionNode(bus=_bus())
        assert await node.decide([]) is None

    @pytest.mark.asyncio
    async def test_single_proposal_returned(self) -> None:
        node = RandomDecisionNode(bus=_bus())
        p = _proposal("Only", urgency=0.5)
        assert await node.decide([p]) is p

    @pytest.mark.asyncio
    async def test_zero_urgency_uses_random_choice(self) -> None:
        # All weights zero → random.choice fallback, must not raise
        node = RandomDecisionNode(bus=_bus())
        proposals = [_proposal("A", urgency=0.0), _proposal("B", urgency=0.0)]
        result = await node.decide(proposals)
        assert result in proposals

    @pytest.mark.asyncio
    async def test_positive_urgency_returns_a_proposal(self) -> None:
        node = RandomDecisionNode(bus=_bus())
        proposals = [_proposal("A", urgency=0.2), _proposal("B", urgency=0.8)]
        result = await node.decide(proposals)
        assert result in proposals


# ── DecisionMasterNode ────────────────────────────────────────────────────────

class TestDecisionMasterNodeSetStrategy:
    def test_default_strategy_is_greedy(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        assert isinstance(dm.strategy, GreedyDecisionNode)

    def test_set_strategy_replaces_active(self) -> None:
        bus = _bus()
        dm  = DecisionMasterNode(bus=bus)
        new = WeightedDecisionNode(bus=bus)
        dm.set_strategy(new)
        assert dm.strategy is new

    def test_strategy_property_returns_current(self) -> None:
        bus = _bus()
        dm  = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
        assert isinstance(dm.strategy, WeightedDecisionNode)


class TestDecisionMasterNodeDecide:
    @pytest.mark.asyncio
    async def test_decide_delegates_to_strategy(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        p  = _proposal("Act", priority=100)
        chosen = await dm.decide([p])
        assert chosen is p

    @pytest.mark.asyncio
    async def test_decide_empty_returns_none(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        assert await dm.decide([]) is None


class TestDecisionMasterNodeOnNewProposals:
    @pytest.mark.asyncio
    async def test_no_current_proposal_no_interrupt(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        p  = _proposal("Act", priority=100)
        chosen, interrupt = await dm.on_new_proposals(
            [p], current_proposal=None, action_is_interruptible=True
        )
        assert chosen is p
        assert interrupt is None

    @pytest.mark.asyncio
    async def test_chosen_none_returns_none_none(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        chosen, interrupt = await dm.on_new_proposals(
            [], current_proposal=_proposal("Old"), action_is_interruptible=True
        )
        assert chosen is None
        assert interrupt is None

    @pytest.mark.asyncio
    async def test_outranks_and_interruptible_issues_interrupt(self) -> None:
        dm      = DecisionMasterNode(bus=_bus())
        current = _proposal("SlowAct",  priority=50)
        newer   = _proposal("FastAct",  priority=200, urgency=1.0)
        chosen, interrupt = await dm.on_new_proposals(
            [newer], current_proposal=current, action_is_interruptible=True
        )
        assert chosen is newer
        assert isinstance(interrupt, InterruptRequest)
        assert interrupt.new_proposal is newer
        assert "SlowAct" in interrupt.reason

    @pytest.mark.asyncio
    async def test_outranks_but_not_interruptible_no_interrupt(self) -> None:
        dm      = DecisionMasterNode(bus=_bus())
        current = _proposal("Current", priority=50)
        newer   = _proposal("Newer",   priority=200, urgency=1.0)
        chosen, interrupt = await dm.on_new_proposals(
            [newer], current_proposal=current, action_is_interruptible=False
        )
        assert chosen is newer
        assert interrupt is None

    @pytest.mark.asyncio
    async def test_lower_priority_no_interrupt(self) -> None:
        dm      = DecisionMasterNode(bus=_bus())
        current = _proposal("HighPri", priority=200)
        lower   = _proposal("LowPri",  priority=50, urgency=1.0)
        chosen, interrupt = await dm.on_new_proposals(
            [lower], current_proposal=current, action_is_interruptible=True
        )
        assert chosen is lower
        assert interrupt is None


# ══════════════════════════════════════════════════════════════════════════════
# decide_many tests
# ══════════════════════════════════════════════════════════════════════════════

class TestDecideManyGreedy:
    @pytest.mark.asyncio
    async def test_empty_proposals(self) -> None:
        strategy = GreedyDecisionNode(bus=_bus())
        result = await strategy.decide_many([])
        assert result == []

    @pytest.mark.asyncio
    async def test_one_per_action_id(self) -> None:
        """Selects highest-priority proposal per unique action_id."""
        strategy = GreedyDecisionNode(bus=_bus())
        p1 = _proposal("ActA", priority=100)
        p2 = _proposal("ActA", priority=50)
        p3 = _proposal("ActB", priority=80)
        result = await strategy.decide_many([p1, p2, p3])
        ids = {p.action_id for p in result}
        assert ids == {"ActA", "ActB"}
        for p in result:
            if p.action_id == "ActA":
                assert p.priority == 100

    @pytest.mark.asyncio
    async def test_skips_running_action_ids(self) -> None:
        strategy = GreedyDecisionNode(bus=_bus())
        p1 = _proposal("Running", priority=100)
        p2 = _proposal("Free",    priority=80)
        result = await strategy.decide_many([p1, p2], running_action_ids={"Running"})
        assert len(result) == 1
        assert result[0].action_id == "Free"

    @pytest.mark.asyncio
    async def test_all_running_returns_empty(self) -> None:
        strategy = GreedyDecisionNode(bus=_bus())
        p1 = _proposal("Running", priority=100)
        result = await strategy.decide_many([p1], running_action_ids={"Running"})
        assert result == []


class TestDecideManyWeighted:
    @pytest.mark.asyncio
    async def test_weighted_one_per_action_id(self) -> None:
        from arachnite.nodes.decision import WeightedDecisionNode
        strategy = WeightedDecisionNode(bus=_bus())
        p1 = _proposal("ActA", priority=100, urgency=0.5)
        p2 = _proposal("ActB", priority=80,  urgency=0.9)
        result = await strategy.decide_many([p1, p2])
        assert len(result) == 2


class TestOnNewProposalsMany:
    @pytest.mark.asyncio
    async def test_no_proposals_returns_empty(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        dispatched, interrupts = await dm.on_new_proposals_many(
            [], running_proposals={}, running_interruptible={},
        )
        assert dispatched == []
        assert interrupts == []

    @pytest.mark.asyncio
    async def test_dispatches_non_running(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        p1 = _proposal("ActA", priority=100)
        p2 = _proposal("ActB", priority=80)
        dispatched, interrupts = await dm.on_new_proposals_many(
            [p1, p2], running_proposals={}, running_interruptible={},
        )
        assert len(dispatched) == 2
        assert interrupts == []

    @pytest.mark.asyncio
    async def test_skips_running_unless_outranks(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        running_p = _proposal("ActA", priority=50)
        new_p     = _proposal("ActA", priority=100)
        dispatched, interrupts = await dm.on_new_proposals_many(
            [new_p],
            running_proposals={"ActA": running_p},
            running_interruptible={"ActA": True},
        )
        assert len(interrupts) == 1
        assert interrupts[0].new_proposal.action_id == "ActA"
        assert len(dispatched) == 1

    @pytest.mark.asyncio
    async def test_no_interrupt_when_not_interruptible(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        running_p = _proposal("ActA", priority=50)
        new_p     = _proposal("ActA", priority=100)
        dispatched, interrupts = await dm.on_new_proposals_many(
            [new_p],
            running_proposals={"ActA": running_p},
            running_interruptible={"ActA": False},
        )
        assert interrupts == []
        # Not dispatched either since ActA is running and not interrupted
        assert len(dispatched) == 0


class TestResourceConflictsAndHandOff:
    """ADR 0005: conflicts over shared resources and parked hand-off proposals."""

    @pytest.mark.asyncio
    async def test_interrupt_targets_the_resource_owner(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        owner = _proposal("Patrol", priority=40, instinct_id="Planner")
        new_p = _proposal("Inspect", priority=90, instinct_id="Inspector")
        dispatched, interrupts = await dm.on_new_proposals_many(
            [new_p],
            running_proposals={"Patrol": owner},
            running_interruptible={"Patrol": True},
            conflicts=lambda p: {"Patrol"} if p.action_id == "Inspect" else set(),
        )
        assert [ir.target_action_id for ir in interrupts] == ["Patrol"]
        assert interrupts[0].new_proposal is new_p
        assert dispatched == [new_p]

    @pytest.mark.asyncio
    async def test_conflict_with_non_interruptible_owner_is_held(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        owner = _proposal("Patrol", priority=40)
        new_p = _proposal("Inspect", priority=90, instinct_id="Inspector")
        dispatched, interrupts = await dm.on_new_proposals_many(
            [new_p],
            running_proposals={"Patrol": owner},
            running_interruptible={"Patrol": False},   # mandatory block / reflex-owned
            conflicts=lambda p: {"Patrol"},
        )
        assert dispatched == [] and interrupts == []

    @pytest.mark.asyncio
    async def test_conflict_with_unknown_claimant_is_held(self) -> None:
        """An armed reflex's claim is not a running proposal: never interrupted."""
        dm = DecisionMasterNode(bus=_bus())
        new_p = _proposal("MoveArm", priority=150)
        dispatched, interrupts = await dm.on_new_proposals_many(
            [new_p], running_proposals={}, running_interruptible={},
            conflicts=lambda p: {"Retract"},
        )
        assert dispatched == [] and interrupts == []

    @pytest.mark.asyncio
    async def test_parked_proposal_survives_a_quiet_instinct(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        parked = _proposal("Inspect", priority=90, instinct_id="Inspector")
        dm.park(parked)
        dispatched, _ = await dm.on_new_proposals_many(
            [], running_proposals={}, running_interruptible={},
            evaluated_instinct_ids={"Inspector"},     # evaluated, proposed nothing
        )
        assert dispatched == [parked]
        assert dm.handoff_proposals == {}

    @pytest.mark.asyncio
    async def test_new_proposal_supersedes_parked_one(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        dm.park(_proposal("Inspect", priority=90, instinct_id="Inspector"))
        fresh = _proposal("Wave", priority=70, instinct_id="Inspector")
        dispatched, _ = await dm.on_new_proposals_many(
            [fresh], running_proposals={}, running_interruptible={},
        )
        assert dispatched == [fresh]

    @pytest.mark.asyncio
    async def test_parked_proposal_ages_out(self) -> None:
        dm = DecisionMasterNode(bus=_bus(), max_handoff_ticks=2)
        dm.park(_proposal("Inspect", priority=90, instinct_id="Inspector"))
        blocked = lambda p: {"Patrol"}  # noqa: E731 — owner never releases
        for _ in range(3):
            await dm.on_new_proposals_many(
                [], running_proposals={}, running_interruptible={},
                conflicts=blocked,
            )
        assert dm.handoff_proposals == {}

    def test_defer_keeps_only_persist_proposals_pending(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        keep = Proposal(instinct_id="A", action_id="X", priority=50,
                        urgency=0.5, persist=True)
        drop = _proposal("Y", instinct_id="B")
        dm.defer(keep)
        dm.defer(drop)
        assert dm.pending_proposals == {"A": keep}
        assert dm.handoff_proposals == {}

    @pytest.mark.asyncio
    async def test_clear_pending_drops_parked_proposal(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        dm.park(_proposal("Inspect", instinct_id="Inspector"))
        dm.clear_pending("Inspector")
        assert dm.handoff_proposals == {}


# ══════════════════════════════════════════════════════════════════════════════
# Pending proposal persistence tests
# ══════════════════════════════════════════════════════════════════════════════

class TestPendingProposals:
    @pytest.mark.asyncio
    async def test_pending_empty_by_default(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        assert dm.pending_proposals == {}

    @pytest.mark.asyncio
    async def test_persist_proposal_carried_forward(self) -> None:
        """persist=True proposal not dispatched → stays in pending, dispatched next tick."""
        dm = DecisionMasterNode(bus=_bus())
        p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        # Tick 1: ActA is running with higher priority, not interruptible
        dispatched, _ = await dm.on_new_proposals_many(
            [p],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        assert dispatched == []
        assert "InstX" in dm.pending_proposals

        # Tick 2: ActA finished, InstX throttled → pending carried forward
        dispatched, _ = await dm.on_new_proposals_many(
            [],
            running_proposals={},
            running_interruptible={},
            evaluated_instinct_ids=set(),
        )
        assert len(dispatched) == 1
        assert dispatched[0].instinct_id == "InstX"
        assert "InstX" not in dm.pending_proposals

    @pytest.mark.asyncio
    async def test_non_persist_proposal_not_carried(self) -> None:
        """persist=False proposal not dispatched → gone forever."""
        dm = DecisionMasterNode(bus=_bus())
        p = _proposal("ActA", priority=80, instinct_id="InstX")
        # persist defaults to False
        dispatched, _ = await dm.on_new_proposals_many(
            [p],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        assert dispatched == []
        assert dm.pending_proposals == {}

    @pytest.mark.asyncio
    async def test_supersession_by_instinct_id(self) -> None:
        """New persist=True proposal from same instinct replaces pending."""
        dm = DecisionMasterNode(bus=_bus())
        old_p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        # Tick 1: not dispatched
        await dm.on_new_proposals_many(
            [old_p],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        assert dm.pending_proposals["InstX"].priority == 80

        # Tick 2: same instinct, new proposal with higher priority
        new_p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=90, urgency=0.7, persist=True,
        )
        await dm.on_new_proposals_many(
            [new_p],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        assert dm.pending_proposals["InstX"].priority == 90

    @pytest.mark.asyncio
    async def test_clear_on_none_return(self) -> None:
        """Instinct evaluated + no proposal → clears pending."""
        dm = DecisionMasterNode(bus=_bus())
        p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        # Tick 1: pending added
        await dm.on_new_proposals_many(
            [p],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        assert "InstX" in dm.pending_proposals

        # Tick 2: InstX evaluated but returned None (conditions changed)
        await dm.on_new_proposals_many(
            [],
            running_proposals={},
            running_interruptible={},
            evaluated_instinct_ids={"InstX"},
        )
        assert "InstX" not in dm.pending_proposals

    @pytest.mark.asyncio
    async def test_throttled_instinct_keeps_pending(self) -> None:
        """Instinct not in evaluated_ids (throttled) → pending stays."""
        dm = DecisionMasterNode(bus=_bus())
        p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        # Tick 1: not dispatched
        await dm.on_new_proposals_many(
            [p],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        assert "InstX" in dm.pending_proposals

        # Tick 2: InstX is throttled (not in evaluated_ids)
        await dm.on_new_proposals_many(
            [],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids=set(),  # InstX not evaluated
        )
        assert "InstX" in dm.pending_proposals

    @pytest.mark.asyncio
    async def test_max_pending_ticks_drops_stale(self) -> None:
        """Proposal exceeding max_pending_ticks is dropped."""
        dm = DecisionMasterNode(bus=_bus(), max_pending_ticks=2)
        p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        running = {"ActA": _proposal("ActA", priority=100, instinct_id="InstY")}
        interruptible = {"ActA": False}

        # Tick 1: added, age → 1
        await dm.on_new_proposals_many(
            [p], running_proposals=running, running_interruptible=interruptible,
            evaluated_instinct_ids={"InstX"},
        )
        assert "InstX" in dm.pending_proposals

        # Tick 2: age → 2, still ≤ max
        await dm.on_new_proposals_many(
            [], running_proposals=running, running_interruptible=interruptible,
            evaluated_instinct_ids=set(),
        )
        assert "InstX" in dm.pending_proposals

        # Tick 3: age → 3, exceeds max_pending_ticks=2 → dropped
        await dm.on_new_proposals_many(
            [], running_proposals=running, running_interruptible=interruptible,
            evaluated_instinct_ids=set(),
        )
        assert "InstX" not in dm.pending_proposals

    @pytest.mark.asyncio
    async def test_dispatched_proposal_removed_from_pending(self) -> None:
        """Selected and dispatched proposal is removed from pending."""
        dm = DecisionMasterNode(bus=_bus())
        p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        dispatched, _ = await dm.on_new_proposals_many(
            [p],
            running_proposals={},
            running_interruptible={},
            evaluated_instinct_ids={"InstX"},
        )
        assert len(dispatched) == 1
        assert dm.pending_proposals == {}

    @pytest.mark.asyncio
    async def test_pending_merged_into_decide_pool(self) -> None:
        """Pending proposals participate alongside new proposals in decide_many."""
        dm = DecisionMasterNode(bus=_bus())
        # Tick 1: InstX persist=True, not dispatched (ActA running)
        px = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        await dm.on_new_proposals_many(
            [px],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )

        # Tick 2: InstY fires for ActB, InstX throttled, ActA now free
        py = _proposal("ActB", priority=70, instinct_id="InstY")
        dispatched, _ = await dm.on_new_proposals_many(
            [py],
            running_proposals={},
            running_interruptible={},
            evaluated_instinct_ids={"InstY"},
        )
        action_ids = {p.action_id for p in dispatched}
        assert "ActA" in action_ids  # from pending
        assert "ActB" in action_ids  # from new proposals

    @pytest.mark.asyncio
    async def test_pending_proposals_accessor(self) -> None:
        """Public accessor returns current pending (read-only copy)."""
        dm = DecisionMasterNode(bus=_bus())
        p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        await dm.on_new_proposals_many(
            [p],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        pending = dm.pending_proposals
        assert "InstX" in pending
        # Mutating the copy should not affect internal state
        pending.pop("InstX")
        assert "InstX" in dm.pending_proposals


# ══════════════════════════════════════════════════════════════════════════════
# clear_pending tests
# ══════════════════════════════════════════════════════════════════════════════

class TestClearPending:
    @pytest.mark.asyncio
    async def test_clear_pending_removes_pending_and_ages(self) -> None:
        """clear_pending() removes both _pending and _pending_ages for the instinct"""
        dm = DecisionMasterNode(bus=_bus())
        p = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        # Place InstX into pending by running a tick where it can't dispatch
        await dm.on_new_proposals_many(
            [p],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        assert "InstX" in dm.pending_proposals
        assert "InstX" in dm._pending_ages

        dm.clear_pending("InstX")

        assert "InstX" not in dm.pending_proposals
        assert "InstX" not in dm._pending_ages

    def test_clear_pending_nonexistent_is_noop(self) -> None:
        """clear_pending() on unknown instinct_id does not raise"""
        dm = DecisionMasterNode(bus=_bus())
        dm.clear_pending("NoSuchInstinct")  # must not raise
        assert dm.pending_proposals == {}


# ══════════════════════════════════════════════════════════════════════════════
# last_considered tests (A-10)
# ══════════════════════════════════════════════════════════════════════════════

class TestLastConsidered:
    def test_last_considered_empty_by_default(self) -> None:
        dm = DecisionMasterNode(bus=_bus())
        assert dm.last_considered == []

    @pytest.mark.asyncio
    async def test_last_considered_includes_fresh_and_pending(self) -> None:
        """last_considered must contain both new proposals and carried-forward pending ones"""
        dm = DecisionMasterNode(bus=_bus())
        # Tick 1: InstX persist=True, not dispatched (ActA running)
        px = Proposal(
            instinct_id="InstX", action_id="ActA",
            priority=80, urgency=0.5, persist=True,
        )
        await dm.on_new_proposals_many(
            [px],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstX"},
        )
        assert "InstX" in dm.pending_proposals

        # Tick 2: InstX throttled (not evaluated), new InstZ fires for ActB
        pz = _proposal("ActB", priority=70, instinct_id="InstZ")
        await dm.on_new_proposals_many(
            [pz],
            running_proposals={"ActA": _proposal("ActA", priority=100, instinct_id="InstY")},
            running_interruptible={"ActA": False},
            evaluated_instinct_ids={"InstZ"},
        )
        considered = dm.last_considered
        considered_ids = {p.instinct_id for p in considered}
        # Must include both the fresh proposal (InstZ) and the pending one (InstX)
        assert "InstZ" in considered_ids
        assert "InstX" in considered_ids

    @pytest.mark.asyncio
    async def test_last_considered_returns_copy(self) -> None:
        """Mutating last_considered must not affect internal state"""
        dm = DecisionMasterNode(bus=_bus())
        p = _proposal("ActA", priority=80, instinct_id="InstX")
        await dm.on_new_proposals_many(
            [p],
            running_proposals={},
            running_interruptible={},
            evaluated_instinct_ids={"InstX"},
        )
        copy = dm.last_considered
        copy.clear()
        assert len(dm.last_considered) > 0

    @pytest.mark.asyncio
    async def test_last_considered_updated_each_call(self) -> None:
        """last_considered reflects the most recent on_new_proposals_many() call"""
        dm = DecisionMasterNode(bus=_bus())
        p1 = _proposal("ActA", priority=80, instinct_id="InstX")
        await dm.on_new_proposals_many(
            [p1],
            running_proposals={},
            running_interruptible={},
            evaluated_instinct_ids={"InstX"},
        )
        assert len(dm.last_considered) == 1

        # Second call with different proposals
        p2 = _proposal("ActB", priority=60, instinct_id="InstY")
        p3 = _proposal("ActC", priority=40, instinct_id="InstZ")
        await dm.on_new_proposals_many(
            [p2, p3],
            running_proposals={},
            running_interruptible={},
            evaluated_instinct_ids={"InstY", "InstZ"},
        )
        assert len(dm.last_considered) == 2
        ids = {p.instinct_id for p in dm.last_considered}
        assert ids == {"InstY", "InstZ"}


# ── Sixth review: the strategy's decide() drives selection (R6-F1) ────────────


def _old_decide_many(
    proposals: list[Proposal], running_action_ids: set[str] | None = None,
) -> list[Proposal]:
    """The pre-R6 BaseDecisionNode.decide_many(), kept verbatim as the Greedy reference"""
    if not proposals:
        return []
    running = running_action_ids or set()
    sorted_proposals = sorted(
        proposals, key=lambda p: (p.priority, p.urgency), reverse=True,
    )
    selected: dict[str, Proposal] = {}
    for p in sorted_proposals:
        if p.action_id not in selected and p.action_id not in running:
            selected[p.action_id] = p
    return list(selected.values())


class _OneShot(BaseInstinctNode):
    """Proposes ``action_id`` with a fixed priority and urgency every tick."""

    def __init__(
        self, bus: SignalBus, node_id: str, action_id: str, priority: int, urgency: float,
    ) -> None:
        super().__init__(bus=bus)
        self.node_id = node_id
        self.priority = priority
        self._action_id = action_id
        self._urgency = urgency

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id=self._action_id,
            priority=self.priority, urgency=self._urgency,
            parameters={"who": self.node_id},
        )


class _Recorder(BaseActionNode):
    def __init__(self, bus: SignalBus, node_id: str, log: list[str]) -> None:
        super().__init__(bus=bus)
        self.node_id = node_id
        self._log = log

    async def execute(self, proposal: Proposal) -> Result:
        self._log.append(f"{self.node_id}:{proposal.parameters['who']}")
        return Result(action_id=self.node_id, success=True)


class _IdleStrategy(BaseDecisionNode):
    node_id = "IdleStrategy"

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus=bus)
        self.calls = 0

    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        self.calls += 1
        return None


class _ForeignStrategy(BaseDecisionNode):
    """Returns a proposal it was never offered (a strategy bug)."""

    node_id = "ForeignStrategy"

    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        return _proposal("Elsewhere", priority=999)


async def _run_one_tick(
    strategy: BaseDecisionNode, instincts: list[tuple[str, str, int, float]],
) -> list[str]:
    log: list[str] = []
    bus = strategy.bus
    im = InstinctMasterNode(bus=bus)
    am = ActionMasterNode(bus=bus)
    for node_id, action_id, priority, urgency in instincts:
        im.register(_OneShot(bus, node_id, action_id, priority, urgency))
    for action_id in sorted({a for _, a, _, _ in instincts}):
        am.register(_Recorder(bus, action_id, log))
    rt = ArachniteRuntime(
        sense_master=SenseMasterNode(bus=bus), context=ContextNode(),
        instinct_master=im,
        decision_master=DecisionMasterNode(bus=bus, strategy=strategy),
        action_master=am, bus=bus, log_sinks=[NullLogSink()],
    )
    await rt.tick()
    assert await rt.wait_idle(2.0)
    return sorted(log)


class TestSixthReviewRegressions:
    @pytest.mark.asyncio
    async def test_weighted_strategy_selects_at_runtime(self) -> None:
        """R6-F1: the runtime runs what WeightedDecisionNode.decide() picks, not the top priority"""
        ran = await _run_one_tick(
            WeightedDecisionNode(bus=_bus()),
            [("A", "Act", 60, 0.1), ("B", "Act", 50, 1.0)],   # scores 6 vs 50
        )
        assert ran == ["Act:B"]

    @pytest.mark.asyncio
    async def test_strategy_returning_none_idles_the_agent(self) -> None:
        """R6-F1: a strategy whose decide() returns None starts nothing"""
        strategy = _IdleStrategy(_bus())
        ran = await _run_one_tick(strategy, [("A", "Act", 60, 0.5), ("C", "Other", 40, 0.5)])
        assert ran == []
        assert strategy.calls == 1

    @pytest.mark.asyncio
    async def test_decide_many_calls_decide_on_the_remaining_candidates(self) -> None:
        """R6-F1: decide() is called repeatedly; each choice removes its action_id"""
        offered: list[list[str]] = []

        class _Spy(WeightedDecisionNode):
            async def decide(self, proposals: list[Proposal]) -> Proposal | None:
                offered.append([f"{p.action_id}/{p.priority}" for p in proposals])
                return await super().decide(proposals)

        a_hi = _proposal("A", priority=90, urgency=0.1)    # score 9
        a_lo = _proposal("A", priority=20, urgency=1.0)    # score 20
        b    = _proposal("B", priority=50, urgency=0.5)    # score 25
        run  = _proposal("R", priority=99, urgency=1.0)    # running: never offered
        chosen = await _Spy(bus=_bus()).decide_many([a_lo, b, a_hi, run], {"R"})
        assert chosen == [b, a_lo]
        assert offered == [["A/90", "B/50", "A/20"], ["A/90", "A/20"]]

    @pytest.mark.asyncio
    async def test_choice_that_was_not_offered_stops_selection(self) -> None:
        """R6-F1: decide() returning a foreign proposal is a strategy error; nothing is selected"""
        chosen = await _ForeignStrategy(bus=_bus()).decide_many(
            [_proposal("A", priority=60), _proposal("B", priority=50)],
        )
        assert chosen == []

    @pytest.mark.asyncio
    async def test_decision_master_uses_the_strategy(self) -> None:
        """R6-F1: on_new_proposals_many() dispatches the strategy's choice"""
        dm = DecisionMasterNode(bus=_bus(), strategy=WeightedDecisionNode(bus=_bus()))
        hi = _proposal("Act", priority=60, urgency=0.1, instinct_id="A")
        lo = _proposal("Act", priority=50, urgency=1.0, instinct_id="B")
        dispatched, _ = await dm.on_new_proposals_many(
            [hi, lo], running_proposals={}, running_interruptible={},
        )
        assert dispatched == [lo]

    @pytest.mark.asyncio
    async def test_random_strategy_is_reproducible_with_a_seed(self) -> None:
        """R6-F1: RandomDecisionNode(seed=...) repeats its sequence of choices"""
        props = [_proposal(f"A{i}", priority=50, urgency=0.1 * (i + 1)) for i in range(6)]

        async def picks(seed: int) -> list[str]:
            node = RandomDecisionNode(bus=_bus(), seed=seed)
            out = []
            for _ in range(20):
                chosen = await node.decide(props)
                assert chosen is not None
                out.append(chosen.action_id)
            return out

        assert await picks(7) == await picks(7)

    @pytest.mark.asyncio
    async def test_active_inference_softmax_is_reproducible_with_a_seed(self) -> None:
        """R6-F1: ActiveInferenceDecisionNode(seed=...) repeats its softmax choices"""
        props = [_proposal(f"A{i}", priority=10 * (i + 1), urgency=0.5) for i in range(5)]

        async def picks(seed: int) -> list[str]:
            node = ActiveInferenceDecisionNode(bus=_bus(), temperature=20.0, seed=seed)
            out = []
            for _ in range(20):
                chosen = await node.decide(props)
                assert chosen is not None
                out.append(chosen.action_id)
            return out

        assert await picks(3) == await picks(3)


class TestGreedySelectionUnchanged:
    @pytest.mark.asyncio
    async def test_greedy_decide_many_matches_the_pre_r6_selection(self) -> None:
        """R6-F1 pin: GreedyDecisionNode selects exactly what the old default did, in order"""
        rng = random.Random(20261027)
        greedy = GreedyDecisionNode(bus=_bus())
        for _ in range(2000):
            n = rng.randint(0, 12)
            props = [
                _proposal(
                    f"A{rng.randint(0, 5)}",
                    priority=rng.choice([10, 50, 50, 80, 150, 200]),
                    urgency=rng.choice([0.0, 0.3, 0.3, 0.9, 1.0]),
                    instinct_id=f"I{i}",
                )
                for i in range(n)
            ]
            running = {f"A{rng.randint(0, 5)}"} if rng.random() < 0.3 else None
            got = await greedy.decide_many(list(props), running)
            want = _old_decide_many(list(props), running)
            assert [id(p) for p in got] == [id(p) for p in want]

    @pytest.mark.asyncio
    async def test_greedy_and_weighted_shortcuts_match_the_delegation(self) -> None:
        """R6-F1 pin: Greedy/Weighted decide_many() equals the decide()-delegating default"""
        rng = random.Random(1027)
        strategies = [GreedyDecisionNode(bus=_bus()), WeightedDecisionNode(bus=_bus())]
        for _ in range(2000):
            props = [
                _proposal(
                    f"A{rng.randint(0, 5)}",
                    priority=rng.choice([10, 20, 50, 80, 100, 200]),
                    urgency=rng.choice([0.0, 0.1, 0.25, 0.5, 0.8, 1.0]),
                    instinct_id=f"I{i}",
                )
                for i in range(rng.randint(0, 12))
            ]
            running = {f"A{rng.randint(0, 5)}"} if rng.random() < 0.3 else None
            for strategy in strategies:
                fast = await strategy.decide_many(list(props), running)
                general = await BaseDecisionNode.decide_many(strategy, list(props), running)
                assert [id(p) for p in fast] == [id(p) for p in general]
