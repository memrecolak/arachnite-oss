"""
arachnite.nodes.decision
~~~~~~~~~~~~~~~~~~~~~~~~
BaseDecisionNode, DecisionMasterNode, and built-in decision strategies.
Spec reference: Section 5.6.
"""

from __future__ import annotations

import random
from abc import abstractmethod

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.logging import BaseLogSink
from arachnite.models import InterruptRequest, Proposal
from arachnite.nodes.base import BaseNode


class BaseDecisionNode(BaseNode):
    """
    Receives the full list of Proposals from InstinctMasterNode and
    selects exactly one to execute (or None to idle this tick).

    Developer contract:
    - Extend this class and implement decide().
    - The proposals list is always pre-sorted by priority descending.
    - Returning None is valid — it means the agent idles this tick.
    - The strategy can be swapped at runtime via DecisionMasterNode.set_strategy().

    Spec reference: Section 5.6.
    """

    @abstractmethod
    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        """
        Select one Proposal from the list, or return None if no action
        should be taken this tick.

        Receives proposals sorted by priority descending. May be empty.
        """

    async def decide_many(
        self,
        proposals: list[Proposal],
        running_action_ids: set[str] | None = None,
    ) -> list[Proposal]:
        """
        Select multiple proposals for concurrent execution.

        Default implementation: pick the best proposal per unique
        ``action_id`` (by priority descending), skipping any
        ``action_id`` already in ``running_action_ids``.

        Subclasses may override for custom multi-select logic.
        """
        if not proposals:
            return []
        running = running_action_ids or set()
        # Sort by priority desc, urgency desc for stable tie-breaking
        sorted_proposals = sorted(
            proposals, key=lambda p: (p.priority, p.urgency), reverse=True,
        )
        selected: dict[str, Proposal] = {}
        for p in sorted_proposals:
            if p.action_id not in selected and p.action_id not in running:
                selected[p.action_id] = p
        return list(selected.values())


class GreedyDecisionNode(BaseDecisionNode):
    """
    Returns the proposal with the highest priority.
    Ties are broken by urgency (higher wins). Simple and predictable.
    Spec reference: Section 5.6.
    """

    node_id = "GreedyDecisionNode"

    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        if not proposals:
            return None
        # proposals is already sorted by priority desc; break ties by urgency
        return max(proposals, key=lambda p: (p.priority, p.urgency))


class WeightedDecisionNode(BaseDecisionNode):
    """
    Selects the proposal with the highest combined score.
    score = priority × urgency

    Good when multiple instincts partially apply and you want the most
    pressing combination to win rather than just the highest priority.
    Spec reference: Section 5.6.
    """

    node_id = "WeightedDecisionNode"

    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        if not proposals:
            return None
        return max(proposals, key=lambda p: p.priority * p.urgency)


class RandomDecisionNode(BaseDecisionNode):
    """
    Samples probabilistically from proposals weighted by urgency.

    Useful for exploratory or creative agents where deterministic
    behaviour is undesirable.
    Spec reference: Section 5.6.
    """

    node_id = "RandomDecisionNode"

    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        if not proposals:
            return None
        weights = [p.urgency for p in proposals]
        total   = sum(weights)
        if total == 0:
            return random.choice(proposals)
        return random.choices(proposals, weights=weights, k=1)[0]


class DecisionMasterNode(BaseNode):
    """
    Wraps the active DecisionNode and exposes decide() to the runtime.

    Also handles interrupt detection: when a new proposal outranks the
    currently running action, it issues an InterruptRequest.

    Spec reference: Section 5.6.
    """

    node_id = "DecisionMasterNode"

    def __init__(
        self,
        bus: SignalBus,
        strategy: BaseDecisionNode | None = None,
        config: NodeConfig | None = None,
        log_sinks: list[BaseLogSink] | None = None,
        agent_node_id: str = "local",
        max_pending_ticks: int = 50,
    ) -> None:
        super().__init__(bus, config, log_sinks, agent_node_id)
        self._strategy: BaseDecisionNode = strategy or GreedyDecisionNode(bus=bus)
        self._pending: dict[str, Proposal] = {}
        self._pending_ages: dict[str, int] = {}
        self._last_considered: list[Proposal] = []
        self.max_pending_ticks: int = max_pending_ticks

    def set_strategy(self, strategy: BaseDecisionNode) -> None:
        """Swap the decision strategy at runtime."""
        self.logger.info(
            "Decision strategy changed",
            old=type(self._strategy).__name__,
            new=type(strategy).__name__,
        )
        self._strategy = strategy

    @property
    def strategy(self) -> BaseDecisionNode:
        return self._strategy

    def clear_pending(self, instinct_id: str) -> None:
        """Remove any pending proposal and age tracking for the given instinct"""
        self._pending.pop(instinct_id, None)
        self._pending_ages.pop(instinct_id, None)

    @property
    def pending_proposals(self) -> dict[str, Proposal]:
        """Current pending proposals keyed by instinct_id (read-only copy)."""
        return dict(self._pending)

    @property
    def last_considered(self) -> list[Proposal]:
        """All proposals considered in the most recent on_new_proposals_many() call"""
        return list(self._last_considered)

    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        """Select one proposal using the active strategy."""
        chosen = await self._strategy.decide(proposals)
        if chosen:
            self.logger.debug(
                "Decision made",
                chosen_action=chosen.action_id,
                chosen_priority=chosen.priority,
                total_proposals=len(proposals),
            )
        return chosen

    async def on_new_proposals(
        self,
        proposals: list[Proposal],
        current_proposal: Proposal | None,
        action_is_interruptible: bool,
    ) -> tuple[Proposal | None, InterruptRequest | None]:
        """
        Extended entry point called by the runtime when a running action exists.

        If a proposal outranks the running action and the action reports
        is_interruptible(), returns an InterruptRequest alongside the new proposal.

        Returns (chosen_proposal, interrupt_request_or_None).
        """
        chosen = await self.decide(proposals)

        if chosen is None:
            return None, None

        # No running action — no interrupt needed
        if current_proposal is None:
            return chosen, None

        # New proposal outranks current and action is interruptible
        if (
            chosen.priority > current_proposal.priority
            and action_is_interruptible
        ):
            interrupt = InterruptRequest(
                new_proposal           = chosen,
                requesting_instinct_id = chosen.instinct_id,
                reason                 = (
                    f"Higher priority proposal (priority={chosen.priority}) "
                    f"arrived while '{current_proposal.action_id}' was running "
                    f"(priority={current_proposal.priority})"
                ),
            )
            self.logger.info(
                "Issuing interrupt request",
                new_action=chosen.action_id,
                current_action=current_proposal.action_id,
                new_priority=chosen.priority,
            )
            return chosen, interrupt

        return chosen, None

    async def on_new_proposals_many(
        self,
        proposals: list[Proposal],
        running_proposals: dict[str, Proposal],
        running_interruptible: dict[str, bool],
        evaluated_instinct_ids: set[str] | None = None,
    ) -> tuple[list[Proposal], list[InterruptRequest]]:
        """
        Extended entry point for concurrent dispatch.

        Returns a list of proposals to dispatch and a list of interrupt
        requests for running actions that should be pre-empted.

        Manages persistent proposals across ticks.  A ``Proposal`` with
        ``persist=True`` that is not dispatched is carried forward to
        subsequent ticks.  Supersession rules:

        * New ``persist=True`` proposal from the same instinct replaces
          the pending one.
        * An instinct that was evaluated but did not produce a
          ``persist=True`` proposal clears its pending entry (conditions
          changed).
        * Throttled/gated instincts (not in ``evaluated_instinct_ids``)
          keep their pending proposal.
        * Pending proposals exceeding ``max_pending_ticks`` are dropped.

        Args:
            proposals: All proposals from this tick's instinct evaluation.
            running_proposals: Currently running proposals keyed by action_id.
            running_interruptible: Whether each running action is interruptible.
            evaluated_instinct_ids: Instinct IDs that were actually evaluated
                this tick (passed signal gate and throttle).  ``None`` disables
                pending-clear logic (backward compat).
        """
        # ── 1. Update pending: supersede with new persist=True proposals ──
        new_persist_ids: set[str] = set()
        for p in proposals:
            if p.persist:
                new_persist_ids.add(p.instinct_id)
                self._pending[p.instinct_id] = p
                self._pending_ages[p.instinct_id] = 0

        # ── 2. Clear pending for evaluated instincts that didn't persist ──
        if evaluated_instinct_ids is not None:
            for iid in evaluated_instinct_ids:
                if iid not in new_persist_ids and iid in self._pending:
                    del self._pending[iid]
                    self._pending_ages.pop(iid, None)

        # ── 3. Age out stale entries ─────────────────────────────────────
        stale: list[str] = []
        for iid in list(self._pending_ages):
            self._pending_ages[iid] += 1
            if self._pending_ages[iid] > self.max_pending_ticks:
                stale.append(iid)
        for iid in stale:
            del self._pending[iid]
            del self._pending_ages[iid]
            self.logger.debug("Dropped stale pending proposal", instinct_id=iid)

        # ── 4. Merge: new proposals + pending that weren't re-proposed ───
        new_instinct_ids = {p.instinct_id for p in proposals}
        merged = list(proposals)
        for iid, pending_p in self._pending.items():
            if iid not in new_instinct_ids:
                merged.append(pending_p)

        self._last_considered = list(merged)

        # ── 5. Select best proposals (no running filter — need for interrupt check)
        chosen = await self._strategy.decide_many(merged)

        interrupts: list[InterruptRequest] = []
        for prop in chosen:
            # Check if this proposal's action_id is already running
            # with a lower priority and the running action is interruptible
            running = running_proposals.get(prop.action_id)
            if (
                running is not None
                and running_interruptible.get(prop.action_id, False)
                and prop.priority > running.priority
            ):
                    interrupts.append(InterruptRequest(
                        new_proposal           = prop,
                        requesting_instinct_id = prop.instinct_id,
                        reason                 = (
                            f"Higher priority proposal (priority={prop.priority}) "
                            f"arrived while '{running.action_id}' was running "
                            f"(priority={running.priority})"
                        ),
                    ))

        # Filter out proposals whose action_id is running and NOT being interrupted
        interrupted_ids = {ir.new_proposal.action_id for ir in interrupts}
        to_dispatch = [
            p for p in chosen
            if p.action_id not in running_proposals or p.action_id in interrupted_ids
        ]

        # ── 6. Remove dispatched proposals from pending ──────────────────
        for p in to_dispatch:
            self._pending.pop(p.instinct_id, None)
            self._pending_ages.pop(p.instinct_id, None)

        if to_dispatch:
            self.logger.debug(
                "Multi-decision made",
                dispatching=[p.action_id for p in to_dispatch],
                interrupting=[ir.new_proposal.action_id for ir in interrupts],
                total_proposals=len(proposals),
                pending=len(self._pending),
            )
        return to_dispatch, interrupts
