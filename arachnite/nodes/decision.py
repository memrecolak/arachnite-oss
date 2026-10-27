"""
arachnite.nodes.decision
~~~~~~~~~~~~~~~~~~~~~~~~
BaseDecisionNode, DecisionMasterNode, and built-in decision strategies.
Spec reference: Section 5.6.
"""

from __future__ import annotations

import random
from abc import abstractmethod
from collections.abc import Callable
from typing import Any

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.logging import BaseLogSink
from arachnite.models import InterruptRequest, Proposal
from arachnite.nodes.base import BaseNode


class BaseDecisionNode(BaseNode):
    """
    Receives the Proposals of a tick and selects the ones to execute, one
    at a time through decide() (or none, to idle this tick).

    Developer contract:
    - Extend this class and implement decide().
    - The proposals list is always pre-sorted by priority descending.
    - Returning None is valid — it means the agent starts nothing more this
      tick (None on the first call idles the agent).
    - The runtime selects through decide_many(), whose default calls decide()
      repeatedly (see decide_many()); override decide_many() only for custom
      multi-select logic.
    - The strategy can be swapped at runtime via DecisionMasterNode.set_strategy().

    Spec reference: Section 5.6.
    """

    @abstractmethod
    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        """
        Select one Proposal from the list, or return None if no (further)
        action should be taken this tick.

        Receives proposals sorted by priority descending. Must return one of
        the list's own objects (or None).
        """

    async def decide_many(
        self,
        proposals: list[Proposal],
        running_action_ids: set[str] | None = None,
    ) -> list[Proposal]:
        """
        Select the proposals to start this tick by delegating to decide().

        Default implementation: drop the candidates whose ``action_id`` is in
        ``running_action_ids``; then call ``decide()`` on the remaining
        candidates (sorted by priority, then urgency, descending), take its
        choice, drop every candidate with the same ``action_id``, and repeat
        until ``decide()`` returns None or no candidate is left. Returns the
        choices in selection order. A choice that is not one of the offered
        candidates is a strategy error: it is logged and selection stops.

        For GreedyDecisionNode this is the best proposal per ``action_id``,
        highest priority (then urgency) first.

        Subclasses may override for custom multi-select logic.
        """
        running = running_action_ids or set()
        # Stable sort: ties keep the caller's order.
        candidates = sorted(
            (p for p in proposals if p.action_id not in running),
            key=lambda p: (p.priority, p.urgency),
            reverse=True,
        )
        selected: list[Proposal] = []
        while candidates:
            chosen = await self.decide(list(candidates))
            if chosen is None:
                break
            if not any(c is chosen for c in candidates):
                self.logger.error(
                    "Strategy decide() returned a proposal it was not offered; "
                    "selection stopped",
                    strategy=type(self).__name__,
                    action_id=getattr(chosen, "action_id", None),
                    selected=len(selected),
                )
                break
            selected.append(chosen)
            candidates = [c for c in candidates if c.action_id != chosen.action_id]
        return selected


def _best_per_action(
    proposals: list[Proposal],
    running_action_ids: set[str] | None,
    score: Callable[[Proposal], float] | None = None,
) -> list[Proposal]:
    """
    The default decide_many() selection for a strategy whose decide() returns
    the first candidate with the highest ``score`` (Greedy: priority, then
    urgency; Weighted: priority x urgency), in O(n log n) rather than one
    decide() call per choice: rank the candidates as decide_many() offers
    them, stable-sort by ``score``, keep the first per action_id.
    """
    running = running_action_ids or set()
    ranked = sorted(
        (p for p in proposals if p.action_id not in running),
        key=lambda p: (p.priority, p.urgency),
        reverse=True,
    )
    if score is not None:
        ranked.sort(key=score, reverse=True)  # stable: ties keep the offer order
    seen: set[str] = set()
    selected: list[Proposal] = []
    for p in ranked:
        if p.action_id not in seen:
            seen.add(p.action_id)
            selected.append(p)
    return selected


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

    async def decide_many(
        self,
        proposals: list[Proposal],
        running_action_ids: set[str] | None = None,
    ) -> list[Proposal]:
        """The default delegation's result, computed directly (decide() not overridden)"""
        if type(self).decide is not GreedyDecisionNode.decide:
            return await super().decide_many(proposals, running_action_ids)
        return _best_per_action(proposals, running_action_ids)


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

    async def decide_many(
        self,
        proposals: list[Proposal],
        running_action_ids: set[str] | None = None,
    ) -> list[Proposal]:
        """The default delegation's result, computed directly (decide() not overridden)"""
        if type(self).decide is not WeightedDecisionNode.decide:
            return await super().decide_many(proposals, running_action_ids)
        return _best_per_action(
            proposals, running_action_ids, score=lambda p: p.priority * p.urgency,
        )


class RandomDecisionNode(BaseDecisionNode):
    """
    Samples probabilistically from proposals weighted by urgency.

    Useful for exploratory or creative agents where deterministic
    behaviour is undesirable. Pass ``seed`` for a reproducible sequence of
    choices (its own ``random.Random``); without it the module-level
    ``random`` generator is used.
    Spec reference: Section 5.6.
    """

    node_id = "RandomDecisionNode"

    def __init__(self, *args: Any, seed: int | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._rng: random.Random | None = random.Random(seed) if seed is not None else None

    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        if not proposals:
            return None
        weights = [p.urgency for p in proposals]
        total   = sum(weights)
        if self._rng is None:
            if total == 0:
                return random.choice(proposals)
            return random.choices(proposals, weights=weights, k=1)[0]
        if total == 0:
            return self._rng.choice(proposals)
        return self._rng.choices(proposals, weights=weights, k=1)[0]


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
        max_handoff_ticks: int = 100,
    ) -> None:
        super().__init__(bus, config, log_sinks, agent_node_id)
        self._strategy: BaseDecisionNode = strategy or GreedyDecisionNode(bus=bus)
        self._pending: dict[str, Proposal] = {}
        self._pending_ages: dict[str, int] = {}
        # Proposals that were selected but could not start because a running
        # action still owned their action/resources (typically: they just
        # triggered an interrupt and the owner is unwinding). Re-considered
        # every tick until they start, are superseded, or age out.
        self._handoff: dict[str, Proposal] = {}
        self._handoff_ages: dict[str, int] = {}
        self._last_considered: list[Proposal] = []
        self.max_pending_ticks: int = max_pending_ticks
        self.max_handoff_ticks: int = max_handoff_ticks

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
        self._handoff.pop(instinct_id, None)
        self._handoff_ages.pop(instinct_id, None)

    def park(self, proposal: Proposal) -> None:
        """
        Hold a selected proposal for hand-off: it could not start because a
        running action still owns its action_id or resources. It is offered
        to the strategy again on every tick until it starts, its instinct
        makes a new proposal, or ``max_handoff_ticks`` pass.
        """
        if proposal.instinct_id not in self._handoff:
            self._handoff_ages[proposal.instinct_id] = 0
        self._handoff[proposal.instinct_id] = proposal

    def defer(self, proposal: Proposal) -> None:
        """
        Return a selected proposal that could not start (it lost a same-tick
        resource conflict, without interrupting anyone) to the normal pending
        rules: kept for the next tick if ``persist=True``, dropped otherwise.
        """
        if proposal.persist:
            self._pending[proposal.instinct_id] = proposal
            self._pending_ages[proposal.instinct_id] = 0

    @property
    def handoff_proposals(self) -> dict[str, Proposal]:
        """Proposals parked for hand-off, keyed by instinct_id (read-only copy)."""
        return dict(self._handoff)

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
        conflicts: Callable[[Proposal], set[str]] | None = None,
    ) -> tuple[list[Proposal], list[InterruptRequest]]:
        """
        Extended entry point for concurrent dispatch.

        Returns a list of proposals to dispatch and a list of interrupt
        requests for running actions that should be pre-empted.

        A chosen proposal *conflicts* with the running actions named by
        ``conflicts(proposal)`` — by default only a running action with the
        same ``action_id``; the runtime passes
        ``ActionMasterNode.conflicting_owners`` so shared resources count
        too (ADR 0005). With no conflict it is dispatched. When every
        conflicting owner is running, interruptible (per
        ``running_interruptible``) and of lower priority, an
        ``InterruptRequest`` is issued to each owner (``target_action_id``)
        and the proposal is returned for dispatch; if it cannot start yet
        the runtime ``park()``s it for hand-off. The runtime reports every
        deliberative owner as interruptible, also one inside a mandatory
        block: the request is latched on it and honoured at its first stop
        point after the block (ADR 0009). Otherwise — a reflex-owned action
        (reported non-interruptible), or an owner of equal/higher priority —
        it is held: not dispatched, kept only under the ``persist`` rules.

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

        Parked hand-off proposals are superseded only by a new proposal
        (persist or not) from the same instinct, and dropped after
        ``max_handoff_ticks``.

        Args:
            proposals: All proposals from this tick's instinct evaluation.
            running_proposals: Currently running proposals keyed by action_id.
            running_interruptible: Whether deliberation may interrupt each
                running action (False for a reflex-owned action).
            evaluated_instinct_ids: Instinct IDs that were actually evaluated
                this tick (passed signal gate and throttle).  ``None`` disables
                pending-clear logic (backward compat).
            conflicts: Maps a proposal to the action_ids it conflicts with.
        """
        new_instinct_ids = {p.instinct_id for p in proposals}

        # ── 1. Update pending: supersede with new persist=True proposals ──
        new_persist_ids: set[str] = set()
        for p in proposals:
            if p.persist:
                new_persist_ids.add(p.instinct_id)
                self._pending[p.instinct_id] = p
                self._pending_ages[p.instinct_id] = 0

        # ── 2. Clear pending for evaluated instincts that didn't persist.
        #       A parked hand-off proposal was already *selected* (it stopped
        #       its owner), so only a fresh proposal from the same instinct
        #       supersedes it — an instinct going quiet does not cancel it.
        if evaluated_instinct_ids is not None:
            for iid in evaluated_instinct_ids:
                if iid not in new_persist_ids and iid in self._pending:
                    del self._pending[iid]
                    self._pending_ages.pop(iid, None)
        for iid in list(self._handoff):
            if iid in new_instinct_ids:
                del self._handoff[iid]
                self._handoff_ages.pop(iid, None)

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
        for iid in list(self._handoff_ages):
            self._handoff_ages[iid] += 1
            if self._handoff_ages[iid] > self.max_handoff_ticks:
                del self._handoff[iid]
                del self._handoff_ages[iid]
                self.logger.info("Dropped stale hand-off proposal", instinct_id=iid)

        # ── 4. Merge: new proposals + pending/hand-off not re-proposed ───
        merged = list(proposals)
        for iid, pending_p in self._pending.items():
            if iid not in new_instinct_ids:
                merged.append(pending_p)
        for iid, parked_p in self._handoff.items():
            if iid not in new_instinct_ids and iid not in self._pending:
                merged.append(parked_p)

        self._last_considered = list(merged)

        # ── 5. Select best proposals (no running filter — need for interrupt check)
        chosen = await self._strategy.decide_many(merged)

        interrupts: list[InterruptRequest] = []
        to_dispatch: list[Proposal] = []
        for prop in chosen:
            if conflicts is not None:
                owners = conflicts(prop)
            else:
                owners = {prop.action_id} if prop.action_id in running_proposals else set()
            if not owners:
                to_dispatch.append(prop)
                continue
            # Interrupt only if every owner is a running, interruptible,
            # lower-priority action; anything else holds the proposal. Inside
            # a mandatory block the request is latched (ADR 0009).
            owner_props = [running_proposals.get(o) for o in sorted(owners)]
            can_interrupt = all(
                running is not None
                and running_interruptible.get(running.action_id, False)
                and prop.priority > running.priority
                for running in owner_props
            )
            if not can_interrupt:
                continue
            for running in owner_props:
                assert running is not None
                interrupts.append(InterruptRequest(
                    new_proposal           = prop,
                    requesting_instinct_id = prop.instinct_id,
                    reason                 = (
                        f"Higher priority proposal (priority={prop.priority}) "
                        f"arrived while '{running.action_id}' was running "
                        f"(priority={running.priority})"
                    ),
                    target_action_id       = running.action_id,
                ))
            to_dispatch.append(prop)

        # ── 6. Remove dispatched proposals from pending / hand-off ───────
        for p in to_dispatch:
            self._pending.pop(p.instinct_id, None)
            self._pending_ages.pop(p.instinct_id, None)
            self._handoff.pop(p.instinct_id, None)
            self._handoff_ages.pop(p.instinct_id, None)

        if to_dispatch:
            self.logger.debug(
                "Multi-decision made",
                dispatching=[p.action_id for p in to_dispatch],
                interrupting=[ir.target_action_id for ir in interrupts],
                total_proposals=len(proposals),
                pending=len(self._pending),
            )
        return to_dispatch, interrupts
