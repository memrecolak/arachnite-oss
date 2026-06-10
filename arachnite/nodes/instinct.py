"""
arachnite.nodes.instinct
~~~~~~~~~~~~~~~~~~~~~~~~
BaseInstinctNode, BaseReflexInstinctNode, and InstinctMasterNode.
Spec reference: Sections 5.4, 5.5.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
import time
from abc import abstractmethod
from collections.abc import Callable, Sequence
from typing import Any

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.exceptions import NodeRegistrationError, ReflexConflictError
from arachnite.logging import BaseLogSink
from arachnite.models import Context, Proposal
from arachnite.nodes.base import BaseNode


class BaseInstinctNode(BaseNode):
    """
    Observes the Context and, if the situation warrants it,
    produces a Proposal recommending a specific action.

    Developer contract:
    - Extend this class and implement evaluate().
    - Set priority: 100-199 safety, 50-99 goal-directed, 1-49 exploratory.
    - Return None explicitly when this instinct does not apply.
    - Keep evaluate() fast and stateless where possible.
    - Use ctx.state for any persistent reasoning state.

    Spec reference: Section 5.4.
    """

    #: Higher priority proposals win in greedy strategy.
    priority: int = 50

    #: Can be toggled at runtime to disable this instinct.
    enabled: bool = True

    #: True for ReflexInstinctNode subclasses — do not override.
    reflex: bool = False

    #: Minimum seconds between consecutive evaluate() calls.
    #: None (default) means evaluate() is called every tick.
    #: Set to e.g. 30.0 for reflection/curiosity instincts that should
    #: fire at most once per 30 seconds regardless of tick rate.
    trigger_interval_s: float | None = None

    #: Signal kinds that activate this instinct.  When set, evaluate()
    #: is only called if at least one signal in ctx.signals has a kind
    #: in this list.  None (default) means evaluate() is called every
    #: tick regardless of which signals are present.
    #: Example: trigger_on_signals = ["face", "speech", "proximity"]
    trigger_on_signals: list[str] | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # ``-inf`` so the first call always passes the throttle check
        # ``(now - _last_trigger_s) < trigger_interval_s`` regardless of the
        # absolute value of ``time.monotonic()`` on this machine. Using ``0.0``
        # used to make the first evaluation skip on freshly-booted systems
        # where ``time.monotonic()`` is smaller than ``trigger_interval_s``.
        self._last_trigger_s: float = -math.inf

    @abstractmethod
    async def evaluate(self, ctx: Context) -> Proposal | None:
        """
        Inspect the context. Return a Proposal if this instinct applies,
        or None if the situation does not trigger it.
        """

    async def on_proposal_rejected(self, proposal: Proposal) -> None:
        """
        Called if this instinct produced a proposal but DecisionNode chose
        a different one. Override to log or learn from outcomes.
        """


class BaseReflexInstinctNode(BaseInstinctNode):
    """
    A ReflexInstinctNode bypasses the DecisionNode entirely.

    If evaluate() returns a Proposal, InstinctMasterNode dispatches it
    directly via ActionMasterNode before the normal instinct pass runs.

    This models the biological reflex arc: a signal pathway that triggers
    a motor response without involving the brain. The target ActionNode
    MUST be co-located on the same AgentNode as this reflex.

    Developer contract:
    - Extend this class instead of BaseInstinctNode for emergency/safety responses.
    - Keep evaluate() extremely fast — it runs before the rest of the pipeline.
    - No LLM calls, no blocking I/O.
    - Use priority ≥ 200 to distinguish from normal instincts.
    - Set reflex: bool = True (already set by this class).

    Spec reference: Section 5.5.
    """

    reflex: bool = True
    priority: int = 200


class InstinctMasterNode(BaseNode):
    """
    Evaluates all registered instinct nodes and collects proposals.

    Maintains two separate registries:
    - Reflex nodes (reflex=True): evaluated first, bypass DecisionNode.
    - Normal nodes: evaluated after reflexes, proposals go to DecisionNode.

    Spec reference: Sections 5.4, 5.5.
    """

    node_id = "InstinctMasterNode"

    #: How to handle two reflex nodes with identical priority firing in one tick.
    #: 'raise'       — raise ReflexConflictError
    #: 'dispatch_all'— dispatch all in priority order (first registered wins ties)
    reflex_conflict: str = "dispatch_all"

    def __init__(
        self,
        bus: SignalBus,
        config: NodeConfig | None = None,
        log_sinks: list[BaseLogSink] | None = None,
        agent_node_id: str = "local",
        reflex_conflict: str = "dispatch_all",
    ) -> None:
        super().__init__(bus, config, log_sinks, agent_node_id)
        self.reflex_conflict = reflex_conflict
        self._normal_nodes: dict[str, BaseInstinctNode] = {}
        self._reflex_nodes: dict[str, BaseReflexInstinctNode] = {}
        self._last_evaluated_ids: set[str] = set()
        self._pre_evaluate_gate: (
            Callable[[BaseInstinctNode, Context], bool] | None
        ) = None

    def register(self, node: BaseInstinctNode) -> None:
        """
        Register a normal or reflex instinct node.
        Reflex nodes (node.reflex == True) go to the reflex registry.
        Raises NodeRegistrationError on duplicate node_id, or if a reflex
        node has priority < 200 (the documented reflex band).
        """
        if node.reflex and node.priority < 200:
            raise NodeRegistrationError(
                node.node_id,
                self.node_id,
                reason=(
                    f"Reflex node '{node.node_id}' has priority {node.priority}; "
                    "reflex priorities must be >= 200. Either raise the priority "
                    "or extend BaseInstinctNode instead of BaseReflexInstinctNode."
                ),
            )
        registry = self._reflex_nodes if node.reflex else self._normal_nodes
        if node.node_id in registry:
            raise NodeRegistrationError(node.node_id, self.node_id)
        registry[node.node_id] = node  # type: ignore[assignment]
        kind = "reflex" if node.reflex else "normal"
        self.logger.debug(
            "Registered instinct node",
            instinct_node_id=node.node_id,
            kind=kind,
            priority=node.priority,
        )
        if node.reflex and node.trigger_interval_s is not None:
            self.logger.warning(
                "Reflex trigger_interval_s ignored",
                instinct_node_id=node.node_id,
                trigger_interval_s=node.trigger_interval_s,
            )

    def get_node(self, node_id: str) -> BaseInstinctNode | None:
        """Return a registered instinct node by ID, or None"""
        return self._normal_nodes.get(node_id) or self._reflex_nodes.get(node_id)

    def unregister(self, node_id: str) -> None:
        """Remove a node from either registry. Silent if not found."""
        self._normal_nodes.pop(node_id, None)
        self._reflex_nodes.pop(node_id, None)

    @property
    def normal_nodes(self) -> Sequence[BaseInstinctNode]:
        return list(self._normal_nodes.values())

    @property
    def reflex_nodes(self) -> Sequence[BaseReflexInstinctNode]:
        return list(self._reflex_nodes.values())

    async def setup(self) -> None:
        all_nodes = [*self._normal_nodes.values(), *self._reflex_nodes.values()]
        await asyncio.gather(*(n.setup() for n in all_nodes))

    async def teardown(self) -> None:
        all_nodes = [*self._normal_nodes.values(), *self._reflex_nodes.values()]
        await asyncio.gather(*(n.cancel_background_tasks() for n in all_nodes))
        await asyncio.gather(*(n.teardown() for n in all_nodes))

    async def on_pause(self) -> None:
        all_nodes = [*self._normal_nodes.values(), *self._reflex_nodes.values()]
        await asyncio.gather(*(n.on_pause() for n in all_nodes))

    async def on_resume(self) -> None:
        all_nodes = [*self._normal_nodes.values(), *self._reflex_nodes.values()]
        await asyncio.gather(*(n.on_resume() for n in all_nodes))

    async def notify_tick_start(self, tick: int) -> None:
        all_nodes = [*self._normal_nodes.values(), *self._reflex_nodes.values()]
        await asyncio.gather(*(n.on_tick_start(tick) for n in all_nodes))

    async def notify_tick_end(self, tick: int, duration_s: float) -> None:
        all_nodes = [*self._normal_nodes.values(), *self._reflex_nodes.values()]
        await asyncio.gather(*(n.on_tick_end(tick, duration_s) for n in all_nodes))

    async def notify_rejected(self, rejected: list[Proposal]) -> None:
        """Notify instinct nodes that their proposals were not selected."""
        for p in rejected:
            node = self._normal_nodes.get(p.instinct_id)
            if node is not None:
                with contextlib.suppress(Exception):
                    await node.on_proposal_rejected(p)

    async def evaluate_reflexes(self, ctx: Context) -> list[Proposal]:
        """
        Evaluate all ReflexInstinctNodes concurrently.

        Returns non-None proposals sorted by priority descending.
        Called by the runtime before evaluate_all().
        """
        if not self._reflex_nodes:
            return []

        enabled = [n for n in self._reflex_nodes.values() if n.enabled]

        async def _eval(node: BaseReflexInstinctNode) -> Proposal | None:
            try:
                return await node.evaluate(ctx)
            except Exception as exc:  # noqa: BLE001
                self.logger.error(
                    "Reflex instinct raised",
                    node_id=node.node_id,
                    error=str(exc),
                )
                return None

        results = await asyncio.gather(*(_eval(n) for n in enabled))
        proposals = [p for p in results if p is not None]
        proposals.sort(key=lambda p: p.priority, reverse=True)

        # Check for priority conflicts if policy is 'raise'
        if self.reflex_conflict == "raise" and len(proposals) > 1:
            top_priority = proposals[0].priority
            conflicts = [p for p in proposals if p.priority == top_priority]
            if len(conflicts) > 1:
                raise ReflexConflictError(
                    top_priority,
                    [p.instinct_id for p in conflicts],
                )

        self.logger.debug(
            "evaluate_reflexes complete",
            proposals=len(proposals),
        )
        return proposals

    @property
    def last_evaluated_ids(self) -> set[str]:
        """
        Node IDs of instincts that were actually evaluated (passed signal
        gate, throttle, and any installed pre-evaluate gate) in the most
        recent evaluate_all() call.

        Used by DecisionMasterNode to distinguish "instinct returned None"
        (clear its pending proposal) from "instinct was throttled/gated"
        (keep its pending proposal).
        """
        return self._last_evaluated_ids

    def set_pre_evaluate_gate(
        self,
        gate: Callable[[BaseInstinctNode, Context], bool] | None,
    ) -> None:
        """
        Install (or clear) an external policy gate consulted by
        evaluate_all() before each enabled normal instinct's evaluate()
        call. Returning False skips that node for the current tick.

        Intended for cross-instinct policies the framework does not own —
        e.g. single-LLM contention scheduling where only one instinct
        may hold the model slot per tick. The per-node
        ``trigger_on_signals`` and ``trigger_interval_s`` skips run
        first; the gate only sees nodes those would have evaluated.

        Contract:
        - Must be a sync function. Async gates are rejected at install
          time because their coroutine return value is always truthy
          and the bug would be silent at evaluation time.
        - Must be fast and side-effect-free. The gate runs inside the
          tick loop's ``asyncio.gather`` over all enabled instincts;
          blocking I/O, lock acquisition, or model calls will stall the
          tick. Per-tick gate cost rolls up into instinct-stage tick
          time, so an overrun will surface via the standard
          ``overrun_warn_pct`` / ``overrun_warn_consecutive`` path.
        - Reflex nodes (``BaseReflexInstinctNode``) are not gated.
          ``evaluate_reflexes()`` runs before ``evaluate_all()`` and
          bypasses this hook entirely; reflexes must remain a hard
          real-time path.
        - Gate exceptions are logged and treated as True (fail-open).
          A broken gate must not deadlock cognition; for fail-closed
          semantics, wrap the gate at the call site.
        - A node skipped by the gate is *not* added to
          ``last_evaluated_ids``, so ``DecisionMasterNode`` retains its
          prior pending proposal. A gate that denies a node
          indefinitely will keep that node's last proposal alive
          indefinitely — gate consumers are responsible for proposal
          freshness, typically via aging in the gate's own state.
        - The gate does *not* advance the node's ``_last_trigger_s``;
          a gate denial is orthogonal to the node's intrinsic
          ``trigger_interval_s`` cadence.

        Passing ``None`` clears any installed gate.
        """
        if gate is not None and inspect.iscoroutinefunction(gate):
            raise TypeError(
                "pre_evaluate_gate must be sync; got coroutine function "
                f"{gate.__qualname__}",
            )
        self._pre_evaluate_gate = gate

    async def evaluate_all(self, ctx: Context) -> list[Proposal]:
        """
        Evaluate all normal InstinctNodes concurrently.
        Reflex nodes are excluded from this call.

        Returns non-None proposals sorted by priority descending.
        """
        if not self._normal_nodes:
            self._last_evaluated_ids = set()
            return []

        enabled = [n for n in self._normal_nodes.values() if n.enabled]

        now = time.monotonic()
        signal_kinds: set[str]
        if ctx.signals and any(n.trigger_on_signals is not None for n in enabled):
            signal_kinds = {s.kind for s in ctx.signals}
        else:
            signal_kinds = set()
        evaluated_ids: set[str] = set()

        gate = self._pre_evaluate_gate

        async def _eval(node: BaseInstinctNode) -> Proposal | None:
            # Enforce trigger_on_signals — skip if no matching signal present
            if (
                node.trigger_on_signals is not None
                and not signal_kinds.intersection(node.trigger_on_signals)
            ):
                return None
            # Enforce trigger_interval_s — skip if interval hasn't elapsed
            if (
                node.trigger_interval_s is not None
                and (now - node._last_trigger_s) < node.trigger_interval_s
            ):
                return None
            # Consult external pre-evaluate gate, if installed. Fail-open on
            # exception so a broken gate cannot deadlock cognition. Gate-skipped
            # nodes are NOT added to evaluated_ids — DecisionMasterNode keeps
            # their prior pending proposal.
            if gate is not None:
                try:
                    allowed = gate(node, ctx)
                except Exception as exc:  # noqa: BLE001
                    self.logger.error(
                        "Pre-evaluate gate raised; treating as allowed",
                        node_id=node.node_id,
                        error=str(exc),
                    )
                    allowed = True
                if not allowed:
                    return None
            evaluated_ids.add(node.node_id)
            try:
                result = await node.evaluate(ctx)
            except Exception as exc:  # noqa: BLE001
                self.logger.error(
                    "Instinct node raised",
                    node_id=node.node_id,
                    error=str(exc),
                )
                result = None
            if node.trigger_interval_s is not None:
                node._last_trigger_s = time.monotonic()
            return result

        results = await asyncio.gather(*(_eval(n) for n in enabled))
        self._last_evaluated_ids = evaluated_ids
        proposals = [p for p in results if p is not None]
        proposals.sort(key=lambda p: p.priority, reverse=True)

        self.logger.debug(
            "evaluate_all complete",
            proposals=len(proposals),
        )
        return proposals
