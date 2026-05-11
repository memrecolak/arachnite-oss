"""
arachnite.builder
~~~~~~~~~~~~~~~~~
RuntimeBuilder: fluent API for constructing ArachniteRuntime instances
with minimal boilerplate.

Spec reference: Section 7 (runtime construction).
"""

from __future__ import annotations

from arachnite.bus import SignalBus
from arachnite.context import ContextNode
from arachnite.logging import BaseLogSink
from arachnite.models import Permission
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.decision import (
    BaseDecisionNode,
    DecisionMasterNode,
    GreedyDecisionNode,
)
from arachnite.nodes.instinct import BaseInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from arachnite.shutdown import ShutdownCoordinator


class RuntimeBuilder:
    """
    Fluent builder for ArachniteRuntime

    Handles creation of SignalBus, master nodes, and registration so that
    simple agents can be assembled in a few chained calls::

        rt = (
            RuntimeBuilder()
            .sense(TempSense)
            .instinct(HotInstinct)
            .action(CoolDown)
            .strategy(GreedyDecisionNode)
            .tick_rate(2.0)
            .build()
        )

    Pass node **classes** and the builder instantiates them with its
    internal bus.  Pass pre-built **instances** when you need custom
    constructor arguments (the instance keeps whatever bus it was created
    with — make sure it matches if you also access ``builder.bus``).

    Defaults:
    - strategy: GreedyDecisionNode
    - tick_rate: 10.0 Hz
    - reflex_conflict: 'dispatch_all'
    """

    def __init__(self) -> None:
        self._bus = SignalBus()
        self._sense_nodes: list[BaseSenseNode] = []
        self._instinct_nodes: list[BaseInstinctNode] = []
        self._action_nodes: list[BaseActionNode] = []
        self._strategy_instance: BaseDecisionNode | None = None
        self._strategy_cls: type[BaseDecisionNode] = GreedyDecisionNode
        self._tick_rate_hz: float = 10.0
        self._log_sinks: list[BaseLogSink] | None = None
        self._shutdown_coordinator: ShutdownCoordinator | None = None
        self._allowed_permissions: dict[str, set[Permission]] | None = None
        self._reflex_conflict: str = "dispatch_all"
        self._overrun_warn_pct: float = 0.2
        self._overrun_warn_consecutive: int = 3

    # ── Node registration ────────────────────────────────────────────────────

    def sense(self, node: BaseSenseNode | type[BaseSenseNode]) -> RuntimeBuilder:
        """Add a sense node (class or instance)"""
        if isinstance(node, type):
            node = node(bus=self._bus)
        self._sense_nodes.append(node)
        return self

    def instinct(self, node: BaseInstinctNode | type[BaseInstinctNode]) -> RuntimeBuilder:
        """Add an instinct node (class or instance)"""
        if isinstance(node, type):
            node = node(bus=self._bus)
        self._instinct_nodes.append(node)
        return self

    def action(self, node: BaseActionNode | type[BaseActionNode]) -> RuntimeBuilder:
        """Add an action node (class or instance)"""
        if isinstance(node, type):
            node = node(bus=self._bus)
        self._action_nodes.append(node)
        return self

    # ── Configuration ────────────────────────────────────────────────────────

    def strategy(self, strategy: BaseDecisionNode | type[BaseDecisionNode]) -> RuntimeBuilder:
        """Set the decision strategy (class or instance, default: GreedyDecisionNode)"""
        if isinstance(strategy, type):
            self._strategy_cls = strategy
            self._strategy_instance = None
        else:
            self._strategy_instance = strategy
        return self

    def tick_rate(self, hz: float) -> RuntimeBuilder:
        """Set the tick rate in Hz (default: 10.0)"""
        self._tick_rate_hz = hz
        return self

    def log_sinks(self, sinks: list[BaseLogSink]) -> RuntimeBuilder:
        """Set log sinks for the runtime"""
        self._log_sinks = sinks
        return self

    def reflex_conflict(self, policy: str) -> RuntimeBuilder:
        """Set reflex conflict policy: 'raise' or 'dispatch_all' (default)"""
        self._reflex_conflict = policy
        return self

    def permissions(self, allowed: dict[str, set[Permission]]) -> RuntimeBuilder:
        """Set allowed permissions whitelist"""
        self._allowed_permissions = allowed
        return self

    def shutdown(self, coordinator: ShutdownCoordinator) -> RuntimeBuilder:
        """Set a custom ShutdownCoordinator"""
        self._shutdown_coordinator = coordinator
        return self

    def overrun_warn(self, pct: float) -> RuntimeBuilder:
        """Set the tick overrun warning threshold (default: 0.2 = 20%)"""
        self._overrun_warn_pct = pct
        return self

    def overrun_warn_consecutive(self, n: int) -> RuntimeBuilder:
        """Set the consecutive overrun count before warning (default: 3)"""
        self._overrun_warn_consecutive = n
        return self

    # ── Access ───────────────────────────────────────────────────────────────

    @property
    def bus(self) -> SignalBus:
        """The shared SignalBus used by all nodes created through this builder"""
        return self._bus

    # ── Build ────────────────────────────────────────────────────────────────

    def build(self) -> ArachniteRuntime:
        """Assemble and return the configured ArachniteRuntime"""
        sense_master = SenseMasterNode(bus=self._bus)
        instinct_master = InstinctMasterNode(
            bus=self._bus, reflex_conflict=self._reflex_conflict,
        )
        decision_strategy = self._strategy_instance or self._strategy_cls(bus=self._bus)
        decision_master = DecisionMasterNode(bus=self._bus, strategy=decision_strategy)
        action_master = ActionMasterNode(bus=self._bus)

        for sense_node in self._sense_nodes:
            sense_master.register(sense_node)
        for instinct_node in self._instinct_nodes:
            instinct_master.register(instinct_node)
        for action_node in self._action_nodes:
            action_master.register(action_node)

        return ArachniteRuntime(
            sense_master=sense_master,
            context=ContextNode(),
            instinct_master=instinct_master,
            decision_master=decision_master,
            action_master=action_master,
            bus=self._bus,
            tick_rate_hz=self._tick_rate_hz,
            log_sinks=self._log_sinks,
            overrun_warn_pct=self._overrun_warn_pct,
            overrun_warn_consecutive=self._overrun_warn_consecutive,
            shutdown_coordinator=self._shutdown_coordinator,
            allowed_permissions=self._allowed_permissions,
        )
