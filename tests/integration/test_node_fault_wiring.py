"""
Node-fault → ctx.signals wiring contract (CHANGELOG 0.11.6).

The framework documents (llms.txt tick step 2, NodeFaultSignal docstring, the
supervisors tutorial) that a leaf node raising during a tick transitions its
supervisor to FAULTED and surfaces a ``node_fault`` signal in the *next* tick's
Context. These tests pin that end-to-end contract:

  - a sense / instinct / action node that raises produces a kind="node_fault"
    signal (plus its kind="supervisor" twin) in ctx.signals on tick N+1,
  - the supervisor actually transitions the faulting node to FAULTED,
  - a reflex filtering ctx.signals for node_fault fires on N+1,
  - a non-faulting tick produces no node_fault signal,
  - a master driven standalone (no runtime, no handler) still swallows.

Nodes are registered on the masters *before* the runtime is built, so the
ArachniteRuntime constructor auto-tracks them with the supervisors and wires
each master's fault sink — exercising the real production wiring path.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from arachnite import ContextNode, SignalBus
from arachnite.models import Context, NodeState, Proposal, Result, Signal
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.base import BaseNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime

# ── Faulting + supporting test nodes ──────────────────────────────────────────

class _FaultySense(BaseSenseNode):
    node_id = "_FaultySense"
    signal_kind = "thermal"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        raise RuntimeError("sense boom")


class _OkSense(BaseSenseNode):
    node_id = "_OkSense"
    signal_kind = "thermal"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=100.0, confidence=1.0, timestamp=time.monotonic(),
        )


class _FaultyInstinct(BaseInstinctNode):
    node_id = "_FaultyInstinct"

    async def evaluate(self, ctx: Context) -> Proposal | None:
        raise RuntimeError("instinct boom")


class _ProposeFaultyAction(BaseReflexInstinctNode):
    """Reflex that always proposes the faulting action so it gets dispatched."""
    node_id = "_ProposeFaultyAction"
    priority = 200

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="_FaultyAction",
            priority=self.priority, urgency=1.0,
        )


class _FaultyAction(BaseActionNode):
    node_id = "_FaultyAction"

    async def execute(self, proposal: Proposal) -> Result:
        raise RuntimeError("action boom")


class _FaultWatcherReflex(BaseReflexInstinctNode):
    """Consumer pattern from the tutorial: react to node_fault in ctx.signals."""
    node_id = "_FaultWatcherReflex"
    priority = 250

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus=bus)
        self.seen_faults: list[str] = []

    async def evaluate(self, ctx: Context) -> Proposal | None:
        for s in ctx.signals:
            if s.kind == "node_fault":
                self.seen_faults.append(getattr(s, "node_id", ""))
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

NodeFactory = Callable[[SignalBus], BaseNode]


def _runtime(
    sense: list[NodeFactory] | None = None,
    instincts: list[NodeFactory] | None = None,
    actions: list[NodeFactory] | None = None,
) -> tuple[ArachniteRuntime, list[Context]]:
    """
    Build a runtime with the given node factories registered before wiring.

    Returns ``(runtime, captures)`` where ``captures`` is appended with the
    fresh Context snapshot each tick (via a context observer), so tests can
    inspect ``captures[-1].signals`` — the runtime does not otherwise expose
    the assembled Context.
    """
    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am = ActionMasterNode(bus=bus)
    for make in sense or []:
        sm.register(make(bus))  # type: ignore[arg-type]
    for make in instincts or []:
        im.register(make(bus))  # type: ignore[arg-type]
    for make in actions or []:
        am.register(make(bus))  # type: ignore[arg-type]
    captures: list[Context] = []
    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(),
        instinct_master=im, decision_master=dm,
        action_master=am, bus=bus,
        tick_rate_hz=200.0,
        context_observers=[captures.append],
    )
    return rt, captures


def _node_faults(ctx: Context) -> list[Signal]:
    return [s for s in ctx.signals if s.kind == "node_fault"]


# ── Sense / instinct / action faults surface on N+1 ───────────────────────────

class TestFaultSurfacesNextTick:

    async def test_sense_fault_visible_on_n_plus_1(self) -> None:
        rt, captures = _runtime(sense=[_FaultySense])
        await rt.start()
        await rt.pause()
        try:
            await rt.tick()  # tick 1: sense raises → fault buffered for next tick
            assert _node_faults(captures[-1]) == [], "fault must not leak into tick N"
            await rt.tick()  # tick 2: fault now visible
            faults = _node_faults(captures[-1])
            assert len(faults) == 1
            assert faults[0].node_id == "_FaultySense"
            assert faults[0].error_type == "RuntimeError"
            assert faults[0].current_state == NodeState.FAULTED
            assert rt._supervisors[0].state_of("_FaultySense") == NodeState.FAULTED
        finally:
            await rt.stop()

    async def test_instinct_fault_visible_on_n_plus_1(self) -> None:
        rt, captures = _runtime(instincts=[_FaultyInstinct])
        await rt.start()
        await rt.pause()
        try:
            await rt.tick()
            assert _node_faults(captures[-1]) == []
            await rt.tick()
            faults = _node_faults(captures[-1])
            assert any(f.node_id == "_FaultyInstinct" for f in faults)
            assert rt._supervisors[1].state_of("_FaultyInstinct") == NodeState.FAULTED
        finally:
            await rt.stop()

    async def test_action_fault_visible_on_n_plus_1(self) -> None:
        rt, captures = _runtime(
            instincts=[_ProposeFaultyAction],
            actions=[_FaultyAction],
        )
        await rt.start()
        await rt.pause()
        try:
            await rt.tick()  # reflex proposes → action dispatched → raises
            await rt.tick()
            faults = _node_faults(captures[-1])
            assert any(f.node_id == "_FaultyAction" for f in faults)
            assert rt._supervisors[3].state_of("_FaultyAction") == NodeState.FAULTED
        finally:
            await rt.stop()


# ── Documented consumer (tutorial CrashReflex) actually fires ─────────────────

class TestFaultConsumerFires:

    async def test_reflex_reacts_to_node_fault(self) -> None:
        rt, _ = _runtime(sense=[_FaultySense], instincts=[_FaultWatcherReflex])
        watcher = rt._instinct_master.get_node("_FaultWatcherReflex")
        assert isinstance(watcher, _FaultWatcherReflex)
        await rt.start()
        await rt.pause()
        try:
            await rt.tick()  # fault occurs
            assert watcher.seen_faults == [], "watcher must not see fault on tick N"
            await rt.tick()  # watcher observes it
            assert "_FaultySense" in watcher.seen_faults
        finally:
            await rt.stop()


# ── Regression: clean ticks produce no node_fault ─────────────────────────────

class TestNoSpuriousFaults:

    async def test_healthy_runtime_emits_no_node_fault(self) -> None:
        rt, captures = _runtime(sense=[_OkSense])
        await rt.start()
        await rt.pause()
        try:
            for _ in range(4):
                await rt.tick()
                assert _node_faults(captures[-1]) == []
        finally:
            await rt.stop()


# ── Backward-compat: standalone master with no handler swallows ───────────────

class TestStandaloneMasterUnchanged:

    async def test_master_without_handler_swallows(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        sm.register(_FaultySense(bus=bus))
        # No runtime, no fault handler installed.
        signals = await sm.read_all()
        # Swallow-and-continue: on_error returns None, no fault propagated.
        assert signals == []
