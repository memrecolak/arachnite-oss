"""
Bus-published StateUpdateSignal → ctx.state wiring contract.

The framework documents (StateUpdateSignal docstring, spec 5.3, the
smarter-context tutorial) that any node can update ctx.state by emitting a
StateUpdateSignal — and that a node which *publishes* one on the bus (rather
than a sense node returning one) has it applied at the start of the *next*
tick (N+1), because instincts and actions run after ContextNode.update().

These tests pin that end-to-end contract:

  - an instinct that publishes a StateUpdateSignal has it absent on tick N and
    present in ctx.state on tick N+1,
  - delete=True published on the bus removes the key on N+1,
  - a sense node *returning* a StateUpdateSignal is applied in the SAME tick,
  - a bus write wins over a same-key sense write within one drain,
  - the documented latch pattern (set this tick, read next tick) actually
    latches.

Mirrors tests/integration/test_node_fault_wiring.py: nodes are registered on
the masters before the runtime is built, exercising the real wiring path.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from arachnite import ContextNode, SignalBus
from arachnite.models import Context, Proposal, Signal, StateUpdateSignal
from arachnite.nodes.action import ActionMasterNode
from arachnite.nodes.base import BaseNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime


def _state_update(
    source: str, key: str, state_value: object = None, delete: bool = False
) -> StateUpdateSignal:
    return StateUpdateSignal(
        source=source, kind="state_update", value=None,
        confidence=1.0, timestamp=time.monotonic(),
        key=key, state_value=state_value, delete=delete,
    )


# ── Test nodes ────────────────────────────────────────────────────────────────

class _PublishStateInstinct(BaseInstinctNode):
    """Instinct that publishes a state write on its first evaluate()."""
    node_id = "_PublishStateInstinct"

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if ctx.tick == 1:
            await self.bus.publish(
                _state_update(self.node_id, "feedback", {"ok": True})
            )
        return None


class _LatchInstinct(BaseInstinctNode):
    """The tutorial latch: fire once, then never again."""
    node_id = "_LatchInstinct"

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus=bus)
        self.fire_ticks: list[int] = []

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if ctx.state.get("fired"):
            return None
        self.fire_ticks.append(ctx.tick)
        await self.bus.publish(_state_update(self.node_id, "fired", True))
        return None


class _DeleteStateInstinct(BaseInstinctNode):
    node_id = "_DeleteStateInstinct"

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if ctx.tick == 1:
            await self.bus.publish(
                _state_update(self.node_id, "seed", delete=True)
            )
        return None


class _WorldSense(BaseSenseNode):
    """Sense node that *returns* a StateUpdateSignal — applied same tick."""
    node_id = "_WorldSense"
    signal_kind = "state_update"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        return _state_update(self.node_id, "world", {"temp": 72})


# ── Helper (mirrors test_node_fault_wiring._runtime) ──────────────────────────

NodeFactory = Callable[[SignalBus], BaseNode]


def _runtime(
    sense: list[NodeFactory] | None = None,
    instincts: list[NodeFactory] | None = None,
    context: ContextNode | None = None,
) -> tuple[ArachniteRuntime, list[Context]]:
    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am = ActionMasterNode(bus=bus)
    for make in sense or []:
        sm.register(make(bus))  # type: ignore[arg-type]
    for make in instincts or []:
        im.register(make(bus))  # type: ignore[arg-type]
    captures: list[Context] = []
    rt = ArachniteRuntime(
        sense_master=sm, context=context or ContextNode(),
        instinct_master=im, decision_master=dm,
        action_master=am, bus=bus,
        tick_rate_hz=200.0,
        context_observers=[captures.append],
    )
    return rt, captures


# ── Bus publish from an instinct surfaces on N+1 ──────────────────────────────

class TestBusPublishVisibleNextTick:

    async def test_published_state_absent_on_n_present_on_n_plus_1(self) -> None:
        rt, captures = _runtime(instincts=[_PublishStateInstinct])
        await rt.start()
        await rt.pause()
        try:
            await rt.tick()  # tick 1: instinct publishes → buffered
            assert "feedback" not in captures[-1].state, "must not leak into tick N"
            await rt.tick()  # tick 2: drained at tick start, now applied
            assert captures[-1].state["feedback"] == {"ok": True}
            assert rt.context.get("feedback") == {"ok": True}
        finally:
            await rt.stop()

    async def test_published_delete_visible_next_tick(self) -> None:
        ctx = ContextNode()
        ctx.set("seed", "present")
        rt, captures = _runtime(instincts=[_DeleteStateInstinct], context=ctx)
        await rt.start()
        await rt.pause()
        try:
            await rt.tick()  # tick 1: publishes delete → buffered
            assert captures[-1].state.get("seed") == "present"
            await rt.tick()  # tick 2: delete applied
            assert "seed" not in captures[-1].state
        finally:
            await rt.stop()

    async def test_latch_pattern_fires_exactly_once(self) -> None:
        rt, _ = _runtime(instincts=[_LatchInstinct])
        latch = rt._instinct_master.get_node("_LatchInstinct")
        assert isinstance(latch, _LatchInstinct)
        await rt.start()
        await rt.pause()
        try:
            for _ in range(5):
                await rt.tick()
            # Tick 1 fires and publishes; flag lands at tick 2; ticks 2-5 see it.
            assert latch.fire_ticks == [1], latch.fire_ticks
        finally:
            await rt.stop()


# ── Sense node returning one is applied SAME tick ─────────────────────────────

class TestSenseReturnSameTick:

    async def test_sense_returned_state_applied_immediately(self) -> None:
        rt, captures = _runtime(sense=[_WorldSense])
        await rt.start()
        await rt.pause()
        try:
            await rt.tick()  # applied within this very tick
            assert captures[-1].state["world"] == {"temp": 72}
        finally:
            await rt.stop()


# ── Bus write wins over a same-key sense write within one drain ───────────────

class _WorldSenseLow(BaseSenseNode):
    node_id = "_WorldSenseLow"
    signal_kind = "state_update"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        return _state_update(self.node_id, "shared", "from_sense")


class _PublishSharedInstinct(BaseInstinctNode):
    node_id = "_PublishSharedInstinct"

    async def evaluate(self, ctx: Context) -> Proposal | None:
        await self.bus.publish(_state_update(self.node_id, "shared", "from_bus"))
        return None


class TestBusWinsWithinDrain:

    async def test_bus_write_overrides_same_key_sense_write(self) -> None:
        rt, captures = _runtime(
            sense=[_WorldSenseLow], instincts=[_PublishSharedInstinct]
        )
        await rt.start()
        await rt.pause()
        try:
            await rt.tick()  # tick 1: sense sets "shared"; instinct publishes
            assert captures[-1].state["shared"] == "from_sense"
            await rt.tick()  # tick 2: both in drain; bus appended last → wins
            assert captures[-1].state["shared"] == "from_bus"
        finally:
            await rt.stop()
