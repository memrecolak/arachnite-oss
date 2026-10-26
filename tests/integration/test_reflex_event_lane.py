"""Event-driven reflex lane: reflexes evaluated on signal arrival, not per tick (ADR 0005)."""

from __future__ import annotations

import asyncio
import time

import pytest

from arachnite import SignalBus
from arachnite.context import ContextNode
from arachnite.models import Context, Proposal, Result, Signal
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, BaseReflexInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime

SLOW_TICK_HZ = 2.0      # 500 ms tick: tick-quantised reflexes would be slow


def _signal(source: str, kind: str, value: float) -> Signal:
    return Signal(source=source, kind=kind, value=value, confidence=1.0,
                  timestamp=time.perf_counter())


class EventProximity(BaseSenseNode):
    """Event-driven sensor: read() reports nothing; hazards are pushed."""
    node_id = "EventProximity"
    signal_kind = "proximity"
    poll_interval_s = 0.0

    async def read(self) -> Signal | None:
        return None


class PolledProximity(BaseSenseNode):
    node_id = "PolledProximity"
    signal_kind = "proximity"
    poll_interval_s = 0.0

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.value = 1.0

    async def read(self) -> Signal | None:
        return _signal(self.node_id, self.signal_kind, self.value)


class CollisionReflex(BaseReflexInstinctNode):
    node_id = "CollisionReflex"
    priority = 250
    trigger_on_signals = ["proximity"]

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.evaluations = 0
        self.seen: list[list[str]] = []

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.evaluations += 1
        self.seen.append([f"{s.source}:{s.kind}" for s in ctx.signals])
        if any(s.kind == "proximity" and s.value < 0.05 for s in ctx.signals):
            return Proposal(instinct_id=self.node_id, action_id="Retract",
                            priority=self.priority, urgency=1.0)
        return None


class ThermalReflex(BaseReflexInstinctNode):
    """No trigger_on_signals: stays in the tick's reflex stage."""
    node_id = "ThermalReflex"
    priority = 240

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.evaluations = 0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.evaluations += 1
        return None


class Retract(BaseActionNode):
    node_id = "Retract"
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.entered: list[float] = []

    async def execute(self, proposal: Proposal) -> Result:
        self.entered.append(time.perf_counter())
        return Result(action_id=self.node_id, success=True)


class SeenByDeliberation(BaseInstinctNode):
    node_id = "SeenByDeliberation"
    priority = 10

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.kinds: list[str] = []

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.kinds.extend(s.source for s in ctx.signals)
        return None


def _build(
    *sense: BaseSenseNode,
    reflex_mode: str = "event",
    extra_reflexes: tuple[BaseReflexInstinctNode, ...] = (),
    instincts: tuple[BaseInstinctNode, ...] = (),
    tick_hz: float = SLOW_TICK_HZ,
    bus: SignalBus,
) -> tuple[ArachniteRuntime, CollisionReflex, Retract]:
    sm = SenseMasterNode(bus=bus)
    for node in sense:
        sm.register(node)
    im = InstinctMasterNode(bus=bus)
    reflex = CollisionReflex(bus)
    im.register(reflex)
    for extra in extra_reflexes:
        im.register(extra)
    for inst in instincts:
        im.register(inst)
    am = ActionMasterNode(bus=bus)
    retract = Retract(bus)
    am.register(retract)
    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(), instinct_master=im,
        decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
        action_master=am, bus=bus, tick_rate_hz=tick_hz, reflex_mode=reflex_mode,
    )
    return rt, reflex, retract


class TestEventLane:
    @pytest.mark.asyncio
    async def test_pushed_hazard_triggers_reflex_between_ticks(self) -> None:
        bus = SignalBus()
        sensor = EventProximity(bus)
        rt, _, retract = _build(sensor, bus=bus)
        await rt.start()
        await asyncio.sleep(0.05)              # first tick done; next is ~450 ms away

        pushed_at = time.perf_counter()
        await sensor.push(_signal("EventProximity", "proximity", 0.01))
        await rt.wait_idle(1.0)
        await rt.stop()

        assert len(retract.entered) == 1
        # Event-driven: no wait for the next tick (which is ~0.45 s away).
        assert retract.entered[0] - pushed_at < 0.02

    @pytest.mark.asyncio
    async def test_tick_mode_ignores_pushes_until_the_next_tick(self) -> None:
        """Control: with reflex_mode='tick' the same push waits for a tick."""
        bus = SignalBus()
        sensor = EventProximity(bus)
        rt, _, retract = _build(sensor, reflex_mode="tick", bus=bus)
        await rt.start()
        await asyncio.sleep(0.05)
        pushed_at = time.perf_counter()
        await sensor.push(_signal("EventProximity", "proximity", 0.01))
        await asyncio.sleep(0.05)
        assert retract.entered == []           # not yet: still between ticks
        deadline = time.perf_counter() + 1.5
        while not retract.entered and time.perf_counter() < deadline:
            await asyncio.sleep(0.01)
        await rt.stop()
        assert retract.entered and retract.entered[0] - pushed_at > 0.2

    @pytest.mark.asyncio
    async def test_polled_signal_evaluated_once_by_the_lane(self) -> None:
        bus = SignalBus()
        sensor = PolledProximity(bus)
        thermal = ThermalReflex(bus)
        rt, reflex, retract = _build(
            sensor, extra_reflexes=(thermal,), tick_hz=50.0, bus=bus,
        )
        await rt.start()
        await rt.pause()
        await rt.wait_idle(1.0)
        reflex.evaluations = 0
        thermal.evaluations = 0
        sensor.value = 0.01

        await rt.tick()
        await rt.wait_idle(1.0)
        await rt.stop()

        # The lane evaluated the triggered reflex once (on the sense-stage
        # publish); the tick stage skipped it but still ran ThermalReflex.
        assert reflex.evaluations == 1
        assert thermal.evaluations == 1
        assert len(retract.entered) == 1

    @pytest.mark.asyncio
    async def test_reflex_sees_fresh_signals_only(self) -> None:
        bus = SignalBus()
        sensor = EventProximity(bus)
        rt, reflex, _ = _build(sensor, bus=bus)
        rt._reflex_signal_ttl_s = 0.05
        await rt.start()
        await sensor.push(_signal("left", "proximity", 1.0))
        await asyncio.sleep(0.1)                # "left" is now stale
        await sensor.push(_signal("right", "proximity", 1.0))
        await rt.stop()
        assert reflex.seen[-1] == ["right:proximity"]

    @pytest.mark.asyncio
    async def test_pushed_signal_reaches_deliberation_once(self) -> None:
        bus = SignalBus()
        sensor = EventProximity(bus)
        watcher = SeenByDeliberation(bus)
        published: list[str] = []

        async def count(signal: Signal) -> None:
            published.append(signal.source)

        bus.subscribe("proximity", count)
        rt, _, _ = _build(sensor, instincts=(watcher,), tick_hz=50.0, bus=bus)
        await rt.start()
        await rt.pause()
        await sensor.push(_signal("EventProximity", "proximity", 1.0))
        await rt.tick()
        await rt.tick()
        await rt.stop()
        assert watcher.kinds.count("EventProximity") == 1   # queued for one tick
        assert published.count("EventProximity") == 1      # not re-published

    @pytest.mark.asyncio
    async def test_lane_silent_after_stop(self) -> None:
        bus = SignalBus()
        sensor = EventProximity(bus)
        rt, reflex, retract = _build(sensor, bus=bus)
        await rt.start()
        await rt.stop()
        before = reflex.evaluations
        await sensor.push(_signal("EventProximity", "proximity", 0.01))
        assert reflex.evaluations == before
        assert retract.entered == []

    @pytest.mark.asyncio
    async def test_slow_reflex_evaluation_is_abandoned(self) -> None:
        bus = SignalBus()
        sensor = EventProximity(bus)
        rt, reflex, retract = _build(sensor, bus=bus)
        rt._reflex_eval_timeout_s = 0.02

        async def slow(ctx: Context) -> Proposal | None:
            await asyncio.sleep(1.0)
            return Proposal(instinct_id="CollisionReflex", action_id="Retract",
                            priority=250, urgency=1.0)

        reflex.evaluate = slow  # type: ignore[method-assign]
        await rt.start()
        started = time.perf_counter()
        await sensor.push(_signal("EventProximity", "proximity", 0.01))
        assert time.perf_counter() - started < 0.5      # publisher not held
        await rt.stop()
        assert retract.entered == []


def test_invalid_reflex_mode_rejected() -> None:
    bus = SignalBus()
    with pytest.raises(ValueError, match="reflex_mode"):
        ArachniteRuntime(
            sense_master=SenseMasterNode(bus=bus), context=ContextNode(),
            instinct_master=InstinctMasterNode(bus=bus),
            decision_master=DecisionMasterNode(bus=bus),
            action_master=ActionMasterNode(bus=bus), bus=bus,
            reflex_mode="sometimes",
        )
