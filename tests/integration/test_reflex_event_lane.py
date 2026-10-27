"""Event-driven reflex lane: reflexes evaluated on signal arrival, not per tick (ADR 0005)."""

from __future__ import annotations

import asyncio
import time

import pytest

import arachnite.runtime as runtime_module
from arachnite import SignalBus
from arachnite.context import ContextNode
from arachnite.models import (
    ActionStep,
    Context,
    InterruptPolicy,
    Proposal,
    ReflexPreemption,
    Result,
    Signal,
    StepResult,
)
from arachnite.nodes.action import ActionMasterNode, BaseActionNode, MultiStepActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, BaseReflexInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from tests.conftest import ManualClock

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
    extra_actions: tuple[BaseActionNode, ...] = (),
    reflex_conflict: str = "dispatch_all",
    tick_hz: float = SLOW_TICK_HZ,
    bus: SignalBus,
) -> tuple[ArachniteRuntime, CollisionReflex, Retract]:
    sm = SenseMasterNode(bus=bus)
    for node in sense:
        sm.register(node)
    im = InstinctMasterNode(bus=bus, reflex_conflict=reflex_conflict)
    reflex = CollisionReflex(bus)
    im.register(reflex)
    for extra in extra_reflexes:
        im.register(extra)
    for inst in instincts:
        im.register(inst)
    am = ActionMasterNode(bus=bus)
    retract = Retract(bus)
    am.register(retract)
    for action in extra_actions:
        am.register(action)
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


# ── Mutation-evidence hardening (v0.13) ───────────────────────────────────────

class SmokeReflex(BaseReflexInstinctNode):
    """Lane reflex on another signal kind; counts its evaluations."""
    node_id = "SmokeReflex"
    priority = 240
    trigger_on_signals = ["smoke"]

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.evaluations = 0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.evaluations += 1
        return None


class BrakeReflex(BaseReflexInstinctNode):
    """Fires on the same hazard and priority as CollisionReflex (a tie)."""
    node_id = "BrakeReflex"
    priority = 250
    trigger_on_signals = ["proximity"]

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if any(s.kind == "proximity" and s.value < 0.05 for s in ctx.signals):
            return Proposal(instinct_id=self.node_id, action_id="Brake",
                            priority=self.priority, urgency=1.0)
        return None


class Brake(Retract):
    """Reflex action on a resource disjoint from Retract's."""
    node_id = "Brake"
    resources = frozenset({"wheels"})


class TestEventLaneEdges:
    @pytest.mark.asyncio
    async def test_signal_age_on_a_freshly_booted_clock(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A lane signal older than the TTL is stale, also when the clock started near zero"""
        clock = ManualClock(now=0.5)
        monkeypatch.setattr(runtime_module, "time", clock)
        bus = SignalBus()
        sensor = EventProximity(bus)
        rt, reflex, _ = _build(sensor, bus=bus)
        rt._reflex_signal_ttl_s = 0.05
        await rt.start()
        await rt.pause()
        await sensor.push(_signal("left", "proximity", 1.0))
        clock.now = 1.0                         # "left" is now 0.5 s old
        await sensor.push(_signal("right", "proximity", 1.0))
        await rt.stop()
        assert reflex.seen[-1] == ["right:proximity"]

    @pytest.mark.asyncio
    async def test_zero_ttl_still_sees_the_trigger_signal(self) -> None:
        """With reflex_signal_ttl_s=0 the reflex still sees the signal that triggered it"""
        bus = SignalBus()
        sensor = EventProximity(bus)
        rt, _, retract = _build(sensor, bus=bus)
        rt._reflex_signal_ttl_s = 0.0
        await rt.start()
        await rt.pause()
        await sensor.push(_signal("EventProximity", "proximity", 0.01))
        await rt.wait_idle(1.0)
        await rt.stop()
        assert len(retract.entered) == 1

    @pytest.mark.asyncio
    async def test_lane_evaluates_only_reflexes_triggered_by_the_kind(self) -> None:
        """A pushed signal evaluates only the lane reflexes whose trigger kinds include it"""
        bus = SignalBus()
        sensor = EventProximity(bus)
        smoke = SmokeReflex(bus)
        rt, reflex, _ = _build(sensor, extra_reflexes=(smoke,), bus=bus)
        await rt.start()
        await rt.pause()
        evaluations_before = reflex.evaluations
        await sensor.push(_signal("EventProximity", "proximity", 1.0))
        await rt.stop()
        assert reflex.evaluations == evaluations_before + 1
        assert smoke.evaluations == 0

    @pytest.mark.asyncio
    async def test_raise_policy_tie_on_the_lane_dispatches_all(self) -> None:
        """A lane priority tie under reflex_conflict='raise' dispatches every reflex"""
        bus = SignalBus()
        sensor = EventProximity(bus)
        brake = Brake(bus)
        rt, _, retract = _build(
            sensor, extra_reflexes=(BrakeReflex(bus),), extra_actions=(brake,),
            reflex_conflict="raise", bus=bus,
        )
        await rt.start()
        await rt.pause()
        await sensor.push(_signal("EventProximity", "proximity", 0.01))
        await rt.wait_idle(1.0)
        await rt.stop()
        assert len(retract.entered) == 1
        assert len(brake.entered) == 1

    @pytest.mark.asyncio
    async def test_evaluation_timeout_never_raises_into_the_publisher(self) -> None:
        """A lane evaluation that times out is logged; bus.publish() does not raise"""
        bus = SignalBus()
        sensor = EventProximity(bus)
        rt, reflex, retract = _build(sensor, bus=bus)
        rt._reflex_eval_timeout_s = 0.02

        async def hung(ctx: Context) -> Proposal | None:
            await asyncio.Event().wait()
            return None

        reflex.evaluate = hung  # type: ignore[method-assign]
        await rt.start()
        await rt.pause()
        await bus.publish(_signal("EventProximity", "proximity", 0.01))   # must not raise
        await rt.stop()
        assert retract.entered == []


# ── Sixth review: lane re-entrancy and the producing node's preemption ───────


class _FaultReflex(BaseReflexInstinctNode):
    """The node_fault reflex pattern (spec §6.6) with a bug: evaluate() raises."""

    node_id = "FaultReflex"
    priority = 250
    trigger_on_signals = ["node_fault", "hazard"]

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.calls = 0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.calls += 1
        raise KeyError("bug in reflex")


class _GuardReflex(BaseReflexInstinctNode):
    """WAIT_FOR_BLOCK reflex whose Proposal.instinct_id differs from its node_id."""

    node_id = "Guard"
    priority = 250
    preemption = ReflexPreemption.WAIT_FOR_BLOCK
    trigger_on_signals = ["proximity"]

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if any(s.kind == "proximity" and s.value < 0.05 for s in ctx.signals):
            return Proposal(instinct_id="guard-reflex", action_id="Retract",
                            priority=self.priority, urgency=1.0)
        return None


class _Lift(MultiStepActionNode):
    """grip + raise form a mandatory block; each step waits for the test."""

    node_id = "Lift"
    resources = frozenset({"arm"})
    interrupt_policy = InterruptPolicy.ALWAYS
    timeout_s = 10.0

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.go = {name: asyncio.Event() for name in ("grip", "raise", "swing")}
        self.log: list[str] = []

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("grip", interruptible=False),
            ActionStep("raise", interruptible=False),
            ActionStep("swing", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        self.log.append(step.name)
        await self.go[step.name].wait()
        return StepResult(step_name=step.name, success=True)


class TestSixthReviewRegressions:
    @pytest.mark.asyncio
    async def test_fault_reflex_is_not_re_evaluated_inside_its_own_evaluation(self) -> None:
        """R6-F9: a raising node_fault reflex is not re-entered by its own node_fault"""
        bus = SignalBus()
        sense = EventProximity(bus)
        fault = _FaultReflex(bus)
        rt, _, _ = _build(sense, extra_reflexes=(fault,), bus=bus)
        rt._supervisors[1]._restart_delay = 10.0   # keep the restart out of the picture
        await rt.start()
        try:
            started = time.perf_counter()
            await sense.push(_signal("EventProximity", "hazard", 1.0))
            elapsed = time.perf_counter() - started
            assert fault.calls == 1
            assert elapsed < 0.5
            # The node_fault published during the evaluation is kept as the
            # lane's latest signal of its kind, for the next evaluation.
            assert any(kind == "node_fault" for _, kind in rt._lane_latest)
            # The guard is released afterwards: a later trigger is evaluated.
            fault.enabled = True
            await sense.push(_signal("EventProximity", "hazard", 1.0))
            assert fault.calls == 2
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_lane_reflex_keeps_its_preemption_whatever_its_instinct_id(self) -> None:
        """R6-F7: a lane WAIT_FOR_BLOCK reflex keeps its mode if Proposal.instinct_id != node_id"""
        bus = SignalBus()
        sense = EventProximity(bus)
        sm = SenseMasterNode(bus=bus)
        sm.register(sense)
        im = InstinctMasterNode(bus=bus)
        im.register(_GuardReflex(bus))
        am = ActionMasterNode(bus=bus)
        lift = _Lift(bus)
        am.register(lift)
        am.register(Retract(bus))
        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(), instinct_master=im,
            decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
            action_master=am, bus=bus, tick_rate_hz=SLOW_TICK_HZ, reflex_mode="event",
        )
        await rt.start()
        try:
            am.launch(Proposal(instinct_id="op", action_id="Lift", priority=50, urgency=0.5))
            for _ in range(100):
                if lift.log == ["grip"]:
                    break
                await asyncio.sleep(0.005)
            await sense.push(_signal("EventProximity", "proximity", 0.01))
            await asyncio.sleep(0.02)
            assert am.running_action_ids() == {"Lift"}     # not cut into the block
            lift.go["grip"].set()
            lift.go["raise"].set()
            assert await rt.wait_idle(2.0)
            results = {r.action_id: r for r in am.drain_completed()}
            assert results["Lift"].preempted is True
            assert results["Lift"].stopped_at_step == "swing"
            assert [s.step_name for s in results["Lift"].step_results] == ["grip", "raise"]
            assert "Retract" in results
        finally:
            await rt.stop()
