"""End-to-end reflex arbitration through the runtime's own tick loop (ADR 0005)."""

from __future__ import annotations

import asyncio
import time

import pytest

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

TICK_HZ = 50.0          # 20 ms tick
STEP_S  = 0.1           # each step of the pick takes 100 ms


class World:
    """Shared plant: a collision flag and a log of events with timestamps."""

    def __init__(self) -> None:
        self.collision = False
        self.collision_at: float | None = None
        self.reads = 0
        self.events: list[tuple[float, str]] = []

    def log(self, what: str) -> None:
        self.events.append((time.perf_counter(), what))


class ProximitySense(BaseSenseNode):
    node_id = "ProximitySense"
    signal_kind = "proximity"
    poll_interval_s = 0.0

    def __init__(self, bus: SignalBus, world: World) -> None:
        super().__init__(bus)
        self.world = world

    async def read(self) -> Signal:
        self.world.reads += 1
        return Signal(source=self.node_id, kind=self.signal_kind,
                      value=0.01 if self.world.collision else 1.0,
                      confidence=1.0, timestamp=time.perf_counter())


class CollisionReflex(BaseReflexInstinctNode):
    node_id = "CollisionReflex"
    priority = 250

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if any(s.kind == "proximity" and s.value < 0.05 for s in ctx.signals):
            return Proposal(instinct_id=self.node_id, action_id="Retract",
                            priority=self.priority, urgency=1.0)
        return None


class StartPick(BaseInstinctNode):
    node_id = "StartPick"
    priority = 80

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.fired = False

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if self.fired:
            return None
        self.fired = True
        return Proposal(instinct_id=self.node_id, action_id="Pick",
                        priority=self.priority, urgency=0.5)


class Pick(MultiStepActionNode):
    node_id = "Pick"
    resources = frozenset({"arm", "gripper"})
    interrupt_policy = InterruptPolicy.ROLLBACK
    timeout_s = 10.0

    def __init__(self, bus: SignalBus, world: World) -> None:
        super().__init__(bus)
        self.world = world
        self.holding = False

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("approach", interruptible=True),
            ActionStep("lower",    interruptible=False),
            ActionStep("close",    interruptible=False),
            ActionStep("raise",    interruptible=False),
            ActionStep("place",    interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        self.world.log(f"step:{step.name}")
        await asyncio.sleep(STEP_S)
        if step.name == "close":
            self.holding = True
        if step.name == "place":
            self.holding = False
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        # Forward safe-stop: freeze the arm, keep the grip (never drop).
        self.world.log(f"safe_stop:holding={self.holding}")


class Retract(BaseActionNode):
    node_id = "Retract"
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus, world: World) -> None:
        super().__init__(bus)
        self.world = world
        self.entered_at: float | None = None

    async def execute(self, proposal: Proposal) -> Result:
        self.entered_at = time.perf_counter()
        self.world.log("retract")
        self.world.collision = False
        return Result(action_id=self.node_id, success=True)


def _build(
    world: World, preemption: ReflexPreemption,
) -> tuple[ArachniteRuntime, Pick, Retract]:
    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    am = ActionMasterNode(bus=bus)
    sm.register(ProximitySense(bus, world))
    reflex = CollisionReflex(bus=bus)
    reflex.preemption = preemption
    im.register(reflex)
    im.register(StartPick(bus))
    pick, retract = Pick(bus, world), Retract(bus, world)
    am.register(pick)
    am.register(retract)
    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(), instinct_master=im,
        decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
        action_master=am, bus=bus, tick_rate_hz=TICK_HZ,
    )
    return rt, pick, retract


async def _wait_for_step(world: World, name: str, timeout: float = 3.0) -> None:
    deadline = time.perf_counter() + timeout
    while not any(e == f"step:{name}" for _, e in world.events):
        if time.perf_counter() > deadline:
            raise AssertionError(f"step {name} never started")
        await asyncio.sleep(0.005)


async def _collide_and_wait(world: World, retract: Retract) -> float:
    world.collision = True
    world.collision_at = time.perf_counter()
    deadline = time.perf_counter() + 3.0
    while retract.entered_at is None:
        if time.perf_counter() > deadline:
            raise AssertionError("reflex never executed")
        await asyncio.sleep(0.002)
    return retract.entered_at - world.collision_at


class TestTickLoopDuringLongAction:
    @pytest.mark.asyncio
    async def test_sensing_continues_while_action_runs(self) -> None:
        world = World()
        rt, _, _ = _build(world, ReflexPreemption.PREEMPT)
        await rt.start()
        await _wait_for_step(world, "lower")
        reads_before = world.reads
        await asyncio.sleep(0.2)                       # mid-pick
        # ~10 ticks at 50 Hz; the v0.12 runtime made 0 reads here.
        assert world.reads - reads_before >= 5
        await rt.stop()


class TestReflexDuringMandatoryBlock:
    @pytest.mark.asyncio
    async def test_preempt_reflex_fires_within_a_tick_inside_the_block(self) -> None:
        world = World()
        rt, pick, retract = _build(world, ReflexPreemption.PREEMPT)
        await rt.start()
        await _wait_for_step(world, "close")

        latency = await _collide_and_wait(world, retract)
        await rt.wait_idle(2.0)
        await rt.stop()

        # Bounded by one tick period plus pipeline + safe-stop, not by the
        # ~0.3 s left of the pick.
        assert latency < 2.5 / TICK_HZ
        names = [e for _, e in world.events]
        assert "step:raise" not in names               # block was cut
        assert names.index("safe_stop:holding=False") < names.index("retract")

    @pytest.mark.asyncio
    async def test_wait_for_block_reflex_fires_after_the_block(self) -> None:
        world = World()
        rt, pick, retract = _build(world, ReflexPreemption.WAIT_FOR_BLOCK)
        await rt.start()
        await _wait_for_step(world, "lower")

        latency = await _collide_and_wait(world, retract)
        await rt.wait_idle(2.0)
        await rt.stop()

        names = [e for _, e in world.events]
        assert names.index("step:raise") < names.index("retract")   # block completed
        assert "step:place" not in names                              # then handed over
        assert names.index("safe_stop:holding=True") < names.index("retract")
        # Collision arrives as "lower" starts: the reflex waits for the rest
        # of the block (lower + close + raise ≈ 0.3 s), no longer.
        assert 0.15 < latency < 3 * STEP_S + 3 / TICK_HZ


class _Patrol(MultiStepActionNode):
    node_id = "Patrol"
    resources = frozenset({"arm"})
    interrupt_policy = InterruptPolicy.ALWAYS
    timeout_s = 10.0

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.executed: list[str] = []

    def steps(self) -> list[ActionStep]:
        return [ActionStep(f"leg{i}", interruptible=True) for i in range(10)]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        self.executed.append(step.name)
        await asyncio.sleep(0.05)
        return StepResult(step_name=step.name, success=True)


class _Inspect(BaseActionNode):
    node_id = "Inspect"
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.ran_after_patrol_stopped = False
        self.patrol: _Patrol | None = None

    async def execute(self, proposal: Proposal) -> Result:
        assert self.patrol is not None
        self.ran_after_patrol_stopped = self.patrol._current_step is None
        return Result(action_id=self.node_id, success=True)


class _Planner(BaseInstinctNode):
    """Proposes Patrol first; once it runs, proposes the higher-priority Inspect."""
    node_id = "Planner"
    priority = 60

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.ticks = 0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.ticks += 1
        if self.ticks == 1:
            return Proposal(instinct_id=self.node_id, action_id="Patrol",
                            priority=40, urgency=0.5)
        if self.ticks == 5:
            return Proposal(instinct_id=self.node_id, action_id="Inspect",
                            priority=90, urgency=0.9)
        return None


class TestDeliberativeHandOff:
    @pytest.mark.asyncio
    async def test_higher_priority_proposal_interrupts_owner_and_starts_after(self) -> None:
        bus = SignalBus()
        am = ActionMasterNode(bus=bus)
        patrol, inspect = _Patrol(bus), _Inspect(bus)
        inspect.patrol = patrol
        am.register(patrol)
        am.register(inspect)
        im = InstinctMasterNode(bus=bus)
        im.register(_Planner(bus))
        rt = ArachniteRuntime(
            sense_master=SenseMasterNode(bus=bus), context=ContextNode(),
            instinct_master=im,
            decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
            action_master=am, bus=bus, tick_rate_hz=TICK_HZ,
        )
        await rt.start()
        deadline = time.perf_counter() + 3.0
        while not inspect.ran_after_patrol_stopped and time.perf_counter() < deadline:
            await asyncio.sleep(0.01)
        await rt.stop()

        # Patrol was stopped at a boundary (never ran all 10 legs) and the
        # parked Inspect proposal started only once Patrol released the arm.
        assert inspect.ran_after_patrol_stopped
        assert len(patrol.executed) < 10


class _ArmMove(BaseActionNode):
    resources = frozenset({"arm"})

    def __init__(self, bus: SignalBus, node_id: str) -> None:
        self.node_id = node_id  # type: ignore[misc]
        super().__init__(bus)
        self.calls = 0

    async def execute(self, proposal: Proposal) -> Result:
        self.calls += 1
        await asyncio.sleep(0.05)
        return Result(action_id=self.node_id, success=True)


class _OneShot(BaseInstinctNode):
    """Proposes its action on the first evaluation only."""

    def __init__(
        self, bus: SignalBus, node_id: str, action_id: str, priority: int, persist: bool,
    ) -> None:
        self.node_id = node_id  # type: ignore[misc]
        super().__init__(bus)
        self.action_id = action_id
        self.priority = priority
        self.persist = persist
        self.fired = False

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if self.fired:
            return None
        self.fired = True
        return Proposal(instinct_id=self.node_id, action_id=self.action_id,
                        priority=self.priority, urgency=0.5, persist=self.persist)


class TestSameTickResourceConflict:
    @staticmethod
    async def _run(persist: bool) -> tuple[_ArmMove, _ArmMove]:
        bus = SignalBus()
        am = ActionMasterNode(bus=bus)
        high, low = _ArmMove(bus, "HighMove"), _ArmMove(bus, "LowMove")
        am.register(high)
        am.register(low)
        im = InstinctMasterNode(bus=bus)
        im.register(_OneShot(bus, "HighInst", "HighMove", 90, persist=False))
        im.register(_OneShot(bus, "LowInst", "LowMove", 60, persist=persist))
        rt = ArachniteRuntime(
            sense_master=SenseMasterNode(bus=bus), context=ContextNode(),
            instinct_master=im,
            decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
            action_master=am, bus=bus, tick_rate_hz=TICK_HZ,
        )
        await rt.start()
        await asyncio.sleep(0.3)        # ~15 ticks: HighMove done long before the end
        await rt.stop()
        return high, low

    @pytest.mark.asyncio
    async def test_loser_without_persist_is_not_run_later(self) -> None:
        """Losing a same-tick conflict is not a hand-off: no stale dispatch."""
        high, low = await self._run(persist=False)
        assert high.calls == 1
        assert low.calls == 0

    @pytest.mark.asyncio
    async def test_persist_loser_follows_pending_rules(self) -> None:
        """A persist loser is pending, but its instinct going quiet clears it."""
        high, low = await self._run(persist=True)
        assert high.calls == 1
        assert low.calls == 0
