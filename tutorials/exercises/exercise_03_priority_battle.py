"""
Exercise 3 — Priority Battle

Goal: see what happens when two instincts want to do conflicting things at
the same time. You will pick the priorities so that the RIGHT one wins.

The story:
  We have a robot car. Each tick, it senses one number called "battery_pct"
  (the battery percentage, 0..100).

  - If battery_pct < 20, an instinct called "ReturnToBase" wants to drive home.
  - If battery_pct >= 20, an instinct called "ExploreInstinct" wants to drive
    further into unknown territory.

These two instincts are NEVER both true at the same time, so there's no
conflict. But to keep things interesting, we'll add a third instinct:

  - "SpinForFun" — fires every tick, no matter what, and wants to run
    "SpinInCircles" (priority should be low).

All three actions drive the same wheels, so each one declares
resources = frozenset({"wheels"}) (lesson 6). Actions that share a resource
never run at the same time: the highest-priority proposal gets the wheels.
That part is already done for you below.

Your job:
  1. Fill in all three instincts (the conditions are described above).
  2. Set the priorities so that:
       - When the battery is low, the car goes home (highest priority of any
         normal action).
       - When the battery is fine, the car explores AND we accept that
         spinning is lower priority and never wins.
  3. Run the agent. Look at the printed output. Does it match what you expect?
  4. Bonus: delete the resources line from SpinAction and run it again.
     Now spinning doesn't need the wheels (as far as Arachnite knows), so
     it no longer competes and runs every tick alongside the others.

Hint: with the greedy decision strategy, the proposal with the highest
priority wins. So SpinForFun should have the LOWEST priority.
"""

import asyncio
import random
import time

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    BaseSenseNode, SenseMasterNode,
    BaseInstinctNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    BaseActionNode, ActionMasterNode,
)


class BatterySensor(BaseSenseNode):
    node_id = "BatterySensor"
    signal_kind = "battery_pct"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=random.randint(0, 100),
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class ReturnToBase(BaseInstinctNode):
    node_id = "ReturnToBase"
    priority = ...   # TODO: pick a number (safety range = 100..199)

    async def evaluate(self, ctx) -> Proposal | None:
        # TODO: fire when battery_pct < 20.
        ...


class ExploreInstinct(BaseInstinctNode):
    node_id = "ExploreInstinct"
    priority = ...   # TODO: pick a number (goal range = 50..99)

    async def evaluate(self, ctx) -> Proposal | None:
        # TODO: fire when battery_pct >= 20.
        ...


class SpinForFun(BaseInstinctNode):
    node_id = "SpinForFun"
    priority = ...   # TODO: pick a number (exploratory range = 1..49)

    async def evaluate(self, ctx) -> Proposal | None:
        # TODO: always fire (return a Proposal every tick).
        ...


class DriveHomeAction(BaseActionNode):
    node_id = "DriveHome"
    resources = frozenset({"wheels"})   # all three actions drive the wheels
    async def execute(self, proposal) -> Result:
        print("[CAR] Driving home...")
        return Result(action_id=self.node_id, success=True)


class ExploreAction(BaseActionNode):
    node_id = "Explore"
    resources = frozenset({"wheels"})
    async def execute(self, proposal) -> Result:
        print("[CAR] Exploring!")
        return Result(action_id=self.node_id, success=True)


class SpinAction(BaseActionNode):
    node_id = "SpinInCircles"
    resources = frozenset({"wheels"})
    async def execute(self, proposal) -> Result:
        print("[CAR] Spinning in circles, wheee!")
        return Result(action_id=self.node_id, success=True)


async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(BatterySensor(bus=bus))
    instinct_master.register(ReturnToBase(bus=bus))
    instinct_master.register(ExploreInstinct(bus=bus))
    instinct_master.register(SpinForFun(bus=bus))
    action_master.register(DriveHomeAction(bus=bus))
    action_master.register(ExploreAction(bus=bus))
    action_master.register(SpinAction(bus=bus))

    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=2.0,
    )

    await rt.start()
    await asyncio.sleep(5.0)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
