"""
Exercise 3 — SOLUTION
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
    priority = 150  # safety range

    async def evaluate(self, ctx) -> Proposal | None:
        battery = next(
            (s.value for s in ctx.signals if s.kind == "battery_pct"), None
        )
        if battery is not None and battery < 20:
            return Proposal(
                instinct_id=self.node_id,
                action_id="DriveHome",
                priority=self.priority,
                urgency=0.95,
            )
        return None


class ExploreInstinct(BaseInstinctNode):
    node_id = "ExploreInstinct"
    priority = 70  # goal range

    async def evaluate(self, ctx) -> Proposal | None:
        battery = next(
            (s.value for s in ctx.signals if s.kind == "battery_pct"), None
        )
        if battery is not None and battery >= 20:
            return Proposal(
                instinct_id=self.node_id,
                action_id="Explore",
                priority=self.priority,
                urgency=0.5,
            )
        return None


class PlayMusic(BaseInstinctNode):
    node_id = "PlayMusic"
    priority = 10  # exploratory range — always loses

    async def evaluate(self, ctx) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id,
            action_id="PlayMusicAction",
            priority=self.priority,
            urgency=0.1,
        )


class DriveHomeAction(BaseActionNode):
    node_id = "DriveHome"
    async def execute(self, proposal) -> Result:
        print("[CAR] Driving home...")
        return Result(action_id=self.node_id, success=True)


class ExploreAction(BaseActionNode):
    node_id = "Explore"
    async def execute(self, proposal) -> Result:
        print("[CAR] Exploring!")
        return Result(action_id=self.node_id, success=True)


class PlayMusicAction(BaseActionNode):
    node_id = "PlayMusicAction"
    async def execute(self, proposal) -> Result:
        print("[CAR] Playing music")
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
    instinct_master.register(PlayMusic(bus=bus))
    action_master.register(DriveHomeAction(bus=bus))
    action_master.register(ExploreAction(bus=bus))
    action_master.register(PlayMusicAction(bus=bus))

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
