"""
Exercise 2 — SOLUTION
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


class SoilMoistureSensor(BaseSenseNode):
    node_id = "SoilMoistureSensor"
    signal_kind = "soil_moisture"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=random.randint(0, 100),
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class TemperatureSensor(BaseSenseNode):
    node_id = "TemperatureSensor"
    signal_kind = "temperature"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=random.randint(10, 35),
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class WaterPlantsInstinct(BaseInstinctNode):
    node_id = "WaterPlantsInstinct"
    priority = 60

    async def evaluate(self, ctx) -> Proposal | None:
        moisture = next(
            (s.value for s in ctx.signals if s.kind == "soil_moisture"), None
        )
        temperature = next(
            (s.value for s in ctx.signals if s.kind == "temperature"), None
        )
        if moisture is None or temperature is None:
            return None
        if moisture < 30 and temperature > 20:
            return Proposal(
                instinct_id=self.node_id,
                action_id="WaterPlants",
                priority=self.priority,
                urgency=0.6,
                rationale=f"moisture={moisture}, temperature={temperature}",
            )
        return None


class WaterPlants(BaseActionNode):
    node_id = "WaterPlants"

    async def execute(self, proposal) -> Result:
        print(f"Watering plants ({proposal.rationale})")
        return Result(action_id=self.node_id, success=True)


async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(SoilMoistureSensor(bus=bus))
    sense_master.register(TemperatureSensor(bus=bus))
    instinct_master.register(WaterPlantsInstinct(bus=bus))
    action_master.register(WaterPlants(bus=bus))

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
