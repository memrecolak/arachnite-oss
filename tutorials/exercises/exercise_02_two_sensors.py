"""
Exercise 2 — Two Sensors

Goal: build an agent that combines TWO different sensor readings inside one
instinct.

Imagine a greenhouse. We want to water the plants only when:
  - the soil is dry (moisture < 30), AND
  - the temperature is warm (temperature > 20).

You will:
  1. Build two sensors: SoilMoistureSensor and TemperatureSensor.
     For now, both should return random-ish values so things vary tick by
     tick. (Use the random module.)
  2. Build one instinct: WaterPlantsInstinct that fires only when BOTH
     conditions are true.
  3. Build one action: WaterPlants that prints what it's doing.
  4. Wire it up and run it for 5 seconds at 2 ticks per second.

Look for the TODO comments below.
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
        # TODO: return a Signal whose value is a random integer 0..100.
        ...


class TemperatureSensor(BaseSenseNode):
    node_id = "TemperatureSensor"
    signal_kind = "temperature"

    async def read(self) -> Signal:
        # TODO: return a Signal whose value is a random integer 10..35.
        ...


class WaterPlantsInstinct(BaseInstinctNode):
    node_id = "WaterPlantsInstinct"
    priority = 60

    async def evaluate(self, ctx) -> Proposal | None:
        # TODO:
        #   1. Find the most recent soil_moisture signal in ctx.signals.
        #   2. Find the most recent temperature signal in ctx.signals.
        #   3. If moisture < 30 AND temperature > 20, return a Proposal
        #      for action_id="WaterPlants".
        #   4. Otherwise return None.
        ...


class WaterPlants(BaseActionNode):
    node_id = "WaterPlants"

    async def execute(self, proposal) -> Result:
        # TODO: print a message saying you are watering the plants.
        #       Return a successful Result.
        ...


async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    # TODO: register both sensors with sense_master
    # TODO: register WaterPlantsInstinct with instinct_master
    # TODO: register WaterPlants with action_master

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
