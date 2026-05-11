"""
Exercise 1 — Hello, Agent

Goal: build the simplest possible Arachnite agent from scratch.

Your agent should:
  - Have one sensor that always reports the value 100.
  - Have one instinct that fires whenever it sees a value > 50.
  - Have one action that prints "Hello from my first agent!".
  - Run at 1 tick per second for 3 seconds.

Look for the `TODO` comments below and fill in the blanks.
When you run this file, you should see "Hello from my first agent!" printed
about 3 times.

Hint: lesson 4 (Your First Agent) walks through almost the same program.
"""

import asyncio
import time

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    BaseSenseNode, SenseMasterNode,
    BaseInstinctNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    BaseActionNode, ActionMasterNode,
)


# ─── 1. Sensor ───────────────────────────────────────────
class HundredSensor(BaseSenseNode):
    node_id = "HundredSensor"
    signal_kind = "number"

    async def read(self) -> Signal:
        # TODO: return a Signal with value=100, confidence=1.0,
        #       and source/kind matching the class.
        ...


# ─── 2. Instinct ─────────────────────────────────────────
class BigNumberInstinct(BaseInstinctNode):
    node_id = "BigNumberInstinct"
    priority = 50

    async def evaluate(self, ctx) -> Proposal | None:
        # TODO: look through ctx.signals for any "number" signal
        #       whose value is greater than 50. If you find one,
        #       return a Proposal with action_id="SayHello".
        #       Otherwise return None.
        ...


# ─── 3. Action ───────────────────────────────────────────
class SayHello(BaseActionNode):
    node_id = "SayHello"

    async def execute(self, proposal) -> Result:
        # TODO: print "Hello from my first agent!" and return
        #       a successful Result.
        ...


# ─── 4. Wiring ───────────────────────────────────────────
async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    # TODO: register HundredSensor with sense_master
    # TODO: register BigNumberInstinct with instinct_master
    # TODO: register SayHello with action_master

    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=1.0,
    )

    await rt.start()
    await asyncio.sleep(3.0)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
