"""
Exercise 1 — SOLUTION
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


class HundredSensor(BaseSenseNode):
    node_id = "HundredSensor"
    signal_kind = "number"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=100,
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class BigNumberInstinct(BaseInstinctNode):
    node_id = "BigNumberInstinct"
    priority = 50

    async def evaluate(self, ctx) -> Proposal | None:
        big = [s for s in ctx.signals if s.kind == "number" and s.value > 50]
        if big:
            return Proposal(
                instinct_id=self.node_id,
                action_id="SayHello",
                priority=self.priority,
                urgency=0.5,
            )
        return None


class SayHello(BaseActionNode):
    node_id = "SayHello"

    async def execute(self, proposal) -> Result:
        print("Hello from my first agent!")
        return Result(action_id=self.node_id, success=True)


async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(HundredSensor(bus=bus))
    instinct_master.register(BigNumberInstinct(bus=bus))
    action_master.register(SayHello(bus=bus))

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
