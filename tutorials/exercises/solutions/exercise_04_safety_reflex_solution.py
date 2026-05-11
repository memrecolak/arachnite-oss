"""
Exercise 4 — SOLUTION
"""

import asyncio
import random
import time

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    BaseSenseNode, SenseMasterNode,
    BaseInstinctNode, BaseReflexInstinctNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    BaseActionNode, ActionMasterNode,
)


class HotEndSensor(BaseSenseNode):
    node_id = "HotEndSensor"
    signal_kind = "hotend_temp"

    async def read(self) -> Signal:
        if random.random() < 0.15:
            value = random.uniform(265, 320)
        else:
            value = random.uniform(150, 240)
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=value,
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class WaitForWarmupInstinct(BaseInstinctNode):
    node_id = "WaitForWarmupInstinct"
    priority = 60

    async def evaluate(self, ctx) -> Proposal | None:
        temp = next(
            (s.value for s in ctx.signals if s.kind == "hotend_temp"), None
        )
        if temp is not None and temp < 180:
            return Proposal(
                instinct_id=self.node_id,
                action_id="WaitForWarmup",
                priority=self.priority,
                urgency=0.4,
                rationale=f"temp={temp:.0f}",
            )
        return None


class PrintLayerInstinct(BaseInstinctNode):
    node_id = "PrintLayerInstinct"
    priority = 80

    async def evaluate(self, ctx) -> Proposal | None:
        temp = next(
            (s.value for s in ctx.signals if s.kind == "hotend_temp"), None
        )
        if temp is not None and 180 <= temp <= 230:
            return Proposal(
                instinct_id=self.node_id,
                action_id="PrintLayer",
                priority=self.priority,
                urgency=0.7,
                rationale=f"temp={temp:.0f}",
            )
        return None


class OverheatReflex(BaseReflexInstinctNode):
    node_id = "OverheatReflex"
    priority = 250

    async def evaluate(self, ctx) -> Proposal | None:
        temp = next(
            (s.value for s in ctx.signals if s.kind == "hotend_temp"), None
        )
        if temp is not None and temp > 260:
            return Proposal(
                instinct_id=self.node_id,
                action_id="ShutdownPrinter",
                priority=self.priority,
                urgency=1.0,
                rationale=f"temp={temp:.0f} CRITICAL",
            )
        return None


class WaitForWarmup(BaseActionNode):
    node_id = "WaitForWarmup"
    async def execute(self, proposal) -> Result:
        print(f"  [warm up] {proposal.rationale}")
        return Result(action_id=self.node_id, success=True)


class PrintLayer(BaseActionNode):
    node_id = "PrintLayer"
    async def execute(self, proposal) -> Result:
        print(f"  [print]   {proposal.rationale}")
        return Result(action_id=self.node_id, success=True)


class ShutdownPrinter(BaseActionNode):
    node_id = "ShutdownPrinter"
    async def execute(self, proposal) -> Result:
        print(f"  !!! SHUTDOWN !!! {proposal.rationale}")
        return Result(action_id=self.node_id, success=True)


async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(HotEndSensor(bus=bus))
    instinct_master.register(WaitForWarmupInstinct(bus=bus))
    instinct_master.register(PrintLayerInstinct(bus=bus))
    instinct_master.register(OverheatReflex(bus=bus))
    action_master.register(WaitForWarmup(bus=bus))
    action_master.register(PrintLayer(bus=bus))
    action_master.register(ShutdownPrinter(bus=bus))

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
    await asyncio.sleep(8.0)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
