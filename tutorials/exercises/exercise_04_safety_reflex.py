"""
Exercise 4 — Safety Reflex

Goal: practice writing a reflex that bypasses the decision layer.

The story:
  We have a 3D printer. Each tick, we sense the print head temperature.

  Normal instincts:
    - "PrintLayerInstinct" — fires when the temperature is in a healthy
      printing range (180..230). Wants to run "PrintLayer".
    - "WaitForWarmupInstinct" — fires when temp is below 180. Wants to run
      "WaitForWarmup".

  REFLEX:
    - "OverheatReflex" — fires when temp > 260. Must instantly run
      "ShutdownPrinter" no matter what else is happening.

  Printing a layer takes 1.5 seconds, so PrintLayer is often still running
  when the next ticks happen (lesson 5). PrintLayer, WaitForWarmup and
  ShutdownPrinter all drive the same print head, so they all declare
  resources = frozenset({"hotend"}) (lesson 6). That part is already done
  for you below.

Your job:
  1. Make OverheatReflex inherit from BaseReflexInstinctNode (NOT
     BaseInstinctNode).
  2. Give it a priority >= 200.
  3. Implement evaluate() so it returns a Proposal when temp > 260.
  4. Run the agent. When the temperature randomly spikes, you should see
     "!!! SHUTDOWN !!!" instead of any normal action. On the next tick with a
     normal reading the printer carries on (e.g. "layer started" again):
     this pretend agent does not remember the shutdown, and every tick is
     decided afresh. A real printer would stay off until someone resets it.
  5. Look for a spike that lands while a layer is printing. You'll see
       [print]   layer cut short by OverheatReflex, head parked
       !!! SHUTDOWN !!! ...
     The reflex did not wait for the layer to finish: it preempted
     PrintLayer, PrintLayer's on_preempted() safe-stop ran, and only then
     did ShutdownPrinter start. (The sensor is random: if you don't see it,
     run the file again.)

Look at lesson 7 if you need a refresher on reflexes.
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
    """Pretend print-head sensor — usually safe, sometimes spikes."""
    node_id = "HotEndSensor"
    signal_kind = "hotend_temp"

    async def read(self) -> Signal:
        # 15% chance of a critical spike for the demo
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


# TODO: implement this reflex.
class OverheatReflex(...):     # TODO: pick the right base class
    node_id = "OverheatReflex"
    priority = ...             # TODO: must be >= 200

    async def evaluate(self, ctx) -> Proposal | None:
        # TODO: read the most recent hotend_temp signal.
        #       If it's > 260, return a Proposal for "ShutdownPrinter".
        #       Otherwise return None.
        ...


class WaitForWarmup(BaseActionNode):
    node_id = "WaitForWarmup"
    resources = frozenset({"hotend"})

    async def execute(self, proposal) -> Result:
        print(f"  [warm up] {proposal.rationale}")
        return Result(action_id=self.node_id, success=True)


class PrintLayer(BaseActionNode):
    """Printing a layer takes a while, so it spans several ticks."""
    node_id = "PrintLayer"
    resources = frozenset({"hotend"})   # the print head this action drives

    async def execute(self, proposal) -> Result:
        print(f"  [print]   layer started ({proposal.rationale})")
        await asyncio.sleep(1.5)
        print("  [print]   layer done")
        return Result(action_id=self.node_id, success=True)

    async def on_preempted(self, proposal, preempted_by) -> None:
        # Safe-stop: called when a reflex takes the hot end away mid-layer.
        # Park the head where it is; don't try to "undo" the layer.
        print(f"  [print]   layer cut short by {preempted_by.instinct_id}, head parked")


class ShutdownPrinter(BaseActionNode):
    node_id = "ShutdownPrinter"
    resources = frozenset({"hotend"})   # same hardware as PrintLayer

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
