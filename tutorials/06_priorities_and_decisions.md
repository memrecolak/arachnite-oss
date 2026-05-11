# Lesson 6 — When Instincts Compete

So far, our agent has had one instinct. But real agents have many. What
happens when **two or more instincts fire on the same tick**?

That's what the **decision layer** is for.

## A small story

Imagine a robot vacuum cleaner. It has these instincts:

| Instinct | When it fires | What it proposes |
|---|---|---|
| `LowBattery` | Battery below 20% | Go to the charging dock |
| `RoomDirty` | Sees dust ahead | Vacuum |
| `Bored` | Hasn't moved in a while | Drive somewhere new |

What should happen if **all three fire at the same time?** The vacuum can't
charge, vacuum, *and* drive somewhere new in the same instant. It has to pick
one.

Without the decision layer, the program would have to be one giant `if/elif`
ladder full of "what if A and B and not C" rules. That gets messy fast.

Arachnite handles it differently: every instinct just says what it wants
("here's my Proposal, with my priority"), and the decision layer picks the
winner.

## Priority is the main idea

Each Proposal has a `priority` number. The higher, the more important. By
default, the decision layer is **greedy** — it picks the proposal with the
highest priority and runs it. The others are ignored for that tick.

Arachnite suggests these priority ranges (you saw them in the code):

| Range | Meaning |
|---|---|
| **200+** | Reflex instincts only (we'll cover these in lesson 7) |
| **100–199** | Safety / survival ("don't crash", "don't run out of power") |
| **50–99** | Goal-directed ("do the job we're built for") |
| **1–49** | Exploratory / maintenance ("try something new", "self-test") |

For our vacuum:

```python
class LowBatteryInstinct(BaseInstinctNode):
    node_id = "LowBatteryInstinct"
    priority = 150  # safety — we don't want to die

class RoomDirtyInstinct(BaseInstinctNode):
    node_id = "RoomDirtyInstinct"
    priority = 70   # goal-directed — that's what we're for

class BoredInstinct(BaseInstinctNode):
    node_id = "BoredInstinct"
    priority = 20   # exploratory
```

If all three fire on the same tick:

1. The decision layer collects three proposals.
2. The greedy strategy sorts them: 150, 70, 20.
3. Priority 150 wins: the vacuum heads to the dock.
4. The other two are dropped for this tick.

Next tick, the battery situation hasn't changed yet, so `LowBatteryInstinct`
fires again and wins again. The vacuum keeps driving to the dock until it
reaches it, then `LowBatteryInstinct` stops firing, and the next tick
`RoomDirtyInstinct` finally gets its turn.

That's the magic: **each instinct is selfish — it just shouts what it wants
— and priorities sort it all out.** No tangled `if/else` ladder.

## Building a competing-instincts demo

Let's modify our smart-fan example so it has two instincts that can disagree.

```python
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


class TempSense(BaseSenseNode):
    node_id = "TempSense"
    signal_kind = "temperature"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=42.0,
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class MildHeat(BaseInstinctNode):
    """Suggests opening a window when it's a bit warm."""
    node_id = "MildHeat"
    priority = 30  # exploratory

    async def evaluate(self, ctx) -> Proposal | None:
        warm = [s for s in ctx.signals
                if s.kind == "temperature" and 30 < s.value <= 45]
        if warm:
            return Proposal(
                instinct_id=self.node_id,
                action_id="OpenWindow",
                priority=self.priority,
                urgency=0.4,
            )
        return None


class HotEmergency(BaseInstinctNode):
    """Demands the fan when it's actually hot."""
    node_id = "HotEmergency"
    priority = 120  # safety

    async def evaluate(self, ctx) -> Proposal | None:
        hot = [s for s in ctx.signals
               if s.kind == "temperature" and s.value > 40]
        if hot:
            return Proposal(
                instinct_id=self.node_id,
                action_id="TurnOnFan",
                priority=self.priority,
                urgency=0.95,
            )
        return None


class OpenWindow(BaseActionNode):
    node_id = "OpenWindow"
    async def execute(self, proposal) -> Result:
        print(">>> Window opened")
        return Result(action_id=self.node_id, success=True)


class TurnOnFan(BaseActionNode):
    node_id = "TurnOnFan"
    async def execute(self, proposal) -> Result:
        print(">>> Fan turned ON")
        return Result(action_id=self.node_id, success=True)


async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(TempSense(bus=bus))
    instinct_master.register(MildHeat(bus=bus))
    instinct_master.register(HotEmergency(bus=bus))
    action_master.register(OpenWindow(bus=bus))
    action_master.register(TurnOnFan(bus=bus))

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
    await asyncio.sleep(3.0)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

When the temperature is 42 degrees, **both** `MildHeat` (30 < 42 ≤ 45) **and**
`HotEmergency` (42 > 40) want to fire. But `HotEmergency` has priority 120
and `MildHeat` has priority 30. The fan wins. You'll only see "Fan turned
ON".

Now try changing `value=42.0` to `value=35.0`. Now only `MildHeat` fires
(35 isn't > 40). You'll see "Window opened" instead.

Try `value=20.0`. Neither fires. You'll see nothing.

This is decision-making by priority, in action.

## Other decision strategies

`GreedyDecisionNode` is the simplest strategy: always pick the highest
priority. But Arachnite has others, and you can write your own. Some
built-ins:

- **Greedy** — pick the highest priority. (You've used this one.)
- **Weighted random** — pick randomly, but with higher-priority proposals
  more likely to win. (Useful for exploration.)
- **Top-K** — run the top K proposals concurrently. (Useful when actions
  don't conflict — for example, "turn on a light" and "play a sound" can
  happen at the same time.)

You don't need any of these for now. Just know they exist for when your agent
gets fancier.

## When two instincts have the same priority

What if `MildHeat` and `HotEmergency` *both* have priority 80? The greedy
strategy picks one, but you don't get to control which. **Best practice:
give every instinct a different priority** so you always know which one will
win.

## Recap

- Multiple instincts can fire on the same tick.
- Each one returns a `Proposal` with a `priority`.
- The decision layer picks a winner.
- Higher priority = more important.
- Stick to the priority ranges (safety > goal > exploration) so your numbers
  stay meaningful.

## What's next?

There's still one more piece to the puzzle: **what if a situation is so
urgent that we don't even want to go through the decision layer?** That's
what reflexes are for.

[← Previous: The Tick Loop](05_the_tick_loop.md) | [Next: Lesson 7 — Reflexes →](07_reflexes.md)
