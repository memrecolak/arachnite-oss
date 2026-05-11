   # Lesson 4 — Your First Agent

Time to build something that actually runs! In this lesson we'll write a
complete agent and walk through every line of code together.

Our agent will be the **smart fan** we've been talking about: it senses the
temperature, fires an instinct when things get too hot, and runs a "cool down"
action.

## The whole program

Here's the full code. Don't be intimidated by the length — most of it is
glue, and we'll explain every part. Save this as `my_first_agent.py` in any
folder you like.

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


# 1. The sensor
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


# 2. The instinct
class HotInstinct(BaseInstinctNode):
    node_id = "HotInstinct"
    priority = 80

    async def evaluate(self, ctx) -> Proposal | None:
        hot = [s for s in ctx.signals
               if s.kind == "temperature" and s.value > 40.0]
        if hot:
            return Proposal(
                instinct_id=self.node_id,
                action_id="CoolDown",
                priority=self.priority,
                urgency=0.9,
            )
        return None


# 3. The action
class CoolDown(BaseActionNode):
    node_id = "CoolDown"

    async def execute(self, proposal) -> Result:
        print("Cooling down!")
        return Result(action_id=self.node_id, success=True)


# 4. Wire everything together and run
async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(TempSense(bus=bus))
    instinct_master.register(HotInstinct(bus=bus))
    action_master.register(CoolDown(bus=bus))

    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=2.0,  # 2 ticks per second
    )

    await rt.start()
    await asyncio.sleep(5.0)  # let it run for 5 seconds
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

### The easier way — RuntimeBuilder

That was a lot of wiring! Arachnite provides a **RuntimeBuilder** that handles
all the bus, master node, and registration boilerplate for you:

```python
from arachnite import RuntimeBuilder

async def main() -> None:
    rt = (
        RuntimeBuilder()
        .sense(TempSense)
        .instinct(HotInstinct)
        .action(CoolDown)
        .tick_rate(2.0)
        .build()
    )
    await rt.start()
    await asyncio.sleep(5.0)
    await rt.stop()
```

Same result, much less code. The builder creates the `SignalBus`, all four
master nodes, and a `ContextNode` internally. It defaults to
`GreedyDecisionNode` as the decision strategy.

You can pass **classes** (like above) and the builder instantiates them with
its internal bus, or pass **instances** when you need custom constructor
arguments:

```python
builder = RuntimeBuilder()
rt = (
    builder
    .sense(TempSense(bus=builder.bus, value=42.0))   # pre-built instance
    .instinct(HotInstinct)                            # class — auto-instantiated
    .action(CoolDown)
    .strategy(RandomDecisionNode)                     # override decision strategy
    .tick_rate(2.0)
    .build()
)
```

Both approaches produce exactly the same `ArachniteRuntime` — use whichever
you prefer. The manual version gives you full control over every object; the
builder is great for getting started quickly.

---

To run either version, install Arachnite first (`pip install -e .` from the project root)
and then:

```bash
python my_first_agent.py
```

You should see "Cooling down!" printed about 10 times (2 times per second for
5 seconds).

Now let's walk through what every part does.

## Part 1 — The imports

```python
import asyncio
import time
```

- `asyncio` is Python's built-in tool for running things at the same time
  ("concurrently"). We need it because Arachnite is async.
- `time` is for timestamps.

```python
from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    BaseSenseNode, SenseMasterNode,
    BaseInstinctNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    BaseActionNode, ActionMasterNode,
)
```

This is a long import, but it's just listing every Arachnite piece we need:

- The data envelopes — `Signal`, `Proposal`, `Result`
- The base classes we'll inherit from — `BaseSenseNode`, `BaseInstinctNode`,
  `BaseActionNode`
- The "master" nodes that hold collections of our nodes — `SenseMasterNode`,
  `InstinctMasterNode`, `ActionMasterNode`, `DecisionMasterNode`
- The infrastructure — `SignalBus`, `ContextNode`, `ArachniteRuntime`
- A built-in decision strategy — `GreedyDecisionNode` (picks the
  highest-priority proposal)

You don't have to memorize this list. Most agents need most of these.

## Part 2 — The sensor

```python
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
```

We're inheriting from `BaseSenseNode` and filling in the parts unique to our
sensor:

- The `node_id` is just `"TempSense"` — a unique name.
- The `signal_kind` is `"temperature"` — what kind of data this sensor produces.
- `read()` is the method the framework calls every tick. We return a Signal
  with the value `42.0`. (In a real program, you'd read from an actual
  temperature sensor here, but hardcoding is fine for learning.)

The `async` keyword in front of `def read` says "this is an async function".
You'll see it in front of every node method. For now, just remember: **all
node methods need `async`.** We'll explain why in lesson 5.

## Part 3 — The instinct

```python
class HotInstinct(BaseInstinctNode):
    node_id = "HotInstinct"
    priority = 80

    async def evaluate(self, ctx) -> Proposal | None:
        hot = [s for s in ctx.signals
               if s.kind == "temperature" and s.value > 40.0]
        if hot:
            return Proposal(
                instinct_id=self.node_id,
                action_id="CoolDown",
                priority=self.priority,
                urgency=0.9,
            )
        return None
```

This instinct watches for hot temperatures. Let's read it carefully:

- `priority = 80` — this is our instinct's importance score. Higher numbers
  win when multiple instincts fire at the same time.
- `evaluate(self, ctx)` — the framework calls this every tick and gives us
  the current context (`ctx`).
- `ctx.signals` — a list of all the signals from this tick.
- The list comprehension `[s for s in ctx.signals if ...]` filters the
  signals down to only the temperature ones above 40 degrees.
- If we found any, we return a `Proposal` saying "run the CoolDown action".
- Otherwise we return `None`, which means "I don't want to do anything right
  now."

**Important:** the `action_id="CoolDown"` is the name that will match our
action node down below. If you typo it, the action won't run.

## Part 4 — The action

```python
class CoolDown(BaseActionNode):
    node_id = "CoolDown"

    async def execute(self, proposal) -> Result:
        print("Cooling down!")
        return Result(action_id=self.node_id, success=True)
```

This is the simplest possible action. It:

- Has `node_id = "CoolDown"` (matches the proposal's `action_id`).
- Defines `execute(self, proposal)`, which is what runs when the action is
  chosen.
- Prints a message and returns a successful Result.

In a real program, instead of `print()`, you might turn on a fan via a GPIO
pin, send a network message, or update a database.

## Part 5 — Wiring it all together

Now the most complicated-looking part, but it's pure plumbing. We're building
the agent piece by piece.

```python
async def main() -> None:
    bus = SignalBus()
```

We start by creating the **SignalBus**. This is the messaging system that
lets all the nodes talk to each other without needing references.

```python
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)
```

We create one **master node** for each layer. A master is a node that holds
*other* nodes — it manages all the senses, all the instincts, all the
actions. You can have many sensors but only one sense master.

The decision master also gets a **strategy** — a small object that decides
*how* to pick winners when multiple proposals come in. We're using
`GreedyDecisionNode`, which always picks the proposal with the highest
priority. (There are other strategies you can use later.)

```python
    sense_master.register(TempSense(bus=bus))
    instinct_master.register(HotInstinct(bus=bus))
    action_master.register(CoolDown(bus=bus))
```

We register our three custom nodes with their masters. This is how the agent
knows which sensors to read, which instincts to evaluate, and which actions
are available.

```python
    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=2.0,
    )
```

We build the **runtime** — the heart of the agent. The runtime owns the tick
loop. It needs to know about every master node and the bus and the context.

`tick_rate_hz=2.0` means "run 2 ticks per second" (every 500 milliseconds).
Try changing it to `5.0` later and see what happens.

```python
    await rt.start()
    await asyncio.sleep(5.0)
    await rt.stop()
```

- `start()` kicks off the tick loop in the background.
- `asyncio.sleep(5.0)` waits 5 seconds while the agent runs.
- `stop()` shuts the loop down cleanly.

Notice all three of these have `await` in front of them. That's because
they're async functions — they can run alongside other things. We'll cover
async properly in the next lesson.

## Part 6 — Running it

```python
if __name__ == "__main__":
    asyncio.run(main())
```

This is the standard Python "main" pattern, plus `asyncio.run()` because our
`main` function is async. `asyncio.run()` is what actually starts the async
machinery.

## Try it!

After running it, try these experiments:

1. **Change the temperature.** In `TempSense`, change `value=42.0` to
   `value=10.0`. Re-run. The instinct shouldn't fire any more — no more
   "Cooling down!" messages.
2. **Change the threshold.** Change `> 40.0` in `HotInstinct` to `> 50.0`.
   Re-run.
3. **Change the tick rate.** Set `tick_rate_hz=5.0`. The agent will print
   more often.
4. **Add another action.** Add a second print statement so the message says
   "Fan ON!" instead of "Cooling down!".
5. **Add a second instinct.** Make a `MildHeatInstinct` that fires for
   temperatures between 30 and 40, and just prints a warning.

Each of these tiny experiments will teach you more than reading another page
of explanation. Go play!

## What's next?

You wrote and ran your first agent. Next up: a closer look at what's
happening *inside* the runtime when it ticks — and a proper explanation of
that mysterious `async` keyword.

[← Previous: Meet the Pieces](03_meet_the_pieces.md) | [Next: Lesson 5 — The Tick Loop →](05_the_tick_loop.md)
