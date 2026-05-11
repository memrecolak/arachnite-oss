# Lesson 3 — Meet the Pieces

In this lesson we'll meet the actual Python classes you'll work with. Don't
worry about memorizing every field — just get a feel for what each piece is
*for*.

There are two kinds of pieces:

1. **Data classes** — small bundles of information that get passed around.
   Think of them as labelled envelopes.
2. **Node classes** — pieces of behaviour that you customize. You write a
   class that **inherits** from one of Arachnite's base classes and fills in
   the parts that are specific to your idea.

## The three envelopes

### Signal — "I observed something"

A `Signal` is what a sensor produces. It's a labelled package of data.

```python
from arachnite import Signal
import time

my_signal = Signal(
    source="TempSensor",        # who made me?
    kind="temperature",         # what kind of data am I?
    value=42.0,                 # the actual reading
    confidence=1.0,             # how sure are we? (0.0 to 1.0)
    timestamp=time.monotonic(), # when was this read?
)
```

Think of a Signal as a sticky note: *"At 3:14pm, the temperature sensor read
42 degrees, and we're 100% confident in this reading."*

### Proposal — "I want to do something"

A `Proposal` is what an instinct produces when it wants something to happen.

```python
from arachnite import Proposal

my_proposal = Proposal(
    instinct_id="HotInstinct",  # who proposed me?
    action_id="CoolDown",       # which action should run?
    priority=80,                # how important is this? (higher = more important)
    urgency=0.8,                # 0.0 (whenever) to 1.0 (right now!)
    parameters={"target_temp": 25.0},  # extra info for the action
)
```

A Proposal is like a sticky note that says *"Hey, I'm the HotInstinct, and I
want you to run the CoolDown action. It's pretty important. Here's the target
temperature."*

The most confusing part for beginners: `action_id` is a **string** that has to
match the name of an action node somewhere else. We'll see why this works in
the next lesson.

### Result — "I finished the action"

A `Result` is what an action node produces after it runs.

```python
from arachnite import Result

my_result = Result(
    action_id="CoolDown",    # which action ran?
    success=True,            # did it work?
    output={"fan_speed": 100},  # any data the action produced
)
```

A Result is the receipt: *"The CoolDown action ran. It worked. The fan is now
at 100% speed."*

## The three node types

Now let's meet the classes you actually write. Each one has **one job** and
**one method** you absolutely need to implement.

### BaseSenseNode — for sensing

```python
from arachnite import BaseSenseNode, Signal
import time

class TempSensor(BaseSenseNode):
    node_id = "TempSensor"        # a unique name for this node
    signal_kind = "temperature"   # what kind of signal it produces

    async def read(self) -> Signal:
        # This is the one method you must write.
        # Pretend we're reading from a real sensor here.
        value = 42.0
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=value,
            confidence=1.0,
            timestamp=time.monotonic(),
        )
```

Let's break this down:

- `class TempSensor(BaseSenseNode):` — we're making a new class that
  **inherits** from `BaseSenseNode`. That means we get all of Arachnite's
  built-in sensor behaviour for free, and we just add the parts that are
  unique to our temperature sensor.
- `node_id = "TempSensor"` — every node needs a unique name (a string).
  Other parts of the framework use this name to talk about your node.
- `signal_kind = "temperature"` — the kind of signal this sensor produces.
  Other nodes can listen for "temperature" signals.
- `async def read(self):` — the method the framework calls every tick.
  The `async` keyword is required (we'll explain it more in a later lesson —
  for now, just know that all node methods need it).

> **Tip — `poll_interval_s`:** By default, sensors are throttled to read
> once every 0.1 seconds (even if the tick rate is faster). If you need a
> sensor to read on **every** tick, add `poll_interval_s = 0.0` to your
> class. See [Lesson 4](04_your_first_agent.md) and
> [the spec](../spec/02_nodes.md) for details.

### BaseInstinctNode — for thinking

```python
from arachnite import BaseInstinctNode, Proposal

class HotInstinct(BaseInstinctNode):
    node_id = "HotInstinct"
    priority = 80   # how important am I? (1 to 199 for normal instincts)

    async def evaluate(self, ctx) -> Proposal | None:
        # Look at all the temperature signals in the current context
        hot_readings = [
            s for s in ctx.signals
            if s.kind == "temperature" and s.value > 40.0
        ]
        if hot_readings:
            return Proposal(
                instinct_id=self.node_id,
                action_id="CoolDown",
                priority=self.priority,
                urgency=0.8,
            )
        return None  # no proposal this tick
```

Things to notice:

- The instinct gets a `ctx` (short for *context*) — this is the agent's
  short-term memory. It holds all the signals that came in during this tick.
- We look through `ctx.signals` for any temperature reading above 40.
- If we find one, we return a `Proposal`. If not, we return `None`.
- **Never raise an exception** here. If your instinct doesn't apply, just
  return `None`.

### BaseActionNode — for acting

```python
from arachnite import BaseActionNode, Result

class CoolDown(BaseActionNode):
    node_id = "CoolDown"

    async def execute(self, proposal) -> Result:
        # In a real system, this is where you'd turn on a fan,
        # send a signal to hardware, etc.
        print("Cooling down!")
        return Result(action_id=self.node_id, success=True)
```

Things to notice:

- The action gets the `proposal` so it can read any extra parameters the
  instinct passed.
- It always returns a `Result`. Even if it fails, you return a Result with
  `success=False` — never raise an exception.

## How they connect

The instinct's `action_id="CoolDown"` matches the action's
`node_id="CoolDown"`. That's how Arachnite knows which action to run when the
instinct fires. The two classes never call each other directly — they're
linked by name.

This is a key idea: **nodes never reference each other directly**. They
communicate through messages and names. This makes it easy to add, remove, or
swap pieces without breaking the rest of the agent.

## Recap

| You write... | To do... | And return... |
|---|---|---|
| `BaseSenseNode.read()` | Read the world | A `Signal` |
| `BaseInstinctNode.evaluate()` | Decide if you want to act | A `Proposal` or `None` |
| `BaseActionNode.execute()` | Carry out the action | A `Result` |

That's the whole framework, in a nutshell. Everything else is plumbing that
ties these three things together.

## What's next?

You've seen the pieces. In the next lesson, we'll combine them into a
complete, runnable agent — and walk through every line of the code.

[← Previous: The Big Idea](02_the_big_idea.md) | [Next: Lesson 4 — Your First Agent →](04_your_first_agent.md)
