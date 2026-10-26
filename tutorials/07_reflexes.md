# Lesson 7 — Reflexes: Emergency Reactions

Some situations are so urgent that you can't afford to think. If you touch a
hot stove, you don't deliberate — your hand jerks back before your brain even
registers what happened. That's a **reflex**.

Real spiders work the same way. A spider doesn't reason about "should I jump
when something pokes me?" — it just jumps. Reflex first, thought later.

Arachnite has reflexes too. They're a special kind of instinct that **bypass
the decision layer entirely** and run their action immediately.

## Why bypass the decision layer?

Imagine our smart fan agent grew into a smart factory controller. It now has
20 instincts and a decision layer that takes a few milliseconds to evaluate
all of them and pick a winner. That's fine for normal operation.

But what if a temperature sensor reads **95 degrees** — way above any safe
limit? You don't want to wait for the decision layer to consider 19 other
proposals. You want to **shut things down right now**, this instant.

That's the job of a reflex.

## How reflexes are different

A reflex looks almost identical to a normal instinct, but with three
differences:

1. It inherits from `BaseReflexInstinctNode` instead of `BaseInstinctNode`.
2. It must have `priority >= 200`. (Remember the priority ranges? 200+ is
   reserved for reflexes.)
3. The reflex and its target action node must live on the **same machine**
   (the same `AgentNode`). This is so the reflex doesn't have to send its
   action across the network — it just runs it immediately.

That last point only matters for distributed agents (where you split your
program across multiple devices). For now, on a single computer, you don't
have to worry about it.

## A reflex example

Let's add an emergency-stop reflex to our smart-fan agent. If the temperature
ever goes above 80 degrees, we want to shut everything down immediately.

```python
from arachnite import BaseReflexInstinctNode, BaseActionNode, Proposal, Result


class CriticalHeatReflex(BaseReflexInstinctNode):
    node_id = "CriticalHeatReflex"
    priority = 250  # Reflex range (>= 200)

    async def evaluate(self, ctx) -> Proposal | None:
        critical = [s for s in ctx.signals
                    if s.kind == "temperature" and s.value > 80.0]
        if critical:
            return Proposal(
                instinct_id=self.node_id,
                action_id="EmergencyStop",
                priority=self.priority,
                urgency=1.0,
            )
        return None


class EmergencyStop(BaseActionNode):
    node_id = "EmergencyStop"

    async def execute(self, proposal) -> Result:
        print("!!! EMERGENCY STOP !!!")
        # In a real system: cut power, sound alarm, lock doors...
        return Result(action_id=self.node_id, success=True)
```

To add it to the agent, you register the reflex with the **instinct master**
(it knows the difference and routes reflex nodes specially):

```python
instinct_master.register(CriticalHeatReflex(bus=bus))
action_master.register(EmergencyStop(bus=bus))
```

Now, every tick, the runtime checks all reflexes **before** the normal
instinct evaluation. If `CriticalHeatReflex` fires, the `EmergencyStop`
action is started immediately. Its proposal never goes to the decision layer.

The rest of the tick still happens as usual: the normal instincts are asked
and the decision layer picks among *their* proposals. A reflex doesn't switch
off the rest of the agent. It just gets its action going first, without
waiting for anyone's opinion.

## When a reflex meets a running action

Remember from lesson 5 that actions run in the background, sometimes for
many ticks. So what happens if the reflex fires while another action is
still busy with the same hardware?

Let's give our fan agent a slow action. `SpinUpFan` ramps the fan up gently
over a couple of seconds. Both it and `EmergencyStop` control the fan motor,
so both declare that resource (you met `resources` in lesson 6):

```python
import asyncio

from arachnite import BaseActionNode, Result


class SpinUpFan(BaseActionNode):
    node_id = "SpinUpFan"
    resources = frozenset({"fan_motor"})

    async def execute(self, proposal) -> Result:
        for speed in range(0, 101, 20):
            print(f"  fan speed {speed}%")
            await asyncio.sleep(0.5)   # ramp up gently
        return Result(action_id=self.node_id, success=True)

    async def on_preempted(self, proposal, preempted_by) -> None:
        # Called if a reflex takes the fan motor away mid-ramp.
        print(f"  SpinUpFan cut short by {preempted_by.instinct_id}: holding the motor")


class EmergencyStop(BaseActionNode):
    node_id = "EmergencyStop"
    resources = frozenset({"fan_motor"})

    async def execute(self, proposal) -> Result:
        print("!!! EMERGENCY STOP !!!")
        return Result(action_id=self.node_id, success=True)
```

If the temperature shoots past 80 degrees halfway through the ramp, you'll
see something like:

```
  fan speed 0%
  fan speed 20%
  fan speed 40%
  SpinUpFan cut short by CriticalHeatReflex: holding the motor
!!! EMERGENCY STOP !!!
...
```

(The reflex keeps firing on every tick that is still too hot, so you may see
"EMERGENCY STOP" more than once.)

Here's what happened, step by step:

1. The reflex fired and wanted `EmergencyStop`, which needs `fan_motor`.
2. `SpinUpFan` was holding `fan_motor`, so the runtime stopped it right
   where it was, in the middle of its `await asyncio.sleep(...)`. This is
   called **preemption**: the reflex *preempts* (cuts in front of) the
   running action.
3. The runtime called `SpinUpFan.on_preempted()`. This is the action's
   **safe-stop**: a last chance to leave the hardware in a safe, steady
   state.
4. Only then did `EmergencyStop` start.

A few things worth knowing:

- **A safe-stop holds, it doesn't undo.** Think "brake, keep the grip". A
  robot arm that is carrying a cup should freeze and keep holding the cup,
  not let go of it. Undoing work in the middle of an emergency can be
  dangerous on its own.
- **Keep `on_preempted()` short.** It's given a time limit
  (`safe_stop_timeout_s`, 1 second by default) so a stuck safe-stop can
  never hold the reflex back.
- **Actions that don't share hardware are left alone.** If the reflex's
  action needs `fan_motor` and another action is blinking an LED, the LED
  keeps blinking. Both run at the same time.
- **A reflex never cuts into another reflex's action.** It waits for that
  one to finish instead.
- **The result tells you what happened.** The preempted action's `Result`
  has `preempted=True`, so an instinct can notice it on a later tick and,
  for example, try again once things are safe.

Cutting straight in is the default, and it cuts in everywhere, even in the
middle of a part of an action that is marked "must finish" (you'll meet
these "mandatory blocks" in the advanced lesson on multi-step actions). If
cutting such a part in half would be worse than waiting a moment, the
reflex can choose to let it finish first:

```python
from arachnite import ReflexPreemption


class CriticalHeatReflex(BaseReflexInstinctNode):
    node_id = "CriticalHeatReflex"
    priority = 250
    preemption = ReflexPreemption.WAIT_FOR_BLOCK  # default: ReflexPreemption.PREEMPT
```

Exercise 4 lets you watch preemption happen in a small 3D-printer agent.

## Even faster: event-driven reflexes

By default, reflexes are checked once per tick. At 10 ticks per second, a
danger signal can sit around for up to 100 ms before the reflex even looks
at it. For most agents that's fine. When it isn't, you can switch the
runtime to **event mode**:

```python
rt = (
    RuntimeBuilder()
    # ... sensors, instincts, actions ...
    .reflex_mode("event")
    .build()
)
```

In event mode, a reflex that lists the signal kinds it cares about in
`trigger_on_signals` is checked **the moment** such a signal arrives, not at
the next tick:

```python
import time

from arachnite import (
    BaseReflexInstinctNode, BaseSenseNode, Proposal, Signal,
)


class BumpReflex(BaseReflexInstinctNode):
    node_id = "BumpReflex"
    priority = 240
    trigger_on_signals = ["bump"]   # check me as soon as a "bump" arrives

    async def evaluate(self, ctx) -> Proposal | None:
        if any(s.kind == "bump" for s in ctx.signals):
            return Proposal(instinct_id=self.node_id, action_id="BackOff",
                            priority=self.priority, urgency=1.0)
        return None
```

Reflexes without `trigger_on_signals` are still checked once per tick, as
before.

Event mode really shines with sensors that *tell you* when something
happens (a bumper switch, a button), instead of being asked every tick. Such
a sensor starts a small background task in `setup()` and calls
`await self.push(signal)` whenever the event happens:

```python
class BumperSense(BaseSenseNode):
    node_id = "BumperSense"
    signal_kind = "bump"

    async def setup(self) -> None:
        self.spawn_background_task(self._listen())

    async def _listen(self) -> None:
        while True:
            await wait_for_bumper()   # stand-in for your hardware's "wait for event"
            await self.push(Signal(source=self.node_id, kind=self.signal_kind,
                                   value=True, confidence=1.0,
                                   timestamp=time.monotonic()))

    async def read(self) -> Signal | None:
        return None   # nothing to ask every tick: bumps are pushed
```

With event mode on, the `BackOff` action starts within a millisecond or so
of the bump, even if the agent only ticks once per second. The pushed signal
also shows up in the next tick's `ctx.signals`, so your normal instincts see
it too.

## Reflexes vs. `emergency_stop()`

The runtime also has a method called `emergency_stop()`. Despite the name,
it is **not** how your agent reacts to danger. It shuts the whole agent
down, right now: every running action gets its safe-stop, then everything
is torn down. Use it like the big red button on the wall, when you want the
program to end.

Reacting to danger *while staying alive* is the reflex's job, as shown
above. And the normal `stop()` is the polite way to end a program: it lets
running work wind down cleanly first.

## Reflex vs normal instinct — when to choose which

Use a **reflex** when:

- Safety is at stake.
- You can't afford the decision layer's overhead.
- The response is always the same — no judgement needed.
- Examples: emergency stops, collision avoidance, overheat shutdown,
  watchdog timeouts.

Use a **normal instinct** when:

- The situation needs to compete with other goals.
- You want the decision layer to weigh trade-offs.
- The response might be different depending on context.
- Examples: "go vacuum the dirty spot", "look for the charging dock", "say
  hello when you see a face".

A good rule of thumb: **if you'd write `try:/except:` around it in normal
code to make sure it always runs**, it's probably a reflex.

## Don't overdo it

It's tempting to make everything a reflex because reflexes feel "fast" and
"safe". Resist that temptation. If everything is a reflex, you've thrown away
the decision layer entirely, and your agent becomes a tangled web of
high-priority shouts.

In a typical agent, you might have **1 to 3 reflexes** and **5 to 20** normal
instincts. Reflexes are the exception, not the rule.

## A common confusion

You might wonder: "If reflexes have priority 250 and normal instincts have
priority up to 199, why don't normal instincts with priority 250 work the
same way?"

The answer: **the priority number alone doesn't make something a reflex**.
What makes a reflex is the *base class* you inherit from
(`BaseReflexInstinctNode`). The priority range is a *convention* to keep your
numbers organized. The runtime checks the class type, not the priority, to
decide whether to fast-path through the decision layer.

So always inherit from `BaseReflexInstinctNode` for reflex behaviour, and
use priority >= 200 to make your intent clear to anyone reading the code.

## What's next?

You've now seen every major piece of Arachnite: senses, instincts, reflexes,
decisions, actions. In the next (and final) lesson, we'll combine everything
into a small project: a **smart lamp** that uses multiple sensors, two
instincts, a reflex, and two actions.

[← Previous: Priorities and Decisions](06_priorities_and_decisions.md) | [Next: Lesson 8 — Smart Lamp Project →](08_smart_lamp_project.md)
