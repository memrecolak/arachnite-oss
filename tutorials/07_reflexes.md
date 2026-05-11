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
action runs immediately, and the rest of the tick's instinct/decision process
is skipped (or runs after the reflex has been handled, depending on
configuration).

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
