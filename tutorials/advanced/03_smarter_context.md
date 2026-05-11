# Advanced 3 — Smarter Context

Up to now, every instinct has only looked at *the current tick's signals*.
That's enough for simple rules ("if temperature > 40, fan on"), but real
agents need more:

- Detecting **trends** ("temperature has been rising for 5 ticks").
- Remembering **persistent state** ("we already alerted about this today").
- Reacting to what happened on **previous actions**.

The `ContextNode` gives you all three — you've just been ignoring most of
its features. Time to use them.

## What's actually in the context object

When your instinct's `evaluate()` is called, it receives a `Context` (note:
the data class is `Context`; `ContextNode` is the runtime object that
produces it). The Context has these fields:

```python
ctx.tick: int                    # current tick number (0, 1, 2, ...)
ctx.signals: list[Signal]        # this tick's signals (you've used this)
ctx.history: deque[list[Signal]] # rolling window of past ticks' signals
ctx.state: dict[str, Any]        # persistent key/value store
ctx.last_result: Result | None   # the most recent action's result
ctx.last_results: list[Result]   # all results from the last tick (concurrent dispatch)
ctx.action_state: ActionExecutionState | None  # currently-running action info
ctx.action_states: list[ActionExecutionState]  # all running actions
```

## Detecting trends with `ctx.history`

`ctx.history` is a deque (a fixed-size queue) of past signal lists. Each
entry is the `signals` list from one previous tick. The default size is
small; you set it when you create the `ContextNode`:

```python
context = ContextNode(history_length=20)  # remember the last 20 ticks
```

Now an instinct can look at trends:

```python
class RisingTempInstinct(BaseInstinctNode):
    node_id = "RisingTempInstinct"
    priority = 75

    async def evaluate(self, ctx) -> Proposal | None:
        # Get the last 5 ticks of temperature readings
        recent: list[float] = []
        for past_signals in list(ctx.history)[-5:]:
            for s in past_signals:
                if s.kind == "temperature":
                    recent.append(s.value)

        # Trend: 5 readings, all monotonically increasing
        if len(recent) >= 5 and all(
            recent[i] < recent[i + 1] for i in range(len(recent) - 1)
        ):
            return Proposal(
                instinct_id=self.node_id,
                action_id="PreemptiveCool",
                priority=self.priority,
                urgency=0.7,
                rationale=f"rising trend: {recent}",
            )
        return None
```

This is genuinely powerful. With ten lines, you've gone from "react to a
threshold" to "predict where the system is heading and intervene early."

## Persistent state with `ctx.state`

Sometimes you need to remember something **across many ticks** — not
"the last 20 readings" but "we already sent the daily alert". That's what
`ctx.state` is for. It's a regular dict, accessible from any instinct.

```python
class DailyAlertInstinct(BaseInstinctNode):
    node_id = "DailyAlertInstinct"
    priority = 90

    async def evaluate(self, ctx) -> Proposal | None:
        already_alerted = ctx.state.get("alerted_today", False)
        if already_alerted:
            return None

        hot = [s for s in ctx.signals
               if s.kind == "temperature" and s.value > 40]
        if hot:
            # Mark as alerted via a state-update signal (see below)
            await self.bus.publish(StateUpdateSignal(
                source=self.node_id,
                kind="state_update",
                value=None,
                confidence=1.0,
                timestamp=time.monotonic(),
                key="alerted_today",
                state_value=True,
            ))
            return Proposal(
                instinct_id=self.node_id,
                action_id="SendAlert",
                priority=self.priority,
                urgency=0.8,
            )
        return None
```

### Why publish a `StateUpdateSignal` instead of just writing `ctx.state`?

Because the `ctx` your instinct sees is a **snapshot**. If you mutate it,
you're only mutating the snapshot — the next tick will hand you a fresh copy
and your change will be lost. To make a change stick, you have to publish a
`StateUpdateSignal` and let the `ContextNode` apply it during its update
phase.

This is the most surprising thing about state in Arachnite. Get it wrong
and your "memory" silently disappears. Get it right and your agent has
durable state.

## Persisting state across restarts

You can ask the `ContextNode` to write state to disk:

```python
context = ContextNode(
    history_length=20,
    state_path="/var/lib/myagent/state.json",
    flush_on_write=True,
)
```

Now `ctx.state` survives a crash or a reboot. Combine this with the
supervisor and you have an agent that can remember what it was doing even
after recovering from a hardware fault.

## Looking at the last action's result

Sometimes the next tick's instinct should react to whether the *previous*
action worked:

```python
class RetryOnFailureInstinct(BaseInstinctNode):
    node_id = "RetryOnFailureInstinct"
    priority = 85

    async def evaluate(self, ctx) -> Proposal | None:
        last = ctx.last_result
        if last and last.action_id == "SendAlert" and not last.success:
            return Proposal(
                instinct_id=self.node_id,
                action_id="SendAlert",
                priority=self.priority,
                urgency=0.95,
                rationale="retry: previous send failed",
            )
        return None
```

Notice that `last_result` is from the **previous** tick — never the current
one. That's because the action runs *after* the instinct, in the same tick,
so by the time your instinct runs, the result for this tick doesn't exist
yet.

## A complete example

A simple agent that:
- Tracks temperature readings.
- Fires once when temperature is sustained above 40°C for 3 ticks.
- Doesn't refire until it cools back below 30°C.

```python
import asyncio
import random
import time

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, StateUpdateSignal, Proposal, Result,
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
            source=self.node_id, kind=self.signal_kind,
            value=random.uniform(20, 50),
            confidence=1.0, timestamp=time.monotonic(),
        )


class SustainedHeatInstinct(BaseInstinctNode):
    node_id = "SustainedHeatInstinct"
    priority = 80

    async def evaluate(self, ctx) -> Proposal | None:
        # Get the last 3 temperature readings (current + 2 history)
        recent: list[float] = []
        for s in ctx.signals:
            if s.kind == "temperature":
                recent.append(s.value)
        for past in list(ctx.history)[-2:]:
            for s in past:
                if s.kind == "temperature":
                    recent.append(s.value)

        already_fired = ctx.state.get("hot_alert", False)

        # Reset latch when we cool down
        if already_fired and recent and recent[-1] < 30:
            await self.bus.publish(StateUpdateSignal(
                source=self.node_id, kind="state_update",
                value=None, confidence=1.0, timestamp=time.monotonic(),
                key="hot_alert", state_value=False,
            ))
            return None

        # Fire once when 3 consecutive readings are above 40
        if (
            not already_fired
            and len(recent) >= 3
            and all(v > 40 for v in recent[-3:])
        ):
            await self.bus.publish(StateUpdateSignal(
                source=self.node_id, kind="state_update",
                value=None, confidence=1.0, timestamp=time.monotonic(),
                key="hot_alert", state_value=True,
            ))
            return Proposal(
                instinct_id=self.node_id,
                action_id="HotAlert",
                priority=self.priority,
                urgency=0.9,
                rationale=f"3 ticks above 40: {recent[-3:]}",
            )
        return None


class HotAlert(BaseActionNode):
    node_id = "HotAlert"
    async def execute(self, proposal) -> Result:
        print(f"!!! HOT ALERT — {proposal.rationale}")
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
    instinct_master.register(SustainedHeatInstinct(bus=bus))
    action_master.register(HotAlert(bus=bus))

    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(history_length=10),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=2.0,
    )
    await rt.start()
    await asyncio.sleep(15.0)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

Watch the output: the alert only fires after a sustained hot streak, and it
doesn't fire again until the temperature drops back below 30. That's the
combination of `ctx.history` (for the streak) and `ctx.state` (for the
latch).

## What's next?

Now that your instincts can think across time, let's look at how to make the
**decision layer** smarter than just "highest priority wins".

[← Supervisors and Health](02_supervisors_and_health.md) | [Next: Custom Decision Strategies →](04_custom_decision_strategies.md)
