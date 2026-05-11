# Advanced 4 — Custom Decision Strategies

In the beginner course, every example used `GreedyDecisionNode` — the
strategy that picks the highest-priority proposal and ignores the rest.
That's a sensible default, but it's not the only option.

Arachnite ships three built-in strategies, and you can write your own.

## The built-in strategies

All decision strategies inherit from `BaseDecisionNode`. Three are exported
out of the box:

| Class | Behaviour | When to use |
|---|---|---|
| `GreedyDecisionNode` | Picks the highest priority. Ties broken by urgency. | Default — when priorities cleanly express importance. |
| `WeightedDecisionNode` | Picks the proposal with the highest `priority * urgency`. | When some instincts have low priority but high urgency, and you want both to matter. |
| `RandomDecisionNode` | Samples randomly, weighted by urgency. | Exploration — when you want the agent to occasionally pick "lesser" options to learn. |

You swap strategies by passing one to the `DecisionMasterNode`:

```python
from arachnite import DecisionMasterNode, WeightedDecisionNode

decision_master = DecisionMasterNode(
    bus=bus,
    strategy=WeightedDecisionNode(bus=bus),
)
```

That's the only change. Every other part of your agent stays the same.

## When `Greedy` isn't enough

Imagine these three proposals fire on the same tick:

| Proposal | Priority | Urgency |
|---|---|---|
| RoutineCleanup | 90 | 0.1 |
| RespondToUser | 70 | 0.95 |
| LogMaintenance | 30 | 0.5 |

`Greedy` picks `RoutineCleanup` (priority 90) and never gets to the user.
But the user is 0.95 urgent — maybe we should respond *first* and clean up
later?

`Weighted` would compute:
- RoutineCleanup → 90 × 0.1 = **9**
- RespondToUser  → 70 × 0.95 = **66.5**  ← winner
- LogMaintenance → 30 × 0.5 = 15

Now the user gets answered. This is one reason `urgency` exists as a
separate field on Proposal — so a strategy can weigh it.

## Writing your own strategy

Sometimes you want behaviour none of the built-ins offer. For example: "pick
the highest priority, but skip any proposal whose action is currently
running so we don't double-trigger." Here's the full skeleton:

```python
from arachnite import BaseDecisionNode, Proposal, SignalBus


class SkipRunningDecision(BaseDecisionNode):
    node_id = "SkipRunningDecision"

    def __init__(self, bus: SignalBus, **kwargs) -> None:
        super().__init__(bus=bus, **kwargs)

    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        # Proposals are pre-sorted by priority descending.
        # Return the first one whose action isn't already running.
        for p in proposals:
            if p.action_id not in self._running_action_ids():
                return p
        return None  # all already running, do nothing this tick

    def _running_action_ids(self) -> set[str]:
        # The runtime publishes ActionExecutionState events you can track,
        # or you can keep your own bookkeeping. Empty set for this example.
        return set()
```

The contract is simple:

- `decide()` is async and gets a list of `Proposal` objects pre-sorted by
  priority (highest first).
- Return one of them, or `None` to do nothing this tick.

You can also override `decide_many()` to return *multiple* proposals when
you want concurrent action dispatch — useful when "play sound" and "blink
light" can both happen at once.

## A complete example

Let's build an agent with two instincts (high priority + low urgency, low
priority + high urgency) and watch how the strategy choice changes the
outcome.

```python
import asyncio
import time

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    BaseSenseNode, SenseMasterNode,
    BaseInstinctNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode, WeightedDecisionNode,
    BaseActionNode, ActionMasterNode,
)


class TickSensor(BaseSenseNode):
    node_id = "TickSensor"
    signal_kind = "tick"
    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=1, confidence=1.0, timestamp=time.monotonic(),
        )


class CleanupInstinct(BaseInstinctNode):
    node_id = "CleanupInstinct"
    priority = 90  # high priority
    async def evaluate(self, ctx) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="Cleanup",
            priority=self.priority, urgency=0.1,  # low urgency
        )


class UserRequestInstinct(BaseInstinctNode):
    node_id = "UserRequestInstinct"
    priority = 70  # lower priority
    async def evaluate(self, ctx) -> Proposal | None:
        return Proposal(
            instinct_id=self.node_id, action_id="AnswerUser",
            priority=self.priority, urgency=0.95,  # very urgent
        )


class Cleanup(BaseActionNode):
    node_id = "Cleanup"
    async def execute(self, p) -> Result:
        print("[Cleanup]")
        return Result(action_id=self.node_id, success=True)


class AnswerUser(BaseActionNode):
    node_id = "AnswerUser"
    async def execute(self, p) -> Result:
        print("[AnswerUser]")
        return Result(action_id=self.node_id, success=True)


async def run_with(strategy_factory, label: str) -> None:
    print(f"\n=== {label} ===")
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=strategy_factory(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(TickSensor(bus=bus))
    instinct_master.register(CleanupInstinct(bus=bus))
    instinct_master.register(UserRequestInstinct(bus=bus))
    action_master.register(Cleanup(bus=bus))
    action_master.register(AnswerUser(bus=bus))

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
    await asyncio.sleep(2.0)
    await rt.stop()


async def main() -> None:
    await run_with(GreedyDecisionNode, "GreedyDecisionNode")
    await run_with(WeightedDecisionNode, "WeightedDecisionNode")


if __name__ == "__main__":
    asyncio.run(main())
```

You'll see "Cleanup" win every tick under `Greedy`, but "AnswerUser" win
under `Weighted`. Same instincts, completely different agent behaviour.

## When to write a custom strategy

Most agents are fine with the built-ins. Reach for a custom strategy when:

1. You want **fairness** — round-robin between equally important instincts.
2. You want **deduplication** — skip a proposal if its action is already
   running.
3. You want **deadline-aware scheduling** — pick the proposal that expires
   soonest.
4. You want **learning** — let an ML model rank proposals.

If you find yourself adding `if/elif` ladders to your instincts to
work around the strategy, that's a sign: write a custom strategy instead.

## What's next?

Once your agent has many moving parts, you'll need to *see* what it's doing.
Next we'll look at structured logging and observability.

[← Smarter Context](03_smarter_context.md) | [Next: Logging and Observability →](05_logging_and_observability.md)
