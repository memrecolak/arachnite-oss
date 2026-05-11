# Advanced 1 — Multi-Step Actions

In the beginner course, every action ran instantly. You called `execute()`,
it did one thing, and returned. Real-world actions are rarely that simple.

Suppose your action is "deploy a new version of an app". That involves:

1. Download the package.
2. Verify the checksum.
3. Stop the old version.
4. Start the new version.
5. Run a health check.

Each step takes time. Some can be safely interrupted ("we haven't started the
deploy yet, just abort"). Others **must** finish or you leave the system
broken (interrupting halfway through "stop old + start new" is bad). And if
something goes wrong on step 4, you might want to roll back step 3.

That's what `MultiStepActionNode` is for.

## The class

`MultiStepActionNode` is a subclass of `BaseActionNode` that already
implements `execute()` for you. Instead, **you** implement two new methods:

```python
def steps(self) -> list[ActionStep]:
    """Define the ordered sequence of steps."""
    ...

async def execute_step(
    self,
    step: ActionStep,
    proposal: Proposal,
    completed: list[StepResult],
) -> StepResult:
    """Run one step. Always return a StepResult — never raise."""
    ...
```

`steps()` is called once at the start. It returns a list of `ActionStep`
descriptors that describe the work. `execute_step()` is then called once per
step in order, with the results of previous steps available so later steps
can branch on what happened earlier.

## ActionStep

An `ActionStep` describes one unit of work:

```python
@dataclass
class ActionStep:
    name: str                                          # step identifier
    interruptible: bool = True                         # can it be paused?
    rollback: Callable[[], Awaitable[None]] | None = None  # async undo function
    timeout_s: float | None = None                     # max time for this step
    checkpoint: bool = False                           # safe pause point?
    metadata: dict[str, Any] = field(default_factory=dict)
```

The two fields you'll care about most:

- **`interruptible`** — if `True`, the step can be paused between
  iterations. If `False`, it's a *mandatory block* — the framework guarantees
  it will run to completion.
- **`rollback`** — an async function to call if the action gets interrupted
  *after* this step has completed. Used for transactional operations.

## Interrupt policies

You also pick an `interrupt_policy` for the whole action, which tells the
framework how to handle interruption requests:

```python
from arachnite.models import InterruptPolicy

# Stop at the next interruptible step boundary
interrupt_policy = InterruptPolicy.ALWAYS

# Run to completion no matter what (atomic action)
interrupt_policy = InterruptPolicy.NEVER

# Only stop at steps marked checkpoint=True
interrupt_policy = InterruptPolicy.CHECKPOINT

# Stop at any boundary, then call rollback() of completed non-interruptible steps
interrupt_policy = InterruptPolicy.ROLLBACK
```

`ROLLBACK` is the most useful for serious work — it lets you "undo" a
partially-completed transaction.

## A complete example: a fake software deploy

```python
import asyncio
import time

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    ActionStep, StepResult, InterruptPolicy,
    BaseSenseNode, SenseMasterNode,
    BaseInstinctNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    MultiStepActionNode, ActionMasterNode,
)


class DeployAction(MultiStepActionNode):
    node_id = "DeployAction"
    interrupt_policy = InterruptPolicy.ROLLBACK
    timeout_s = 30.0

    def __init__(self, bus, **kwargs):
        super().__init__(bus=bus, **kwargs)
        self.rolled_back_steps: list[str] = []

    async def _undo_stop_old(self) -> None:
        self.rolled_back_steps.append("stop_old")
        print("  [rollback] starting old version again")

    async def _undo_start_new(self) -> None:
        self.rolled_back_steps.append("start_new")
        print("  [rollback] stopping new version")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("download", interruptible=True, timeout_s=5.0),
            ActionStep("verify", interruptible=True),
            ActionStep("stop_old", interruptible=False, rollback=self._undo_stop_old),
            ActionStep("start_new", interruptible=False, rollback=self._undo_start_new),
            ActionStep("health_check", interruptible=True),
        ]

    async def execute_step(
        self,
        step: ActionStep,
        proposal: Proposal,
        completed: list[StepResult],
    ) -> StepResult:
        match step.name:
            case "download":
                print("  [download] fetching package...")
                await asyncio.sleep(0.3)
                return StepResult(step_name=step.name, success=True)

            case "verify":
                print("  [verify]   checking checksum...")
                await asyncio.sleep(0.2)
                return StepResult(step_name=step.name, success=True)

            case "stop_old":
                print("  [stop_old] shutting down v1.0")
                await asyncio.sleep(0.5)
                return StepResult(step_name=step.name, success=True)

            case "start_new":
                print("  [start_new] launching v1.1")
                await asyncio.sleep(0.5)
                return StepResult(step_name=step.name, success=True)

            case "health_check":
                print("  [health]   probing v1.1...")
                await asyncio.sleep(0.2)
                return StepResult(step_name=step.name, success=True)

        return StepResult(step_name=step.name, success=False)


# A trivial sensor + instinct so the action actually runs once
class TickSensor(BaseSenseNode):
    node_id = "TickSensor"
    signal_kind = "tick"
    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=1, confidence=1.0, timestamp=time.monotonic(),
        )


class DeployOnce(BaseInstinctNode):
    node_id = "DeployOnce"
    priority = 70
    fired = False

    async def evaluate(self, ctx) -> Proposal | None:
        if not self.fired:
            self.fired = True
            return Proposal(
                instinct_id=self.node_id,
                action_id="DeployAction",
                priority=self.priority,
                urgency=0.9,
            )
        return None


async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(TickSensor(bus=bus))
    instinct_master.register(DeployOnce(bus=bus))
    action_master.register(DeployAction(bus=bus))

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
    await asyncio.sleep(4.0)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

When you run it, you'll see each step print its message in order. The whole
deploy takes about 1.7 seconds. Notice how the action *spans many ticks*,
yet the runtime keeps ticking the rest of the agent the whole time.

## Tips and gotchas

1. **`execute_step()` must always return a `StepResult`.** Catch exceptions
   inside and return `StepResult(success=False, error=...)` instead. Raising
   from inside a mandatory block triggers a `MandatoryBlockViolation`.

2. **Use `completed` to branch later steps.** For example, "if step 2 found
   no changes, skip step 3":
   ```python
   case "deploy":
       if not completed[1].output["has_changes"]:
           return StepResult(step_name=step.name, success=True, output={"skipped": True})
       ...
   ```

3. **Rollback only runs for non-interruptible steps with the ROLLBACK
   policy.** If you mark a step `interruptible=True`, its rollback is never
   called.

4. **Don't make every step a mandatory block.** It defeats the purpose. Mark
   only the steps that *truly* can't be safely stopped halfway.

5. **Don't put a multi-step action where a normal action would do.** If your
   action runs in 100 ms with no rollback needs, just use `BaseActionNode`.

## What's next?

Multi-step actions can fail in interesting new ways. Next we'll learn how to
*automatically restart* nodes that crash, using the supervisor.

[← Back to advanced index](README.md) | [Next: Supervisors and Health →](02_supervisors_and_health.md)
