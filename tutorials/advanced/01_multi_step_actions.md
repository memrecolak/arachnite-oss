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

- **`interruptible`** — if `True`, the action may be stopped at the boundary
  before this step. If `False`, the step belongs to a *mandatory block*. The
  action can still be stopped just *before* the block starts (a pending stop
  is honoured there rather than running the block only to undo it), but once
  the block has started the decision layer can never interrupt it, and a graceful `stop()` waits for
  it to finish. Only two things cut into a mandatory block: a reflex that
  needs the same hardware (with the default `ReflexPreemption.PREEMPT`, see
  below) and `emergency_stop()`.
- **`rollback`** — an async function that undoes this step. Under the
  `ROLLBACK` policy it is called (most recent step first) when the action is
  interrupted or aborted *after* this step has completed. Used for
  transactional operations.

## Interrupt policies

You also pick an `interrupt_policy` for the whole action, which tells the
framework how to handle interruption requests from the decision layer (a
more important proposal wants the same hardware) and from a graceful
`stop()`:

```python
from arachnite.models import InterruptPolicy

# Stop at the next safe boundary (any boundary not inside a mandatory block)
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

Interrupt policies govern *deliberation*. Reflexes follow their own rule,
**preemption**, covered further down.

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
    resources = frozenset({"app_server"})  # what this action takes control of

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
yet the runtime keeps ticking the rest of the agent the whole time: the tick
only *starts* the action, which then runs as a background task (beginner
lesson 5). Sensing, instincts and reflexes carry on while it works, and its
`Result` shows up in `ctx.last_results` on the tick after it finishes.

## Who owns the hardware: resources

`DeployAction` declares `resources = frozenset({"app_server"})`. A resource
names something the action takes control of: an actuator, a device, a
server. At most one running action owns a given resource at any time. That
is what stops two actions from driving the same motor at once.

An action conflicts with a running one if they have the same `node_id` or
share any resource. What happens next depends on who is asking:

| Who wants the resource | Running owner is... | What happens |
|---|---|---|
| A normal proposal | a normal action of lower priority | The owner gets an interrupt request and stops at its next safe boundary (the start of an interruptible step), under its `interrupt_policy` (ROLLBACK unwinds; a plain `BaseActionNode` simply runs to its end). Inside a mandatory block the request is held until the block ends, then honoured at the first safe boundary. The new proposal is **parked** and starts as soon as the owner releases: a **hand-off**. |
| A normal proposal | a reflex's action, or equal/higher priority | The proposal is held, not started. If its instinct keeps proposing it (or it has `persist=True`), it gets another chance on later ticks. |
| A reflex | anything except another reflex's action | **Preemption**, see the next section. |
| A reflex | a reflex's action | The reflex waits until that action finishes. |

Actions with no shared resource don't conflict and run side by side. An
action that declares no resources only conflicts with itself. If you want
the runtime to insist that every action names its resources, build it with
`RuntimeBuilder().strict_resources()` (or
`ActionMasterNode(strict_resources=True)`).

## When a reflex cuts in: preemption and safe-stop

A deploy can wait. A robot arm next to a person can't. When a reflex needs a
resource that a running action owns, the reflex's `preemption` setting
decides what happens:

- **`ReflexPreemption.PREEMPT`** (the default) — the owner is cancelled right
  where it is, at its current `await`, **even inside a mandatory block**.
  Then its `on_preempted()` hook runs. Only after that does the reflex's
  action start.
- **`ReflexPreemption.WAIT_FOR_BLOCK`** — if the owner is inside a
  mandatory block, the block is allowed to finish. The owner is then
  preempted at the very next step boundary, whatever its `interrupt_policy`
  says, and the reflex's action starts. Outside a block it behaves like
  `PREEMPT`.

`on_preempted(proposal, preempted_by)` is the action's **safe-stop**. Its
job is to bring the hardware to a safe hold, *not* to undo work: "brake,
keep the grip". That is also why **rollback does not run on preemption**. A
rollback that opens the gripper would drop the box right next to the
obstacle the reflex is reacting to. If undoing really is the safe thing for
your action, set `rollback_on_preempt = True` and the rollback walk runs
after the safe-stop.

Here is a pick-up action with a mandatory "lower, grip, lift" block, and an
obstacle that appears while the gripper is closing:

```python
import asyncio
import time

from arachnite import (
    RuntimeBuilder, Signal, Proposal, Result,
    ActionStep, StepResult, InterruptPolicy, ReflexPreemption,
    BaseSenseNode, BaseInstinctNode, BaseReflexInstinctNode,
    BaseActionNode, MultiStepActionNode,
)

START = time.monotonic()


class ObstacleSensor(BaseSenseNode):
    node_id = "ObstacleSensor"
    signal_kind = "obstacle"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        # Something passes by about 1 s in, while the gripper is closing
        near = 1.0 < time.monotonic() - START < 1.1
        return Signal(source=self.node_id, kind=self.signal_kind, value=near,
                      confidence=1.0, timestamp=time.monotonic())


class PickUpOnce(BaseInstinctNode):
    node_id = "PickUpOnce"
    priority = 70
    fired = False

    async def evaluate(self, ctx) -> Proposal | None:
        if self.fired:
            return None
        self.fired = True
        return Proposal(instinct_id=self.node_id, action_id="PickUpBox",
                        priority=self.priority, urgency=0.8)


class ObstacleReflex(BaseReflexInstinctNode):
    node_id = "ObstacleReflex"
    priority = 250
    preemption = ReflexPreemption.PREEMPT   # the default; try WAIT_FOR_BLOCK

    async def evaluate(self, ctx) -> Proposal | None:
        if any(s.kind == "obstacle" and s.value for s in ctx.signals):
            return Proposal(instinct_id=self.node_id, action_id="Retract",
                            priority=self.priority, urgency=1.0)
        return None


class PickUpBox(MultiStepActionNode):
    node_id = "PickUpBox"
    resources = frozenset({"arm", "gripper"})
    interrupt_policy = InterruptPolicy.ROLLBACK
    timeout_s = 2.0

    async def _open_gripper(self) -> None:
        print("  [rollback] open gripper")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("reach", interruptible=True),
            ActionStep("lower", interruptible=False),
            ActionStep("grip", interruptible=False, rollback=self._open_gripper),
            ActionStep("lift", interruptible=False),
            ActionStep("carry", interruptible=True),
        ]

    async def execute_step(self, step, proposal, completed) -> StepResult:
        print(f"  [{step.name}]")
        await asyncio.sleep(0.4)
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal, preempted_by) -> None:
        print(f"  [safe-stop] arm braked, grip kept (cut in by {preempted_by.instinct_id})")


class Retract(BaseActionNode):
    node_id = "Retract"
    resources = frozenset({"arm"})   # the arm only: the gripper is left alone

    async def execute(self, proposal) -> Result:
        print("  [Retract] moving the arm away")
        return Result(action_id=self.node_id, success=True)


def report(ctx) -> None:
    for r in ctx.last_results:
        if r.action_id == "PickUpBox":
            print(f"  result: preempted={r.preempted} rolled_back={r.rolled_back} "
                  f"stopped_at={r.stopped_at_step}")


async def main() -> None:
    rt = (
        RuntimeBuilder()
        .sense(ObstacleSensor)
        .instinct(PickUpOnce)
        .instinct(ObstacleReflex)
        .action(PickUpBox)
        .action(Retract)
        .tick_rate(10.0)
        .build()
    )
    rt.add_context_observer(report)
    await rt.start()
    await asyncio.sleep(2.5)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

Output:

```
  [reach]
  [lower]
  [grip]
  [safe-stop] arm braked, grip kept (cut in by ObstacleReflex)
  [Retract] moving the arm away
  result: preempted=True rolled_back=False stopped_at=grip
```

The reflex cut into the middle of the mandatory block, the safe-stop ran,
the rollback did not, and `Retract` started right after. Change the reflex to
`ReflexPreemption.WAIT_FOR_BLOCK` and run it again: now `lift` completes
first, and the action is preempted at the boundary before `carry`
(`stopped_at=carry`).

A preempted action's `Result` has `preempted=True` and `interrupted=True`.
It is never retried automatically, even with `max_retries > 0`. If the job
should still be finished once the danger has passed, write an instinct that
notices the preempted result (or the state the hardware was left in) and
proposes a recovery action.

### Write cancellation-safe steps

Under `PREEMPT`, the step you are in receives `asyncio.CancelledError` at
whatever `await` it is waiting on. Don't swallow it, and don't leave the
hardware half-commanded in a way `on_preempted()` can't fix. Keep each
command short, and let `on_preempted()` bring things to a safe hold. Note
that a blocking call wrapped in `asyncio.to_thread()` cannot be cancelled:
the thread keeps running, so the safe-stop must cope with a command that may
still be in flight.

## When a step fails: `on_aborted()`

A step can end the whole sequence in three ways: it returns
`StepResult(..., abort_sequence=True)`, it exceeds its timeout, or it raises
an exception. All three go through the same hook,
`on_aborted(completed, failed_step, proposal)`. By default it runs the
rollback walk under the `ROLLBACK` policy and does nothing under the other
policies. The action's `Result` then has `success=False` and an `error` that
is a `StepAbortError` (with the step's own exception as its `__cause__`).

The rollback walk always tries **every** rollback, even if one of them
fails. If any fail, the `Result.error` is a `RollbackError` whose `failures`
list names each failed step, and `rolled_back` is `False`.

A step that returns `success=False` *without* `abort_sequence=True` does not
abort anything: the failure is logged and the next step runs.

The action's own `timeout_s` bounds the whole run, not one step. When it
runs out, the run is cut at its current `await`, even inside a mandatory
block, and wound down like this: `on_preempted()` runs first (its
`preempted_by` is a stand-in proposal with `instinct_id="timeout"`), then
`on_aborted()` for the step that was cut, then `on_timeout()` supplies the
`Result`. That `Result` has `cancelled=True` and keeps `step_results` and
`stopped_at_step`. With `max_retries > 0`, a run the timeout cut inside a
mandatory block is not retried.

## Stopping the agent: `stop()` vs `emergency_stop()`

- **`await rt.stop()`** is the graceful path. A running mandatory block is
  allowed to finish, then every running action is interrupted at its next
  safe boundary, so `ROLLBACK` actions unwind properly. Whatever is still running
  after that is cancelled with its safe-stop.
- **`await rt.emergency_stop()`** stops *now*: every running action is
  cancelled, mandatory block or not, and its `on_preempted()` safe-stop runs.
  There is no rollback — `rollback_on_preempt` applies to reflex preemption
  only. It's a shutdown, not a way to react to a hazard. To react to a hazard
  and keep the agent alive, use a reflex.

## Tips and gotchas

1. **`execute_step()` must always return a `StepResult`.** Catch exceptions
   inside and return `StepResult(success=False, error=...)` instead (add
   `abort_sequence=True` if the sequence must stop there). If a step does
   raise, the framework treats it as an abort: the sequence stops and goes
   through `on_aborted()` (see above).

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
   called. It runs on interrupt and on abort, but not on reflex preemption
   unless you set `rollback_on_preempt = True`.

4. **Don't make every step a mandatory block.** It defeats the purpose. Mark
   only the steps that *truly* can't be safely stopped halfway.

5. **Don't put a multi-step action where a normal action would do.** If your
   action runs in 100 ms with no rollback needs, just use `BaseActionNode`.
   (`on_preempted()` and `resources` work on a plain `BaseActionNode` too.)

6. **Declare resources on every action that touches hardware.** Undeclared
   access is invisible to the framework, so nothing stops two actions from
   commanding the same actuator.

## What's next?

Multi-step actions can fail in interesting new ways. Next we'll learn how to
*automatically restart* nodes that crash, using the supervisor.

[← Back to advanced index](README.md) | [Next: Supervisors and Health →](02_supervisors_and_health.md)
