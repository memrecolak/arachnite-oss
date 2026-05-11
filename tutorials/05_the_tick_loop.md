# Lesson 5 — The Tick Loop

So far you've heard the word "tick" a lot. In this lesson we'll open up the
runtime and see exactly what happens during one tick. We'll also finally
explain that mysterious `async` keyword you keep typing.

## What is a tick?

A **tick** is one full pass through the sense → think → act loop. It's like
the tick of a clock — at a regular rate, the runtime "advances" by one step.

If you set `tick_rate_hz=2.0`, then there are 2 ticks per second, which means
each tick takes about 500 milliseconds. If you set it to `10.0`, there are 10
ticks per second (100 milliseconds each).

The faster the tick rate, the more reactive your agent — but the more work
your computer has to do. For most things, 5 to 10 ticks per second is plenty.

## What happens during one tick?

Here's the order, in plain English:

```
TICK STARTS
│
├─ 1. Read all sensors at the same time.
│     Collect every Signal they produce.
│
├─ 2. Update the context with those signals.
│     The context is the agent's short-term memory.
│
├─ 3. Check the reflex instincts first.
│     If a reflex fires, run its action immediately.
│     (More on reflexes in lesson 7.)
│
├─ 4. Ask every normal instinct: "do you want to do something?"
│     Collect all the proposals they return.
│
├─ 5. Hand the proposals to the decision layer.
│     The decision layer picks one (or a few) winners.
│
├─ 6. Run the chosen action(s).
│     Each one returns a Result.
│
├─ 7. Feed the Results back into the context.
│     The next tick's instincts can see what just happened.
│
└─ TICK ENDS — sleep until the next tick.
```

That's it. Every tick, this exact sequence happens. Forever (or until you
call `stop()`).

## Why does this matter?

Once you understand the tick order, the framework stops feeling magical:

- Your sensor `read()` is called in step 1.
- Your instinct `evaluate()` is called in step 4.
- Your action `execute()` is called in step 6.
- Your instinct never sees a result during the *same* tick — only on the
  *next* tick, because results aren't added to context until step 7.

This last point is important. If your instinct wants to react to what just
happened, it has to wait one tick.

## Now, about that `async` keyword

You've been typing `async def read(self):` and `async def evaluate(self,
ctx):` without really knowing why. Time to explain.

In a normal Python function:

```python
def slow_thing():
    time.sleep(2)   # the program completely stops for 2 seconds
    return "done"
```

If you call this, your whole program freezes for 2 seconds. Nothing else can
happen.

In an async Python function:

```python
async def slow_thing():
    await asyncio.sleep(2)   # the function pauses, but other stuff can run
    return "done"
```

The `await` keyword says "I'm waiting for something — Python, please go do
other useful work while I wait." This is huge for our agent because:

- We might have **5 sensors** that all need to be read at the same time.
- We don't want to wait for sensor 1 to finish before starting sensor 2.
- With async, the runtime can ask all 5 sensors to read **at the same time**
  and collect their answers when they're all done.

So that's why `read`, `evaluate`, and `execute` are all async: the runtime
runs them concurrently for speed.

### The two rules of async (for now)

1. **If a function uses `await`, it must be `async def`.**
2. **If you call an async function, you usually need `await` in front.**

For example:

```python
await rt.start()        # rt.start() is async, so we await it
await asyncio.sleep(5)  # asyncio.sleep is async, so we await it
```

You don't need to deeply understand async to use Arachnite. Just remember:
async functions go with `await`. Eventually you'll meet `asyncio.gather`,
`asyncio.create_task`, and other tools, but you can ignore them for now.

## What if a tick takes too long?

Suppose you set `tick_rate_hz=10.0` (one tick every 100 ms), but your action
takes 300 ms to finish. What happens?

The runtime will log a warning ("tick overrun") and run the next tick *late*.
It doesn't skip ticks or crash. Your agent slows down, but it keeps working.

This is good news — it means you don't have to worry about your code being
"too slow" to start with. Build it, then if you see overrun warnings, look
for ways to make your nodes faster.

## A peek under the hood

If you ever want to see what the runtime is doing, every node has a logger
you can use:

```python
class HotInstinct(BaseInstinctNode):
    node_id = "HotInstinct"
    priority = 80

    async def evaluate(self, ctx) -> Proposal | None:
        self.logger.info("Evaluating", tick=ctx.tick, signals=len(ctx.signals))
        # ... rest of the code ...
```

`self.logger` is provided automatically because your class inherits from
`BaseInstinctNode`. You can use `info`, `debug`, `warning`, and `error`. The
**rule:** never use `print()` inside a node — always use the logger. This
way the framework can format and route messages properly.

(In our examples we've been using `print()` for simplicity. From now on,
prefer `self.logger.info("...")`.)

## When things go wrong — error handling

In lesson 3 we said "actions always return a `Result`, never raise an
exception." But what does that actually look like in practice? Let's walk
through the full pattern.

### Step 1: Catch errors inside the action

Imagine our `CoolDown` action talks to a fan over the network. That can fail.
Instead of letting the exception crash the agent, we catch it and pack it
into a `Result`:

```python
class CoolDown(BaseActionNode):
    node_id = "CoolDown"

    async def execute(self, proposal) -> Result:
        try:
            await self.turn_fan_on()
            return Result(action_id=self.node_id, success=True)
        except ConnectionError as e:
            self.logger.error("Fan unreachable", error=str(e))
            return Result(action_id=self.node_id, success=False, error=e)
```

The key fields on `Result`:

- `success=False` — tells everything downstream that it didn't work.
- `error=e` — carries the actual exception, so anything inspecting this
  Result can see *what* went wrong.

### Step 2: React to failures on the next tick

Remember from the tick diagram: results are fed into the context at step 7,
and instincts see them on the **next** tick. So you can write an instinct
that watches for failures and proposes a recovery action:

```python
class RetryOnFailure(BaseInstinctNode):
    node_id = "RetryOnFailure"
    priority = 90  # higher than the original, so the retry wins

    async def evaluate(self, ctx) -> Proposal | None:
        if ctx.last_result and not ctx.last_result.success:
            failed = ctx.last_result
            self.logger.warning(
                "Action failed, proposing retry",
                action=failed.action_id,
                error=str(failed.error),
            )
            return Proposal(
                instinct_id=self.node_id,
                action_id=failed.action_id,  # retry the same action
                priority=self.priority,
                urgency=0.8,
            )
        return None
```

`ctx.last_result` holds the result from the previous tick. If there were
multiple actions, `ctx.last_results` (plural) gives you a list of all of
them.

### Why not just raise exceptions?

In normal Python, you might write `try/except` around every function call.
In Arachnite, we do something different:

1. **Errors are data, not crashes.** A `Result(success=False)` flows through
   the same tick loop as a success. The agent keeps running.
2. **Any instinct can react.** One instinct might retry. Another might
   escalate. A third might switch to a fallback strategy. You choose by
   writing instincts, not by nesting `try/except` blocks.
3. **The agent never stops sensing.** Even while dealing with an error, the
   next tick still reads all sensors and evaluates all instincts. The agent
   stays aware.

Think of it this way: in the tick loop diagram, errors don't jump sideways —
they flow forward through the same path as everything else.

### The same rule applies to instincts

Instinct `evaluate()` should return `None` when nothing applies — not raise.
If your instinct hits an error (say, a flaky database lookup), log the
problem and return `None`:

```python
async def evaluate(self, ctx) -> Proposal | None:
    try:
        data = await self.lookup_something()
    except DatabaseError as e:
        self.logger.warning("Lookup failed, skipping", error=str(e))
        return None  # this tick, do nothing — try again next tick
    # ... normal logic using data ...
```

### Quick summary

| Node type | On success | On failure |
|---|---|---|
| Action | `return Result(success=True)` | `return Result(success=False, error=e)` |
| Instinct | `return Proposal(...)` | `return None` (and log) |
| Sensor | `return Signal(...)` | The framework catches the error and skips that sensor for this tick |

The pattern is always: **don't raise, return.** Errors are just information
that flows through the tick loop like everything else.

### What about errors that can't be fixed?

Imagine a USB microphone gets unplugged. Your sensor's `read()` fails every
tick, the retry instinct keeps proposing retries, and those retries keep
failing. An infinite error loop!

Arachnite has a built-in safety net for this: the **NodeSupervisor**. Every
node is supervised, and the supervisor tracks how many times a node has
faulted. Here's what happens:

1. The node fails → supervisor marks it **FAULTED** and tries to restart it.
2. The restart fails again → supervisor tries again (up to `max_restarts`
   times, default 3).
3. All retries exhausted → supervisor marks the node **DEAD**. It stops being
   called. No more error spam.

The supervisor also publishes a `SupervisorSignal` onto the bus each time a
node changes state, so your instincts can react to a node dying — for
example, by switching to a backup sensor or entering a safe mode:

```python
class FallbackOnDead(BaseInstinctNode):
    node_id = "FallbackOnDead"
    priority = 100

    async def evaluate(self, ctx) -> Proposal | None:
        for sig in ctx.signals:
            if sig.kind == "supervisor" and sig.value == "dead":
                return Proposal(
                    instinct_id=self.node_id,
                    action_id="EnterSafeMode",
                    priority=self.priority,
                    urgency=1.0,
                )
        return None
```

You don't need to set any of this up yourself — supervision is automatic.
We'll cover it properly in the advanced lesson on
[Supervisors and Health](advanced/02_supervisors_and_health.md), but for now
just know that the framework won't let a broken node flood your agent with
errors forever.

## What's next?

You now know what a tick is, what happens during one, why everything is
async, and how to handle errors without crashing the agent. In the next
lesson we'll see what happens when **multiple instincts fire at the same
time** — how the decision layer picks a winner, and how priorities affect the
result.

[← Previous: Your First Agent](04_your_first_agent.md) | [Next: Lesson 6 — When Instincts Compete →](06_priorities_and_decisions.md)
