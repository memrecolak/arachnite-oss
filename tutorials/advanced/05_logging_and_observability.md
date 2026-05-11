# Advanced 5 — Logging and Observability

When your agent does the wrong thing, the first question is: **why?** And
the only way to answer that is to have logged enough information that you
can reconstruct the agent's reasoning after the fact.

Arachnite has a structured logging system built in. Use it. Don't `print`.

## What "structured" means

A normal log line is a string:

```
[INFO] Action started for proposal CoolDown at temp 42.5
```

A structured log line is a record with named fields:

```json
{
  "level": "INFO",
  "node_id": "CoolDown",
  "tick": 412,
  "message": "Action started",
  "data": {
    "proposal": "CoolDown",
    "temperature": 42.5
  }
}
```

The difference matters because you can **filter, group, and chart**
structured logs without writing parsers. "Show me every tick where the
temperature was above 50" is one query when your logs are structured. It's a
miserable regex hunt when they're strings.

## How to log

Every `BaseNode` subclass gets `self.logger` automatically. Use it like
this:

```python
class CoolDown(BaseActionNode):
    node_id = "CoolDown"

    async def execute(self, proposal) -> Result:
        self.logger.info(
            "Cooling down",
            target_temp=proposal.parameters.get("target_temp"),
            urgency=proposal.urgency,
        )
        try:
            await turn_on_fan()
            self.logger.info("Fan running", duration_ms=200)
            return Result(action_id=self.node_id, success=True)
        except Exception as exc:
            self.logger.error(
                "Fan failed",
                error_type=type(exc).__name__,
                error_msg=str(exc),
            )
            return Result(action_id=self.node_id, success=False, error=exc)
```

Notice the **keyword arguments after the message**. Those become structured
fields. The first positional arg is always the message itself.

The five levels:

| Level | When to use |
|---|---|
| `debug` | Verbose tracing — only useful when you're hunting a bug |
| `info`  | Normal operations — actions started, decisions made |
| `warning` | Something's odd but not broken — sensor drift, retries |
| `error` | A specific operation failed but the agent continues |
| `critical` | The whole agent is going down |

## Where the logs go

Logs are sent to **sinks**. A sink is an object that knows how to format and
write a log event. Arachnite ships several:

- **`StdoutLogSink`** — coloured human-readable output to your terminal.
  Default for development.
- **`JSONLogSink`** — newline-delimited JSON to a file or stream. Default
  for production.
- **`FileLogSink`** — file-based output (used by the web dashboard).

You can attach as many sinks as you want, and each can have its own minimum
level:

```python
from arachnite import (
    StructuredLogger, StdoutLogSink, JSONLogSink, LogLevel,
)

logger = StructuredLogger(
    node_id="CoolDown",
    sinks=[
        StdoutLogSink(level=LogLevel.INFO),                  # info+ to terminal
        JSONLogSink(stream=open("agent.log", "a"),
                    level=LogLevel.DEBUG),                    # everything to file
    ],
)
```

In a real agent you'd configure sinks once at startup and inject them into
your nodes (or use the framework config to do it automatically).

## What to log — and what not to

**Log:**
- Every action start and result.
- Every instinct proposal (at debug level).
- Every supervisor restart.
- Every failed sensor read.
- Every state transition.
- Anything you'd want in a postmortem.

**Don't log:**
- Every tick (will drown your storage).
- Every signal (use ctx inspection, not logs).
- Secrets (API keys, passwords, tokens — even at debug level).
- Personally identifiable data unless you have a good reason.

A good rule of thumb: **if you'd be embarrassed for someone to read your
logs in production, fix the logging**.

## Reading logs while debugging

When something looks wrong:

1. Find the tick number where it happened (your logs include `tick=`).
2. Filter to that tick.
3. Read the events in order: sensors → instincts → decisions → actions.
4. The wrong thing is somewhere in there.

This is much faster than printing variables and re-running. Once you trust
your logs, you stop using `print()` for debugging at all.

## A worked example

```python
import asyncio
import time

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    StructuredLogger, StdoutLogSink, LogLevel,
    BaseSenseNode, SenseMasterNode,
    BaseInstinctNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    BaseActionNode, ActionMasterNode,
)


class TempSense(BaseSenseNode):
    node_id = "TempSense"
    signal_kind = "temperature"

    async def read(self) -> Signal:
        value = 42.0
        self.logger.debug("Reading temperature", value=value)
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=value, confidence=1.0, timestamp=time.monotonic(),
        )


class HotInstinct(BaseInstinctNode):
    node_id = "HotInstinct"
    priority = 80

    async def evaluate(self, ctx) -> Proposal | None:
        hot = [s for s in ctx.signals
               if s.kind == "temperature" and s.value > 40]
        if hot:
            self.logger.info(
                "Firing hot instinct",
                tick=ctx.tick,
                reading=hot[-1].value,
            )
            return Proposal(
                instinct_id=self.node_id, action_id="CoolDown",
                priority=self.priority, urgency=0.9,
            )
        return None


class CoolDown(BaseActionNode):
    node_id = "CoolDown"

    async def execute(self, proposal) -> Result:
        self.logger.info(
            "Cooling",
            instinct_id=proposal.instinct_id,
            urgency=proposal.urgency,
        )
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
    instinct_master.register(HotInstinct(bus=bus))
    action_master.register(CoolDown(bus=bus))

    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=1.0,
    )
    await rt.start()
    await asyncio.sleep(3.0)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

Run it and watch the structured fields appear in the output. Compare to a
program that just `print`s — you can immediately see why structured beats
strings.

## What's next?

Logs help you understand a running agent. **Configuration** helps you change
its behaviour without editing code. That's next.

[← Custom Decision Strategies](04_custom_decision_strategies.md) | [Next: Configuration Injection →](06_configuration_injection.md)
