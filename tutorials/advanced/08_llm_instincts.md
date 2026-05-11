# Advanced 8 — LLM Instincts

Up to this point, every instinct you've written has been a hand-coded rule:
"if temperature > 40, propose CoolDown". That's perfect for clear-cut
situations, but what about messier ones?

- "Decide whether this customer-service ticket needs escalation."
- "Look at the camera frame and decide if the visitor seems hostile."
- "Read the recent server logs and pick a debugging action."

Writing those as `if/elif` ladders would be a nightmare. They want
**judgement**. Arachnite has a special instinct base class for that:
`LLMInstinctNode`. It plugs a language model into the decision layer.

## How LLMInstinctNode works

`LLMInstinctNode` is a regular instinct (it inherits from
`BaseInstinctNode`), but instead of writing your own `evaluate()`, you tell
it:

1. **What actions are available** — a dict of `{action_id: description}`.
2. **What the agent's goal is** — a system prompt (optional but
   recommended).
3. **How to format the context** — by default it serializes signals to
   plain text (you can override).

Each tick, the instinct hands the current context to the LLM and asks:
*"Given these signals, should I propose any of these actions?"* The LLM
either picks one (with a rationale) or says "no action". The framework
turns the answer into a `Proposal` and the rest of the agent treats it like
any other.

The LLM call runs **in a background thread**, so the tick loop is never
blocked. While the call is in flight, the instinct returns its most recent
cached proposal. This means LLM instincts feel fast even when the underlying
model takes seconds to respond.

## Providers

You don't talk to the LLM directly. You inject a **provider** — a small
adapter that knows how to call a specific API:

| Provider | Use for |
|---|---|
| `AnthropicProvider` | Claude (Anthropic API) |
| `OllamaProvider` | Local Ollama models |
| `LocalProvider` | Local models loaded directly into memory |
| `ThreadSafeProvider` | Wraps another provider for concurrent use |

You build a provider once, then pass it to your instinct's constructor.

## A complete example

Here's an "is this a real alert?" instinct that uses Claude to filter
sensor noise:

```python
import asyncio
import time
import os

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    LLMInstinctNode, AnthropicProvider,
    BaseSenseNode, SenseMasterNode,
    InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    BaseActionNode, ActionMasterNode,
)


# 1. A sensor that returns a fake server health snapshot
class ServerHealthSensor(BaseSenseNode):
    node_id = "ServerHealthSensor"
    signal_kind = "server_health"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value={
                "cpu_pct": 78,
                "mem_pct": 91,
                "errors_per_min": 12,
                "p99_latency_ms": 480,
            },
            confidence=1.0,
            timestamp=time.monotonic(),
        )


# 2. The LLM-backed instinct
class TriageInstinct(LLMInstinctNode):
    node_id = "TriageInstinct"
    priority = 90
    min_interval_s = 5.0  # don't call the LLM more than once every 5 seconds

    def available_actions(self) -> dict[str, str]:
        return {
            "PageOnCall": "Page the on-call engineer for an immediate response",
            "RestartService": "Restart the affected service to clear bad state",
            "ScaleUp": "Add more capacity to handle increased load",
        }

    def system_prompt(self) -> str:
        return (
            "You are an SRE triage assistant. Each tick you receive server "
            "health metrics. Decide whether the situation warrants paging, "
            "a restart, scaling up, or no action. Be conservative — pick "
            "no_action unless something is clearly wrong."
        )


# 3. The actions
class PageOnCall(BaseActionNode):
    node_id = "PageOnCall"
    async def execute(self, p: Proposal) -> Result:
        print(f"  PAGING ON-CALL — {p.rationale}")
        return Result(action_id=self.node_id, success=True)


class RestartService(BaseActionNode):
    node_id = "RestartService"
    async def execute(self, p: Proposal) -> Result:
        print(f"  RESTARTING SERVICE — {p.rationale}")
        return Result(action_id=self.node_id, success=True)


class ScaleUp(BaseActionNode):
    node_id = "ScaleUp"
    async def execute(self, p: Proposal) -> Result:
        print(f"  SCALING UP — {p.rationale}")
        return Result(action_id=self.node_id, success=True)


async def main() -> None:
    bus = SignalBus()
    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    sense_master.register(ServerHealthSensor(bus=bus))

    # Build the LLM provider — needs ANTHROPIC_API_KEY in your environment
    provider = AnthropicProvider(
        model="claude-haiku-4-5-20251001",
        api_key=os.environ["ANTHROPIC_API_KEY"],
    )
    instinct_master.register(TriageInstinct(bus=bus, provider=provider))

    action_master.register(PageOnCall(bus=bus))
    action_master.register(RestartService(bus=bus))
    action_master.register(ScaleUp(bus=bus))

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
    await asyncio.sleep(15.0)
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

You'll see the LLM choose one of the three actions (or no action) every few
ticks, and print a rationale you can read. **The behaviour emerges from a
prompt, not from `if/elif`.** You can change the agent's judgement by
editing the system prompt — no code change needed.

## Local models with Ollama

Don't want to send data to a cloud API? Run a local model with
[Ollama](https://ollama.ai) and use `OllamaProvider`:

```python
from arachnite import OllamaProvider

provider = OllamaProvider(model="llama3.1")
instinct = TriageInstinct(bus=bus, provider=provider)
```

That's the only change. Same instinct, same actions, model running on your
laptop instead of in the cloud.

## When to use LLM instincts (and when not to)

**Good fits:**
- Triage / classification problems with fuzzy criteria.
- Natural language inputs (chat messages, log lines, error descriptions).
- Situations with too many edge cases for hand-coded rules.
- When you want the agent's reasoning to be explainable in plain English.

**Bad fits:**
- Hard real-time loops (LLM latency is measured in hundreds of ms to
  several seconds).
- Anything safety-critical (use a deterministic instinct or a reflex).
- Hot paths where you can't tolerate the cost of API calls.
- Cases where a simple `if` statement would do the job.

A practical pattern is to combine both:

1. Hand-coded reflexes and instincts for **safety** and **clear cases**.
2. An LLM instinct with **lower priority** for **judgement calls** the
   rules don't cover.

The LLM only "wins" when no higher-priority rule fires. Best of both worlds.

## Tips

1. **Set `min_interval_s`.** LLM calls cost money and time. Don't call them
   every tick — once every few seconds is usually plenty. The default is
   1.0 seconds.
2. **Use a small/fast model first.** Claude Haiku, Llama 3.1 8B — these
   respond in under a second and cost almost nothing. Save the big models
   for when you really need them.
3. **Override `context_to_text()`** if your signals don't read well as raw
   data. The LLM's behaviour will be much better if the prompt is clear.
4. **Log every LLM proposal** with its rationale. When the agent does
   something weird, the rationale tells you whether the LLM is at fault.
5. **Never trust an LLM with a destructive action.** Wrap destructive
   actions with a confirmation step or a dry-run mode. Treat the LLM as
   an *advisor*, not a root user.

## What's next?

You've now built a complete picture of what Arachnite can do. The final
advanced lesson is the most important one for keeping all of this working
as your agent grows: **testing**.

[← Going Distributed](07_going_distributed.md) | [Next: Testing Your Agents →](09_testing_your_agents.md)
