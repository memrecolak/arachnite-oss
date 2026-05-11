# Advanced Topics

You finished the [beginner course](../README.md). You can build a working
agent with sensors, instincts, reflexes, and actions. Now it's time to learn
the parts of Arachnite that turn a hobby program into something serious:
multi-step actions with rollback, supervisors, distributed deployment, LLM
integration, and proper testing.

## Who these lessons are for

- You've completed lessons 1–8 of the beginner course (or you already know
  the basics of how senses, instincts, and actions fit together).
- You're comfortable reading async Python code.
- You want to **build a real agent**, not just learn the framework.

These lessons are slightly denser than the beginner ones — they assume you
won't get lost when an example introduces a new class without ten lines of
warm-up. Each one still includes runnable code, but you're expected to read
the source and experiment.

## The lessons

| # | Lesson | What it covers |
|---|---|---|
| 1 | [Multi-Step Actions](01_multi_step_actions.md) | Long-running actions with steps, interruptions, and rollback |
| 2 | [Supervisors and Health](02_supervisors_and_health.md) | Auto-restart for crashed nodes, health monitoring |
| 3 | [Smarter Context](03_smarter_context.md) | Using `ctx.history`, `ctx.state`, and `StateUpdateSignal` for memory and trends |
| 4 | [Custom Decision Strategies](04_custom_decision_strategies.md) | Built-in strategies + writing your own |
| 5 | [Logging and Observability](05_logging_and_observability.md) | Structured logs, sinks, and how to debug a running agent |
| 6 | [Configuration Injection](06_configuration_injection.md) | `NodeConfig`, env vars, and keeping secrets out of code |
| 7 | [Going Distributed](07_going_distributed.md) | `AgentNode`, transports, deployment manifests, the mesh |
| 8 | [LLM Instincts](08_llm_instincts.md) | Plugging an LLM into the decision layer with `LLMInstinctNode` |
| 9 | [Testing Your Agents](09_testing_your_agents.md) | Patterns, fixtures, and factories for fast async tests |
| 10 | [Sensor Fusion](10_sensor_fusion.md) | Combining redundant sensors with Bayesian and Ensemble merge policies |
| 11 | [Active Inference](11_active_inference.md) | Decision-making that balances goal achievement and uncertainty reduction |
| 12 | [Safety Monitors](12_safety_monitors.md) | Runtime verification of safety invariants with automatic alerting |
| 13 | [Benchmarking](13_benchmarking.md) | Measuring tick latency, reflex response time, and scalability sweeps |

You don't have to read them in order — each lesson stands on its own. But
some build on others (testing pulls in patterns from earlier lessons; the
distributed lesson assumes you've seen the supervisor lesson). The numbering
is a suggested path, not a hard sequence.

## A warning before you start

The advanced features are powerful. They're also where it's easiest to over-
engineer. Build the simple version of your agent first (one process, no
supervisors, no distributed transport, no LLM). Add complexity only when you
have a real reason. Every layer you add is a layer you have to debug later.

A good rule: if you can explain to a friend exactly what problem this feature
solves *for your specific agent*, use it. If you're adding it because it
sounds cool, don't.

[← Back to beginner course](../README.md)
