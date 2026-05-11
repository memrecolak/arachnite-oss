# Learn Arachnite — A Beginner's Course

Welcome! This folder contains a friendly, step-by-step course for people who
have **just learned the basics of programming** (variables, functions, classes,
maybe a little Python) and want to learn how to build smart, reactive programs
using the **Arachnite** framework.

You don't need to know anything about robotics, AI, or async programming yet.
We'll build your understanding piece by piece.

## What you'll learn

By the end of this course you will be able to:

- Explain what Arachnite is and what kind of programs it builds
- Read and write your own *agents* (small programs that sense, think, and act)
- Use the three main building blocks: **sensors**, **instincts**, and **actions**
- Run an agent on your own computer and watch it work
- Build a small project on your own

## What you need before starting

- Python 3.10 or newer installed
- A code editor (VS Code, PyCharm, even IDLE works)
- A terminal (Terminal on Mac, PowerShell on Windows)
- A little bit of patience — programming takes practice!

You should already know what a **variable**, a **function**, a **class**, and a
**list** are in Python. If any of those words are unfamiliar, take a quick
detour through a Python beginner tutorial first, then come back.

## The course

Read the lessons in order. Each one builds on the last.

| # | Lesson | What it covers |
|---|---|---|
| 1 | [Welcome to Arachnite](01_welcome.md) | What Arachnite is and the spider metaphor that makes it click |
| 2 | [The Big Idea: Sense → Think → Act](02_the_big_idea.md) | The basic loop every agent follows |
| 3 | [Meet the Pieces](03_meet_the_pieces.md) | Signals, Proposals, Results, and the three node types |
| 4 | [Your First Agent](04_your_first_agent.md) | Build and run a complete agent, line by line |
| 5 | [The Tick Loop](05_the_tick_loop.md) | How the runtime brings everything to life |
| 6 | [When Instincts Compete](06_priorities_and_decisions.md) | What happens when more than one instinct fires |
| 7 | [Reflexes — Emergency Reactions](07_reflexes.md) | How to build safety responses that bypass thinking |
| 8 | [A Smart Lamp Project](08_smart_lamp_project.md) | Putting everything together in a small project |
| 9 | [Benchmarking Your Agent](09_benchmarking.md) | How fast is your agent? Measuring and understanding performance |

When you're done with the lessons, head to [`exercises/`](exercises/) to
practice on your own. Each exercise comes with a starter file and a solution
you can peek at if you get stuck.

If you forget what a word means, check the [glossary](glossary.md).

## Ready for more? Advanced topics

Once you've worked through the beginner course and feel comfortable building
small agents, the [`advanced/`](advanced/README.md) folder has thirteen deeper
lessons covering the parts of Arachnite that turn a hobby program into
something serious:

| # | Lesson | What it covers |
|---|---|---|
| 1 | [Multi-Step Actions](advanced/01_multi_step_actions.md) | Long-running actions with steps, interruption, and rollback |
| 2 | [Supervisors and Health](advanced/02_supervisors_and_health.md) | Auto-restart for crashed nodes |
| 3 | [Smarter Context](advanced/03_smarter_context.md) | History, persistent state, and detecting trends |
| 4 | [Custom Decision Strategies](advanced/04_custom_decision_strategies.md) | Going beyond `GreedyDecisionNode` |
| 5 | [Logging and Observability](advanced/05_logging_and_observability.md) | Structured logs and how to debug a running agent |
| 6 | [Configuration Injection](advanced/06_configuration_injection.md) | `NodeConfig`, env vars, and keeping secrets out of code |
| 7 | [Going Distributed](advanced/07_going_distributed.md) | `AgentNode`, transports, deployment manifests |
| 8 | [LLM Instincts](advanced/08_llm_instincts.md) | Plugging a language model into the decision layer |
| 9 | [Testing Your Agents](advanced/09_testing_your_agents.md) | Patterns for fast async tests of every node type |
| 10 | [Sensor Fusion](advanced/10_sensor_fusion.md) | Combining redundant sensors with Bayesian and Ensemble merge policies |
| 11 | [Active Inference](advanced/11_active_inference.md) | Decision-making that balances goal achievement and uncertainty reduction |
| 12 | [Safety Monitors](advanced/12_safety_monitors.md) | Runtime verification of safety invariants with automatic alerting |
| 13 | [Benchmarking](advanced/13_benchmarking.md) | Measuring tick latency, reflex response time, and scalability sweeps |

Don't rush into these. Build something with the basics first. The advanced
features make sense only when you've felt the limitations they're solving.

## How to use this course

- **Read slowly.** Don't skim the code blocks. Type them out yourself.
- **Run everything.** Don't just read — actually run each example. You learn
  by doing.
- **Break things.** Change a number, see what happens. Programming is play.
- **Ask why.** If something feels like magic, find out how it really works.

Good luck, and have fun building your first reactive agent!
