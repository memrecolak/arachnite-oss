# Glossary

A quick reference for terms used in this course. If something feels fuzzy,
look it up here.

### Action
The third part of the sense → think → act loop. The thing your agent
actually *does* when an instinct decides it should. In code, an action is a
class that inherits from `BaseActionNode` and implements `execute()`.

### Action Node
A class that inherits from `BaseActionNode`. It has one method, `execute()`,
which receives a `Proposal` and returns a `Result`.

### Agent
A program built with Arachnite. It senses, thinks, and acts in a loop until
you stop it.

### Async / await
Python keywords for writing **concurrent** code. An `async def` function can
pause itself with `await` to let other things run. Arachnite uses async so
many sensors can be read at the same time, and so the agent doesn't freeze
while waiting for slow operations.

### BaseActionNode
The Arachnite base class you inherit from to make a custom action. Provides
all the plumbing — you only have to write `execute()`.

### BaseInstinctNode
The base class you inherit from to make a normal instinct. You write
`evaluate()`.

### BaseReflexInstinctNode
A special base class for **reflex** instincts that bypass the decision layer.
Use it for emergency / safety reactions.

### BaseSenseNode
The base class you inherit from to make a sensor. You write `read()`.

### Class
A Python concept: a blueprint that defines the data and behaviour of a kind
of object. In Arachnite, you create custom classes by inheriting from one of
the base classes.

### Concurrent
Doing multiple things at the "same" time. In Arachnite, multiple sensors are
read concurrently each tick.

### Confidence
A field on `Signal` that says how reliable the reading is, from 0.0
(no idea) to 1.0 (absolutely certain). Useful when sensors are noisy.

### Context (ContextNode)
The agent's short-term memory. Each tick, the runtime fills the context with
the current signals and the result of the last action. Instincts read the
context to decide whether to fire.

### Decision layer
The part of the runtime that picks **one winner** when multiple instincts
fire on the same tick. The simplest strategy is "pick the highest priority"
(`GreedyDecisionNode`).

### evaluate()
The method you implement on a `BaseInstinctNode`. The framework calls it
every tick. It returns a `Proposal` if the instinct wants to fire, or `None`
otherwise. Never raise exceptions from here.

### execute()
The method you implement on a `BaseActionNode`. The framework calls it when
your action is chosen. It returns a `Result`. Never raise exceptions — return
a Result with `success=False` if something goes wrong.

### Framework
A pile of code that someone else wrote, designed to be filled in with your
own code. Arachnite is a framework for building reactive agents.

### Greedy decision
A decision strategy that always picks the proposal with the highest priority.
Implemented by `GreedyDecisionNode`.

### Inheritance
A Python feature where one class is built on top of another. When you write
`class MySensor(BaseSenseNode):`, your `MySensor` automatically gets all the
behaviour of `BaseSenseNode` for free, plus whatever you add or override.

### Instinct
The "thinking" part of the loop. An instinct watches the context and
proposes an action when it spots a situation it cares about. Implemented by
`BaseInstinctNode` (normal) or `BaseReflexInstinctNode` (emergency).

### Master node
A node that holds a collection of other nodes. You have one
`SenseMasterNode` (which holds all your sensors), one `InstinctMasterNode`,
one `ActionMasterNode`, and one `DecisionMasterNode`. They're created once
when the agent starts.

### Node
The general name for any building block in Arachnite — a sensor, an
instinct, an action, or a master. They all inherit from `BaseNode` deep
down.

### node_id
A unique string name that identifies a node. Used so other nodes can refer
to it without holding a direct reference.

### Priority
A number on a Proposal that says how important it is. Higher numbers win
over lower numbers when the decision layer picks. Suggested ranges:
- 200+ = reflexes
- 100–199 = safety
- 50–99 = goal-directed
- 1–49 = exploratory

### Proposal
A bundle of data an instinct returns when it wants to fire. It says which
action to run (`action_id`), how important it is (`priority`), how urgent
(`urgency`), and any extra parameters the action might need.

### Reactive
Describes a program that responds to events as they happen, instead of
running through a fixed script. Arachnite agents are reactive.

### Reflex
A special instinct that bypasses the decision layer. Use for emergencies
where you can't afford to wait. Inherit from `BaseReflexInstinctNode` and
use a priority >= 200.

### Result
A bundle of data an action returns after it runs. It says whether the action
succeeded, optionally what output it produced, and how long it took. Always
return one — never raise.

### Runtime (ArachniteRuntime)
The main orchestrator. Owns the tick loop. You build it, hand it the
masters, then call `start()`, wait, and `stop()`.

### Sensor / Sense node
The "perception" part of the loop. A sensor reads something from the world
(a number, a status, a message) and returns a `Signal`. Implemented by
inheriting from `BaseSenseNode` and writing `read()`.

### Signal
A bundle of data a sensor produces. It has a `source` (which sensor),
`kind` (what type of data), `value` (the actual reading), `confidence`,
and `timestamp`.

### SignalBus
The messaging system that all nodes use to talk to each other. Nodes never
hold references to each other directly — they communicate through the bus
by publishing signals and subscribing to kinds.

### Strategy (decision strategy)
The pluggable algorithm a `DecisionMasterNode` uses to pick a winning
proposal. `GreedyDecisionNode` is the simplest one (highest priority wins).

### Tick
One full pass through the sense → think → act loop. The tick rate
(`tick_rate_hz`) controls how many ticks happen per second. A typical agent
runs at 5–10 ticks per second.

### Urgency
A 0.0–1.0 number on a Proposal that says how soon the action should be run.
The decision layer can use this to break ties or weight choices in more
advanced strategies.
