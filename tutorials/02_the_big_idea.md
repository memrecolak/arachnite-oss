# Lesson 2 — The Big Idea: Sense → Think → Act

Every Arachnite agent does the same three things, in a loop:

```
   ┌─────────────┐
   │   SENSE     │   "What is happening right now?"
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │   THINK     │   "What should I do about it?"
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐
   │   ACT       │   "Do the thing."
   └──────┬──────┘
          │
          └─────► (loop back to SENSE, very fast)
```

This loop runs many times per second. Each pass through the loop is called a
**tick** — like the tick of a clock.

Let's walk through each step with a simple example: **a smart fan that turns
on when the room is hot.**

## Step 1 — Sense

Sensing means gathering information from the outside world. The information
could come from anywhere:

- A real temperature sensor connected to your computer
- A file you read
- A message from the internet
- Even just the current time

In our hot-room example, sensing means:

> *"Read the thermometer. It says 42 degrees Celsius."*

In Arachnite, you write a small class called a **Sense Node** that does this
one job. Every tick, the framework will call your sense node and ask "what
do you see?", and you reply with a piece of data called a **Signal**.

A Signal is just a tidy little package that says "I observed *this*, with
*this much* confidence, at *this* moment in time."

## Step 2 — Think

Once your agent has gathered signals, it has to decide what to do.

In Arachnite, "thinking" is split into two smaller jobs:

- **Instincts** — patterns that watch for specific situations and say "I
  want to do something about this!"
- **Decisions** — when more than one instinct fires at once, the decision
  layer picks which one wins.

For our hot-room example:

> *"The temperature signal says 42 degrees. That's higher than 40. My
> 'too hot' instinct fires and proposes: turn on the fan."*

You write each instinct as a small class called an **Instinct Node**. Every
tick, the framework asks each instinct, "do you want to do anything?", and
the instinct replies with either:

- A **Proposal** — "Yes! Please run this action."
- Nothing (`None`) — "Not right now."

A Proposal is another tidy package that says *which* action to run, *how
urgently*, and any extra information that action might need.

## Step 3 — Act

Finally, the chosen proposal gets carried out by an **Action Node**. Action
nodes are small classes that actually *do* something — turn on hardware,
print a message, write to a file, send a network request.

For our hot-room example:

> *"OK, I'm turning the fan on. Done!"*

Every action returns a **Result** — yes, another tidy package — that says
"the action succeeded" or "it failed, here's why." This way the rest of the
agent can learn from what just happened.

## And repeat!

After acting, the loop starts over. Sense again, think again, act again.
Every fraction of a second.

This means your agent is constantly aware of its surroundings and constantly
ready to react. If the temperature drops back down, the "too hot" instinct
will stop firing, and the fan can turn off. If it spikes higher, an even more
urgent instinct can kick in.

## Why split it into three pieces?

You might wonder: why not just write one big function?

```python
while True:
    temperature = read_temperature()
    if temperature > 40:
        turn_on_fan()
```

That works for one sensor and one action. But what if you have:

- Five sensors (temperature, humidity, motion, sound, light)
- Ten different things you might do
- Some safety rules that should always win
- The need to log everything for later
- Code that runs on a Raspberry Pi *and* on a laptop

Your one big function will turn into a tangled mess. Splitting your program
into senses, instincts, and actions keeps each piece small and focused. You
can add a new sensor without touching any actions. You can add a new action
without rewriting any sensors. Each part does **one thing**.

This is called **separation of concerns**, and it's one of the most important
ideas in software engineering. Arachnite makes it easy.

## A picture worth keeping

```
   sensors       instincts        actions
   ───────       ─────────        ───────
    [eye]  ──┐                ┌── [hand]
    [ear]  ──┼──► think ──────┼── [voice]
   [touch] ──┘                └── [legs]
```

Information flows left to right, every tick. Sensors *push* data in,
instincts *evaluate* it, decisions *pick a winner*, actions *carry it out*.

## What's next?

Now you understand the basic shape. In the next lesson we'll look at the
actual Python pieces you'll be writing: the Signal, Proposal, and Result
classes, and the three node types that you extend.

[← Previous: Welcome](01_welcome.md) | [Next: Lesson 3 — Meet the Pieces →](03_meet_the_pieces.md)
