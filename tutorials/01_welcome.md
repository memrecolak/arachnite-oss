# Lesson 1 — Welcome to Arachnite

## What is Arachnite?

Arachnite is a Python **framework**. A framework is a collection of code
someone else wrote that gives you a head start when building a certain kind of
program. You don't start from a blank file — you fill in the parts that are
unique to *your* idea, and the framework handles the boring plumbing.

Arachnite specifically helps you build **reactive agents**. An agent is a
small program that:

1. Watches the world (through sensors, files, network messages, anything)
2. Decides what to do about what it sees
3. Takes an action

Then it does that again. And again. And again. Many times per second, forever
(or until you turn it off).

## Where do you use this?

You'd use Arachnite to build things like:

- A program that watches a temperature sensor and turns on a fan when it gets
  hot
- A robot that drives around and avoids obstacles
- A monitoring tool that watches your computer and sends you an alert when
  something looks wrong
- A smart lamp that turns on automatically when it gets dark
- A game enemy that reacts to where the player is

All of these programs share the same shape: **look, think, do — repeat.**
Arachnite gives you a clean structure for that shape.

## The spider metaphor

The framework is named after **arachnids** (spiders) for a reason. Imagine a
spider sitting on its web. What does it do all day?

- It **feels vibrations** in the silk threads (sensing)
- When something hits the web, it instantly knows "is that prey, a leaf, or a
  predator?" (recognizing)
- It **decides** whether to attack, run, or ignore (deciding)
- It **acts** — it pounces, hides, or stays still (acting)

A spider doesn't sit there and write a five-page essay before each move. Its
nervous system is built for fast, automatic responses. Some reactions are even
faster than thinking — if you poke a spider, it jumps before any "thought"
happens at all. That's a **reflex**.

Arachnite copies this design. Your agent has:

- **Sense nodes** — like the spider's leg hairs that feel vibration
- **Instincts** — like its built-in patterns ("vibration + small + moving =
  prey")
- **Reflexes** — emergency reactions that skip thinking entirely
- **Actions** — what the spider actually *does* with its body

Once you see this picture in your head, the rest of the framework will fall
into place easily.

## Why "biologically inspired"?

A lot of programming frameworks try to imitate biology because biology is
*really good* at building systems that react fast and don't break when one
piece fails. A spider doesn't need a central computer telling every leg what
to do — each part contributes to the whole. Arachnite tries to give your
programs that same robustness.

You don't need to remember all of this. Just keep one image in your head: a
spider on a web, sensing, reacting, acting.

## What's next?

In the next lesson we'll zoom in on the basic loop every Arachnite agent
follows: **sense → think → act → repeat**. You'll see why this simple cycle
is powerful enough to build everything from a smart lightbulb to a robot.

[Next: Lesson 2 — The Big Idea →](02_the_big_idea.md)
