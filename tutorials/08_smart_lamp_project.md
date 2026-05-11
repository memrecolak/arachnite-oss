# Lesson 8 — Project: A Smart Lamp

You've made it! In this final lesson we'll build a **complete, slightly
realistic project** from scratch using everything you've learned:

- Two sensors (a light sensor and a motion sensor)
- Two normal instincts (turn on when dark + motion, turn off when bright)
- One reflex (panic if the lamp gets way too hot)
- Three actions (turn lamp on, turn lamp off, emergency shut down)

We'll fake the sensor data so the agent runs interestingly without needing
real hardware.

## What the lamp should do

1. If it's dark **and** there's motion in the room → turn on.
2. If it's bright (sun came up) → turn off.
3. If the lamp temperature ever goes above 70°C → emergency shut down,
   ignoring everything else.

This is a classic reactive-agent pattern: a few sensors, a few rules, a
priority hierarchy.

## The full code

Save this as `smart_lamp.py`:

```python
import asyncio
import random
import time

from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    Signal, Proposal, Result,
    BaseSenseNode, SenseMasterNode,
    BaseInstinctNode, BaseReflexInstinctNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    BaseActionNode, ActionMasterNode,
)


# ────────────────────────────────────────────────────────
# SENSORS
# ────────────────────────────────────────────────────────

class LightSensor(BaseSenseNode):
    """Pretend light sensor — returns a brightness value 0..100."""
    node_id = "LightSensor"
    signal_kind = "brightness"
    poll_interval_s = 0.0   # read every tick (default 0.1s would throttle)

    async def read(self) -> Signal:
        # Random brightness, weighted toward darker
        value = random.choice([5, 10, 20, 30, 60, 80])
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=value,
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class MotionSensor(BaseSenseNode):
    """Pretend motion sensor — True if motion detected."""
    node_id = "MotionSensor"
    signal_kind = "motion"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        value = random.random() < 0.4   # 40% chance of motion
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=value,
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class LampTempSensor(BaseSenseNode):
    """Pretend lamp temperature sensor — usually safe, occasionally spikes."""
    node_id = "LampTempSensor"
    signal_kind = "lamp_temperature"
    poll_interval_s = 0.0

    async def read(self) -> Signal:
        # 5% chance of a critical reading (for the reflex demo)
        if random.random() < 0.05:
            value = random.uniform(75, 95)
        else:
            value = random.uniform(20, 50)
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=value,
            confidence=1.0,
            timestamp=time.monotonic(),
        )


# ────────────────────────────────────────────────────────
# INSTINCTS
# ────────────────────────────────────────────────────────

class TurnOnWhenDarkAndMoving(BaseInstinctNode):
    node_id = "TurnOnWhenDarkAndMoving"
    priority = 70  # goal-directed

    async def evaluate(self, ctx) -> Proposal | None:
        brightness = next(
            (s.value for s in ctx.signals if s.kind == "brightness"), None
        )
        motion = next(
            (s.value for s in ctx.signals if s.kind == "motion"), None
        )
        if brightness is not None and motion and brightness < 25:
            return Proposal(
                instinct_id=self.node_id,
                action_id="LampOn",
                priority=self.priority,
                urgency=0.7,
                rationale=f"dark ({brightness}) and motion detected",
            )
        return None


class TurnOffWhenBright(BaseInstinctNode):
    node_id = "TurnOffWhenBright"
    priority = 60

    async def evaluate(self, ctx) -> Proposal | None:
        brightness = next(
            (s.value for s in ctx.signals if s.kind == "brightness"), None
        )
        if brightness is not None and brightness >= 60:
            return Proposal(
                instinct_id=self.node_id,
                action_id="LampOff",
                priority=self.priority,
                urgency=0.5,
                rationale=f"bright ({brightness})",
            )
        return None


# ────────────────────────────────────────────────────────
# REFLEX
# ────────────────────────────────────────────────────────

class OverheatReflex(BaseReflexInstinctNode):
    node_id = "OverheatReflex"
    priority = 250  # reflex range

    async def evaluate(self, ctx) -> Proposal | None:
        temp = next(
            (s.value for s in ctx.signals if s.kind == "lamp_temperature"),
            None,
        )
        if temp is not None and temp > 70.0:
            return Proposal(
                instinct_id=self.node_id,
                action_id="EmergencyShutdown",
                priority=self.priority,
                urgency=1.0,
                rationale=f"lamp temp critical: {temp:.1f}",
            )
        return None


# ────────────────────────────────────────────────────────
# ACTIONS
# ────────────────────────────────────────────────────────

class LampOn(BaseActionNode):
    node_id = "LampOn"

    async def execute(self, proposal) -> Result:
        print(f"  [LAMP] ON  ({proposal.rationale})")
        return Result(action_id=self.node_id, success=True)


class LampOff(BaseActionNode):
    node_id = "LampOff"

    async def execute(self, proposal) -> Result:
        print(f"  [LAMP] OFF ({proposal.rationale})")
        return Result(action_id=self.node_id, success=True)


class EmergencyShutdown(BaseActionNode):
    node_id = "EmergencyShutdown"

    async def execute(self, proposal) -> Result:
        print(f"  !!! EMERGENCY SHUTDOWN !!! ({proposal.rationale})")
        return Result(action_id=self.node_id, success=True)


# ────────────────────────────────────────────────────────
# WIRING + MAIN
# ────────────────────────────────────────────────────────

async def main() -> None:
    bus = SignalBus()

    sense_master = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=GreedyDecisionNode(bus=bus)
    )
    action_master = ActionMasterNode(bus=bus)

    # Sensors
    sense_master.register(LightSensor(bus=bus))
    sense_master.register(MotionSensor(bus=bus))
    sense_master.register(LampTempSensor(bus=bus))

    # Instincts (normal + reflex)
    instinct_master.register(TurnOnWhenDarkAndMoving(bus=bus))
    instinct_master.register(TurnOffWhenBright(bus=bus))
    instinct_master.register(OverheatReflex(bus=bus))

    # Actions
    action_master.register(LampOn(bus=bus))
    action_master.register(LampOff(bus=bus))
    action_master.register(EmergencyShutdown(bus=bus))

    rt = ArachniteRuntime(
        sense_master=sense_master,
        context=ContextNode(),
        instinct_master=instinct_master,
        decision_master=decision_master,
        action_master=action_master,
        bus=bus,
        tick_rate_hz=2.0,
    )

    print("Smart lamp running for 10 seconds...")
    await rt.start()
    await asyncio.sleep(10.0)
    await rt.stop()
    print("Stopped.")


if __name__ == "__main__":
    asyncio.run(main())
```

## Run it!

```bash
python smart_lamp.py
```

You'll see output like:

```
Smart lamp running for 10 seconds...
  [LAMP] ON  (dark (10) and motion detected)
  [LAMP] OFF (bright (80))
  [LAMP] ON  (dark (5) and motion detected)
  [LAMP] OFF (bright (60))
  !!! EMERGENCY SHUTDOWN !!! (lamp temp critical: 78.3)
  [LAMP] ON  (dark (20) and motion detected)
  ...
Stopped.
```

Every couple of seconds, the agent senses the environment, decides what to do
based on priority, and acts. Sometimes the overheat reflex fires and
overrides everything else.

## What to notice

1. **Each piece is small.** The biggest class is maybe 15 lines. That's the
   whole point of separating sense, instinct, and action.

2. **Priorities make the order obvious.**
   - Reflex (250) > goal instincts (70, 60).
   - Within goal instincts, "turn on" (70) wins over "turn off" (60) if both
     somehow fired together. This happens to be a safe order — turning on
     accidentally is better than getting stuck off.

3. **Adding new behaviour is trivial.** Want to also play a sound when the
   lamp turns on? Add a `PlaySound` action and call it from the same
   instinct (you can have one instinct propose multiple actions over time —
   or have a second instinct that fires under the same conditions).

4. **No tangled `if/else`.** Each instinct only knows its own condition.
   None of them know about the others.

## Things to try

1. **Tune the rules.** Change `brightness < 25` to `< 50` and watch the lamp
   come on more often.
2. **Add an "energy saver" instinct.** If brightness is 25–60 (twilight),
   propose `LampOff` with low priority. See how it interacts.
3. **Slow it down.** Set `tick_rate_hz=1.0` to make the output easier to
   follow.
4. **Make the sensors deterministic.** Use a counter instead of `random` so
   the agent's behaviour is predictable while you experiment.
5. **Add logging.** Replace `print()` with `self.logger.info(...)` in your
   actions to see how the framework's structured logging works.

## You've finished the course!

You now know:

- What Arachnite is and what kind of programs it builds
- The sense → think → act loop and the spider metaphor
- How to write `BaseSenseNode`, `BaseInstinctNode`, and `BaseActionNode`
- What `Signal`, `Proposal`, and `Result` are
- How the tick loop works and what happens during one tick
- Why everything is async, and the rules for using `await`
- How priorities and the decision layer pick winners
- When and how to use reflexes for emergency reactions
- How to combine all of these into a working project

That's the entire core of the framework. There's more to learn — multi-step
actions, supervisors, distributed agents, transport layers — but you're
ready to explore the [main README](../README.md), [SPEC.md](../SPEC.md), and
the [examples/](../examples/) folder on your own.

## Where to go from here

- **Practice:** Try the [exercises](exercises/) folder. Each one is a small,
  self-contained challenge with a starter file and a worked solution.
- **Read the real examples:** Look at `../examples/temperature_monitor.py`
  for a more advanced agent with multi-step actions and supervisors.
- **Build your own project.** Pick something simple — a fake stock trader,
  an idle game enemy, a self-watering plant simulator. Use what you learned.

Happy building!

[← Previous: Reflexes](07_reflexes.md) | [Back to course index](README.md)
