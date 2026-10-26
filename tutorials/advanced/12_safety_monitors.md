# Lesson 12 — Runtime Safety Monitors

When your agent controls physical hardware — a robot arm, a drone, a valve —
you need to know *immediately* if something goes wrong at the architectural
level. Safety monitors watch your agent's behaviour every tick and raise an
alarm if a safety invariant is breached.

## What Gets Monitored

Arachnite ships with six built-in monitors:

| Monitor | What it checks | Severity |
|---------|---------------|----------|
| `ReflexBypassMonitor` | A reflex's proposal was offered to the decision layer | CRITICAL |
| `MandatoryBlockMonitor` | A deliberative interrupt was honoured inside a mandatory block | CRITICAL |
| `ReflexDispatchMonitor` | A reflex fired but its action could not be reached | CRITICAL |
| `ActuatorExclusivityMonitor` | A declared resource had more than one running owner | CRITICAL |
| `ReflexAvailabilityMonitor` | A reflex node is FAULTED, RESTARTING or DEAD | WARNING |
| `TickBudgetMonitor` | 3+ consecutive tick overruns | WARNING |

The first four are the framework's structural safety properties — the same
ones the CSP models in `formal/` verify. The monitors check them at runtime.

## Quick Setup

```python
from arachnite import SignalBus
from arachnite.safety_monitor import SafetyMonitorRegistry

bus = SignalBus()
monitors = SafetyMonitorRegistry.default(bus)  # all 6 monitors
```

The runtime does **not** run the monitors for you. After each tick, ask the
runtime for a `MonitorState` and call `check_all()`:

```python
state = rt.monitor_state()
violations = await monitors.check_all(rt.tick_count, state)
for v in violations:
    print(f"SAFETY: {v.property_name} — {v.details}")
```

`rt.monitor_state()` is built from what the runtime actually observed during
the last tick — nothing in it is a constant you have to remember to fill in:

- **`reflex_fired` / `reflex_action_dispatched`** — the launch outcome of every
  reflex proposal since the previous tick (event-lane reflexes included).
  Launched, armed (waiting for a preempted owner's safe-stop) and
  de-duplicated all count as "dispatched"; an unknown action does not.
- **`reflex_routed_through_decision`** — whether a reflex node's proposal was
  offered to the decision layer. The decide stage runs on *every* tick, also
  when a reflex fired — the normal instincts still get their turn — so the
  property is about the reflex's own proposal, which must never go there.
- **`interrupt_accepted_during_block`** — whether an action that finished
  reported a *deliberative* stop strictly inside a mandatory block. A reflex
  with `ReflexPreemption.PREEMPT` cutting into a block is declared behaviour,
  not a violation (see [Multi-Step Actions](01_multi_step_actions.md)).
- **`mandatory_block_active`, `resources_with_multiple_owners`,
  `active_reflex_nodes` / `total_reflex_nodes`, `tick_duration_ms`,
  `tick_budget_ms`** — read from the running actions, their resources, the
  reflex nodes' supervisor states and the tick itself.

## Reacting to Violations

Violations are published as `SafetyViolationSignal` on the `SignalBus` with
kind `"safety_violation"`. They are **not** added to `ctx.signals` by the
tick, so an ordinary instinct won't see them. Two ways to react:

- subscribe a callback: `bus.subscribe("safety_violation", on_violation)`, or
- run the runtime in event mode (`reflex_mode="event"`) and give a reflex
  `trigger_on_signals=["safety_violation"]`. The event-driven reflex lane
  then evaluates it the moment a violation is published, with the violation
  in `ctx.signals`:

```python
class SafeModeReflex(BaseReflexInstinctNode):
    node_id = "SafeModeReflex"
    priority = 250
    trigger_on_signals = ["safety_violation"]  # needs reflex_mode="event"

    async def evaluate(self, ctx) -> Proposal | None:
        violations = [
            s for s in ctx.signals
            if s.kind == "safety_violation"
        ]
        if violations:
            return Proposal(
                instinct_id=self.node_id,
                action_id="EnterSafeMode",
                priority=self.priority,
                urgency=1.0,
                rationale=f"Safety violation: {violations[0].value}",
            )
        return None
```

## Checking Health

```python
# Are all monitors happy?
if monitors.all_healthy:
    print("All safety invariants hold")

# How many violations total?
print(f"Total violations: {monitors.total_violations}")

# Check individual monitors
for m in monitors.monitors:
    print(f"  {m.monitor_id}: healthy={m.healthy}, violations={m.violation_count}")
```

## Writing Custom Monitors

Extend `BaseSafetyMonitor` to check your own invariants:

```python
from arachnite.safety_monitor import BaseSafetyMonitor, SafetySeverity

class BatteryMonitor(BaseSafetyMonitor):
    monitor_id = "BatteryMonitor"

    async def check(self, tick, state):
        # Your custom check logic
        if battery_level < 0.1:
            return await self.emit_violation(
                property_name="battery_critical",
                severity=SafetySeverity.WARNING,
                details=f"Battery at {battery_level*100:.0f}%",
            )
        return None

# Register it
monitors.register(BatteryMonitor(bus))
```

## Severity Levels

| Level | Meaning | Typical response |
|-------|---------|-----------------|
| `WARNING` | Invariant degraded but not breached | Log, increase monitoring |
| `VIOLATION` | Invariant breached | Trigger compensating action |
| `CRITICAL` | Safety-critical invariant breached | Safe-mode reflex, or shut down with `emergency_stop()` |

## Next Steps

- **Lesson 13** covers benchmarking — measuring tick latency, reflex response
  time, and memory footprint.
