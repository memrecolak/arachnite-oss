# Lesson 12 — Runtime Safety Monitors

When your agent controls physical hardware — a robot arm, a drone, a valve —
you need to know *immediately* if something goes wrong at the architectural
level. Safety monitors watch your agent's behaviour every tick and raise an
alarm if a safety invariant is breached.

## What Gets Monitored

Arachnite ships with five built-in monitors:

| Monitor | What it checks | Severity |
|---------|---------------|----------|
| `ReflexBypassMonitor` | Reflex fires but decision layer also entered | CRITICAL |
| `MandatoryBlockMonitor` | Deliberative interrupt accepted during mandatory block | CRITICAL |
| `ReflexDispatchMonitor` | Reflex fires but action not dispatched | CRITICAL |
| `ReflexAvailabilityMonitor` | Reflex node is FAULTED or DEAD | WARNING |
| `TickBudgetMonitor` | 3+ consecutive tick overruns | WARNING |

These correspond to the formal safety properties verified in the UPPAAL model
(Properties P1-P5). The monitors check the *same* invariants at runtime.

## Quick Setup

```python
from arachnite import SignalBus
from arachnite.safety_monitor import SafetyMonitorRegistry, MonitorState

bus = SignalBus()
monitors = SafetyMonitorRegistry.default(bus)  # all 5 monitors
```

The runtime does **not** run the monitors for you. After each tick, build a
`MonitorState` from what actually happened and call `check_all()`. Every
field must come from a real source: a monitor fed constant values checks
nothing. Some fields can be read from the master nodes you built; the
others come from your own instrumentation:

```python
from arachnite import NodeState


def build_monitor_state(
    rt, instinct_master, action_master, *,
    reflex_fired: bool,              # your tracking, e.g. a log sink that sees "Reflex fired"
    reflex_action_dispatched: bool,  # ... and the dispatch that followed it
    tick_duration_ms: float,         # e.g. from a TickInstrumenter's on_tick_complete()
    tick_budget_ms: float,           # 1000 / tick_rate_hz
) -> MonitorState:
    reflex_ids = {n.node_id for n in instinct_master.reflex_nodes}
    running = set(rt.health.nodes_in_state(NodeState.RUNNING))
    return MonitorState(
        tick=rt.tick_count,
        reflex_fired=reflex_fired,
        reflex_action_dispatched=reflex_action_dispatched,
        # A reflex's own proposal never enters the decision layer (see below)
        decision_entered=False,
        mandatory_block_active=not action_master.is_interruptible(),
        # Deliberation is refused inside a block (MandatoryBlockViolation)
        interrupt_accepted_during_block=False,
        active_reflex_nodes=len(reflex_ids & running),
        total_reflex_nodes=len(reflex_ids),
        tick_duration_ms=tick_duration_ms,
        tick_budget_ms=tick_budget_ms,
    )


state = build_monitor_state(
    rt, instinct_master, action_master,
    reflex_fired=False, reflex_action_dispatched=False,
    tick_duration_ms=0.5, tick_budget_ms=100.0,
)
violations = await monitors.check_all(rt.tick_count, state)
if violations:
    for v in violations:
        print(f"SAFETY: {v.property_name} — {v.details}")
```

Two fields need care, because the runtime's behaviour is more subtle than
their names suggest:

- **`decision_entered`** is about the *reflex's proposal*. In
  `ArachniteRuntime` the decide stage runs on every tick, including ticks
  where a reflex fired: the normal instincts still get their turn. The
  reflex's own proposal never goes through it. Don't set this field from
  "did the decide stage run", or `ReflexBypassMonitor` reports a violation
  on every reflex tick.
- **`interrupt_accepted_during_block`** should count only *deliberative*
  interrupts. The decision layer can't interrupt a mandatory block (the
  request is refused with `MandatoryBlockViolation`). A reflex with
  `ReflexPreemption.PREEMPT` cutting into a block is declared behaviour, not
  a violation (see [Multi-Step Actions](01_multi_step_actions.md)).

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
