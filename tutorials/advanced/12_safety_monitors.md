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
| `MandatoryBlockMonitor` | Interrupt accepted during mandatory block | CRITICAL |
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

After each tick, build a `MonitorState` and call `check_all()`:

```python
state = MonitorState(
    tick=tick_count,
    reflex_fired=True,
    reflex_action_dispatched=True,
    decision_entered=False,
    mandatory_block_active=False,
    interrupt_accepted_during_block=False,
    active_reflex_nodes=2,
    total_reflex_nodes=2,
    tick_duration_ms=0.5,
    tick_budget_ms=100.0,
)

violations = await monitors.check_all(tick_count, state)
if violations:
    for v in violations:
        print(f"SAFETY: {v.property_name} — {v.details}")
```

## Reacting to Violations

Violations are published as `SafetyViolationSignal` on the `SignalBus` with
kind `"safety_violation"`. Any instinct — including reflex instincts — can
subscribe and react:

```python
class SafeModeReflex(BaseReflexInstinctNode):
    node_id = "SafeModeReflex"
    priority = 250

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
| `CRITICAL` | Safety-critical invariant breached | Emergency stop |

## Next Steps

- **Lesson 13** covers benchmarking — measuring tick latency, reflex response
  time, and memory footprint.
