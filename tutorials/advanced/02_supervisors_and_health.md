# Advanced 2 — Supervisors and Health

In a real agent, things break. A sensor times out, a network call fails, a
hardware library throws an unexpected exception. By default, an exception in
a node will cause the runtime to log a warning and *that node stops working*
for the rest of the program's life. Not great.

The fix is a **supervisor**: a tiny manager that watches your nodes and
**automatically restarts** them when they crash.

## NodeSupervisor

`NodeSupervisor` is the class that does this. You create one (or more) at
startup, *track* the nodes you want supervised, and the supervisor handles
crashes from there.

```python
from arachnite import NodeSupervisor, RestartPolicy, NodeState

supervisor = NodeSupervisor(
    bus=bus,
    supervisor_id="main",
    restart_policy=RestartPolicy.ON_FAILURE,
    max_restarts=5,
    restart_delay_s=2.0,
)

# Tell it which nodes to watch
supervisor.track(temp_sensor)
supervisor.track(camera_sensor)
supervisor.track(deploy_action)
```

## Restart policies

You pick what "restart" means for your agent:

- **`RestartPolicy.NEVER`** — never restart. If a node faults, it goes
  straight to `DEAD`.
- **`RestartPolicy.ON_FAILURE`** — restart only when an unhandled exception
  is raised. Clean stops are respected. *(Most common.)*
- **`RestartPolicy.ALWAYS`** — restart on any non-clean exit, including
  silent exits.

You also set:

- `max_restarts` — how many times to retry before giving up.
- `restart_delay_s` — how long to wait between restarts (so a flapping node
  doesn't spin the CPU).

## Node lifecycle states

While supervised, every node moves through these states:

| State | Meaning |
|---|---|
| `STARTING` | Just registered, hasn't started yet |
| `RUNNING` | Working normally |
| `IDLE` | Started but currently doing nothing |
| `FAULTED` | Raised an unhandled exception |
| `RESTARTING` | Being torn down and rebuilt |
| `STOPPED` | Cleanly stopped (intentional) |
| `DEAD` | Out of restart attempts; permanently broken |

You can query a node's current state:

```python
state = supervisor.state_of("TempSense")
if state == NodeState.DEAD:
    print("Sensor is permanently broken — alert someone!")
```

## A flaky-sensor demo

Here's a sensor that fails every 5 reads, with a supervisor that picks it
back up:

```python
import asyncio
import random
import time

from arachnite import (
    SignalBus, NodeSupervisor, RestartPolicy, NodeState,
    BaseSenseNode, Signal,
)


class FlakySensor(BaseSenseNode):
    node_id = "FlakySensor"
    signal_kind = "flaky"
    fail_every = 5
    counter = 0

    async def read(self) -> Signal:
        self.counter += 1
        if self.counter % self.fail_every == 0:
            raise RuntimeError("Sensor exploded!")
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=self.counter,
            confidence=1.0,
            timestamp=time.monotonic(),
        )


async def main() -> None:
    bus = SignalBus()
    sensor = FlakySensor(bus=bus)

    supervisor = NodeSupervisor(
        bus=bus,
        supervisor_id="main",
        restart_policy=RestartPolicy.ON_FAILURE,
        max_restarts=10,
        restart_delay_s=0.2,
    )
    supervisor.track(sensor)

    # Manually drive the sensor (simulating a tick loop)
    for i in range(20):
        try:
            sig = await sensor.read()
            print(f"  tick {i}: ok ({sig.value})")
        except Exception as exc:
            print(f"  tick {i}: FAULT — {exc}")
            await supervisor.on_fault(sensor.node_id, exc)
        await asyncio.sleep(0.1)

    print("\nFinal state:", supervisor.state_of(sensor.node_id))
    print("Healthy?    ", supervisor.is_healthy())


if __name__ == "__main__":
    asyncio.run(main())
```

In a real program, the runtime would call `on_fault` for you when a node
raises. The example drives it manually so you can see the lifecycle clearly.

## HealthMonitor

Once you have one or more supervisors, you can wrap them in a
`HealthMonitor` to get a single "is the whole agent healthy?" answer:

```python
from arachnite import HealthMonitor, NodeState

monitor = HealthMonitor(supervisors=[supervisor])

if not monitor.system_healthy():
    dead = monitor.nodes_in_state(NodeState.DEAD)
    print("Dead nodes:", dead)
```

`HealthMonitor` is what you'd connect to a `/health` HTTP endpoint, a
heartbeat signal, or a paging system. It's also used by distributed agents
to share health across the mesh.

## Reacting to faults from a reflex

The supervisor publishes a `SupervisorSignal` (and `NodeFaultSignal`) onto
the bus whenever a node's state changes. That means you can write a *reflex*
that reacts to crashes:

```python
from arachnite import BaseReflexInstinctNode

class CrashReflex(BaseReflexInstinctNode):
    node_id = "CrashReflex"
    priority = 230

    async def evaluate(self, ctx) -> Proposal | None:
        faults = [s for s in ctx.signals if s.kind == "node_fault"]
        if faults:
            return Proposal(
                instinct_id=self.node_id,
                action_id="NotifyOps",
                priority=self.priority,
                urgency=1.0,
                rationale=f"{len(faults)} node(s) faulted",
            )
        return None
```

Now your agent treats its own crashes as just another signal it can react to.

## Tips

- **Don't supervise every node.** Reserve supervision for nodes that touch
  unreliable resources (hardware, network, files). Pure-logic nodes don't
  need it.
- **Pick `restart_delay_s` carefully.** Too small → crash loops eat your
  CPU. Too large → an outage takes ages to recover. 1–5 seconds is a good
  default.
- **`max_restarts` is a circuit breaker.** Once a node hits it, the
  supervisor stops trying. That's intentional — flapping forever is worse
  than dying loudly.

## What's next?

Now that crashes are handled, let's look at how an agent **remembers** —
using the context's history and persistent state.

[← Multi-Step Actions](01_multi_step_actions.md) | [Next: Smarter Context →](03_smarter_context.md)
