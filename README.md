# Arachnite

A biologically-inspired reactive agent framework for Python.

The architecture models the nervous system of arachnids — `sense → context →
reflex → instinct → decide → act`. Developers extend abstract base classes
to build agents that run on edge devices (Raspberry Pi, Jetson Nano), laptops,
or cloud servers, all connected by a pluggable transport layer.

- **Async-first**: every node interface is `asyncio`-native
- **Typed**: strict type annotations throughout (mypy strict)
- **Pluggable transports**: in-process, MQTT, NATS, Redis
- **Distributed by manifest**: declarative multi-device deployment
- **Reflex co-location**: safety-critical reflexes are validated at deploy time
- **Python 3.10+**

## Install

```bash
pip install arachnite
```

With optional extras:

```bash
pip install "arachnite[all]"            # every optional dependency
pip install "arachnite[mqtt]"           # MQTT transport
pip install "arachnite[nats]"           # NATS transport
pip install "arachnite[redis]"          # Redis transport
pip install "arachnite[web]"            # bundled signal dashboard
pip install "arachnite[llm]"            # Anthropic LLM provider
pip install "arachnite[benchmarks]"     # psutil for RSS measurement
```

Development install from source:

```bash
git clone https://github.com/memrecolak/arachnite-oss.git arachnite
cd arachnite
pip install -e ".[all,dev]"
```

## Quick start

A minimal agent: a sensor that reads temperature, an instinct that fires
when it gets hot, and an action that cools things down.

```python
import asyncio
import time

from arachnite import (
    BaseActionNode,
    BaseInstinctNode,
    BaseSenseNode,
    Proposal,
    Result,
    RuntimeBuilder,
    Signal,
)


class TempSense(BaseSenseNode):
    node_id = "TempSense"
    signal_kind = "temperature"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id,
            kind=self.signal_kind,
            value=42.0,
            confidence=1.0,
            timestamp=time.monotonic(),
        )


class HotInstinct(BaseInstinctNode):
    node_id = "HotInstinct"
    priority = 80

    async def evaluate(self, ctx) -> Proposal | None:
        hot = [s for s in ctx.signals if s.kind == "temperature" and s.value > 40.0]
        if hot:
            return Proposal(
                instinct_id=self.node_id,
                action_id="CoolDown",
                priority=self.priority,
                urgency=0.9,
            )
        return None


class CoolDown(BaseActionNode):
    node_id = "CoolDown"

    async def execute(self, proposal) -> Result:
        print(f"Cooling down! params={proposal.parameters}")
        return Result(action_id=self.node_id, success=True)


async def main() -> None:
    rt = (
        RuntimeBuilder()
        .sense(TempSense)
        .instinct(HotInstinct)
        .action(CoolDown)
        .tick_rate(5.0)
        .build()
    )
    await rt.start()
    await asyncio.sleep(5.0)
    await rt.stop()


asyncio.run(main())
```

See the [`examples/`](examples) directory for more complete programs:
reflex nodes, multi-step actions, supervisor restart policies, and a
web dashboard.

## Documentation

- [`tutorials/`](tutorials) — step-by-step lessons starting at
  [tutorials/01_welcome.md](tutorials/01_welcome.md). Advanced topics
  (multi-step actions, supervisors, distributed deployment, LLM instincts,
  active inference, safety monitors) live under
  [`tutorials/advanced/`](tutorials/advanced).
- [`spec/`](spec) — the formal framework specification, eight numbered
  sections covering architecture, nodes, runtime, distributed deployment,
  infrastructure, and the benchmark suite.

## Core concepts

### Nodes

Five node families, each with an abstract base and a master that owns the
registered instances:

| Node          | Returns      | When to use                                 |
| ------------- | ------------ | ------------------------------------------- |
| `BaseSenseNode`     | `Signal`        | Read hardware/state, emit one signal per tick |
| `BaseInstinctNode`  | `Proposal` or `None` | Evaluate context, propose an action     |
| `BaseReflexInstinctNode` | `Proposal` or `None` | Same as instinct, but bypasses the decision layer (priority ≥ 200, co-located with target action) |
| `BaseDecisionNode`  | `Decision`      | Pick which proposal to execute (Greedy / Weighted / Random / ActiveInference built-ins, or your own) |
| `BaseActionNode`    | `Result`        | Carry out the work — must always return, never raise |
| `MultiStepActionNode` | `Result`      | Long-running actions with interrupt/rollback policies |

### Priority convention

- **200+** — reflex instincts only
- **100–199** — safety / survival
- **50–99** — goal-directed
- **1–49** — exploratory / maintenance
- **0** — reserved (inactive)

### Architectural rules

1. Nodes never hold references to each other; communication goes through `SignalBus`.
2. `ReflexInstinctNode` and its target `ActionNode` must be on the same `AgentNode`.
3. `MultiStepActionNode` mandatory blocks cannot be interrupted (except `emergency_stop`).
4. `execute()` on any `ActionNode` must always return a `Result` — never raise.
5. `evaluate()` on any `InstinctNode` must return `None` when not applicable — never raise.
6. All node I/O must be async — wrap blocking hardware calls in `asyncio.to_thread()`.

## Distributed deployments

A `DeploymentManifest` declares which nodes run on which `AgentNode`. The
manifest validator enforces co-location rules and fails loudly on missing
environment variables.

```yaml
agents:
  vision:
    transport: nats
    transport_url: ${NATS_URL}
    nodes:
      - ProximitySense
      - ObjectDetectionSense
  control:
    transport: nats
    transport_url: ${NATS_URL}
    nodes:
      - JointPositionSense
      - CollisionReflex     # priority 250, reflex
      - EmergencyRetract    # co-located target
      - GraspInstinct
      - PickAndPlace
```

See [`examples/robot_arm/`](examples/robot_arm) for a runnable two-agent
case study.

## Benchmarks

A reproducible benchmark suite ships under [`benchmarks/`](benchmarks):

```bash
# Full suite (30 runs, JSON output)
python benchmarks/suite.py

# Quick run (5 runs)
python benchmarks/suite.py --runs 5

# Individual benchmarks
python benchmarks/tick_latency.py
python benchmarks/reflex_latency.py
python benchmarks/scalability_sweep.py
python benchmarks/transport_latency.py
```

Benchmarks include tick latency, per-stage breakdown, reflex arc timing,
memory footprint, scalability sweeps, multi-step action interrupt latency,
long-horizon stability soak, and transport publish-to-deliver latency.
All emit JSON with bootstrap CIs for median / P95 / P99.

### Cross-framework comparison (optional)

[`baselines/`](baselines) holds comparison harnesses against
[py_trees](https://github.com/splintered-reality/py_trees),
[ROS 2](https://docs.ros.org/), and the [Jason](https://jason-lang.github.io/)
AgentSpeak BDI engine. **These are not part of the framework** — they are
not installed by `pip install arachnite` and are excluded from the wheel.
They require external toolchains (JVM, ROS 2) to run. See
[`baselines/README.md`](baselines/README.md) for setup.

## Development

```bash
# Run all tests
pytest

# With coverage
pytest --cov=arachnite --cov-report=term-missing

# Type check
mypy arachnite

# Lint
ruff check arachnite tests benchmarks
```

## License

MIT — see [LICENSE](LICENSE).
