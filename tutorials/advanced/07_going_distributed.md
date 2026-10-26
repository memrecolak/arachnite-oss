# Advanced 7 — Going Distributed

So far, every agent in this course has been **one Python process** on **one
machine**. That works for many things — a smart fan, a desk lamp, a small
robot. But as soon as you have:

- Sensors on a Raspberry Pi but the brains in the cloud,
- Two robots that need to coordinate,
- A dashboard that watches a fleet of edge devices,

…you need more than one process. That's where Arachnite's distributed layer
comes in.

## The big idea

A distributed Arachnite system is just **many `AgentNode` instances**,
connected by a **transport** (a message bus that crosses machines). Each
`AgentNode` runs its own `ArachniteRuntime` — its own tick loop, sensors,
instincts, actions. They share signals over the transport, so an instinct on
one machine can react to a sensor on another.

```
   ┌─ AgentNode "edge-1" ─┐         ┌─ AgentNode "cloud-brain" ─┐
   │  Pi: temperature      │         │  Server: ML model         │
   │  sensor               │  MQTT   │  decision strategy        │
   │                       │ ──────► │                           │
   │  publishes signal     │         │  publishes proposal back  │
   └───────────────────────┘         └───────────────────────────┘
```

There's no master-slave hierarchy. Every `AgentNode` is a peer. The
transport is just a shared bus.

## AgentNode

`AgentNode` is the wrapper around a single runtime, plus a transport:

```python
from arachnite import ArachniteRuntime
from arachnite.distributed.agent_node import AgentNode
from arachnite.transport.local import LocalTransport

runtime = ArachniteRuntime(...)  # built as you've been doing all along
transport = LocalTransport(agent_node_id="agent-1")

agent = AgentNode(
    node_id="agent-1",
    runtime=runtime,
    transport=transport,
    tags=["edge", "thermal"],
    description="Thermal monitor on Raspberry Pi #1",
)

await agent.start()
# ... runs until ...
await agent.stop()
```

The transport is what makes it distributed. Pick the right one for your
deployment.

## Transports

| Transport | Class | Best for |
|---|---|---|
| Local (default) | `LocalTransport` | Single process, no overhead |
| MQTT | `MQTTTransport` | Edge devices, broker-based pub/sub |
| NATS | `NATSTransport` | Cloud / LAN, low-latency, built-in clustering |
| Redis | `RedisTransport` | When Redis is already in your stack |

All four implement the same `BaseTransport` interface, so swapping one for
another is **one line of code**:

```python
from arachnite.transport.mqtt import MQTTTransport

transport = MQTTTransport(
    broker_host="192.168.1.10",
    broker_port=1883,
    agent_node_id="edge-1",
    topic_prefix="arachnite/",
    qos=1,
)
```

The MQTT, NATS, and Redis transports are **optional dependencies**. Install
them with:

```bash
pip install "arachnite[mqtt]"
pip install "arachnite[nats]"
pip install "arachnite[redis]"
```

## Deployment manifests

Hardcoding a multi-machine setup in Python gets messy fast. Arachnite lets
you describe the whole topology in a YAML file — a **deployment manifest**:

```yaml
mesh:
  transport_default: "mqtt"

agents:
  - id: "edge-1"
    description: "Pi with thermal sensor"
    transport: "mqtt"
    tick_rate_hz: 5.0
    tags: ["edge"]
    transport_config:
      broker_host: "${MQTT_HOST:-localhost}"
      broker_port: 1883
      topic_prefix: "arachnite/"
      qos: 1
    nodes:
      sense:
        - kind: "myapp.sensors.ThermalSensor"
          config:
            pin: 17
            threshold: 40

  - id: "cloud-brain"
    description: "Decision-making + alerting"
    transport: "mqtt"
    tick_rate_hz: 10.0
    transport_config:
      broker_host: "${MQTT_HOST:-localhost}"
    nodes:
      instinct:
        - kind: "myapp.instincts.AlertInstinct"
      action:
        - kind: "myapp.actions.SendPagerDuty"
          config:
            api_key: "${PAGERDUTY_KEY}"
```

You then load this manifest and start everything in one go:

```python
from arachnite.distributed.manifest import DeploymentManifest
from arachnite.distributed.mesh import MeshRuntime

manifest = DeploymentManifest.from_yaml("deployment.yaml")
manifest.validate()  # checks env vars, imports, co-location

mesh = MeshRuntime(manifest=manifest)
await mesh.start()
# ... agents run until ...
await mesh.stop()
```

`MeshRuntime` is a launch coordinator. It builds every `AgentNode` in the
manifest, starts them concurrently, and shuts them all down cleanly. It's
especially useful for testing and simulation, where you want to run "fake
edge" + "fake cloud" in one process.

## Keeping signals off the wire

By default, once a bus has a transport, **every** signal it publishes goes to
the broker — even kinds nobody on another machine subscribes to. Not
subscribing elsewhere only stops the signal from being *consumed*; it was still
serialised and sent. For high-frequency internal chatter (ticks, debug,
intermediate proposals) that's wasted bandwidth, and for sensitive data it's a
privacy leak.

To keep a kind local you gate it at the publisher with an egress allowlist.
`SignalBus` takes a `wire_kinds` set — only those kinds leave for the transport;
everything else stays in-process:

```python
from arachnite import SignalBus

# Only 'alert' and 'remote_command' cross the mesh; everything else is local.
bus = SignalBus(
    transport=transport,
    agent_node_id="edge-1",
    wire_kinds={"alert", "remote_command"},
)
```

- `wire_kinds=None` (the default) forwards every kind — unchanged behaviour.
- A set restricts egress to those kinds; `'*'` is a catch-all; an empty set
  forwards nothing (a transport attached for receiving only).
- It's **egress-only**: you still *receive* any kind you subscribe to, even one
  you don't publish onto the wire.

In a manifest, set it per agent — omit it to forward everything:

```yaml
agents:
  - id: "edge-1"
    transport: "mqtt"
    wire_kinds: ["alert", "remote_command"]   # egress allowlist
    nodes:
      # ...
```

Prefer this over hoping remote nodes don't subscribe: the set of kinds that
must cross the mesh is usually far smaller than the internal traffic, so an
allowlist is both less error-prone and auditable — one list tells you exactly
what leaves the device.

## Co-location and reflexes

There's one rule you have to know about distributed agents:

> **A reflex instinct must live on the same `AgentNode` as the action it
> proposes.**

The whole point of a reflex is to react instantly. If the reflex has to send
its proposal across a network to reach its action, the latency defeats the
purpose. The framework enforces this with a **co-location validator** —
`manifest.validate()` will fail if you put a reflex on `edge-1` and its
target action on `cloud-brain`.

The validator checks reflex → action only. Put the **sensor that feeds the
reflex** on the same `AgentNode` as well, so the danger signal never has to
cross the network before the reflex sees it. This matters even more with the
event-driven reflex lane (`reflex_mode="event"`), whose whole point is to
react the moment the signal is published.

Normal instincts have no such restriction. They can fire on one machine and
target an action on another.

## When NOT to go distributed

This deserves its own section. **Single-process is almost always the right
starting point.** A distributed system has more failure modes than a
single-process one — network partitions, broker outages, version skew, race
conditions across hosts. Don't accept that complexity until you actually
need it.

You probably need distribution when:
- Your sensors and actions are physically separated.
- You're processing more data than one machine can handle.
- You need fault isolation (one node crashing shouldn't take down the rest).

You probably don't need distribution when:
- Your "sensors" and "actions" are all in the same Python process.
- You're tempted because it sounds modern.
- You haven't profiled to confirm one machine is the bottleneck.

If in doubt: build the single-process version, then split it later. The
beauty of Arachnite is that splitting is mostly a config change, not a
rewrite.

## Tips

1. **Use `LocalTransport` in tests.** It has no network, no broker, no
   surprises. Switch to MQTT/NATS/Redis only for real deployment.
2. **Validate your manifest in CI.** Catch typos and missing env vars
   before shipping.
3. **Tag your agents.** `tags=["edge", "thermal"]` makes it easy to query
   "give me all edge thermal agents" from the health monitor.
4. **Keep transport configs in env vars, not files.** Especially anything
   with credentials.

## What's next?

We've now covered the entire infrastructure of Arachnite. There's just one
thing left to talk about that's unique to this branch of the framework:
**LLM-backed instincts**. You can plug a language model into the decision
layer and let it propose actions in plain English.

[← Configuration Injection](06_configuration_injection.md) | [Next: LLM Instincts →](08_llm_instincts.md)
