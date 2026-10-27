<!-- Arachnite SPEC §10–§11 -->

# __10\. Distributed Deployment__

Arachnite supports deployment across multiple physical devices and cloud environments\. A Jetson Nano, a Raspberry Pi, a laptop, and a cloud inference server can all participate in the same agent — each running a subset of nodes that suits its hardware profile — with the SignalBus providing transparent signal delivery across all of them\.

The core principle is that the Transport layer sits beneath the SignalBus\. Every node continues to call bus\.publish\(\) and bus\.subscribe\(\) exactly as before\. The transport handles serialisation and delivery, whether that means an in\-process queue or an MQTT broker on the other side of a WiFi link\.

> **How the bridge works:** `SignalBus(transport=transport, agent_node_id=…)` attaches a transport\. `publish()` then dispatches to local subscribers **and** forwards the signal to the transport; signals arriving from the transport are dispatched to local subscribers\. Nodes keep calling `publish()`/`subscribe()` unchanged\. After the transport is connected, the bus must subscribe its inbound handler for each locally\-subscribed kind — call `await bus.start_transport()` once; `AgentNode.start()` does this automatically\. Loop/echo prevention: each forwarded signal is tagged with its originating `agent_node_id`, and the inbound handler drops messages that originated from this same agent \(so a broker echo, or a shared `LocalTransport`, never double\-delivers\)\. The bus's `agent_node_id` defaults to the transport's `agent_node_id`, so it must be unique per AgentNode across the mesh \(two buses with the same id drop each other's signals as echoes\)\. Transport interest follows local interest: a kind's inbound handler is subscribed on the transport while the kind has a local subscriber and withdrawn when the last one unsubscribes; when `'*'` is wanted only `'*'` is subscribed, so each remote signal arrives once; a transport subscription that fails is logged and retried with backoff\. `AgentNode.stop()` calls `bus.stop_transport()`, which withdraws the bus's subscriptions \(a transport shared with other buses stops delivering to it\)\. Remote signals also reach the receiving agent's **tick**: see §10\.5\. Note: every published signal is currently forwarded to the transport regardless of whether a remote agent has subscribed to that kind — the publish\-side "transmit only subscribed kinds" filtering \(spec §11\) is not yet implemented\.

## __10\.1 Deployment Patterns__

__Pattern__

__Typical Hardware__

__Recommended Transport__

Edge\-only

Jetson Nano, Raspberry Pi, industrial PC

LocalTransport \(default\)

Edge \+ Cloud

Pi for sensing/actuation, cloud LLM for reasoning

MQTTTransport on edge, NATSTransport on cloud

Multi\-edge mesh

Pi body \+ Jetson vision \+ cloud brain

MQTTTransport throughout, broker on local network

Laptop \+ Pi

Laptop for decision/instinct, Pi for sense/action

NATSTransport or RedisTransport over LAN

## __10\.2 Transport Layer__

The Transport abstraction decouples the SignalBus from any specific delivery mechanism\. Swapping transports requires no changes to node code\.

### __BaseTransport__

class BaseTransport\(ABC\):

    """Pluggable delivery backend for the SignalBus\.

    Handles serialisation, network delivery, and deserialization\.

    All methods are async\."""

    @abstractmethod

    async def connect\(self\) \-> None:

        """Establish connection to the transport backend\.

        Called once during runtime startup\."""

    @abstractmethod

    async def disconnect\(self\) \-> None:

        """Close the connection cleanly\."""

    @abstractmethod

    async def publish\(self, signal: Signal\) \-> None:

        """Serialise and deliver a signal to all subscribers

        of signal\.kind, including those on remote AgentNodes\."""

    @abstractmethod

    async def subscribe\(self, kind: str,

                        callback: Callable\[\[Signal\], Awaitable\[None\]\]\) \-> None:

        """Register an async callback for signals of the given kind\.

        The callback is invoked for both local and remote signals\."""

    @abstractmethod

    async def unsubscribe\(self, kind: str, callback: Callable\) \-> None: \.\.\.

### __Built\-in Transports__

__Transport__

__Characteristics__

__LocalTransport__

Default\. In\-memory asyncio queues\. Zero overhead\. Single process only\. No configuration required\.

__MQTTTransport__

Lightweight pub/sub over TCP\. Ideal for constrained edge devices\. Requires an MQTT broker \(e\.g\. Mosquitto\)\. Supports QoS levels 0, 1, 2\. Uses aiomqtt\.

__NATSTransport__

High\-throughput, low\-latency messaging\. Best for cloud nodes and laptop\-class hardware\. Supports JetStream persistence\. Uses nats\-py\.

__RedisTransport__

Pub/sub backed by Redis Streams\. Good when Redis is already in the deployment stack\. Uses ``redis\.asyncio`` from ``redis-py`` >= 5\.0 \(the legacy ``aioredis`` package was abandoned in 2022 and is unusable on Python >= 3\.12\)\.

### __Configuring a Transport__

from arachnite\.transport\.mqtt import MQTTTransport

from arachnite import SignalBus

transport = MQTTTransport\(

    broker\_host='192\.168\.1\.10',

    broker\_port=1883,

    agent\_node\_id='pi\-body',     \# unique name for this device

    topic\_prefix='arachnite/',   \# all signals published under this prefix

    qos=1,                       \# at\-least\-once delivery

    reconnect\_interval\_s=2\.0,

    tls=True,                    \# enable TLS \(creates a default ssl\.SSLContext\)

\)

bus = SignalBus\(transport=transport, agent_node_id='pi\-body'\)

\# All nodes using this bus now communicate over MQTT transparently\.
\# \(AgentNode wires this for you; standalone, call `await bus\.start_transport\(\)` after transport\.connect\(\)\.\)

## __10\.3 AgentNode__

An AgentNode represents a single deployment unit — one device, one process, or one cloud service\. It has a unique name, a transport configuration, and owns an ArachniteRuntime\. In a distributed deployment, each physical device runs one AgentNode\.

class AgentNode:

    node\_id: str              \# unique name, e\.g\. 'jetson\-vision', 'pi\-body'

    transport: BaseTransport

    runtime: ArachniteRuntime

    tags: list\[str\]           \# optional labels, e\.g\. \['edge', 'gpu', 'arm64'\]

    async def start\(self\) \-> None:

        """Connect transport, start supervisor, start runtime loop\."""

    async def stop\(self\) \-> None:

        """Stop runtime, disconnect transport cleanly\."""

    @property

    def health\(self\) \-> HealthMonitor: \.\.\.

## __10\.4 The Co\-location Constraint__

Reflex instinct nodes must be co\-located with the action nodes they trigger\. A reflex that detects a collision on a Raspberry Pi but dispatches to an EmergencyStopActionNode running on a cloud server would have its response time dominated by network latency — defeating the entire purpose of the reflex arc\.

Arachnite enforces this at startup via the co\-location validator\. When a DeploymentManifest is loaded, the validator checks every ReflexInstinctNode against its target action\_id \(the manifest ``action\_id`` key\) and raises ManifestValidationError — carrying a CoLocationError message — if the action node is assigned to a different AgentNode, or if the reflex declares no action\_id or names an action that no AgentNode registers\.

\# Co\-location rule \(enforced at manifest load time\):

\#

\# For every ReflexInstinctNode R on AgentNode A:

\#   The ActionNode whose node\_id == R\.action\_id

\#   MUST also be assigned to AgentNode A\.

\#

\# Normal InstinctNodes have no co\-location constraint on their

\# inputs — remote signals enter their ctx\.signals \(§10\.5\)\. Their

\# proposals, like every proposal, are dispatched to the LOCAL

\# ActionMasterNode, so the action a proposal names must be on the

\# same AgentNode as the instinct \(proposals do not cross the bus\)\.

The validator matches node **ids**: a reflex's ``action\_id`` is compared with each action's ``node\_id`` \(the class attribute, which is what ``Proposal\.action\_id`` must equal\), never with its class name; the duplicate check uses node\_ids too\. Validation runs when the manifest is loaded \(``from\_dict``/``from\_yaml``\) and again in ``AgentNode\.from\_manifest`` and ``MeshRuntime\.start``, so a violating manifest never starts\.

The constraint covers the reflex's **inputs** as well, but the validator checks only the reflex → action edge\. A reflex in the tick's reflex stage sees ``ctx\.signals``; a sense signal from another AgentNode enters them only at the receiving agent's **next** tick \(§10\.5\), after crossing the network\. In ``reflex\_mode='event'`` the reflex lane subscribes on the bus and also receives transport\-delivered signals, but then network latency is on the reflex path\. The SenseNodes that feed a reflex MUST therefore be assigned to the reflex's AgentNode — a deployment rule the manifest author is responsible for\.

## __10\.5 Signal Routing__

When a signal is published on one AgentNode, the transport delivers it to all AgentNodes that have a subscriber for that signal kind\. Each AgentNode filters signals and delivers only those relevant to its own nodes\. Signal serialisation uses msgpack by default for compact binary encoding; JSON is available as a fallback for debuggability\. Dicts with int/float keys \(in the value or the metadata\) round\-trip; msgpack's str\-only key default is not used on decode\.

__Wildcard and delivery semantics \(every transport\)\.__ Kind ``'*'`` receives every signal: MQTT subscribes ``{topic\_prefix}#``, NATS ``{subject\_prefix}\.>`` \(kinds containing dots included\), Redis ``PSUBSCRIBE {channel\_prefix}:\*``\. A callback subscribed under both a kind and ``'*'`` is called **once** per signal, and a raising callback is logged without stopping delivery to the others — on ``LocalTransport`` too, which also iterates a snapshot so a callback that unsubscribes itself does not make the next one be skipped\.

__Remote signals in the receiving agent's tick\.__ ``AgentNode`` attaches a remote listener to its bus \(``SignalBus\.add\_remote\_listener``\); while one is attached the bus subscribes ``'*'`` on the transport, so every kind any peer publishes is received\. Each signal that arrives from another AgentNode \(never a local one — the listener only sees transport\-delivered signals that pass echo suppression\) is handed to ``ArachniteRuntime\.inject\_remote\_signal`` and drained into the **next** tick's ``ctx\.signals``, after the local sense output\. The buffer holds 1024 signals; when more arrive between two ticks the oldest are dropped with a warning at the tick\. ``supervisor``/``node\_fault`` signals are not injected a second time \(the runtime's own bus subscriptions already buffer them, local or remote\), and a remote ``state\_update`` is never applied: ``ctx\.state`` is per agent, so a peer's ``StateUpdateSignal`` reaches bus subscribers but does not write this agent's state\. Event\-lane reflexes see remote signals through their bus subscription as before\. Restrict what a peer sends with its ``wire\_kinds`` egress allowlist \(below\)\. A bus built by hand gets the same behaviour with ``bus\.add\_remote\_listener\(runtime\.inject\_remote\_signal\)``\.

\# Signal envelope on the wire \(msgpack\-encoded\):

\{

    'v':  1,                    \# protocol version

    'src': 'jetson\-vision',     \# originating AgentNode

    'sig': \{                    \# serialised Signal fields

        'source': 'CameraSenseNode',

        'kind':   'visual',

        'value':  '<bytes>',    \# node\-defined encoding

        'confidence': 0\.97,

        'timestamp':  1234567\.89,

        'metadata':   \{\.\.\.\},

        'type': 'NodeFaultSignal',  \# optional: Signal subclass, see below

        'ext':  \{\.\.\.\}             \# optional: that subclass's fields

    \}

\}

``type``/``ext`` carry the fields of ``SupervisorSignal`` \(node\_id, previous/current state, restart\_count\), ``NodeFaultSignal`` \(those plus error\_type/error\_message; the exception object itself does not cross\) and ``StateUpdateSignal`` \(key, codec\-encoded state\_value, delete\), so the receiver rebuilds the same subclass\. A user subclass travels as its nearest framework ancestor \(or as a plain ``Signal``\)\. Decoders that predate these keys ignore them\.

### __Keeping Signal Kinds Local \(Egress Allowlist\)__

By default, attaching a transport forwards __every__ published kind onto the wire — so any signal a bus publishes reaches the broker, with the privacy and bandwidth cost that implies\. Declining to subscribe to a kind on other AgentNodes only stops it from being _consumed_; it is still serialised and published to the broker topic on the sender\. Locality must therefore be enforced at the publisher\.

SignalBus accepts a `wire_kinds` egress allowlist for this:

\# Only 'telemetry' and 'alert' leave this device; everything else stays local\.

bus = SignalBus\(

    transport=transport,

    agent_node_id='pi\-body',

    wire_kinds=\{'telemetry', 'alert'\},

\)

\- `wire_kinds=None` \(the default\) forwards every kind — the historical publish\-everything behaviour\.

\- A set restricts egress to those kinds; `'*'` is a catch\-all meaning "forward everything"\.

\- An empty set forwards nothing — a transport attached for inbound delivery only\.

\- The allowlist is __egress\-only__: inbound delivery is unaffected, so an AgentNode can still receive a kind it chooses not to publish onto the wire\.

The same allowlist is configurable declaratively per agent in a DeploymentManifest, so you need not construct the `SignalBus` by hand:

agents:

  \- id: pi\-body

    wire\_kinds: \['telemetry', 'alert'\]   \# only these leave this device

`AgentNode.from_manifest` threads the parsed set through to the agent's `SignalBus`\. Omitting the `wire_kinds` key \(or setting it null\) forwards every kind; an empty list forwards nothing; a non\-list value raises `ManifestValidationError`\.

__Recommended\.__ In privacy\- or bandwidth\-sensitive deployments, prefer an explicit `wire_kinds` allowlist over relying on remote nodes simply not subscribing\. In a typical agent the set of kinds that must cross the mesh is far smaller than the internal chatter \(ticks, debug, intermediate proposals\), so an allowlist is both less error\-prone — you enumerate the small set that leaves rather than remembering to exclude the large set that stays — and auditable: one set tells you exactly what leaves the device\.

## __10\.6 Distributed Supervisor Health__

SupervisorSignals flow over the transport like any other signal\. This means a NodeFaultReflex on one AgentNode can react to a node failure on a different AgentNode\. The HealthMonitor on each AgentNode receives RemoteNodeState updates from other agents via the bus and maintains a mesh\-wide health picture: ``AgentNode``'s remote listener \(§10\.5\) calls ``HealthMonitor\.update\_remote\(\)`` for every remote ``SupervisorSignal``/``NodeFaultSignal``, keyed by the originating agent \(``metadata\['\_\_origin\_agent\_\_'\]``\) and the signal's ``node\_id``/``current\_state``\.

@dataclass

class RemoteNodeState:

    agent\_node\_id: str       \# which AgentNode owns this node

    node\_id: str             \# the node that changed state

    state: NodeState

    timestamp: float

\# HealthMonitor extended interface for distributed deployments:

class HealthMonitor:

    def mesh\_healthy\(self\) \-> bool:

        """True if all known AgentNodes report system\_healthy\(\)\.

        Only meaningful when a non\-local transport is in use\."""

    def remote\_states\(self\) \-> dict\[str, dict\[str, NodeState\]\]:

        """Returns \{agent\_node\_id: \{node\_id: NodeState\}\} for

        all AgentNodes heard from on the bus\."""

# __11\. Deployment Manifest__

The DeploymentManifest is a declarative description of a distributed Arachnite deployment\. It specifies which nodes run on which AgentNode, what transport each AgentNode uses, and what constraints must hold \(such as reflex co\-location\)\. The manifest is written in YAML and loaded at startup\.

The manifest serves two purposes: it is the authoritative configuration for a deployment, and it is the input to the co\-location validator and the MeshRuntime assembler\.

## __11\.1 Manifest Schema__

\# arachnite\-manifest\.yaml

version: '1'

mesh:

  name: 'greenhouse\-robot'

  transport\_default: mqtt          \# used if agent doesn't specify

  mqtt\_broker: '192\.168\.1\.10:1883' \# default broker for mqtt agents \('host' or 'host:port'\)

  nats\_server: 'nats://cloud\.example\.com:4222'   \# default servers for nats agents

  redis\_url: 'redis://cache\.local:6379'         \# default url for redis agents

agents:

  \- id: pi\-body

    description: 'Raspberry Pi 4 — physical body, sensors, motors'

    transport: mqtt

    tags: \[edge, arm64\]

    tick\_rate\_hz: 20

    wire\_kinds: \[alert, remote\_command\]   \# egress allowlist; omit to forward every kind

    nodes:

      sense:

        \- TemperatureSenseNode

        \- ProximitySenseNode

        \- HumiditySenseNode

      instinct:

        \- kind: EmergencyStopReflex    \# ReflexInstinctNode

          reflex: true

          action\_id: EmergencyStopActionNode   \# required for reflexes

                                               \# \(co\-location target\)

        \- kind: OverheatInstinct

      action:

        \- EmergencyStopActionNode      \# co\-located with reflex above

        \- SetTemperatureActionNode

        \- MoveActionNode

  \- id: jetson\-vision

    description: 'Jetson Nano — camera, object detection'

    transport: mqtt

    tags: \[edge, gpu, arm64\]

    tick\_rate\_hz: 30

    nodes:

      sense:

        \- CameraSenseNode

        \- ObjectDetectionSenseNode     \# wraps a local GPU model

      instinct: \[\]

      action: \[\]

  \- id: cloud\-brain

    description: 'Cloud VM — LLM reasoning, high\-level goals'

    transport: nats

    tags: \[cloud, x86\_64\]

    tick\_rate\_hz: 2                   \# slower — LLM calls are expensive

    nodes:

      sense: \[\]

      instinct:

        \- kind: LLMGoalInstinct        \# calls remote LLM API

        \- kind: NavigationInstinct

      decision:

        \- WeightedDecisionNode

      action:

        \- SpeakActionNode              \# text\-to\-speech on cloud

        \- LogActionNode

Per agent, ``transport\_config`` overrides the mesh defaults and accepts exactly these keys \(any other key is a ``ManifestValidationError``; values may use ``$\{VAR\}`` interpolation\):

\- ``mqtt``: broker\_host, broker\_port, topic\_prefix, qos, username, password, tls, reconnect\_interval\_s, max\_reconnect\_attempts

\- ``nats``: servers, subject\_prefix, reconnect\_interval\_s, max\_reconnect\_attempts

\- ``redis``: url, channel\_prefix, db, password, reconnect\_interval\_s, max\_reconnect\_attempts

\- ``local``: none \(the section is ignored\)

__Node entries\.__ A node entry is a class path, or a mapping with the keys ``kind``, ``config``, ``reflex``, ``action\_id`` and ``permissions``; any other key \(e\.g\. ``priority``\) is rejected with ``ManifestValidationError`` — node attributes such as ``priority`` are set on the class\. A node is treated as a reflex — and checked for co\-location with its ``action\_id`` target — if the entry says ``reflex: true``, if its class subclasses ``BaseReflexInstinctNode``, or if its class sets ``reflex = True`` \(the rule the runtime routes by\)\.


## __11\.2 Loading and Validating a Manifest__

from arachnite\.distributed import DeploymentManifest, MeshRuntime

manifest = DeploymentManifest\.from\_yaml\('arachnite\-manifest\.yaml'\)

\# Loading validates \(and raises ManifestValidationError\); validate\(\) runs

\# again in AgentNode\.from\_manifest\(\) and MeshRuntime\.start\(\):

\#   \- all referenced node classes are importable

\#   \- node sections are sense/instinct/decision/action, and each class

\#     extends its section's base class

\#   \- every reflex node declares its target action\_id and is co\-located

\#     with the action whose node\_id it names \(the sense nodes feeding

\#     it are not checked\)

\#   \- no duplicate node\_ids within an AgentNode

\#   \- declared permissions are within each node's whitelist \(§11\.3\.1\)

\#   \- transport kinds are known and transport\_config keys are valid;

\#     on\_transport\_fault \(not implemented, §11\.6\) is rejected

manifest\.validate\(\)   \# explicit re\-check, e\.g\. after building one by hand

\# Build and start the full mesh \(runs on a coordinator machine\)

mesh = MeshRuntime\(manifest\)

await mesh\.start\(\)    \# starts each AgentNode in its own process/thread

\# Or start just one AgentNode on the current machine:

agent = AgentNode\.from\_manifest\(manifest, 'pi\-body'\)

await agent\.start\(\)

## __11\.3 NodeAssignment__

Each entry in the manifest nodes section produces a NodeAssignment — the internal binding between a node class, its configuration, and its owning AgentNode\.

@dataclass

class NodeAssignment:

    node\_class: type\[BaseNode\]

    agent\_node\_id: str

    config: dict              \# passed to node constructor as kwargs

    is\_reflex: bool = False

    reflex\_action\_id: str | None = None  \# manifest action\_id of a reflex node

    @property

    def node\_id\(self\) \-> str: \.\.\.     \# node\_class\.node\_id \(not the class name\)

### __11\.3\.1 Permission Whitelist__

Nodes may declare the capabilities they require via a `permissions` class attribute on `BaseNode`\. The manifest or `ArachniteRuntime` configuration defines the allowed permissions per node\. Validation is startup\-only — zero runtime cost\.

__Permission enum values:__ `NETWORK`, `FILESYSTEM_READ`, `FILESYSTEM_WRITE`, `SUBPROCESS`, `GPU`\.

__Declaration \(on the node class\):__

```
class MyNetworkSense\(BaseSenseNode\):
    permissions = \{Permission\.NETWORK\}
```

__Manifest syntax \(allowed whitelist per node\):__

```yaml
nodes:
  sense:
    \- kind: myapp\.nodes\.MyNetworkSense
      permissions: \[network\]
```

__Validation rules:__

\- If `permissions` key is absent from a node definition: no restriction \(backward compatible\)\.
\- If `permissions: \[\]`: the node must declare zero permissions\.
\- If a node declares a permission not in its allowed set: `ManifestValidationError` at manifest validation time, or `PermissionValidationError` at runtime `start\(\)`\.
\- Nodes not listed in the allowed map are unrestricted\.

__Programmatic path:__ `ArachniteRuntime\(allowed\_permissions=\{"NodeId": \{Permission\.NETWORK\}\}\)`\.

## __11\.4 MeshRuntime__

MeshRuntime coordinates a multi\-agent deployment from a single entry point\. It reads the manifest, builds all AgentNodes, and starts them in the current process as asyncio tasks; agents meant for other devices are started there independently \(``AgentNode\.from\_manifest``\) and meet over the transport layer\.

class MeshRuntime:

    def \_\_init\_\_\(self, manifest: DeploymentManifest\): \.\.\.

    async def start\(self\) \-> None:

        """Validate the manifest, then start all AgentNodes in it\.

        Agents with transport 'local' \(and no override\) share one

        in\-process LocalTransport, so they exchange signals\. If any

        agent fails to start, every agent of the attempt is stopped

        and the first error is raised \(a retry starts clean\)\.

        Local agents start as asyncio tasks\.

        Remote agents are expected to be started independently

        and connect via the configured transport\."""

    async def stop\(self\) \-> None:

        """Stop all local AgentNodes and disconnect transports\."""

    def agent\(self, agent\_node\_id: str\) \-> AgentNode: \.\.\.

    def mesh\_health\(self\) \-> dict\[str, bool\]:

        """Returns \{agent\_node\_id: is\_healthy\} for all known agents\.

        Remote agent health is inferred from their SupervisorSignals\."""

## __11\.5 Deployment Topology Examples__

The examples below are abbreviated: each reflex entry also needs the ``action\_id`` key shown in §11\.1, and every reflex shares its AgentNode with its action and with the sense nodes that feed it \(§10\.4\)\.

### __Single Edge Device__

agents:

  \- id: standalone

    transport: local          \# no network, in\-process bus

    tick\_rate\_hz: 10

    nodes:

      sense:    \[TemperatureSenseNode, ProximitySenseNode\]

      instinct: \[OverheatInstinct, EmergencyStopReflex\]

      decision: \[GreedyDecisionNode\]

      action:   \[SetTemperatureActionNode, EmergencyStopActionNode\]

### __Raspberry Pi \+ Cloud LLM__

agents:

  \- id: pi

    transport: mqtt

    tick\_rate\_hz: 20

    nodes:

      sense:    \[TemperatureSenseNode, MicrophoneSenseNode\]

      instinct: \[EmergencyStopReflex\]   \# reflex stays on edge

      action:   \[EmergencyStopActionNode, SpeakerActionNode\]

  \- id: cloud

    transport: nats

    tick\_rate\_hz: 1

    nodes:

      instinct: \[LLMConversationInstinct, LLMTaskInstinct\]

      decision: \[WeightedDecisionNode\]

      action:   \[LogActionNode\]

### __Jetson \+ Pi \+ Cloud \(Full Mesh\)__

agents:

  \- id: jetson

    transport: mqtt

    tick\_rate\_hz: 30

    nodes:

      sense: \[CameraSenseNode, ObjectDetectionSenseNode\]

  \- id: pi

    transport: mqtt

    tick\_rate\_hz: 20

    nodes:

      sense:    \[ProximitySenseNode, IMUSenseNode\]

      instinct: \[CollisionReflex, ObstacleReflex\]

      action:   \[EmergencyStopActionNode, MoveActionNode\]

  \- id: cloud

    transport: nats

    tick\_rate\_hz: 2

    nodes:

      instinct: \[NavigationInstinct, LLMGoalInstinct\]

      decision: \[WeightedDecisionNode\]

      action:   \[SpeakActionNode\]

## __11\.6 Network Failure Handling__

When a transport loses connectivity, the affected AgentNode cannot receive signals from the mesh\. Transports are **not** supervised: no NodeSupervisor tracks a transport, and there is no built\-in TransportFaultReflex\. An edge device that must not keep operating on stale remote data needs a node of its own for that \(e\.g\. an instinct that watches the age of the last remote signal in ``ctx\.signals``\)\. The ``on\_transport\_fault`` manifest key that earlier drafts described is rejected by ``validate\(\)`` rather than silently ignored\.

__Reconnect vs\. intentional disconnect\.__ MQTTTransport and RedisTransport maintain automatic reconnection on connection loss\. An internal `\_stopped` flag distinguishes intentional disconnects \(via `disconnect\(\)`\) from unexpected connection loss\. When `disconnect\(\)` is called, the flag is set and a pending or running reconnect task is cancelled, so the transport stays down; a (re)connect attempt re\-checks the flag before each try\. `connect\(\)` clears the flag\. `connect\(\)` itself retries up to `max\_reconnect\_attempts` times, `reconnect\_interval\_s` apart, then raises `TransportConnectionError`; RedisTransport verifies the server with a `PING` \(``from\_url\(\)`` is lazy\)\. NATSTransport relies on nats\-py's own reconnect; after a `disconnect\(\)`/`connect\(\)` cycle it subscribes every registered kind again on the new connection\.

\# NOT SUPPORTED — rejected by validate\(\) \(kept to document the key\):

agents:

  \- id: pi

    transport: mqtt

    on\_transport\_fault:

      reflex: TransportFaultReflex   \# fires immediately on disconnect

      action: SafeModeActionNode     \# must be co\-located

      reconnect\_policy: exponential\_backoff

      max\_reconnect\_attempts: 10

