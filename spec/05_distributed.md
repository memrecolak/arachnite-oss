<!-- Arachnite SPEC §10–§11 -->

# __10\. Distributed Deployment__

Arachnite supports deployment across multiple physical devices and cloud environments\. A Jetson Nano, a Raspberry Pi, a laptop, and a cloud inference server can all participate in the same agent — each running a subset of nodes that suits its hardware profile — with the SignalBus providing transparent signal delivery across all of them\.

The core principle is that the Transport layer sits beneath the SignalBus\. Every node continues to call bus\.publish\(\) and bus\.subscribe\(\) exactly as before\. The transport handles serialisation and delivery, whether that means an in\-process queue or an MQTT broker on the other side of a WiFi link\.

> **How the bridge works:** `SignalBus(transport=transport, agent_node_id=…)` attaches a transport\. `publish()` then dispatches to local subscribers **and** forwards the signal to the transport; signals arriving from the transport are dispatched to local subscribers\. Nodes keep calling `publish()`/`subscribe()` unchanged\. After the transport is connected, the bus must subscribe its inbound handler for each locally\-subscribed kind — call `await bus.start_transport()` once; `AgentNode.start()` does this automatically\. Loop/echo prevention: each forwarded signal is tagged with its originating `agent_node_id`, and the inbound handler drops messages that originated from this same agent \(so a broker echo, or a shared `LocalTransport`, never double\-delivers\)\. Note: every published signal is currently forwarded to the transport regardless of whether a remote agent has subscribed to that kind — the publish\-side "transmit only subscribed kinds" filtering \(spec §11\) is not yet implemented\.

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

\# Normal InstinctNodes have no co\-location constraint —

\# their proposals travel over the bus to any AgentNode\.

The constraint covers the reflex's **inputs** as well, but the validator checks only the reflex → action edge\. A reflex in the tick's reflex stage sees ``ctx\.signals``, which hold the signals returned by the **local** SenseMasterNode \(plus buffered supervisor, node\_fault and state\_update signals\); a sense signal arriving from another AgentNode over the transport never enters ``ctx\.signals``, so a reflex fed only by a remote sensor never fires\. In ``reflex\_mode='event'`` the reflex lane subscribes on the bus and would also receive transport\-delivered signals, but then network latency is on the reflex path\. The SenseNodes that feed a reflex MUST therefore be assigned to the reflex's AgentNode — a deployment rule the manifest author is responsible for\.

## __10\.5 Signal Routing__

When a signal is published on one AgentNode, the transport delivers it to all AgentNodes that have a subscriber for that signal kind\. Each AgentNode filters signals and delivers only those relevant to its own nodes\. Signal serialisation uses msgpack by default for compact binary encoding; JSON is available as a fallback for debuggability\.

\# Signal envelope on the wire \(msgpack\-encoded\):

\{

    'v':  1,                    \# protocol version

    'src': 'jetson\-vision',     \# originating AgentNode

    'sig': \{                    \# serialised Signal fields

        'source': 'CameraSenseNode',

        'kind':   'visual',

        'value':  '<bytes>',    \# node\-defined encoding

        'confidence': 0\.97,

        'timestamp':  1234567\.89

    \}

\}

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

SupervisorSignals flow over the transport like any other signal\. This means a NodeFaultReflex on one AgentNode can react to a node failure on a different AgentNode\. The HealthMonitor on each AgentNode receives RemoteNodeState updates from other agents via the bus and maintains a mesh\-wide health picture\.

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

  mqtt\_broker: '192\.168\.1\.10:1883'

  nats\_server: 'nats://cloud\.example\.com:4222'

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

          priority: 200

          action\_id: EmergencyStopActionNode   \# required for reflexes

                                               \# \(co\-location target\)

        \- kind: OverheatInstinct

          priority: 100

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

          priority: 60

        \- kind: NavigationInstinct

          priority: 50

      decision:

        \- WeightedDecisionNode

      action:

        \- SpeakActionNode              \# text\-to\-speech on cloud

        \- LogActionNode

## __11\.2 Loading and Validating a Manifest__

from arachnite\.distributed import DeploymentManifest, MeshRuntime

manifest = DeploymentManifest\.from\_yaml\('arachnite\-manifest\.yaml'\)

\# Validates:

\#   \- all referenced node classes are importable

\#   \- every reflex node declares its target action\_id and is co\-located

\#     with that action node \(the sense nodes feeding it are not checked\)

\#   \- no duplicate node\_ids within an AgentNode

\#   \- transport configs are complete

manifest\.validate\(\)   \# raises ManifestValidationError on failure

\# Build and start the full mesh \(runs on a coordinator machine\)

mesh = MeshRuntime\(manifest\)

await mesh\.start\(\)    \# starts each AgentNode in its own process/thread

\# Or start just one AgentNode on the current machine:

agent = manifest\.build\_agent\('pi\-body'\)

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

MeshRuntime coordinates a multi\-agent deployment from a single entry point\. It reads the manifest, builds all AgentNodes, and either starts them as subprocesses \(for deployment on one machine with multiple logical agents\) or connects them over the transport layer when they run on physically separate devices\.

class MeshRuntime:

    def \_\_init\_\_\(self, manifest: DeploymentManifest\): \.\.\.

    async def start\(self\) \-> None:

        """Start all AgentNodes defined in the manifest\.

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

When a transport loses connectivity, the affected AgentNode cannot receive signals from the mesh\. The NodeSupervisor detects this as a transport fault and transitions the transport to FAULTED state\. A TransportFaultReflex — a built\-in optional reflex — can be configured to trigger a safe\-mode action when the connection drops, ensuring the edge device does not continue operating on stale data\.

__Reconnect vs\. intentional disconnect\.__ MQTTTransport and RedisTransport maintain automatic reconnection on connection loss\. An internal `\_stopped` flag distinguishes intentional disconnects \(via `disconnect\(\)`\) from unexpected connection loss\. When `disconnect\(\)` is called, the flag is set so that any in\-flight or pending reconnection attempt exits immediately instead of re\-establishing the connection\. `connect\(\)` clears the flag\.

\# Built\-in reflex for transport loss \(opt\-in via manifest\):

agents:

  \- id: pi

    transport: mqtt

    on\_transport\_fault:

      reflex: TransportFaultReflex   \# fires immediately on disconnect

      action: SafeModeActionNode     \# must be co\-located

      reconnect\_policy: exponential\_backoff

      max\_reconnect\_attempts: 10

