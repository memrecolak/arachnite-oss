<!-- Arachnite SPEC §1–§4 -->

# __1\. Introduction__

## __1\.1 Overview__

Arachnite is a Python framework for building reactive AI\-powered software systems using a biologically inspired node architecture\. The design is modelled on the instinct\-driven nervous system of arachnids, where raw environmental signals travel through structured layers of perception, evaluation, and decision before producing a physical action\.

The framework does not generate or provide any AI behaviour itself\. It provides the structural scaffolding — abstract base classes, a typed signal bus, a context store, and a runtime loop — that developers extend with their own domain logic\.

## __1\.2 Design Philosophy__

- Separation of concerns: sensing, reasoning, deciding, and acting are fully decoupled\.
- Composability: individual nodes are independently testable and replaceable\.
- Convention over configuration: sensible defaults at every layer; escape hatches everywhere\.
- Async\-first: every node interface is async\-compatible from the ground up\.
- Model agnostic: no dependency on any specific AI provider or model\.

## __1\.3 Biological Metaphor__

The architecture maps directly to arachnid neurology:

__Framework Layer__

__Biological Equivalent__

__Responsibility__

SenseNode

Mechanoreceptors / eyes

Gather raw data from the environment

ContextNode

Short\-term sensory memory

Maintain working state and history

InstinctNode

Ventral nerve cord ganglia

Evaluate situation, propose responses

ReflexInstinctNode

Ganglionic reflex arc

Bypass decision layer for immediate action

DecisionNode

Central brain integration

Choose one proposal from many

ActionNode

Muscle effectors

Execute the chosen response

MultiStepActionNode

Fixed action pattern

Ordered step sequence with atomic blocks and rollback

SignalBus

Neural pathways

Carry typed signals between nodes

Transport

Peripheral nervous system

Carry signals across process and network boundaries

AgentNode

Organism / body segment

A named deployment unit — one device or process

NodeSupervisor

Glial / support cells

Monitor, restart, and report node health

# __2\. Architecture__

## __2\.1 Execution Pipeline__

Each runtime tick of an Arachnite agent executes the following pipeline\. Reflex instincts are evaluated first and short\-circuit the decision layer if triggered:

SenseMasterNode\.read\_all\(\)

    └─ each SenseNode\.read\(\) → Signal

        └─ published to SignalBus

ContextNode\.update\(signals\)

    └─ merges signals into snapshot

InstinctMasterNode\.evaluate\_reflexes\(context\)   ← NEW

    └─ each ReflexInstinctNode\.evaluate\(ctx\) → Proposal | None

    └─ if any reflex fires → dispatch immediately, skip decision step

InstinctMasterNode\.evaluate\_all\(context\)

    └─ each InstinctNode\.evaluate\(ctx\) → Proposal | None

DecisionMasterNode\.decide\(proposals\)

    └─ DecisionNode\.decide\(proposals\) → Proposal

ActionMasterNode\.dispatch\(proposal\)

    └─ ActionNode\.execute\(proposal\) → Result

        └─ Result fed back to ContextNode

## __2\.2 Directory Structure__

arachnite/

    \_\_init\_\_\.py

    runtime\.py          \# ArachniteRuntime orchestrator

    bus\.py              \# SignalBus

    context\.py          \# ContextNode

    supervisor\.py       \# NodeSupervisor, NodeState, SupervisorSignal

    health\.py           \# HealthMonitor

    logging\.py          \# StructuredLogger, LogEvent, ObservabilityMixin

    codec\.py            \# SignalCodec, CodecRegistry

    config\.py           \# NodeConfig, config injection helpers

    shutdown\.py         \# ShutdownCoordinator

    transport/

        \_\_init\_\_\.py

        base\.py         \# BaseTransport ABC

        local\.py        \# LocalTransport \(default, in\-memory\)

        mqtt\.py         \# MQTTTransport \(edge devices\)

        nats\.py         \# NATSTransport \(cloud / laptop\)

        redis\.py        \# RedisTransport \(Redis\-backed deployments\)

    distributed/

        \_\_init\_\_\.py

        agent\_node\.py   \# AgentNode \(named deployment unit\)

        manifest\.py     \# DeploymentManifest, NodeAssignment

        mesh\.py         \# MeshRuntime \(multi\-AgentNode coordinator\)

        colocation\.py   \# Reflex co\-location validator

    nodes/

        \_\_init\_\_\.py

        base\.py         \# BaseNode ABC

        sense\.py        \# BaseSenseNode, SenseMasterNode

        instinct\.py     \# BaseInstinctNode, BaseReflexInstinctNode,

                        \# InstinctMasterNode

        decision\.py     \# BaseDecisionNode, DecisionMasterNode

        action\.py       \# BaseActionNode, MultiStepActionNode,

                        \# ActionMasterNode

    models\.py           \# Signal, Proposal, Result, Context,

                        \# ActionStep, StepResult, InterruptPolicy,

                        \# InterruptRequest, ActionExecutionState

    media\.py            \# MediaStore \(on\-disk storage for large signal payloads\)

    exceptions\.py

    py\.typed

# __3\. Data Models__

All data flowing between nodes is typed using Python dataclasses\. These are defined in arachnite/models\.py\.

## __3\.1 Signal__

Produced by SenseNodes\. Carries raw environmental data onto the SignalBus\.

@dataclass

class Signal:

    source: str            \# node id that produced this signal

    kind: str              \# e\.g\. 'thermal', 'visual', 'audio'

    value: Any             \# raw reading — type defined by the SenseNode

    confidence: float      \# 0\.0 to 1\.0 \(NaN and Inf rejected\)

    timestamp: float       \# time\.monotonic\(\) at read time

    metadata: dict = field\(default\_factory=dict\)

## __3\.2 Context__

A snapshot assembled by ContextNode each tick\. Passed to all InstinctNodes\.

@dataclass

class Context:

    tick: int

    signals: list\[Signal\]           \# all signals from this tick

    history: deque\[list\[Signal\]\]    \# past N ticks \(default N=10\)

    state: dict\[str, Any\]           \# mutable key\-value store

    last\_result: Result | None      \# outcome of the previous action

    timestamp: float

    action\_state: ActionExecutionState | None  \# singular, backward compat

    last\_results: list\[Result\]      \# all results from concurrent dispatch

    action\_states: list\[ActionExecutionState\]  \# all running action states

## __3\.3 Proposal__

Produced by InstinctNodes\. Recommends an action to the DecisionNode\.

@dataclass

class Proposal:

    instinct\_id: str       \# which instinct generated this

    action\_id: str         \# which ActionNode to invoke

    priority: int          \# higher = more urgent \(no fixed ceiling\)

    urgency: float         \# normalised 0\.0\-1\.0, for weighted strategies

    parameters: dict       \# passed through to ActionNode\.execute\(\)

    rationale: str = ''    \# optional human\-readable explanation

    evidence: dict = \{\}    \# supporting signal references and summaries

    persist: bool = False  \# True = carry forward if not selected this tick

The ``persist`` flag controls whether a proposal survives across ticks\. When ``persist=True`` and the proposal is not selected by the DecisionNode, it is carried forward to the next tick's decision pool\. A persistent proposal is superseded when the same instinct produces a new ``persist=True`` proposal, cleared when the instinct is re\-evaluated and does not produce a ``persist=True`` proposal \(conditions changed\), or dropped after ``max\_pending\_ticks`` \(configurable on DecisionMasterNode, default 50\)\. Proposals from throttled or signal\-gated instincts \(not evaluated this tick\) retain their pending status\. Default is ``False`` — current behaviour, lost if not selected\.

The ``evidence`` field enables instincts to pass supporting data — file paths, analysis summaries, signal snapshots — through the decision and action layers\. This is particularly important for multi\-modal agents where the decision layer needs context beyond raw priority\/urgency numbers to make informed choices\. For example, a vision instinct can attach both a media file path and a natural\-language summary of what it detected:

    evidence=\{

        "camera\_path": "\/tmp\/arachnite\/media\/cam\_tick42\.jpg",

        "camera\_summary": "Fire detected in kitchen \(confidence 0\.97\)",

    \}

Custom DecisionNode strategies can inspect ``proposal\.evidence`` to make context\-aware decisions\. Action nodes receive the evidence via the proposal and can load referenced files directly\.  Built\-in strategies \(Greedy, Weighted, Random\) ignore the field for backward compatibility\.

## __3\.4 Result__

Returned by ActionNodes\. Fed back into the ContextNode for the next tick\. Extended fields support MultiStepActionNode outcomes\.

@dataclass

class Result:

    action\_id: str

    success: bool

    output: Any                         \# any data the action produced

    error: Exception | None = None

    duration\_s: float = 0\.0

    \# Multi\-step fields \(None for single\-step BaseActionNode\)

    interrupted: bool = False           \# True if stopped by InterruptRequest

    stopped\_at\_step: str | None = None  \# step name where execution paused

    step\_results: list\[StepResult\] = field\(default\_factory=list\)

    rolled\_back: bool = False           \# True if ROLLBACK policy ran

# __4\. SignalBus__

The SignalBus is the central nervous system of the framework\. Nodes communicate exclusively through it — they never hold direct references to each other\. This keeps every node independently testable and allows new nodes to be added without touching existing ones\.

## __4\.1 Interface__

class SignalBus:

    def subscribe\(self, kind: str, callback: Callable\[\[Signal\], Awaitable\[None\]\]\) \-> None:

        """Register an async callback for a given signal kind\."""

    def unsubscribe\(self, kind: str, callback: Callable\) \-> None:

        """Remove a previously registered callback\."""

    async def publish\(self, signal: Signal\) \-> None:

        """Broadcast a signal to all subscribers of its kind\."""

    async def publish\_many\(self, signals: list\[Signal\]\) \-> None:

        """Publish a batch of signals concurrently\."""

    def clear\(self\) \-> None:

        """Remove all subscribers\. Useful between test cases\."""

## __4\.2 Design Notes__

- Callbacks are always async\. Synchronous subscribers must be wrapped\.
- publish\(\) dispatches to all matching subscribers concurrently via asyncio\.gather\(\)\.
- The bus does not persist signals\. Persistence is ContextNode’s responsibility\.
- A wildcard kind '\*' receives every signal regardless of type\.
- Exceptions in a subscriber are caught and re\-raised as SignalBusError after all other subscribers have been notified\.

