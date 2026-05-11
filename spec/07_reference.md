<!-- Arachnite SPEC §18–§21 -->

# __18\. Exceptions__

__Exception__

__When raised__

__ArachniteError__

Base class for all framework exceptions\.

__SignalBusError__

A subscriber raised during publish\(\)\.

__NodeRegistrationError__

Duplicate node\_id registered to a master node\.

__ActionTimeoutError__

ActionNode\.execute\(\) or a step exceeded timeout\_s\.

__ActionNotFoundError__

Proposal\.action\_id has no matching ActionNode\.

__ContextError__

ContextNode accessed before first update\(\)\.

__SupervisorError__

NodeSupervisor encountered an unrecoverable error during restart\.

__ReflexConflictError__

Two reflex nodes with equal priority both fired in the same tick \(configurable: raise or dispatch both\)\.

__TransportError__

Base class for transport\-layer failures\.

__TransportConnectionError__

Transport failed to connect or lost connection and exhausted reconnect attempts\.

__CoLocationError__

A ReflexInstinctNode’s target action\_id is assigned to a different AgentNode in the manifest\.

__ManifestValidationError__

DeploymentManifest\.validate\(\) found an inconsistency\.

__InterruptError__

An interrupt request could not be fulfilled — e\.g\. action policy is NEVER and the queue is full\.

__RollbackError__

A rollback callable raised during on\_interrupted\(\)\. Contains the original interrupt context\.

__MandatoryBlockViolation__

Raised if runtime configuration attempts to force\-stop an action inside a mandatory completion block outside of a shutdown sequence\.

__StepAbortError__

A step returned abort\_sequence=True\. Carries the StepResult for diagnosis\.

__DependencyValidationError__

A node declared a node\_id in its requires list that is not registered with any master node at startup\. Raised by ArachniteRuntime\.start\(\) before any setup\(\) call\.

# __19\. Roadmap__

The following features are planned for future versions and are explicitly out of scope for v0\.5\.

- Persistent memory store: pluggable backend \(SQLite, Redis\) for long\-term state beyond the in\-memory context history\.
- LLM InstinctNode: built\-in base class that calls a language model to produce a Proposal from natural language reasoning\.
- Node graph visualiser: runtime introspection tool that renders live signal flow, supervisor health, active action step, and mesh topology\.
- Node marketplace: a package convention \(arachnite\-nodes\-\*\) for community\-contributed nodes\.
- Typed signal registry: schema validation for Signal\.value using Pydantic or dataclass field annotations\.
- Supervisor dashboard: web\-based real\-time view of NodeState, mesh health, and per\-tick latency across all AgentNodes\.
- Manifest hot\-reload: update node assignments in a running mesh without restarting AgentNodes\.
- gRPC transport: high\-performance binary transport for low\-latency LAN deployments as an alternative to NATS\.
- Static interrupt latency analyser: a CLI tool that reads step definitions and emits a worst\-case interrupt latency bound for every MultiStepActionNode in a manifest\.
- Step visualiser: real\-time display of action step progress, interrupt state, and rollback history in the developer console\.
- OpenTelemetry sink: export LogEvents and metrics as OTLP traces and spans for integration with Grafana, Jaeger, and similar tools\.
- Prometheus metrics exporter: expose ObservabilityMixin counters and histograms as a /metrics HTTP endpoint\.
- Config schema validation: declare a Pydantic model for NodeConfig and have the manifest validator enforce it at load time\.

# __20\. Glossary__

__Term__

__Definition__

__Signal__

A typed, timestamped data packet emitted by a SenseNode\.

__Context__

A snapshot of all signals and state for the current tick\.

__Proposal__

A recommendation from an InstinctNode: do this action with these parameters\.

__Result__

The outcome of an executed action, fed back into the next tick’s context\.

__Tick__

One complete pipeline cycle: sense → context → reflex → instinct → decide → act\.

__SignalBus__

The publish\-subscribe channel connecting all nodes\.

__Master Node__

A coordinator that owns and orchestrates a collection of leaf nodes of one type\.

__Leaf Node__

A developer\-extended node that implements one specific sense, instinct, decision, or action\.

__Reflex Arc__

A signal pathway that bypasses the DecisionNode and triggers an action directly\. Modelled on the biological ganglionic reflex arc\.

__ReflexInstinctNode__

A specialisation of InstinctNode whose proposals are dispatched immediately, skipping the DecisionNode\.

__NodeSupervisor__

A per\-master\-node component that tracks the lifecycle state of registered child nodes and applies restart policies\.

__NodeState__

The lifecycle state of a supervised node: STARTING, RUNNING, FAULTED, RESTARTING, STOPPED, or DEAD\.

__SupervisorSignal__

A Signal of kind 'supervisor' emitted by a NodeSupervisor when a node changes state\.

__NodeFaultSignal__

A SupervisorSignal subclass of kind 'node\_fault' emitted alongside the generic SupervisorSignal when a node transitions to FAULTED or DEAD with an error\. Carries `error_type` and `error_message` for clean pattern\-matching without inspecting `fault_error`\.

__HealthMonitor__

A runtime\-level aggregator that combines health status from all NodeSupervisors, including remote AgentNodes\.

__RestartPolicy__

The rule governing whether and how many times a supervisor will restart a faulted node\.

__MergePolicy__

Controls how multiple signals of the same kind are resolved within one tick\. Values: ALL \(keep all — default\), LATEST \(newest timestamp\), HIGHEST\_CONFIDENCE \(highest confidence\), MEAN \(average numeric values\), BAYESIAN \(inverse\-variance weighted fusion\), ENSEMBLE \(confidence\-weighted mean with epistemic/aleatoric uncertainty decomposition\)\. Configured per\-kind on SenseMasterNode\.

__Permission__

A capability a node may declare it requires \(NETWORK, FILESYSTEM\_READ, FILESYSTEM\_WRITE, SUBPROCESS, GPU\)\. Validated at startup against a deployment whitelist\. Opt\-in: if no whitelist is configured, validation is skipped\.

__PermissionValidationError__

Raised when a node declares permissions not in its allowed set\. Startup\-only — never raised during the tick loop\.

__Transport__

The pluggable delivery backend beneath the SignalBus\. Implementations: LocalTransport, MQTTTransport, NATSTransport, RedisTransport\.

__AgentNode__

A named deployment unit — one device or process — running an ArachniteRuntime with a specific transport and set of nodes\.

__DeploymentManifest__

A YAML file declaring which nodes run on which AgentNode, transport config, and co\-location constraints\.

__MeshRuntime__

A coordinator that builds and starts all AgentNodes defined in a DeploymentManifest\.

__Co\-location Constraint__

The rule that a ReflexInstinctNode and its target ActionNode must be assigned to the same AgentNode\.

__NodeAssignment__

The internal binding of a node class, its configuration, and its owning AgentNode, derived from the manifest\.

__RemoteNodeState__

A supervisor state record for a node running on a different AgentNode, received via SupervisorSignal over the transport\.

__ActionStep__

The atomic unit of a MultiStepActionNode\. Declares name, interruptibility, optional rollback, and per\-step timeout\.

__StepResult__

The outcome of a single ActionStep execution\. Can signal success, failure, or request sequence abort\.

__InterruptPolicy__

Declares how a MultiStepActionNode responds to an interrupt request: ALWAYS, NEVER, CHECKPOINT, or ROLLBACK\.

__InterruptRequest__

A typed object issued by DecisionMasterNode to ActionMasterNode, carrying the new proposal and interrupt reason\.

__MultiStepActionNode__

An ActionNode that decomposes its behaviour into an ordered sequence of ActionSteps with explicit interrupt and rollback semantics\.

__Mandatory Completion Block__

A sequence of consecutive non\-interruptible ActionSteps that the framework will not interrupt, even if a reflex fires\.

__Fixed Action Pattern__

The biological model for NEVER interrupt policy: a neural sequence that runs to completion once initiated, regardless of new input\.

__Bounded Interrupt Latency__

The statically computable worst\-case delay before a reflex action executes, equal to the sum of remaining mandatory step timeouts\.

__ActionExecutionState__

A Context field exposing the current action id, step name, interruptibility, and mandatory block time remaining\.

__NodeConfig__

A typed wrapper around a node’s config dict, providing typed accessors with defaults and descriptive errors for missing required keys\.

__StructuredLogger__

A per\-node logger that emits typed LogEvent objects rather than raw strings, enabling machine\-readable log routing and filtering\.

__LogEvent__

A structured log record carrying level, node\_id, tick, message, and arbitrary key\-value data\.

__BaseLogSink__

Abstract base class for log destinations\. Built\-in: StdoutLogSink, JSONLogSink, FileLogSink, SignalBusLogSink\.

__ObservabilityMixin__

An optional mixin for nodes providing per\-tick timing histograms, signal counters, and Prometheus\-compatible metrics export\.

__SignalCodec__

Handles serialisation and deserialisation of Signal\.value for a specific signal kind when crossing a network transport boundary\.

__CodecRegistry__

Maps signal kinds to SignalCodec instances\. Supports a wildcard '\*' fallback\. Configured per\-transport\.

__HistoryConfig__

Per\-signal\-kind configuration for ContextNode history retention: max ticks, max bytes, and value TTL\.

__ShutdownCoordinator__

Manages the ordered seven\-phase graceful shutdown sequence, ensuring hardware is left in a safe state\.

__ShutdownPhase__

One of seven enumerated phases in the graceful shutdown sequence: stop sensing, drain reflexes, complete mandatory block, interrupt action, stop supervisors, teardown nodes, disconnect transport\.

__Emergency Stop__

An immediate shutdown triggered by runtime\.emergency\_stop\(\), skipping phases 1–3 and forcibly abandoning mandatory completion blocks\.

__Bounded Interrupt Latency \(shutdown\)__

During normal operation: sum of remaining mandatory step timeouts\. During emergency stop: zero — mandatory blocks are abandoned immediately\.

__spawn\_background\_task__

A BaseNode method that schedules a coroutine as an asyncio\.Task tracked by the node\. Tasks are automatically cancelled by master nodes before teardown\(\)\. Used in setup\(\) to start long\-running event listeners without blocking the tick loop\.

__cancel\_background\_tasks__

A BaseNode method that cancels and awaits all tasks registered via spawn\_background\_task\(\)\. Called automatically by SenseMasterNode, InstinctMasterNode, and ActionMasterNode before their teardown\(\) sequence\.

__requires__

A BaseNode class attribute \(list\[str\], default \[\]\) listing node\_ids that must be present in the registered node set before this node can operate\. ArachniteRuntime validates all requires lists at startup and raises DependencyValidationError if any are unmet\.

__DependencyValidationError__

Raised by ArachniteRuntime\.start\(\) when a node's requires list names a node\_id not registered with any master node\. Startup fails before any setup\(\) call\.

__artifact\_dir__

A BaseNode property returning a Path for per\-node output files \(model checkpoints, debug frames, log dumps\)\. Path: \{artifact\_root\}/\{agent\_node\_id\}/\{node\_id\}/\. Directory is created lazily on first access\. Default root is 'artifacts/'; override via the artifact\_root constructor parameter\.


# __21\. AI\-Assisted Development__

Arachnite is designed to be used with AI coding assistants \(e\.g\. Claude, Copilot, Cursor\)\. This section documents the resources provided to facilitate AI\-assisted development and the conventions that make the framework predictable for code generation\.

## __21\.1 llms\.txt__

A file named llms\.txt at the project root provides a concise, structured overview of the framework for AI tools that support the llmstxt\.org convention\. It contains:

\- A one\-paragraph description of the architecture and pipeline\.
\- A single import block showing every class available from the root package\.
\- Canonical extension patterns for all four node types \(Sense, Instinct, Action, MultiStepAction\) as copy\-paste\-ready code\.
\- The full wiring example \(SignalBus → master nodes → ArachniteRuntime\)\.
\- The six architectural rules that must never be violated\.
\- The priority convention table\.
\- A file map of the entire package\.
\- Known stubs and gaps\.

AI assistants should read llms\.txt before generating any Arachnite code\. It is intentionally shorter than SPEC\.md and optimised for prompt context windows\.

## __21\.2 Root\-level public API__

All classes required to build a complete agent are importable directly from the arachnite root package — no internal module paths are needed:

from arachnite import \(

    \# Runtime

    ArachniteRuntime, SignalBus, ContextNode,

    \# Node base classes

    BaseNode,

    BaseSenseNode, SenseMasterNode,

    BaseInstinctNode, BaseReflexInstinctNode, InstinctMasterNode,

    BaseDecisionNode, DecisionMasterNode,

    GreedyDecisionNode, WeightedDecisionNode, RandomDecisionNode,

    ActiveInferenceDecisionNode,

    BaseActionNode, MultiStepActionNode, ActionMasterNode,

    \# Safety monitors

    BaseSafetyMonitor, SafetyMonitorRegistry, SafetyViolationSignal,

    SafetySeverity, MonitorState,

    ReflexBypassMonitor, MandatoryBlockMonitor, ReflexDispatchMonitor,

    ReflexAvailabilityMonitor, TickBudgetMonitor,

    \# Models

    Signal, Context, Proposal, Result,

    ActionStep, StepResult, InterruptPolicy, InterruptRequest,

    NodeState, RestartPolicy, MergePolicy, SupervisorSignal, NodeFaultSignal,

    \# Infrastructure

    NodeSupervisor, HealthMonitor, NodeConfig,

    StructuredLogger, BaseLogSink, StdoutLogSink, JSONLogSink,

    SignalCodec, CodecRegistry,

    \# LLM providers

    LLMInstinctNode, LLMProvider, AnthropicProvider, OllamaProvider,

    LocalProvider, ThreadSafeProvider, SharedModelRegistry,

    \# Web dashboard

    SignalDashboard, FileLogSink,

    \# Framework config

    FrameworkConfig, RuntimeSettings, TransportSettings,

    \# Exceptions

    DependencyValidationError, PermissionValidationError,

\)

This is enforced by the \_\_all\_\_ list in arachnite/\_\_init\_\_\.py\.

## __21\.3 Examples__

The examples/ directory at the project root contains three runnable programs:

__examples/minimal\_agent\.py__

The simplest complete pipeline\. One sensor, one instinct, one action\. Heavily annotated\. Suitable as a starting template for new agents\.

__examples/temperature\_monitor\.py__

A realistic thermal monitoring agent demonstrating: simulated hardware sensor with drift, normal instinct, reflex instinct \(bypasses decision node\), multi\-step action with mandatory block and rollback, emergency stop action, structured logging, and keyboard interrupt handling\.

__examples/web\_dashboard\_demo\.py__

Extends temperature\_monitor\.py to add the SignalDashboard web UI\. All signals and log events stream to a browser at http://localhost:7070 and are written to a plain\-text log file\. Demonstrates SIGINT/SIGTERM graceful shutdown\.

## __21\.4 AI development guidelines__

When an AI generates Arachnite code it must follow these rules:

1\. Import everything from the root package \(arachnite\), not from internal submodules\.

2\. node\_id class attributes must be unique strings; Proposal\.action\_id must exactly match the target ActionNode\.node\_id\.

3\. execute\(\) must always return a Result\. evaluate\(\) must always return None when not applicable\. Neither may raise\.

4\. All hardware I/O must be async\. Wrap blocking calls in asyncio\.to\_thread\(\)\.

5\. Reflex instincts require priority ≥ 200 and must be co\-located with their target action on the same AgentNode\.

6\. Multi\-step actions must implement steps\(\) returning a list\[ActionStep\] and execute\_step\(\) dispatching on step\.name with match/case\.

7\. Do not use time\.sleep\(\) — use await asyncio\.sleep\(\)\.

8\. Do not import transport modules at the top level — they are optional dependencies and must be imported lazily\.

9\. New public classes must be added to arachnite/\_\_init\_\_\.py and \_\_all\_\_\.

10\. Run pytest and mypy arachnite after any change\.

## __21\.5 Architectural decisions__

__Reflex vs normal instinct__

Use BaseReflexInstinctNode when the response must bypass the decision layer \(safety, emergency stop\)\. Use BaseInstinctNode for goal\-directed and exploratory behaviour\. Reflex nodes require priority ≥ 200 and must be co\-located with their target action on the same AgentNode\.

__InterruptPolicy for MultiStepActionNode__

\- ALWAYS — interrupt at any step boundary \(stateless, idempotent ops\)

\- NEVER — never interrupt \(atomic operations that must complete\)

\- CHECKPOINT — interrupt only at steps with checkpoint=True \(safe pause points\)

\- ROLLBACK — interrupt at any step and invoke the rollback callable \(transactional ops\)

__Single\-process vs distributed__

Use ArachniteRuntime directly for single\-process agents\. Use AgentNode \+ DeploymentManifest for multi\-device deployments\. All AgentNodes are peers — there is no master/slave relationship\. MeshRuntime is a same\-process launch coordinator for testing and simulation\.

__Transport selection__

LocalTransport: single process, zero overhead\. MQTTTransport: constrained edge devices, broker\-based fan\-out\. NATSTransport: low\-latency LAN or cloud\. RedisTransport: when Redis is already in the stack\.

__Manifest: warn vs raise__

Warn for suspicious\-looking secret values \(never raise — value might be intentional\)\. Raise ManifestValidationError for structural problems: missing env var, unknown node class, co\-location violation\.

## __21\.6 Task workflow__

When implementing any change, follow this sequence:

1\. Read the relevant spec section \(section numbers appear in source file docstrings\)\.

2\. Read the existing implementation — never modify code you have not read\.

3\. Read the existing tests for the module\.

4\. Make the change — minimal scope, no speculative additions\.

5\. Run pytest — all tests must pass\.

6\. Run mypy arachnite — no new type errors\.

7\. If adding a public class, update arachnite/\_\_init\_\_\.py and \_\_all\_\_\.

8\. If the change affects the spec, update the relevant spec/\*\.md file\.

## __21\.7 Common errors__

__ActionNotFoundError at runtime__: Proposal\.action\_id does not match any registered ActionNode\.node\_id\. Verify the strings are identical\. During reflex dispatch this error is caught and logged as a warning; the tick continues\.

__Reflex fires but action never executes__: reflex and its target action are on different AgentNodes\. Co\-locate them; DeploymentManifest\.validate\(\) will catch this\.

__ManifestValidationError: environment variable not set__: manifest uses \$\{X\} but the var is absent\. Set it or add a default: \$\{X:\-fallback\}\.

__request\_interrupt\(\) has no effect__: execute\(\) resets \_interrupt\_requested at start\. Schedule the interrupt with asyncio\.create\_task\(\) after execute\(\) has started\.

__SupervisorSignal not emitted__: node was not registered with sv\.track\(\) before on\_fault\(\) was called\.

## __21\.8 Task → files map__

| Task area | Files to read first |
| --- | --- |
| New sense node | arachnite/nodes/sense\.py, arachnite/models\.py |
| New instinct node | arachnite/nodes/instinct\.py, arachnite/models\.py |
| New action node | arachnite/nodes/action\.py, arachnite/models\.py |
| Reflex node | arachnite/nodes/instinct\.py, arachnite/distributed/colocation\.py |
| Multi\-step action | arachnite/nodes/action\.py \(MultiStepActionNode, InterruptPolicy\) |
| Supervisor / health | arachnite/supervisor\.py, arachnite/health\.py |
| Transport | arachnite/transport/base\.py, then the specific transport file |
| Distributed / manifest | arachnite/distributed/manifest\.py, spec/05\_distributed\.md |
| Runtime / tick loop | arachnite/runtime\.py, spec/03\_runtime\.md |
| Logging | arachnite/logging\.py, spec/06\_infrastructure\.md §13 |
| Codecs | arachnite/codec\.py, spec/06\_infrastructure\.md §14 |
| Config injection | arachnite/config\.py, spec/06\_infrastructure\.md §12 |
| Public API | arachnite/\_\_init\_\_\.py — always update \_\_all\_\_ |
| Tests | tests/conftest\.py for shared fixtures |
