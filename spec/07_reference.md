<!-- Arachnite SPEC §18–§21 -->

# __18\. Exceptions__

__Exception__

__When raised__

__ArachniteError__

Base class for all framework exceptions\.

__SignalBusError__

A subscriber raised during publish\(\)\.

__NodeRegistrationError__

Duplicate node\_id registered to a master node\. InstinctMasterNode checks normal and reflex instincts against one namespace\.

__ActionTimeoutError__

ActionNode\.execute\(\) or a step exceeded timeout\_s\.

__ActionNotFoundError__

Proposal\.action\_id has no matching ActionNode\. Raised by dispatch\(\); launch\(\) and launch\_reflex\(\) return LaunchStatus\.NOT\_FOUND instead\.

__ResourceConflictError__

A proposal's action needs a declared resource owned by a running action \(ADR 0005\)\. Carried in Result\.error by dispatch\(\) \(never raised\); ``owners`` names the running action\_ids\. launch\(\) reports the same conflict as LaunchStatus\.BLOCKED\.

__ContextError__

ContextNode accessed before first update\(\)\.

__SupervisorError__

NodeSupervisor encountered an unrecoverable error during restart\.

__ReflexConflictError__

Two reflex nodes with equal top priority both fired in the same evaluation and reflex\_conflict='raise'\. Raised by InstinctMasterNode\.evaluate\_reflexes\(\) and carries every evaluated proposal in ``proposals``; the runtime logs it at ERROR and still launches them in priority order — it never aborts the tick\. The default reflex\_conflict='dispatch\_all' launches them without raising\.

__TransportError__

Base class for transport\-layer failures\.

__TransportConnectionError__

Transport failed to connect or lost connection and exhausted reconnect attempts\.

__CoLocationError__

A ReflexInstinctNode’s target action\_id is assigned to a different AgentNode in the manifest\.

__ManifestValidationError__

DeploymentManifest\.validate\(\) found an inconsistency\.

__InterruptError__

Reserved for interrupt requests that cannot be fulfilled\. Not raised by the current runtime: an interrupt that a NEVER action ignores leaves the interrupting proposal parked for hand\-off \(§17\.6\)\.

__RollbackError__

One or more rollback callables raised during a rollback walk \(on\_interrupted\(\), on\_aborted\(\), or rollback\_on\_preempt\)\. The walk attempts every rollback first; ``failures`` lists each \(step\_name, exception\), ``step\_name`` / ``cause`` describe the first\. execute\(\) reports it in Result\.error with rolled\_back=False; the action is never retried\.

__MandatoryBlockViolation__

No longer raised by the framework: since ADR 0009 ``ActionMasterNode\.request\_interrupt\(\)`` latches a deliberative interrupt that targets an action inside a mandatory completion block and the action honours it at the first stop point after the block\. Kept, and exported, for compatibility\. Only a ReflexPreemption\.PREEMPT reflex, a cancellation \(``cancel\(\)``, shutdown, ``emergency\_stop\(\)``\) or the action timeout can cut into a block \(§17\.5\)\.

__StepAbortError__

A step aborted the sequence: it returned abort\_sequence=True, exceeded its timeout, or raised \(the step's error is chained as ``\_\_cause\_\_``\)\. Carried in Result\.error after on\_aborted\(\) ran\.

__ActionVetoed__

An ActionGuard denied a proposal before dispatch\. Carried in Result\.error \(never raised\); the action's execute\(\) is not called and no retry runs\. See ActionMasterNode pre\-dispatch guards \(§5\.7\)\.

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

The outcome of an executed action, fed back into the context of the first tick that starts after the action finished\.

__Tick__

One complete pipeline cycle: sense → context → reflex → instinct → decide → act\. The reflex and act stages launch actions; actions run as tasks across later ticks\.

__SignalBus__

The publish\-subscribe channel connecting all nodes\.

__Master Node__

A coordinator that owns and orchestrates a collection of leaf nodes of one type\.

__Leaf Node__

A developer\-extended node that implements one specific sense, instinct, decision, or action\.

__Reflex Arc__

A signal pathway that bypasses the DecisionNode and triggers an action directly\. Modelled on the biological ganglionic reflex arc\.

__ReflexInstinctNode__

A specialisation of InstinctNode whose proposals are launched immediately, skipping the DecisionNode, and take actuators from running actions according to its ReflexPreemption\.

__ReflexPreemption__

Declared per reflex node: what the reflex does when its action needs a resource a running action owns\. PREEMPT \(default\) cancels the owner even inside a mandatory block, runs its safe\-stop hook, then starts the reflex; WAIT\_FOR\_BLOCK lets a running mandatory block finish and preempts the owner at the first boundary after it\.

__Reflex Lane__

The event\-driven path of ArachniteRuntime\(reflex\_mode='event'\): reflex nodes with trigger\_on\_signals are evaluated when a matching signal is published \(by read\_all\(\) or BaseSenseNode\.push\(\)\) rather than once per tick\.

__Actuator Resource__

A name in BaseActionNode\.resources for an actuator the action commands\. Two actions conflict when they share an action\_id or a resource\.

__Actuator Exclusivity__

The safety property that at most one running action owns a given declared resource at any time\. Enforced by ActionMasterNode\.

__Armed Reflex__

A reflex proposal waiting for its owners to release\. It claims its resources, so no deliberative proposal can take them, and starts the moment they are free\.

__Safe\-stop Hook__

BaseActionNode\.on\_preempted\(\): brings the actuators to a safe hold when the action is preempted by a reflex or cancelled by shutdown; bounded by safe\_stop\_timeout\_s; does not undo work\.

__LaunchStatus / LaunchOutcome__

The synchronous result of ActionMasterNode\.launch\(\) / launch\_reflex\(\): LAUNCHED, BLOCKED, ARMED, DEDUPED, VETOED, NOT\_FOUND or CLOSED \(the master was closed by ``close\(\)`` / ``cancel\_all\(\)`` for shutdown\), plus the conflicting owners\.

__Hand\-off__

A selected proposal whose launch was BLOCKED by an owner still unwinding is parked on DecisionMasterNode and retried every tick until it launches, is superseded by its instinct, or max\_handoff\_ticks pass\.

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

The rule that a ReflexInstinctNode, its target ActionNode and the SenseNodes that feed it must be assigned to the same AgentNode\. The manifest validator checks the reflex → action part\.

__NodeAssignment__

The internal binding of a node class, its configuration, and its owning AgentNode, derived from the manifest\.

__RemoteNodeState__

A supervisor state record for a node running on a different AgentNode, received via SupervisorSignal over the transport\.

__ActionStep__

The atomic unit of a MultiStepActionNode\. Declares name, interruptibility, optional rollback, and per\-step timeout\.

__StepResult__

The outcome of a single ActionStep execution\. Can signal success, failure, request sequence abort \(abort\_sequence\), or a cooperative mid\-step interrupt \(interrupted\)\.

__ActionGuard__

A synchronous, bounded, no\-I/O Protocol consulted by ActionMasterNode before an action executes\. Returns a GuardVerdict \(allow / deny\+reason\) to veto a concrete proposal by its parameters\. The runtime, parameter\-aware complement to the startup\-validated Permission\.

__GuardVerdict__

The outcome of an ActionGuard check: allow, or deny with a reason\. Returned by ActionGuard\.\_\_call\_\_\(\)\.

__InterruptPolicy__

Declares how a MultiStepActionNode responds to an interrupt request: ALWAYS, NEVER, CHECKPOINT, or ROLLBACK\.

__InterruptRequest__

A typed object issued by DecisionMasterNode to ActionMasterNode, carrying the new proposal, the interrupt reason and the running action to stop \(target\_action\_id\)\. Reflexes never issue InterruptRequests\.

__MultiStepActionNode__

An ActionNode that decomposes its behaviour into an ordered sequence of ActionSteps with explicit interrupt and rollback semantics\.

__Mandatory Completion Block__

A sequence of consecutive non\-interruptible ActionSteps that no deliberative interrupt can cut into \(deliberative atomicity\)\. A PREEMPT reflex, emergency\_stop\(\), a timeout or an abort can end it early \(§17\.5\)\.

__Fixed Action Pattern__

The biological model for NEVER interrupt policy: a neural sequence that runs to completion once initiated, regardless of new input\.

__Bounded Reflex Admission__

The upper bound on the delay before a conflicting reflex action starts, derived from declared timeouts: safe\_stop\_timeout\_s under PREEMPT; the remaining mandatory block plus safe\_stop\_timeout\_s under WAIT\_FOR\_BLOCK\. Valid under the assumptions of §17\.12; not a hard real\-time guarantee\.

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

An immediate shutdown triggered by runtime\.emergency\_stop\(\), bypassing the shutdown phases: the loop and reflex lane stop, every running action is cancelled — mandatory blocks included — through its safe\-stop hook without rollback, and the nodes are torn down\.

__Bounded Interrupt Latency \(shutdown\)__

Graceful stop\(\): the longest remaining mandatory block × 1\.1 \(phase 3\), then up to teardown\_timeout\_s for actions to stop at a boundary before they are cancelled with safe\-stop \(phase 4\)\. Emergency stop: the safe\-stop budget only — mandatory blocks are cut immediately\.

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

    ActionGuard, GuardVerdict, LaunchStatus, LaunchOutcome,

    \# Safety monitors

    BaseSafetyMonitor, SafetyMonitorRegistry, SafetyViolationSignal,

    SafetySeverity, MonitorState,

    ReflexBypassMonitor, MandatoryBlockMonitor, ReflexDispatchMonitor,

    ActuatorExclusivityMonitor, ReflexAvailabilityMonitor, TickBudgetMonitor,

    \# Models

    Signal, Context, Proposal, Result,

    ActionStep, StepResult, InterruptPolicy, InterruptRequest, ReflexPreemption,

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

    DependencyValidationError, PermissionValidationError, ResourceConflictError,

\)

Members added with ADR 0005 \(non\-blocking dispatch and reflex arbitration\), specified in §5 and §17\.12:

\- ``Result\.preempted``; ``InterruptRequest\.target\_action\_id``\.

\- ``BaseActionNode\.resources``, ``safe\_stop\_timeout\_s``, ``on\_preempted\(\)``; ``MultiStepActionNode\.rollback\_on\_preempt``, ``on\_aborted\(\)``, ``request\_reflex\_preempt\(\)``\.

\- ``BaseReflexInstinctNode\.preemption``; ``BaseSenseNode\.push\(\)``; ``ContextNode\.reflex\_view\(\)``\.

\- ``ActionMasterNode\(strict\_resources=\.\.\.\)``, ``launch\(\)``, ``launch\_reflex\(\)``, ``drain\_completed\(\)``, ``wait\_idle\(\)``, ``conflicting\_owners\(\)``, ``cancel\(\)``, ``cancel\_all\(\)``, ``interrupt\_all\(\)``; ``dispatch\(\)`` / ``dispatch\_many\(\)`` take ``origin=``\.

\- ``DecisionMasterNode\(max\_handoff\_ticks=\.\.\.\)``, ``park\(\)``, ``handoff\_proposals``; ``on\_new\_proposals\_many\(conflicts=\.\.\.\)``\.

\- ``ArachniteRuntime\(reflex\_mode=\.\.\., reflex\_signal\_ttl\_s=\.\.\., reflex\_eval\_timeout\_s=\.\.\.\)``, ``wait\_idle\(\)``; ``RuntimeBuilder\.reflex\_mode\(\)``, ``strict\_resources\(\)``\.

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

__ReflexPreemption for a reflex__

PREEMPT \(default\) when the hazard outweighs finishing a running mandatory block — the owner is cut, safe\-stopped, and not rolled back\. WAIT\_FOR\_BLOCK when cutting the block is the greater hazard — the reflex waits at most the remaining block plus the safe\-stop\. Declare resources on every action so conflicts are visible\.

__InterruptPolicy for MultiStepActionNode__

\- ALWAYS — interrupt at any step boundary \(stateless, idempotent ops\)

\- NEVER — ignore deliberative interrupts \(atomic operations that must complete; a PREEMPT reflex can still cut in\)

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

__ActionNotFoundError at runtime__: Proposal\.action\_id does not match any registered ActionNode\.node\_id\. Verify the strings are identical\. The runtime's launch\(\) / launch\_reflex\(\) return LaunchStatus\.NOT\_FOUND instead of raising; the miss is logged as a warning and the tick continues\.

__Reflex fires but action never executes__: reflex and its target action are on different AgentNodes\. Co\-locate them; DeploymentManifest\.validate\(\) will catch this\. If the reflex never fires at all, check that the SenseNodes feeding it are on its AgentNode too\.

__Action Result missing after tick\(\)__: tick\(\) launches actions without waiting for them\. Await runtime\.wait\_idle\(\) and run one more tick; the Result is in that tick's ctx\.last\_results\.

__ManifestValidationError: environment variable not set__: manifest uses \$\{X\} but the var is absent\. Set it or add a default: \$\{X:\-fallback\}\.

__request\_interrupt\(\) has no effect__: when execute\(\) is called directly \(not through ActionMasterNode\), it resets \_interrupt\_requested at start\. Schedule the interrupt with asyncio\.create\_task\(\) after execute\(\) has started\. Under ActionMasterNode the reset happens at reservation, so an interrupt delivered after launch\(\) is kept\.

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
