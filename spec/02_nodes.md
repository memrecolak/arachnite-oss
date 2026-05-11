<!-- Arachnite SPEC §5–§6 -->

# __5\. Node Reference__

## __5\.1 BaseNode__

Abstract base class inherited by every node type\. Provides identity, lifecycle hooks, configuration injection, structured logging, and access to the shared SignalBus\.

class BaseNode\(ABC\):

    node\_id: str            \# unique identifier, defaults to class name

    permissions: frozenset\[Permission\] = frozenset\(\)  \# capabilities this node requires \(opt\-in sandbox\); class\-level default MUST be immutable \(frozenset\); \_\_init\_\_ copies to a mutable set for per\-instance use

    bus: SignalBus

    config: dict            \# injected from manifest NodeAssignment\.config

    logger: StructuredLogger  \# pre\-configured per\-node logger

    \# ── Lifecycle hooks ──────────────────────────────────────

    async def setup\(self\) \-> None:

        """Called once before the runtime loop starts\. Override to

        initialise hardware, open connections, load models, etc\."""

    async def teardown\(self\) \-> None:

        """Called once after the runtime loop stops or on graceful

        shutdown\. Override to release resources cleanly\."""

    async def on\_pause\(self\) \-> None:

        """Called when the runtime is paused \(e\.g\. power\-save mode\)\.

        Override to suspend hardware polling, throttle I/O, etc\."""

    async def on\_resume\(self\) \-> None:

        """Called when the runtime resumes from a paused state\.

        Override to restore hardware to active polling rate\."""

    \# ── Per\-tick hooks \(optional instrumentation\) ────────────

    async def on\_tick\_start\(self, tick: int\) \-> None:

        """Called at the start of each tick, before read\(\)/evaluate\(\)\.

        Override for per\-tick metrics, watchdogs, or rate limiting\."""

    async def on\_tick\_end\(self, tick: int, duration\_s: float\) \-> None:

        """Called at the end of each tick with the tick duration\.

        Override to emit timing metrics or detect slow ticks\."""

    \# ── Background task lifecycle ────────────────────────────

    def spawn\_background\_task\(self, coro: Coroutine\) \-> asyncio\.Task:

        """Schedule a coroutine as a background asyncio\.Task tracked by

        this node\.  Call from setup\(\) to start long\-running listeners

        \(e\.g\. hardware event streams, MQTT subscribers\)\. All tasks

        spawned here are automatically cancelled before teardown\(\) by

        the master node shutdown sequence\."""

    async def cancel\_background\_tasks\(self\) \-> None:

        """Cancel and await all background tasks registered via

        spawn\_background\_task\(\)\.  Called automatically by

        SenseMasterNode, InstinctMasterNode, and ActionMasterNode

        before their teardown\(\) sequence\. Override only if custom

        cleanup ordering is required\."""

    \# ── Node dependency declaration ──────────────────────────

    requires: tuple\[str, \.\.\.\] = \(\)  \# class\-level default MUST be immutable \(tuple\); \_\_init\_\_ copies to a mutable list for per\-instance use

        \# List of node\_ids that must be registered before this node

        \# can operate\.  ArachniteRuntime\.start\(\) iterates all

        \# registered nodes, collects their requires lists, and raises

        \# DependencyValidationError if any required node\_id is absent

        \# from the combined set of registered nodes across all master

        \# nodes\.  Validation occurs once at startup, before any

        \# setup\(\) call\.

    \# ── Artifact directory ───────────────────────────────────

    @property

    def artifact\_dir\(self\) \-> Path:

        """Return a per\-node directory for large outputs \(model

        checkpoints, debug frames, logs\)\.  Path pattern:

        \{artifact\_root\}/\{agent\_node\_id\}/\{node\_id\}/

        Directory is created lazily on first access\.

        Default root: 'artifacts/'\.  Override by passing

        artifact\_root='\/path\/to\/dir' to the constructor\."""

__5\.2  Sense Nodes__

SenseNodes are the entry points of the system\. Each one reads from exactly one sensor or data source and emits a typed Signal onto the bus\.

### __BaseSenseNode__

class BaseSenseNode\(BaseNode, ABC\):

    signal\_kind: str        \# the kind string this node emits, e\.g\. 'thermal'

    poll\_interval\_s: float  \# how often to read, in seconds \(default: 0\.1\)

    @abstractmethod

    async def read\(self\) \-> Signal | None:

        """Read from the sensor and return a Signal, or None if there is

        nothing to report this tick\.  Returning None is the correct way

        to indicate 'no data' — e\.g\. a BootstrapSenseNode that fires once

        then becomes idle\.  SenseMasterNode filters out None returns\.

        Must be non\-blocking\. For blocking hardware calls, wrap in

        asyncio\.to\_thread\(\)\."""

    async def on\_error\(self, exc: Exception\) \-> None:

        """Called when read\(\) raises\. Default: log and continue\.

        Override to handle sensor failures, trigger fallback signals, etc\."""

### __SenseMasterNode__

Owns a collection of BaseSenseNode instances\. On each tick it calls read\(\) on all of them concurrently, publishes the resulting signals to the bus, and hands the signal list to the ContextNode\.

class SenseMasterNode\(BaseNode\):

    def register\(self, node: BaseSenseNode\) \-> None: \.\.\.

    def get\_node\(self, node\_id: str\) \-> BaseSenseNode | None: \.\.\.

    def unregister\(self, node\_id: str\) \-> None: \.\.\.

    async def read\_all\(self\) \-> list\[Signal\]: \.\.\.

### __Signal Merge Policies__

When multiple SenseNodes emit the same signal kind in a single tick, `SenseMasterNode` can resolve the conflict before the signals reach the bus\. Merge behaviour is configured per\-kind via the `merge_policies` parameter:

    sm = SenseMasterNode\(bus=bus, merge_policies=\{"temperature": MergePolicy\.MEAN\}\)

Available policies \(`MergePolicy` enum\):

| Policy | Behaviour |
|---|---|
| `ALL` \(default\) | Keep all signals — no merging |
| `LATEST` | Keep only the signal with the latest timestamp |
| `HIGHEST_CONFIDENCE` | Keep only the signal with the highest confidence |
| `MEAN` | Average numeric values and confidences; falls back to `HIGHEST_CONFIDENCE` if values are non\-numeric |
| `BAYESIAN` | Inverse\-variance weighted fusion: treats confidence as precision \(conf/\(1\-conf\)\), computes precision\-weighted mean\. Fused confidence reflects combined precision \(always ≥ max individual\)\. Reports `fused_variance` and `per_sensor_precisions` in metadata\. Falls back to `HIGHEST_CONFIDENCE` for non\-numeric values\. |
| `ENSEMBLE` | Confidence\-weighted mean with uncertainty decomposition: separates epistemic variance \(sensor disagreement\) from aleatoric variance \(individual sensor uncertainty\)\. Fused confidence = 1/\(1 \+ total\_uncertainty\)\. Reports `epistemic_variance`, `aleatoric_variance`, `total_uncertainty` in metadata\. Falls back to `HIGHEST_CONFIDENCE` for non\-numeric values\. |

Merged signals carry metadata: `merge_policy` \(the policy name\), `merged_from` \(list of source node\_ids\), and for MEAN, `sample_count`\. BAYESIAN additionally includes `fused_variance` and `per_sensor_precisions`\. ENSEMBLE includes `epistemic_variance`, `aleatoric_variance`, and `total_uncertainty`\.

Kinds without a configured policy default to `ALL`\. Single signals of a configured kind pass through unchanged \(no metadata added\)\.

### __Sensor Throttling \(poll\_interval\_s\)__

`poll\_interval\_s` \(default: 0\.1 s\) controls how often `SenseMasterNode\.read\_all\(\)` calls `read\(\)` on a node\. If less than `poll\_interval\_s` has elapsed since the last read, the node is **silently skipped** — no signal is produced for that tick\. This prevents expensive hardware calls from running every tick when the tick rate is faster than the sensor can respond\.

| `poll\_interval\_s` | Behaviour |
|---|---|
| `0\.0` | Read every tick \(no throttle\) — use for cheap or virtual sensors |
| `0\.1` \(default\) | Read at most 10 times per second |
| `0\.5` | Read at most 2 times per second — typical for I2C temperature probes |
| `1\.0+` | Read once per second or less — GPS modules, power\-hungry peripherals |

**Timestamps are per\-sensor:** `read\_all\(\)` captures a fresh `time\.monotonic\(\)` for each sensor individually, both for the throttle check and for updating `\_last\_read\_time`\. This ensures that `\_last\_read\_time` reflects when the sensor's `read\(\)` actually completed, not when the batch started\. Sensors with slow `read\(\)` implementations therefore get accurate throttle windows\.

**Important:** if your tick rate is higher than 1/poll\_interval\_s, the sensor will not produce a signal every tick\. Instincts that depend on this sensor's signals will see no matching signals on throttled ticks\. This is by design — set `poll\_interval\_s` to match your hardware's actual read rate\.

### __Developer Contract__

- Extend BaseSenseNode and implement read\(\)\.
- Set signal\_kind to a string that other nodes can subscribe to\.
- Set poll\_interval\_s to match the sensor's actual read rate \(default 0\.1 s\)\. Set to `0\.0` for virtual or very fast sensors\.
- Never cache state in read\(\) — each call should be a fresh observation\.

### __Example__

class TemperatureSenseNode\(BaseSenseNode\):

    signal\_kind = 'thermal'

    poll\_interval\_s = 0\.5

    async def read\(self\) \-> Signal:

        celsius = await asyncio\.to\_thread\(self\.\_hw\_read\_celsius\)

        return Signal\(

            source=self\.node\_id,

            kind=self\.signal\_kind,

            value=celsius,

            confidence=0\.95,

            timestamp=time\.monotonic\(\),

        \)

__5\.3  ContextNode__

The ContextNode is the working memory of the agent\. It receives all signals from the current tick and assembles a Context snapshot that is passed to every InstinctNode\. It also stores the Result of the previous action, giving instincts access to feedback\.

**Snapshot isolation:** Each `Context` returned by `update()` or `snapshot()` MUST be an independent copy\. The history deque and each inner signal list are shallow\-copied before inclusion in the snapshot, so that subsequent calls to `_apply_history_config()` do not mutate previously returned `Context` objects\. Instincts MAY hold references to a `Context` across await points without risk of concurrent modification\.

class ContextNode\(BaseNode\):

    history\_length: int = 10   \# number of past ticks to retain

    def \_\_init\_\_\(

        self,

        history\_length: int = 10,

        history\_config: dict\[str, HistoryConfig\] | None = None,

        state\_path: str | Path | None = None,

        flush\_on\_write: bool = False,

        max\_state\_keys: int | None = None,

    \): \.\.\.

    def update\(self, signals: list\[Signal\], result: Result | None\) \-> Context:

        """Merge this tick’s signals and last result into a new Context\.

        StateUpdateSignals in the signal list are applied to \_state

        before the snapshot is built, so instincts see updated state

        in the same tick the signal was emitted\."""

    def get\(self, key: str, default: Any = None\) \-> Any:

        """Read a named value from the persistent state dict\."""

    def set\(self, key: str, value: Any\) \-> None:

        """Write a named value to the persistent state dict\.

        Flushes to disk if flush\_on\_write=True\."""

    def delete\(self, key: str\) \-> None:

        """Remove a key from the persistent state dict\."""

    def flush\_state\(self\) \-> None:

        """Write \_state to state\_path as JSON\. No\-op if state\_path is None\.

        Values that are not JSON\-serialisable are converted to strings\."""

    def snapshot\(self\) \-> Context:

        """Return the most recently assembled Context without updating\."""

The state dict persists across ticks\. Nodes can use it to remember facts beyond the rolling signal history, for example a counter of consecutive failures or a learned threshold value\.

**State key limit:** Pass `max_state_keys` to cap the number of keys in the state dict\. When a `set()`, `StateUpdateSignal` write, or persisted\-state load would cause the dict to exceed the limit, the oldest key by insertion order is evicted until the size is within bounds\. `None` \(the default\) means no limit\. The value MUST be `>= 1` or `None`; values `< 1` raise `ValueError` at construction time\. The corresponding `ContextSettings.max_state_keys` field in `FrameworkConfig` uses `0` to mean no limit \(converted to `None` internally\)\.

**State persistence:** Pass `state_path` to persist the state dict across reboots\. The file is written as JSON\. Pass `flush_on_write=True` to flush automatically on every `set()`/`delete()` call \(appropriate for the Ariadne self\-model and world\-model\), or call `flush_state()` explicitly from teardown\(\)\.

**StateUpdateSignal:** Any node can write to `ContextNode._state` without holding a direct reference to ContextNode by emitting a `StateUpdateSignal` onto the bus\. The ContextNode intercepts these during `update()` and applies them before returning the snapshot\.

@dataclass

class StateUpdateSignal\(Signal\):

    kind: str = ‘state\_update’   \# always ‘state\_update’

    key: str = ‘’                \# the state dict key to write

    state\_value: Any = None      \# the value to store \(ignored when delete=True\)

    delete: bool = False         \# True → remove key from state

Example \(sense node updating world model\)::

    await self\.bus\.publish\(StateUpdateSignal\(

        source=self\.node\_id, kind=’state\_update’,

        value=None, confidence=1\.0, timestamp=time\.monotonic\(\),

        key=’world’, state\_value=\{‘temp’: 72, ‘objects’: \[\.\.\.\]\},

    \)\)

__5\.4  Instinct Nodes__

InstinctNodes are the reasoning layer\. Each one observes the Context and, if the situation warrants it, produces a Proposal recommending a specific action\. An instinct that does not apply to the current situation returns None\.

### __BaseInstinctNode__

class BaseInstinctNode\(BaseNode, ABC\):

    priority: int                   \# higher priority proposals win in greedy strategy

    enabled: bool = True            \# can be toggled at runtime

    trigger\_interval\_s: float | None = None

                                    \# minimum seconds between consecutive evaluate\(\)

                                    \# calls\. The interval is measured from the

                                    \# *completion* of the previous evaluate\(\) call,

                                    \# not its start\. This ensures slow instincts

                                    \# cannot be re\-evaluated before their previous

                                    \# result has been produced\.

                                    \# None \(default\) = evaluate every tick\.

                                    \# Set to e\.g\. 30\.0 for reflection or curiosity

                                    \# instincts that should fire at most once per

                                    \# 30 seconds regardless of tick rate\.

                                    \# Enforced by InstinctMasterNode\.evaluate\_all\(\)\.

                                    \# Does NOT apply to reflex nodes\.

    trigger\_on\_signals: list\[str\] | None = None

                                    \# Signal kinds that activate this instinct\.

                                    \# When set, evaluate\(\) is only called if at

                                    \# least one signal in ctx\.signals has a kind

                                    \# in this list\. None \(default\) = evaluate every

                                    \# tick regardless of which signals are present\.

                                    \# Enforced by InstinctMasterNode\.evaluate\_all\(\)\.

                                    \# Does NOT apply to reflex nodes\.

    @abstractmethod

    async def evaluate\(self, ctx: Context\) \-> Proposal | None:

        """Inspect the context\. Return a Proposal if this instinct applies,

        or None if the situation does not trigger it\."""

    async def on\_proposal\_rejected\(self, proposal: Proposal\) \-> None:

        """Called if this instinct produced a proposal but DecisionNode

        chose a different one\. Override to log or learn from outcomes\."""

### __InstinctMasterNode__

Calls evaluate\(\) on all registered instincts concurrently and collects the non\-None results into a ranked list of Proposals\.

class InstinctMasterNode\(BaseNode\):

    def register\(self, node: BaseInstinctNode\) \-> None: \.\.\.

    def unregister\(self, node\_id: str\) \-> None: \.\.\.

    async def evaluate\_all\(self, ctx: Context\) \-> list\[Proposal\]: \.\.\.

### __LLMInstinctNode__

A concrete subclass of BaseInstinctNode that delegates evaluation to a language model \(Claude by default via the Anthropic API\)\. The LLM is called in the background via asyncio\.to\_thread\(\) so the tick loop is never blocked\. The most recently completed Proposal is cached and returned on each tick until superseded\.

Requires the optional dependency: pip install arachnite\[llm\]

class LLMInstinctNode\(BaseInstinctNode, ABC\):

    model: str = 'claude\-haiku\-4\-5\-20251001'  \# Claude model to use

    max\_tokens: int = 256                      \# max tokens in LLM response

    min\_interval\_s: float = 1\.0               \# minimum seconds between API calls

    api\_key: str | None = None                \# None → use ANTHROPIC\_API\_KEY env var

    @abstractmethod

    def available\_actions\(self\) \-> dict\[str, str\]:

        """Return \{action\_id: description\} for actions the LLM may propose\.

        The LLM will only be allowed to propose actions listed here\."""

    def system\_prompt\(self\) \-> str:

        """Override to give the LLM context about the agent's purpose\.

        Default: generic instinct\-node prompt with available\_actions listed\."""

    def context\_to\_text\(self, ctx: Context\) \-> str:

        """Override to customise how the Context is serialised for the LLM\.

        Default: tick number, signals \(kind/value/confidence\), agent state

        \(ctx\.state key/value pairs\), execution state, and last action result\."""

The LLM is called with two tools: propose\_action \(returns action\_id, urgency, parameters, rationale\) and no\_action \(returns reason\)\. Tool use guarantees a structured, parseable response regardless of model verbosity\.

**Concurrency:** `_cached_proposal` is read by `evaluate()` and written by the background `_call_llm()` task\. An `asyncio.Lock` MUST guard both accesses to prevent a TOCTOU race\. Logging MUST remain outside the lock to avoid holding it during I/O\.

**setup\(\) and preloading:** `LLMInstinctNode.setup()` calls `provider.preload()` if the injected provider supports it \(e\.g\. `LocalProvider`\)\. This loads the GGUF model eagerly before the first tick, avoiding a latency spike on the first `evaluate()` call\.

**Plain\-text completion surface:** Every `LLMProvider` additionally exposes `async complete_text\(prompt, *, system="", max_tokens=None\) \-> str`, a convenience method for consumers that need unstructured text rather than a structured tool call\. Concrete providers override `_complete_text_sync\(\)` to read the assistant text directly \(Anthropic: `TextBlock\.text`; Ollama/Local: `message\.content`\); the base `complete_text\(\)` wraps that helper in `asyncio\.to_thread\(\)`\. The tool\-calling `complete\(\)` path is not reused because it discards assistant text blocks, so a default adapter would silently return an empty string\. `ThreadSafeProvider` overrides `_complete_text_sync\(\)` to acquire the same lock as `complete\(\)`, so concurrent text and tool\-calling paths are mutually serialised\.

**Thread\-safe model sharing:** On memory\-constrained devices multiple LLMInstinctNode instances must share a single model\. `ThreadSafeProvider` wraps any `LLMProvider` with a `threading\.Lock` so concurrent `asyncio\.to\_thread\(\)` calls are serialised — preventing corruption in libraries like llama\-cpp\-python that are not thread\-safe\. `SharedModelRegistry` is a keyed cache that creates a `ThreadSafeProvider` on first access and returns the same instance on subsequent requests:

    registry = SharedModelRegistry\(\)

    provider = registry\.get\_or\_create\(

        "llama\-8b",

        lambda: LocalProvider\(model\_path="/models/llama\.gguf"\),

    \)

    node\_a = CuriosityInstinct\(bus=bus, provider=provider\)

    node\_b = SocialInstinct\(bus=bus, provider=provider\)

### __Developer Contract__

- Extend BaseInstinctNode and implement evaluate\(\)\.
- Keep evaluate\(\) fast\. Move heavy computation to setup\(\) or a background task\.
- Set priority to reflect urgency class: 100\+ for safety/survival, 50\-99 for goal\-directed, 1\-49 for exploratory\.
- Return None explicitly when the instinct does not apply\. Do not raise\.
- For LLM\-backed instincts, extend LLMInstinctNode instead of BaseInstinctNode\. Set min\_interval\_s to control API call frequency\. Never use LLMInstinctNode as a reflex node — LLM latency is incompatible with the reflex arc requirement\.
- Use trigger\_interval\_s to rate\-limit instincts that should not run every tick \(e\.g\. ReflectionInstinct, CuriosityInstinct\)\.
- Use trigger\_on\_signals to gate instincts on specific signal kinds \(e\.g\. SocialInstinct fires only when "face", "speech", or "proximity" signals are present\)\. Both trigger\_on\_signals and trigger\_interval\_s can be combined — the signal gate is checked first, then the cooldown\.

### __Example__

class OverheatInstinct\(BaseInstinctNode\):

    priority = 100          \# safety — always wins

    async def evaluate\(self, ctx: Context\) \-> Proposal | None:

        thermal = \[s for s in ctx\.signals if s\.kind == 'thermal'\]

        if not thermal:

            return None

        if thermal\[\-1\]\.value > 80\.0:    \# °C threshold

            return Proposal\(

                instinct\_id=self\.node\_id,

                action\_id='SetTemperatureActionNode',

                priority=self\.priority,

                urgency=min\(1\.0, \(thermal\[\-1\]\.value \- 80\) / 20\),

                parameters=\{'target\_celsius': 60\.0\},

                rationale='Temperature above safe threshold',

            \)

        return None

__5\.5  Reflex Instinct Nodes__

ReflexInstinctNodes model the biological reflex arc — a neural pathway that triggers a motor response without involving the brain\. In the same way that an animal instinctively retracts from a flame without conscious deliberation, a ReflexInstinctNode short\-circuits the normal sense → instinct → decide → act pipeline and dispatches an action immediately when a critical condition is detected\.

This is the most significant departure from a standard reactive agent architecture and is one of the primary novelty contributions of the Arachnite framework\.

### __How Reflex Differs from Normal Instinct__

__Property__

__Normal InstinctNode vs ReflexInstinctNode__

__Pipeline position__

Normal: evaluated with all instincts, result passed to DecisionNode\.
Reflex: evaluated before all instincts, dispatched immediately if triggered\.

__Decision layer__

Normal: DecisionNode always chooses the final action\.
Reflex: DecisionNode is bypassed entirely\.

__Normal instincts__

Normal: unaffected\.
Reflex: still run after the reflex fires; their proposals are evaluated by DecisionNode for the same tick\.

__Conflicts__

Multiple reflexes can fire in one tick\. They are dispatched in priority order, all before the decision step\.

__Use cases__

Emergency stop, thermal shutdown, collision avoidance, power failure response\.

### __BaseReflexInstinctNode__

class BaseReflexInstinctNode\(BaseInstinctNode, ABC\):

    """

    A ReflexInstinctNode bypasses the DecisionNode entirely\.

    If evaluate\(\) returns a Proposal, InstinctMasterNode dispatches

    it directly via ActionMasterNode before the normal instinct pass\.

    """

    reflex: bool = True          \# always True; do not override

    priority: int                \# higher priority reflexes fire first

                                 \# if multiple reflexes trigger same tick

    @abstractmethod

    async def evaluate\(self, ctx: Context\) \-> Proposal | None:

        """Same contract as BaseInstinctNode\.evaluate\(\)\.

        Return a Proposal to trigger the reflex; return None to pass\.

        Keep this method extremely fast — it runs synchronously before

        the rest of the pipeline\. No LLM calls, no blocking I/O\."""

### __InstinctMasterNode — Updated Interface__

InstinctMasterNode now maintains two separate registries: one for reflex nodes and one for normal instinct nodes\. The evaluate\_reflexes\(\) call happens first each tick\.

class InstinctMasterNode\(BaseNode\):

    def register\(self, node: BaseInstinctNode\) \-> None:

        """Register a normal or reflex instinct\.

        Reflex nodes \(node\.reflex == True\) go to the reflex registry\."""

    def get\_node\(self, node\_id: str\) \-> BaseInstinctNode | None: \.\.\.

    def unregister\(self, node\_id: str\) \-> None: \.\.\.

    async def evaluate\_reflexes\(self, ctx: Context\) \-> list\[Proposal\]:

        """Evaluate all ReflexInstinctNodes concurrently\.

        Returns non\-None proposals sorted by priority descending\.

        Called by the runtime before evaluate\_all\(\)\."""

    async def evaluate\_all\(self, ctx: Context\) \-> list\[Proposal\]:

        """Evaluate all normal InstinctNodes concurrently\.

        Reflex nodes are excluded from this call\."""

### __Developer Contract__

- Extend BaseReflexInstinctNode instead of BaseInstinctNode for emergency or safety\-critical responses\.
- Keep evaluate\(\) as fast as possible\. It is called every tick before anything else\. No I/O, no network calls, no model inference\.
- A reflex node with priority 200\+ will always fire before a reflex node with priority 100\.
- Do not use reflex nodes for goal\-directed or exploratory behaviour\. They are exclusively for situations requiring sub\-tick response\.
- A reflex action that fires does not prevent normal instincts from also producing proposals for DecisionNode in the same tick\.

### __Example__

class EmergencyStopReflex\(BaseReflexInstinctNode\):

    """Fires instantly if any proximity sensor reads below safe distance\.

    Bypasses all decision logic — the agent stops first, thinks later\."""

    priority = 200

    async def evaluate\(self, ctx: Context\) \-> Proposal | None:

        proximity = \[s for s in ctx\.signals if s\.kind == 'proximity'\]

        if any\(s\.value < 0\.15 for s in proximity\):   \# metres

            return Proposal\(

                instinct\_id=self\.node\_id,

                action\_id='EmergencyStopActionNode',

                priority=self\.priority,

                urgency=1\.0,

                parameters=\{\},

                rationale='Obstacle within 15 cm — reflex stop',

            \)

        return None

__5\.6  Decision Nodes__

DecisionNodes receive the full list of Proposals from InstinctMasterNode and select exactly one to execute\. The framework ships with two built\-in strategies; developers can extend BaseDecisionNode to implement their own\.

### __BaseDecisionNode__

class BaseDecisionNode\(BaseNode, ABC\):

    @abstractmethod

    async def decide\(self, proposals: list\[Proposal\]\) \-> Proposal | None:

        """Select one Proposal from the list, or return None if no action

        should be taken this tick\. Receives proposals sorted by priority

        descending\. The list may be empty\."""

### __DecisionMasterNode__

Wraps the active DecisionNode and exposes the decide\(\) interface to the runtime\. Only one DecisionNode is active at a time; it can be swapped at runtime to change strategy\.

### __Built\-in Strategies__

__Strategy__

__Behaviour__

__GreedyDecisionNode__

Returns the proposal with the highest priority\. Ties broken by urgency\. Simple and predictable\.

__WeightedDecisionNode__

Selects the proposal with the highest combined score: score = priority × urgency\. Good when multiple instincts partially apply\.

__RandomDecisionNode__

Samples probabilistically from proposals weighted by urgency\. Useful for exploratory or creative agents\.

__ActiveInferenceDecisionNode__

Selects proposals by minimising expected free energy \(EFE\), trading off pragmatic value \(goal achievement\) against epistemic value \(uncertainty reduction\)\. EFE\(p\) = \-priority\(p\) × urgency\(p\) \+ β × \(1 \- avg\_confidence\(p\)\)\. Parameters: `beta` \(0\.0 = pure exploitation equivalent to WeightedDecisionNode; \>0 = exploration\-exploitation balance\), `temperature` \(0\.0 = deterministic argmin; \>0 = softmax probabilistic selection\), `prior_confidence` \(default confidence when evidence lacks \_confidence keys\)\. Confidence is extracted from the proposal's `evidence` dict \(keys ending in `_confidence`\)\. Based on Friston's free\-energy principle and Pezzato et al\.'s active inference for robotics\.

### __Multi\-Proposal Selection \(Concurrent Dispatch\)__

BaseDecisionNode also exposes decide\_many\(\), which selects the best proposal per unique action\_id for concurrent dispatch\. The default implementation iterates proposals by priority descending and picks one per action\_id, skipping those already running\. All built\-in strategies inherit this default\.

DecisionMasterNode\.on\_new\_proposals\_many\(\) extends this further: it calls decide\_many\(\) to select proposals, checks whether any outrank currently running actions \(issuing InterruptRequests where applicable\), and filters out proposals for action\_ids that are running and not being interrupted\.

### __Proposal Persistence__

DecisionMasterNode manages persistent proposals across ticks\.  A ``Proposal`` with ``persist=True`` that is not dispatched is carried forward to subsequent ticks and merged into the decision pool alongside new proposals\.  The ``max\_pending\_ticks`` parameter \(default 50\) limits how long a pending proposal survives without being dispatched\.

Supersession rules:

- New ``persist=True`` proposal from the same instinct replaces the pending one\.
- An instinct that was evaluated \(passed signal gate and throttle\) but did not produce a ``persist=True`` proposal clears its pending entry — conditions have changed\.
- Throttled or signal\-gated instincts \(not in ``evaluated\_instinct\_ids``\) retain their pending proposal\.
- A dispatched proposal is removed from the pending pool\.
- A pending proposal that exceeds ``max\_pending\_ticks`` is dropped\.
- When an instinct is unregistered at runtime via ``unregister\_instinct\_live()``, ``DecisionMasterNode.clear\_pending(instinct\_id)`` removes its pending proposal and age tracking entry, preventing orphaned state from accumulating over long runs\.

The ``evaluated\_instinct\_ids`` set is populated by InstinctMasterNode during evaluate\_all\(\) and passed to on\_new\_proposals\_many\(\) by the runtime\.  This distinguishes "instinct said no" \(clear pending\) from "instinct wasn't asked" \(keep pending\)\.

### __Developer Contract__

- Extend BaseDecisionNode and implement decide\(\)\.
- Optionally override decide\_many\(\) for custom multi\-select logic\.
- The proposals list is pre\-sorted by priority descending\.
- Returning None is valid and means 'take no action this tick'\.
- The decision strategy can be swapped at runtime via DecisionMasterNode\.set\_strategy\(\)\.

__5\.7  Action Nodes__

ActionNodes are the effectors of the system\. Each one knows how to perform a single concrete operation, whether that is moving a motor, sending a message, calling an API, or adjusting a hardware parameter\. They receive a Proposal and return a Result\.

### __BaseActionNode__

class BaseActionNode\(BaseNode, ABC\):

    timeout\_s: float = 5\.0     \# max execution time before TimeoutError

    max\_retries: int = 0       \# automatic retries on transient failure

    @abstractmethod

    async def execute\(self, proposal: Proposal\) \-> Result:

        """Carry out the action described by the proposal\.

        Read parameters from proposal\.parameters\.

        Always return a Result — never raise unless unrecoverable\."""

    async def on\_timeout\(self, proposal: Proposal\) \-> Result:

        """Called when execute\(\) exceeds timeout\_s\.

        Default: return Result\(success=False, \.\.\.\)\. Override to clean up\."""

### __ActionMasterNode__

Routes Proposals to the correct ActionNode by matching proposal\.action\_id to the registered node’s node\_id\. Enforces timeout and retry logic transparently\.

Supports concurrent dispatch: different ActionNodes can execute in parallel via dispatch\_many\(\)\. The same ActionNode MUST NOT run twice concurrently\. This invariant is enforced at two levels: dispatch\_many\(\) pre\-filters proposals whose action\_id is already in the running set, and \_dispatch\_one\(\) performs an atomic check\-and\-set against the \_running\_nodes set before execution begins\. The atomic guard in \_dispatch\_one\(\) prevents a TOCTOU race where concurrent dispatch calls \(e\.g\. a reflex and a normal proposal in the same tick\) could both pass the pre\-filter before either registers\. If the action\_id is already running, \_dispatch\_one\(\) MUST return Result\(success=False\) immediately without invoking execute\(\)\.

class ActionMasterNode\(BaseNode\):

    def register\(self, node: BaseActionNode\) \-> None: \.\.\.

    def get\_node\(self, node\_id: str\) \-> BaseActionNode | None: \.\.\.

    def unregister\(self, node\_id: str\) \-> None: \.\.\.

    async def dispatch\(self, proposal: Proposal\) \-> Result: \.\.\.

    async def dispatch\_many\(self, proposals: list\[Proposal\]\) \-> list\[Result\]: \.\.\.

    def current\_actions\(\) \-> dict\[str, BaseActionNode\]: \.\.\.

    def current\_proposals\(\) \-> dict\[str, Proposal\]: \.\.\.

    def running\_action\_ids\(\) \-> set\[str\]: \.\.\.

    \# Backward\-compat singular accessors:

    def current\_action\(\) \-> BaseActionNode | None: \.\.\.

    def current\_proposal\(\) \-> Proposal | None: \.\.\.

### __Developer Contract__

- Set node\_id to the action\_id that instincts will reference in their Proposals\.
- Read action parameters from proposal\.parameters\. Define a schema and validate at the top of execute\(\)\.
- Return Result\(success=False, error=exc\) instead of raising for expected failure modes\.
- Use timeout\_s and max\_retries to guard against hardware hangs without custom retry logic\.

### __Example__

class SetTemperatureActionNode\(BaseActionNode\):

    node\_id = 'SetTemperatureActionNode'

    timeout\_s = 3\.0

    async def execute\(self, proposal: Proposal\) \-> Result:

        target = proposal\.parameters\.get\('target\_celsius', 20\.0\)

        try:

            await asyncio\.to\_thread\(self\.\_hw\_set\_temp, target\)

            return Result\(action\_id=self\.node\_id, success=True, output=target\)

        except HardwareError as e:

            return Result\(action\_id=self\.node\_id, success=False,

                          output=None, error=e\)

# __6\. Node Supervisors__

In a deployed agent running on real hardware, nodes can fail\. A sensor thread can crash, an action can hang indefinitely, a network connection can drop\. The NodeSupervisor system provides each master node with a built\-in health management layer that monitors its child nodes, restarts them on failure, and broadcasts state changes onto the SignalBus so the rest of the agent can react\.

Each master node owns exactly one NodeSupervisor instance\. A HealthMonitor at the runtime level aggregates status from all four supervisors and provides a single system\-wide health view\.

## __6\.1 NodeState__

Every managed node has a lifecycle state tracked by its supervisor:

__State__

__Meaning__

__STARTING__

setup\(\) is in progress\.

__RUNNING__

Node is operating normally\.

__FAULTED__

Node raised an unhandled exception\. Awaiting restart decision\.

__RESTARTING__

Supervisor is calling teardown\(\) then setup\(\) to recover the node\.

__STOPPED__

Node was intentionally stopped by the runtime or supervisor\.

__DEAD__

Node exceeded max\_restarts and will not be restarted\.

## __6\.2 NodeSupervisor__

Attached to each master node\. Tracks the state of every registered child node and applies a restart policy when a node enters the FAULTED state\.

class NodeSupervisor:

    def \_\_init\_\_\(

        self,

        bus: SignalBus,

        restart\_policy: RestartPolicy = RestartPolicy\.ON\_FAILURE,

        max\_restarts: int = 3,

        restart\_delay\_s: float = 1\.0,

    \): \.\.\.

    def track\(self, node: BaseNode\) \-> None:

        """Begin supervising a node\. Called automatically by

        master node register\(\) methods\."""

    def untrack\(self, node\_id: str\) \-> None: \.\.\.

    def state\_of\(self, node\_id: str\) \-> NodeState: \.\.\.

    def all\_states\(self\) \-> dict\[str, NodeState\]:

        """Returns a snapshot of \{node\_id: NodeState\} for all tracked nodes\."""

    def is\_healthy\(self\) \-> bool:

        """Returns True if no tracked node is in FAULTED or DEAD state\."""

    async def restart\(self, node\_id: str\) \-> None:

        """Manually trigger a restart for a specific node\.

        Calls teardown\(\) then setup\(\), transitions through RESTARTING\."""

    async def cancel\_restart\_tasks\(self\) \-> None:

        """Cancel all in\-flight restart tasks\. MUST be called during

        shutdown phase 5 before marking nodes as STOPPED\."""

    @property

    def restart\_task\_count\(self\) \-> int:

        """Number of restart tasks currently in flight\."""

NodeSupervisor MUST track every restart coroutine as a task in an internal `\_restart\_tasks` set\. When a restart completes or is cancelled, it MUST be removed from the set\. `cancel\_restart\_tasks\(\)` cancels all tasks in the set and awaits their cancellation\.

## __6\.3 RestartPolicy__

class RestartPolicy\(Enum\):

    NEVER        = 'never'         \# never restart; go straight to DEAD

    ON\_FAILURE   = 'on\_failure'    \# restart only on unhandled exception

    ALWAYS       = 'always'        \# restart on any non\-STOPPED exit

## __6\.4 SupervisorSignal__

Whenever a node changes state, the supervisor publishes a SupervisorSignal onto the SignalBus with kind 'supervisor'\. This allows InstinctNodes — including ReflexInstinctNodes — to react to node failures in the same tick they are detected\.

@dataclass

class SupervisorSignal\(Signal\):

    kind: str = 'supervisor'       \# always 'supervisor'

    node\_id: str = ''              \# the node that changed state

    previous\_state: NodeState = NodeState\.STARTING

    current\_state: NodeState  = NodeState\.RUNNING

    restart\_count: int = 0

    fault_error: BaseException | None = None

### __6\.4\.1 NodeFaultSignal__

When a node transitions to FAULTED or DEAD with an error, the supervisor also publishes a `NodeFaultSignal` with kind `'node_fault'`\. This allows instincts to subscribe specifically to faults via `bus\.subscribe\("node_fault", \.\.\.\)` without filtering all supervisor signals\.

@dataclass

class NodeFaultSignal\(SupervisorSignal\):

    kind: str = 'node\_fault'

    error\_type: str = ''         \# e\.g\. 'ValueError', auto\-populated from fault\_error

    error\_message: str = ''      \# e\.g\. 'bad value', auto\-populated from fault\_error

Both `SupervisorSignal` \(kind='supervisor'\) and `NodeFaultSignal` \(kind='node\_fault'\) are emitted on fault transitions — existing subscribers to 'supervisor' are unaffected\.

## __6\.5 HealthMonitor__

The HealthMonitor is owned by ArachniteRuntime and aggregates the health status of all four master node supervisors into a single system\-wide view\.

class HealthMonitor:

    def \_\_init\_\_\(self, supervisors: list\[NodeSupervisor\]\): \.\.\.

    def system\_healthy\(self\) \-> bool:

        """True if every supervisor reports is\_healthy\(\)\."""

    def report\(self\) \-> dict\[str, dict\[str, NodeState\]\]:

        """Returns a nested dict: \{supervisor\_id: \{node\_id: NodeState\}\}\.

        Useful for logging, dashboards, and test assertions\."""

    def nodes\_in\_state\(self, state: NodeState\) \-> list\[str\]:

        """Returns node\_ids of all nodes currently in the given state

        across all supervisors\."""

## __6\.6 Interaction with Reflex Nodes__

The combination of the supervisor system and reflex instincts produces an important emergent behaviour: an agent can automatically react to its own component failures\. A NodeFaultReflex subscribed to 'supervisor' signals can trigger a safe\-mode action the moment a critical sensor enters FAULTED state, before any normal instinct has a chance to act on stale or missing data\.

class NodeFaultReflex\(BaseReflexInstinctNode\):

    """If a critical sensor goes FAULTED, immediately trigger safe mode\.

    Fires as a reflex — the agent enters safe mode before anything else

    processes the missing sensor data\."""

    priority = 250

    critical\_nodes = \{'TemperatureSenseNode', 'ProximitySenseNode'\}

    async def evaluate\(self, ctx: Context\) \-> Proposal | None:

        faults = \[

            s for s in ctx\.signals

            if s\.kind == 'supervisor'

            and s\.node\_id in self\.critical\_nodes

            and s\.current\_state == NodeState\.FAULTED

        \]

        if faults:

            return Proposal\(

                instinct\_id=self\.node\_id,

                action\_id='SafeModeActionNode',

                priority=self\.priority,

                urgency=1\.0,

                parameters=\{'faulted\_nodes': \[s\.node\_id for s in faults\]\},

                rationale='Critical sensor faulted — entering safe mode',

            \)

        return None

## __6\.5 Runtime Safety Monitors__

The safety monitor subsystem provides continuous runtime verification of the safety properties formalised in the UPPAAL model\. Monitors are lightweight checkers attached to the runtime that verify invariants after each tick and emit `SafetyViolationSignal` on the `SignalBus` when an invariant is breached\.

### __BaseSafetyMonitor__

Abstract base class\. Subclass and implement `check\(tick, state\)` to verify a specific invariant\.

    class BaseSafetyMonitor\(ABC\):
        monitor\_id: str
        async def check\(self, tick: int, state: MonitorState\) \-> SafetyViolationSignal | None
        async def emit\_violation\(property\_name, severity, details\) \-> SafetyViolationSignal

### __MonitorState__

Snapshot of runtime state provided to monitors each tick: tick, reflex\_fired, reflex\_action\_dispatched, decision\_entered, mandatory\_block\_active, interrupt\_accepted\_during\_block, active\_reflex\_nodes, total\_reflex\_nodes, tick\_duration\_ms, tick\_budget\_ms\.

### __SafetyMonitorRegistry__

Manages a set of monitors\. `SafetyMonitorRegistry\.default\(bus\)` creates all five standard monitors\.

### __Built\-in Monitors__

| Monitor | Property | Severity |
|---|---|---|
| `ReflexBypassMonitor` | reflex\_fired ⟹ ¬decision\_entered | CRITICAL |
| `MandatoryBlockMonitor` | mandatory\_block ⟹ ¬interrupted | CRITICAL |
| `ReflexDispatchMonitor` | reflex\_fired ⟹ action\_dispatched | CRITICAL |
| `ReflexAvailabilityMonitor` | all reflex nodes available | WARNING |
| `TickBudgetMonitor` | 3\+ consecutive tick overruns | WARNING |

Violations are published as `SafetyViolationSignal` \(kind `"safety_violation"`\) with `monitor_id`, `severity`, `property_name`, and `details`\. Reflex instincts can subscribe and trigger compensating actions\.

