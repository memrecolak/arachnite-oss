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

    async def push\(self, signal: Signal\) \-> None:

        """Emit an event\-driven observation now, outside the tick\.

        Publishes the signal on the bus immediately — an event\-mode reflex

        whose trigger\_on\_signals includes its kind is evaluated before

        push\(\) returns \(§7\.2\.3\) — and queues it so the next read\_all\(\)

        also hands it to the ContextNode, once\. Call it from a background

        task started in setup\(\)\. A bus subscriber that raises is logged,

        not propagated\. A StateUpdateSignal is published only \(the runtime

        already buffers it, §5\.3\)\."""

### __SenseMasterNode__

Owns a collection of BaseSenseNode instances\. On each tick it calls read\(\) on all of them concurrently, publishes the resulting signals to the bus, and hands the signal list to the ContextNode\.

class SenseMasterNode\(BaseNode\):

    def register\(self, node: BaseSenseNode\) \-> None: \.\.\.

    def get\_node\(self, node\_id: str\) \-> BaseSenseNode | None: \.\.\.

    def unregister\(self, node\_id: str\) \-> None: \.\.\.

    async def read\_all\(self\) \-> list\[Signal\]: \.\.\.

    def returned\_this\_tick\(self, signal: Signal\) \-> bool: \.\.\.

`read_all()` never lets a raising bus subscriber \(a dashboard, a logging hook, a transport\) abort the tick: every publish completes, the `SignalBusError` is logged at ERROR, and the reflex and deliberation stages run as usual\. `returned_this_tick()` reports whether a signal is one of the objects the most recent `read_all()` returned \(and published\), by identity\. A bus subscriber that defers what it receives to a later tick uses it to skip this tick’s own sense output — see **StateUpdateSignal** below\.

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

**Timestamps are per\-sensor:** `read\_all\(\)` captures a fresh `time\.monotonic\(\)` for each sensor individually, and `poll\_interval\_s` is the minimum spacing between the *starts* of two reads, as ticks are spaced between tick starts \(less a slack of one monotonic\-clock resolution plus 1 ms, so a tick that wakes a clock tick early is not skipped\)\. A sensor whose `poll\_interval\_s` equals the tick period is therefore read on every tick, whatever its `read\(\)` duration, and no two reads start within one interval\.

**Important:** if your tick rate is higher than 1/poll\_interval\_s, the sensor will not produce a signal every tick\. Instincts that depend on this sensor's signals will see no matching signals on throttled ticks\. This is by design — set `poll\_interval\_s` to match your hardware's actual read rate\.

### __Developer Contract__

- Extend BaseSenseNode and implement read\(\)\.
- Set signal\_kind to a string that other nodes can subscribe to\.
- Set poll\_interval\_s to match the sensor's actual read rate \(default 0\.1 s\)\. Set to `0\.0` for virtual or very fast sensors\.
- Never cache state in read\(\) — each call should be a fresh observation\.
- For interrupt\-driven hardware \(GPIO edge, ToF threshold\), additionally call push\(\) from a background task so an event\-mode reflex sees the stimulus without waiting for the next tick\.

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

The ContextNode is the working memory of the agent\. It receives all signals from the current tick and assembles a Context snapshot that is passed to every InstinctNode\. It also stores the Results of the actions that completed since the previous tick \(§7\.2\.1\), giving instincts access to feedback\.

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

    def update\(self, signals: list\[Signal\], result: Result | None = None,

               action\_state: ActionExecutionState | None = None,

               results: list\[Result\] | None = None,

               action\_states: list\[ActionExecutionState\] | None = None\) \-> Context:

        """Merge this tick’s signals, completed results and running action

        states into a new Context\. StateUpdateSignals in the signal list

        are applied to \_state before the snapshot is built, so instincts

        see updated state in the same tick the signal was emitted\."""

    def reflex\_view\(self, signals: list\[Signal\]\) \-> Context:

        """Lightweight Context for the event\-driven reflex lane \(§7\.2\.3\):

        the given signals, the current state, results and action states,

        and an empty history \(no per\-event history copy\)\. Never raises,

        does not mutate the node\."""

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

**StateUpdateSignal:** Any node can write to `ContextNode._state` without holding a direct reference to ContextNode by emitting a `StateUpdateSignal`\. Timing depends on the emitter\. A **sense node** returning one from `read()` has it applied in the **same tick** — it rides the sense output into `update()`, which intercepts it before the snapshot is built and before instincts run\. **Any other node** \(instinct / action / reflex\) publishing one onto the bus has it buffered by the runtime and applied at the **start of the next tick** \(N\+1\), mirroring the `node_fault` path: instincts and actions run after `update()`, so a write they emit cannot land in the tick it was published\. `read_all()` also publishes the sense output on the bus for subscribers; the runtime recognises that copy \(`SenseMasterNode.returned_this_tick()`\) and does **not** buffer it, since it has already been applied this tick — otherwise the echo would be re\-applied at the start of the next tick, after and therefore over that tick’s fresh reading, leaving every sense\-written key one tick stale\. A genuine bus write still wins over a same\-key sense write within one drain\.

@dataclass

class StateUpdateSignal\(Signal\):

    kind: str = ‘state\_update’   \# always ‘state\_update’

    key: str = ‘’                \# the state dict key to write

    state\_value: Any = None      \# the value to store \(ignored when delete=True\)

    delete: bool = False         \# True → remove key from state

Example \(publishing on the bus — applied next tick, N\+1\)::

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

                                    \# On a reflex node it is not a gate: with

                                    \# ArachniteRuntime\(reflex\_mode='event'\) it

                                    \# moves the reflex to the event\-driven lane

                                    \# \(§7\.2\.3\); in 'tick' mode it is ignored\.

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

**Not a request/response primitive\.** ``LLMInstinctNode`` is a cached, cooldown\-throttled reactive control input: ``evaluate\(\)`` returns the last cached ``Proposal`` immediately and only spawns a fresh model call when ``min_interval_s`` has elapsed \(see §7\.2 on the tick loop as a sampling cadence\)\. It is therefore **wrong** for an interactive "user asks, waits for an answer" flow — there is no synchronous answer to await\. For that, call the model SDK \(or ``provider\.complete_text\(\)``\) directly in your own loop, outside the tick path\.

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
Reflex: evaluated before all instincts \(or, in reflex\_mode='event', the moment a trigger signal arrives\), launched immediately if triggered\.

__Decision layer__

Normal: DecisionNode always chooses the final action\.
Reflex: DecisionNode is bypassed entirely\.

__Normal instincts__

Normal: unaffected\.
Reflex: still run after the reflex fires; their proposals are evaluated by DecisionNode for the same tick\.

__Conflicts__

Multiple reflexes can fire in one tick\. They are launched in priority order, all before the decision step\.

__Running actions__

Normal: a conflicting running action is interrupted only if it is interruptible, not reflex\-owned and of lower priority; never inside a mandatory block \(§17\.6\)\.
Reflex: takes the actuators it needs from a running action according to its ``preemption`` mode, even inside a mandatory block under ``PREEMPT`` \(§17\.12\)\.

__Use cases__

Emergency stop, thermal shutdown, collision avoidance, power failure response\.

### __BaseReflexInstinctNode__

class BaseReflexInstinctNode\(BaseInstinctNode, ABC\):

    """

    A ReflexInstinctNode bypasses the DecisionNode entirely\.

    If evaluate\(\) returns a Proposal, the runtime launches it directly

    via ActionMasterNode\.launch\_reflex\(\) before the normal instinct pass\.

    """

    reflex: bool = True          \# always True; do not override

    priority: int                \# higher priority reflexes fire first

                                 \# if multiple reflexes trigger same tick

    preemption: ReflexPreemption = ReflexPreemption\.PREEMPT

                                 \# what to do when the target action needs

                                 \# a resource a running action owns \(§17\.12\)

    trigger\_on\_signals: list\[str\] | None = None

                                 \# reflex\_mode='event': evaluate on arrival

                                 \# of these signal kinds \(§7\.2\.3\)

    @abstractmethod

    async def evaluate\(self, ctx: Context\) \-> Proposal | None:

        """Same contract as BaseInstinctNode\.evaluate\(\)\.

        Return a Proposal to trigger the reflex; return None to pass\.

        Keep this method extremely fast — it runs before the rest of the

        pipeline \(or inside the publisher, in the event lane\)\. No LLM

        calls, no blocking I/O\."""

### __InstinctMasterNode — Updated Interface__

InstinctMasterNode now maintains two separate registries: one for reflex nodes and one for normal instinct nodes\. The evaluate\_reflexes\(\) call happens first each tick\.

class InstinctMasterNode\(BaseNode\):

    def register\(self, node: BaseInstinctNode\) \-> None:

        """Register a normal or reflex instinct\.

        Reflex nodes \(node\.reflex == True\) go to the reflex registry\."""

    def get\_node\(self, node\_id: str\) \-> BaseInstinctNode | None: \.\.\.

    def unregister\(self, node\_id: str\) \-> None: \.\.\.

    async def evaluate\_reflexes\(

        self, ctx: Context,

        nodes: Sequence\[BaseReflexInstinctNode\] | None = None,

    \) \-> list\[Proposal\]:

        """Evaluate the enabled ReflexInstinctNodes concurrently — all of

        them, or only ``nodes`` \(the tick stage's or the event lane's

        subset\)\. Returns non\-None proposals sorted by priority descending\.

        With reflex\_conflict='raise', equal top priorities raise

        ReflexConflictError carrying every proposal; the runtime logs it

        at ERROR and still launches them in priority order\.

        Called by the runtime before evaluate\_all\(\)\."""

    async def evaluate\_all\(self, ctx: Context\) \-> list\[Proposal\]:

        """Evaluate all normal InstinctNodes concurrently\.

        Reflex nodes are excluded from this call\."""

### __Developer Contract__

- Extend BaseReflexInstinctNode instead of BaseInstinctNode for emergency or safety\-critical responses\.
- Keep evaluate\(\) as fast as possible\. It is called every tick before anything else, or on every arriving trigger signal in the event lane\. No I/O, no network calls, no model inference\.
- A reflex node with priority 200\+ will always fire before a reflex node with priority 100\.
- Do not use reflex nodes for goal\-directed or exploratory behaviour\. They are exclusively for situations requiring sub\-tick response\.
- A reflex action that fires does not prevent normal instincts from also producing proposals for DecisionNode in the same tick\. Those proposals cannot take the reflex action's resources: they are blocked or held while the reflex owns or has claimed them\.
- Choose ``preemption``: ``PREEMPT`` \(default\) when the hazard outweighs finishing the running action's mandatory block; ``WAIT\_FOR\_BLOCK`` when interrupting the block is the greater hazard\. Give the target action ``resources`` so the arbitration can see the conflict\.
- Set ``trigger\_on\_signals`` when the runtime uses ``reflex\_mode='event'``\.

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

DecisionMasterNode\.on\_new\_proposals\_many\(\) extends this further: it calls decide\_many\(\) to select proposals, then checks each selected proposal against the running actions it conflicts with — same action\_id, shared declared resources, or resources claimed by an armed reflex \(the runtime passes ``conflicts=ActionMasterNode\.conflicting\_owners``\)\. A proposal with no conflict is dispatched\. If every conflicting owner is running, interruptible, not reflex\-owned and of lower priority, an InterruptRequest is issued to each owner \(``target\_action\_id``\) and the proposal is dispatched; otherwise it is held \(§17\.6\)\.

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

### __Hand\-off Pool__

A dispatched proposal whose launch is ``BLOCKED`` — typically because it just interrupted an owner that is still unwinding — is handed back with ``DecisionMasterNode\.park\(proposal\)``\. Parked proposals \(``handoff\_proposals``\) re\-enter the decision pool every tick, regardless of ``persist``, until they launch\. A parked proposal is superseded only by a new proposal \(persist or not\) from the same instinct — an instinct that goes quiet does not cancel it — and is dropped after ``max\_handoff\_ticks`` \(default 100\)\. ``clear\_pending\(\)`` also removes it\.

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

    resources: frozenset\[str\] = frozenset\(\)

                               \# actuators this action commands, e\.g\.

                               \# frozenset\(\{'arm', 'gripper'\}\) \(§17\.12\)

    safe\_stop\_timeout\_s: float = 1\.0

                               \# bound on on\_preempted\(\); an overrun is

                               \# logged and abandoned

    @abstractmethod

    async def execute\(self, proposal: Proposal\) \-> Result:

        """Carry out the action described by the proposal\.

        Read parameters from proposal\.parameters\.

        Always return a Result — never raise unless unrecoverable\.

        Must be cancellation\-safe: a reflex preemption, emergency\_stop\(\)

        or shutdown arrives as asyncio\.CancelledError at an await\."""

    async def on\_timeout\(self, proposal: Proposal\) \-> Result:

        """Called when execute\(\) exceeds timeout\_s\.

        Default: return Result\(success=False, \.\.\.\)\. Override to clean up\."""

    async def on\_preempted\(self, proposal: Proposal,

                           preempted\_by: Proposal\) \-> None:

        """Safe\-stop hook, run before the resources are released when a

        reflex \(preempted\_by\) preempts this action — after cancelling

        execute\(\), or at the boundary after a block under WAIT\_FOR\_BLOCK

        — and when emergency\_stop\(\), shutdown or cancel\(\) cancels it\.

        Bring the actuators to a safe hold — do not undo work\.

        Default: no\-op\."""

### __ActionMasterNode__

Routes Proposals to the correct ActionNode by matching proposal\.action\_id to the registered node’s node\_id\. Enforces timeout and retry logic transparently, and arbitrates actuator resources \(§17\.12\)\.

Two dispatch styles share one reservation path\. ``launch\(\)`` / ``launch\_reflex\(\)`` — used by ArachniteRuntime — perform the action lookup, conflict check, guards and resource reservation synchronously, start ``execute\(\)`` as a tracked task and return a ``LaunchOutcome`` at once; finished Results are collected with ``drain\_completed\(\)``\. ``dispatch\(\)`` / ``dispatch\_many\(\)`` await the Result \(runtime\-less use, §7\.2\.2\)\.

A proposal conflicts with a running action when they share the action\_id or a declared resource\. At most one running action owns a resource at any time, and the same ActionNode MUST NOT run twice concurrently\. Because the check and the reservation happen with no ``await`` in between, two launches or dispatches in the same tick cannot both pass the check\. A conflicting deliberative launch returns ``LaunchStatus\.BLOCKED``; a conflicting ``dispatch\(\)`` returns ``Result\(success=False\)`` without invoking execute\(\) — ``error=ResourceConflictError`` for a shared resource\. ``dispatch\_many\(\)`` additionally keeps only the highest\-priority proposal per action\_id and skips running ones\. Retries \(``max\_retries``\) stop on an interrupted or preempted Result and on a ``RollbackError``\.

class LaunchStatus\(Enum\):

    LAUNCHED  = 'launched'    \# execute\(\) started as a tracked task

    BLOCKED   = 'blocked'     \# conflicts with a running owner; not started

    ARMED     = 'armed'       \# reflex waits for its owners to release

    DEDUPED   = 'deduped'     \# reflex is already being handled

    VETOED    = 'vetoed'      \# an ActionGuard denied the proposal

    NOT\_FOUND = 'not\_found'   \# no ActionNode with that action\_id

    CLOSED    = 'closed'      \# master closed \(shutdown\); nothing new starts

@dataclass\(frozen=True\)

class LaunchOutcome:

    status: LaunchStatus

    action\_id: str

    owners: frozenset\[str\] = frozenset\(\)  \# running/armed action\_ids it

                                          \# conflicted with

class ActionMasterNode\(BaseNode\):

    def \_\_init\_\_\(self, bus, \.\.\., guards: list\[ActionGuard\] | None = None,

                 strict\_resources: bool = False\): \.\.\.

                 \# strict\_resources: register\(\) raises NodeRegistrationError

                 \# for an action that declares no resources

    def register\(self, node: BaseActionNode\) \-> None: \.\.\.

    def get\_node\(self, node\_id: str\) \-> BaseActionNode | None: \.\.\.

    def unregister\(self, node\_id: str\) \-> None: \.\.\.

    \# Non\-blocking launch \(runtime\):

    def launch\(self, proposal: Proposal, \*, origin: str = 'decision'\) \-> LaunchOutcome: \.\.\.

    def launch\_reflex\(self, proposal: Proposal,

                      preemption: ReflexPreemption = ReflexPreemption\.PREEMPT\) \-> LaunchOutcome: \.\.\.

    def drain\_completed\(self\) \-> list\[Result\]: \.\.\.     \# completion order

    async def wait\_idle\(self, timeout: float | None = None\) \-> bool: \.\.\.

    async def cancel\(self, action\_id: str, timeout: float | None = None\) \-> None: \.\.\.

    async def cancel\_all\(self, timeout: float | None = None\) \-> None: \.\.\.

                 \# cancel \+ safe\-stop, no rollback; drops armed reflexes

    def interrupt\_all\(self, request: InterruptRequest\) \-> None: \.\.\.

    \# Awaiting dispatch \(runtime\-less\):

    async def dispatch\(self, proposal: Proposal, \*, origin: str | None = None\) \-> Result: \.\.\.

    async def dispatch\_many\(self, proposals: list\[Proposal\], \*,

                            origin: str | None = None\) \-> list\[Result\]: \.\.\.

    \# Introspection:

    def conflicting\_owners\(self, proposal: Proposal\) \-> set\[str\]: \.\.\.

    def resource\_owners\(self\) \-> dict\[str, str\]: \.\.\.     \# resource → action\_id

    def armed\_reflexes\(self\) \-> dict\[str, Proposal\]: \.\.\.

    def origin\_of\(self, action\_id: str\) \-> str | None: \.\.\.  \# 'reflex' | 'decision' | 'direct'

    def current\_actions\(\) \-> dict\[str, BaseActionNode\]: \.\.\.

    def current\_proposals\(\) \-> dict\[str, Proposal\]: \.\.\.

    def running\_action\_ids\(\) \-> set\[str\]: \.\.\.

    \# Backward\-compat singular accessors:

    def current\_action\(\) \-> BaseActionNode | None: \.\.\.

    def current\_proposal\(\) \-> Proposal | None: \.\.\.

    \# Pre\-dispatch guards \(optional\):

    def register\_guard\(self, guard: ActionGuard\) \-> None: \.\.\.

Deliberative interrupts \(``request\_interrupt\(\)``, ``is\_interruptible\(\)``, ``current\_step\(\)``\) are specified in §17\.6\.

### __Pre\-dispatch Guards__

A guard is an optional, deterministic veto consulted *before* an action executes — the runtime, parameter\-aware complement to ``Permission`` \(which is startup\-validated and parameter\-blind\)\. Use it to deny a concrete invocation based on ``proposal\.parameters`` \(a destructive shell command, a write to an out\-of\-scope path, an out\-of\-range actuation\)\.

class GuardVerdict:        \# allow, or deny with a reason

    allow: bool

    reason: str = ''

class ActionGuard\(Protocol\):

    def \_\_call\_\_\(self, proposal: Proposal\) \-> GuardVerdict: \.\.\.

Guards run in ``launch\(\)`` and ``dispatch\(\)`` after the not\-found and conflict checks, **before** ``execute\(\)`` and **outside** the retry loop\. Semantics:

- On deny, ``dispatch\(\)`` returns ``Result\(success=False, error=ActionVetoed\(reason\)\)`` with **no side effect**: ``execute\(\)`` is never called and no retry runs\. ``dispatch\_many\(\)`` honours guards identically; ``launch\(\)`` returns ``LaunchStatus\.VETOED`` and buffers the same Result for the next tick's ``ctx\.last\_results``\.
- Guards MUST be **synchronous, bounded, and perform no I/O** \(a reflex\-class contract\)\. A coroutine function is rejected at ``register\_guard\(\)``\.
- Guards run in registration order; the **first deny wins** \(remaining guards are skipped\)\.
- A guard that **raises fails closed** \(treated as a deny\)\. This deliberately differs from ``InstinctMasterNode\.set\_pre\_evaluate\_gate``, which fails open — a safety veto layer must not fail open\.
- **The reflex path bypasses all guards\.** Bypass is keyed on the dispatch origin, not on priority: ``launch\_reflex\(\)`` and ``dispatch\(\.\.\., origin='reflex'\)`` skip guards, because silently vetoing a reflex would break the reflex\-always\-fires guarantee \(``ReflexDispatchMonitor``\); a deliberative proposal \(``origin='decision'``\) is checked whatever its priority\. Runtime\-less callers that pass no origin \(``origin=None``\) keep the legacy rule: ``priority >= 200`` bypasses\.

### __Developer Contract__

- Set node\_id to the action\_id that instincts will reference in their Proposals\.
- Read action parameters from proposal\.parameters\. Define a schema and validate at the top of execute\(\)\.
- Return Result\(success=False, error=exc\) instead of raising for expected failure modes\.
- Use timeout\_s and max\_retries to guard against hardware hangs without custom retry logic\.
- Declare ``resources`` for every actuator the action commands; set ``strict\_resources=True`` on ActionMasterNode to make an undeclared action a registration error\.
- Write execute\(\) to be cancellation\-safe, and override ``on\_preempted\(\)`` with a forward safe\-stop if a reflex may take this action's resources mid\-execution\.

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

Whenever a node changes state, the supervisor publishes a SupervisorSignal onto the SignalBus with kind 'supervisor'\. The runtime buffers these signals and injects them at the start of the following tick, so InstinctNodes — including ReflexInstinctNodes — observe a node's state change in `ctx.signals` on the **next** tick after it is detected \(a fault raised during tick N is visible on tick N+1\)\.

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

The safety monitor subsystem provides continuous runtime verification of the safety properties formalised in the CSP models under ``formal/``\. Monitors are lightweight checkers that verify invariants after each tick and emit `SafetyViolationSignal` on the `SignalBus` when an invariant is breached\. The runtime does not run them automatically: build a `SafetyMonitorRegistry`, and after each tick call `check_all\(rt\.tick\_count, rt\.monitor\_state\(\)\)` \(see the safety\-monitors tutorial\)\. ``ArachniteRuntime\.monitor\_state\(\)`` builds the state from what the runtime observed in the last tick, so no field is left to the caller\. `check_all\(\)` returns the violations directly; violations are also published on the bus but are not injected into `ctx\.signals`\.

### __BaseSafetyMonitor__

Abstract base class\. Subclass and implement `check\(tick, state\)` to verify a specific invariant\.

    class BaseSafetyMonitor\(ABC\):
        monitor\_id: str
        async def check\(self, tick: int, state: MonitorState\) \-> SafetyViolationSignal | None
        async def emit\_violation\(property\_name, severity, details\) \-> SafetyViolationSignal

### __MonitorState__

Snapshot of runtime state provided to monitors each tick: tick, reflex\_fired, reflex\_action\_dispatched, reflex\_routed\_through\_decision, decision\_entered \(deprecated, ignored\), mandatory\_block\_active, interrupt\_accepted\_during\_block, resources\_with\_multiple\_owners, active\_reflex\_nodes, total\_reflex\_nodes, tick\_duration\_ms, tick\_budget\_ms\.

### __SafetyMonitorRegistry__

Manages a set of monitors\. `SafetyMonitorRegistry\.default\(bus\)` creates all six standard monitors\.

### __Built\-in Monitors__

| Monitor | Property | Severity |
|---|---|---|
| `ReflexBypassMonitor` | reflex\_fired ⟹ ¬reflex\_routed\_through\_decision | CRITICAL |
| `MandatoryBlockMonitor` | no deliberative interrupt honoured strictly inside a mandatory block | CRITICAL |
| `ReflexDispatchMonitor` | reflex\_fired ⟹ reflex\_action\_dispatched | CRITICAL |
| `ActuatorExclusivityMonitor` | resources\_with\_multiple\_owners = 0 | CRITICAL |
| `ReflexAvailabilityMonitor` | all reflex nodes available | WARNING |
| `TickBudgetMonitor` | 3\+ consecutive tick overruns | WARNING |

Field meanings, as ``monitor\_state\(\)`` fills them: ``reflex\_routed\_through\_decision`` — a **reflex node's proposal** was offered to the decision layer \(the decide stage itself runs on every tick, after the reflex stage, so \"the decide stage ran\" is not the property\); ``interrupt\_accepted\_during\_block`` — a completed action reported a **deliberative** stop strictly inside a mandatory block \(a ``PREEMPT`` reflex or ``emergency\_stop\(\)`` ending a block is declared behaviour, §17\.5, not a violation\); ``reflex\_action\_dispatched`` — every ``launch\_reflex\(\)`` since the previous tick returned ``LAUNCHED``, ``ARMED`` or ``DEDUPED``; ``resources\_with\_multiple\_owners`` — declared resources held by more than one running action \(0 by construction\); ``active\_reflex\_nodes`` — reflex nodes not FAULTED, RESTARTING or DEAD\.

Violations are published as `SafetyViolationSignal` \(kind `"safety_violation"`\) with `monitor_id`, `severity`, `property_name`, and `details`\. Reflex instincts can subscribe and trigger compensating actions\.

