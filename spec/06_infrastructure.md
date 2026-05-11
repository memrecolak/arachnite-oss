<!-- Arachnite SPEC §12–§14 -->

# __12\. Node Configuration__

Every node in a real deployment needs configuration: API keys, hardware pin numbers, threshold values, model paths, polling intervals\. Arachnite provides a structured injection mechanism so configuration is never hardcoded and flows cleanly from the DeploymentManifest into individual nodes\.

## __12\.1 NodeConfig__

NodeConfig is a thin typed wrapper around the config dict from a NodeAssignment\. It provides typed access with defaults and raises descriptive errors for missing required keys\.

class NodeConfig:

    def \_\_init\_\_\(self, data: dict, node\_id: str\): \.\.\.

    def get\(self, key: str, default: Any = REQUIRED\) \-> Any:

        """Retrieve a config value\. If default is REQUIRED and key

        is absent, raises NodeConfigError with the node\_id and key\."""

    def get\_int\(self, key: str, default: int = REQUIRED\) \-> int: \.\.\.

    def get\_float\(self, key: str, default: float = REQUIRED\) \-> float: \.\.\.

    def get\_bool\(self, key: str, default: bool = REQUIRED\) \-> bool: \.\.\.

    def get\_str\(self, key: str, default: str = REQUIRED\) \-> str: \.\.\.

    def section\(self, key: str\) \-> 'NodeConfig':

        """Return a nested NodeConfig for a sub\-dict key\."""

## __12\.2 Configuration in BaseNode__

BaseNode exposes config as a NodeConfig instance populated before setup\(\) is called\. Nodes access it in setup\(\) or lazily in their main method\.

class TemperatureSenseNode\(BaseSenseNode\):

    signal\_kind = 'thermal'

    async def setup\(self\) \-> None:

        self\.threshold\_c  = self\.config\.get\_float\('threshold\_celsius', 80\.0\)

        self\.poll\_interval\_s = self\.config\.get\_float\('poll\_interval\_s', 0\.5\)

        self\.\_hw = await HardwareI2C\.connect\(

            address=self\.config\.get\_int\('i2c\_address'\),   \# required

            bus=self\.config\.get\_int\('i2c\_bus', 1\),

        \)

## __12\.3 Configuration in the Manifest__

Configuration is declared per\-node in the manifest under a config key\. All values are passed through as\-is — strings, numbers, booleans, and nested dicts are all supported\.

agents:

  \- id: pi\-body

    nodes:

      sense:

        \- kind: TemperatureSenseNode

          config:

            i2c\_address: 0x48

            i2c\_bus: 1

            threshold\_celsius: 75\.0

            poll\_interval\_s: 0\.25

        \- kind: CameraSenseNode

          config:

            resolution: \[640, 480\]

            fps: 30

            device: '/dev/video0'

      instinct:

        \- kind: LLMGoalInstinct

          config:

            api\_key: '$\{OPENAI\_API\_KEY\}'   \# env var interpolation

            model: 'gpt\-4o\-mini'

            timeout\_s: 5\.0

String values beginning with $\{ are treated as environment variable references and resolved at manifest load time\. Missing env vars raise ManifestValidationError unless a default is provided: $\{VAR\_NAME:\-default\_value\}\.

## __12\.4 Secrets and Environment Variables__

- Never store API keys or credentials directly in the manifest file\.
- Use environment variable interpolation: $\{VAR\_NAME\} or $\{VAR\_NAME:\-fallback\}\.
- For production deployments, use a secrets manager and inject via environment variables at runtime\.
- The manifest validator warns \(but does not error\) if a string value looks like a plain secret \(matches common patterns like Bearer\_, sk\-\_, password=\)\.


# __13\. Logging and Observability__

Arachnite provides structured, per\-node logging built into BaseNode\. Every framework event — tick start, signal published, proposal generated, action dispatched, supervisor state change, interrupt request — emits a typed LogEvent\. Developers can attach log sinks to receive these events and route them to stdout, a file, a remote logging service, or a monitoring dashboard\.

## __13\.1 StructuredLogger__

Each node receives a pre\-configured StructuredLogger instance\. It emits LogEvents rather than raw strings, making logs machine\-readable and filterable\.

The runtime is responsible for keeping each logger's tick counter in sync so that every emitted `LogEvent.tick` reflects the current tick number\. Concretely, on every `runtime.tick()`:

\- the runtime logger and each master logger \(sense, instinct, decision, action\) are synced via direct `_set_tick(tick)` calls;

\- leaf\-node loggers are synced through the default `BaseNode.on_tick_start(tick)` implementation, which calls `self.logger._set_tick(tick)`\.

Subclasses overriding `on_tick_start` must call `super().on_tick_start(tick)` to preserve this sync, otherwise their LogEvents will report stale tick numbers\.

@dataclass

class LogEvent:

    level: LogLevel             \# DEBUG, INFO, WARNING, ERROR, CRITICAL

    node\_id: str

    agent\_node\_id: str

    tick: int

    message: str

    data: dict                  \# structured payload

    timestamp: float

class StructuredLogger:

    def debug\(self, msg: str, \*\*data\) \-> None: \.\.\.

    def info\(self, msg: str, \*\*data\) \-> None: \.\.\.

    def warning\(self, msg: str, \*\*data\) \-> None: \.\.\.

    def error\(self, msg: str, \*\*data\) \-> None: \.\.\.

    def critical\(self, msg: str, \*\*data\) \-> None: \.\.\.

\# Usage inside any node:

self\.logger\.info\('Sensor read', value=celsius, confidence=0\.95\)

self\.logger\.warning\('Threshold exceeded', value=celsius, threshold=self\.threshold\_c\)

## __13\.2 Log Sinks__

Log sinks receive all LogEvents from all nodes\. Multiple sinks can be registered simultaneously\.

class BaseLogSink\(ABC\):

    @abstractmethod

    async def emit\(self, event: LogEvent\) \-> None: \.\.\.

\# Built\-in sinks:

class StdoutLogSink\(BaseLogSink\): \.\.\.     \# colourised human\-readable

class JSONLogSink\(BaseLogSink\): \.\.\.        \# newline\-delimited JSON

class FileLogSink\(BaseLogSink\): \.\.\.        \# rotating file

class SignalBusLogSink\(BaseLogSink\): \.\.\.   \# publishes LogEvents as Signals

                                           \# \(kind='log'\) for remote collection

\# Registration:

runtime = ArachniteRuntime\(\.\.\., log\_sinks=\[StdoutLogSink\(level=LogLevel\.INFO\)\]\)

## __13\.3 Framework\-Emitted Events__

The framework automatically emits the following LogEvents without any developer code\. Events are identified by their `(level, message)` pair on the `LogEvent` dataclass; the "Event" column below is the conceptual name used in spec text\.

| Event              | Level   | Message                              | Trigger / data                                                                                  |
|--------------------|---------|--------------------------------------|--------------------------------------------------------------------------------------------------|
| `tick.overrun`     | WARNING | `"Tick overrun"`                     | Tick took longer than `interval × (1 + overrun_warn_pct)` for `overrun_warn_consecutive` consecutive ticks \(default 3\)\. Data: `tick`, `overrun_ms`, `elapsed_ms`, `interval_ms`, `consecutive`\. |
| `reflex.fired`     | INFO    | `"Reflex fired"`                     | Each `ReflexInstinctNode` activation, before dispatch\. Data: `instinct_id`, `action_id`, `priority`, `urgency`\. |
| `action.dispatched`| INFO    | `"Dispatching action"`               | `ActionMasterNode._dispatch_one()` starts execution\. Data: `action_id`, `priority`\.            |
| `action.completed` | INFO    | `"Action complete"`                  | `ActionNode.execute()` returned a `Result`\. Data: `action_id`, `success`, `duration_ms`\.       |
| `action.interrupted`| INFO   | `"Action interrupted"`               | An `InterruptRequest` was honoured\. Data: `action_id`, `step_name`\.                            |
| `step.completed`   | DEBUG   | `"Step complete"`                    | `MultiStepActionNode` finished one `ActionStep`\. Data: `step`, `duration_ms`\.                   |
| `transport.connected` | INFO | `"Transport connected"`              | `BaseTransport.connect()` succeeded\. Data: `transport` \(class name\)\.                          |
| `transport.disconnected` | INFO | `"Transport disconnected"`        | `BaseTransport.disconnect()` called\. Data: `transport` \(class name\)\.                          |
| `emergency_stop.initiated` | INFO | `"Emergency stop initiated"`    | `ArachniteRuntime.emergency_stop()` called, before the interrupt\-delivery loop\.                 |
| `emergency_stop.interrupt_delivered` | INFO | `"Emergency interrupt delivered"` | One running action received the emergency interrupt\. Data: `action_id`\.                    |

__High\-frequency events are not auto\-emitted by design\.__ The following are intentionally *not* emitted by the framework on every occurrence, because doing so would add per\-tick or per\-signal overhead in the hot path\. Users who need them opt in:

| Conceptual event     | How to observe it                                                                                         |
|----------------------|------------------------------------------------------------------------------------------------------------|
| `tick.start / tick.end` | Override `BaseNode.on_tick_start(tick)` / `on_tick_end(tick, duration_s)` and emit a custom log line\. |
| `signal.published`   | `bus.subscribe(kind, callback)` from any node \(or from a debug listener\)\.                                |
| `proposal.generated` | Override `BaseInstinctNode.evaluate()` to log on the success path; or use `ObservabilityMixin.increment("proposals_generated")`\. |
| `node.state_change`  | Subscribe to `SupervisorSignal` \(kind=`"supervisor"`\) on the bus\.                                        |
| `transport.connected / disconnected` | Now auto\-emitted by all four transports via `BaseTransport._logger` \(see framework\-emitted table above\)\. |

## __13\.4 ObservabilityMixin__

Nodes can opt into richer instrumentation by mixing in ObservabilityMixin, which adds per\-tick timing histograms, per\-signal counters, and proposal acceptance rate tracking\.

class MySenseNode\(BaseSenseNode, ObservabilityMixin\):

    signal\_kind = 'thermal'

    async def read\(self\) \-> Signal:

        with self\.observe\('read\_latency'\):   \# records duration

            celsius = await asyncio\.to\_thread\(self\.\_hw\_read\)

        self\.increment\('reads\_total'\)

        return Signal\(\.\.\.\)

\# Metrics are accessible via runtime\.metrics\(\) and exported

\# as Prometheus\-compatible text via runtime\.metrics\_text\(\)


## __13\.5 Web Dashboard \(SignalDashboard\)__

SignalDashboard is a real\-time observability component that streams every signal and log event to a browser via WebSocket\. It acts simultaneously as a BaseLogSink \(receives structured log events\) and a SignalBus wildcard subscriber \(receives all signals\)\.

Requires: pip install "arachnite\[web\]" \(fastapi, uvicorn\)

### __Constructor__

SignalDashboard\(

    bus:      SignalBus,          \# subscribe to all signals

    host:     str = "127\.0\.0\.1",

    port:     int = 7070,

    log\_file: str \| None = None,  \# optional plain\-text log file path

    level:    LogLevel = LogLevel\.INFO,

    backlog:  int = 500,          \# events replayed to new browser connections

\)

### __Lifecycle__

await dashboard\.start\(\)    \# binds HTTP server, subscribes to bus

await dashboard\.stop\(\)     \# unsubscribes, shuts down server

### __HTTP endpoints__

GET /    — serves the self\-contained HTML dashboard \(no external dependencies\)

GET /ws  — WebSocket endpoint; replays backlog on connect, then streams live events

### __Event format \(JSON over WebSocket\)__

\# Signal event

\{"type": "signal", "source": "SimTempSensor", "kind": "temperature",

 "value": 42\.1, "confidence": 0\.95, "timestamp": 1234\.56\}

\# Log event

\{"type": "log", "level": "INFO", "node\_id": "CoolFan",

 "message": "Fan ramping up", "tick": 12, "timestamp": 1234\.57, "extra": \{\}\}

### __FileLogSink__

FileLogSink writes structured plain\-text lines to a file for both log events and signals\. Pass it as a log sink alongside SignalDashboard to retain a persistent record\.

FileLogSink\(path: str \| Path, level: LogLevel = LogLevel\.DEBUG\)

Log line format:

\[YYYY\-MM\-DD HH:MM:SS\.mmm\] LOG  LEVEL    node\_id    tick=N  message  key=val \.\.\.

Signal line format:

\[YYYY\-MM\-DD HH:MM:SS\.mmm\] SIG  kind=temperature  source=SimTempSensor  value=42\.1  confidence=0\.95

### __Usage example__

from arachnite import SignalDashboard, LogLevel

dashboard = SignalDashboard\(

    bus,

    host     = "127\.0\.0\.1",

    port     = 7070,

    log\_file = "agent\.log",

    level    = LogLevel\.DEBUG,

    backlog  = 500,

\)

log\_sinks = \[dashboard\]

\# Pass log\_sinks to all nodes and to ArachniteRuntime

sense\_master\.register\(MyNode\(bus=bus, log\_sinks=log\_sinks\)\)

rt = ArachniteRuntime\(\.\.\., log\_sinks=log\_sinks\)

await dashboard\.start\(\)

print\("Dashboard: http://localhost:7070"\)

await rt\.start\(\)

await rt\.wait\(\)

await dashboard\.stop\(\)

### __Dashboard UI features__

\- Dark\-themed live feed; each source colour\-coded by hash

\- Per\-source event count badges

\- Filter bar \(substring match on source, kind, or message\)

\- Pause / Resume button \(stops auto\-scroll, keeps receiving events\)

\- Clear button

\- Auto\-reconnect on WebSocket drop \(exponential backoff\)

### __Rules__

\- SignalDashboard\.start\(\) must be called before ArachniteRuntime\.start\(\)

\- SignalDashboard\.stop\(\) must be called after ArachniteRuntime\.stop\(\)

\- log\_file writes are synchronous and wrapped in asyncio\.to\_thread\(\) to avoid blocking the event loop


# __14\. Signal Serialisation__

When signals cross a network boundary via a non\-local transport, Signal\.value must be serialised and deserialised\. Because Signal\.value is typed Any, the framework cannot assume a universal encoding\. The CodecRegistry solves this by mapping signal kinds to developer\-provided codecs\.

## __14\.1 SignalCodec__

class SignalCodec\(ABC\):

    """Handles serialisation for one signal kind\."""

    @abstractmethod

    def encode\(self, value: Any\) \-> bytes:

        """Serialise Signal\.value to bytes for wire transmission\."""

    @abstractmethod

    def decode\(self, data: bytes\) \-> Any:

        """Deserialise bytes back to the original value type\."""

## __14\.2 CodecRegistry__

The CodecRegistry maps signal kinds to codecs\. It is configured at the transport level and applies to all signals crossing the network\.

class CodecRegistry:

    def register\(self, kind: str, codec: SignalCodec\) \-> None:

        """Register a codec for a signal kind\.

        Wildcard '\*' applies to all unregistered kinds\."""

    def encode\(self, signal: Signal\) \-> bytes: \.\.\.

    def decode\(self, kind: str, data: bytes\) \-> Any: \.\.\.

\# Built\-in codecs:

class MsgpackCodec\(SignalCodec\): \.\.\.    \# default for primitive values

class JSONCodec\(SignalCodec\): \.\.\.        \# human\-readable fallback

class PickleCodec\(SignalCodec\): \.\.\.      \# arbitrary Python objects \(network\_safe=False\)

                                         \# BLOCKED on network transports — see §14\.2\.1

class NumpyCodec\(SignalCodec\): \.\.\.       \# numpy arrays \(requires numpy\)

class PILCodec\(SignalCodec\): \.\.\.         \# PIL/Pillow images

## __14\.2\.1 Codec Network Safety__

`PickleCodec` uses Python's `pickle` module, which can execute arbitrary code during deserialisation\. This is a known remote code execution \(RCE\) vector when data crosses a network boundary\.

**`SignalCodec.network_safe`** — Every `SignalCodec` subclass declares a class attribute `network_safe: bool`\. The base class defaults to `True`\. `PickleCodec` overrides with `network_safe = False`\.

**`CodecRegistry.check_network_safety(transport_name: str)`** — Iterates all registered codecs\. If any codec has `network_safe = False`, it MUST raise `UnsafeCodecError` naming the offending codec and the transport\.

**Transport enforcement** — Every non\-local transport \(`MQTTTransport`, `NATSTransport`, `RedisTransport`\) MUST call `CodecRegistry.check_network_safety()` at the start of its `connect()` method, before establishing any network connection\. `LocalTransport` is exempt because signals never leave the process\.

### Rules

- `PickleCodec` MUST NOT be used with any network transport\. Attempting to do so raises `UnsafeCodecError` at connect time\.
- Custom codecs that perform unsafe deserialisation SHOULD set `network_safe = False`\.
- Developers MAY override `PickleCodec.network_safe = True` if they accept the risk in a fully trusted mesh\. This is NOT RECOMMENDED for production deployments\.
- `UnsafeCodecError` is a subclass of `ArachniteError` and is part of the public API \(exported from `arachnite`\)\.

## __14\.3 Registering Codecs__

from arachnite\.codec import CodecRegistry, NumpyCodec, MsgpackCodec

from arachnite\.transport\.mqtt import MQTTTransport

registry = CodecRegistry\(\)

registry\.register\('visual', NumpyCodec\(\)\)     \# camera frames as ndarrays

registry\.register\('thermal', MsgpackCodec\(\)\)  \# floats, uses default

registry\.register\('\*', MsgpackCodec\(\)\)        \# fallback for all others

transport = MQTTTransport\(

    broker\_host='192\.168\.1\.10',

    codec\_registry=registry,

\)

## __14\.4 Memory Management for Large Signal Values__

On constrained edge devices, Signal\.value may be large \(camera frames, audio buffers\)\. Two mechanisms control memory usage:

- ContextNode\.history\_length limits the number of ticks retained\. For large signal kinds, consider a smaller history or per\-kind limits via history\_config\.
- HistoryConfig\.max\_ticks: keep only the most recent N tick occurrences of this kind in history\. Older occurrences are evicted in place \(value set to None, metadata\["evicted"\]=True, metadata\["reason"\]="max\_ticks"\) so signals of other kinds in the same tick slot remain readable\. `None` \(default\) falls back to ContextNode\.history\_length; `0` evicts every occurrence\.
- HistoryConfig\.value\_ttl\_s: signals older than this are evicted from history \(value set to None, confidence to 0\.0\) regardless of history\_length\. Useful for high\-frequency sensors on memory\-constrained devices\.
- Large values are never transmitted over the bus unless a subscriber on a different AgentNode has subscribed to that kind\. The transport filters before serialising\.

\# Per\-kind history configuration \(optional, overrides history\_length\):

context = ContextNode\(

    bus=bus,

    history\_length=10,

    history\_config=\{

        'visual': HistoryConfig\(max\_ticks=2, value\_ttl\_s=0\.5\),

        'audio':  HistoryConfig\(max\_ticks=5, value\_ttl\_s=1\.0\),

    \}

\)

HistoryConfig supports three eviction policies: max\_ticks \(per\-kind tick limit\), value\_ttl\_s \(time\-based eviction\), and max\_bytes \(byte\-budget eviction via `sys.getsizeof()` on Signal\.value, oldest entries evicted first\)\. All three can be combined on the same kind; TTL eviction runs before byte\-budget eviction\.

## __14\.5 MediaStore__

For multi\-modal agents handling large signal payloads \(camera frames, audio buffers, LiDAR scans\), passing raw binary data through Signal\.value → Context → Proposal is memory\-intensive and makes transport serialisation fragile\. MediaStore provides lightweight on\-disk persistence so that only file paths travel through the pipeline\.

class MediaStore:

    def \_\_init\_\_\(self, base\_dir: str | Path = 'arachnite\_media',

                 extensions: dict\[str, str\] | None = None\): \.\.\.

    def store\(self, data: bytes, kind: str, source: str,

              tick: int | None = None, extension: str | None = None\) \-> Path:

        """Write data to disk\. Returns absolute path\.

        Files organised as base\_dir / kind / source\_tickN\_timestamp\.ext\."""

    def load\(self, path: str | Path\) \-> bytes:

        """Read raw bytes from a previously stored file\."""

    def cleanup\(self, max\_age\_s: float\) \-> int:

        """Remove files older than max\_age\_s\. Returns count removed\."""

    def clear\(self\) \-> None:

        """Remove all stored files and subdirectories\."""

Default file extensions are provided per signal kind \(e\.g\. camera → \.jpg, audio → \.wav, lidar → \.bin\)\. Custom extensions can be passed at construction or per\-call via the extension parameter\.

### __Recommended usage pattern__

The recommended pipeline for multi\-modal signals is:

1\. **SenseNode**: save payload to MediaStore, put path in Signal\.value and Signal\.metadata\["media\_path"\]\.

2\. **InstinctNode**: read file from path, analyse, attach path \+ summary to Proposal\.evidence\.

3\. **DecisionNode**: inspect evidence summaries for context\-aware decisions beyond priority\/urgency\.

4\. **ActionNode**: load file from proposal\.evidence path if needed\.

Example \(camera sense node\)::

    class CameraSense\(BaseSenseNode\):

        signal\_kind = 'camera'

        def \_\_init\_\_\(self, bus, media: MediaStore, \*\*kw\):

            super\(\)\.\_\_init\_\_\(bus, \*\*kw\)

            self\.\_media = media

        async def read\(self\) \-> Signal:

            frame = await asyncio\.to\_thread\(self\.\_capture\)

            path  = self\.\_media\.store\(frame, kind=self\.signal\_kind,

                                       source=self\.node\_id\)

            return Signal\(source=self\.node\_id, kind=self\.signal\_kind,

                          value=str\(path\), confidence=1\.0,

                          timestamp=time\.monotonic\(\),

                          metadata=\{'media\_path': str\(path\)\}\)

Example \(vision instinct with evidence\)::

    class VisionInstinct\(BaseInstinctNode\):

        trigger\_on\_signals = \['camera'\]

        async def evaluate\(self, ctx\) \-> Proposal | None:

            for sig in ctx\.signals:

                if sig\.kind == 'camera':

                    path = sig\.metadata\.get\('media\_path'\)

                    summary = await asyncio\.to\_thread\(self\.\_analyze, path\)

                    if 'fire' in summary\.lower\(\):

                        return Proposal\(

                            instinct\_id=self\.node\_id,

                            action\_id='Evacuate',

                            priority=150, urgency=0\.95,

                            evidence=\{

                                'camera\_path': path,

                                'camera\_summary': summary,

                            \},

                        \)

            return None

### __Security: path component validation__

The `kind` and `source` parameters to `store()` are used to construct filesystem paths\. To prevent path traversal attacks \(e\.g\. `kind="../../etc"`\), both parameters MUST match the pattern `[a-zA-Z0-9_][a-zA-Z0-9_.\-]*`\. If either parameter fails validation, `store()` MUST raise `PathTraversalError`\. As defense\-in\-depth, the resolved output path MUST be verified to reside under `base_dir` before any write occurs\.

`PathTraversalError` is a subclass of `ArachniteError` and is part of the public API \(exported from `arachnite`\)\.

### __Cleanup__

MediaStore does not automatically evict files\. The agent is responsible for calling cleanup\(max\_age\_s\) periodically \(e\.g\. in a maintenance InstinctNode or during teardown\) or clear\(\) on shutdown\.

