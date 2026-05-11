<!-- Arachnite SPEC §7, §15, §16, §17 -->

# __7\. ArachniteRuntime__

The ArachniteRuntime is the orchestrator that wires all master nodes together and runs the tick loop\.

## __7\.1 Interface__

class ArachniteRuntime:

    def \_\_init\_\_\(

        self,

        sense\_master: SenseMasterNode,

        context: ContextNode,

        instinct\_master: InstinctMasterNode,

        decision\_master: DecisionMasterNode,

        action\_master: ActionMasterNode,

        bus: SignalBus,

        tick\_rate\_hz: float = 10\.0,

        tick\_instrumenter: TickInstrumenter \| None = None,

    \): \.\.\.

        """The optional ``tick\_instrumenter`` parameter installs an observer
        that receives per\-stage wall\-clock timings on every tick\. Pass ``None``
        \(the default\) for zero instrumentation overhead\. The instrumenter MAY
        be attached or replaced after construction via ``set\_tick\_instrumenter\(\)``
        \(see §7\.5\)\."""

    @property

    def health\(self\) \-> HealthMonitor: \.\.\.

        """Access the aggregated health view across all supervisors\."""

    async def start\(self\) \-> None:

        """Call setup\(\) on all nodes, start supervisors, begin tick loop\.

        If any master's setup\(\) raises, already\-started masters are torn down

        before re\-raising the exception\."""

    async def stop\(self\) \-> None:

        """Gracefully stop the loop, stop supervisors, call teardown\(\)\."""

    async def tick\(self\) \-> None:

        """Execute one full pipeline cycle manually \(useful for testing\)\."""

    @property

    def tick\_count\(self\) \-> int: \.\.\.

    @property

    def is\_running\(self\) \-> bool: \.\.\.

    async def register\_sense\_live\(self, node: BaseSenseNode\) \-> None:

        """Register a SenseNode after the runtime has started\.

        Calls setup\(\) immediately \(if running\) and begins supervisor tracking\.

        If setup\(\) raises, the node is automatically unregistered\.

        Primary hook for Phase 1 self\-assembly: DynamicLoaderAction generates

        a new SenseNode and registers it without restarting the agent\."""

    async def register\_instinct\_live\(self, node: BaseInstinctNode\) \-> None:

        """Register an InstinctNode after the runtime has started\."""

    async def register\_action\_live\(self, node: BaseActionNode\) \-> None:

        """Register an ActionNode after the runtime has started\."""

    async def unregister\_sense\_live\(self, node\_id: str\) \-> None:

        """Unregister a SenseNode while the runtime is running\.

        Calls teardown\(\) on the node, removes it from the sense master,

        and untracks it from the supervisor\.  Safe to call when stopped\.

        Counterpart of register\_sense\_live\(\) — used by the Phase 5

        repair cycle to remove a dead node before registering a replacement\.

        Silent if node\_id is not registered\."""

    async def unregister\_instinct\_live\(self, node\_id: str\) \-> None:

        """Unregister an InstinctNode \(normal or reflex\) while the runtime

        is running\.  Calls teardown\(\), removes from the instinct master,

        and untracks from the supervisor\."""

    async def unregister\_action\_live\(self, node\_id: str\) \-> None:

        """Unregister an ActionNode while the runtime is running\.

        Calls teardown\(\), removes from the action master, and untracks

        from the supervisor\."""

## __7\.2 Tick Loop__

The runtime maintains a fixed tick rate\. Reflex evaluation happens first each tick before the normal pipeline\. If a tick takes longer than the interval, the next tick starts immediately\. A warning is logged when a tick overruns by more than `overrun_warn_pct` \(default 20%\) for `overrun_warn_consecutive` consecutive ticks \(default 3\); the consecutive counter resets on the first non\-overrunning tick\. The dual threshold mirrors `TickBudgetMonitor` \(which emits a `SafetyViolationSignal` after 3 consecutive overruns\) so the runtime warning and the safety signal agree on what counts as a real overrun rather than a transient schedule slip\. Pass `overrun_warn_consecutive=1` for legacy warn\-on\-every\-overrun behaviour\.

async def \_loop\(self\):

    while self\.\_running:

        start = time\.monotonic\(\)

        await self\.tick\(\)

        elapsed = time\.monotonic\(\) \- start

        sleep = max\(0\.0, self\.\_interval \- elapsed\)

        await asyncio\.sleep\(sleep\)

async def tick\(self\) \-> None:

    tick\_start = time\.monotonic\(\)

    \# 0a\. Reset per\-tick result state\.  \_last\_results and \_last\_result
    \#     MUST be cleared at the start of every tick so that stale results
    \#     from a previous tick never leak into the current tick's context\.
    self\.\_last\_results = \[\]
    self\.\_last\_result  = None

    \# 0b\. Notify all leaf nodes that a new tick is starting

    await asyncio\.gather\(

        self\.\_sense\_master\.notify\_tick\_start\(self\.\_tick\_count\),

        self\.\_instinct\_master\.notify\_tick\_start\(self\.\_tick\_count\),

        self\.\_action\_master\.notify\_tick\_start\(self\.\_tick\_count\),

    \)

    \# 1\. Sense — poll\_interval\_s is enforced per node; nodes whose

    \#    interval has not elapsed return None and are skipped\.

    signals  = await self\.\_sense\_master\.read\_all\(\)

    \# 2\. Drain buffered supervisor signals so instincts can observe

    \# node faults \(FAULTED, DEAD, RESTARTING\) in this tick\.

    signals \+= self\.\_supervisor\_signal\_buffer

    self\.\_supervisor\_signal\_buffer\.clear\(\)

    \# 3\. Context update

    ctx      = self\.\_context\.update\(signals, self\.\_last\_result\)

    \# 4\. Reflex pass — bypass decision if any reflex fires

    reflex\_proposals = await self\.\_instinct\_master\.evaluate\_reflexes\(ctx\)

    reflex\_results: list\[Result\] = \[\]

    for rp in reflex\_proposals:

        try:

            result = await self\.\_action\_master\.dispatch\(rp\)

        except ActionNotFoundError:

            self\.\_logger\.warning\("Reflex dispatch skipped: action not found", \.\.\.\)

            continue

        reflex\_results\.append\(result\)

    if reflex\_results:

        self\.\_last\_results = reflex\_results

        self\.\_last\_result = reflex\_results\[0\]

    \# 5\. Normal instinct evaluation

    proposals = await self\.\_instinct\_master\.evaluate\_all\(ctx\)

    evaluated\_ids = self\.\_instinct\_master\.last\_evaluated\_ids

    \# 6\. Decision — select proposals to dispatch

    to\_dispatch, interrupts = await self\.\_decision\_master\.on\_new\_proposals\_many\(

        proposals,

        running\_proposals=self\.\_action\_master\.current\_proposals\(\),

        running\_interruptible=\{\.\.\.interruptibility map\.\.\.\},

        evaluated\_instinct\_ids=evaluated\_ids,

    \)

    for interrupt\_req in interrupts:

        try:

            await self\.\_action\_master\.request\_interrupt\(interrupt\_req, \.\.\.\)

        except MandatoryBlockViolation:

            \# Expected when the target action is inside a mandatory

            \# completion block\.  Logged as WARNING with action\_id and

            \# detail fields — MUST NOT be silently suppressed\.

            logger\.warning\("Interrupt deferred: mandatory block active", \.\.\.\)

        except Exception:

            \# Unexpected error — logged as ERROR with full context\.

            logger\.error\("Interrupt dispatch failed", \.\.\.\)

    \# 7\. Notify rejected instincts — MUST use last\_considered (fresh +
    \#    carried\-forward pending proposals) so that persistent proposals
    \#    that lost the decision also receive on\_proposal\_rejected\(\)\.

    dispatched\_ids = \{p\.instinct\_id for p in to\_dispatch\}

    all\_considered = self\.\_decision\_master\.last\_considered

    rejected = \[p for p in all\_considered if p\.instinct\_id not in dispatched\_ids\]

    if rejected:

        await self\.\_instinct\_master\.notify\_rejected\(rejected\)

    \# 8\. Dispatch — merge reflex and normal results

    if to\_dispatch:

        results = await self\.\_action\_master\.dispatch\_many\(to\_dispatch\)

        self\.\_last\_results = reflex\_results \+ results

        self\.\_last\_result = self\.\_last\_results\[0\]

    \# 9\. Notify all leaf nodes that the tick has ended

    tick\_duration = time\.monotonic\(\) \- tick\_start

    await asyncio\.gather\(

        self\.\_sense\_master\.notify\_tick\_end\(self\.\_tick\_count, tick\_duration\),

        self\.\_instinct\_master\.notify\_tick\_end\(self\.\_tick\_count, tick\_duration\),

        self\.\_action\_master\.notify\_tick\_end\(self\.\_tick\_count, tick\_duration\),

    \)

## __7\.3 Quick\-Start Example__

import asyncio

from arachnite import ArachniteRuntime, SignalBus, ContextNode

from arachnite\.nodes import SenseMasterNode, InstinctMasterNode

from arachnite\.nodes import DecisionMasterNode, ActionMasterNode

from arachnite\.nodes\.decision import GreedyDecisionNode

async def main\(\):

    bus     = SignalBus\(\)

    context = ContextNode\(bus=bus, history\_length=20\)

    sense\_master    = SenseMasterNode\(bus=bus\)

    instinct\_master = InstinctMasterNode\(bus=bus\)

    decision\_master = DecisionMasterNode\(bus=bus, strategy=GreedyDecisionNode\(\)\)

    action\_master   = ActionMasterNode\(bus=bus\)

    sense\_master\.register\(TemperatureSenseNode\(bus=bus\)\)

    instinct\_master\.register\(OverheatInstinct\(bus=bus, priority=100\)\)

    action\_master\.register\(SetTemperatureActionNode\(bus=bus\)\)

    runtime = ArachniteRuntime\(

        sense\_master, context, instinct\_master,

        decision\_master, action\_master, bus,

        tick\_rate\_hz=5\.0,

    \)

    await runtime\.start\(\)

    await asyncio\.sleep\(60\)

    await runtime\.stop\(\)

asyncio\.run\(main\(\)\)

## __7\.4 RuntimeBuilder \(Fluent API\)__

For simpler agents, `RuntimeBuilder` eliminates the boilerplate of creating
the bus, master nodes, and registering individual nodes manually:

    from arachnite import RuntimeBuilder

    rt = \(
        RuntimeBuilder\(\)
        \.sense\(TemperatureSenseNode\)
        \.instinct\(OverheatInstinct\)
        \.action\(SetTemperatureActionNode\)
        \.tick\_rate\(5\.0\)
        \.build\(\)
    \)

Pass node **classes** to auto\-instantiate with the builder's internal bus,
or pre\-built **instances** for custom constructor arguments\.

Chainable options: `\.strategy\(\)`, `\.tick\_rate\(\)`, `\.log\_sinks\(\)`,
`\.reflex\_conflict\(\)`, `\.permissions\(\)`, `\.shutdown\(\)`, `\.overrun\_warn\(\)`,
`\.overrun\_warn\_consecutive\(\)`\.

Defaults: `GreedyDecisionNode` strategy, 10\.0 Hz tick rate,
`dispatch\_all` reflex conflict policy\.

## __7\.5 Tick Instrumentation \(optional\)__

The runtime SHALL expose an optional instrumentation hook that delivers
per\-stage wall\-clock timings for each tick\. The hook is provided as a
`typing\.Protocol` so that any conforming implementation \(benchmark
collector, OpenTelemetry adapter, Prometheus sink, user\-defined profiler\)
can attach without a framework dependency\.

    from typing import Protocol, runtime\_checkable

    TICK\_STAGE\_NAMES: tuple\[str, \.\.\.\] = \(
        "sense", "context", "reflex", "instinct", "decide", "act",
    \)

    @runtime\_checkable
    class TickInstrumenter\(Protocol\):
        """Optional per\-stage timing callback installed on ArachniteRuntime\."""

        def on\_stage\(self, stage: str, duration\_s: float\) \-> None: \.\.\.

        def on\_tick\_complete\(
            self, tick\_index: int, total\_s: float,
        \) \-> None: \.\.\.

Both symbols \(`TickInstrumenter`, `TICK\_STAGE\_NAMES`\) are re\-exported from
`arachnite` for third\-party implementors\.

__Six stage boundaries\.__ The runtime MUST invoke `on\_stage\(name, duration\_s\)`
exactly once per stage per tick, in the following order, with `duration\_s`
reporting the wall\-clock time spent inside that stage\. The six stage names
are the elements of `TICK\_STAGE\_NAMES` and map to §7\.2 as follows:

| \# | Stage name | Spans |
|---|---|---|
| 1 | `sense` | `notify\_tick\_start` gather \+ `SenseMasterNode\.read\_all\(\)` \+ supervisor\-signal\-buffer drain |
| 2 | `context` | `ContextNode\.update\(\)` \(includes action\-state assembly\) |
| 3 | `reflex` | `evaluate\_reflexes\(\)` \+ the sequential reflex\-dispatch loop \(fused because the reflex arc bypasses decision; see §7\.2 step 4\) |
| 4 | `instinct` | `InstinctMasterNode\.evaluate\_all\(ctx\)` \(evaluate only — proposals dispatch in `act`\) |
| 5 | `decide` | Interruptibility\-map build \+ `on\_new\_proposals\_many` \+ `notify\_rejected` \+ interrupt issuing |
| 6 | `act` | `ActionMasterNode\.dispatch\_many\(\)` \+ `notify\_tick\_end` gather |

After the six stages have fired, the runtime MUST invoke
`on\_tick\_complete\(tick\_index, total\_s\)` exactly once, with `total\_s`
reporting the full wall\-clock tick duration \(i\.e\. the sum of the six stage
durations plus the small bookkeeping interval between them\)\. Logger\-tick
sync and `\_tick\_count \+= 1` occur before the first stage and are therefore
excluded from all six stage durations\.

__Installation\.__ An instrumenter MAY be supplied at construction via the
`tick\_instrumenter` kwarg \(§7\.1\) or installed/replaced at any time via:

    def set\_tick\_instrumenter\(
        self, instrumenter: TickInstrumenter \| None,
    \) \-> None: \.\.\.

Passing `None` detaches the current instrumenter\. The setter is safe to
call at any time, including concurrently with a running tick loop; the
runtime reads the reference once per tick and uses that reference for the
full tick, so replacement takes effect on the following tick\.

__Zero\-overhead default\.__ When no instrumenter is installed, `tick\(\)`
MUST NOT perform any instrumentation work — in particular, it MUST NOT
call `time\.monotonic\(\)` at stage boundaries and MUST NOT allocate
per\-tick bookkeeping objects\. The only permitted cost is a single
branch\-predicted `is not None` check per boundary\.

__Error isolation \(ADR 0003\)\.__ Instrumenter implementations MAY raise\.
The runtime MUST catch any exception raised from `on\_stage` or
`on\_tick\_complete`, log it at WARNING with fields `stage` \(or `tick`\),
`error`, and `error\_type`, and continue processing the current tick\. An
instrumenter failure MUST NOT propagate into the tick loop, MUST NOT
fail the tick, and MUST NOT affect downstream stages beyond the dropped
sample for the failing call\. This behaviour is consistent with the
framework's four other observational\-failure isolation sites
\(`ContextNode` history I/O, the `\_loop` tick\-exception handler,
`emergency\_stop` interrupt issuing, `NodeSupervisor` setup errors\) and
with the architectural rules that `execute\(\)` and `evaluate\(\)` must
never raise\.


# __15\. Shutdown Sequence__

Graceful shutdown is non\-trivial when actions may be mid\-execution, mandatory completion blocks may be running, and supervisors may be mid\-restart\. The ShutdownCoordinator manages this process to ensure hardware is left in a safe, defined state\.

## __15\.1 Shutdown Phases__

ArachniteRuntime\.stop\(\) triggers the following ordered phases:

__Phase__

__What happens__

__1\. Stop sensing__

SenseMasterNode stops polling\. No new signals enter the pipeline\.

__2\. Drain reflexes__

Any reflex currently evaluating is allowed to complete\. No new ticks start\.

__3\. Complete mandatory block__

If an action is mid\-mandatory\-block, it runs to the end of the block\. Timeout: sum of remaining mandatory step timeouts \+ 10%\.

__4\. Interrupt remaining action__

Any action still running receives an InterruptRequest with reason='shutdown'\. ROLLBACK policy runs\.

__5\. Stop supervisors__

All NodeSupervisors call `cancel\_restart\_tasks\(\)` to cancel in\-flight restart tasks before marking nodes as STOPPED\. Restart tasks MUST be cancelled before any node is marked STOPPED to prevent a restart racing with teardown\.

__6\. Teardown nodes__

teardown\(\) is called on all nodes concurrently, with a configurable grace period \(default 5s\)\.

__7\. Disconnect transport__

Transport connections closed cleanly\. Pending outbound signals are flushed or dropped\.

## __15\.2 ShutdownCoordinator__

class ShutdownCoordinator:

    def \_\_init\_\_\(

        self,

        teardown\_timeout\_s: float = 5\.0,

        mandatory\_block\_timeout\_multiplier: float = 1\.1,

        on\_shutdown\_action: Callable | None = None,

    \): \.\.\.

    \# on\_shutdown\_action: optional callable invoked at phase 1

    \# e\.g\. trigger a 'safe position' action before stopping sensing

    async def execute\(self, runtime: 'ArachniteRuntime'\) \-> None:

        """Run all shutdown phases in order\. Called by runtime\.stop\(\)\."""

    @property

    def phase\(self\) \-> ShutdownPhase: \.\.\.

    @property

    def completed\(self\) \-> bool: \.\.\.

## __15\.3 Emergency Shutdown__

When runtime\.emergency\_stop\(\) is called \(e\.g\. from a signal handler on SIGTERM or SIGINT\), the coordinator skips phases 1–3 and jumps directly to phase 4\. Mandatory blocks are abandoned immediately\. Rollbacks still run where possible\. This is the only case where a mandatory completion block is forcibly interrupted\.

import signal, asyncio

async def main\(\):

    runtime = ArachniteRuntime\(\.\.\.\)

    loop = asyncio\.get\_running\_loop\(\)

    loop\.add\_signal\_handler\(signal\.SIGTERM,

        lambda: asyncio\.create\_task\(runtime\.emergency\_stop\(\)\)\)

    loop\.add\_signal\_handler\(signal\.SIGINT,

        lambda: asyncio\.create\_task\(runtime\.emergency\_stop\(\)\)\)

    await runtime\.start\(\)

    await runtime\.wait\(\)   \# blocks until stop\(\) or emergency\_stop\(\)

## __15\.4 on\_pause and on\_resume__

For power management on edge devices, the runtime supports pause and resume without a full shutdown\. All nodes receive on\_pause\(\) / on\_resume\(\) lifecycle hooks\. Supervisors continue monitoring during pause; sensing stops; the tick loop suspends\.

await runtime\.pause\(\)    \# triggers on\_pause\(\) on all nodes

await runtime\.resume\(\)   \# triggers on\_resume\(\) on all nodes

\# Nodes can detect pause in on\_tick\_start\(\):

async def on\_pause\(self\) \-> None:

    self\.\_hw\.set\_power\_mode\(PowerMode\.LOW\)

    self\.poll\_interval\_s = 5\.0   \# slow down during pause

async def on\_resume\(self\) \-> None:

    self\.\_hw\.set\_power\_mode\(PowerMode\.NORMAL\)

    self\.poll\_interval\_s = 0\.5


# __16\. Performance and Complexity__

This section characterises the computational complexity and memory footprint of the framework to support capacity planning for constrained edge deployments\.

## __16\.1 Per\-Tick Complexity__

__Pipeline Stage__

__Complexity__

__SenseMasterNode\.read\_all\(\)__

O\(S\) where S = number of SenseNodes\. All reads are concurrent; wall time dominated by slowest sensor\.

__ContextNode\.update\(\)__

O\(S \+ H\) where H = history\_length\. Signal merge is linear; history append is O\(1\) amortised\.

__InstinctMasterNode\.evaluate\_reflexes\(\)__

O\(R\) concurrent where R = number of ReflexInstinctNodes\.

__InstinctMasterNode\.evaluate\_all\(\)__

O\(I\) concurrent where I = number of InstinctNodes\.

__DecisionMasterNode\.decide\(\)__

O\(P log P\) where P = number of proposals \(sort by priority\)\. Typically P << I\.

__ActionMasterNode\.dispatch\(\)__

O\(1\) lookup by node\_id\. Execution time is action\-defined\.

__SignalBus\.publish\(\)__

O\(K\) where K = number of subscribers for that signal kind\.

## __16\.2 Memory Footprint__

Baseline memory usage on a Raspberry Pi 4 \(measured on CPython 3\.11, LocalTransport, 10 nodes, history\_length=10\):

__Component__

__Approximate footprint__

__Framework baseline \(no nodes\)__

~8 MB RSS

__Per node \(leaf node, no data\)__

~2–4 KB per node

__Context history \(scalar signals\)__

~1 KB per tick × history\_length

__Context history \(camera frame, 640×480 RGB\)__

~900 KB per tick × history\_length

__MQTTTransport overhead__

~1\.5 MB additional RSS

__NATSTransport overhead__

~3 MB additional RSS

Large signal values \(camera frames, audio buffers\) dominate memory\. Use history\_config per\-kind limits and value\_ttl\_s to control growth on constrained devices\.

## __16\.3 Latency Budget__

For a 10 Hz tick rate \(100 ms tick interval\) on a Raspberry Pi 4 with LocalTransport:

- Framework overhead per tick \(no node logic\): ~0\.3–1\.0 ms\.
- Per\-node asyncio scheduling overhead: ~0\.05 ms per node\.
- Reflex evaluation latency \(before normal pipeline\): adds ~0\.1 ms per reflex node\.
- Remaining budget for node logic at 10 Hz: ~95 ms\. At 50 Hz: ~16 ms\.
- MQTTTransport round\-trip on local WiFi: ~2–10 ms additional per cross\-device signal\.

These are indicative figures\. Actual numbers depend heavily on node implementation complexity, hardware, and network conditions\. Empirical values for the reference implementation are produced by the benchmark suite under `benchmarks/`\.

## __16\.4 Comparison with Related Frameworks__

__Feature__

__Arachnite__

__Closest Alternative__

Biological architecture metaphor

Full — sense/instinct/decide/act

py\_trees: behaviour tree only

Reflex arc \(pipeline bypass\)

Native, typed, enforced

None in LangChain, AutoGen, py\_trees

Fixed action pattern \(NEVER interrupt\)

Native InterruptPolicy\.NEVER

Not modelled in any reviewed framework

Bounded interrupt latency guarantee

Statically computable from step defs

Not available

Multi\-step actions with rollback

Native MultiStepActionNode

LangGraph: chains, no rollback

Node lifecycle supervision

NodeSupervisor per master node

ROS 2: node lifecycle, no per\-node restart policy

Distributed deployment

Transport layer \+ manifest

ROS 2: DDS; LangChain: no native distribution

Co\-location constraint enforcement

Validator at manifest load

Not available

Single\-process / embedded support

LocalTransport, zero deps

ROS 2: heavy; AutoGen: cloud\-oriented

Language

Python 3\.11\+

py\_trees: Python; ROS 2: C\+\+/Python; LangChain: Python

Primary domain

Embodied / hybrid AI agents

LangChain: LLM pipelines; py\_trees: game AI / robotics

Note: this comparison is based on framework documentation and design intent\.


# __17\. Multi\-Step Actions and Interruption__

Many real\-world actions are not atomic\. Moving a robotic arm, opening a valve, or completing a task sequence requires multiple ordered steps\. Stopping partway through some of these sequences is safe; stopping partway through others could leave hardware in a dangerous or undefined state\.

This section specifies the MultiStepActionNode system, which gives developers explicit control over action decomposition, and the InterruptPolicy system, which governs what happens when a higher\-priority proposal arrives while an action is already executing\.

The biological parallel is the fixed action pattern — a neural sequence that, once initiated past a threshold, runs to completion regardless of new sensory input\. A spider’s strike, a frog’s tongue\-extension, a bird’s landing sequence\. The framework models this explicitly as a mandatory completion block\.

## __17\.1 ActionStep__

ActionStep is the atomic unit of a multi\-step action\. Each step declares its name, whether execution can safely pause after it completes, and an optional rollback callable to undo its effects if the action is interrupted mid\-block\.

@dataclass

class ActionStep:

    name: str                          \# human\-readable label for logging

    interruptible: bool = True         \# safe to stop after this step?

    rollback: Callable\[\[\], Awaitable\[None\]\] | None = None

                                       \# undo function, called if this step

                                       \# completed inside an interrupted block

    timeout\_s: float | None = None     \# per\-step timeout \(overrides node default\)

    checkpoint: bool = False           \# used by CHECKPOINT policy

    metadata: dict = field\(default\_factory=dict\)

Steps are declared as a flat list\. Consecutive non\-interruptible steps form an implicit atomic block — the framework will not interrupt execution inside such a block\. A step with interruptible=True is a safe stop point; the framework may pause there if an interrupt is requested\.

## __17\.2 StepResult__

Each step returns a StepResult indicating whether it succeeded, failed, or requests the sequence to abort\.

@dataclass

class StepResult:

    step\_name: str

    success: bool

    output: Any = None

    error: Exception | None = None

    abort\_sequence: bool = False       \# step requests immediate sequence abort

                                       \# e\.g\. sensor reading makes remaining

                                       \# steps unsafe

## __17\.3 InterruptPolicy__

InterruptPolicy declares how a MultiStepActionNode responds when the ActionMasterNode receives an interrupt request while the action is executing\.

__Policy__

__Behaviour on interrupt request__

__ALWAYS__

Stop at the next step boundary where interruptible=True\. If the current step is interruptible, stop immediately after it completes\. Fast and simple\.

__NEVER__

Run the entire sequence to completion regardless\. The interrupt request is queued; the new action starts after this one finishes\. Models the biological fixed action pattern\.

__CHECKPOINT__

Stop only at steps explicitly flagged as checkpoints \(interruptible=True AND checkpoint=True\)\. More selective than ALWAYS\.

__ROLLBACK__

Stop at the next interruptible step, then call rollback\(\) on each completed non\-interruptible step in reverse order before yielding control\.

class InterruptPolicy\(Enum\):

    ALWAYS      = 'always'

    NEVER       = 'never'

    CHECKPOINT  = 'checkpoint'

    ROLLBACK    = 'rollback'

## __17\.4 MultiStepActionNode__

Extends BaseActionNode\. The developer declares the step sequence and implements execute\_step\(\) for each step by name\. The framework handles iteration, interrupt checking, rollback, and result aggregation\.

class MultiStepActionNode\(BaseActionNode, ABC\):

    interrupt\_policy: InterruptPolicy = InterruptPolicy\.ALWAYS

    @abstractmethod

    def steps\(self\) \-> list\[ActionStep\]:

        """Return the ordered list of ActionSteps for this action\.

        Called once at the start of execute\(\)\. Steps are immutable

        during execution\."""

    @abstractmethod

    async def execute\_step\(

        self,

        step: ActionStep,

        proposal: Proposal,

        completed: list\[StepResult\],

    \) \-> StepResult:

        """Execute one step\. Receives the full list of already\-completed

        step results so later steps can branch on earlier outcomes\.

        Must return a StepResult — never raise\."""

    async def on\_interrupted\(

        self,

        completed: list\[StepResult\],

        pending: list\[ActionStep\],

        proposal: Proposal,

    \) \-> None:

        """Called after the sequence stops due to an interrupt\.

        Default: call rollback\(\) on completed non\-interruptible steps

        in reverse order\. Override for custom cleanup logic\."""

    async def on\_step\_timeout\(

        self,

        step: ActionStep,

        proposal: Proposal,

    \) \-> StepResult:

        """Called when a step exceeds its timeout\.

        Default: return StepResult\(success=False, abort\_sequence=True\)\.

        Override to attempt recovery before aborting\."""

## __17\.5 Mandatory Completion Blocks__

A mandatory completion block is a sequence of consecutive steps where every step has interruptible=False\. The framework will never interrupt execution inside such a block, even if a reflex fires\. The maximum latency before a reflex action executes is therefore bounded by the sum of the remaining mandatory step timeouts — a property that can be asserted statically from the step definitions\.

\# Example: valve control sequence\.

\# Steps 1\-3 form a mandatory block — the valve must be closed

\# before anything else can happen\.

def steps\(self\) \-> list\[ActionStep\]:

    return \[

        ActionStep\('open\_valve',

                   interruptible=False,

                   rollback=self\.\_close\_valve,

                   timeout\_s=2\.0\),

        ActionStep\('transfer\_fluid',

                   interruptible=False,

                   rollback=self\.\_close\_valve,

                   timeout\_s=10\.0\),

        ActionStep\('close\_valve',

                   interruptible=True,    \# ← safe stop point

                   timeout\_s=2\.0\),

        ActionStep\('verify\_closed',

                   interruptible=True,

                   timeout\_s=1\.0\),

    \]

\# Maximum reflex delay for this action = 2\.0 \+ 10\.0 \+ 2\.0 = 14 seconds

\# \(worst case: interrupt arrives just after open\_valve starts\)

\# This bound is computable statically from the step definitions\.

## __17\.6 Interrupt Flow in ActionMasterNode__

ActionMasterNode is updated to track the currently executing action and handle interrupt requests from DecisionMasterNode\. The full interrupt flow is:

\# 1\. New high\-priority proposal arrives while action A is running\.

\#    DecisionMasterNode calls ActionMasterNode\.request\_interrupt\(new\_proposal\)\.

\# 2\. ActionMasterNode checks A\.interrupt\_policy:

\#    NEVER     → queue new\_proposal, A continues to completion

\#    ALWAYS    → set interrupt flag; A stops at next interruptible step

\#    CHECKPOINT→ set interrupt flag; A stops at next checkpoint step

\#    ROLLBACK  → set interrupt flag; A stops \+ rolls back

\# 3\. When A yields \(stopped or completed\):

\#    ActionMasterNode dispatches new\_proposal to its ActionNode\.

\# 4\. If A was in a mandatory block when interrupt arrived:

\#    Interrupt flag is held\. A completes the block first\.

\#    Then interrupt is applied at the next interruptible step\.

\# Interface additions:

class ActionMasterNode\(BaseNode\):

    async def request\_interrupt\(self, new\_proposal: Proposal\) \-> None: \.\.\.

    def current\_action\(self\) \-> BaseActionNode | None: \.\.\.

    def current\_step\(self\) \-> ActionStep | None: \.\.\.

    def is\_interruptible\(self\) \-> bool:

        """True if the running action can be stopped right now\.

        False if inside a mandatory completion block\."""

## __17\.7 InterruptRequest__

The interrupt request is a typed object passed between DecisionMasterNode and ActionMasterNode, carrying the new proposal and the reason for interruption\.

@dataclass

class InterruptRequest:

    new\_proposal: Proposal

    requesting\_instinct\_id: str

    reason: str = ''

    timestamp: float = field\(default\_factory=time\.monotonic\)

## __17\.8 DecisionMasterNode — Updated Behaviour__

DecisionMasterNode now compares incoming proposals against the currently executing action\. If a new proposal has higher priority and the running action is interruptible, it issues an InterruptRequest rather than waiting for the next tick\.

class DecisionMasterNode\(BaseNode\):

    async def decide\(self, proposals: list\[Proposal\]\) \-> Proposal | None:

        """Existing behaviour: select from proposals\."""

    async def on\_new\_proposals\(

        self,

        proposals: list\[Proposal\],

        current\_action: BaseActionNode | None,

        current\_proposal: Proposal | None,

    \) \-> Proposal | None:

        """Extended entry point\. If a proposal outranks the running

        action and the action reports is\_interruptible\(\), emit an

        InterruptRequest to ActionMasterNode and return the new proposal\.

        Otherwise return decide\(proposals\) as normal\."""

## __17\.9 Context — Action Execution State__

The Context snapshot is extended to include the current action execution state so InstinctNodes can factor it into their proposals\. An instinct that knows the agent is mid\-sequence can raise or lower its urgency accordingly\.

@dataclass

class ActionExecutionState:

    action\_id: str | None           \# currently running action, or None

    current\_step: str | None        \# name of the step in progress

    completed\_steps: list\[str\]      \# names of steps already done this run

    interruptible: bool             \# True if safe to interrupt right now

    mandatory\_block\_remaining\_s: float  \# worst\-case time to next safe stop

\# Added to Context:

@dataclass

class Context:

    \# \.\.\. existing fields \.\.\.

    action\_state: ActionExecutionState | None = None

## __17\.10 Example: Robotic Arm Pick\-and\-Place__

A complete example showing a five\-step action with one mandatory block, ROLLBACK interrupt policy, and a reflex interaction\.

class PickAndPlaceActionNode\(MultiStepActionNode\):

    node\_id = 'PickAndPlaceActionNode'

    interrupt\_policy = InterruptPolicy\.ROLLBACK

    def steps\(self\) \-> list\[ActionStep\]:

        return \[

            ActionStep\('move\_to\_object',   interruptible=True\),

            ActionStep\('lower\_gripper',    interruptible=False,

                       rollback=self\.\_raise\_gripper, timeout\_s=2\.0\),

            ActionStep\('close\_gripper',    interruptible=False,

                       rollback=self\.\_open\_gripper,  timeout\_s=1\.5\),

            ActionStep\('raise\_gripper',    interruptible=False,

                       rollback=self\.\_lower\_gripper, timeout\_s=2\.0\),

            ActionStep\('move\_to\_target',   interruptible=True\),

            ActionStep\('release\_gripper',  interruptible=True\),

        \]

    async def execute\_step\(

        self, step: ActionStep,

        proposal: Proposal,

        completed: list\[StepResult\],

    \) \-> StepResult:

        match step\.name:

            case 'move\_to\_object':

                ok = await self\.\_arm\.move\_to\(

                    proposal\.parameters\['object\_position'\]\)

                return StepResult\(step\.name, success=ok\)

            case 'lower\_gripper':

                ok = await self\.\_arm\.lower\(\)

                return StepResult\(step\.name, success=ok\)

            case 'close\_gripper':

                ok = await self\.\_arm\.grip\(\)

                if not ok:

                    \# Gripper missed — abort is safer than continuing

                    return StepResult\(step\.name, success=False,

                                      abort\_sequence=True\)

                return StepResult\(step\.name, success=True\)

            \# \.\.\. remaining steps \.\.\.

    async def \_raise\_gripper\(self\):  await self\.\_arm\.raise\_\(\)

    async def \_open\_gripper\(self\):   await self\.\_arm\.open\(\)

    async def \_lower\_gripper\(self\):  await self\.\_arm\.lower\(\)

If a CollisionReflex fires while the arm is mid\-block \(steps 2–4\), the interrupt is held until step 4 completes\. Then ROLLBACK runs: open\_gripper, lower\_gripper \(in reverse\), then control passes to EmergencyStopActionNode\. Maximum interrupt latency for this scenario: 1\.5 \+ 2\.0 = 3\.5 seconds — bounded and statically computable\.

## __17\.11 Directory Structure — Additions__

arachnite/

    nodes/

        action\.py       \# BaseActionNode, MultiStepActionNode,  ← updated

                        \# ActionMasterNode

    models\.py           \# \+ ActionStep, StepResult, InterruptPolicy,  ← updated

                        \#   InterruptRequest, ActionExecutionState

