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

        reflex\_mode: str = 'tick',              \# 'tick' \| 'event' \(§7\.2\.3\)

        reflex\_signal\_ttl\_s: float \| None = None,  \# None → one tick period

        reflex\_eval\_timeout\_s: float = 0\.05,

    \): \.\.\.

        """The optional ``tick\_instrumenter`` parameter installs an observer
        that receives per\-stage wall\-clock timings on every tick\. Pass ``None``
        \(the default\) for zero instrumentation overhead\. The instrumenter MAY
        be attached or replaced after construction via ``set\_tick\_instrumenter\(\)``
        \(see §7\.5\)\. ``reflex\_mode``, ``reflex\_signal\_ttl\_s`` and
        ``reflex\_eval\_timeout\_s`` configure the event\-driven reflex lane
        \(§7\.2\.3\); any other ``reflex\_mode`` raises ``ValueError``\."""

    @property

    def health\(self\) \-> HealthMonitor: \.\.\.

        """Access the aggregated health view across all supervisors\."""

    async def start\(self\) \-> None:

        """Call setup\(\) on all nodes, start supervisors, begin tick loop\.

        If any master's setup\(\) raises, already\-started masters are torn down

        before re\-raising the exception\. Within a master, every leaf setup\(\)

        is awaited to completion; if one raised, the background tasks of all

        that master's leaves are cancelled and the leaves whose setup\(\)

        succeeded are torn down before the error propagates — a failed

        start\(\) leaves no listener running\."""

    async def stop\(self\) \-> None:

        """Gracefully stop the loop, stop supervisors, call teardown\(\)\. See §15\.2\."""

    async def emergency\_stop\(self\) \-> None:

        """Stop the loop and the reflex lane, cancel every running action

        through its safe\-stop hook \(no rollback\), tear down\. See §15\.3\."""

    async def tick\(self\) \-> None:

        """Execute one full pipeline cycle manually \(useful for testing\)\.

        Launches actions but does not wait for them to finish\."""

    async def wait\_idle\(self, timeout: float \| None = None\) \-> bool:

        """Wait until no action is running or armed\. Returns False on

        timeout\. The Results reach ctx\.last\_results on the next tick\."""

    @property

    def tick\_count\(self\) \-> int: \.\.\.

    @property

    def is\_running\(self\) \-> bool: \.\.\.

    async def register\_sense\_live\(self, node: BaseSenseNode\) \-> None:

        """Register a SenseNode after the runtime has started\.

        Calls setup\(\) immediately \(if running\) and begins supervisor tracking

        \(before start\(\) too: the node is then supervised like a built\-in one\)\.

        If setup\(\) raises, the node is automatically unregistered\.

        Primary hook for Phase 1 self\-assembly: DynamicLoaderAction generates

        a new SenseNode and registers it without restarting the agent\."""

    async def register\_instinct\_live\(self, node: BaseInstinctNode\) \-> None:

        """Register an InstinctNode after the runtime has started\."""

    async def register\_action\_live\(self, node: BaseActionNode\) \-> None:

        """Register an ActionNode after the runtime has started\."""

    async def unregister\_sense\_live\(self, node\_id: str\) \-> None:

        """Unregister a SenseNode while the runtime is running\.

        Removes it from the sense master, untracks it from the supervisor

        \(a restart of it still pending is cancelled\), cancels its background

        tasks, then calls teardown\(\)\.  Safe to call when stopped\.

        Counterpart of register\_sense\_live\(\) — used by the Phase 5

        repair cycle to remove a dead node before registering a replacement\.

        Silent if node\_id is not registered\."""

    async def unregister\_instinct\_live\(self, node\_id: str\) \-> None:

        """Unregister an InstinctNode \(normal or reflex\) while the runtime

        is running\.  Calls teardown\(\), removes from the instinct master,

        and untracks from the supervisor\."""

    async def unregister\_action\_live\(self, node\_id: str\) \-> None:

        """Unregister an ActionNode while the runtime is running\.

        Cancels the action if it is running \(its safe\-stop hook runs\),

        then calls teardown\(\), removes it from the action master, and

        untracks it from the supervisor\."""

## __7\.2 Tick Loop__

The runtime maintains a fixed tick rate\. Reflex evaluation happens first each tick before the normal pipeline\. A tick never waits for an action: ``ActionMasterNode\.launch\(\)`` / ``launch\_reflex\(\)`` start ``execute\(\)`` as tracked tasks that run across later ticks, so sensing, reflex evaluation and deliberation continue while a long ``MultiStepActionNode`` executes \(ADR 0005\)\. If a tick takes longer than the interval, the next tick starts immediately\. A warning is logged when a tick overruns by more than `overrun_warn_pct` \(default 20%\) for `overrun_warn_consecutive` consecutive ticks \(default 3\); the consecutive counter resets on the first non\-overrunning tick\. The dual threshold mirrors `TickBudgetMonitor` \(which emits a `SafetyViolationSignal` after 3 consecutive overruns\) so the runtime warning and the safety signal agree on what counts as a real overrun rather than a transient schedule slip\. Pass `overrun_warn_consecutive=1` for legacy warn\-on\-every\-overrun behaviour\.

async def \_loop\(self\):

    while self\.\_running:

        start = time\.monotonic\(\)

        await self\.tick\(\)

        elapsed = time\.monotonic\(\) \- start

        sleep = max\(0\.0, self\.\_interval \- elapsed\)

        await asyncio\.sleep\(sleep\)

async def tick\(self\) \-> None:

    tick\_start = time\.monotonic\(\)

    \# 0\. Notify all leaf nodes that a new tick is starting

    await asyncio\.gather\(

        self\.\_sense\_master\.notify\_tick\_start\(self\.\_tick\_count\),

        self\.\_instinct\_master\.notify\_tick\_start\(self\.\_tick\_count\),

        self\.\_action\_master\.notify\_tick\_start\(self\.\_tick\_count\),

    \)

    \# 1\. Drain — snapshot\-and\-clear the buffered supervisor / node\_fault and

    \#    bus\-published state\_update signals, and collect the Results of

    \#    actions that finished since the previous tick \(completion order\)\.

    \#    Signals from other AgentNodes \(inject\_remote\_signal, spec §10\.5\)

    \#    come first, right after the local sense output\.

    buffered = \(self\.\_remote\_signal\_buffer \+ self\.\_supervisor\_signal\_buffer

                \+ self\.\_state\_update\_buffer\)

    completed = self\.\_action\_master\.drain\_completed\(\)

    if completed:

        self\.\_last\_results = completed

        self\.\_last\_result  = completed\[0\]

    \# 2\. Sense — poll\_interval\_s is enforced per node; nodes whose

    \#    interval has not elapsed return None and are skipped\. Signals

    \#    pushed since the last tick \(BaseSenseNode\.push\(\)\) are appended\.

    \#    In reflex\_mode='event' the publish inside read\_all\(\) already

    \#    runs the event\-driven reflex lane \(§7\.2\.3\)\.

    signals  = await self\.\_sense\_master\.read\_all\(\)

    signals \+= buffered

    \# 3\. Context update — snapshot the completed Results and the

    \#    execution state of every running action into ctx, then clear

    \#    the runtime\-side slots \(one\-tick lifetime, §7\.2\.1\)\.

    ctx = self\.\_context\.update\(signals, result=self\.\_last\_result,

                               action\_states=\[\.\.\.running actions\.\.\.\],

                               results=self\.\_last\_results\)

    self\.\_last\_results = \[\]
    self\.\_last\_result  = None

    \# 4\. Reflex pass — bypass decision\. In 'event' mode only reflex nodes

    \#    without trigger\_on\_signals are evaluated here\.

    try:

        fired = await self\.\_instinct\_master\.evaluate\_reflex\_pairs\(

            ctx, nodes=self\.\_tick\_reflex\_nodes\(\)\)   \# \(reflex node, proposal\)

    except ReflexConflictError as exc:

        \# reflex\_conflict='raise': logged at ERROR, never aborts the tick;

        \# the conflicting reflexes are launched in priority order\.

        fired = exc\.fired

    for node, rp in fired:                 \# priority descending

        logger\.info\("Reflex fired", \.\.\.\)

        \# The mode comes from the node that produced the proposal, never

        \# from a lookup of rp\.instinct\_id \(ADR 0009\)\.

        outcome = self\.\_action\_master\.launch\_reflex\(rp, node\.preemption\)

        \# LAUNCHED / ARMED: the reflex owns or has claimed its resources

        \# \(§17\.12\)\. DEDUPED: already running or armed as a reflex\.

        \# NOT\_FOUND: logged at WARNING\.

    await asyncio\.sleep\(0\)   \# if any launched: execute\(\) is entered now

    \# 5\. Normal instinct evaluation

    proposals = await self\.\_instinct\_master\.evaluate\_all\(ctx\)

    evaluated\_ids = self\.\_instinct\_master\.last\_evaluated\_ids

    \# 6\. Decision — select proposals; conflicts include shared resources

    \#    and resources claimed by armed reflexes\. A reflex\-owned action

    \#    is never reported interruptible; every deliberative owner is,

    \#    also inside a mandatory block \(the request is latched, ADR 0009\)\.

    to\_dispatch, interrupts = await self\.\_decision\_master\.on\_new\_proposals\_many\(

        proposals,

        running\_proposals=self\.\_action\_master\.current\_proposals\(\),

        running\_interruptible=\{aid: origin \!= 'reflex'\},

        evaluated\_instinct\_ids=evaluated\_ids,

        conflicts=self\.\_action\_master\.conflicting\_owners,

    \)

    \# Notify rejected instincts — MUST use last\_considered \(fresh \+

    \# carried\-forward pending and hand\-off proposals\) so that persistent

    \# proposals that lost the decision also receive on\_proposal\_rejected\(\)\.

    dispatched\_ids = \{p\.instinct\_id for p in to\_dispatch\}

    all\_considered = self\.\_decision\_master\.last\_considered

    rejected = \[p for p in all\_considered if p\.instinct\_id not in dispatched\_ids\]

    if rejected:

        await self\.\_instinct\_master\.notify\_rejected\(rejected\)

    for interrupt\_req in interrupts:

        target = interrupt\_req\.target\_action\_id or interrupt\_req\.new\_proposal\.action\_id

        try:

            await self\.\_action\_master\.request\_interrupt\(interrupt\_req, action\_id=target\)

        except Exception:

            \# Unexpected error — logged as ERROR with full context\.

            logger\.error\("Interrupt request failed", \.\.\.\)

    \# 7\. Act — launch the selected proposals \(highest priority per

    \#    action\_id\) as tasks; nothing is awaited\. A launch that is

    \#    BLOCKED by a running owner — typically one that is unwinding

    \#    from the interrupt just issued — is parked for hand\-off\.

    for p in best\_per\_action\_id\(to\_dispatch\):

        outcome = self\.\_action\_master\.launch\(p, origin='decision'\)

        if outcome\.status is LaunchStatus\.BLOCKED:

            self\.\_decision\_master\.park\(p\)

    \# 8\. Notify all leaf nodes that the tick has ended

    tick\_duration = time\.monotonic\(\) \- tick\_start

    await asyncio\.gather\(

        self\.\_sense\_master\.notify\_tick\_end\(self\.\_tick\_count, tick\_duration\),

        self\.\_instinct\_master\.notify\_tick\_end\(self\.\_tick\_count, tick\_duration\),

        self\.\_action\_master\.notify\_tick\_end\(self\.\_tick\_count, tick\_duration\),

    \)

### __7\.2\.1 ``last\_results`` lifecycle__

The runtime's ``\_last\_results`` / ``\_last\_result`` slots carry action ``Result``s into exactly *one* subsequent tick, providing the feedback channel that ``Result``'s docstring promises and that ``LLMInstinctNode`` reads via ``ctx\.last\_results`` / ``ctx\.last\_result``\.

Actions run as tasks, so a ``Result`` is produced whenever its action finishes — possibly many ticks after the launch\. ``ActionMasterNode`` appends each finished ``Result`` \(and each guard veto, at once\) to a completion buffer\. Lifecycle per tick N:

1\. Tick N starts; step 1 drains the completion buffer \(``drain\_completed\(\)``\) into ``self\.\_last\_results``, in completion order\. ``self\.\_last\_result`` is the first of them\.
2\. Step 3 \(``\_context\.update``\) snapshots them into the new ``Context``\. Every instinct evaluated this tick — reflex or normal — observes them via ``ctx\.last\_results`` / ``ctx\.last\_result``\.
3\. Immediately after the snapshot, the runtime clears ``self\.\_last\_results`` and ``self\.\_last\_result``\. The snapshot inside ``ctx`` is unaffected \(dataclass field copy\)\.
4\. An action that finishes after tick N's drain lands in the buffer and surfaces at tick N\+1\. If nothing finishes, tick N\+1's ``ctx\.last\_results`` is ``\[\]``\.

Each ``Result`` is therefore observed exactly once, on the first tick that starts after the action finished — never repeatedly\. ``tick\(\)`` returning does **not** imply that the actions it launched have finished: a caller that needs completion awaits ``ArachniteRuntime\.wait\_idle\(\)`` \(or ``ActionMasterNode\.wait\_idle\(\)``\) and then runs one more tick, or drives actions with the awaiting ``dispatch\(\)`` \(§7\.2\.2\)\. While an action runs, its ``ActionExecutionState`` appears in ``ctx\.action\_states`` on every tick\.

### __7\.2\.2 Using nodes without the runtime \(direct\-drive\)__

``ArachniteRuntime`` and the tick loop are **optional**\. The node primitives — ``BaseSenseNode``, ``BaseActionNode``, ``ActionMasterNode`` — are usable standalone with a bare ``SignalBus``, driven on a loop the caller owns\. Construct the nodes, ``register\(\)`` actions on an ``ActionMasterNode``, and call ``read\(\)`` / ``evaluate\(\)`` / ``execute\(\)`` / ``await dispatch\(proposal\)`` directly\. No ``RuntimeBuilder``, no ``InstinctMaster``, no ``DecisionMaster``, no free\-running tick\. ``dispatch\(\)`` / ``dispatch\_many\(\)`` await the ``Result`` and apply the same conflict check as ``launch\(\)``: a proposal whose ``action\_id`` or declared resources are owned by a running action gets ``Result\(success=False\)`` without ``execute\(\)`` running \(``ResourceConflictError`` for a shared resource\)\. With ``origin=None`` \(the default\) guards are bypassed only at ``priority >= 200`` \(§5\.7\)\.

**When is the deliberative runtime warranted?** Only when **≥2 autonomous behaviors compete continuously on dynamic priority** — the robotics / edge control shape the ``InstinctMaster`` → ``DecisionMaster`` → tick pipeline is built for\. For a single agent, or a human\-in\-the\-loop application with a *static* priority order \(a coding cockpit, a one\-shot pipeline, a request\-driven service\), prefer direct dispatch \+ your own arbitration\. The direct\-drive pattern used in unit tests \(see ``04_extension_testing``\) is also a legitimate production deployment shape — it is not a test\-only affordance\.

### __7\.2\.3 Event\-driven reflex lane \(``reflex\_mode='event'``\)__

With the default ``reflex\_mode='tick'`` every reflex node is evaluated once per tick in step 4\. With ``ArachniteRuntime\(reflex\_mode='event'\)`` \(``RuntimeBuilder\.reflex\_mode\('event'\)``\), each reflex node that declares ``trigger\_on\_signals`` is moved to the **reflex lane**: the runtime subscribes to those signal kinds on the bus and evaluates the triggered reflexes the moment a matching signal is published, instead of waiting for the next tick:

- A polled sense node's signal arrives during the sense stage \(``read\_all\(\)`` publishes it\), before the context update; the lane's work is charged to the ``sense`` stage\.
- An event\-driven sense node calls ``await self\.push\(signal\)`` from a background task \(e\.g\. a GPIO or ToF interrupt listener started in ``setup\(\)``\)\. The lane evaluates the reflex before ``push\(\)`` returns, outside any tick, which removes tick quantisation from the reflex path\. The pushed signal also reaches the next tick's ``ctx\.signals`` once, for deliberation\.
- The reflex sees ``ContextNode\.reflex\_view\(signals\)``: the freshest signal per ``\(source, kind\)`` of its trigger kinds received within ``reflex\_signal\_ttl\_s`` \(default: one tick period\), the current ``state``, results and action states, and an **empty** ``history``\.
- Evaluation is bounded by ``reflex\_eval\_timeout\_s`` \(default 0\.05 s\)\. A timeout or exception is logged at ERROR and never propagates into the publisher; ``ReflexConflictError`` is handled as in step 4\.
- Proposals go through the same ``launch\_reflex\(\)`` arbitration \(§17\.12\), each with the ``preemption`` of the reflex node that produced it\.
- A reflex is not re\-evaluated by a signal published during its own evaluation \(for example the ``node\_fault`` of a ``node\_fault``\-triggered reflex whose ``evaluate\(\)`` raised\): that would recurse without bound\. The signal is still stored as the latest of its ``\(source, kind\)``, so the reflex's next evaluation within ``reflex\_signal\_ttl\_s`` sees it\. Other lane reflexes it triggers are evaluated as usual\.
- The lane stays live while the runtime is paused \(polled sensing and deliberation stop; a pushed hazard signal still gets its reflex\)\. ``stop\(\)`` and ``emergency\_stop\(\)`` detach it first; an evaluation still in flight then launches nothing\.

Reflex nodes without ``trigger\_on\_signals`` remain in the tick's reflex stage in both modes\. In ``'tick'`` mode ``trigger\_on\_signals`` has no effect on reflex nodes\.

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
`\.reflex\_conflict\(\)`, `\.reflex\_mode\(\)`, `\.strict\_resources\(\)`,
`\.permissions\(\)`, `\.shutdown\(\)`, `\.overrun\_warn\(\)`,
`\.overrun\_warn\_consecutive\(\)`\.

Defaults: `GreedyDecisionNode` strategy, 10\.0 Hz tick rate,
`dispatch\_all` reflex conflict policy, `'tick'` reflex mode,
`strict\_resources=False`\.

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
| 1 | `sense` | `notify\_tick\_start` gather \+ buffer and completed\-result drain \+ `SenseMasterNode\.read\_all\(\)` \(in `'event'` mode including the reflex\-lane work it triggers\) |
| 2 | `context` | `ContextNode\.update\(\)` \(includes action\-state assembly\) |
| 3 | `reflex` | `evaluate\_reflexes\(\)` \+ the `launch\_reflex\(\)` loop \+ one event\-loop yield \(fused because the reflex arc bypasses decision; see §7\.2 step 4\) |
| 4 | `instinct` | `InstinctMasterNode\.evaluate\_all\(ctx\)` \(evaluate only — proposals launch in `act`\) |
| 5 | `decide` | Interruptibility\-map build \+ `on\_new\_proposals\_many` \+ `notify\_rejected` \+ interrupt issuing |
| 6 | `act` | `ActionMasterNode\.launch\(\)` per selected proposal \(\+ `park\(\)` on `BLOCKED`\) \+ `notify\_tick\_end` gather |

The `reflex` and `act` stages measure **launch cost only**: `execute\(\)` runs in its own task, outside every stage, and its duration is not part of any tick\.

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

__Error isolation \(instrumenter error\-isolation ADR\)\.__ Instrumenter implementations MAY raise\.
The runtime MUST catch any exception raised from `on\_stage` or
`on\_tick\_complete`, log it at WARNING with fields `stage` \(or `tick`\),
`error`, and `error\_type`, and continue processing the current tick\. An
instrumenter failure MUST NOT propagate into the tick loop, MUST NOT
fail the tick, and MUST NOT affect downstream stages beyond the dropped
sample for the failing call\. This behaviour is consistent with the
framework's four other observational\-failure isolation sites
\(`ContextNode` history I/O, the `\_loop` tick\-exception handler,
the bounded safe\-stop hook `on\_preempted\(\)`, `NodeSupervisor` setup errors\) and
with the architectural rules that `execute\(\)` and `evaluate\(\)` must
never raise\.


# __15\. Shutdown Sequence__

Graceful shutdown is non\-trivial when actions may be mid\-execution, mandatory completion blocks may be running, and supervisors may be mid\-restart\. The ShutdownCoordinator manages this process to ensure hardware is left in a safe, defined state\.

## __15\.1 Shutdown Phases__

ArachniteRuntime\.stop\(\) first detaches the event\-driven reflex lane \(§7\.2\.3\), then triggers the following ordered phases:

__Phase__

__What happens__

__1\. Stop sensing__

The tick loop is told to stop \(``on\_shutdown\_action`` runs here if configured\)\. No new tick starts, so no new signals enter the pipeline\.

__2\. Drain reflexes__

The in\-flight tick, including its reflex stage, runs to completion \(bounded by ``teardown\_timeout\_s``, after which the loop task is cancelled\)\. Actions run as tasks outside the tick, so this wait is at most one tick long\.

__3\. Complete mandatory block__

Every running multi\-step action receives an InterruptRequest with reason 'stop\(\) called' \(``ActionMasterNode\.interrupt\_all\(\)``\)\. Inside a mandatory block the interrupt is held, so the block runs to its end; the coordinator waits until no running action is inside a block\. Timeout: the longest remaining mandatory\-block time across running actions × ``mandatory\_block\_timeout\_multiplier`` \(default 1\.1\), or ``teardown\_timeout\_s`` when no action is in a block\. No new step starts after the block: each action stops at the first boundary its policy allows\.

__4\. Interrupt remaining action__

The coordinator waits up to ``teardown\_timeout\_s`` for the interrupted actions to stop at their boundary — ROLLBACK actions run their rollback walk here\. Whatever is still running then \(single\-step actions, NEVER policy, overrunning steps\) is cancelled through its ``on\_preempted\(\)`` safe\-stop hook, without rollback; the coordinator waits for the largest ``safe\_stop\_bound\_s\(\)`` of the running actions \(the hook bound, and a rollback walk already in progress — §17\.12\) \+ 0\.5 s\.

__5\. Stop supervisors__

All NodeSupervisors call `cancel\_restart\_tasks\(\)` to cancel in\-flight restart tasks before marking nodes as STOPPED\. Restart tasks MUST be cancelled before any node is marked STOPPED to prevent a restart racing with teardown\.

__6\. Teardown nodes__

teardown\(\) is called on all nodes concurrently, with a configurable grace period \(default 5s\)\. Every teardown runs to completion; one that raises — or ends in its own ``CancelledError``, reported as a ``RuntimeError`` — is logged, the shutdown still completes \(phase 7\), and the first such error is then re\-raised to ``stop\(\)`` / ``wait\(\)``\.

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

When runtime\.emergency\_stop\(\) is called \(e\.g\. from a signal handler on SIGTERM or SIGINT\), the ShutdownCoordinator is bypassed:

1\. The action master is closed at once — ``launch\(\)`` / ``launch\_reflex\(\)`` return ``LaunchStatus\.CLOSED`` until the next ``setup\(\)`` — and the tick loop and the event\-driven reflex lane are stopped; an in\-flight tick is cancelled and awaited\.
2\. Every running action is cancelled at its current ``await`` — inside a mandatory block too — and its ``on\_preempted\(\)`` safe\-stop hook runs, bounded as in §17\.12; a safe\-stop already running for a reflex preemption is waited for, not cut short\. Armed reflexes are dropped\. **No rollback runs**: reversing a physical operation during an emergency can itself be unsafe\.
3\. All master nodes are torn down\.

__Shutdown task\.__ Both ``stop\(\)`` and ``emergency\_stop\(\)`` run the shutdown in one runtime\-owned task, so it completes even if the caller is cancelled \(for instance an action that called it and is cancelled by step 2\)\. ``wait\(\)`` returns when that task has finished — the safe\-stops have run and the nodes are torn down — not merely when it started\. A second call waits for the shutdown in progress; ``emergency\_stop\(\)`` after a completed shutdown does nothing, so nodes are never torn down twice\. ``emergency\_stop\(\)`` during a graceful ``stop\(\)`` takes it over \(``stop\(\)`` then returns normally\), unless that stop is already tearing nodes down\. Called from inside runtime work — the tick in flight, ``start\(\)``, the shutdown itself \(e\.g\. a node's ``teardown\(\)``, ``on\_shutdown\_action``, a bus subscriber during phase 5\) or a running action including its ``on\_preempted\(\)`` safe\-stop — both return as soon as the shutdown has started, since waiting there would wait for the caller itself, and the rest of that tick launches nothing deliberative; elsewhere they return when it is complete\. A task started from such work counts as inside only while that work lasts\. A stop at any point of ``start\(\)`` before the loop starts aborts the start \(the masters already set up are torn down and the loop never starts\); a concurrent second ``start\(\)`` waits for the first; ``start\(\)`` after a shutdown first waits for it to finish, following an emergency take\-over, and an ``emergency\_stop\(\)`` during that wait takes the graceful shutdown over\. ``wait\(\)`` called during ``start\(\)`` waits for that run\. The emergency path cancels pending supervisor restarts before the teardown, as phase 5 does\. A master teardown that raises is logged and re\-raised only after every other teardown has finished; an ``emergency\_stop\(\)`` that arrives while a graceful stop is tearing nodes down waits for it and re\-raises its error\.

Emergency stop is a shutdown path\. Cutting into a mandatory block to *respond* to a hazard while the agent keeps running is the job of a ``ReflexPreemption\.PREEMPT`` reflex \(§17\.12\); the complete list of ways a mandatory block can end early is in §17\.5\.

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

For power management on edge devices, the runtime supports pause and resume without a full shutdown\. All nodes receive on\_pause\(\) / on\_resume\(\) lifecycle hooks\. Supervisors continue monitoring during pause; sensing stops; the tick loop suspends\. Actions already running keep running to completion \(pause launches nothing new and stops nothing: a tick in flight when ``pause\(\)`` arrives issues no deliberative interrupt either\), and the event\-driven reflex lane \(§7\.2\.3\) stays live\. A tick the loop is running when ``pause\(\)`` is called finishes first — its reflexes launch, its deliberative launches are dropped — and only then do the nodes get ``on\_pause\(\)``\. Signals pushed while paused queue for the next tick up to ``MAX\_PENDING\_PUSHES`` \(256\) per sense node; older ones are dropped with a warning\. A ``resume\(\)`` while ``pause\(\)`` is still waiting for the tick wins: ``on\_pause\(\)`` and ``on\_resume\(\)`` are applied once each, in order\. A ``pause\(\)`` during ``start\(\)`` holds: the nodes get ``on\_pause\(\)`` once they are set up, and the runtime starts paused\. ``start\(\)`` after a stop issued while paused starts unpaused, and gives ``on\_resume\(\)`` to the nodes once they are set up again\. Results of actions the lane launches while paused are kept up to ``MAX\_PENDING\_RESULTS`` \(1024\), bus\-published state updates and supervisor signals up to 1024 each; older ones are dropped with a warning\. A restart's first tick sees none of the previous run's buffered signals or Results\.

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

__ActionMasterNode\.launch\(\) / launch\_reflex\(\)__

O\(1\) lookup by node\_id \+ O\(r\) conflict check, r = declared resources of the action\. Execution time is action\-defined and runs in a task outside the tick\.

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

Bounded reflex admission

Derived from declared step and safe\-stop timeouts under stated assumptions \(§17\.12\); not a hard real\-time guarantee

Not available

Actuator resource exclusivity

Declared per action, enforced by ActionMasterNode with reflex preemption

ROS 2 \(ros2\_control\): exclusive command\-interface claiming per controller

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

ActionStep is the atomic unit of a multi\-step action\. Each step declares its name, whether execution may stop at the boundary before it, and an optional rollback callable to undo its effects if the action is interrupted after a block\.

@dataclass

class ActionStep:

    name: str                          \# human\-readable label for logging

    interruptible: bool = True         \# False: part of a mandatory block

    rollback: Callable\[\[\], Awaitable\[None\]\] | None = None

                                       \# undo function, called if this step

                                       \# completed inside an interrupted block

    timeout\_s: float | None = None     \# per\-step timeout \(overrides node default\)

    checkpoint: bool = False           \# used by CHECKPOINT policy

    metadata: dict = field\(default\_factory=dict\)

Steps are declared as a flat list\. Consecutive non\-interruptible steps form an implicit mandatory block — no deliberative interrupt is honoured inside it \(§17\.5 lists the only ways it can end early\)\. Every step boundary that is not strictly inside a block is a safe stop point — the boundary before an interruptible step, and the boundary *before* a block \(so a pending interrupt is honoured before the block starts, instead of running the block only to undo it\)\. Boundaries between two non\-interruptible steps are not stop points\.

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

    interrupted: bool = False          \# step observed a cooperative interrupt

                                       \# mid\-execution and bailed \(see 17\.4

                                       \# check\_interrupt\(\)\)

A step that sets ``interrupted=True`` is surfaced by ``execute\(\)`` as ``Result\(interrupted=True, stopped\_at\_step=\.\.\.\)`` via the normal interrupt path — **not** as a ``StepAbortError`` failure — but only where the policy allows a cooperative stop: an interruptible step under ``ALWAYS`` or ``ROLLBACK``, a checkpoint step under ``CHECKPOINT``, never under ``NEVER`` and never inside a mandatory block \(the same rule as ``check\_interrupt\(\)``\)\. Elsewhere the flag is a contract violation; it is logged and ignored, and the sequence continues\. This distinguishes "the step chose to stop because a higher\-priority action wants control" from "the step aborted the sequence due to an error" \(``abort\_sequence``\)\.

**Aborts\.** A step aborts the sequence when it returns ``abort\_sequence=True``, when it exceeds its timeout \(default ``on\_step\_timeout\(\)``\), or when ``execute\_step\(\)`` raises — the exception is converted into an aborting ``StepResult`` and logged at ERROR\. Every abort goes through ``on\_aborted\(\)`` \(§17\.4\) and ``execute\(\)`` returns ``Result\(success=False, stopped\_at\_step=\.\.\., error=StepAbortError\)``, chained to the step's error, or ``error=RollbackError`` if the abort rollback failed\.

## __17\.3 InterruptPolicy__

InterruptPolicy declares how a MultiStepActionNode responds when the ActionMasterNode receives a deliberative interrupt request \(and the shutdown interrupt, §15\.1\) while the action is executing\. It does not govern reflexes: what a reflex does to a running action is declared by the reflex's ``ReflexPreemption`` \(§17\.12\)\.

__Policy__

__Behaviour on interrupt request__

__ALWAYS__

Stop at the next safe stop point \(any boundary not strictly inside a mandatory block\)\. Outside a block this is as soon as the current step completes\. Fast and simple\.

__NEVER__

Run the entire sequence to completion regardless\. The interrupt request is ignored; the interrupting proposal stays parked for hand\-off \(§17\.6\) and starts once this action releases its resources, if that happens within ``max\_handoff\_ticks``\. Models the biological fixed action pattern\.

__CHECKPOINT__

Stop only at boundaries before steps explicitly flagged as checkpoints \(interruptible=True AND checkpoint=True\)\. More selective than ALWAYS\.

__ROLLBACK__

Stop at the next interruptible boundary, then call rollback\(\) on each completed non\-interruptible step in reverse order before yielding control\. Every rollback is attempted even if an earlier one raises; failures are collected and raised as one ``RollbackError`` \(``failures`` lists each step\), which ``execute\(\)`` reports as ``Result\(interrupted=True, rolled\_back=False, error=RollbackError\)``\.

class InterruptPolicy\(Enum\):

    ALWAYS      = 'always'

    NEVER       = 'never'

    CHECKPOINT  = 'checkpoint'

    ROLLBACK    = 'rollback'

## __17\.4 MultiStepActionNode__

Extends BaseActionNode\. The developer declares the step sequence and implements execute\_step\(\) for each step by name\. The framework handles iteration, interrupt checking, rollback, and result aggregation\.

class MultiStepActionNode\(BaseActionNode, ABC\):

    interrupt\_policy: InterruptPolicy = InterruptPolicy\.ALWAYS

    rollback\_on\_preempt: bool = False   \# run the rollback walk after

                                        \# on\_preempted\(\) when a reflex preempts

    \# Inherited from BaseActionNode \(§5\.7\): resources, safe\_stop\_timeout\_s,

    \# on\_preempted\(\)

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

    def check\_interrupt\(self\) \-> bool:

        """Poll from inside a long\-running execute\_step\(\) to cooperatively bail\."""

    async def on\_interrupted\(

        self,

        completed: list\[StepResult\],

        pending: list\[ActionStep\],

        proposal: Proposal,

    \) \-> None:

        """Called after the sequence stops due to an interrupt \(ROLLBACK

        policy\)\. Default: call rollback\(\) on completed non\-interruptible

        steps in reverse order, attempting all of them, then raise one

        aggregated RollbackError if any failed\. Override for custom

        cleanup logic\."""

    async def on\_aborted\(

        self,

        completed: list\[StepResult\],

        failed\_step: ActionStep,

        proposal: Proposal,

    \) \-> bool:

        """Called when a step aborts the sequence \(abort\_sequence=True,

        step timeout, or execute\_step\(\) raised\)\. Default: under ROLLBACK

        run the same rollback walk and return True; under every other

        policy do nothing and return False\. Returns whether a rollback ran\."""

    def request\_interrupt\(self, request: InterruptRequest\) \-> None:

        """Set the interrupt flag; honoured at the next boundary the

        policy allows\. Called by ActionMasterNode\."""

    def request\_reflex\_preempt\(self, by: Proposal\) \-> None:

        """Called by ActionMasterNode for a WAIT\_FOR\_BLOCK reflex: stop at

        the first interruptible boundary after the current mandatory

        block, whatever the interrupt\_policy, run the safe\-stop, and

        return Result\(preempted=True\)\."""

    async def on\_step\_timeout\(

        self,

        step: ActionStep,

        proposal: Proposal,

    \) \-> StepResult:

        """Called when a step exceeds its timeout\.

        Default: return StepResult\(success=False, abort\_sequence=True\)\.

        Override to attempt recovery before aborting\."""

__Cooperative mid\-step interrupt\.__ The framework checks for interrupts *between* steps\. A single long\-running step \(an LLM call, a long subprocess\) cannot be interrupted by the framework on its own\. To let such a step yield mid\-execution, ``execute\_step\(\)`` may poll ``check\_interrupt\(\)`` and, when it returns True, stop and return ``StepResult\(\.\.\., interrupted=True\)``\. ``check\_interrupt\(\)`` returns True only when an interrupt is pending AND the current step may honour it: it returns False inside a non\-interruptible \(mandatory\) step and under ``InterruptPolicy\.NEVER``, and under ``CHECKPOINT`` it is True only at a checkpoint step\. It therefore can **never** bypass a mandatory completion block — the ``MandatoryBlockMonitor`` invariant holds\. As defense\-in\-depth, ``execute\(\)`` ignores an ``interrupted=True`` result returned from a non\-interruptible step\. Polling ``check\_interrupt\(\)`` is optional; steps that never call it behave exactly as before\.

__Cancellation safety\.__ Independently of the cooperative path, a ``PREEMPT`` reflex \(§17\.12\), ``emergency\_stop\(\)``, graceful\-shutdown phase 4 and the node\-level ``timeout\_s`` cancel ``execute\(\)`` at its current ``await`` by delivering ``asyncio\.CancelledError`` — inside a mandatory block too — and every one of them then runs the run's ``on\_preempted\(\)`` safe\-stop \(for the timeout with a stand\-in proposal whose ``instinct\_id`` is ``'timeout'``\)\. Step code must therefore be cancellation\-safe\. A hardware call running in ``asyncio\.to\_thread\(\)`` cannot be cancelled and completes in its thread; the ``on\_preempted\(\)`` safe\-stop hook must cope with such an in\-flight command\.

## __17\.5 Mandatory Completion Blocks__

A mandatory completion block is a sequence of consecutive steps where every step has interruptible=False\. "Mandatory block atomicity" is three separate properties \(ADR 0005\):

1\. __Deliberative atomicity__ \(enforced\)\. No deliberative interrupt cuts into a block: ``check\_interrupt\(\)`` returns False there and ``execute\(\)`` honours a pending interrupt only at a stop point\. An interrupt pending when the block is about to start is honoured at the boundary before it \(the block is never entered\); one arriving inside the block — a higher\-priority deliberative proposal's \(``ActionMasterNode\.request\_interrupt\(\)``\) or the shutdown interrupt — is latched on the action and honoured at the first stop point after the block, under the action's ``InterruptPolicy``\. Requests are never refused, only deferred \(ADR 0009\)\.
2\. __Resource exclusivity__ \(enforced for declared resources\)\. While the action runs, no other action can own any of its declared ``resources`` \(§17\.12\)\.
3\. __Bounded reflex admission__ \(derived, not guaranteed\)\. A reflex whose action needs the owner's resources is admitted within a bound computed from declared timeouts — immediately after the owner's safe\-stop under ``PREEMPT``, after the remaining block under ``WAIT\_FOR\_BLOCK``\. The bounds and their assumptions are in §17\.12; they are not hard real\-time guarantees\.

A block can end before its last step only through: a ``ReflexPreemption\.PREEMPT`` reflex \(owner cancelled, safe\-stop, no rollback unless ``rollback\_on\_preempt``\); ``emergency\_stop\(\)`` \(§15\.3\); a step timeout, ``abort\_sequence=True`` or a raising step \(``on\_aborted\(\)``, §17\.2\); the node\-level ``timeout\_s`` \(``execute\(\)`` is cancelled, the run's ``on\_preempted\(\)`` safe\-stop runs, then the abort rules for the step that was cut — ``on\_aborted\(\)``, which rolls back only under ``ROLLBACK`` — then ``on\_timeout\(\)``; the Result keeps ``step\_results`` and ``stopped\_at\_step``, has ``cancelled=True``, and a run cut inside a block is never retried, ADR 0009\); and graceful\-shutdown phase 4 cancelling an action that overran its block timeouts \(§15\.1\)\.

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

                   interruptible=False,

                   timeout\_s=2\.0\),

        ActionStep\('verify\_closed',

                   interruptible=True,    \# ← safe stop point before it

                   timeout\_s=1\.0\),

    \]

\# Worst\-case time until the action leaves its mandatory block

\# = 2\.0 \+ 10\.0 \+ 2\.0 = 14 seconds \(just after open\_valve starts\)\.

\# No deliberative interrupt takes effect earlier\. A WAIT\_FOR\_BLOCK reflex is admitted within

\# 14 s \+ safe\_stop\_timeout\_s; a PREEMPT reflex within safe\_stop\_timeout\_s

\# \(§17\.12\)\. Both are computed from the declared timeouts\.

## __17\.6 Interrupt Flow in ActionMasterNode__

ActionMasterNode tracks every running action, the resources it owns \(§17\.12\) and its dispatch origin \(``'reflex'``, ``'decision'`` or ``'direct'``\)\. The deliberative interrupt flow under the runtime is:

\# 1\. Decide: a selected proposal P conflicts with running owners O —

\#    same action\_id, a shared declared resource, or a resource claimed by

\#    an armed reflex \(ActionMasterNode\.conflicting\_owners\)\.

\# 2\. If every owner in O is running, not reflex\-owned, and of lower

\#    priority than P — inside a mandatory block or not:

\#    DecisionMasterNode emits one InterruptRequest per owner

\#    \(target\_action\_id = owner\) and returns P for dispatch\.

\#    Otherwise P is held: not dispatched, kept only under the persist

\#    rules \(§5\.6\)\.

\# 3\. The runtime calls ActionMasterNode\.request\_interrupt\(req, target\)\.

\#    It never refuses: it sets the owner's interrupt flag, which is held

\#    through a mandatory block \(deliberation never cuts into one, ADR

\#    0009\) and honoured at the first stop point the policy allows:

\#    ALWAYS     → owner stops at the next interruptible boundary

\#    CHECKPOINT → owner stops at the next checkpoint boundary

\#    ROLLBACK   → owner stops at the next interruptible boundary and

\#                 rolls back

\#    NEVER      → flag ignored; owner runs to completion

\#    A single\-step BaseActionNode has no boundary and runs to completion\.

\# 4\. Act: launch\(P\) is BLOCKED while an owner still holds the action or

\#    its resources, so P is parked for hand\-off

\#    \(DecisionMasterNode\.park\(\)\)\. Every tick the parked P re\-enters the

\#    decision pool until it launches — immediately after the owners

\#    release —, its instinct makes a new proposal \(which supersedes it\),

\#    or max\_handoff\_ticks \(default 100\) pass\.

\# Interface additions:

class ActionMasterNode\(BaseNode\):

    async def request\_interrupt\(

        self, request: InterruptRequest, action\_id: str \| None = None,

    \) \-> None:

        """Target action\_id, else request\.target\_action\_id, else the

        highest\-priority running action\. Never refuses: inside a

        mandatory block the request is latched and honoured at the first

        stop point after it \(ADR 0009\)\."""

    def interrupt\_all\(self, request: InterruptRequest\) \-> None:

        """Deliver request to every running multi\-step action \(as

        request\_interrupt\(\): held through a mandatory block\)\. Used by

        graceful shutdown\."""

    def current\_action\(self\) \-> BaseActionNode \| None: \.\.\.

    def current\_step\(self, action\_id: str \| None = None\) \-> ActionStep \| None: \.\.\.

    def is\_interruptible\(self, action\_id: str \| None = None\) \-> bool:

        """True if the given \(or every\) running action can be stopped

        right now\. False if inside a mandatory completion block\."""

    def remaining\_mandatory\_s\(self\) \-> float:

        """Longest remaining mandatory\-block time across running actions\."""

## __17\.7 InterruptRequest__

The interrupt request is a typed object passed between DecisionMasterNode and ActionMasterNode, carrying the new proposal and the reason for interruption\.

@dataclass

class InterruptRequest:

    new\_proposal: Proposal

    requesting\_instinct\_id: str

    reason: str = ''

    timestamp: float = field\(default\_factory=time\.monotonic\)

    target\_action\_id: str \| None = None   \# running action to stop; None →

                                          \# new\_proposal\.action\_id

Reflexes never issue InterruptRequests; they act through ``ReflexPreemption`` \(§17\.12\)\.

## __17\.8 DecisionMasterNode — Updated Behaviour__

DecisionMasterNode compares the selected proposals against the running actions they conflict with and issues InterruptRequests per §17\.6\. It also holds the hand\-off pool of proposals that were selected but could not start yet\.

class DecisionMasterNode\(BaseNode\):

    def \_\_init\_\_\(self, bus, strategy=None, \.\.\., max\_pending\_ticks: int = 50,

                 max\_handoff\_ticks: int = 100\): \.\.\.

    async def decide\(self, proposals: list\[Proposal\]\) \-> Proposal \| None:

        """Existing behaviour: select from proposals\."""

    async def on\_new\_proposals\_many\(

        self,

        proposals: list\[Proposal\],

        running\_proposals: dict\[str, Proposal\],

        running\_interruptible: dict\[str, bool\],

        evaluated\_instinct\_ids: set\[str\] \| None = None,

        conflicts: Callable\[\[Proposal\], set\[str\]\] \| None = None,

    \) \-> tuple\[list\[Proposal\], list\[InterruptRequest\]\]:

        """Entry point used by the runtime \(§17\.6\)\. ``conflicts`` maps a

        proposal to the running action\_ids it conflicts with; the runtime

        passes ActionMasterNode\.conflicting\_owners\. Without it only a

        running action with the same action\_id conflicts\."""

    def park\(self, proposal: Proposal\) \-> None:

        """Hold a selected proposal for hand\-off \(§17\.6 step 4\)\."""

    @property

    def handoff\_proposals\(self\) \-> dict\[str, Proposal\]: \.\.\.

    async def on\_new\_proposals\(

        self,

        proposals: list\[Proposal\],

        current\_proposal: Proposal \| None,

        action\_is\_interruptible: bool,

    \) \-> tuple\[Proposal \| None, InterruptRequest \| None\]:

        """Single\-action form for runtime\-less callers: if the chosen

        proposal outranks current\_proposal and the action is

        interruptible, also return an InterruptRequest\."""

## __17\.9 Context — Action Execution State__

The Context snapshot is extended to include the current action execution state so InstinctNodes can factor it into their proposals\. An instinct that knows the agent is mid\-sequence can raise or lower its urgency accordingly\.

@dataclass

class ActionExecutionState:

    action\_id: str | None           \# currently running action, or None

    current\_step: str | None        \# name of the step in progress

    completed\_steps: list\[str\]      \# names of steps already done this run

    interruptible: bool             \# True if safe to interrupt right now

    mandatory\_block\_remaining\_s: float  \# worst\-case remaining time of the

                                        \# block the action is in now: the

                                        \# current step plus the contiguous

                                        \# non\-interruptible steps after it

                                        \# \(0 outside a block; later blocks

                                        \# are not counted\)

\# Added to Context:

@dataclass

class Context:

    \# \.\.\. existing fields \.\.\.

    action\_state: ActionExecutionState | None = None

    action\_states: list\[ActionExecutionState\]   \# every running multi\-step

                                                \# action, refreshed each tick

## __17\.10 Example: Robotic Arm Pick\-and\-Place__

A complete example showing a six\-step action with one mandatory block, ROLLBACK interrupt policy, declared resources, a safe\-stop hook, and a reflex interaction\.

class PickAndPlaceActionNode\(MultiStepActionNode\):

    node\_id = 'PickAndPlaceActionNode'

    interrupt\_policy = InterruptPolicy\.ROLLBACK

    resources = frozenset\(\{'arm', 'gripper'\}\)

    safe\_stop\_timeout\_s = 0\.5

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

    async def on\_preempted\(self, proposal, preempted\_by\):

        await self\.\_arm\.brake\(\)          \# hold position, keep the grip

    async def \_raise\_gripper\(self\):  await self\.\_arm\.raise\_\(\)

    async def \_open\_gripper\(self\):   await self\.\_arm\.open\(\)

    async def \_lower\_gripper\(self\):  await self\.\_arm\.lower\(\)

Assume a CollisionReflex whose action ``EmergencyRetractActionNode`` declares ``resources = frozenset\(\{'arm'\}\)``, and that it fires while the arm is mid\-block \(steps 2–4\):

- **``preemption = ReflexPreemption\.PREEMPT``** \(default\): the pick is cancelled at its current ``await``, ``on\_preempted\(\)`` brakes the arm, the pick's Result has ``preempted=True`` and no rollback runs; then the retract starts\. Admission delay ≤ ``safe\_stop\_timeout\_s`` = 0\.5 s\.
- **``preemption = ReflexPreemption\.WAIT\_FOR\_BLOCK``**: the reflex is armed and claims ``arm``; the block runs to its end; at the boundary before ``move\_to\_target`` the pick stops — regardless of ROLLBACK — runs ``on\_preempted\(\)``, releases, and the retract starts\. Admission delay ≤ remaining block \+ ``safe\_stop\_timeout\_s``; worst case \(just after ``lower\_gripper`` starts\) 2\.0 \+ 1\.5 \+ 2\.0 \+ 0\.5 = 6\.0 s\.

A **deliberative** proposal for the same resources \(say a priority\-120 obstacle\-avoidance instinct\) that arrives while the pick is in the block interrupts it at once, but the request is latched: the block runs to its end, and the pick stops at the first stop point after it \(the boundary before ``move\_to\_target``\) and ROLLBACK walks the completed block in reverse: ``\_lower\_gripper`` \(undoes raise\_gripper\), ``\_open\_gripper`` \(undoes close\_gripper\), ``\_raise\_gripper`` \(undoes lower\_gripper\)\. The proposal is parked meanwhile and launches as soon as the pick releases\.

## __17\.11 Directory Structure — Additions__

arachnite/

    nodes/

        action\.py       \# BaseActionNode, MultiStepActionNode,  ← updated

                        \# ActionMasterNode, LaunchStatus, LaunchOutcome

    models\.py           \# \+ ActionStep, StepResult, InterruptPolicy,  ← updated

                        \#   InterruptRequest, ActionExecutionState,

                        \#   ReflexPreemption

## __17\.12 Reflex–Action Arbitration__

Actions run as tasks \(§7\.2\), so a reflex can fire while another action is executing — including inside its mandatory block\. This section defines what happens when the reflex's action needs something the running action holds \(ADR 0005\)\.

__Resources and conflicts\.__ ``BaseActionNode\.resources`` names the actuators an action commands\. A proposal **conflicts** with a running action when they share the ``action\_id`` or any declared resource\. ActionMasterNode reserves the action and all its resources synchronously, before ``execute\(\)`` is started as a task, and releases them when the task ends\. This gives **actuator exclusivity**: at most one running action owns a given resource at any time\. An action with no declared resources conflicts only on its own ``action\_id``; ``ActionMasterNode\(strict\_resources=True\)`` rejects such actions at registration\. A deliberative launch that conflicts returns ``LaunchStatus\.BLOCKED`` and is not started\.

class ReflexPreemption\(Enum\):

    PREEMPT        = 'preempt'          \# default

    WAIT\_FOR\_BLOCK = 'wait\_for\_block'

class BaseReflexInstinctNode\(BaseInstinctNode\):

    preemption: ReflexPreemption = ReflexPreemption\.PREEMPT

class BaseActionNode\(BaseNode\):

    resources: frozenset\[str\] = frozenset\(\)

    safe\_stop\_timeout\_s: float = 1\.0

    async def on\_preempted\(self, proposal: Proposal,

                           preempted\_by: Proposal\) \-> None:

        """Forward safe\-stop \("brake, keep the grip"\)\. Default: no\-op\."""

__Mode per node\.__ ``preemption`` is a property of the reflex node, read when its proposal is launched; it is not reassigned inside ``evaluate\(\)``\. A re\-fire while the reflex is armed is de\-duplicated, so the mode of an armed reflex does not change\. A situation\-dependent choice is two reflex nodes with disjoint predicates and distinct target actions; a higher\-priority ``PREEMPT`` reflex still cuts a block that a ``WAIT\_FOR\_BLOCK`` reflex is armed to wait out \(``formal/verify\_two\_reflexes\.py``\)\. Exclusion is per runtime \(one ``ActionMasterNode``\) and over framework\-scheduled commands: a command handed to a worker thread or a device controller is halted by the safe\-stop hook, not by the cancellation\.

__Arbitration\.__ The runtime launches every reflex proposal with ``ActionMasterNode\.launch\_reflex\(proposal, node\.preemption\)``\. When the reflex must wait, it is **armed**: it claims its resources, so ``conflicting\_owners\(\)`` reports the claim and no deliberative proposal can launch on them meanwhile\. The armed reflex starts the moment its last owner releases — from the owner's task, before any later tick — and armed reflexes are admitted in priority order: a lower\-priority armed reflex is not admitted on a resource that a higher\-priority armed reflex still waits for, and every armed reflex keeps its claim until it starts or is cancelled\. Cancelling an armed reflex \(``ActionMasterNode\.cancel\(\)``\) withdraws it: an owner that was only waiting to hand over to it carries on\. To **preempt** an owner, its task is cancelled at its current ``await`` \(even inside a mandatory block\), ``on\_preempted\(\)`` runs in its own task bounded by ``safe\_stop\_timeout\_s`` \(a hook that raises is logged; one that overruns is cancelled, and abandoned — left running, logged — if it has not ended ``SAFE\_STOP\_ABANDON\_GRACE\_S`` = 0\.1 s later, so even a hook that swallows its cancel holds a reflex back at most ``safe\_stop\_timeout\_s`` \+ 0\.1 s\), and the rollback walk runs only if the owner sets ``rollback\_on\_preempt = True`` — and not if a shutdown \(``cancel\_all\(\)``\) arrives before the safe\-stop has started\. A run walks at most once: a preemption arriving during or after an interrupt or abort rollback walk does not start the walk again \(the Result still reports a walk that completed\)\. The safe\-stop \(with that walk\) runs at most once per run: it is shielded, so a later cancel — a second reflex, ``emergency\_stop\(\)``, shutdown — waits for it instead of cutting it short, and a ``WAIT\_FOR\_BLOCK`` hand\-off runs it in the master's task, outside the action's ``timeout\_s``\. A hook that ends in ``CancelledError`` of its own is reported like a raising hook\. The cancel is also recorded on the node, so a step that completes in the same event\-loop turn cannot swallow it \(``asyncio\.wait\_for`` on Python 3\.10\)\. The owner's Result has ``preempted=True`` and ``interrupted=True`` and is never retried; an awaited ``dispatch\(\)`` preempted before its first step returns that Result rather than raising\. A run the master cancels through ``cancel\(\)`` or ``cancel\_all\(\)`` likewise ends with a Result \(``interrupted=True``, ``preempted=False``\), for ``launch\(\)`` and ``dispatch\(\)`` alike; if the *caller* of an awaited ``dispatch\(\)`` is cancelled, the action is stopped the same way and the caller then gets the ``CancelledError``\. ``ActionMasterNode\.cancel\(action\_id\)`` withdraws a reflex armed under ``action\_id`` if there is one and otherwise cancels the running run; withdrawing never touches a running run, even one under the same id\. ``close\(\)`` drops every armed reflex too\. A cancel that ``execute\(\)`` swallowed \(a Python 3\.10 ``asyncio\.wait\_for``, or an action that turned the ``CancelledError`` into a Result\) still ends the run through its safe\-stop, and one that lands as a safe\-stop finishes keeps that safe\-stop's Result\. A cancel from outside the master that arrives during a safe\-stop still propagates once the safe\-stop is done: the master counts the cancels it sends a run, and a run that received more ends cancelled \(Python 3\.10 cannot count cancel requests, so there only one that arrives as a separate delivery is noticed\)\. ``wait\_idle\(\)`` returns only once the finished runs' Results are buffered\. ``dispatch\_many\(\)`` checks every action exists before starting any\. A reflex that waits only on lower\-priority armed claims, with no running owner, starts at once\. The action and step timeouts run in the action's own task \(``asyncio\.timeout``, or an equivalent on Python 3\.10\), so no inner task can be orphaned: the deadline cancels the work once, clean\-up it runs in response is not cut short, and work that swallows that cancel but returns after the deadline is still a timeout\. Each rollback callable is bounded the same way by its step's ``timeout\_s`` \(the action's if unset\); one that overruns is a rollback failure\. A run whose rollback walk the action timeout cut short is not retried\. An action timeout is a cancellation like the others: the run's ``on\_preempted\(\)`` runs \(shielded, bounded by ``safe\_stop\_timeout\_s``, with the stand\-in ``'timeout'`` proposal and without the ``rollback\_on\_preempt`` walk\), then a multi\-step run follows the abort rules for the step that was cut \(``on\_aborted\(\)``\), then ``on\_timeout\(\)`` supplies the Result, which keeps ``step\_results``, ``stopped\_at\_step`` and ``rolled\_back`` and has ``cancelled=True``\. A cancel arriving during that safe\-stop ends the run through the cancel path without running the hook again \(a run's hook runs at most once\)\. A run the timeout cut inside a mandatory block is never retried\. Two Python 3\.10 limits: it does not count cancel requests, so a cancel from outside the master \(e\.g\. event\-loop shutdown\) that lands in the same turn as a step timer is reported as the step timeout; and a 3\.10 ``asyncio\.wait\_for`` inside a step can swallow the timer's cancel, so that step ends when it next returns \(still reported as a timeout\)\.

__Decision table\.__ T\_admit is the time from ``launch\_reflex\(\)`` to the reflex action's ``execute\(\)`` being entered\. With several owners each is handled by its row, and T\_admit is the maximum\.

| Holder of the reflex action's action\_id or resources | ``preemption`` | Outcome \(``LaunchStatus``\) | T\_admit upper bound |
|---|---|---|---|
| none, or only disjoint resources | any | ``LAUNCHED`` — starts now, concurrently with other actions | one event\-loop turn |
| deliberative or direct action, not inside a mandatory block | any | ``ARMED`` — owner preempted now | ``safe\_stop\_timeout\_s`` \(\+ rollback walk if ``rollback\_on\_preempt``\) |
| deliberative or direct action, inside a mandatory block | ``PREEMPT`` | ``ARMED`` — owner preempted now; the block is cut | ``safe\_stop\_timeout\_s`` \(\+ rollback walk if ``rollback\_on\_preempt``\) |
| deliberative or direct action, inside a mandatory block | ``WAIT\_FOR\_BLOCK`` | ``ARMED`` — the block finishes; the owner stops at the first interruptible boundary after it, whatever its ``InterruptPolicy``, runs ``on\_preempted\(\)`` and releases | R\_block \+ ``safe\_stop\_timeout\_s`` \(\+ rollback walk if ``rollback\_on\_preempt``\), R\_block = owner's ``mandatory\_block\_remaining\_s`` when the reflex was armed |
| a running reflex action \(shared resource\) | any | ``ARMED`` — a reflex never preempts a reflex; waits for it to finish | remaining run of that action, ≤ \(``max\_retries`` \+ 1\) × ``timeout\_s`` |
| the same ``action\_id`` already running or armed as a reflex | any | ``DEDUPED`` — already being handled | — |
| no ActionNode with that ``action\_id`` | any | ``NOT\_FOUND`` — logged at WARNING | — |

In the table, the ``safe\_stop\_timeout\_s`` term grows by 0\.1 s for a hook that overruns \(the abandon grace\), and a rollback walk is bounded by the sum of the ``timeout\_s`` of the steps it undoes\. ``BaseActionNode\.safe\_stop\_bound\_s\(\)`` returns this bound for an action; shutdown and ``emergency\_stop\(\)`` wait for the largest bound of the running actions \(\+ 0\.5 s\)\.

A deliberative proposal is never admitted this way: it is dispatched, interrupts lower\-priority deliberative owners \(a request to an owner in a block is latched until the block ends\), or is held \(§17\.6\)\. Reflex dispatches \(``origin='reflex'``\) bypass ActionGuards; deliberative ones never do, whatever their priority\.

__Enforced by the framework\.__

- Actuator exclusivity for declared resources; armed reflexes' claims block deliberative launches\.
- Deliberative atomicity of mandatory blocks \(§17\.5\)\.
- Preemption, arming, reflex\-over\-reflex waiting and de\-duplication as in the table\.
- The bound on ``on\_preempted\(\)`` \(``safe\_stop\_timeout\_s``, then 0\.1 s before an overrunning hook is abandoned\), the per\-callable bound on rollbacks \(the step's ``timeout\_s``\), and the ``reflex\_eval\_timeout\_s`` bound on event\-lane evaluation\.
- No retry after a run that was interrupted, was preempted, ended with a ``RollbackError``, had its rollback walk cut short, was cut inside a mandatory block by the action's ``timeout\_s``, or had a step abort inside a mandatory block \(step timeout, raise, ``abort\_sequence``\) that no completed rollback walk undid; nor while a cancel, a reflex preemption or an interrupt is pending on the node, or a reflex is armed on any of its resources\.

__Delegated to the application\.__

- Declaring ``resources`` completely\. Actuator access that is not declared, or that happens outside an ActionNode, is invisible to the framework\.
- Cancellation\-safe step code, and an ``on\_preempted\(\)`` that brings the hardware to a safe hold within ``safe\_stop\_timeout\_s`` and copes with a command still running in ``asyncio\.to\_thread\(\)``\.
- Choosing ``PREEMPT`` or ``WAIT\_FOR\_BLOCK`` per reflex, and ``rollback\_on\_preempt`` per action\.
- Bounding ``read\(\)`` and reflex ``evaluate\(\)``: the framework has no timeout on them \(event\-lane evaluation excepted\)\. Rollback callables are timed out at their step's ``timeout\_s``, which ends one only if it lets the cancel through\.
- Co\-locating a reflex with its action and with the sense nodes that feed it \(§10\.4\)\.
- Certified or hard real\-time safety functions, which belong in a separate safety controller \(process or MCU\)\.

__Timing: bound vs\. measurement\.__ From stimulus to reflex action, T\_reflex = T\_detect \+ T\_eval \+ T\_admit, with upper bounds:

- T\_detect, until a signal carrying the stimulus is published\. Polled sensor \(either mode\): ≤ max\(1/tick\_rate\_hz, poll\_interval\_s\) \+ the slowest ``read\(\)`` of the sense stage \+ any overrun of the tick in progress\. Pushed signal \(``reflex\_mode='event'``, ``push\(\)``\): ≈ 0, the lane runs inside ``push\(\)``\.
- T\_eval\. ``'tick'`` mode: context update \+ reflex ``evaluate\(\)`` \(no framework timeout\)\. Event lane: ≤ ``reflex\_eval\_timeout\_s``\.
- T\_admit: the decision table\.

Summing these bounds gives a **deterministic worst case derived from declared timeouts** — under four assumptions: \(A1\) no node code blocks the event loop, since any synchronous segment delays every tick, lane and safe\-stop by its length; \(A2\) the stimulus persists for at least one sampling period, or is pushed — a shorter, unpushed stimulus can be missed; \(A3\) ``read\(\)``, reflex ``evaluate\(\)``, hooks and rollback callables are bounded and reach an ``await`` so their timeouts can take effect; \(A4\) GC pauses, OS scheduling, GIL contention from worker threads and transport latency are not modelled\. These bounds are not hard real\-time guarantees\. The latencies reported by the benchmarks \(§22\.3\) are **empirical** statistics of one host and load: their P99 includes the effects excluded by A4, and is not a bound\.

