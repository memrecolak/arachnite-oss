"""
arachnite.runtime
~~~~~~~~~~~~~~~~~
ArachniteRuntime: the main orchestrator and tick loop.
Spec reference: Section 7.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import time
from collections import deque
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from arachnite.bus import SignalBus
from arachnite.context import ContextNode
from arachnite.exceptions import (
    MandatoryBlockViolation,
    ReflexConflictError,
)
from arachnite.health import HealthMonitor
from arachnite.logging import BaseLogSink, LogLevel, StdoutLogSink, StructuredLogger
from arachnite.models import (
    ActionExecutionState,
    Context,
    DecisionEvent,
    NodeState,
    Permission,
    Proposal,
    ReflexPreemption,
    Result,
    ShutdownPhase,
    Signal,
)
from arachnite.nodes.action import (
    ActionMasterNode,
    BaseActionNode,
    LaunchStatus,
    MultiStepActionNode,
)
from arachnite.nodes.base import BaseNode
from arachnite.nodes.decision import DecisionMasterNode
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.safety_monitor import MonitorState
from arachnite.shutdown import ShutdownCoordinator
from arachnite.supervisor import NodeSupervisor

# Token of the runtime work (a tick, start(), the shutdown) whose code runs
# in this context. Child tasks inherit it, so it is checked against the
# runtime's live tokens: a task that outlives that work is no longer inside.
_RUNTIME_CONTEXT: contextvars.ContextVar[object | None] = contextvars.ContextVar(
    "arachnite_runtime_context", default=None,
)

# Event-lane launch outcomes kept for monitor_state() between ticks (the lane
# stays live while paused, when no tick drains them).
_MAX_PENDING_OUTCOMES = 1024

# ── Tick-stage instrumentation protocol ──────────────────────────────────────
#
# Tick-stage instrumentation ADR (2026-04-16; numbered 0002 in the project's
# original ADR series — not docs/decisions/0002 in this repository): opt-in
# per-stage timing hook for the tick loop.
# Default runtime path pays a single ``is not None`` branch per stage
# boundary; when an instrumenter is attached it receives one
# ``on_stage(name, duration_s)`` call per stage per tick plus one
# ``on_tick_complete(tick_index, total_s)`` call per tick.
#
# The vocabulary of stage names is fixed and published as
# ``TICK_STAGE_NAMES`` so external tooling (benchmarks, OpenTelemetry
# adapters, Prometheus sinks) can rely on a stable set of identifiers.
# Additions require a new ADR (see the tick-stage instrumentation ADR §Consequences).

TICK_STAGE_NAMES: tuple[str, ...] = (
    "sense",
    "context",
    "reflex",
    "instinct",
    "decide",
    "act",
)


@runtime_checkable
class TickInstrumenter(Protocol):
    """Optional per-stage timing callback installed on ArachniteRuntime.

    The runtime calls ``on_stage()`` once per stage per tick with the
    wall-clock duration of that stage (in seconds), and
    ``on_tick_complete()`` once per tick with the total tick duration.

    Implementations must be non-blocking; do not perform I/O in these
    callbacks — they run synchronously inside ``tick()``. Exceptions
    raised by the instrumenter are caught by the runtime and logged at
    WARNING level; they never propagate out of ``tick()`` so a misbehaving
    instrumenter cannot crash the tick loop.

    Stage names are drawn from ``TICK_STAGE_NAMES`` and cover the six
    pipeline stages: ``sense``, ``context``, ``reflex``, ``instinct``,
    ``decide``, ``act``. See the tick-stage instrumentation ADR for the mapping to ``tick()``
    source lines.
    """

    def on_stage(self, stage: str, duration_s: float) -> None: ...
    def on_tick_complete(self, tick_index: int, total_s: float) -> None: ...


class ArachniteRuntime:
    """
    Wires all master nodes together and runs the tick loop.

    The tick sequence each cycle:
    0. Drain Results of actions that finished since the previous tick
    1. SenseMasterNode.read_all()         → signals
    1b. Inject buffered supervisor + node_fault signals captured since the
        previous tick (a node raising during tick N surfaces here on tick N+1)
    2. ContextNode.update(signals, results) → context
    3. InstinctMasterNode.evaluate_reflexes(ctx) → reflex proposals
       └─ launch_reflex() each (bypass decision; preempt per ReflexPreemption)
    4. InstinctMasterNode.evaluate_all(ctx) → normal proposals
    5. DecisionMasterNode.on_new_proposals_many() → proposals + interrupts
    6. ActionMasterNode.launch(chosen)    → tasks (blocked ones parked)
    7. Tick-end hooks

    Actions run as tasks across ticks — the tick never awaits them — so
    sensing and reflexes continue while a long action runs (ADR 0005).
    Their Results reach ``ctx.last_results`` on the tick after they finish.
    At most one running action owns a declared resource; the same
    ActionNode never runs twice concurrently. With ``reflex_mode="event"``
    reflexes that declare ``trigger_on_signals`` are evaluated on signal
    arrival instead of in step 3.

    Spec reference: Section 7.
    """

    def __init__(
        self,
        sense_master:    SenseMasterNode,
        context:         ContextNode,
        instinct_master: InstinctMasterNode,
        decision_master: DecisionMasterNode,
        action_master:   ActionMasterNode,
        bus:             SignalBus,
        tick_rate_hz:    float = 10.0,
        log_sinks:       list[BaseLogSink] | None = None,
        overrun_warn_pct: float = 0.2,
        overrun_warn_consecutive: int = 3,
        shutdown_coordinator: ShutdownCoordinator | None = None,
        allowed_permissions: dict[str, set[Permission]] | None = None,
        tick_instrumenter: TickInstrumenter | None = None,
        context_observers: list[Callable[[Context], None]] | None = None,
        decision_observers: list[Callable[[DecisionEvent], None]] | None = None,
        reflex_mode: str = "tick",
        reflex_signal_ttl_s: float | None = None,
        reflex_eval_timeout_s: float = 0.05,
    ) -> None:
        if reflex_mode not in ("tick", "event"):
            raise ValueError(
                f"reflex_mode must be 'tick' or 'event', got {reflex_mode!r}"
            )
        self._sense_master    = sense_master
        self._context         = context
        self._instinct_master = instinct_master
        self._decision_master = decision_master
        self._action_master   = action_master
        self._bus             = bus
        self._tick_rate_hz    = tick_rate_hz
        self._interval        = 1.0 / tick_rate_hz
        self._overrun_warn    = overrun_warn_pct
        # C2 (audit 2026-04-16): suppress per-tick overrun warning spam by
        # only logging once at least ``overrun_warn_consecutive`` overruns
        # have occurred back-to-back. Matches the threshold used by
        # ``TickBudgetMonitor`` (default 3) so the two layers agree on what
        # constitutes a meaningful overrun. Set to 1 to restore the legacy
        # warn-on-every-overrun behaviour.
        self._overrun_warn_consecutive = max(1, overrun_warn_consecutive)
        self._consecutive_overruns = 0
        self._log_sinks       = log_sinks or [StdoutLogSink(level=LogLevel.WARNING)]
        self._logger = StructuredLogger(
            node_id       = "ArachniteRuntime",
            sinks         = self._log_sinks,
        )

        self._tick_count:   int           = 0
        self._running:      bool          = False
        self._paused:       bool          = False
        self._last_result:  Result | None = None
        self._last_results: list[Result]  = []
        self._shutdown_phase = ShutdownPhase.NOT_STARTED
        self._loop_task: asyncio.Task[None] | None = None
        # The one task doing the shutdown work (graceful or emergency);
        # stop(), emergency_stop() and wait() all wait for it.
        self._shutdown_task: asyncio.Task[None] | None = None
        self._shutdown_kind: str | None = None  # "graceful" | "emergency"
        # True once the runtime itself cancels the loop task (shutdown), so
        # the loop can tell that from a stray CancelledError out of a node.
        self._loop_cancel_requested = False
        # Live runtime-work tokens (see _RUNTIME_CONTEXT), and whether the
        # background loop (not a manual tick() call) is running a tick.
        self._live_contexts: set[object] = set()
        self._loop_ticking = False
        # Set while start() runs; a stop meanwhile sets _abort_start.
        self._starting: asyncio.Event | None = None
        self._abort_start = False
        # True from stop()/emergency_stop() until the next start(): the tick
        # in flight launches nothing deliberative.
        self._stopping = False
        # Bumped by every start(); an event-lane evaluation from an earlier
        # run launches nothing.
        self._generation = 0
        self._pause_hooks_applied = False
        self._pause_hooks_lock = asyncio.Lock()
        self._loop_tick_idle = asyncio.Event()
        self._loop_tick_idle.set()
        self._teardown_timeout_s: float = 5.0
        self._shutdown_coordinator = shutdown_coordinator or ShutdownCoordinator(
            teardown_timeout_s=self._teardown_timeout_s,
        )

        # Supervisors (one per master node)
        self._supervisors = [
            NodeSupervisor(bus, supervisor_id=f"supervisor_{i}")
            for i in range(4)
        ]
        self._health = HealthMonitor(self._supervisors)

        # Register all leaf nodes with supervisors
        for sn in sense_master.nodes:
            self._supervisors[0].track(sn)
        for inn in instinct_master.normal_nodes:
            self._supervisors[1].track(inn)
        for rn in instinct_master.reflex_nodes:
            self._supervisors[1].track(rn)
        for an in action_master.nodes:
            self._supervisors[3].track(an)

        # Wire each master's fault sink to its supervisor's on_fault. This is
        # what makes a leaf node raising during a tick actually transition the
        # supervisor (→ FAULTED, restart policy applied) and emit the
        # SupervisorSignal + NodeFaultSignal the buffered-signal injection
        # surfaces in ctx.signals. Without this, master nodes swallow the
        # exception and the documented node_fault path never fires.
        # (supervisors[2] is reserved for the decision master, which has no
        #  fault sink — decision exceptions are handled inline.)
        self._sense_master.set_fault_handler(self._supervisors[0].on_fault)
        self._instinct_master.set_fault_handler(self._supervisors[1].on_fault)
        self._action_master.set_fault_handler(self._supervisors[3].on_fault)

        self._stop_event = asyncio.Event()

        # Buffer for supervisor signals emitted between ticks.
        # The runtime subscribes to BOTH 'supervisor' (lifecycle transitions:
        # RUNNING/STOPPED/RESTARTING) and 'node_fault' (the typed NodeFaultSignal
        # carrying error_type/error_message for FAULTED/DEAD) on the bus, and
        # drains this buffer at the start of each tick so instincts (including
        # reflexes) can observe node faults via ctx.signals. A single fault
        # therefore surfaces as two signals next tick — one kind='supervisor'
        # and one kind='node_fault' — matching the documented contract.
        self._allowed_permissions = allowed_permissions
        self._supervisor_signal_buffer: list[Signal] = []
        self._bus.subscribe("supervisor", self._on_supervisor_signal)
        self._bus.subscribe("node_fault", self._on_supervisor_signal)

        # Buffer for StateUpdateSignals published onto the bus by non-sense
        # nodes (instincts, actions, reflexes). Sense nodes inject their
        # StateUpdateSignals via read_all(), so those are applied in the SAME
        # tick; bus-published ones cannot be — instincts/actions run *after*
        # ContextNode.update() — so they are drained into the signals list at
        # the start of the NEXT tick (N+1), identical to the node_fault path
        # above. See StateUpdateSignal in models.py for the timing contract.
        # read_all() also publishes its signals for bus subscribers, so the
        # callback below filters that echo out by identity — otherwise every
        # sense-written state key would lag one tick.
        self._state_update_buffer: list[Signal] = []
        self._bus.subscribe("state_update", self._on_state_update_signal)

        # Optional per-stage timing callback (the tick-stage instrumentation ADR). When ``None``
        # the tick path short-circuits on a single branch per stage.
        self._tick_instrumenter: TickInstrumenter | None = tick_instrumenter

        # Optional per-tick Context observers. Each callable is invoked with
        # the freshly assembled Context after ContextNode.update() each tick.
        # Exceptions in observers are swallowed (and logged) so a faulty
        # observer cannot crash the tick loop.
        self._context_observers: list[Callable[[Context], None]] = (
            list(context_observers) if context_observers else []
        )

        # Optional per-tick DecisionEvent observers (e.g. SignalDashboard).
        # Fired once per tick after the decide stage, after considered /
        # dispatched / interrupts are all known.  Exceptions are isolated.
        self._decision_observers: list[Callable[[DecisionEvent], None]] = (
            list(decision_observers) if decision_observers else []
        )

        # Event-driven reflex lane (ADR 0005). In "event" mode, reflex nodes
        # that declare trigger_on_signals are evaluated the moment a matching
        # signal is published on the bus — by a polled sense node during the
        # sense stage, or by an event-driven one via BaseSenseNode.push()
        # outside any tick — instead of once per tick in the reflex stage.
        self._reflex_mode = reflex_mode
        self._reflex_signal_ttl_s = (
            reflex_signal_ttl_s if reflex_signal_ttl_s is not None else self._interval
        )
        self._reflex_eval_timeout_s = reflex_eval_timeout_s
        self._lane_active = False
        self._lane_kinds: set[str] = set()
        # (source, kind) -> (latest signal, monotonic receipt time)
        self._lane_latest: dict[tuple[str, str], tuple[Signal, float]] = {}

        # Observations for monitor_state(): reflex launch outcomes (tick
        # stage and event lane) since the last completed tick, and facts
        # recorded by the last tick.
        self._reflex_outcomes: deque[LaunchStatus] = deque(maxlen=_MAX_PENDING_OUTCOMES)
        self._monitor_report: dict[str, object] = {}

    # ── Public properties ─────────────────────────────────────────────────────

    @property
    def health(self) -> HealthMonitor:
        """Access the aggregated health view across all supervisors."""
        return self._health

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_paused(self) -> bool:
        return self._paused

    @property
    def context(self) -> ContextNode:
        return self._context

    @property
    def bus(self) -> SignalBus:
        return self._bus

    # ── Context observers ────────────────────────────────────────────────────

    def add_context_observer(
        self, observer: Callable[[Context], None],
    ) -> None:
        """Register a callable invoked with each tick's Context snapshot."""
        if observer not in self._context_observers:
            self._context_observers.append(observer)

    def remove_context_observer(
        self, observer: Callable[[Context], None],
    ) -> None:
        """Unregister a previously added Context observer (no-op if absent)."""
        with contextlib.suppress(ValueError):
            self._context_observers.remove(observer)

    # ── Decision observers ───────────────────────────────────────────────────

    def add_decision_observer(
        self, observer: Callable[[DecisionEvent], None],
    ) -> None:
        """Register a callable invoked with each tick's DecisionEvent."""
        if observer not in self._decision_observers:
            self._decision_observers.append(observer)

    def remove_decision_observer(
        self, observer: Callable[[DecisionEvent], None],
    ) -> None:
        """Unregister a previously added Decision observer (no-op if absent)."""
        with contextlib.suppress(ValueError):
            self._decision_observers.remove(observer)

    # ── Tick instrumentation (tick-stage instrumentation ADR) ────────────────

    def set_tick_instrumenter(
        self, instrumenter: TickInstrumenter | None,
    ) -> None:
        """Install (or clear) the per-stage timing callback.

        Safe to call before or after ``start()``. Passing ``None`` restores
        the zero-overhead default path. See the tick-stage instrumentation ADR for the contract that
        implementations must follow.
        """
        self._tick_instrumenter = instrumenter

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Call setup() on all master nodes, then start the tick loop.

        A concurrent second call waits for the first. ``stop()`` or
        ``emergency_stop()`` during the setups aborts the start: the masters
        already set up are torn down and the loop never starts.
        """
        if self._running:
            return
        if self._starting is not None:
            await self._starting.wait()
            return
        # A restart waits for the previous shutdown to finish completely, so
        # its teardown cannot run after (and undo) this start.
        previous = self._shutdown_task
        if previous is not None and not previous.done():
            await asyncio.wait({previous})
        self._shutdown_task = None
        self._shutdown_kind = None
        self._loop_cancel_requested = False
        self._abort_start = False
        self._starting = asyncio.Event()
        context = self._enter_context()
        try:
            await self._start_body()
        finally:
            self._leave_context(context)
            starting, self._starting = self._starting, None
            starting.set()

    async def _start_body(self) -> None:
        # Collect all leaf nodes for startup validation
        all_nodes: list[BaseNode] = []
        all_nodes.extend(self._sense_master.nodes)
        all_nodes.extend(self._instinct_master.normal_nodes)
        all_nodes.extend(self._instinct_master.reflex_nodes)
        all_nodes.extend(self._action_master.nodes)

        # Permission whitelist check (startup-only, zero runtime cost)
        if self._allowed_permissions:
            from arachnite.distributed.permissions import validate_permissions
            validate_permissions(all_nodes, self._allowed_permissions)

        # Dependency validation (startup-only)
        self._validate_dependencies(all_nodes)

        masters: list[BaseNode] = [
            self._sense_master, self._instinct_master,
            self._decision_master, self._action_master,
        ]
        setup_ok: list[BaseNode] = []
        try:
            for master in masters:
                await master.setup()
                setup_ok.append(master)
                if self._abort_start:
                    break
        except Exception:
            for m in reversed(setup_ok):
                with contextlib.suppress(Exception):
                    await m.teardown()
            raise
        if self._abort_start:
            self._logger.info("Start aborted by a stop request")
            for m in reversed(setup_ok):
                with contextlib.suppress(Exception):
                    await m.teardown()
            return
        # Mark all tracked nodes as running
        for sup in self._supervisors:
            for node_id in sup.all_states():
                await sup.mark_running(node_id)

        self._paused = False
        self._pause_hooks_applied = False
        self._stopping = False
        self._generation += 1
        self._running = True
        self._stop_event.clear()
        if self._reflex_mode == "event":
            self._start_reflex_lane()
        self._loop_task = asyncio.create_task(self._loop())

    def _abort_starting(self) -> bool:
        """A stop during start(): make start() abort. True if one was starting."""
        if self._starting is None:
            return False
        self._abort_start = True
        self._stop_event.set()
        self._action_master.close()
        return True

    async def stop(self) -> None:
        """
        Gracefully stop the loop and teardown all nodes.
        Delegates to ShutdownCoordinator which runs all 7 phases in order.
        The shutdown runs in its own task, so it completes even if the caller
        is cancelled. A second call (or one during an emergency stop) waits
        for the shutdown in progress. Called from inside the tick, a running
        action or the shutdown itself, it starts the shutdown and returns at
        once — waiting there would wait for the caller itself; ``await
        wait()`` elsewhere waits for completion. During ``start()`` it aborts
        the start.
        Spec reference: Section 15.1, 15.2.
        """
        if self._abort_starting():
            if not self._inside_runtime_work():
                await self._wait_start()
            return
        if self._shutdown_task is None:
            if not self._running:
                return
            self._running = False
            self._stopping = True
            self._stop_event.set()
            self._stop_reflex_lane()
            self._shutdown_kind = "graceful"
            self._shutdown_task = asyncio.get_running_loop().create_task(
                self._graceful_shutdown(), name="arachnite-stop",
            )
        await self._await_shutdown()

    async def _wait_start(self) -> None:
        starting = self._starting
        if starting is not None:
            await starting.wait()

    async def _graceful_shutdown(self) -> None:
        context = self._enter_context()
        try:
            await self._shutdown_coordinator.execute(self)
            self._shutdown_phase = self._shutdown_coordinator.phase
        finally:
            self._leave_context(context)

    async def emergency_stop(self) -> None:
        """
        Immediate shutdown — skips phases 1-3, abandons mandatory blocks.

        Closes the action master at once (nothing new starts), stops the tick
        loop, then cancels every running action: each one's
        ``on_preempted()`` safe-stop hook runs (bounded), no rollback; a
        safe-stop already running is waited for. Pending supervisor restarts
        are cancelled before the teardown. This is a shutdown path; cutting
        into a mandatory block to *respond* to a hazard is the job of a
        PREEMPT reflex (ADR 0005).

        The shutdown runs in its own task. During a graceful ``stop()`` it
        takes over (unless that stop is already tearing nodes down); a second
        call waits for the first; after a completed stop it does nothing;
        during ``start()`` it aborts the start. Called from inside the tick,
        a running action (its safe-stop included) or the shutdown itself, it
        returns once the shutdown has started; anywhere else it returns when
        the shutdown is complete.
        Spec reference: Section 15.3.
        """
        if self._abort_starting():
            self._logger.info("Emergency stop during start: start aborted")
            if not self._inside_runtime_work():
                await self._wait_start()
            return
        current = self._shutdown_task
        if current is not None and (current.done() or self._shutdown_kind == "emergency"):
            await self._await_shutdown()
            return
        if current is None and not self._running:
            return  # never started, or already stopped
        self._running = False
        self._stopping = True
        self._stop_event.set()
        self._stop_reflex_lane()
        self._action_master.close()
        self._logger.info("Emergency stop initiated")
        self._shutdown_kind = "emergency"
        self._shutdown_task = asyncio.get_running_loop().create_task(
            self._finish_emergency_stop(current), name="arachnite-emergency-stop",
        )
        await self._await_shutdown()

    async def _finish_emergency_stop(self, graceful: asyncio.Task[None] | None) -> None:
        """Take over from a graceful stop, cancel the loop, every action, then teardown."""
        context = self._enter_context()
        try:
            await self._emergency_body(graceful)
        finally:
            self._leave_context(context)

    async def _emergency_body(self, graceful: asyncio.Task[None] | None) -> None:
        if graceful is not None and not graceful.done():
            if self._shutdown_coordinator.phase in (
                ShutdownPhase.TEARDOWN_NODES,
                ShutdownPhase.DISCONNECT_TRANSPORT,
                ShutdownPhase.COMPLETE,
            ):
                # Already tearing nodes down: let it finish, never twice.
                await asyncio.wait({graceful})
                return
            graceful.cancel()
            await asyncio.wait({graceful})
        # Cancel and await the tick loop BEFORE stopping actions and tearing
        # masters down, so no tick can launch anything new or race node
        # teardown.
        loop_task = self._loop_task
        if loop_task is not None and not loop_task.done():
            self._loop_cancel_requested = True
            loop_task.cancel()
            await asyncio.wait({loop_task})
        self._loop_task = None
        await self._action_master.cancel_all(
            timeout=self._action_master.safe_stop_budget_s(),
        )
        # A pending restart would tear a node down and set it up again
        # after the stop (spec 15.1: cancelled before teardown).
        for sup in self._supervisors:
            await sup.cancel_restart_tasks()
            for node_id in sup.all_states():
                await sup.mark_stopped(node_id)
        await self._teardown_masters()

    async def _teardown_masters(self) -> None:
        """Tear every master down; a failing one is logged, the rest still run."""
        outcomes = await asyncio.gather(
            self._sense_master.teardown(),
            self._instinct_master.teardown(),
            self._decision_master.teardown(),
            self._action_master.teardown(),
            return_exceptions=True,
        )
        errors = [o for o in outcomes if isinstance(o, BaseException)]
        for err in errors:
            self._logger.error(
                "Teardown raised",
                error=str(err),
                error_type=type(err).__name__,
            )
        if errors:
            raise errors[0]

    def _enter_context(self) -> object:
        """Mark this task (and tasks it starts) as runtime work: tick, start or shutdown."""
        token = object()
        self._live_contexts.add(token)
        _RUNTIME_CONTEXT.set(token)
        return token

    def _leave_context(self, token: object) -> None:
        self._live_contexts.discard(token)

    def _inside_runtime_work(self) -> bool:
        """
        True in code the runtime is running or waiting for: the tick in
        flight, start(), the shutdown, a running action (its safe-stop
        included), or a task one of them started while it lasts.
        """
        token = _RUNTIME_CONTEXT.get()
        if token is not None and token in self._live_contexts:
            return True
        current = asyncio.current_task()
        return current is not None and (
            current is self._loop_task or self._action_master.in_action_context()
        )

    async def _await_shutdown(self) -> None:
        """Wait for the shutdown in progress (following a take-over) to finish."""
        if self._inside_runtime_work():
            return
        task = self._shutdown_task
        while task is not None:
            await asyncio.wait({task})
            if self._shutdown_task is task:
                break
            task = self._shutdown_task
        if task is not None and not task.cancelled():
            exc = task.exception()
            if exc is not None:
                raise exc

    async def wait(self) -> None:
        """Block until stop() or emergency_stop() is called and has finished."""
        await self._stop_event.wait()
        await self._await_shutdown()

    # ── Live node registration ─────────────────────────────────────────────────

    async def register_sense_live(self, node: BaseSenseNode) -> None:
        """
        Register a SenseNode after the runtime has started.

        Calls setup() on the node immediately (if the runtime is running) and
        begins tracking it with the sense supervisor.  Safe to call before
        start() as well — setup() will be called during the normal start().

        This is the primary hook for Phase 1 self-assembly: DynamicLoaderAction
        generates a new SenseNode and registers it without restarting the agent.
        """
        if not self._running:
            self._sense_master.register(node)
            return
        # Set up before registering, so no tick reads or launches a node
        # whose setup() has not finished.
        if self._sense_master.get_node(node.node_id) is not None:
            self._sense_master.register(node)  # raises NodeRegistrationError
        await node.setup()
        self._sense_master.register(node)
        self._supervisors[0].track(node)
        await self._supervisors[0].mark_running(node.node_id)

    async def register_instinct_live(self, node: BaseInstinctNode) -> None:
        """
        Register an InstinctNode after the runtime has started.

        Calls setup() on the node immediately (if the runtime is running) and
        begins tracking it with the instinct supervisor.
        """
        if not self._running:
            self._instinct_master.register(node)
            return
        # Set up before registering, so no tick reads or launches a node
        # whose setup() has not finished.
        if self._instinct_master.get_node(node.node_id) is not None:
            self._instinct_master.register(node)  # raises NodeRegistrationError
        await node.setup()
        self._instinct_master.register(node)
        self._supervisors[1].track(node)
        await self._supervisors[1].mark_running(node.node_id)
        if self._lane_active:
            self._subscribe_lane_kinds()

    async def register_action_live(self, node: BaseActionNode) -> None:
        """
        Register an ActionNode after the runtime has started.

        Calls setup() on the node immediately (if the runtime is running) and
        begins tracking it with the action supervisor.
        """
        if not self._running:
            self._action_master.register(node)
            return
        # Set up before registering, so no tick reads or launches a node
        # whose setup() has not finished.
        if self._action_master.get_node(node.node_id) is not None:
            self._action_master.register(node)  # raises NodeRegistrationError
        await node.setup()
        self._action_master.register(node)
        self._supervisors[3].track(node)
        await self._supervisors[3].mark_running(node.node_id)

    # ── Live node unregistration ──────────────────────────────────────────────

    async def unregister_sense_live(self, node_id: str) -> None:
        """
        Unregister a SenseNode while the runtime is running.

        Calls teardown() on the node, removes it from the sense master,
        and untracks it from the supervisor.  Safe to call when stopped.

        This is the counterpart of register_sense_live() — used by the
        Phase 5 repair cycle to remove a dead node before registering
        its replacement.
        """
        node = self._sense_master.get_node(node_id)
        if node is not None:
            with contextlib.suppress(Exception):
                await node.teardown()
        self._sense_master.unregister(node_id)
        self._supervisors[0].untrack(node_id)

    async def unregister_instinct_live(self, node_id: str) -> None:
        """
        Unregister an InstinctNode while the runtime is running.

        Calls teardown() on the node, removes it from the instinct master,
        and untracks it from the supervisor.
        """
        node = self._instinct_master.get_node(node_id)
        if node is not None:
            with contextlib.suppress(Exception):
                await node.teardown()
        self._instinct_master.unregister(node_id)
        self._decision_master.clear_pending(node_id)
        self._supervisors[1].untrack(node_id)

    async def unregister_action_live(self, node_id: str) -> None:
        """
        Unregister an ActionNode while the runtime is running.

        Calls teardown() on the node, removes it from the action master,
        and untracks it from the supervisor.
        """
        node = self._action_master.get_node(node_id)
        # Unregister first: from here on no tick can launch it again.
        self._action_master.unregister(node_id)
        if node is not None:
            # First call withdraws a reflex armed under node_id, the second
            # cancels its running run (each is a no-op if there is none).
            for _ in range(2):
                await self._action_master.cancel(
                    node_id, timeout=node.safe_stop_timeout_s + 0.5,
                )
            with contextlib.suppress(Exception):
                await node.teardown()
        self._supervisors[3].untrack(node_id)

    # ── Pause / Resume ────────────────────────────────────────────────────────

    async def pause(self) -> None:
        """
        Pause sensing and tick loop. Supervisors keep running.

        A tick the loop is running finishes first (reflexes still launch;
        its deliberative launches are dropped), then the nodes get
        ``on_pause()``. The event-driven reflex lane stays live.
        """
        if self._paused:
            return
        self._paused = True
        if not self._inside_runtime_work():
            await self._loop_tick_idle.wait()
        async with self._pause_hooks_lock:
            if not self._paused or self._pause_hooks_applied:
                return  # resumed meanwhile
            self._pause_hooks_applied = True
            await asyncio.gather(
                self._sense_master.on_pause(),
                self._instinct_master.on_pause(),
                self._action_master.on_pause(),
            )

    async def resume(self) -> None:
        """Resume from paused state."""
        if not self._paused:
            return
        self._paused = False
        async with self._pause_hooks_lock:
            if self._paused or not self._pause_hooks_applied:
                return  # on_pause() never ran, or paused again meanwhile
            self._pause_hooks_applied = False
            await asyncio.gather(
                self._sense_master.on_resume(),
                self._instinct_master.on_resume(),
                self._action_master.on_resume(),
            )

    # ── Dependency validation ─────────────────────────────────────────────────

    @staticmethod
    def _validate_dependencies(all_nodes: list[BaseNode]) -> None:
        """Check that every node's ``requires`` list is satisfied."""
        registered_ids = {n.node_id for n in all_nodes}
        errors: list[str] = []
        for node in all_nodes:
            for dep in node.requires:
                if dep not in registered_ids:
                    errors.append(
                        f"Node '{node.node_id}' requires '{dep}' but it is not registered"
                    )
        if errors:
            from arachnite.exceptions import DependencyValidationError
            raise DependencyValidationError(errors)

    # ── Supervisor signal forwarding ─────────────────────────────────────────

    async def _on_supervisor_signal(self, signal: Signal) -> None:
        """
        Buffer supervisor and node_fault signals for injection into the next
        tick. Subscribed to both the 'supervisor' and 'node_fault' bus kinds,
        so a node fault is surfaced in ctx.signals as both a SupervisorSignal
        and a typed NodeFaultSignal.
        """
        self._supervisor_signal_buffer.append(signal)

    async def _on_state_update_signal(self, signal: Signal) -> None:
        """
        Buffer a bus-published StateUpdateSignal for application at the start of
        the next tick. ContextNode.update() intercepts it from the drained
        signals list and writes ctx.state, exactly as it does for a
        StateUpdateSignal returned by a sense node. Because instincts and
        actions run after the ContextNode update phase, a write they publish is
        applied at N+1 (the following tick), not the tick it was published.

        Sense nodes also reach this callback — read_all() publishes everything
        it returns — but their signals have *already* been applied this tick
        via the returned signals list, so buffering them too would re-apply a
        stale copy at the start of the next tick, on top of (and after) that
        tick's fresh sense reading. That echo is filtered out here by object
        identity, which leaves every other route — instinct/action publishes,
        signals arriving from a transport — buffered exactly as before.
        """
        if self._sense_master.returned_this_tick(signal):
            return
        self._state_update_buffer.append(signal)

    # ── Reflex arbitration ────────────────────────────────────────────────────

    def _tick_reflex_nodes(self) -> list[BaseReflexInstinctNode]:
        """
        Reflex nodes evaluated by the tick's reflex stage: all of them in
        "tick" mode; in "event" mode only those without trigger_on_signals
        (the rest belong to the event-driven lane).
        """
        nodes = list(self._instinct_master.reflex_nodes)
        if self._reflex_mode == "event":
            return [n for n in nodes if not n.trigger_on_signals]
        return nodes

    # ── Event-driven reflex lane ──────────────────────────────────────────────

    def _lane_nodes(self) -> list[BaseReflexInstinctNode]:
        return [n for n in self._instinct_master.reflex_nodes if n.trigger_on_signals]

    def _subscribe_lane_kinds(self) -> None:
        for node in self._lane_nodes():
            for kind in node.trigger_on_signals or ():
                if kind not in self._lane_kinds:
                    self._lane_kinds.add(kind)
                    self._bus.subscribe(kind, self._on_reflex_signal)

    def _start_reflex_lane(self) -> None:
        self._lane_active = True
        self._subscribe_lane_kinds()

    def _stop_reflex_lane(self) -> None:
        self._lane_active = False
        for kind in self._lane_kinds:
            self._bus.unsubscribe(kind, self._on_reflex_signal)
        self._lane_kinds.clear()
        self._lane_latest.clear()

    async def _on_reflex_signal(self, signal: Signal) -> None:
        """
        Bus callback of the event-driven reflex lane: evaluate the reflexes
        triggered by ``signal`` now and launch what fires. The reflex sees
        the freshest signal per (source, kind) of its trigger kinds received
        within ``reflex_signal_ttl_s``. Never raises into the publisher.

        The lane stays live while the runtime is paused: pausing stops
        polled sensing and deliberation, but a pushed hazard signal still
        gets its safety reflex.
        """
        if not self._lane_active or not self._running:
            return
        generation = self._generation
        now = time.perf_counter()
        self._lane_latest[(signal.source, signal.kind)] = (signal, now)
        nodes = [
            n for n in self._lane_nodes()
            if n.enabled and signal.kind in (n.trigger_on_signals or ())
        ]
        if not nodes:
            return
        kinds: set[str] = set()
        for node in nodes:
            kinds.update(node.trigger_on_signals or ())
        fresh = [
            sig for (_, kind), (sig, received) in self._lane_latest.items()
            if kind in kinds and now - received <= self._reflex_signal_ttl_s
        ]
        ctx = self._context.reflex_view(fresh)
        try:
            proposals = await asyncio.wait_for(
                self._instinct_master.evaluate_reflexes(ctx, nodes=nodes),
                timeout=self._reflex_eval_timeout_s,
            )
        except ReflexConflictError as exc:
            self._logger.error(
                "Reflex priority conflict; dispatching all in priority order",
                priority=exc.priority,
                instinct_ids=exc.node_ids,
            )
            proposals = list(exc.proposals)
        except asyncio.TimeoutError:
            self._logger.error(
                "Event-lane reflex evaluation timed out",
                signal_kind=signal.kind,
                timeout_s=self._reflex_eval_timeout_s,
            )
            return
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "Event-lane reflex evaluation failed",
                signal_kind=signal.kind,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return
        if not self._lane_active or not self._running or generation != self._generation:
            return  # a stop (and maybe a restart) happened during the evaluation
        await self._launch_reflexes(proposals)

    async def _launch_reflexes(self, reflex_proposals: list[Proposal]) -> None:
        """
        Launch reflex proposals (priority order) through the action master's
        arbitration, then yield a few loop turns so a started reflex action
        enters ``execute()``, and a preempted owner starts its safe-stop
        (which runs in a shielded inner task), now rather than after the rest
        of the tick.

        On Python < 3.12 these turns are not always enough: the reflex action's
        first slice (and a preempted owner's safe-stop) may run after the
        reflex stage instead. The reflex is still launched, and its resources
        claimed, before deliberation; only the stage it is charged to differs.
        """
        started = False
        for rp in reflex_proposals:
            # Emit a framework-level "Reflex fired" event so safety auditors
            # can reconstruct every reflex-arc activation from the log stream
            # alone (spec §13.3). This is INFO-level and low-volume because
            # reflex activations are rare by design.
            self._logger.info(
                "Reflex fired",
                instinct_id=rp.instinct_id,
                action_id=rp.action_id,
                priority=rp.priority,
                urgency=rp.urgency,
            )
            node = self._instinct_master.get_node(rp.instinct_id)
            preemption = (
                node.preemption
                if isinstance(node, BaseReflexInstinctNode)
                else ReflexPreemption.PREEMPT
            )
            outcome = self._action_master.launch_reflex(rp, preemption)
            self._reflex_outcomes.append(outcome.status)
            if outcome.status is LaunchStatus.NOT_FOUND:
                self._logger.warning(
                    "Reflex dispatch skipped: action not found",
                    action_id=rp.action_id,
                    instinct_id=rp.instinct_id,
                )
            elif outcome.status is LaunchStatus.ARMED:
                started = True
                self._logger.info(
                    "Reflex armed; taking resources from running action(s)",
                    action_id=rp.action_id,
                    owners=sorted(outcome.owners),
                    preemption=preemption.value,
                )
            elif outcome.status is LaunchStatus.LAUNCHED:
                started = True
            elif outcome.status is LaunchStatus.CLOSED:
                self._logger.debug(
                    "Reflex dropped: action master closed (shutting down)",
                    action_id=rp.action_id,
                    instinct_id=rp.instinct_id,
                )
            else:
                self._logger.debug(
                    "Reflex already being handled",
                    action_id=rp.action_id,
                    instinct_id=rp.instinct_id,
                )
        if started:
            # One turn starts the reflex / owner task; the next lets a
            # shielded safe-stop's inner task run its first slice.
            for _ in range(3):
                await asyncio.sleep(0)

    async def wait_idle(self, timeout: float | None = None) -> bool:
        """
        Wait until no action is running or waiting for resources (actions run
        as tasks across ticks). Returns False on timeout. Their Results reach
        ``ctx.last_results`` on the next tick.
        """
        return await self._action_master.wait_idle(timeout)

    # ── Runtime safety monitoring ────────────────────────────────────────────

    def monitor_state(self) -> MonitorState:
        """
        MonitorState for ``SafetyMonitorRegistry.check_all()``, built from what
        the runtime observed in the last tick: reflex launch outcomes (event
        lane included), whether a reflex proposal reached the decision layer,
        whether a completed action reported a deliberative stop inside a
        mandatory block, blocks in progress, resource ownership, reflex-node
        health and the tick's duration.
        """
        report = self._monitor_report
        raw_outcomes = report.get("reflex_outcomes", [])
        outcomes = raw_outcomes if isinstance(raw_outcomes, list) else []
        owners: dict[str, int] = {}
        for node in self._action_master.current_actions().values():
            for resource in node.resources:
                owners[resource] = owners.get(resource, 0) + 1
        reflex_nodes = self._instinct_master.reflex_nodes
        reflex_states = self._supervisors[1].all_states()
        unavailable = (NodeState.FAULTED, NodeState.RESTARTING, NodeState.DEAD)
        active = sum(
            1 for n in reflex_nodes if reflex_states.get(n.node_id) not in unavailable
        )
        duration = report.get("tick_duration_ms", 0.0)
        return MonitorState(
            tick                            = self._tick_count,
            reflex_fired                    = bool(outcomes),
            reflex_action_dispatched        = all(
                status is not LaunchStatus.NOT_FOUND for status in outcomes
            ),
            reflex_routed_through_decision  = bool(report.get("reflex_routed", False)),
            mandatory_block_active          = not self._action_master.is_interruptible(),
            interrupt_accepted_during_block = bool(report.get("interior_interrupt", False)),
            resources_with_multiple_owners  = sum(1 for n in owners.values() if n > 1),
            active_reflex_nodes             = active,
            total_reflex_nodes              = len(reflex_nodes),
            tick_duration_ms                = float(duration) if isinstance(
                duration, (int, float),
            ) else 0.0,
            tick_budget_ms                  = self._interval * 1000.0,
        )

    def _stopped_inside_block(self, result: Result) -> bool:
        """True if a non-preempted interrupt stopped the action inside a block."""
        if not result.interrupted or result.preempted or result.stopped_at_step is None:
            return False
        node = self._action_master.get_node(result.action_id)
        if not isinstance(node, MultiStepActionNode):
            return False
        steps = node.steps()
        names = [s.name for s in steps]
        if result.stopped_at_step not in names:
            return False
        i = names.index(result.stopped_at_step)
        return i > 0 and not steps[i].interruptible and not steps[i - 1].interruptible

    # ── Tick loop ─────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        while self._running:
            if self._paused:
                await self._sleep_or_stop(self._interval)
                continue

            start = time.monotonic()
            self._loop_ticking = True
            self._loop_tick_idle.clear()
            try:
                await self.tick()
            except asyncio.CancelledError:
                # A CancelledError the runtime did not request (a node awaited
                # something that was cancelled) must not end the loop
                # silently. Python 3.10 cannot tell it from a cancel of this
                # task, so there it still ends the loop.
                current = asyncio.current_task()
                cancelling = getattr(current, "cancelling", None)
                if (
                    self._loop_cancel_requested
                    or not self._running
                    or cancelling is None       # Python 3.10
                    or cancelling() > 0
                ):
                    raise
                self._logger.error(
                    "A node raised CancelledError inside the tick; continuing",
                    tick=self._tick_count,
                )
            except Exception as exc:  # noqa: BLE001
                self._logger.error(
                    "Unhandled exception in tick loop",
                    tick=self._tick_count,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            finally:
                self._loop_ticking = False
                self._loop_tick_idle.set()

            elapsed = time.monotonic() - start
            sleep   = max(0.0, self._interval - elapsed)

            if elapsed > self._interval * (1 + self._overrun_warn):
                self._consecutive_overruns += 1
                if self._consecutive_overruns >= self._overrun_warn_consecutive:
                    overrun_ms = round((elapsed - self._interval) * 1000, 1)
                    self._logger.warning(
                        "Tick overrun",
                        tick=self._tick_count,
                        overrun_ms=overrun_ms,
                        elapsed_ms=round(elapsed * 1000, 1),
                        interval_ms=round(self._interval * 1000, 1),
                        consecutive=self._consecutive_overruns,
                    )
            else:
                self._consecutive_overruns = 0

            await self._sleep_or_stop(sleep)

    async def _sleep_or_stop(self, seconds: float) -> None:
        """
        Wait for the next tick, waking at once when stop() / emergency_stop()
        sets the stop event, so shutdown does not lag by up to one period.
        """
        if seconds <= 0.0 or self._stop_event.is_set():
            await asyncio.sleep(0)
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)

    async def tick(self) -> None:
        """Execute one full pipeline cycle. Public for testing.

        This is a testing shim: it executes the pipeline unconditionally and
        **does not honour `self._paused`**. The `_paused` flag only gates the
        background `_loop()`. Tests typically call `pause()` first precisely
        to stop the background loop from racing with manual `tick()` calls,
        and then drive ticks deterministically via this method.

        Actions selected this tick are launched, not awaited: use
        ``await wait_idle()`` before asserting on their effects; their
        Results appear in the next tick's ``ctx.last_results``.
        """
        tick_token = object()
        self._live_contexts.add(tick_token)
        context_token = _RUNTIME_CONTEXT.set(tick_token)
        try:
            await self._run_tick()
        finally:
            _RUNTIME_CONTEXT.reset(context_token)
            self._live_contexts.discard(tick_token)

    async def _run_tick(self) -> None:
        """Pipeline body of :meth:`tick`."""
        self._tick_count += 1
        tick_start = time.monotonic()

        # Sync the per-tick counter on the runtime + master loggers so that
        # every LogEvent.tick reflects the current tick. Leaf-node loggers
        # are synced via the default ``BaseNode.on_tick_start`` below.
        self._logger._set_tick(self._tick_count)
        self._sense_master.logger._set_tick(self._tick_count)
        self._instinct_master.logger._set_tick(self._tick_count)
        self._decision_master.logger._set_tick(self._tick_count)
        self._action_master.logger._set_tick(self._tick_count)

        # Cache the instrumenter reference for the duration of the tick —
        # avoids repeated attribute loads and ensures stage-name bookkeeping
        # stays consistent even if ``set_tick_instrumenter`` is called
        # concurrently (single-threaded event loop guarantees atomicity at
        # attribute-assignment granularity, but the caching makes the
        # semantics explicit). Per the instrumentation ADR the boundary logic short-circuits
        # on ``is not None`` when no instrumenter is attached.
        instrumenter = self._tick_instrumenter
        stage_start = tick_start

        # 0. Tick-start hooks on all leaf nodes (charged to `sense`, instrumentation ADR)
        await asyncio.gather(
            self._sense_master.notify_tick_start(self._tick_count),
            self._instinct_master.notify_tick_start(self._tick_count),
            self._action_master.notify_tick_start(self._tick_count),
        )

        # 1b. Snapshot-and-clear the buffered supervisor/node_fault signals
        #     *before* read_all. Doing this first means a node that faults
        #     during this tick's sense stage is deferred to the next tick's
        #     drain, keeping fault visibility uniformly at N+1 regardless of
        #     which stage (sense/instinct/action) raised — rather than sense
        #     faults leaking into the same tick they occurred in.
        buffered_signals = self._supervisor_signal_buffer
        self._supervisor_signal_buffer = []

        # Drain bus-published StateUpdateSignals collected since the last tick.
        # Snapshot-and-cleared here (before read_all) for the same reason as the
        # supervisor buffer: a signal published during this tick is deferred to
        # the next tick's drain, keeping bus-write visibility uniformly at N+1.
        buffered_state_updates = self._state_update_buffer
        self._state_update_buffer = []

        # Results of actions that finished since the previous tick's context
        # (actions run as tasks across ticks — ADR 0005). Completion order.
        completed = self._action_master.drain_completed()
        if completed:
            self._last_results = completed
            self._last_result = completed[0]
        interior_interrupt = any(self._stopped_inside_block(r) for r in completed)

        # 1. Sense
        signals = await self._sense_master.read_all()

        # Append faults observed since the last tick (sense signals first, then
        # the buffered FAULTED/DEAD/RESTARTING + node_fault signals).
        if buffered_signals:
            signals.extend(buffered_signals)

        # Append bus-published StateUpdateSignals after sense output so that,
        # within this drain, an explicit bus write wins over a same-key sense
        # write (ContextNode.update() applies them in list order, last wins).
        if buffered_state_updates:
            signals.extend(buffered_state_updates)

        if instrumenter is not None:
            now = time.monotonic()
            self._emit_stage(instrumenter, "sense", now - stage_start)
            stage_start = now

        # 2. Context update — build action_states from all running actions
        action_states: list[ActionExecutionState] = []
        for node in self._action_master.current_actions().values():
            if hasattr(node, "execution_state"):
                action_states.append(node.execution_state())

        ctx = self._context.update(
            signals,
            result=self._last_result,
            action_states=action_states,
            results=self._last_results,
        )

        # Results that completed since the previous tick have now been
        # snapshotted into ``ctx`` and are observable by instincts via
        # ``ctx.last_results`` / ``ctx.last_result`` for the duration of this
        # tick.  Clear the runtime-side slots — gives ``last_results``
        # precisely one tick of lifetime.
        self._last_results = []
        self._last_result = None

        # Notify Context observers (e.g. SignalDashboard).  Failures here are
        # isolated — a broken observer cannot stall the tick loop.
        for obs in self._context_observers:
            try:
                obs(ctx)
            except Exception as e:  # noqa: BLE001
                self._logger.warning(
                    "Context observer raised; ignoring",
                    observer=getattr(obs, "__qualname__", repr(obs)),
                    error=str(e),
                )

        if instrumenter is not None:
            now = time.monotonic()
            self._emit_stage(instrumenter, "context", now - stage_start)
            stage_start = now

        # 3. Reflex pass — bypass decision. Reflex actions are launched, not
        #    awaited, and take their resources from running owners per the
        #    reflex's preemption mode (ADR 0005). Charged to `reflex`.
        try:
            reflex_proposals = await self._instinct_master.evaluate_reflexes(
                ctx, nodes=self._tick_reflex_nodes(),
            )
        except ReflexConflictError as exc:
            # reflex_conflict='raise' flags an ambiguous reflex configuration,
            # but aborting the tick would also suppress every reflex and the
            # whole deliberative pipeline. Fail safe: report loudly, then
            # dispatch the conflicting reflexes in priority order.
            self._logger.error(
                "Reflex priority conflict; dispatching all in priority order",
                priority=exc.priority,
                instinct_ids=exc.node_ids,
            )
            reflex_proposals = list(exc.proposals)
        await self._launch_reflexes(reflex_proposals)

        if instrumenter is not None:
            now = time.monotonic()
            self._emit_stage(instrumenter, "reflex", now - stage_start)
            stage_start = now

        # 4. Normal instinct evaluation
        proposals = await self._instinct_master.evaluate_all(ctx)

        if instrumenter is not None:
            now = time.monotonic()
            self._emit_stage(instrumenter, "instinct", now - stage_start)
            stage_start = now

        # 5. Decide — build interruptibility map, resolve proposals, notify
        #    rejected, and issue interrupts (all charged to `decide`).
        # A reflex-owned action is never interruptible by deliberation.
        running_interruptible = {
            aid: (
                self._action_master.origin_of(aid) != "reflex"
                and self._action_master.is_interruptible(aid)
            )
            for aid in self._action_master.running_action_ids()
        }

        to_dispatch, interrupts = await self._decision_master.on_new_proposals_many(
            proposals,
            running_proposals=self._action_master.current_proposals(),
            running_interruptible=running_interruptible,
            evaluated_instinct_ids=self._instinct_master.last_evaluated_ids,
            conflicts=self._action_master.conflicting_owners,
        )

        # Notify rejected instincts (includes carried-forward pending proposals)
        all_considered = self._decision_master.last_considered
        reflex_ids = {n.node_id for n in self._instinct_master.reflex_nodes}
        reflex_routed = any(p.instinct_id in reflex_ids for p in all_considered)
        dispatched_ids = {p.instinct_id for p in to_dispatch}
        rejected = [p for p in all_considered if p.instinct_id not in dispatched_ids]
        if rejected:
            await self._instinct_master.notify_rejected(rejected)

        # Issue interrupts
        for interrupt_req in interrupts:
            target = interrupt_req.target_action_id or interrupt_req.new_proposal.action_id
            try:
                await self._action_master.request_interrupt(
                    interrupt_req, action_id=target,
                )
            except MandatoryBlockViolation as exc:
                self._logger.warning(
                    "Interrupt blocked by mandatory step",
                    action_id=target,
                    detail=str(exc),
                )
            except Exception as exc:  # noqa: BLE001
                self._logger.error(
                    "Interrupt request failed",
                    action_id=target,
                    error=str(exc),
                )

        # Fan out DecisionEvent to observers (e.g. SignalDashboard).  This
        # happens before the act stage so observers see the dispatch plan as
        # decided, independent of action outcomes.  Failures are isolated —
        # a broken observer cannot stall the tick loop.
        if self._decision_observers:
            decision_event = DecisionEvent(
                tick       = self._tick_count,
                timestamp  = time.monotonic(),
                strategy   = type(self._decision_master.strategy).__name__,
                considered = list(all_considered),
                dispatched = list(to_dispatch),
                interrupts = list(interrupts),
            )
            for dec_obs in self._decision_observers:
                try:
                    dec_obs(decision_event)
                except Exception as e:  # noqa: BLE001
                    self._logger.warning(
                        "Decision observer raised; ignoring",
                        observer=getattr(dec_obs, "__qualname__", repr(dec_obs)),
                        error=str(e),
                    )

        if instrumenter is not None:
            now = time.monotonic()
            self._emit_stage(instrumenter, "decide", now - stage_start)
            stage_start = now

        # 6. Act — launch the selected proposals as tasks (not awaited: the
        #    next tick runs while they execute), highest priority first. A
        #    proposal that interrupted an owner which is still unwinding is
        #    parked for hand-off; one that merely lost a same-tick resource
        #    conflict to a sibling falls back to the normal pending rules.
        best: dict[str, Proposal] = {}
        for p in to_dispatch:
            existing = best.get(p.action_id)
            if existing is None or p.priority > existing.priority:
                best[p.action_id] = p
        interrupting = {ir.new_proposal.instinct_id for ir in interrupts}
        if best and (self._stopping or (self._paused and self._loop_ticking)):
            # stop()/emergency_stop()/pause() arrived during this tick:
            # launch nothing deliberative (reflexes above already launched).
            self._logger.info(
                "Stopping or paused during the tick; deliberative launches dropped",
                action_ids=sorted(best),
            )
            best = {}
        for p in sorted(best.values(), key=lambda q: (q.priority, q.urgency), reverse=True):
            outcome = self._action_master.launch(p, origin="decision")
            if outcome.status is LaunchStatus.BLOCKED:
                if p.instinct_id in interrupting:
                    self._decision_master.park(p)
                else:
                    self._decision_master.defer(p)
            elif outcome.status is LaunchStatus.NOT_FOUND:
                self._logger.warning(
                    "Dispatch skipped: action not found",
                    action_id=p.action_id,
                    instinct_id=p.instinct_id,
                )

        # 7. Tick-end hooks on all leaf nodes (charged to `act`, instrumentation ADR)
        tick_duration = time.monotonic() - tick_start
        outcomes = list(self._reflex_outcomes)
        self._reflex_outcomes.clear()
        self._monitor_report = {
            "reflex_outcomes": outcomes,
            "reflex_routed": reflex_routed,
            "interior_interrupt": interior_interrupt,
            "tick_duration_ms": tick_duration * 1000.0,
        }
        await asyncio.gather(
            self._sense_master.notify_tick_end(self._tick_count, tick_duration),
            self._instinct_master.notify_tick_end(self._tick_count, tick_duration),
            self._action_master.notify_tick_end(self._tick_count, tick_duration),
        )

        if instrumenter is not None:
            now = time.monotonic()
            self._emit_stage(instrumenter, "act", now - stage_start)
            self._emit_tick_complete(instrumenter, self._tick_count, now - tick_start)

    def _emit_stage(
        self,
        instrumenter: TickInstrumenter,
        stage: str,
        duration_s: float,
    ) -> None:
        """Invoke ``on_stage`` and isolate any instrumenter error.

        Per the instrumentation ADR: instrumenter exceptions must not crash the tick loop.
        Log at WARNING level and continue. The once-per-tick granularity
        keeps the noise floor manageable even if the instrumenter is
        persistently broken.
        """
        try:
            instrumenter.on_stage(stage, duration_s)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning(
                "TickInstrumenter.on_stage raised",
                stage=stage,
                error=str(exc),
                error_type=type(exc).__name__,
            )

    def _emit_tick_complete(
        self,
        instrumenter: TickInstrumenter,
        tick_index: int,
        total_s: float,
    ) -> None:
        """Invoke ``on_tick_complete`` and isolate any instrumenter error."""
        try:
            instrumenter.on_tick_complete(tick_index, total_s)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning(
                "TickInstrumenter.on_tick_complete raised",
                tick=tick_index,
                error=str(exc),
                error_type=type(exc).__name__,
            )
