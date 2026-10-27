"""
arachnite.nodes.base
~~~~~~~~~~~~~~~~~~~~
BaseNode: the abstract base class for every node type.
Spec reference: Section 5.1.
"""

from __future__ import annotations

import asyncio
import contextlib
from abc import ABC
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from collections.abc import Set as AbstractSet
from pathlib import Path
from typing import Any, TypeVar

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.logging import BaseLogSink, StructuredLogger
from arachnite.models import Permission

_T = TypeVar("_T")

async def gather_settled(*aws: Awaitable[Any]) -> None:
    """
    Await every awaitable to completion, then raise the first exception any
    of them raised. Unlike ``asyncio.gather`` it never returns early on a
    failure, so one node's raising teardown cannot leave its siblings'
    teardowns running unawaited.

    A cancel of the caller propagates as usual (``gather`` raises it). An
    awaitable that ends in its *own* CancelledError (e.g. ``task.cancel();
    await task`` without suppressing it) is an error of that awaitable: it
    is raised as a RuntimeError, never as a cancellation nobody requested.
    """
    error = settled_error(await asyncio.gather(*aws, return_exceptions=True))
    if error is not None:
        raise error


def settled_error(outcomes: Sequence[object]) -> BaseException | None:
    """
    The first exception among ``gather(..., return_exceptions=True)``
    outcomes, with an awaitable's own CancelledError turned into a
    RuntimeError (see ``gather_settled``); None if all succeeded.
    """
    for outcome in outcomes:
        if isinstance(outcome, asyncio.CancelledError):
            error = RuntimeError("awaitable ended with its own CancelledError")
            error.__cause__ = outcome
            return error
        if isinstance(outcome, BaseException):
            return outcome
    return None

async def setup_nodes(nodes: Sequence[BaseNode]) -> None:
    """
    Run every node's ``setup()`` concurrently and wait for all of them.

    If any fails, nothing set up here is left running: the background tasks
    of every node (the failing one's too) are cancelled, the nodes whose
    ``setup()`` succeeded are torn down, and the first error is raised. A
    master's ``setup()`` uses this so a failed ``start()`` leaks no
    listener of a sibling (or of a sibling still inside a slow ``setup()``).
    """
    outcomes = await asyncio.gather(*(n.setup() for n in nodes), return_exceptions=True)
    error = settled_error(outcomes)
    if error is None:
        return
    await asyncio.gather(
        *(n.cancel_background_tasks() for n in nodes), return_exceptions=True,
    )
    await asyncio.gather(
        *(
            n.teardown() for n, outcome in zip(nodes, outcomes, strict=True)
            if not isinstance(outcome, BaseException)
        ),
        return_exceptions=True,
    )
    raise error


#: Signature of the runtime-supplied fault sink: ``(node_id, error) -> None``.
#: Wired to a :class:`~arachnite.supervisor.NodeSupervisor`'s ``on_fault``.
FaultHandler = Callable[[str, BaseException], Awaitable[None]]


class FaultReportingMixin:
    """
    Opt-in fault sink for master nodes that own a collection of leaf nodes.

    Master nodes catch leaf-node exceptions so a single misbehaving node can
    never crash the tick loop. Historically those exceptions were logged and
    swallowed, which meant the supervisor was never told and the documented
    ``node_fault`` → ``ctx.signals`` path (llms.txt tick step 2, NodeFaultSignal
    docstring) never fired. This mixin lets the runtime wire each master's
    fault sink to the matching supervisor's ``on_fault`` so a raise transitions
    the node to FAULTED, emits the SupervisorSignal + NodeFaultSignal, and —
    via the runtime's buffered-signal injection — surfaces in the *next* tick's
    Context.

    The handler is ``None`` by default, so a master driven standalone (without a
    runtime, e.g. in tests or embedded use) keeps its swallow-and-continue
    behaviour unchanged.
    """

    #: Set to a logger by the host class (BaseNode provides ``self.logger``).
    logger: StructuredLogger

    _fault_handler: FaultHandler | None = None

    #: Ids of leaf nodes the supervisor declared DEAD (a live set it owns).
    _dead_nodes: AbstractSet[str] = frozenset()

    def set_fault_handler(self, handler: FaultHandler | None) -> None:
        """Install (or clear) the fault sink. Called by the runtime at wiring time."""
        self._fault_handler = handler

    def set_dead_nodes(self, dead: AbstractSet[str]) -> None:
        """
        Install the supervisor's live set of DEAD node ids (see
        ``NodeSupervisor.dead_node_ids``). Every per-node fan-out
        (``_gather_nodes``) skips those nodes: a DEAD node is not run again.
        Called by the runtime at wiring time.
        """
        self._dead_nodes = dead

    async def _report_fault(self, node_id: str, error: BaseException) -> None:
        """
        Forward a leaf-node fault to the installed handler, if any.

        Failures inside the handler are swallowed (and logged): reporting a
        fault must never itself crash the tick. No-op when no handler is set.
        """
        handler = self._fault_handler
        if handler is None:
            return
        try:
            await handler(node_id, error)
        except Exception as exc:  # noqa: BLE001
            with contextlib.suppress(Exception):
                self.logger.error(
                    "Fault handler raised while reporting node fault",
                    node_id=node_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    async def _gather_nodes(
        self, what: str, calls: Sequence[tuple[str, Awaitable[_T]]],
    ) -> list[_T | None]:
        """
        Await one call per leaf node concurrently. A call that raises — or
        ends in the node's own CancelledError — is logged and reported as
        that node's fault and yields None; its siblings are unaffected. A
        cancel of the caller still propagates (``gather`` raises it).

        A node the supervisor declared DEAD is skipped: its call is never
        started (the unstarted coroutine is closed) and yields None.

        The result list has exactly one entry per call, in order, so a
        caller may pair it with its node list.
        """
        dead = self._dead_nodes
        skipped: set[int] = set()
        if dead:
            for index, (node_id, aw) in enumerate(calls):
                if node_id in dead:
                    skipped.add(index)
                    if asyncio.iscoroutine(aw):
                        aw.close()
        live = [c for i, c in enumerate(calls) if i not in skipped]
        outcomes = await asyncio.gather(*(aw for _, aw in live), return_exceptions=True)
        results: list[_T | None] = []
        live_outcomes = iter(zip(live, outcomes, strict=True))
        for index in range(len(calls)):
            if index in skipped:
                results.append(None)
                continue
            (node_id, _), outcome = next(live_outcomes)
            if not isinstance(outcome, BaseException):
                results.append(outcome)
                continue
            error: BaseException
            if isinstance(outcome, asyncio.CancelledError):
                error = RuntimeError(f"{what} of '{node_id}' ended with its own CancelledError")
                error.__cause__ = outcome
            elif isinstance(outcome, Exception):
                error = outcome
            else:
                raise outcome  # KeyboardInterrupt, SystemExit
            self.logger.error(
                "Node call raised",
                node_id=node_id,
                call=what,
                error=str(error),
                error_type=type(error).__name__,
            )
            await self._report_fault(node_id, error)
            results.append(None)
        return results


class BaseNode(ABC):
    """
    Abstract base class inherited by every node type.

    Provides:
    - Identity (node_id)
    - Access to the shared SignalBus
    - Typed configuration (NodeConfig, injected from manifest)
    - Per-node StructuredLogger
    - Lifecycle hooks: setup, teardown, on_pause, on_resume
    - Per-tick instrumentation hooks: on_tick_start, on_tick_end

    Spec reference: Section 5.1.
    """

    #: Unique identifier for this node. Defaults to the class name.
    #: Must be unique within a master node's registry.
    node_id: str

    #: Permissions this node requires. Empty means no special capabilities.
    #: Validated at startup against the deployment whitelist (opt-in).
    #: Immutable at class level (frozenset) to prevent shared-state mutation
    #: if a subclass skips super().__init__().  __init__ copies to a mutable set.
    permissions: frozenset[Permission] | set[Permission] = frozenset()

    #: Node IDs that must be registered before the runtime starts.
    #: Validated at startup; raises DependencyValidationError if missing.
    #: Immutable at class level (tuple) for the same reason as *permissions*.
    #: __init__ copies to a mutable list.
    requires: tuple[str, ...] | list[str] = ()

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        # Set node_id class attribute to class name if not already defined
        if "node_id" not in cls.__dict__:
            cls.node_id = cls.__name__

    def __init__(
        self,
        bus: SignalBus,
        config: NodeConfig | None = None,
        log_sinks: list[BaseLogSink] | None = None,
        agent_node_id: str = "local",
        artifact_root: str | Path | None = None,
    ) -> None:
        self.bus    = bus
        self.config = config or NodeConfig.empty(self.node_id)
        self.logger = StructuredLogger(
            node_id       = self.node_id,
            agent_node_id = agent_node_id,
            sinks         = log_sinks or [],
        )
        self._agent_node_id = agent_node_id
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._artifact_root = Path(artifact_root) if artifact_root else Path("artifacts")
        # Copy class-level immutable defaults to mutable per-instance copies
        self.permissions: set[Permission] = set(self.__class__.permissions)
        self.requires: list[str] = list(self.__class__.requires)

    # ── Lifecycle hooks ───────────────────────────────────────────────────────

    async def setup(self) -> None:  # noqa: B027
        """
        Called once before the runtime loop starts.
        Override to initialise hardware, open connections, load models, etc.
        """

    async def teardown(self) -> None:  # noqa: B027
        """
        Called once after the runtime loop stops or on graceful shutdown.
        Override to release resources cleanly.
        """

    async def on_pause(self) -> None:  # noqa: B027
        """
        Called when the runtime is paused (e.g. power-save mode).
        Override to suspend hardware polling, throttle I/O, etc.
        """

    async def on_resume(self) -> None:  # noqa: B027
        """
        Called when the runtime resumes from a paused state.
        Override to restore hardware to its active polling rate.
        """

    # ── Per-tick instrumentation hooks ────────────────────────────────────────

    async def on_tick_start(self, tick: int) -> None:
        """
        Called at the start of each tick, before read()/evaluate().

        Default behaviour: sync the per-node logger's tick counter so that
        every ``LogEvent`` emitted from this node carries the current tick.
        Subclasses overriding this method must call ``super().on_tick_start(tick)``
        to preserve logger sync (otherwise their log events will report stale
        tick numbers).

        Override for per-tick metrics, watchdogs, or rate limiting.
        """
        self.logger._set_tick(tick)

    async def on_tick_end(self, tick: int, duration_s: float) -> None:  # noqa: B027
        """
        Called at the end of each tick with the tick duration.
        Override to emit timing metrics or detect slow ticks.
        """

    # ── Background task management ──────────────────────────────────────────

    def spawn_background_task(
        self, coro: Coroutine[Any, Any, Any], *, name: str | None = None,
    ) -> asyncio.Task[Any]:
        """
        Create a tracked background task.

        Use this instead of raw ``asyncio.create_task()`` in ``setup()``
        so the framework can cancel the task automatically on teardown.
        """
        task = asyncio.create_task(coro, name=name or f"{self.node_id}_bg")
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def cancel_background_tasks(self) -> None:
        """Cancel all tracked background tasks and wait for them to finish."""
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    # ── Artifact directory ───────────────────────────────────────────────────

    @property
    def artifact_dir(self) -> Path:
        """
        Standard writable directory for files produced by this node.

        Convention: ``{artifact_root}/{agent_node_id}/{node_id}/``.
        Created lazily on first access.
        """
        path = self._artifact_root / self._agent_node_id / self.node_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    # ── Dunder helpers ────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node_id={self.node_id!r})"
