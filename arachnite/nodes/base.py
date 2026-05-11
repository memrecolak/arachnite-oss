"""
arachnite.nodes.base
~~~~~~~~~~~~~~~~~~~~
BaseNode: the abstract base class for every node type.
Spec reference: Section 5.1.
"""

from __future__ import annotations

import asyncio
from abc import ABC
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.logging import BaseLogSink, StructuredLogger
from arachnite.models import Permission


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
