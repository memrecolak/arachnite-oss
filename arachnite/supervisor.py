"""
arachnite.supervisor
~~~~~~~~~~~~~~~~~~~~
NodeSupervisor: monitors node health and applies restart policies.
Spec reference: Section 6.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Set as AbstractSet

from arachnite.bus import SignalBus
from arachnite.exceptions import SupervisorError
from arachnite.models import NodeFaultSignal, NodeState, RestartPolicy, SupervisorSignal
from arachnite.nodes.base import BaseNode


class NodeSupervisor:
    """
    Attached to each master node. Tracks the lifecycle state of every
    registered child node and applies a restart policy when faulted.

    Supervisor signals flow onto the bus; the runtime buffers them and injects
    them at the start of the next tick, so InstinctNodes — including
    ReflexInstinctNodes — can react to node failures on the tick after they
    occur (a fault on tick N is visible in ctx.signals on tick N+1).

    Restart budget: a node has at most one restart pending or in progress at
    a time. A fault arriving meanwhile is still reported (FAULTED transition
    and signals) but schedules nothing, and every restart attempt counts
    against ``max_restarts``. DEAD is terminal: a DEAD node is never
    restarted automatically and further faults leave it DEAD; only an
    explicit ``restart()`` or a fresh ``track()`` brings it back. Masters
    skip DEAD nodes (see ``dead_node_ids``).

    Spec reference: Section 6.2.
    """

    def __init__(
        self,
        bus: SignalBus,
        supervisor_id: str = "supervisor",
        restart_policy: RestartPolicy = RestartPolicy.ON_FAILURE,
        max_restarts: int = 3,
        restart_delay_s: float = 1.0,
        agent_node_id: str = "local",
    ) -> None:
        self._bus            = bus
        self._supervisor_id  = supervisor_id
        self._restart_policy = restart_policy
        self._max_restarts   = max_restarts
        self._restart_delay  = restart_delay_s
        self._agent_node_id  = agent_node_id

        self._states:         dict[str, NodeState]    = {}
        self._restart_counts: dict[str, int]          = {}
        self._nodes:          dict[str, BaseNode]     = {}
        self._restart_tasks:  set[asyncio.Task[None]] = set()
        self._restart_targets: dict[asyncio.Task[None], BaseNode] = {}
        # The one scheduled restart per node (pending or in progress), and
        # the nodes a manual restart() is restarting right now.
        self._pending_restart: dict[str, asyncio.Task[None]] = {}
        self._manual_restarts: set[str] = set()
        # Ids of DEAD nodes: a live set the masters consult each tick.
        self._dead: set[str] = set()

    # ── Node tracking ─────────────────────────────────────────────────────────

    def track(self, node: BaseNode) -> None:
        """
        Begin supervising a node. Sets initial state to STARTING with a fresh
        restart budget; a restart still pending under the same id is cancelled.
        """
        pending = self._pending_restart.pop(node.node_id, None)
        if pending is not None:
            pending.cancel()
        self._nodes[node.node_id]          = node
        self._states[node.node_id]         = NodeState.STARTING
        self._restart_counts[node.node_id] = 0
        self._dead.discard(node.node_id)

    def untrack(self, node_id: str) -> None:
        """Stop supervising a node; a restart of it still pending is cancelled."""
        node = self._nodes.pop(node_id, None)
        self._states.pop(node_id, None)
        self._restart_counts.pop(node_id, None)
        self._dead.discard(node_id)
        self._pending_restart.pop(node_id, None)
        for task, target in list(self._restart_targets.items()):
            if target is node:
                task.cancel()

    @property
    def dead_node_ids(self) -> AbstractSet[str]:
        """
        Live, read-only view of the ids of tracked nodes in DEAD state.

        The runtime hands it to the master nodes, which skip these nodes when
        they fan a call (``read()``, ``evaluate()``, tick hooks) out to their
        leaves: a DEAD node is not run again. Membership is an O(1) lookup.
        """
        return self._dead

    def state_of(self, node_id: str) -> NodeState:
        """Return the current NodeState for a tracked node."""
        if node_id not in self._states:
            raise KeyError(f"Node '{node_id}' is not tracked by this supervisor.")
        return self._states[node_id]

    @property
    def supervisor_id(self) -> str:
        """The id this supervisor publishes its signals under (e.g. ``supervisor_0``)."""
        return self._supervisor_id

    def all_states(self) -> dict[str, NodeState]:
        """Return a snapshot of {node_id: NodeState} for all tracked nodes."""
        return dict(self._states)

    def is_healthy(self) -> bool:
        """True if no tracked node is in FAULTED or DEAD state."""
        return not any(
            s in (NodeState.FAULTED, NodeState.DEAD)
            for s in self._states.values()
        )

    # ── State transitions ─────────────────────────────────────────────────────

    async def _transition(
        self,
        node_id: str,
        new_state: NodeState,
        error: BaseException | None = None,
    ) -> None:
        """Transition a node to a new state and publish a SupervisorSignal."""
        prev = self._states.get(node_id, NodeState.STARTING)
        self._states[node_id] = new_state
        if new_state is NodeState.DEAD:
            self._dead.add(node_id)
        else:
            self._dead.discard(node_id)

        ts = time.monotonic()
        count = self._restart_counts.get(node_id, 0)

        signal = SupervisorSignal(
            source         = self._supervisor_id,
            kind           = "supervisor",
            value          = new_state.value,
            confidence     = 1.0,
            timestamp      = ts,
            node_id        = node_id,
            previous_state = prev,
            current_state  = new_state,
            restart_count  = count,
            fault_error    = error,
        )
        with contextlib.suppress(Exception):
            await self._bus.publish(signal)

        # Emit a typed NodeFaultSignal for fault/dead transitions
        if new_state in (NodeState.FAULTED, NodeState.DEAD) and error is not None:
            fault_signal = NodeFaultSignal(
                source         = self._supervisor_id,
                kind           = "node_fault",
                value          = new_state.value,
                confidence     = 1.0,
                timestamp      = ts,
                node_id        = node_id,
                previous_state = prev,
                current_state  = new_state,
                restart_count  = count,
                fault_error    = error,
            )
            with contextlib.suppress(Exception):
                await self._bus.publish(fault_signal)

    # ── Lifecycle management ──────────────────────────────────────────────────

    async def mark_running(self, node_id: str) -> None:
        await self._transition(node_id, NodeState.RUNNING)

    async def mark_stopped(self, node_id: str) -> None:
        await self._transition(node_id, NodeState.STOPPED)

    def _budget_exhausted(self, node_id: str) -> bool:
        """
        Single source of truth for the restart budget.

        Returns True when the node has no restart attempts left, i.e. the number
        of attempts recorded in ``_restart_counts`` has reached ``max_restarts``.
        The count is incremented exactly once per attempt, at the top of
        ``_restart``; both budget checks compare the *stored* count against
        ``max_restarts`` but sit on opposite sides of that single increment:

        - ``on_fault`` checks *before* scheduling an attempt, so the count
          reflects already-completed restarts ("have we used them all?").
        - ``_restart`` checks *after* incrementing for the attempt it just made,
          so the count reflects that attempt ("did this one hit the limit?").

        Routing both through here keeps the comparison identical and gives the
        budget one owner, so the invariant — exactly ``max_restarts`` restart
        attempts, no more — cannot drift if one call site is later edited.
        """
        return self._restart_counts.get(node_id, 0) >= self._max_restarts

    async def on_fault(self, node_id: str, error: BaseException) -> None:
        """
        Called when a node raises an unhandled exception.
        Applies the restart policy.

        A DEAD node stays DEAD (no transition, nothing scheduled). While a
        restart of the node is already pending or in progress the fault is
        reported (FAULTED transition + signals) but schedules nothing: one
        restart at a time, so a burst of faults inside ``restart_delay_s``
        cannot spend the budget several times over.
        """
        if self._states.get(node_id) is NodeState.DEAD:
            return
        await self._transition(node_id, NodeState.FAULTED, error=error)

        if self._restart_in_flight(node_id) or self._states.get(node_id) is NodeState.DEAD:
            return

        if self._restart_policy == RestartPolicy.NEVER:
            await self._transition(node_id, NodeState.DEAD, error=error)
            return

        # ON_FAILURE: restart on unhandled exception (always the case in on_fault).
        # ALWAYS: restart on any non-STOPPED exit.
        # Both policies reach here; check the restart budget *before* this
        # attempt (with one restart at a time, the count is every attempt made).
        if self._budget_exhausted(node_id):
            await self._transition(node_id, NodeState.DEAD, error=error)
            return

        if node_id in self._nodes:
            self._schedule_restart(node_id)

    def _restart_in_flight(self, node_id: str) -> bool:
        """True while a scheduled or manual restart of the node is pending or running."""
        return node_id in self._pending_restart or node_id in self._manual_restarts

    async def restart(self, node_id: str) -> None:
        """
        Manually trigger a restart for a specific node.

        The only way out of DEAD besides a fresh ``track()``. A scheduled
        restart still pending for the node is cancelled first, so the two
        never tear the node down concurrently.
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node '{node_id}' is not tracked.")
        pending = self._pending_restart.pop(node_id, None)
        if pending is not None and pending is not asyncio.current_task():
            pending.cancel()
            await asyncio.wait({pending})
        self._manual_restarts.add(node_id)
        try:
            await self._restart(node_id)
        finally:
            self._manual_restarts.discard(node_id)

    async def _restart(self, node_id: str, *, raise_on_exhaustion: bool = True) -> None:
        """
        Tear down and rebuild a node, applying the restart budget.

        On exhausting ``max_restarts`` the node transitions to DEAD and the
        terminal error is published on the bus (SupervisorSignal +
        NodeFaultSignal). ``raise_on_exhaustion`` controls whether the terminal
        failure is *also* raised as a SupervisorError:

        - Directly-awaited callers (the public ``restart()`` API) get the
          default ``True`` so the caller can observe and handle the failure.
        - The fire-and-forget path scheduled by ``_schedule_restart`` passes
          ``False``: nobody awaits that task, so raising would only produce an
          un-retrieved "Task exception was never retrieved" warning. Terminal
          failure there is surfaced purely as the DEAD transition + bus signals,
          matching the framework's best-effort fault-reporting contract.
        """
        await asyncio.sleep(self._restart_delay)
        node = self._nodes.get(node_id)
        current = asyncio.current_task()
        expected = self._restart_targets.get(current) if current is not None else None
        if node is None or (expected is not None and node is not expected):
            return  # untracked, or replaced by a new node under the same id
        if expected is not None and self._states.get(node_id) is NodeState.DEAD:
            return  # DEAD is terminal for scheduled restarts

        await self._transition(node_id, NodeState.RESTARTING)
        self._restart_counts[node_id] = self._restart_counts.get(node_id, 0) + 1

        try:
            # Listeners spawned by the previous setup() would otherwise keep
            # running beside the ones the new setup() starts.
            await node.cancel_background_tasks()
            await node.teardown()
            await node.setup()
            await self._transition(node_id, NodeState.RUNNING)
        except Exception as exc:  # noqa: BLE001
            await self._transition(node_id, NodeState.FAULTED, error=exc)
            # Budget checked *after* the increment above, so the count includes
            # the attempt that just failed.
            if self._budget_exhausted(node_id):
                await self._transition(node_id, NodeState.DEAD, error=exc)
                if raise_on_exhaustion:
                    raise SupervisorError(node_id, exc) from exc
                return
            self._schedule_restart(node_id)

    # ── Restart-task management ─────────────────────────────────────────────────

    def _schedule_restart(self, node_id: str) -> None:
        """Schedule a tracked restart task"""
        # Fire-and-forget: nobody awaits this task, so terminal failure must not
        # raise (it would leak as an un-retrieved task exception). It is reported
        # via the DEAD transition + bus signals instead.
        task = asyncio.create_task(
            self._restart(node_id, raise_on_exhaustion=False),
            name=f"supervisor_restart_{node_id}",
        )
        self._restart_tasks.add(task)
        # The node this restart is for: a replacement registered under the
        # same id later is not torn down by it.
        self._restart_targets[task] = self._nodes[node_id]
        self._pending_restart[node_id] = task
        task.add_done_callback(self._restart_task_done)

    def _restart_task_done(self, task: asyncio.Task[None]) -> None:
        self._restart_tasks.discard(task)
        self._restart_targets.pop(task, None)
        for node_id, pending in list(self._pending_restart.items()):
            if pending is task:
                del self._pending_restart[node_id]

    async def cancel_restart_tasks(self) -> None:
        """Cancel all in-flight restart tasks. Called during shutdown."""
        for task in list(self._restart_tasks):
            task.cancel()
        if self._restart_tasks:
            await asyncio.gather(*self._restart_tasks, return_exceptions=True)
        self._restart_tasks.clear()
        self._restart_targets.clear()
        self._pending_restart.clear()

    @property
    def restart_task_count(self) -> int:
        """Number of in-flight restart tasks"""
        return len(self._restart_tasks)

    # ── Introspection ─────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        healthy = sum(
            1 for s in self._states.values()
            if s not in (NodeState.FAULTED, NodeState.DEAD)
        )
        return (
            f"NodeSupervisor("
            f"id={self._supervisor_id!r}, "
            f"nodes={len(self._nodes)}, "
            f"healthy={healthy})"
        )
