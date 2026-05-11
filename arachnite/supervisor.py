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

from arachnite.bus import SignalBus
from arachnite.exceptions import SupervisorError
from arachnite.models import NodeFaultSignal, NodeState, RestartPolicy, SupervisorSignal
from arachnite.nodes.base import BaseNode


class NodeSupervisor:
    """
    Attached to each master node. Tracks the lifecycle state of every
    registered child node and applies a restart policy when faulted.

    Supervisor signals flow onto the bus so InstinctNodes — including
    ReflexInstinctNodes — can react to node failures in the same tick.

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

    # ── Node tracking ─────────────────────────────────────────────────────────

    def track(self, node: BaseNode) -> None:
        """Begin supervising a node. Sets initial state to STARTING."""
        self._nodes[node.node_id]          = node
        self._states[node.node_id]         = NodeState.STARTING
        self._restart_counts[node.node_id] = 0

    def untrack(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)
        self._states.pop(node_id, None)
        self._restart_counts.pop(node_id, None)

    def state_of(self, node_id: str) -> NodeState:
        """Return the current NodeState for a tracked node."""
        if node_id not in self._states:
            raise KeyError(f"Node '{node_id}' is not tracked by this supervisor.")
        return self._states[node_id]

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

    async def on_fault(self, node_id: str, error: BaseException) -> None:
        """
        Called when a node raises an unhandled exception.
        Applies the restart policy.
        """
        await self._transition(node_id, NodeState.FAULTED, error=error)

        policy = self._restart_policy
        count  = self._restart_counts.get(node_id, 0)

        if policy == RestartPolicy.NEVER:
            await self._transition(node_id, NodeState.DEAD, error=error)
            return

        # ON_FAILURE: restart on unhandled exception (always the case in on_fault).
        # ALWAYS: restart on any non-STOPPED exit.
        # Both policies reach here; check the restart budget.
        if count >= self._max_restarts:
            await self._transition(node_id, NodeState.DEAD, error=error)
            return

        # Schedule restart
        self._schedule_restart(node_id)

    async def restart(self, node_id: str) -> None:
        """Manually trigger a restart for a specific node."""
        if node_id not in self._nodes:
            raise KeyError(f"Node '{node_id}' is not tracked.")
        await self._restart(node_id)

    async def _restart(self, node_id: str) -> None:
        await asyncio.sleep(self._restart_delay)
        node = self._nodes.get(node_id)
        if node is None:
            return

        await self._transition(node_id, NodeState.RESTARTING)
        self._restart_counts[node_id] = self._restart_counts.get(node_id, 0) + 1

        try:
            await node.teardown()
            await node.setup()
            await self._transition(node_id, NodeState.RUNNING)
        except Exception as exc:  # noqa: BLE001
            await self._transition(node_id, NodeState.FAULTED, error=exc)
            count = self._restart_counts.get(node_id, 0)
            if count >= self._max_restarts:
                await self._transition(node_id, NodeState.DEAD, error=exc)
                raise SupervisorError(node_id, exc) from exc
            self._schedule_restart(node_id)

    # ── Restart-task management ─────────────────────────────────────────────────

    def _schedule_restart(self, node_id: str) -> None:
        """Schedule a tracked restart task"""
        task = asyncio.create_task(
            self._restart(node_id),
            name=f"supervisor_restart_{node_id}",
        )
        self._restart_tasks.add(task)
        task.add_done_callback(self._restart_tasks.discard)

    async def cancel_restart_tasks(self) -> None:
        """Cancel all in-flight restart tasks. Called during shutdown."""
        for task in list(self._restart_tasks):
            task.cancel()
        if self._restart_tasks:
            await asyncio.gather(*self._restart_tasks, return_exceptions=True)
        self._restart_tasks.clear()

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
