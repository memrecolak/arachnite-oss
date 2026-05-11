"""
arachnite.health
~~~~~~~~~~~~~~~~
HealthMonitor: aggregates NodeSupervisor health across the runtime.
Spec reference: Section 6.5.
"""

from __future__ import annotations

from arachnite.models import NodeState, RemoteNodeState
from arachnite.supervisor import NodeSupervisor


class HealthMonitor:
    """
    Owned by ArachniteRuntime. Aggregates the health status of all
    master node supervisors into a single system-wide view.

    In distributed deployments, also tracks RemoteNodeState updates
    received via SupervisorSignals from other AgentNodes.

    Spec reference: Section 6.5.
    """

    def __init__(self, supervisors: list[NodeSupervisor]) -> None:
        self._supervisors    = supervisors
        self._remote_states: dict[str, dict[str, NodeState]] = {}

    # ── Local health ──────────────────────────────────────────────────────────

    def system_healthy(self) -> bool:
        """True if every local supervisor reports is_healthy()."""
        return all(s.is_healthy() for s in self._supervisors)

    def report(self) -> dict[str, dict[str, NodeState]]:
        """
        Returns a nested dict: {supervisor_id: {node_id: NodeState}}
        for all local supervisors.
        """
        return {
            str(i): s.all_states()
            for i, s in enumerate(self._supervisors)
        }

    def nodes_in_state(self, state: NodeState) -> list[str]:
        """
        Returns node_ids of all local nodes currently in the given state.
        """
        result: list[str] = []
        for supervisor in self._supervisors:
            for node_id, node_state in supervisor.all_states().items():
                if node_state == state:
                    result.append(node_id)
        return result

    # ── Distributed health ────────────────────────────────────────────────────

    def update_remote(self, remote: RemoteNodeState) -> None:
        """
        Called when a SupervisorSignal arrives from a remote AgentNode.
        Updates the remote state registry.
        """
        if remote.agent_node_id not in self._remote_states:
            self._remote_states[remote.agent_node_id] = {}
        self._remote_states[remote.agent_node_id][remote.node_id] = remote.state

    def mesh_healthy(self) -> bool:
        """
        True if all known AgentNodes (local + remote) report healthy states.
        Only meaningful when a non-local transport is in use.
        """
        if not self.system_healthy():
            return False
        for agent_states in self._remote_states.values():
            for state in agent_states.values():
                if state in (NodeState.FAULTED, NodeState.DEAD):
                    return False
        return True

    def remote_states(self) -> dict[str, dict[str, NodeState]]:
        """
        Returns {agent_node_id: {node_id: NodeState}} for all remote
        AgentNodes heard from on the bus.
        """
        return dict(self._remote_states)

    def __repr__(self) -> str:
        return (
            f"HealthMonitor("
            f"supervisors={len(self._supervisors)}, "
            f"remote_agents={len(self._remote_states)}, "
            f"healthy={self.system_healthy()})"
        )
