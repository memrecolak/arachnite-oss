"""
arachnite.distributed.mesh
~~~~~~~~~~~~~~~~~~~~~~~~~~~
MeshRuntime: coordinates a multi-AgentNode deployment from one entry point.
Spec reference: Section 11.4.
"""

from __future__ import annotations

import asyncio

from arachnite.distributed.agent_node import AgentNode
from arachnite.distributed.manifest import DeploymentManifest
from arachnite.logging import BaseLogSink
from arachnite.models import NodeState
from arachnite.transport.base import BaseTransport


class MeshRuntime:
    """
    Coordinates a multi-agent deployment from a single entry point.

    Reads the manifest, builds all AgentNodes, and starts them.
    Local agents run as asyncio tasks in the current process.
    Remote agents are expected to start independently and connect
    via the configured transport.

    Spec reference: Section 11.4.
    """

    def __init__(
        self,
        manifest:   DeploymentManifest,
        log_sinks:  list[BaseLogSink] | None = None,
        transports: dict[str, BaseTransport] | None = None,
    ) -> None:
        """
        Args:
            manifest:   Validated DeploymentManifest.
            log_sinks:  Log sinks applied to all nodes.
            transports: Optional dict of {agent_id: transport} overrides.
                        Useful in tests to inject mock transports.
        """
        self._manifest   = manifest
        self._log_sinks  = log_sinks or []
        self._transports = transports or {}
        self._agents:    dict[str, AgentNode] = {}
        self._started = False

    async def start(self) -> None:
        """
        Build and start all AgentNodes defined in the manifest.

        Builds each AgentNode from the manifest, then starts them
        all concurrently. Each AgentNode connects its transport and
        starts its runtime loop.
        """
        if self._started:
            return

        # Build agents
        for agent_id in self._manifest.agent_ids():
            transport = self._transports.get(agent_id)
            agent     = AgentNode.from_manifest(
                manifest   = self._manifest,
                agent_id   = agent_id,
                transport  = transport,
                log_sinks  = self._log_sinks,
            )
            self._agents[agent_id] = agent

        # Start all concurrently
        await asyncio.gather(*(a.start() for a in self._agents.values()))
        self._started = True

    async def stop(self) -> None:
        """Stop all local AgentNodes and disconnect their transports."""
        await asyncio.gather(*(a.stop() for a in self._agents.values()))
        self._started = False

    def agent(self, agent_node_id: str) -> AgentNode:
        """Return a specific AgentNode by id."""
        if agent_node_id not in self._agents:
            raise KeyError(
                f"AgentNode '{agent_node_id}' not found. "
                f"Available: {list(self._agents)}"
            )
        return self._agents[agent_node_id]

    def mesh_health(self) -> dict[str, bool]:
        """
        Returns {agent_node_id: is_healthy} for all known agents.
        Remote agent health is inferred from their SupervisorSignals.
        """
        return {
            agent_id: agent.health.system_healthy()
            for agent_id, agent in self._agents.items()
        }

    def nodes_in_state(self, state: NodeState) -> dict[str, list[str]]:
        """
        Returns {agent_node_id: [node_ids]} for nodes in the given state
        across all agents.
        """
        result: dict[str, list[str]] = {}
        for agent_id, agent in self._agents.items():
            nodes = agent.health.nodes_in_state(state)
            if nodes:
                result[agent_id] = nodes
        return result

    @property
    def agents(self) -> dict[str, AgentNode]:
        return dict(self._agents)

    @property
    def started(self) -> bool:
        return self._started

    def __repr__(self) -> str:
        return (
            f"MeshRuntime("
            f"agents={list(self._agents)}, "
            f"started={self._started})"
        )
