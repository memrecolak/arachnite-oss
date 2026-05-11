"""
arachnite.distributed.agent_node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
AgentNode: a named deployment unit — one device or process —
running an ArachniteRuntime with a specific transport.
Spec reference: Section 10.3.
"""

from __future__ import annotations

from typing import Any

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.context import ContextNode
from arachnite.distributed.manifest import DeploymentManifest
from arachnite.health import HealthMonitor
from arachnite.logging import BaseLogSink
from arachnite.nodes.action import ActionMasterNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import (
    InstinctMasterNode,
)
from arachnite.nodes.sense import SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from arachnite.transport.base import BaseTransport
from arachnite.transport.local import LocalTransport


class AgentNode:
    """
    A named deployment unit running an ArachniteRuntime.

    In single-device deployments there is typically one AgentNode
    using LocalTransport. In distributed deployments each physical
    device or cloud service runs one AgentNode with an appropriate
    transport.

    Spec reference: Section 10.3.
    """

    def __init__(
        self,
        node_id:      str,
        runtime:      ArachniteRuntime,
        transport:    BaseTransport,
        tags:         list[str] | None = None,
        description:  str = "",
    ) -> None:
        self.node_id     = node_id
        self.runtime     = runtime
        self.transport   = transport
        self.tags        = tags or []
        self.description = description

    async def start(self) -> None:
        """Connect transport, then start runtime loop."""
        await self.transport.connect()
        await self.runtime.start()

    async def stop(self) -> None:
        """Stop runtime, then disconnect transport."""
        await self.runtime.stop()
        await self.transport.disconnect()

    async def emergency_stop(self) -> None:
        await self.runtime.emergency_stop()
        await self.transport.disconnect()

    @property
    def health(self) -> HealthMonitor:
        return self.runtime.health

    @property
    def is_running(self) -> bool:
        return self.runtime.is_running

    def __repr__(self) -> str:
        return (
            f"AgentNode("
            f"id={self.node_id!r}, "
            f"transport={type(self.transport).__name__}, "
            f"running={self.is_running})"
        )

    # ── Factory: build from manifest assignments ──────────────────────────────

    @classmethod
    def from_manifest(
        cls,
        manifest: DeploymentManifest,
        agent_id: str,
        transport: BaseTransport | None = None,
        log_sinks: list[BaseLogSink] | None = None,
    ) -> AgentNode:
        """
        Build an AgentNode from a validated DeploymentManifest.

        Instantiates all node classes for the given agent_id,
        wires them into master nodes, and creates the runtime.
        """
        ac = manifest.agent_config(agent_id)
        assignments = manifest.assignments_for(agent_id)

        if transport is None:
            transport = _build_transport(ac.transport, agent_id, ac.transport_config)

        bus     = SignalBus()
        context = ContextNode()

        sense_master    = SenseMasterNode(bus=bus, agent_node_id=agent_id)
        instinct_master = InstinctMasterNode(bus=bus, agent_node_id=agent_id)
        decision_master = DecisionMasterNode(
            bus=bus,
            strategy=GreedyDecisionNode(bus=bus),
            agent_node_id=agent_id,
        )
        action_master = ActionMasterNode(bus=bus, agent_node_id=agent_id)

        for assignment in assignments:
            node_config = NodeConfig(assignment.config, node_id=assignment.node_id)
            node = assignment.node_class(
                bus           = bus,
                config        = node_config,
                log_sinks     = log_sinks,
                agent_node_id = agent_id,
            )

            section = assignment.node_section
            if section == "sense":
                sense_master.register(node)  # type: ignore[arg-type]
            elif section == "instinct":
                instinct_master.register(node)  # type: ignore[arg-type]
            elif section == "decision":
                decision_master.set_strategy(node)  # type: ignore[arg-type]
            elif section == "action":
                action_master.register(node)  # type: ignore[arg-type]

        runtime = ArachniteRuntime(
            sense_master    = sense_master,
            context         = context,
            instinct_master = instinct_master,
            decision_master = decision_master,
            action_master   = action_master,
            bus             = bus,
            tick_rate_hz    = ac.tick_rate_hz,
            log_sinks       = log_sinks,
        )

        return cls(
            node_id     = agent_id,
            runtime     = runtime,
            transport   = transport,
            tags        = ac.tags,
            description = ac.description,
        )


def _build_transport(
    kind: str,
    agent_id: str,
    config: dict[str, Any],
) -> BaseTransport:
    """Instantiate the appropriate transport from manifest config."""
    if kind in ("local", ""):
        return LocalTransport(agent_node_id=agent_id)
    if kind == "mqtt":
        from arachnite.transport.mqtt import MQTTTransport
        return MQTTTransport(
            broker_host   = config.get("broker_host", "localhost"),
            broker_port   = int(config.get("broker_port", 1883)),
            agent_node_id = agent_id,
            topic_prefix  = config.get("topic_prefix", "arachnite/"),
            qos           = int(config.get("qos", 1)),
        )
    if kind == "nats":
        from arachnite.transport.nats import NATSTransport
        return NATSTransport(
            servers       = config.get("servers", "nats://localhost:4222"),
            agent_node_id = agent_id,
            subject_prefix = config.get("subject_prefix", "arachnite"),
        )
    if kind == "redis":
        from arachnite.transport.redis import RedisTransport
        return RedisTransport(
            url           = config.get("url", "redis://localhost:6379"),
            agent_node_id = agent_id,
            channel_prefix = config.get("channel_prefix", "arachnite"),
        )
    raise ValueError(
        f"Unknown transport kind '{kind}'. "
        "Valid options: local, mqtt, nats, redis."
    )
