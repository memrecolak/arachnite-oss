"""
arachnite.distributed.agent_node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
AgentNode: a named deployment unit — one device or process —
running an ArachniteRuntime with a specific transport.
Spec reference: Section 10.3.
"""

from __future__ import annotations

from typing import Any

from arachnite.bus import ORIGIN_AGENT_KEY, SignalBus
from arachnite.config import NodeConfig
from arachnite.context import ContextNode
from arachnite.distributed.manifest import DeploymentManifest, _parse_host_port
from arachnite.health import HealthMonitor
from arachnite.logging import BaseLogSink
from arachnite.models import RemoteNodeState, Signal, SupervisorSignal
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

    Remote signals: the constructor attaches a remote listener to the runtime's
    bus (``SignalBus.add_remote_listener``), so every signal another AgentNode
    publishes is queued into this runtime's next tick (``ctx.signals``, via
    ``ArachniteRuntime.inject_remote_signal``) and every remote
    SupervisorSignal updates this agent's ``health.remote_states()``.

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
        # Remote signals feed the runtime's next tick and the mesh health view
        # however the bridge is started (start(), or bus.start_transport()).
        runtime.bus.add_remote_listener(self._on_remote_signal)

    async def start(self) -> None:
        """Connect transport, bridge it to the bus, then start the runtime."""
        await self.transport.connect()
        # Subscribe the bus's inbound handler on the transport for every kind
        # that has a local subscriber (and '*' for the remote listener), so
        # remote signals are delivered here. No-op without a bus transport.
        await self.runtime.bus.start_transport()
        await self.runtime.start()

    async def stop(self) -> None:
        """Stop runtime, withdraw the bus's transport subscriptions, disconnect."""
        await self.runtime.stop()
        await self.runtime.bus.stop_transport()
        await self.transport.disconnect()

    async def emergency_stop(self) -> None:
        await self.runtime.emergency_stop()
        await self.runtime.bus.stop_transport()
        await self.transport.disconnect()

    def _on_remote_signal(self, signal: Signal) -> None:
        """Feed a signal from another AgentNode to the runtime and mesh health."""
        self.runtime.inject_remote_signal(signal)
        if isinstance(signal, SupervisorSignal) and signal.node_id:
            origin = signal.metadata.get(ORIGIN_AGENT_KEY)
            if origin:
                self.runtime.health.update_remote(RemoteNodeState(
                    agent_node_id = str(origin),
                    node_id       = signal.node_id,
                    state         = signal.current_state,
                    timestamp     = signal.timestamp,
                ))

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
        Build an AgentNode from a DeploymentManifest.

        Validates the manifest first (raising ManifestValidationError), then
        instantiates all node classes for the given agent_id, wires them into
        master nodes, and creates the runtime. Without an explicit
        ``transport`` it is built from the agent's ``transport`` /
        ``transport_config`` and the mesh-level broker settings.
        """
        manifest.validate()
        ac = manifest.agent_config(agent_id)
        assignments = manifest.assignments_for(agent_id)

        if transport is None:
            transport = _build_transport(
                ac.transport, agent_id, ac.transport_config, manifest.mesh_config,
            )

        bus     = SignalBus(
            transport=transport,
            agent_node_id=agent_id,
            wire_kinds=ac.wire_kinds,
        )
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
    mesh: dict[str, Any] | None = None,
) -> BaseTransport:
    """
    Instantiate the appropriate transport from manifest config.

    ``config`` is the agent's ``transport_config``; mesh-level defaults
    (``mqtt_broker: 'host:port'``, ``nats_server``, ``redis_url``) apply where
    the agent does not set the corresponding key.
    """
    mesh = mesh or {}
    if kind in ("local", ""):
        return LocalTransport(agent_node_id=agent_id)
    common: dict[str, Any] = {"agent_node_id": agent_id}
    if "reconnect_interval_s" in config:
        common["reconnect_interval_s"] = float(config["reconnect_interval_s"])
    if "max_reconnect_attempts" in config:
        common["max_reconnect_attempts"] = int(config["max_reconnect_attempts"])
    if kind == "mqtt":
        from arachnite.transport.mqtt import MQTTTransport
        host, port = _parse_host_port(str(mesh.get("mqtt_broker", "localhost")), 1883) or (
            "localhost", 1883,
        )
        return MQTTTransport(
            broker_host   = str(config.get("broker_host", host)),
            broker_port   = int(config.get("broker_port", port)),
            topic_prefix  = config.get("topic_prefix", "arachnite/"),
            qos           = int(config.get("qos", 1)),
            username      = config.get("username"),
            password      = config.get("password"),
            tls           = bool(config.get("tls", False)),
            **common,
        )
    if kind == "nats":
        from arachnite.transport.nats import NATSTransport
        return NATSTransport(
            servers       = config.get(
                "servers", mesh.get("nats_server", "nats://localhost:4222"),
            ),
            subject_prefix = config.get("subject_prefix", "arachnite"),
            **common,
        )
    if kind == "redis":
        from arachnite.transport.redis import RedisTransport
        return RedisTransport(
            url           = config.get("url", mesh.get("redis_url", "redis://localhost:6379")),
            channel_prefix = config.get("channel_prefix", "arachnite"),
            db            = int(config.get("db", 0)),
            password      = config.get("password"),
            **common,
        )
    raise ValueError(
        f"Unknown transport kind '{kind}'. "
        "Valid options: local, mqtt, nats, redis."
    )
