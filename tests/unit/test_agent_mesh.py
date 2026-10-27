"""Unit tests for AgentNode and MeshRuntime."""

from __future__ import annotations

import asyncio
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from arachnite.distributed.agent_node import AgentNode, _build_transport
from arachnite.distributed.manifest import DeploymentManifest
from arachnite.distributed.mesh import MeshRuntime
from arachnite.exceptions import TransportConnectionError
from arachnite.health import HealthMonitor
from arachnite.models import NodeState, Signal
from arachnite.transport.local import LocalTransport

# ── Shared manifest fixtures ──────────────────────────────────────────────────

_INSTINCT_MANIFEST = {
    "mesh": {"transport_default": "local"},
    "agents": [
        {
            "id":           "agent-I",
            "transport":    "local",
            "tick_rate_hz": 10.0,
            "nodes": {
                "sense":   [{"kind": "tests.conftest.ConstantSenseNode"}],
                "instinct": [{"kind": "tests.conftest.ThresholdInstinct"}],
                "action":  [{"kind": "tests.conftest.RecordingAction"}],
            },
        }
    ],
}

_DECISION_MANIFEST = {
    "mesh": {"transport_default": "local"},
    "agents": [
        {
            "id":           "agent-D",
            "transport":    "local",
            "tick_rate_hz": 10.0,
            "nodes": {
                "sense":    [{"kind": "tests.conftest.ConstantSenseNode"}],
                "decision": [{"kind": "arachnite.nodes.decision.WeightedDecisionNode"}],
                "action":   [{"kind": "tests.conftest.RecordingAction"}],
            },
        }
    ],
}

_ONE_AGENT_MANIFEST = {
    "mesh": {"transport_default": "local"},
    "agents": [
        {
            "id":           "edge-01",
            "transport":    "local",
            "tick_rate_hz": 10.0,
            "nodes": {
                "sense":  [{"kind": "tests.conftest.ConstantSenseNode"}],
                "action": [{"kind": "tests.conftest.RecordingAction"}],
            },
        }
    ],
}

_TWO_AGENT_MANIFEST = {
    "mesh": {"transport_default": "local"},
    "agents": [
        {
            "id":           "agent-A",
            "transport":    "local",
            "tick_rate_hz": 10.0,
            "nodes": {
                "sense": [{"kind": "tests.conftest.ConstantSenseNode"}],
            },
        },
        {
            "id":           "agent-B",
            "transport":    "local",
            "tick_rate_hz": 10.0,
            "nodes": {
                "action": [{"kind": "tests.conftest.RecordingAction"}],
            },
        },
    ],
}


# ── AgentNode ─────────────────────────────────────────────────────────────────

class TestAgentNodeFromManifest:
    def test_from_manifest_sets_node_id(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        assert agent.node_id == "edge-01"

    def test_from_manifest_uses_local_transport(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        assert isinstance(agent.transport, LocalTransport)

    def test_from_manifest_transport_override(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        override = LocalTransport(agent_node_id="edge-01")
        agent = AgentNode.from_manifest(manifest, "edge-01", transport=override)
        assert agent.transport is override

    def test_from_manifest_unknown_agent_raises(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        with pytest.raises(KeyError):
            AgentNode.from_manifest(manifest, "nonexistent")

    def test_tags_and_description_defaults(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        assert agent.tags == []
        assert agent.description == ""

    def test_tags_populated_from_manifest(self) -> None:
        m = {
            "mesh": {},
            "agents": [{
                "id": "pi", "transport": "local", "tick_rate_hz": 5.0,
                "tags": ["edge", "arm64"],
                "nodes": {},
            }],
        }
        manifest = DeploymentManifest.from_dict(m)
        agent = AgentNode.from_manifest(manifest, "pi")
        assert "edge" in agent.tags
        assert "arm64" in agent.tags


class TestAgentNodeLifecycle:
    def test_not_running_before_start(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        assert not agent.is_running

    @pytest.mark.asyncio
    async def test_start_makes_running(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        await agent.start()
        try:
            assert agent.is_running
        finally:
            await agent.stop()

    @pytest.mark.asyncio
    async def test_stop_makes_not_running(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        await agent.start()
        await agent.stop()
        assert not agent.is_running

    @pytest.mark.asyncio
    async def test_health_returns_health_monitor(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        assert isinstance(agent.health, HealthMonitor)

    @pytest.mark.asyncio
    async def test_emergency_stop_halts_runtime(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        await agent.start()
        await agent.emergency_stop()
        assert not agent.is_running

    def test_repr_contains_node_id(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "edge-01")
        assert "edge-01" in repr(agent)


# ── MeshRuntime ───────────────────────────────────────────────────────────────

class TestMeshRuntimeStart:
    @pytest.mark.asyncio
    async def test_start_sets_started_flag(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        assert not mesh.started
        await mesh.start()
        try:
            assert mesh.started
        finally:
            await mesh.stop()

    @pytest.mark.asyncio
    async def test_stop_clears_started_flag(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        await mesh.stop()
        assert not mesh.started

    @pytest.mark.asyncio
    async def test_double_start_is_idempotent(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        await mesh.start()   # second call must not raise or duplicate agents
        try:
            assert len(mesh.agents) == 1
        finally:
            await mesh.stop()

    @pytest.mark.asyncio
    async def test_all_agents_are_running_after_start(self) -> None:
        manifest = DeploymentManifest.from_dict(_TWO_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        try:
            assert all(a.is_running for a in mesh.agents.values())
        finally:
            await mesh.stop()

    @pytest.mark.asyncio
    async def test_all_agents_stopped_after_stop(self) -> None:
        manifest = DeploymentManifest.from_dict(_TWO_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        await mesh.stop()
        assert all(not a.is_running for a in mesh.agents.values())


class TestMeshRuntimeAccessors:
    @pytest.mark.asyncio
    async def test_agent_returns_correct_node(self) -> None:
        manifest = DeploymentManifest.from_dict(_TWO_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        try:
            a = mesh.agent("agent-A")
            assert a.node_id == "agent-A"
        finally:
            await mesh.stop()

    @pytest.mark.asyncio
    async def test_agent_unknown_raises_key_error(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        try:
            with pytest.raises(KeyError):
                mesh.agent("nonexistent")
        finally:
            await mesh.stop()

    @pytest.mark.asyncio
    async def test_agents_property_contains_all_ids(self) -> None:
        manifest = DeploymentManifest.from_dict(_TWO_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        try:
            assert "agent-A" in mesh.agents
            assert "agent-B" in mesh.agents
        finally:
            await mesh.stop()

    @pytest.mark.asyncio
    async def test_mesh_health_all_true_when_running(self) -> None:
        manifest = DeploymentManifest.from_dict(_TWO_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        try:
            health = mesh.mesh_health()
            assert set(health.keys()) == {"agent-A", "agent-B"}
            assert all(health.values())
        finally:
            await mesh.stop()

    @pytest.mark.asyncio
    async def test_nodes_in_state_empty_before_any_fault(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        try:
            result = mesh.nodes_in_state(NodeState.FAULTED)
            assert result == {}
        finally:
            await mesh.stop()

    def test_repr_before_start(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        r = repr(mesh)
        assert "MeshRuntime" in r
        assert "started=False" in r

    @pytest.mark.asyncio
    async def test_nodes_in_state_returns_faulted_nodes(self) -> None:
        manifest = DeploymentManifest.from_dict(_ONE_AGENT_MANIFEST)
        mesh = MeshRuntime(manifest)
        await mesh.start()
        try:
            agent = mesh.agent("edge-01")
            # Fault a node so nodes_in_state returns non-empty (line 109)
            sv = agent.runtime._supervisors[0]
            node_id = next(iter(sv.all_states()))
            await sv.on_fault(node_id, RuntimeError("test"))
            result = mesh.nodes_in_state(NodeState.FAULTED)
            assert "edge-01" in result
        finally:
            await mesh.stop()


# ── AgentNode.from_manifest — instinct / decision sections ───────────────────

class TestAgentNodeFromManifestSections:
    def test_instinct_section_registers_instinct_node(self) -> None:
        manifest = DeploymentManifest.from_dict(_INSTINCT_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "agent-I")
        # Just verify the agent built without error
        assert agent.node_id == "agent-I"

    def test_decision_section_sets_strategy(self) -> None:
        from arachnite.nodes.decision import WeightedDecisionNode
        manifest = DeploymentManifest.from_dict(_DECISION_MANIFEST)
        agent = AgentNode.from_manifest(manifest, "agent-D")
        assert isinstance(agent.runtime._decision_master.strategy, WeightedDecisionNode)


# ── _build_transport — optional backends + unknown ────────────────────────────

class TestBuildTransport:
    def test_local_transport(self) -> None:
        t = _build_transport("local", "a1", {})
        assert isinstance(t, LocalTransport)

    def test_empty_string_is_local(self) -> None:
        t = _build_transport("", "a1", {})
        assert isinstance(t, LocalTransport)

    def test_unknown_kind_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown transport kind"):
            _build_transport("websocket", "a1", {})

    def test_mqtt_transport_constructed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_mod = MagicMock()
        mock_transport = MagicMock()
        mock_mod.MQTTTransport.return_value = mock_transport
        monkeypatch.setitem(sys.modules, "arachnite.transport.mqtt", mock_mod)
        result = _build_transport("mqtt", "a1", {"broker_host": "broker"})
        assert result is mock_transport
        mock_mod.MQTTTransport.assert_called_once()

    def test_nats_transport_constructed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_mod = MagicMock()
        mock_transport = MagicMock()
        mock_mod.NATSTransport.return_value = mock_transport
        monkeypatch.setitem(sys.modules, "arachnite.transport.nats", mock_mod)
        result = _build_transport("nats", "a1", {})
        assert result is mock_transport

    def test_redis_transport_constructed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_mod = MagicMock()
        mock_transport = MagicMock()
        mock_mod.RedisTransport.return_value = mock_transport
        monkeypatch.setitem(sys.modules, "arachnite.transport.redis", mock_mod)
        result = _build_transport("redis", "a1", {})
        assert result is mock_transport


# ── Review 6 (distributed): mesh wiring, partial starts, mesh health ─────────

_R6_TWO = {"agents": [
    {"id": "A", "tick_rate_hz": 50.0, "nodes": {"sense": ["tests.conftest.R6TempSense"]}},
    {"id": "B", "tick_rate_hz": 50.0, "nodes": {"instinct": ["tests.conftest.R6SignalRecorder"]}},
]}


class _R6FlakyTransport(LocalTransport):
    """Fails its first connect() (a broker that is briefly unreachable)."""

    def __init__(self, agent_node_id: str) -> None:
        super().__init__(agent_node_id=agent_node_id)
        self.failures = 1

    async def connect(self) -> None:
        if self.failures:
            self.failures -= 1
            raise TransportConnectionError("Flaky", "broker unreachable")
        await super().connect()


class TestR6MeshRuntime:
    """R6-F12-07/16/11: local agents connected, clean partial starts, mesh health."""

    async def test_local_agents_of_a_mesh_exchange_signals(self) -> None:
        """R6-F12-07: 'transport: local' agents of one MeshRuntime share a transport"""
        mesh = MeshRuntime(DeploymentManifest.from_dict(_R6_TWO))
        got: list[Signal] = []

        async def collect(signal: Signal) -> None:
            got.append(signal)

        await mesh.start()
        try:
            mesh.agent("B").runtime.bus.subscribe("cross", collect)
            await asyncio.sleep(0.01)
            await mesh.agent("A").runtime.bus.publish(Signal(
                source="A", kind="cross", value=1, confidence=1.0, timestamp=0.0))
        finally:
            await mesh.stop()
        assert [s.value for s in got] == [1]

    async def test_failed_start_stops_the_agents_it_started(self) -> None:
        """R6-F12-16: a partial start leaves nothing running; a retry starts cleanly"""
        flaky = _R6FlakyTransport("B")
        mesh = MeshRuntime(DeploymentManifest.from_dict(_R6_TWO), transports={"B": flaky})
        with pytest.raises(TransportConnectionError):
            await mesh.start()
        await asyncio.sleep(0.05)
        assert not mesh.started
        assert [aid for aid, a in mesh.agents.items() if a.is_running] == []
        await mesh.start()                     # retry after the broker recovers
        assert mesh.started and mesh.agent("A").is_running
        await mesh.stop()
        assert not mesh.agent("A").is_running

    async def test_partial_start_does_not_orphan_running_agents(self) -> None:
        """R6-F12-16: agents of the failed attempt are stopped, not left running"""
        flaky = _R6FlakyTransport("B")
        mesh = MeshRuntime(DeploymentManifest.from_dict(_R6_TWO), transports={"B": flaky})
        built: list[AgentNode] = []
        real_from_manifest = AgentNode.from_manifest

        def spy(*args: Any, **kwargs: Any) -> AgentNode:
            agent = real_from_manifest(*args, **kwargs)
            built.append(agent)
            return agent

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(AgentNode, "from_manifest", staticmethod(spy))
            with pytest.raises(TransportConnectionError):
                await mesh.start()
            first_generation = list(built)
            await mesh.start()
            await mesh.stop()
        await asyncio.sleep(0.05)
        assert first_generation
        assert [a.node_id for a in first_generation if a.is_running] == []

    async def test_remote_node_fault_updates_mesh_health(self, fake_nats_server: Any) -> None:
        """R6-F12-11: a fault on agent A shows up in agent B's mesh health view"""
        manifest = DeploymentManifest.from_dict({"agents": [
            {"id": "A", "transport": "nats", "nodes": {"sense": ["tests.conftest.R6TempSense"]}},
            {"id": "B", "transport": "nats",
             "nodes": {"instinct": ["tests.conftest.R6SignalRecorder"]}},
        ]})
        mesh = MeshRuntime(manifest)
        await mesh.start()
        try:
            a, b = mesh.agent("A"), mesh.agent("B")
            assert b.health.mesh_healthy()
            sv = a.runtime._supervisors[0]
            await sv.on_fault("r6.temp", RuntimeError("i2c bus hung"))
            await asyncio.sleep(0.01)
            remote = {k: dict(v) for k, v in b.health.remote_states().items()}
            healthy = b.health.mesh_healthy()
            await sv.cancel_restart_tasks()
        finally:
            await mesh.stop()
        assert remote["A"]["r6.temp"] in (NodeState.FAULTED, NodeState.RESTARTING)
        assert healthy is False
