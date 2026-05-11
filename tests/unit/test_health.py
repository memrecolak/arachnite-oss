"""Unit tests for HealthMonitor — local and mesh health."""

from __future__ import annotations

import pytest

from arachnite import SignalBus
from arachnite.health import HealthMonitor
from arachnite.models import NodeState, RemoteNodeState, RestartPolicy
from arachnite.nodes.base import BaseNode
from arachnite.supervisor import NodeSupervisor


class DummyNode(BaseNode):
    node_id = "DummyNode"


@pytest.fixture
def bus() -> SignalBus:
    return SignalBus()


def make_supervisor(bus: SignalBus, supervisor_id: str = "sv") -> NodeSupervisor:
    return NodeSupervisor(
        bus             = bus,
        supervisor_id   = supervisor_id,
        restart_policy  = RestartPolicy.NEVER,
        restart_delay_s = 0.0,
    )


class TestSystemHealthy:
    def test_empty_supervisors_is_healthy(self, bus: SignalBus) -> None:
        hm = HealthMonitor([])
        assert hm.system_healthy()

    def test_healthy_when_all_nodes_running(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = DummyNode(bus=bus)
        sv.track(node)
        hm = HealthMonitor([sv])
        assert hm.system_healthy()

    @pytest.mark.asyncio
    async def test_unhealthy_when_node_faulted(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = DummyNode(bus=bus)
        sv.track(node)
        await sv.on_fault("DummyNode", RuntimeError("boom"))
        hm = HealthMonitor([sv])
        assert not hm.system_healthy()

    @pytest.mark.asyncio
    async def test_unhealthy_when_node_dead(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, supervisor_id="sv2")
        node = DummyNode(bus=bus)
        sv.track(node)
        await sv.on_fault("DummyNode", RuntimeError("dead"))
        hm = HealthMonitor([sv])
        assert not hm.system_healthy()


class TestNodesInState:
    @pytest.mark.asyncio
    async def test_nodes_in_state_finds_running(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = DummyNode(bus=bus)
        sv.track(node)
        await sv.mark_running("DummyNode")
        hm = HealthMonitor([sv])
        assert "DummyNode" in hm.nodes_in_state(NodeState.RUNNING)

    @pytest.mark.asyncio
    async def test_nodes_in_state_empty_when_none_match(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = DummyNode(bus=bus)
        sv.track(node)
        await sv.mark_running("DummyNode")
        hm = HealthMonitor([sv])
        assert hm.nodes_in_state(NodeState.DEAD) == []


class TestMeshHealthy:
    def test_mesh_healthy_no_remotes(self, bus: SignalBus) -> None:
        hm = HealthMonitor([])
        assert hm.mesh_healthy()

    def test_mesh_healthy_with_healthy_remote(self, bus: SignalBus) -> None:
        hm = HealthMonitor([])
        hm.update_remote(RemoteNodeState(
            agent_node_id = "edge-01",
            node_id       = "SensorA",
            state         = NodeState.RUNNING,
            timestamp     = 0.0,
        ))
        assert hm.mesh_healthy()

    def test_mesh_unhealthy_when_remote_faulted(self, bus: SignalBus) -> None:
        hm = HealthMonitor([])
        hm.update_remote(RemoteNodeState(
            agent_node_id = "edge-01",
            node_id       = "SensorA",
            state         = NodeState.FAULTED,
            timestamp     = 0.0,
        ))
        assert not hm.mesh_healthy()

    def test_mesh_unhealthy_when_remote_dead(self, bus: SignalBus) -> None:
        hm = HealthMonitor([])
        hm.update_remote(RemoteNodeState(
            agent_node_id = "edge-02",
            node_id       = "ActuatorB",
            state         = NodeState.DEAD,
            timestamp     = 0.0,
        ))
        assert not hm.mesh_healthy()

    @pytest.mark.asyncio
    async def test_mesh_unhealthy_when_local_faulted(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = DummyNode(bus=bus)
        sv.track(node)
        await sv.on_fault("DummyNode", RuntimeError("x"))
        hm = HealthMonitor([sv])
        hm.update_remote(RemoteNodeState(
            agent_node_id = "remote-01",
            node_id       = "RemoteNode",
            state         = NodeState.RUNNING,
            timestamp     = 0.0,
        ))
        assert not hm.mesh_healthy()

    def test_update_remote_overwrites_previous_state(self, bus: SignalBus) -> None:
        hm = HealthMonitor([])
        hm.update_remote(RemoteNodeState(
            agent_node_id="a1", node_id="N1", state=NodeState.RUNNING, timestamp=0.0
        ))
        hm.update_remote(RemoteNodeState(
            agent_node_id="a1", node_id="N1", state=NodeState.FAULTED, timestamp=0.0
        ))
        assert hm.remote_states()["a1"]["N1"] == NodeState.FAULTED

    def test_report_returns_supervisor_states(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = DummyNode(bus=bus)
        sv.track(node)
        hm = HealthMonitor([sv])
        report = hm.report()
        assert "0" in report
        assert "DummyNode" in report["0"]

    def test_repr(self, bus: SignalBus) -> None:
        hm = HealthMonitor([make_supervisor(bus)])
        r  = repr(hm)
        assert "HealthMonitor" in r
        assert "supervisors=1" in r

    def test_remote_states_returns_snapshot(self, bus: SignalBus) -> None:
        hm = HealthMonitor([])
        hm.update_remote(RemoteNodeState(
            agent_node_id="a1", node_id="N1", state=NodeState.STARTING, timestamp=0.0
        ))
        hm.update_remote(RemoteNodeState(
            agent_node_id="a2", node_id="N2", state=NodeState.RUNNING, timestamp=0.0
        ))
        states = hm.remote_states()
        assert "a1" in states and "a2" in states
        assert states["a1"]["N1"] == NodeState.STARTING
