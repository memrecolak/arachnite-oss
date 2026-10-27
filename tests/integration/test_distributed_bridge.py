"""
End-to-end SignalBus↔transport bridge across AgentNodes (spec §10).

Builds AgentNodes and verifies that a signal published on one agent's bus is
delivered to a subscriber on another agent's bus — exercising the real wiring
path: AgentNode.start() connects the transport, calls bus.start_transport(),
and starts the runtime. Besides the hand-shared LocalTransport case, the
review-6 tests drive the same path through MeshRuntime (which shares one
LocalTransport among its local agents) and through NATSTransport over an
in-memory broker fake, so the wire encode/decode is exercised too.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from arachnite.bus import SignalBus
from arachnite.context import ContextNode
from arachnite.distributed.agent_node import AgentNode
from arachnite.distributed.manifest import DeploymentManifest
from arachnite.distributed.mesh import MeshRuntime
from arachnite.logging import BaseLogSink, LogEvent
from arachnite.models import Context, Proposal, Result, Signal, StateUpdateSignal
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseReflexInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from arachnite.transport.local import LocalTransport
from tests.conftest import R6SignalRecorder

_TWO_AGENT_MANIFEST = {
    "mesh": {"transport_default": "local"},
    "agents": [
        {
            "id": "agent-A",
            "transport": "local",
            "tick_rate_hz": 10.0,
            "nodes": {"sense": [{"kind": "tests.conftest.ConstantSenseNode"}]},
        },
        {
            "id": "agent-B",
            "transport": "local",
            "tick_rate_hz": 10.0,
            "nodes": {"action": [{"kind": "tests.conftest.RecordingAction"}]},
        },
    ],
}


class _Collector:
    def __init__(self) -> None:
        self.seen: list[Signal] = []

    async def __call__(self, signal: Signal) -> None:
        self.seen.append(signal)


class _R6LaneReflex(BaseReflexInstinctNode):
    node_id = "r6.lane_reflex"
    priority = 250
    trigger_on_signals = ["proximity"]

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.evaluations = 0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        self.evaluations += 1
        if any(s.kind == "proximity" and s.value < 0.05 for s in ctx.signals):
            return Proposal(instinct_id=self.node_id, action_id="r6.lane_action",
                            priority=self.priority, urgency=1.0)
        return None


class _R6LaneAction(BaseActionNode):
    node_id = "r6.lane_action"

    def __init__(self, bus: SignalBus) -> None:
        super().__init__(bus)
        self.calls: list[Proposal] = []

    async def execute(self, proposal: Proposal) -> Result:
        self.calls.append(proposal)
        return Result(action_id=self.node_id, success=True)


async def test_signal_published_on_a_reaches_subscriber_on_b() -> None:
    manifest = DeploymentManifest.from_dict(_TWO_AGENT_MANIFEST)
    shared = LocalTransport(agent_node_id="mesh")
    agent_a = AgentNode.from_manifest(manifest, "agent-A", transport=shared)
    agent_b = AgentNode.from_manifest(manifest, "agent-B", transport=shared)

    received: list[Signal] = []
    on_a = _Collector()

    async def collector(signal: Signal) -> None:
        received.append(signal)

    # Subscribe on B's bus before start so start_transport() bridges the kind.
    agent_b.runtime.bus.subscribe("cross_agent", collector)
    agent_a.runtime.bus.subscribe("cross_agent", on_a)

    await agent_a.start()
    await agent_b.start()
    try:
        await agent_a.runtime.bus.publish(
            Signal(
                source="agent-A", kind="cross_agent", value=7,
                confidence=1.0, timestamp=time.monotonic(),
            )
        )
        # LocalTransport delivery is synchronous within publish().
        assert len(received) == 1
        assert received[0].value == 7
        # A's own subscriber got the local delivery only — its message did not
        # come back to it as an echo through the shared transport.
        assert len(on_a.seen) == 1
    finally:
        await agent_a.stop()
        await agent_b.stop()


# ── Review 6 (distributed) ────────────────────────────────────────────────────

_R6 = "tests.conftest."


def _edge_cloud(transport: str) -> dict[str, Any]:
    """pi: a temperature sensor (+ recorder); cloud: a recorder instinct only."""
    return {"agents": [
        {"id": "pi", "transport": transport, "tick_rate_hz": 100.0,
         "nodes": {"sense": [f"{_R6}R6TempSense"],
                   "instinct": [f"{_R6}R6SignalRecorder"]}},
        {"id": "cloud", "transport": transport, "tick_rate_hz": 100.0,
         "nodes": {"instinct": [f"{_R6}R6SignalRecorder"]}},
    ]}


def _recorder(agent: AgentNode) -> R6SignalRecorder:
    nodes = [n for n in agent.runtime._instinct_master.normal_nodes
             if isinstance(n, R6SignalRecorder)]
    assert len(nodes) == 1
    return nodes[0]


async def _run_until(pred: Any, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not pred() and time.monotonic() < deadline:
        await asyncio.sleep(0.01)


class _CaptureSink(BaseLogSink):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[LogEvent] = []

    async def emit(self, event: LogEvent) -> None:
        self.events.append(event)


class TestR6RemoteSignalsReachContext:
    """R6-F3: sense signals from another AgentNode enter this agent's ctx.signals."""

    async def _check_edge_cloud(self, mesh: MeshRuntime) -> None:
        await mesh.start()
        try:
            cloud, pi = _recorder(mesh.agent("cloud")), _recorder(mesh.agent("pi"))
            await _run_until(lambda: any(("r6.temp", "temperature") in t for t in cloud.seen))
        finally:
            await mesh.stop()
        assert any(("r6.temp", "temperature") in tick for tick in cloud.seen)
        # The pi's own reading is in its ctx exactly once per tick: the remote
        # path never re-injects local signals.
        assert pi.seen and all(
            tick.count(("r6.temp", "temperature")) == 1 for tick in pi.seen
        )

    async def test_edge_sensor_reaches_cloud_instinct_local_mesh(self) -> None:
        """R6-F3: in-process mesh (shared LocalTransport)"""
        await self._check_edge_cloud(MeshRuntime(DeploymentManifest.from_dict(
            _edge_cloud("local"))))

    async def test_edge_sensor_reaches_cloud_instinct_over_nats(
        self, fake_nats_server: Any,
    ) -> None:
        """R6-F3: through a network transport (real wire encode/decode)"""
        await self._check_edge_cloud(MeshRuntime(DeploymentManifest.from_dict(
            _edge_cloud("nats"))))

    async def test_remote_buffer_is_bounded_and_warns(self) -> None:
        """R6-F3: >1024 remote signals between ticks keep the newest 1024, with a warning"""
        sink = _CaptureSink()
        manifest = DeploymentManifest.from_dict(_edge_cloud("local"))
        cloud = AgentNode.from_manifest(manifest, "cloud", log_sinks=[sink])
        for i in range(1100):
            cloud.runtime.inject_remote_signal(Signal(
                source="far", kind="temperature", value=i, confidence=1.0,
                timestamp=float(i)))
        await cloud.runtime.tick()
        await asyncio.sleep(0.01)
        seen = _recorder(cloud).seen[-1]
        assert len(seen) == 1024
        assert any("remote signals dropped" in e.message for e in sink.events)
        await cloud.runtime.tick()
        assert _recorder(cloud).seen[-1] == []      # drained, not re-injected

    async def test_remote_state_update_does_not_write_local_state(self) -> None:
        """R6-F3/F12-07: a peer's StateUpdateSignal does not land in this agent's ctx.state"""
        manifest = DeploymentManifest.from_dict(_edge_cloud("local"))
        shared = LocalTransport(agent_node_id="mesh")
        pi = AgentNode.from_manifest(manifest, "pi", transport=shared)
        cloud = AgentNode.from_manifest(manifest, "cloud", transport=shared)
        await shared.connect()
        await pi.runtime.bus.start_transport()
        await cloud.runtime.bus.start_transport()
        await pi.runtime.bus.publish(StateUpdateSignal(
            source="r6.recorder", kind="state_update", value=None, confidence=1.0,
            timestamp=time.monotonic(), key="pi_private_mode", state_value="safe"))
        await cloud.runtime.tick()
        await pi.runtime.tick()
        assert "pi_private_mode" not in cloud.runtime.context.snapshot().state
        assert pi.runtime.context.snapshot().state["pi_private_mode"] == "safe"


class TestR6BridgeCoverage:
    """R6-F20: the bridge exercised end to end, beyond a hand-shared LocalTransport."""

    async def test_signal_crosses_over_nats_and_is_not_echoed(
        self, fake_nats_server: Any,
    ) -> None:
        """R6-F20: AgentNode -> NATS encode/decode -> AgentNode, publisher sees one copy"""
        manifest = DeploymentManifest.from_dict(_TWO_AGENT_MANIFEST)
        from arachnite.transport.nats import NATSTransport
        agent_a = AgentNode.from_manifest(manifest, "agent-A",
                                          transport=NATSTransport(agent_node_id="agent-A"))
        agent_b = AgentNode.from_manifest(manifest, "agent-B",
                                          transport=NATSTransport(agent_node_id="agent-B"))
        on_a, on_b = _Collector(), _Collector()
        agent_a.runtime.bus.subscribe("cross_agent", on_a)
        agent_b.runtime.bus.subscribe("cross_agent", on_b)
        await agent_a.start()
        await agent_b.start()
        try:
            await agent_a.runtime.bus.publish(Signal(
                source="agent-A", kind="cross_agent", value={"n": 7, 0: "zero"},
                confidence=0.5, timestamp=123.0, metadata={"tag": "x"}))
        finally:
            await agent_a.stop()
            await agent_b.stop()
        assert len(on_a.seen) == 1                 # local delivery, no echo
        assert len(on_b.seen) == 1
        got = on_b.seen[0]
        assert got is not on_a.seen[0]             # really went through the wire
        assert (got.value, got.confidence, got.timestamp) == ({"n": 7, 0: "zero"}, 0.5, 123.0)
        assert got.metadata["tag"] == "x"
        assert got.metadata["__origin_agent__"] == "agent-A"

    async def test_event_lane_reflex_sees_remote_signals(self) -> None:
        """R6-F20 (guard for F3): a reflex lane subscribed on the bus fires on a remote signal"""
        shared = LocalTransport(agent_node_id="mesh")
        far_bus = SignalBus(transport=shared, agent_node_id="far")
        bus = SignalBus(transport=shared, agent_node_id="near")
        reflex, action = _R6LaneReflex(bus=bus), _R6LaneAction(bus=bus)
        instinct_master = InstinctMasterNode(bus=bus)
        instinct_master.register(reflex)
        action_master = ActionMasterNode(bus=bus)
        action_master.register(action)
        rt = ArachniteRuntime(
            sense_master=SenseMasterNode(bus=bus), context=ContextNode(),
            instinct_master=instinct_master,
            decision_master=DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
            action_master=action_master, bus=bus, tick_rate_hz=1.0, reflex_mode="event",
        )
        near = AgentNode("near", rt, shared)
        await shared.connect()
        await far_bus.start_transport()
        await near.start()
        try:
            await far_bus.publish(Signal(source="far.prox", kind="proximity", value=0.01,
                                         confidence=1.0, timestamp=time.perf_counter()))
            await _run_until(lambda: bool(action.calls), timeout=1.0)
        finally:
            await near.stop()
        assert reflex.evaluations >= 1
        assert len(action.calls) == 1
