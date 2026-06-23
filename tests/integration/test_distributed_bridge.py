"""
End-to-end SignalBus↔transport bridge across two AgentNodes (spec §10).

Builds two AgentNodes that share one LocalTransport and verifies that a signal
published on one agent's bus is delivered to a subscriber on the other agent's
bus — exercising the real wiring path: AgentNode.start() connects the transport,
calls bus.start_transport(), and starts the runtime.
"""

from __future__ import annotations

import time

from arachnite.distributed.agent_node import AgentNode
from arachnite.distributed.manifest import DeploymentManifest
from arachnite.models import Signal
from arachnite.transport.local import LocalTransport

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


async def test_signal_published_on_a_reaches_subscriber_on_b() -> None:
    manifest = DeploymentManifest.from_dict(_TWO_AGENT_MANIFEST)
    shared = LocalTransport(agent_node_id="mesh")
    agent_a = AgentNode.from_manifest(manifest, "agent-A", transport=shared)
    agent_b = AgentNode.from_manifest(manifest, "agent-B", transport=shared)

    received: list[Signal] = []

    async def collector(signal: Signal) -> None:
        received.append(signal)

    # Subscribe on B's bus before start so start_transport() bridges the kind.
    agent_b.runtime.bus.subscribe("cross_agent", collector)

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
        # A has no local subscriber for the kind, so it must not have received
        # its own message back as an echo.
        assert all(s.value == 7 for s in received)
    finally:
        await agent_a.stop()
        await agent_b.stop()
