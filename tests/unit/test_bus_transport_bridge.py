"""
SignalBus ↔ transport bridge (spec §10).

A SignalBus constructed with a transport forwards local publishes onto the
transport and dispatches transport-delivered signals to local subscribers, so
nodes keep calling publish()/subscribe() unchanged while signals cross between
AgentNodes. These tests pin:

  - transport=None is unchanged (pure in-process bus),
  - publish() forwards to the transport,
  - a signal published on bus A is delivered to a local subscriber on bus B
    (sharing one LocalTransport),
  - the publisher does NOT receive its own message back (echo dropped by origin),
  - inbound signals are not re-forwarded (no delivery loop / no duplication),
  - kinds subscribed before AND after start_transport() are bridged,
  - local-subscriber error semantics (SignalBusError) are preserved.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from arachnite.bus import ORIGIN_AGENT_KEY, SignalBus
from arachnite.exceptions import SignalBusError
from arachnite.models import Signal
from arachnite.transport.local import LocalTransport


def _sig(kind: str = "k", value: object = 1) -> Signal:
    return Signal(
        source="tester", kind=kind, value=value,
        confidence=1.0, timestamp=time.monotonic(),
    )


# ── transport=None: no behaviour change ───────────────────────────────────────

class TestNoTransport:
    async def test_local_only_delivery(self) -> None:
        bus = SignalBus()
        cb = _Collector()
        bus.subscribe("k", cb)
        await bus.publish(_sig())
        assert len(cb.seen) == 1

    async def test_subscriber_error_still_raises(self) -> None:
        bus = SignalBus()

        async def boom(_s: Signal) -> None:
            raise ValueError("nope")

        bus.subscribe("k", boom)
        with pytest.raises(SignalBusError):
            await bus.publish(_sig())


# ── Cross-bus delivery over a shared LocalTransport ───────────────────────────

class _Collector:
    def __init__(self) -> None:
        self.seen: list[Signal] = []

    async def __call__(self, signal: Signal) -> None:
        self.seen.append(signal)


async def _two_buses() -> tuple[SignalBus, SignalBus, LocalTransport, _Collector, _Collector]:
    transport = LocalTransport(agent_node_id="shared")
    await transport.connect()
    bus_a = SignalBus(transport=transport, agent_node_id="A")
    bus_b = SignalBus(transport=transport, agent_node_id="B")
    a_cb, b_cb = _Collector(), _Collector()
    bus_a.subscribe("k", a_cb)
    bus_b.subscribe("k", b_cb)
    await bus_a.start_transport()
    await bus_b.start_transport()
    return bus_a, bus_b, transport, a_cb, b_cb


class TestCrossBusDelivery:
    async def test_publish_forwards_to_transport(self) -> None:
        transport = LocalTransport()
        await transport.connect()
        bus = SignalBus(transport=transport, agent_node_id="A")
        relay = _Collector()
        await transport.subscribe("k", relay)  # stand-in remote subscriber
        await bus.publish(_sig())
        assert len(relay.seen) == 1
        assert relay.seen[0].metadata.get(ORIGIN_AGENT_KEY) == "A"

    async def test_signal_crosses_to_other_bus(self) -> None:
        bus_a, _bus_b, _t, a_cb, b_cb = await _two_buses()
        await bus_a.publish(_sig(value=42))
        assert len(b_cb.seen) == 1
        assert b_cb.seen[0].value == 42

    async def test_publisher_does_not_receive_own_echo(self) -> None:
        bus_a, _bus_b, _t, a_cb, b_cb = await _two_buses()
        await bus_a.publish(_sig())
        # a_cb fires exactly once (local dispatch); the echo back through the
        # shared transport is dropped by origin, not dispatched a second time.
        assert len(a_cb.seen) == 1
        assert len(b_cb.seen) == 1

    async def test_no_duplication_with_three_buses(self) -> None:
        transport = LocalTransport()
        await transport.connect()
        buses = [SignalBus(transport=transport, agent_node_id=n) for n in ("A", "B", "C")]
        cbs = [_Collector() for _ in buses]
        for bus, cb in zip(buses, cbs, strict=True):
            bus.subscribe("k", cb)
            await bus.start_transport()
        await buses[0].publish(_sig())
        # Publisher: one local dispatch, echo dropped. Others: one each.
        assert [len(cb.seen) for cb in cbs] == [1, 1, 1]

    async def test_inbound_is_not_reforwarded(self) -> None:
        # If inbound signals were re-forwarded, the forward count would exceed
        # the number of publish() calls. Track transport.publish invocations.
        bus_a, _bus_b, transport, _a, _b = await _two_buses()
        count = {"n": 0}
        orig = transport.publish

        async def counting_publish(sig: Signal) -> None:
            count["n"] += 1
            await orig(sig)

        transport.publish = counting_publish  # type: ignore[method-assign]
        await bus_a.publish(_sig())
        assert count["n"] == 1  # exactly one forward; receivers do not re-publish


# ── Subscription timing ────────────────────────────────────────────────────────

class TestSubscriptionTiming:
    async def test_kind_subscribed_after_start_is_bridged(self) -> None:
        transport = LocalTransport()
        await transport.connect()
        bus_a = SignalBus(transport=transport, agent_node_id="A")
        bus_b = SignalBus(transport=transport, agent_node_id="B")
        await bus_a.start_transport()
        await bus_b.start_transport()
        # Subscribe a new kind *after* start_transport on both buses.
        b_cb = _Collector()
        bus_b.subscribe("late", b_cb)
        await asyncio.sleep(0)  # let the scheduled transport subscription run
        await bus_a.publish(_sig(kind="late", value="hi"))
        assert len(b_cb.seen) == 1
        assert b_cb.seen[0].value == "hi"
