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
from typing import Any

import pytest

from arachnite.bus import ORIGIN_AGENT_KEY, SignalBus
from arachnite.exceptions import SignalBusError
from arachnite.models import Signal
from arachnite.transport.local import LocalTransport
from tests.conftest import settle


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


# ── Egress allowlist (wire_kinds) ─────────────────────────────────────────────

class TestWireKindsAllowlist:
    async def test_default_forwards_every_kind(self) -> None:
        transport = LocalTransport()
        await transport.connect()
        bus = SignalBus(transport=transport, agent_node_id="A")
        assert bus.wire_kinds() is None
        relay = _Collector()
        await transport.subscribe("secret", relay)
        await bus.publish(_sig(kind="secret"))
        assert len(relay.seen) == 1  # no allowlist => everything egresses

    async def test_allowlisted_kind_egresses(self) -> None:
        transport = LocalTransport()
        await transport.connect()
        bus = SignalBus(transport=transport, agent_node_id="A", wire_kinds={"pub"})
        assert bus.wire_kinds() == {"pub"}
        relay = _Collector()
        await transport.subscribe("pub", relay)
        await bus.publish(_sig(kind="pub"))
        assert len(relay.seen) == 1

    async def test_non_allowlisted_kind_stays_local(self) -> None:
        transport = LocalTransport()
        await transport.connect()
        bus = SignalBus(transport=transport, agent_node_id="A", wire_kinds={"pub"})
        wire = _Collector()
        local = _Collector()
        await transport.subscribe("secret", wire)  # would fire if it egressed
        bus.subscribe("secret", local)
        await bus.publish(_sig(kind="secret", value=7))
        assert len(wire.seen) == 0   # never reached the transport
        assert len(local.seen) == 1  # but local delivery is unaffected
        assert local.seen[0].value == 7

    async def test_wildcard_allowlist_forwards_everything(self) -> None:
        transport = LocalTransport()
        await transport.connect()
        bus = SignalBus(transport=transport, agent_node_id="A", wire_kinds={"*"})
        relay = _Collector()
        await transport.subscribe("anything", relay)
        await bus.publish(_sig(kind="anything"))
        assert len(relay.seen) == 1

    async def test_empty_allowlist_forwards_nothing(self) -> None:
        transport = LocalTransport()
        await transport.connect()
        bus = SignalBus(transport=transport, agent_node_id="A", wire_kinds=set())
        relay = _Collector()
        await transport.subscribe("k", relay)
        await bus.publish(_sig(kind="k"))
        assert len(relay.seen) == 0

    async def test_inbound_unaffected_by_allowlist(self) -> None:
        # A node may receive a kind it chooses not to publish onto the wire.
        transport = LocalTransport(agent_node_id="shared")
        await transport.connect()
        bus_a = SignalBus(transport=transport, agent_node_id="A")  # publisher, no gate
        bus_b = SignalBus(transport=transport, agent_node_id="B", wire_kinds=set())
        b_cb = _Collector()
        bus_b.subscribe("k", b_cb)
        await bus_a.start_transport()
        await bus_b.start_transport()
        await bus_a.publish(_sig(value=99))
        assert len(b_cb.seen) == 1        # inbound delivery still works
        assert b_cb.seen[0].value == 99


# ── Review 6 (distributed): transport interest, echo ids, retries, remote tap ─

async def _r6_nats_pair(server: Any, a_id: str | None = "A", b_id: str | None = "B",
                        ) -> tuple[SignalBus, SignalBus, Any, Any]:
    from arachnite.transport.nats import NATSTransport
    ta = NATSTransport(agent_node_id="A")
    tb = NATSTransport(agent_node_id="B")
    await ta.connect()
    await tb.connect()
    return (SignalBus(transport=ta, agent_node_id=a_id),
            SignalBus(transport=tb, agent_node_id=b_id), ta, tb)


class TestR6BusTransportInterest:
    """R6-F12-01: one transport subscription per bus, so each remote signal arrives once."""

    async def test_kind_and_wildcard_subscribers_get_one_copy_local(self) -> None:
        """R6-F12-01: shared LocalTransport, B has a 'k' and a '*' subscriber"""
        transport = LocalTransport(agent_node_id="shared")
        await transport.connect()
        bus_a = SignalBus(transport=transport, agent_node_id="A")
        bus_b = SignalBus(transport=transport, agent_node_id="B")
        kind_cb, star_cb = _Collector(), _Collector()
        bus_b.subscribe("node_fault", kind_cb)
        bus_b.subscribe("*", star_cb)
        await bus_a.start_transport()
        await bus_b.start_transport()
        await bus_a.publish(_sig("node_fault", "faulted"))
        assert (len(kind_cb.seen), len(star_cb.seen)) == (1, 1)

    async def test_kind_and_wildcard_subscribers_get_one_copy_nats(
        self, fake_nats_server: Any,
    ) -> None:
        """R6-F12-01: over NATS (real encode/decode) each subscriber gets 1, not 4"""
        bus_a, bus_b, _ta, tb = await _r6_nats_pair(fake_nats_server)
        kind_cb, star_cb = _Collector(), _Collector()
        bus_b.subscribe("node_fault", kind_cb)
        bus_b.subscribe("*", star_cb)
        await bus_a.start_transport()
        await bus_b.start_transport()
        await bus_a.publish(_sig("node_fault", "faulted"))
        assert (len(kind_cb.seen), len(star_cb.seen)) == (1, 1)
        assert fake_nats_server.live_subjects(tb._nc) == ["arachnite.>"]

    async def test_unsubscribe_withdraws_transport_interest(self, fake_nats_server: Any) -> None:
        """R6-F12-01: dropping the last '*' subscriber falls back to kind subscriptions"""
        bus_a, bus_b, _ta, tb = await _r6_nats_pair(fake_nats_server)
        kind_cb, star_cb = _Collector(), _Collector()
        bus_b.subscribe("node_fault", kind_cb)
        bus_b.subscribe("*", star_cb)
        await bus_a.start_transport()
        await bus_b.start_transport()
        bus_b.unsubscribe("*", star_cb)
        await settle()
        assert fake_nats_server.live_subjects(tb._nc) == ["arachnite.node_fault"]
        await bus_a.publish(_sig("node_fault", "faulted"))
        await bus_a.publish(_sig("other", 1))
        assert [s.kind for s in kind_cb.seen] == ["node_fault"]
        assert star_cb.seen == []
        bus_b.unsubscribe("node_fault", kind_cb)
        await settle()
        assert fake_nats_server.live_subjects(tb._nc) == []


class TestR6BusEchoId:
    """R6-F12-12: the bus's echo key defaults to the transport's agent id."""

    async def test_default_bus_ids_do_not_drop_peer_signals(self, fake_nats_server: Any) -> None:
        """R6-F12-12: buses built without agent_node_id still exchange signals"""
        from arachnite.transport.nats import NATSTransport
        tp, tl = NATSTransport(agent_node_id="pi"), NATSTransport(agent_node_id="laptop")
        await tp.connect()
        await tl.connect()
        bus_pi, bus_laptop = SignalBus(transport=tp), SignalBus(transport=tl)
        got = _Collector()
        bus_laptop.subscribe("temperature", got)
        await bus_pi.start_transport()
        await bus_laptop.start_transport()
        await bus_pi.publish(_sig("temperature", 71.5))
        assert [s.value for s in got.seen] == [71.5]
        assert got.seen[0].metadata[ORIGIN_AGENT_KEY] == "pi"


class TestR6BusSubscribeRetry:
    """R6-F12-18: a transport subscription that fails is retried, not lost."""

    async def test_failed_late_subscribe_is_retried_on_next_subscribe(
        self, fake_nats_server: Any,
    ) -> None:
        """R6-F12-18: the next subscribe() of the kind retries the failed one"""
        bus_a, bus_b, _ta, _tb = await _r6_nats_pair(fake_nats_server)
        await bus_a.start_transport()
        await bus_b.start_transport()
        fake_nats_server.fail_next_subscribe = 1
        got = _Collector()
        bus_b.subscribe("proximity", got)            # transport subscribe fails once
        await settle()
        bus_b.subscribe("proximity", _Collector())   # a second subscriber of the kind
        await settle()
        await bus_a.publish(_sig("proximity", 0.1))
        assert [s.value for s in got.seen] == [0.1]

    async def test_failed_subscribe_is_retried_in_background(
        self, fake_nats_server: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """R6-F12-18: without further subscribe() calls the retry still happens"""
        import arachnite.bus as bus_module
        monkeypatch.setattr(bus_module, "_TRANSPORT_RETRY_S", 0.01, raising=False)
        bus_a, bus_b, _ta, _tb = await _r6_nats_pair(fake_nats_server)
        await bus_a.start_transport()
        await bus_b.start_transport()
        fake_nats_server.fail_next_subscribe = 1
        got = _Collector()
        bus_b.subscribe("proximity", got)
        await asyncio.sleep(0.1)
        await bus_a.publish(_sig("proximity", 0.2))
        assert [s.value for s in got.seen] == [0.2]


class TestR6BusRemoteListener:
    """R6-F3: the bus reports transport-delivered signals to remote listeners."""

    async def test_remote_listener_sees_remote_not_local_signals(self) -> None:
        """R6-F3: a remote listener gets peers' signals of every kind, never local ones"""
        transport = LocalTransport(agent_node_id="shared")
        await transport.connect()
        bus_a = SignalBus(transport=transport, agent_node_id="A")
        bus_b = SignalBus(transport=transport, agent_node_id="B")
        remote: list[Signal] = []
        bus_b.add_remote_listener(remote.append)
        await bus_a.start_transport()
        await bus_b.start_transport()
        await bus_a.publish(_sig("temperature", 1))   # nobody on B subscribes this kind
        await bus_b.publish(_sig("temperature", 2))   # B's own publish
        assert [(s.kind, s.value) for s in remote] == [("temperature", 1)]
        assert bus_b.is_remote(remote[0])

    async def test_stop_transport_withdraws_subscriptions(self) -> None:
        """R6-F3: after stop_transport() a shared transport no longer feeds the bus"""
        transport = LocalTransport(agent_node_id="shared")
        await transport.connect()
        bus_a = SignalBus(transport=transport, agent_node_id="A")
        bus_b = SignalBus(transport=transport, agent_node_id="B")
        got = _Collector()
        bus_b.subscribe("k", got)
        await bus_a.start_transport()
        await bus_b.start_transport()
        await bus_b.stop_transport()
        await bus_a.publish(_sig("k", 1))
        assert got.seen == []
        await bus_b.start_transport()
        await bus_a.publish(_sig("k", 2))
        assert [s.value for s in got.seen] == [2]
