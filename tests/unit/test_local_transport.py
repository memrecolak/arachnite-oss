"""Unit tests for LocalTransport — publish/subscribe/connect/disconnect."""

from __future__ import annotations

import time

import pytest

from arachnite.models import Signal
from arachnite.transport.local import LocalTransport


def _sig(kind: str = "temperature") -> Signal:
    return Signal(
        source="s", kind=kind, value=1.0,
        confidence=1.0, timestamp=time.monotonic(),
    )


# ── Lifecycle ─────────────────────────────────────────────────────────────────

class TestLocalTransportLifecycle:
    @pytest.mark.asyncio
    async def test_initial_not_connected(self) -> None:
        t = LocalTransport()
        assert not t.connected

    @pytest.mark.asyncio
    async def test_connect_sets_connected(self) -> None:
        t = LocalTransport()
        await t.connect()
        assert t.connected

    @pytest.mark.asyncio
    async def test_disconnect_clears_connected(self) -> None:
        t = LocalTransport()
        await t.connect()
        await t.disconnect()
        assert not t.connected


# ── Publish ───────────────────────────────────────────────────────────────────

class TestLocalTransportPublish:
    @pytest.mark.asyncio
    async def test_publish_no_subscribers_is_silent(self) -> None:
        t = LocalTransport()
        await t.publish(_sig())  # must not raise

    @pytest.mark.asyncio
    async def test_publish_delivers_to_kind_subscriber(self) -> None:
        t        = LocalTransport()
        received = []

        async def cb(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("temperature", cb)
        await t.publish(_sig("temperature"))
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_publish_delivers_to_wildcard_subscriber(self) -> None:
        t        = LocalTransport()
        received = []

        async def cb(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("*", cb)
        await t.publish(_sig("humidity"))
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_publish_delivers_to_both_kind_and_wildcard(self) -> None:
        t        = LocalTransport()
        received = []

        async def cb(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("temperature", cb)
        await t.subscribe("*", cb)
        await t.publish(_sig("temperature"))
        assert len(received) == 2


# ── Subscribe / Unsubscribe ───────────────────────────────────────────────────

class TestLocalTransportSubscribe:
    @pytest.mark.asyncio
    async def test_duplicate_subscribe_is_idempotent(self) -> None:
        t        = LocalTransport()
        received = []

        async def cb(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("temperature", cb)
        await t.subscribe("temperature", cb)  # should not add a second copy
        await t.publish(_sig("temperature"))
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_delivery(self) -> None:
        t        = LocalTransport()
        received = []

        async def cb(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("temperature", cb)
        await t.publish(_sig("temperature"))
        await t.unsubscribe("temperature", cb)
        await t.publish(_sig("temperature"))
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent_is_silent(self) -> None:
        t = LocalTransport()

        async def cb(sig: Signal) -> None:
            pass

        await t.unsubscribe("temperature", cb)  # must not raise


# ── agent_node_id property (BaseTransport) ────────────────────────────────────

class TestBaseTransportProperties:
    def test_agent_node_id_property(self) -> None:
        t = LocalTransport(agent_node_id="my-device")
        assert t.agent_node_id == "my-device"
