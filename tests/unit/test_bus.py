"""Unit tests for SignalBus."""

from __future__ import annotations

import pytest

from arachnite import SignalBus
from arachnite.exceptions import SignalBusError
from tests.conftest import make_signal


class TestSignalBus:
    @pytest.mark.asyncio
    async def test_subscribe_and_publish(self) -> None:
        bus      = SignalBus()
        received = []

        async def cb(sig):  # type: ignore[no-untyped-def]
            received.append(sig)

        bus.subscribe("thermal", cb)
        await bus.publish(make_signal())
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_wildcard_subscriber_receives_all(self) -> None:
        bus      = SignalBus()
        received = []

        async def cb(sig):  # type: ignore[no-untyped-def]
            received.append(sig)

        bus.subscribe("*", cb)
        await bus.publish(make_signal(kind="thermal"))
        await bus.publish(make_signal(kind="visual"))
        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_delivery(self) -> None:
        bus      = SignalBus()
        received = []

        async def cb(sig):  # type: ignore[no-untyped-def]
            received.append(sig)

        bus.subscribe("thermal", cb)
        await bus.publish(make_signal())
        bus.unsubscribe("thermal", cb)
        await bus.publish(make_signal())
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_subscriber_error_raises_bus_error(self) -> None:
        bus = SignalBus()

        async def bad_cb(sig):  # type: ignore[no-untyped-def]
            raise RuntimeError("boom")

        bus.subscribe("thermal", bad_cb)
        with pytest.raises(SignalBusError):
            await bus.publish(make_signal())

    @pytest.mark.asyncio
    async def test_other_subscribers_notified_despite_error(self) -> None:
        bus      = SignalBus()
        received = []

        async def bad_cb(sig):  # type: ignore[no-untyped-def]
            raise RuntimeError("boom")

        async def good_cb(sig):  # type: ignore[no-untyped-def]
            received.append(sig)

        bus.subscribe("thermal", bad_cb)
        bus.subscribe("thermal", good_cb)

        with pytest.raises(SignalBusError):
            await bus.publish(make_signal())

        assert len(received) == 1  # good_cb still ran

    @pytest.mark.asyncio
    async def test_clear_removes_all_subscribers(self) -> None:
        bus      = SignalBus()
        received = []

        async def cb(sig):  # type: ignore[no-untyped-def]
            received.append(sig)

        bus.subscribe("thermal", cb)
        bus.clear()
        await bus.publish(make_signal())
        assert received == []

    @pytest.mark.asyncio
    async def test_publish_many(self) -> None:
        bus      = SignalBus()
        received = []

        async def cb(sig):  # type: ignore[no-untyped-def]
            received.append(sig)

        bus.subscribe("thermal", cb)
        bus.subscribe("visual", cb)
        signals = [make_signal("thermal"), make_signal("visual"), make_signal("thermal")]
        await bus.publish_many(signals)
        assert len(received) == 3

    @pytest.mark.asyncio
    async def test_publish_many_empty_is_noop(self) -> None:
        bus = SignalBus()
        await bus.publish_many([])  # hits early-return guard

    def test_subscriber_count(self) -> None:
        bus = SignalBus()

        async def cb(sig):  # type: ignore[no-untyped-def]
            pass

        bus.subscribe("thermal", cb)
        bus.subscribe("thermal", cb)  # duplicate — not added twice
        assert bus.subscriber_count("thermal") == 1
        assert bus.subscriber_count("missing") == 0

    def test_subscribed_kinds(self) -> None:
        bus = SignalBus()

        async def cb(sig):  # type: ignore[no-untyped-def]
            pass

        bus.subscribe("thermal", cb)
        bus.subscribe("visual", cb)
        kinds = bus.subscribed_kinds()
        assert "thermal" in kinds
        assert "visual" in kinds

    def test_repr(self) -> None:
        bus = SignalBus()
        r = repr(bus)
        assert "SignalBus" in r
        assert "kinds=" in r


class TestSignalBusSubscriberMirror:
    def test_duplicate_subscribe_is_idempotent(self) -> None:
        bus = SignalBus()

        async def cb(sig):  # type: ignore[no-untyped-def]
            pass

        bus.subscribe("k", cb)
        bus.subscribe("k", cb)
        assert bus.subscriber_count("k") == 1

    def test_mirror_invariant_after_unsubscribe_resubscribe(self) -> None:
        bus = SignalBus()

        async def cb(sig):  # type: ignore[no-untyped-def]
            pass

        bus.subscribe("k", cb)
        bus.unsubscribe("k", cb)
        bus.subscribe("k", cb)
        assert bus.subscriber_count("k") == 1

    @pytest.mark.asyncio
    async def test_different_callbacks_across_gc_cycle(self) -> None:
        import gc

        bus = SignalBus()

        async def cb1(sig):  # type: ignore[no-untyped-def]
            pass

        bus.subscribe("k", cb1)
        bus.unsubscribe("k", cb1)
        del cb1
        gc.collect()

        received = []

        async def cb2(sig):  # type: ignore[no-untyped-def]
            received.append(sig)

        bus.subscribe("k", cb2)
        assert bus.subscriber_count("k") == 1
        await bus.publish(make_signal(kind="k"))
        assert len(received) == 1

    def test_bound_method_equality_deduplicated(self) -> None:
        bus = SignalBus()

        class Handler:
            async def handler(self, sig):  # type: ignore[no-untyped-def]
                pass

        obj = Handler()
        bus.subscribe("k", obj.handler)
        bus.subscribe("k", obj.handler)
        assert bus.subscriber_count("k") == 1

    def test_clear_resets_mirror(self) -> None:
        bus = SignalBus()

        async def cb(sig):  # type: ignore[no-untyped-def]
            pass

        bus.subscribe("k", cb)
        bus.clear()
        bus.subscribe("k", cb)
        assert bus.subscriber_count("k") == 1
