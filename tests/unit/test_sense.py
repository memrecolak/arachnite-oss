"""Unit tests for BaseSenseNode and SenseMasterNode edge cases."""

from __future__ import annotations

import time

import pytest

from arachnite import SignalBus
from arachnite.exceptions import NodeRegistrationError
from arachnite.models import Signal
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode

# ── Concrete test nodes ────────────────────────────────────────────────────────

class GoodSenseNode(BaseSenseNode):
    node_id     = "GoodSenseNode"
    signal_kind = "thermal"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=42.0, confidence=1.0, timestamp=time.monotonic(),
        )


class BrokenSenseNode(BaseSenseNode):
    """Always raises during read()."""
    node_id     = "BrokenSenseNode"
    signal_kind = "thermal"

    async def read(self) -> Signal:
        raise RuntimeError("sensor hardware failure")


class CustomErrorSenseNode(BaseSenseNode):
    """Overrides on_error to return a fallback signal."""
    node_id     = "CustomErrorSenseNode"
    signal_kind = "thermal"

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.error_count = 0

    async def read(self) -> Signal:
        raise ValueError("transient error")

    async def on_error(self, exc: Exception) -> Signal | None:
        self.error_count += 1
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=0.0, confidence=0.0, timestamp=time.monotonic(),
        )


# ── BaseSenseNode.on_error default ────────────────────────────────────────────

class TestBaseSenseNodeOnError:
    @pytest.mark.asyncio
    async def test_default_on_error_returns_none(self) -> None:
        node = BrokenSenseNode(bus=SignalBus())
        result = await node.on_error(RuntimeError("boom"))
        assert result is None

    @pytest.mark.asyncio
    async def test_custom_on_error_returns_fallback_signal(self) -> None:
        node = CustomErrorSenseNode(bus=SignalBus())
        result = await node.on_error(ValueError("x"))
        assert result is not None
        assert result.confidence == 0.0


# ── SenseMasterNode.unregister ────────────────────────────────────────────────

class TestSenseMasterNodeUnregister:
    def test_unregister_removes_node(self) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(GoodSenseNode(bus=bus))
        sm.unregister("GoodSenseNode")
        assert "GoodSenseNode" not in {n.node_id for n in sm.nodes}

    def test_unregister_nonexistent_is_silent(self) -> None:
        sm = SenseMasterNode(bus=SignalBus())
        sm.unregister("NotRegistered")   # must not raise


# ── SenseMasterNode.read_all error path ───────────────────────────────────────

class TestSenseMasterNodeReadAll:
    @pytest.mark.asyncio
    async def test_read_raises_calls_on_error_and_drops_signal(self) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(BrokenSenseNode(bus=bus))
        signals = await sm.read_all()
        # Default on_error returns None → signal is excluded
        assert signals == []

    @pytest.mark.asyncio
    async def test_read_error_with_fallback_included_in_signals(self) -> None:
        bus  = SignalBus()
        sm   = SenseMasterNode(bus=bus)
        node = CustomErrorSenseNode(bus=bus)
        sm.register(node)
        signals = await sm.read_all()
        # on_error returns a fallback Signal → included
        assert len(signals) == 1
        assert signals[0].confidence == 0.0
        assert node.error_count == 1

    @pytest.mark.asyncio
    async def test_broken_and_good_node_only_good_returned(self) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(GoodSenseNode(bus=bus))
        sm.register(BrokenSenseNode(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert signals[0].value == 42.0

    @pytest.mark.asyncio
    async def test_empty_master_returns_empty(self) -> None:
        sm = SenseMasterNode(bus=SignalBus())
        assert await sm.read_all() == []

    @pytest.mark.asyncio
    async def test_duplicate_registration_raises(self) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(GoodSenseNode(bus=bus))
        with pytest.raises(NodeRegistrationError):
            sm.register(GoodSenseNode(bus=bus))

    def test_node_repr(self) -> None:
        node = GoodSenseNode(bus=SignalBus())
        r = repr(node)
        assert "GoodSenseNode" in r


# ── SenseMasterNode.get_node ──────────────────────────────────────────────────

class TestSenseMasterNodeGetNode:
    def test_get_node_returns_registered_node(self) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        node = GoodSenseNode(bus=bus)
        sm.register(node)
        assert sm.get_node("GoodSenseNode") is node

    def test_get_node_returns_none_when_not_registered(self) -> None:
        sm = SenseMasterNode(bus=SignalBus())
        assert sm.get_node("NonExistent") is None


# ── SenseNode returning None ("nothing to report") ──────────────────────────

class IdleSenseNode(BaseSenseNode):
    """Returns a signal once, then None on every subsequent tick."""
    node_id     = "IdleSenseNode"
    signal_kind = "bootstrap"

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self._fired = False

    async def read(self) -> Signal | None:
        if not self._fired:
            self._fired = True
            return Signal(
                source=self.node_id, kind=self.signal_kind,
                value="inventory", confidence=1.0, timestamp=time.monotonic(),
            )
        return None


class AlwaysNoneSenseNode(BaseSenseNode):
    """Always returns None — never has anything to report."""
    node_id     = "AlwaysNoneSenseNode"
    signal_kind = "empty"

    async def read(self) -> Signal | None:
        return None


class TestSenseNodeNoneReturn:
    @pytest.mark.asyncio
    async def test_none_return_excluded_from_signals(self) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(AlwaysNoneSenseNode(bus=bus))
        signals = await sm.read_all()
        assert signals == []

    @pytest.mark.asyncio
    async def test_fire_once_then_idle(self) -> None:
        """First tick returns a signal, subsequent ticks return None."""
        bus  = SignalBus()
        sm   = SenseMasterNode(bus=bus)
        node = IdleSenseNode(bus=bus)
        sm.register(node)

        signals_1 = await sm.read_all()
        assert len(signals_1) == 1
        assert signals_1[0].kind == "bootstrap"

        signals_2 = await sm.read_all()
        assert signals_2 == []

        signals_3 = await sm.read_all()
        assert signals_3 == []

    @pytest.mark.asyncio
    async def test_none_and_signal_nodes_mixed(self) -> None:
        """A None-returning node alongside a normal node."""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(GoodSenseNode(bus=bus))
        sm.register(AlwaysNoneSenseNode(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert signals[0].kind == "thermal"

    @pytest.mark.asyncio
    async def test_all_none_returns_empty(self) -> None:
        """When every node returns None, read_all returns empty list."""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(AlwaysNoneSenseNode(bus=bus))
        idle = IdleSenseNode(bus=bus)
        idle._fired = True  # force idle state
        sm.register(idle)
        signals = await sm.read_all()
        assert signals == []


# ── returned_this_tick: identifying this tick's own sense output ──────────────

class TestReturnedThisTick:
    """
    read_all() publishes what it returns, so bus subscribers receive the sense
    output as well. returned_this_tick() lets a subscriber that defers signals
    to a later tick recognise — and skip — the copy the ContextNode already
    got this tick.
    """

    @pytest.mark.asyncio
    async def test_returned_signal_is_recognised(self) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(GoodSenseNode(bus=bus))
        signals = await sm.read_all()
        assert sm.returned_this_tick(signals[0]) is True

    @pytest.mark.asyncio
    async def test_equal_but_distinct_signal_is_not_recognised(self) -> None:
        """Identity, not value: another node's same-valued signal is foreign."""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(GoodSenseNode(bus=bus))
        signals = await sm.read_all()
        twin = Signal(
            source=signals[0].source, kind=signals[0].kind,
            value=signals[0].value, confidence=signals[0].confidence,
            timestamp=signals[0].timestamp,
        )
        assert sm.returned_this_tick(twin) is False

    @pytest.mark.asyncio
    async def test_record_is_dropped_on_the_next_read(self) -> None:
        """The record covers one tick only."""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(GoodSenseNode(bus=bus))
        first = (await sm.read_all())[0]
        await sm.read_all()
        assert sm.returned_this_tick(first) is False

    @pytest.mark.asyncio
    async def test_subscriber_sees_the_record_during_publish(self) -> None:
        """The record must be in place before the signals hit the bus."""
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        sm.register(GoodSenseNode(bus=bus))
        seen: list[bool] = []

        async def _on_signal(signal: Signal) -> None:
            seen.append(sm.returned_this_tick(signal))

        bus.subscribe("thermal", _on_signal)
        await sm.read_all()
        assert seen == [True]

    @pytest.mark.asyncio
    async def test_empty_master_recognises_nothing(self) -> None:
        bus = SignalBus()
        sm  = SenseMasterNode(bus=bus)
        assert await sm.read_all() == []
        sm.register(GoodSenseNode(bus=bus))
        stale = (await sm.read_all())[0]
        sm.unregister("GoodSenseNode")
        assert await sm.read_all() == []
        assert sm.returned_this_tick(stale) is False
