"""Tests for runtime safety monitors."""

from __future__ import annotations

import pytest

from arachnite import SignalBus
from arachnite.safety_monitor import (
    MandatoryBlockMonitor,
    MonitorState,
    ReflexAvailabilityMonitor,
    ReflexBypassMonitor,
    ReflexDispatchMonitor,
    SafetyMonitorRegistry,
    SafetySeverity,
    SafetyViolationSignal,
    TickBudgetMonitor,
)


@pytest.fixture
def bus() -> SignalBus:
    return SignalBus()


def _state(**overrides: object) -> MonitorState:
    return MonitorState(**overrides)  # type: ignore[arg-type]


# ── ReflexBypassMonitor ──────────────────────────────────────────────────────


class TestReflexBypassMonitor:
    @pytest.mark.asyncio
    async def test_no_violation_when_reflex_not_fired(self, bus: SignalBus) -> None:
        m = ReflexBypassMonitor(bus)
        result = await m.check(1, _state(reflex_fired=False, decision_entered=True))
        assert result is None
        assert m.healthy

    @pytest.mark.asyncio
    async def test_no_violation_when_reflex_fires_without_decision(self, bus: SignalBus) -> None:
        m = ReflexBypassMonitor(bus)
        result = await m.check(1, _state(reflex_fired=True, decision_entered=False))
        assert result is None

    @pytest.mark.asyncio
    async def test_violation_when_reflex_and_decision_both_entered(self, bus: SignalBus) -> None:
        m = ReflexBypassMonitor(bus)
        result = await m.check(1, _state(reflex_fired=True, decision_entered=True))
        assert result is not None
        assert result.severity == SafetySeverity.CRITICAL
        assert result.property_name == "reflex_bypass"
        assert not m.healthy
        assert m.violation_count == 1

    @pytest.mark.asyncio
    async def test_violation_published_to_bus(self, bus: SignalBus) -> None:
        received: list[SafetyViolationSignal] = []
        async def _capture(sig: SafetyViolationSignal) -> None:
            received.append(sig)
        bus.subscribe("safety_violation", _capture)

        m = ReflexBypassMonitor(bus)
        await m.check(1, _state(reflex_fired=True, decision_entered=True))
        assert len(received) == 1
        assert received[0].monitor_id == "ReflexBypassMonitor"


# ── MandatoryBlockMonitor ────────────────────────────────────────────────────


class TestMandatoryBlockMonitor:
    @pytest.mark.asyncio
    async def test_no_violation_when_block_not_active(self, bus: SignalBus) -> None:
        m = MandatoryBlockMonitor(bus)
        result = await m.check(
            1,
            _state(mandatory_block_active=False, interrupt_accepted_during_block=True),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_no_violation_when_block_active_no_interrupt(self, bus: SignalBus) -> None:
        m = MandatoryBlockMonitor(bus)
        result = await m.check(
            1,
            _state(mandatory_block_active=True, interrupt_accepted_during_block=False),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_violation_when_interrupted_during_block(self, bus: SignalBus) -> None:
        m = MandatoryBlockMonitor(bus)
        result = await m.check(
            1,
            _state(mandatory_block_active=True, interrupt_accepted_during_block=True),
        )
        assert result is not None
        assert result.severity == SafetySeverity.CRITICAL
        assert result.property_name == "mandatory_block_atomicity"


# ── ReflexDispatchMonitor ────────────────────────────────────────────────────


class TestReflexDispatchMonitor:
    @pytest.mark.asyncio
    async def test_no_violation_when_reflex_not_fired(self, bus: SignalBus) -> None:
        m = ReflexDispatchMonitor(bus)
        result = await m.check(1, _state(reflex_fired=False))
        assert result is None

    @pytest.mark.asyncio
    async def test_no_violation_when_reflex_dispatched(self, bus: SignalBus) -> None:
        m = ReflexDispatchMonitor(bus)
        result = await m.check(1, _state(reflex_fired=True, reflex_action_dispatched=True))
        assert result is None

    @pytest.mark.asyncio
    async def test_violation_when_reflex_not_dispatched(self, bus: SignalBus) -> None:
        m = ReflexDispatchMonitor(bus)
        result = await m.check(1, _state(reflex_fired=True, reflex_action_dispatched=False))
        assert result is not None
        assert result.property_name == "reflex_dispatch_guarantee"


# ── ReflexAvailabilityMonitor ────────────────────────────────────────────────


class TestReflexAvailabilityMonitor:
    @pytest.mark.asyncio
    async def test_no_violation_when_all_available(self, bus: SignalBus) -> None:
        m = ReflexAvailabilityMonitor(bus)
        result = await m.check(1, _state(active_reflex_nodes=2, total_reflex_nodes=2))
        assert result is None

    @pytest.mark.asyncio
    async def test_warning_when_degraded(self, bus: SignalBus) -> None:
        m = ReflexAvailabilityMonitor(bus)
        result = await m.check(1, _state(active_reflex_nodes=1, total_reflex_nodes=3))
        assert result is not None
        assert result.severity == SafetySeverity.WARNING
        assert "2/3" in result.details

    @pytest.mark.asyncio
    async def test_no_violation_when_no_reflex_nodes(self, bus: SignalBus) -> None:
        m = ReflexAvailabilityMonitor(bus)
        result = await m.check(1, _state(active_reflex_nodes=0, total_reflex_nodes=0))
        assert result is None


# ── TickBudgetMonitor ────────────────────────────────────────────────────────


class TestTickBudgetMonitor:
    @pytest.mark.asyncio
    async def test_no_violation_within_budget(self, bus: SignalBus) -> None:
        m = TickBudgetMonitor(bus)
        result = await m.check(1, _state(tick_duration_ms=80.0, tick_budget_ms=100.0))
        assert result is None

    @pytest.mark.asyncio
    async def test_no_violation_on_single_overrun(self, bus: SignalBus) -> None:
        """Single overrun should not trigger (needs 3 consecutive)."""
        m = TickBudgetMonitor(bus)
        result = await m.check(1, _state(tick_duration_ms=150.0, tick_budget_ms=100.0))
        assert result is None

    @pytest.mark.asyncio
    async def test_violation_on_consecutive_overruns(self, bus: SignalBus) -> None:
        m = TickBudgetMonitor(bus)
        await m.check(1, _state(tick_duration_ms=150.0, tick_budget_ms=100.0))
        await m.check(2, _state(tick_duration_ms=150.0, tick_budget_ms=100.0))
        result = await m.check(3, _state(tick_duration_ms=150.0, tick_budget_ms=100.0))
        assert result is not None
        assert result.severity == SafetySeverity.WARNING
        assert "3 consecutive" in result.details

    @pytest.mark.asyncio
    async def test_counter_resets_on_good_tick(self, bus: SignalBus) -> None:
        m = TickBudgetMonitor(bus)
        await m.check(1, _state(tick_duration_ms=150.0, tick_budget_ms=100.0))
        await m.check(2, _state(tick_duration_ms=150.0, tick_budget_ms=100.0))
        await m.check(3, _state(tick_duration_ms=80.0, tick_budget_ms=100.0))  # reset
        await m.check(4, _state(tick_duration_ms=150.0, tick_budget_ms=100.0))
        result = await m.check(5, _state(tick_duration_ms=150.0, tick_budget_ms=100.0))
        assert result is None  # only 2 consecutive after reset


# ── SafetyMonitorRegistry ───────────────────────────────────────────────────


class TestSafetyMonitorRegistry:
    @pytest.mark.asyncio
    async def test_default_has_five_monitors(self, bus: SignalBus) -> None:
        reg = SafetyMonitorRegistry.default(bus)
        assert len(reg.monitors) == 5

    @pytest.mark.asyncio
    async def test_check_all_returns_violations(self, bus: SignalBus) -> None:
        reg = SafetyMonitorRegistry.default(bus)
        violations = await reg.check_all(1, _state(
            reflex_fired=True, decision_entered=True,
            reflex_action_dispatched=False,
        ))
        # ReflexBypass + ReflexDispatch should both fire
        assert len(violations) >= 2

    @pytest.mark.asyncio
    async def test_all_healthy_when_no_violations(self, bus: SignalBus) -> None:
        reg = SafetyMonitorRegistry.default(bus)
        await reg.check_all(1, _state())
        assert reg.all_healthy

    @pytest.mark.asyncio
    async def test_not_healthy_after_violation(self, bus: SignalBus) -> None:
        reg = SafetyMonitorRegistry.default(bus)
        await reg.check_all(1, _state(reflex_fired=True, decision_entered=True))
        assert not reg.all_healthy

    @pytest.mark.asyncio
    async def test_total_violations(self, bus: SignalBus) -> None:
        reg = SafetyMonitorRegistry.default(bus)
        await reg.check_all(1, _state(reflex_fired=True, decision_entered=True))
        assert reg.total_violations >= 1

    def test_register_unregister(self, bus: SignalBus) -> None:
        reg = SafetyMonitorRegistry(bus)
        m = ReflexBypassMonitor(bus)
        reg.register(m)
        assert len(reg.monitors) == 1
        reg.unregister("ReflexBypassMonitor")
        assert len(reg.monitors) == 0
