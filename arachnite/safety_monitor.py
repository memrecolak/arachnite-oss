"""
arachnite.safety_monitor
~~~~~~~~~~~~~~~~~~~~~~~~~
Runtime safety monitors that continuously verify invariants.

Monitors are lightweight checkers attached to the runtime that verify
safety properties after each tick. They complement the static formal
verification (UPPAAL model, §6) with dynamic runtime assurance.

Each monitor implements a single safety invariant and emits a
SafetyViolationSignal on the SignalBus if the invariant is breached.
Reflex instincts can subscribe to these signals and trigger
compensating actions (e.g., emergency stop, safe mode).

References:
  - Leucker & Schallhart, "A Brief Account of Runtime Verification,"
    J. Logic and Algebraic Programming, 2009.
  - Hawkins et al., "Guidance on the Safety Assurance of Autonomous
    Systems in Complex Environments (SACE)," University of York, 2021.
  - Ferrando et al., "ROSMonitoring: A Runtime Verification Framework
    for ROS," ICTAI 2020.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum

from arachnite.bus import SignalBus
from arachnite.models import Signal


class SafetySeverity(Enum):
    """Severity level for safety violations."""
    WARNING  = "warning"    # invariant close to being breached
    VIOLATION = "violation"  # invariant breached
    CRITICAL = "critical"   # safety-critical invariant breached


@dataclass(slots=True)
class SafetyViolationSignal(Signal):
    """Signal emitted when a safety monitor detects a violation.

    Published on the SignalBus with kind="safety_violation" so that
    reflex instincts can react to safety breaches within the same
    tick pipeline.
    """
    monitor_id: str = ""
    severity: SafetySeverity = SafetySeverity.VIOLATION
    property_name: str = ""
    details: str = ""


class BaseSafetyMonitor(ABC):
    """Base class for runtime safety monitors.

    Subclass and implement check() to verify a specific safety invariant.
    The runtime calls check() after each tick with the tick's state.
    """

    monitor_id: str = "BaseSafetyMonitor"

    def __init__(self, bus: SignalBus) -> None:
        self._bus = bus
        self._violation_count = 0
        self._last_check_ok = True

    @abstractmethod
    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        """Verify the safety invariant. Return a violation signal or None."""

    async def emit_violation(
        self,
        property_name: str,
        severity: SafetySeverity,
        details: str,
    ) -> SafetyViolationSignal:
        """Create and publish a safety violation signal."""
        self._violation_count += 1
        self._last_check_ok = False
        signal = SafetyViolationSignal(
            source=self.monitor_id,
            kind="safety_violation",
            value=property_name,
            confidence=1.0,
            timestamp=time.monotonic(),
            monitor_id=self.monitor_id,
            severity=severity,
            property_name=property_name,
            details=details,
        )
        await self._bus.publish(signal)
        return signal

    @property
    def violation_count(self) -> int:
        return self._violation_count

    @property
    def healthy(self) -> bool:
        return self._last_check_ok


@dataclass
class MonitorState:
    """Snapshot of runtime state provided to monitors each tick."""
    tick: int = 0
    reflex_fired: bool = False
    reflex_action_dispatched: bool = False
    decision_entered: bool = False
    mandatory_block_active: bool = False
    interrupt_accepted_during_block: bool = False
    active_reflex_nodes: int = 0
    total_reflex_nodes: int = 0
    tick_duration_ms: float = 0.0
    tick_budget_ms: float = 100.0


# ── Concrete monitors ───────────────────────────────────────────────────────


class ReflexBypassMonitor(BaseSafetyMonitor):
    """Verifies Property 1: reflex arc bypasses the decision layer.

    Corresponds to UPPAAL P1: A[] (reflex_fired => !decision_entered)

    If a reflex fires and the decision layer was also entered in the
    same tick, the pipeline ordering invariant has been violated.
    """

    monitor_id = "ReflexBypassMonitor"

    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        self._last_check_ok = True
        if state.reflex_fired and state.decision_entered:
            return await self.emit_violation(
                property_name="reflex_bypass",
                severity=SafetySeverity.CRITICAL,
                details=(
                    f"Tick {tick}: reflex fired but decision layer was also "
                    f"entered. Pipeline ordering invariant violated."
                ),
            )
        return None


class MandatoryBlockMonitor(BaseSafetyMonitor):
    """Verifies Property 2: mandatory blocks cannot be interrupted.

    Corresponds to UPPAAL P2: A[] (mandatory_block => !interrupted)

    If an interrupt is accepted while a mandatory completion block
    is active, the atomicity invariant has been violated.
    """

    monitor_id = "MandatoryBlockMonitor"

    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        self._last_check_ok = True
        if state.mandatory_block_active and state.interrupt_accepted_during_block:
            return await self.emit_violation(
                property_name="mandatory_block_atomicity",
                severity=SafetySeverity.CRITICAL,
                details=(
                    f"Tick {tick}: interrupt accepted during mandatory "
                    f"completion block. Atomicity invariant violated."
                ),
            )
        return None


class ReflexDispatchMonitor(BaseSafetyMonitor):
    """Verifies Property 3: reflex fires imply action dispatched.

    Corresponds to UPPAAL P3: A[] (reflex_fired => action_dispatched)

    If a reflex fires but its target action was not dispatched,
    the reflex path has a dead end.
    """

    monitor_id = "ReflexDispatchMonitor"

    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        self._last_check_ok = True
        if state.reflex_fired and not state.reflex_action_dispatched:
            return await self.emit_violation(
                property_name="reflex_dispatch_guarantee",
                severity=SafetySeverity.CRITICAL,
                details=(
                    f"Tick {tick}: reflex fired but target action was "
                    f"not dispatched. Reflex path has dead end."
                ),
            )
        return None


class ReflexAvailabilityMonitor(BaseSafetyMonitor):
    """Verifies that reflex nodes remain available (not FAULTED/DEAD).

    This is a liveness property: if any registered reflex node is
    unavailable, the safety response guarantee is degraded.
    """

    monitor_id = "ReflexAvailabilityMonitor"

    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        self._last_check_ok = True
        if state.total_reflex_nodes > 0 and state.active_reflex_nodes < state.total_reflex_nodes:
            degraded = state.total_reflex_nodes - state.active_reflex_nodes
            return await self.emit_violation(
                property_name="reflex_availability",
                severity=SafetySeverity.WARNING,
                details=(
                    f"Tick {tick}: {degraded}/{state.total_reflex_nodes} "
                    f"reflex nodes unavailable. Safety response degraded."
                ),
            )
        return None


class TickBudgetMonitor(BaseSafetyMonitor):
    """Verifies that tick duration stays within the budget.

    Persistent overruns indicate that the tick rate is too high for the
    current workload, which can delay reflex responses.
    """

    monitor_id = "TickBudgetMonitor"

    def __init__(self, bus: SignalBus, overrun_threshold: float = 0.2) -> None:
        super().__init__(bus)
        self._threshold = overrun_threshold
        self._consecutive_overruns = 0

    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        self._last_check_ok = True
        if state.tick_duration_ms > state.tick_budget_ms * (1 + self._threshold):
            self._consecutive_overruns += 1
            if self._consecutive_overruns >= 3:
                return await self.emit_violation(
                    property_name="tick_budget",
                    severity=SafetySeverity.WARNING,
                    details=(
                        f"Tick {tick}: {self._consecutive_overruns} consecutive "
                        f"overruns ({state.tick_duration_ms:.1f} ms > "
                        f"{state.tick_budget_ms:.1f} ms budget). "
                        f"Reflex response times may be degraded."
                    ),
                )
        else:
            self._consecutive_overruns = 0
        return None


# ── Monitor registry ─────────────────────────────────────────────────────────


class SafetyMonitorRegistry:
    """Manages a set of safety monitors.

    Attached to ArachniteRuntime to run all monitors after each tick.
    """

    def __init__(self, bus: SignalBus) -> None:
        self._bus = bus
        self._monitors: dict[str, BaseSafetyMonitor] = {}

    def register(self, monitor: BaseSafetyMonitor) -> None:
        self._monitors[monitor.monitor_id] = monitor

    def unregister(self, monitor_id: str) -> None:
        self._monitors.pop(monitor_id, None)

    @property
    def monitors(self) -> list[BaseSafetyMonitor]:
        return list(self._monitors.values())

    async def check_all(self, tick: int, state: MonitorState) -> list[SafetyViolationSignal]:
        """Run all monitors and return any violations."""
        violations: list[SafetyViolationSignal] = []
        for monitor in self._monitors.values():
            result = await monitor.check(tick, state)
            if result is not None:
                violations.append(result)
        return violations

    @property
    def all_healthy(self) -> bool:
        return all(m.healthy for m in self._monitors.values())

    @property
    def total_violations(self) -> int:
        return sum(m.violation_count for m in self._monitors.values())

    @classmethod
    def default(cls, bus: SignalBus) -> SafetyMonitorRegistry:
        """Create a registry with all standard safety monitors."""
        registry = cls(bus)
        registry.register(ReflexBypassMonitor(bus))
        registry.register(MandatoryBlockMonitor(bus))
        registry.register(ReflexDispatchMonitor(bus))
        registry.register(ReflexAvailabilityMonitor(bus))
        registry.register(TickBudgetMonitor(bus))
        return registry
