"""
arachnite.safety_monitor
~~~~~~~~~~~~~~~~~~~~~~~~~
Runtime safety monitors that continuously verify invariants.

Monitors are lightweight checkers that verify safety properties after each
tick. They complement the mechanically checked CSP models in ``formal/``
with dynamic runtime assurance.

The runtime does NOT run these automatically. You wire them into your own run
loop: build a `SafetyMonitorRegistry` and, after each tick, call
`registry.check_all(rt.tick_count, rt.monitor_state())`. `monitor_state()`
assembles the MonitorState from what the runtime actually observed during
the last tick (reflex launch outcomes, decision inputs, completed Results,
mandatory blocks in progress, reflex-node health, tick duration). See
tutorials/advanced/12_safety_monitors.md for the integration pattern.

Each monitor implements a single safety invariant and emits a
SafetyViolationSignal on the SignalBus if the invariant is breached.
`check_all()` also returns the violations directly. To react to them inside
the tick pipeline, subscribe a callback with `bus.subscribe("safety_violation",
...)` — note that violations are NOT injected into `ctx.signals`, so reading
`ctx.signals` (the node_fault pattern) will not see them.

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
    Drive it from your own run loop — typically via
    SafetyMonitorRegistry.check_all() after each tick — passing a MonitorState
    built from that tick's outcome. The runtime does not call check() for you.
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
    """
    Snapshot of runtime state provided to monitors each tick.

    ``ArachniteRuntime.monitor_state()`` fills every field from the last tick
    (reflex-lane activity since the previous report included).
    """
    tick: int = 0
    #: At least one reflex proposal was produced.
    reflex_fired: bool = False
    #: Every reflex proposal reached its action: launched, armed (waiting to
    #: preempt) or de-duplicated — none was lost (e.g. unknown action).
    reflex_action_dispatched: bool = False
    #: A reflex node's proposal was offered to the decision layer.
    reflex_routed_through_decision: bool = False
    #: Deprecated, ignored by the monitors: the decision stage runs every
    #: tick, also when a reflex fired (ADR 0005). Use
    #: ``reflex_routed_through_decision``.
    decision_entered: bool = False
    #: A running action was inside a mandatory block at the end of the tick.
    mandatory_block_active: bool = False
    #: A *deliberative* interrupt was honoured strictly inside a mandatory
    #: block. Reflex preemption (PREEMPT) and emergency_stop() are declared
    #: exceptions and never set this.
    interrupt_accepted_during_block: bool = False
    #: Declared resources that had more than one running owner.
    resources_with_multiple_owners: int = 0
    active_reflex_nodes: int = 0
    total_reflex_nodes: int = 0
    tick_duration_ms: float = 0.0
    tick_budget_ms: float = 100.0


# ── Concrete monitors ───────────────────────────────────────────────────────


class ReflexBypassMonitor(BaseSafetyMonitor):
    """Verifies Property 1: reflex proposals bypass the decision layer.

    Invariant: reflex_fired => !reflex_routed_through_decision.

    The decision stage itself runs every tick (deliberation continues while
    a reflex acts), so the property is about the *reflex proposal*: it must
    reach its action without ever being offered to the decision strategy.
    """

    monitor_id = "ReflexBypassMonitor"

    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        self._last_check_ok = True
        if state.reflex_fired and state.reflex_routed_through_decision:
            return await self.emit_violation(
                property_name="reflex_bypass",
                severity=SafetySeverity.CRITICAL,
                details=(
                    f"Tick {tick}: a reflex proposal was offered to the "
                    f"decision layer. Reflex bypass invariant violated."
                ),
            )
        return None


class MandatoryBlockMonitor(BaseSafetyMonitor):
    """Verifies Property 2: deliberative atomicity of mandatory blocks.

    Invariant: no deliberative interrupt is honoured strictly inside a
    mandatory block.

    A reflex declaring ReflexPreemption.PREEMPT and emergency_stop() are the
    declared exceptions (ADR 0005) and are not violations; they surface as
    ``Result.preempted`` / a shutdown, not as an accepted interrupt.
    """

    monitor_id = "MandatoryBlockMonitor"

    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        self._last_check_ok = True
        if state.mandatory_block_active and state.interrupt_accepted_during_block:
            return await self.emit_violation(
                property_name="mandatory_block_atomicity",
                severity=SafetySeverity.CRITICAL,
                details=(
                    f"Tick {tick}: deliberative interrupt honoured inside a "
                    f"mandatory completion block. Atomicity invariant violated."
                ),
            )
        return None


class ReflexDispatchMonitor(BaseSafetyMonitor):
    """Verifies Property 3: a fired reflex always reaches its action.

    Invariant: reflex_fired => reflex_action_dispatched.

    Launched, armed (waiting for a preempted owner's safe-stop or for a
    WAIT_FOR_BLOCK block) and de-duplicated outcomes all count as reaching
    the action; a reflex whose action cannot be found is a dead end. A launch
    refused because the runtime is shutting down (CLOSED) is not a dead end.
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


class ActuatorExclusivityMonitor(BaseSafetyMonitor):
    """Verifies Property 7: actuator exclusivity (ADR 0005).

    Invariant: no declared resource has more than one running owner.

    ActionMasterNode enforces this by construction for declared resources;
    the monitor is the runtime check of that guarantee.
    """

    monitor_id = "ActuatorExclusivityMonitor"

    async def check(self, tick: int, state: MonitorState) -> SafetyViolationSignal | None:
        self._last_check_ok = True
        if state.resources_with_multiple_owners > 0:
            return await self.emit_violation(
                property_name="actuator_exclusivity",
                severity=SafetySeverity.CRITICAL,
                details=(
                    f"Tick {tick}: {state.resources_with_multiple_owners} "
                    f"resource(s) owned by more than one running action."
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

    Call check_all(tick, state) from your run loop after each tick to run all
    registered monitors. This is not attached to ArachniteRuntime automatically
    — see tutorials/advanced/12_safety_monitors.md for the wiring pattern.
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
        registry.register(ActuatorExclusivityMonitor(bus))
        registry.register(ReflexAvailabilityMonitor(bus))
        registry.register(TickBudgetMonitor(bus))
        return registry
