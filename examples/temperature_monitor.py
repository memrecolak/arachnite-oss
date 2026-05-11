"""
examples/temperature_monitor.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A realistic thermal-monitoring agent demonstrating:

  - Simulated sensor with realistic drift
  - Normal instinct (fires when temp > warning threshold)
  - Reflex instinct (fires when temp > critical threshold, bypasses decision)
  - Multi-step cooling action with a mandatory sustain block and rollback
  - Emergency shutdown action triggered by the reflex
  - NodeSupervisor restart policy on the action master
  - Structured logging to stdout

Pipeline:
    SimTempSensor
      → WarnInstinct (priority 70, temp > 60 °C)  → CoolFan (multi-step)
      → CriticalReflex (priority 250, temp > 85 °C) → EmergencyStop

Run:
    python examples/temperature_monitor.py
"""

from __future__ import annotations

import asyncio
import math
import random
import time

from arachnite import (
    ArachniteRuntime,
    SignalBus,
    ContextNode,
    Signal,
    Proposal,
    Result,
    ActionStep,
    StepResult,
    InterruptPolicy,
    BaseSenseNode,
    SenseMasterNode,
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    BaseActionNode,
    MultiStepActionNode,
    ActionMasterNode,
    NodeSupervisor,
    RestartPolicy,
    StdoutLogSink,
    LogLevel,
)


# ── Simulated temperature sensor ──────────────────────────────────────────────

class SimTempSensor(BaseSenseNode):
    """Generates a slowly rising temperature with small random noise."""
    node_id     = "SimTempSensor"
    signal_kind = "temperature"

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._start = time.monotonic()

    async def read(self) -> Signal:
        elapsed = time.monotonic() - self._start
        # Temperature rises from 20 °C toward 100 °C over ~60 s, with noise
        value = 20.0 + 80.0 * (1 - math.exp(-elapsed / 40.0)) + random.gauss(0, 0.5)
        return Signal(
            source     = self.node_id,
            kind       = self.signal_kind,
            value      = round(value, 2),
            confidence = 0.95,
            timestamp  = time.monotonic(),
        )


# ── Normal instinct: warn + trigger cooling ───────────────────────────────────

class WarnInstinct(BaseInstinctNode):
    """Triggers the multi-step fan sequence when temperature exceeds 60 °C."""
    node_id  = "WarnInstinct"
    priority = 70

    async def evaluate(self, ctx) -> Proposal | None:  # type: ignore[override]
        readings = [s for s in ctx.signals if s.kind == "temperature"]
        if readings and readings[-1].value > 60.0:
            return Proposal(
                instinct_id = self.node_id,
                action_id   = "CoolFan",
                priority    = self.priority,
                urgency     = min(1.0, (readings[-1].value - 60.0) / 30.0),
                parameters  = {"reading": readings[-1].value},
            )
        return None


# ── Reflex instinct: critical temperature → emergency stop ────────────────────

class CriticalReflex(BaseReflexInstinctNode):
    """
    Reflex: bypasses the DecisionNode entirely.
    Fires EmergencyStop immediately when temperature exceeds 85 °C.
    MUST be on the same AgentNode as EmergencyStop (co-location rule).
    """
    node_id  = "CriticalReflex"
    priority = 250  # reflexes require priority ≥ 200

    async def evaluate(self, ctx) -> Proposal | None:  # type: ignore[override]
        readings = [s for s in ctx.signals if s.kind == "temperature"]
        if readings and readings[-1].value > 85.0:
            return Proposal(
                instinct_id = self.node_id,
                action_id   = "EmergencyStop",
                priority    = self.priority,
                urgency     = 1.0,
                parameters  = {"reading": readings[-1].value},
            )
        return None


# ── Multi-step cooling action ─────────────────────────────────────────────────

class CoolFan(MultiStepActionNode):
    """
    Three-phase fan ramp:
      ramp_up   (interruptible)  — spin fan up to full speed
      sustain   (mandatory)      — run at full speed; cannot be interrupted
      ramp_down (interruptible)  — spin fan back down

    Uses ROLLBACK policy: if interrupted, _undo_sustain is called.
    """
    node_id          = "CoolFan"
    interrupt_policy = InterruptPolicy.ROLLBACK

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("ramp_up",   interruptible=True),
            ActionStep("sustain",   interruptible=False, rollback=self._undo_sustain),
            ActionStep("ramp_down", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        match step.name:
            case "ramp_up":
                self.logger.info("Fan ramping up", reading=proposal.parameters.get("reading"))
                await asyncio.sleep(0.5)   # simulate hardware ramp time
                return StepResult(step_name="ramp_up", success=True, output="100%")
            case "sustain":
                self.logger.info("Fan at full speed (mandatory block)")
                await asyncio.sleep(1.0)   # mandatory cooling period
                return StepResult(step_name="sustain", success=True)
            case "ramp_down":
                self.logger.info("Fan ramping down")
                await asyncio.sleep(0.3)
                return StepResult(step_name="ramp_down", success=True, output="0%")
            case _:
                return StepResult(step_name=step.name, success=False,
                                  error=ValueError(f"Unknown step: {step.name}"))

    async def _undo_sustain(self) -> None:
        """Rollback: spin the fan back down if we're interrupted mid-sustain."""
        self.logger.warning("Rolling back sustain — spinning fan down")
        await asyncio.sleep(0.1)


# ── Emergency stop action (triggered by reflex) ───────────────────────────────

class EmergencyStop(BaseActionNode):
    """Hard stop. Triggered by CriticalReflex, bypassing the decision node."""
    node_id   = "EmergencyStop"
    timeout_s = 2.0

    async def execute(self, proposal) -> Result:  # type: ignore[override]
        reading = proposal.parameters.get("reading", "?")
        self.logger.error("EMERGENCY STOP triggered", reading=reading)
        print(f"\n*** EMERGENCY STOP — temperature {reading} °C exceeded 85 °C ***\n")
        return Result(action_id=self.node_id, success=True,
                      output={"reason": "critical_temperature", "reading": reading})


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    log_sinks = [StdoutLogSink(level=LogLevel.INFO)]
    bus       = SignalBus()

    sense_master    = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    action_master   = ActionMasterNode(bus=bus)

    sense_master.register(SimTempSensor(bus=bus, log_sinks=log_sinks))
    instinct_master.register(WarnInstinct(bus=bus, log_sinks=log_sinks))
    instinct_master.register(CriticalReflex(bus=bus, log_sinks=log_sinks))
    action_master.register(CoolFan(bus=bus, log_sinks=log_sinks))
    action_master.register(EmergencyStop(bus=bus, log_sinks=log_sinks))

    rt = ArachniteRuntime(
        sense_master    = sense_master,
        context         = ContextNode(),
        instinct_master = instinct_master,
        decision_master = decision_master,
        action_master   = action_master,
        bus             = bus,
        tick_rate_hz    = 4.0,
        log_sinks       = log_sinks,
    )

    # Subscribe to all bus signals so we can print readings
    async def log_signal(sig: Signal) -> None:
        if sig.kind == "temperature":
            bar = "█" * int(sig.value / 5)
            print(f"  temp={sig.value:5.1f} °C  {bar}")

    bus.subscribe("temperature", log_signal)

    print("Temperature monitor running. Ctrl+C to stop.")
    print("─" * 60)
    await rt.start()
    try:
        await rt.wait()
    except KeyboardInterrupt:
        pass
    finally:
        await rt.stop()
    print(f"\nStopped after {rt.tick_count} ticks.")


if __name__ == "__main__":
    asyncio.run(main())
