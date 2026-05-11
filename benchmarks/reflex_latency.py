"""
benchmarks/reflex_latency.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Measures reflex arc end-to-end latency within the framework.

Definition of measured latency (§7.2.2):
  T_reflex = time from SenseNode.read() returning a signal that crosses
             the reflex threshold to EmergencyRetractAction.execute()
             being called.

This covers:
  ContextNode.update()  +  InstinctMasterNode.evaluate_reflexes()
  +  ActionMasterNode.dispatch() entry.

It does NOT include hardware I/O latency (sensor read time or actuator
write time), which are hardware-dependent and excluded from the framework
overhead measurement.

Protocol:
  - 1 SenseNode that returns a collision signal on the Nth tick
  - 1 CollisionReflex that fires on collision signal
  - 1 EmergencyRetractAction that records its start timestamp
  - 1,000 trials; each trial resets the sensor state
  - Latency = action_start_time - sense_read_time

Run:
    python benchmarks/reflex_latency.py
"""

from __future__ import annotations

import asyncio
import platform
import statistics
import sys
import time

from arachnite import (
    ActionMasterNode,
    ArachniteRuntime,
    BaseActionNode,
    BaseReflexInstinctNode,
    BaseSenseNode,
    Context,
    ContextNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    InstinctMasterNode,
    Proposal,
    Result,
    SenseMasterNode,
    Signal,
    SignalBus,
)
from benchmarks.stats import percentile

_TRIALS = 1_000


# ── Instrumented stub nodes ───────────────────────────────────────────────────

class _TimedProxSense(BaseSenseNode):
    """Emits a collision signal and records the exact read timestamp."""
    node_id         = "TimedProxSense"
    signal_kind     = "proximity"
    poll_interval_s = 0.0  # no throttle — read every tick

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.fire       = False
        self.read_time: float = 0.0

    async def read(self) -> Signal:
        value = 0.02 if self.fire else 1.0
        self.read_time = time.perf_counter()
        return Signal(source=self.node_id, kind=self.signal_kind,
                      value=value, confidence=1.0, timestamp=self.read_time)


class _TimedReflex(BaseReflexInstinctNode):
    node_id  = "TimedReflex"
    priority = 250

    async def evaluate(self, ctx: Context) -> Proposal | None:
        prox = [s for s in ctx.signals if s.kind == "proximity"]
        if prox and prox[-1].value < 0.05:
            return Proposal(instinct_id=self.node_id,
                            action_id="TimedRetract",
                            priority=self.priority, urgency=1.0)
        return None


class _TimedRetract(BaseActionNode):
    """Records the timestamp at the start of execute()."""
    node_id   = "TimedRetract"
    timeout_s = 1.0

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.exec_time: float = 0.0

    async def execute(self, proposal: Proposal) -> Result:
        self.exec_time = time.perf_counter()
        return Result(action_id=self.node_id, success=True)


# ── Benchmark runner ──────────────────────────────────────────────────────────

async def run() -> list[float]:
    bus     = SignalBus()
    sm      = SenseMasterNode(bus=bus)
    im      = InstinctMasterNode(bus=bus)
    dm      = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am      = ActionMasterNode(bus=bus)
    sense   = _TimedProxSense(bus=bus)
    reflex  = _TimedReflex(bus=bus)
    action  = _TimedRetract(bus=bus)

    sm.register(sense)
    im.register(reflex)
    am.register(action)

    rt = ArachniteRuntime(
        sense_master    = sm,
        context         = ContextNode(),
        instinct_master = im,
        decision_master = dm,
        action_master   = am,
        bus             = bus,
        tick_rate_hz    = 10_000.0,
    )

    # Bypass background tick loop (Bug B — see audit 2026-04-16).
    for m in (sm, im, dm, am):
        await m.setup()

    samples: list[float] = []
    for _ in range(_TRIALS):
        # prime tick (no collision)
        sense.fire = False
        await rt.tick()

        # fire tick (collision)
        sense.fire = True
        action.exec_time = 0.0
        await rt.tick()

        if action.exec_time > 0:
            latency_us = (action.exec_time - sense.read_time) * 1_000_000
            samples.append(latency_us)

        # reset
        sense.fire = False

    for m in (am, dm, im, sm):
        await m.teardown()
    return samples


def report(samples: list[float]) -> None:
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print(f"Trials   : {len(samples)}")
    print(f"Mean     : {statistics.mean(samples):8.1f} µs")
    print(f"Median   : {statistics.median(samples):8.1f} µs")
    print(f"P95      : {percentile(samples, 95.0):8.1f} µs")
    print(f"P99      : {percentile(samples, 99.0):8.1f} µs")
    print(f"Max      : {max(samples):8.1f} µs")
    print(f"Std Dev  : {statistics.stdev(samples):8.1f} µs")


if __name__ == "__main__":
    print("Arachnite reflex latency benchmark")
    print("Measures: sense.read() -> reflex fires -> action.execute() entry")
    print(f"Trials: {_TRIALS}")
    print("-" * 44)
    samples = asyncio.run(run())
    report(samples)
