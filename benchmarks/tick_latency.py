"""
benchmarks/tick_latency.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Measures Arachnite framework tick overhead.

Protocol:
  - 3 SenseNodes returning scalar values
  - 2 InstinctNodes (1 normal + 1 reflex, neither fires)
  - 1 WeightedDecisionNode
  - 1 ActionNode (no-op)
  - 10,000 warm-up ticks discarded; 10,000 measurement ticks recorded
  - Metric: wall-clock time of ArachniteRuntime.tick() call
    (excludes asyncio.sleep scheduling overhead from the tick loop)

Run:
    python benchmarks/tick_latency.py

Output (example on x86-64):
    Platform : win32 / CPython 3.12.x
    Ticks    : 10000
    Mean     :  0.312 ms
    Median   :  0.298 ms
    P95      :  0.441 ms
    P99      :  0.613 ms
    Max      :  2.104 ms
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
    BaseInstinctNode,
    BaseReflexInstinctNode,
    BaseSenseNode,
    Context,
    ContextNode,
    DecisionMasterNode,
    InstinctMasterNode,
    Proposal,
    Result,
    SenseMasterNode,
    Signal,
    SignalBus,
    WeightedDecisionNode,
)
from benchmarks.stats import percentile

_WARMUP = 1_000
_TICKS  = 10_000


# ── Minimal stub nodes ────────────────────────────────────────────────────────

class _ScalarSense(BaseSenseNode):
    node_id     = "BenchSense0"
    signal_kind = "bench"

    async def read(self) -> Signal:
        return Signal(source=self.node_id, kind=self.signal_kind,
                      value=1.0, confidence=1.0, timestamp=time.monotonic())


class _ScalarSense1(_ScalarSense):
    node_id = "BenchSense1"


class _ScalarSense2(_ScalarSense):
    node_id = "BenchSense2"


class _NopInstinct(BaseInstinctNode):
    node_id  = "BenchInstinct"
    priority = 50

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return None


class _NopReflex(BaseReflexInstinctNode):
    node_id  = "BenchReflex"
    priority = 200

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return None


class _NopAction(BaseActionNode):
    node_id   = "BenchAction"
    timeout_s = 1.0

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


# ── Benchmark runner ──────────────────────────────────────────────────────────

async def run() -> list[float]:
    bus = SignalBus()
    sm  = SenseMasterNode(bus=bus)
    im  = InstinctMasterNode(bus=bus)
    dm  = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
    am  = ActionMasterNode(bus=bus)

    for cls in (_ScalarSense, _ScalarSense1, _ScalarSense2):
        sm.register(cls(bus=bus))
    im.register(_NopInstinct(bus=bus))
    im.register(_NopReflex(bus=bus))
    am.register(_NopAction(bus=bus))

    rt = ArachniteRuntime(
        sense_master    = sm,
        context         = ContextNode(),
        instinct_master = im,
        decision_master = dm,
        action_master   = am,
        bus             = bus,
        tick_rate_hz    = 10_000.0,   # as fast as possible; sleep is skipped
    )

    # Bypass rt.start()/stop() so the background tick loop never runs
    # alongside our manual tick() calls. Otherwise both loops increment
    # _tick_count and contend for the event loop, contaminating samples.
    for m in (sm, im, dm, am):
        await m.setup()

    # warm-up
    for _ in range(_WARMUP):
        await rt.tick()

    # measure
    samples: list[float] = []
    for _ in range(_TICKS):
        t0 = time.perf_counter()
        await rt.tick()
        samples.append((time.perf_counter() - t0) * 1_000)   # → ms

    for m in (am, dm, im, sm):
        await m.teardown()
    return samples


def report(samples: list[float]) -> None:
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print(f"Ticks    : {len(samples)}")
    print(f"Mean     : {statistics.mean(samples):7.3f} ms")
    print(f"Median   : {statistics.median(samples):7.3f} ms")
    print(f"P95      : {percentile(samples, 95.0):7.3f} ms")
    print(f"P99      : {percentile(samples, 99.0):7.3f} ms")
    print(f"Max      : {max(samples):7.3f} ms")
    print(f"Std Dev  : {statistics.stdev(samples):7.3f} ms")


if __name__ == "__main__":
    print("Arachnite tick latency benchmark")
    print(f"Warm-up: {_WARMUP} ticks  |  Measurement: {_TICKS} ticks")
    print("-" * 44)
    samples = asyncio.run(run())
    report(samples)
