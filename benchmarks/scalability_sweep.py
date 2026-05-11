"""
benchmarks/scalability_sweep.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Measures how tick latency scales with the number of nodes.

Protocol:
  - Variable SenseNodes + InstinctNodes + ActionNodes
  - Configurations: 3, 9, 15, 30, 75, 150 total nodes (1, 3, 5, 10, 25, 50 per type)
  - Each config: N sense, N instinct (none fires), N action (no-op)
  - 1,000 warm-up ticks, 5,000 measurement ticks per config
  - Metric: wall-clock time of ArachniteRuntime.tick()

Run:
    python benchmarks/scalability_sweep.py
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
_TICKS  = 5_000


# ── Factory helpers ──────────────────────────────────────────────────────────

def _make_sense_class(idx: int) -> type[BaseSenseNode]:
    class _Sense(BaseSenseNode):
        node_id     = f"Sense_{idx}"
        signal_kind = "bench"

        async def read(self) -> Signal:
            return Signal(source=self.node_id, kind=self.signal_kind,
                          value=1.0, confidence=1.0, timestamp=time.monotonic())
    return _Sense


def _make_instinct_class(idx: int) -> type[BaseInstinctNode]:
    class _Instinct(BaseInstinctNode):
        node_id  = f"Instinct_{idx}"
        priority = 50

        async def evaluate(self, ctx: Context) -> Proposal | None:
            return None
    return _Instinct


def _make_action_class(idx: int) -> type[BaseActionNode]:
    class _Action(BaseActionNode):
        node_id   = f"Action_{idx}"
        timeout_s = 1.0

        async def execute(self, proposal: Proposal) -> Result:
            return Result(action_id=self.node_id, success=True)
    return _Action


# ── Benchmark runner ─────────────────────────────────────────────────────────

async def run_config(n_per_type: int) -> list[float]:
    bus = SignalBus()
    sm  = SenseMasterNode(bus=bus)
    im  = InstinctMasterNode(bus=bus)
    dm  = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
    am  = ActionMasterNode(bus=bus)

    for i in range(n_per_type):
        sm.register(_make_sense_class(i)(bus=bus))
        im.register(_make_instinct_class(i)(bus=bus))
        am.register(_make_action_class(i)(bus=bus))

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

    for _ in range(_WARMUP):
        await rt.tick()

    samples: list[float] = []
    for _ in range(_TICKS):
        t0 = time.perf_counter()
        await rt.tick()
        samples.append((time.perf_counter() - t0) * 1_000)

    for m in (am, dm, im, sm):
        await m.teardown()
    return samples


def report_line(label: str, samples: list[float]) -> dict[str, float]:
    result = {
        "mean":   statistics.mean(samples),
        "median": statistics.median(samples),
        "p95":    percentile(samples, 95.0),
        "p99":    percentile(samples, 99.0),
        "stddev": statistics.stdev(samples),
    }
    print(f"  {label:>12s}  |  {result['mean']:7.3f}  {result['median']:7.3f}"
          f"  {result['p95']:7.3f}  {result['p99']:7.3f}  {result['stddev']:7.3f}")
    return result


async def main() -> None:
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print(f"Warm-up: {_WARMUP} ticks  |  Measurement: {_TICKS} ticks per config")
    print("-" * 72)
    print(f"  {'Config':>12s}  |  {'Mean':>7s}  {'Median':>7s}"
          f"  {'P95':>7s}  {'P99':>7s}  {'StdDev':>7s}   (all ms)")
    print("-" * 72)

    configs = [
        (1,  "3 nodes"),     # 1S + 1I + 1A = 3
        (3,  "9 nodes"),     # 3S + 3I + 3A = 9
        (5,  "15 nodes"),    # 5S + 5I + 5A = 15
        (10, "30 nodes"),    # 10 + 10 + 10
        (25, "75 nodes"),    # 25 + 25 + 25
        (50, "150 nodes"),   # 50 + 50 + 50
    ]

    for n_per_type, label in configs:
        samples = await run_config(n_per_type)
        report_line(label, samples)


if __name__ == "__main__":
    print("Arachnite scalability sweep benchmark")
    asyncio.run(main())
