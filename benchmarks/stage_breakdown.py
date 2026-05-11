"""
benchmarks/stage_breakdown.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Per-stage tick latency breakdown.

Slices the existing tick latency into the six pipeline stages defined by
``arachnite.runtime.TICK_STAGE_NAMES``:

    sense → context → reflex → instinct → decide → act

The runtime exposes a ``TickInstrumenter`` protocol with a no-op default.
This benchmark attaches a lightweight ``StageTimingCollector`` that
appends per-stage durations to pre-allocated lists and then produces a
``DescriptiveStats`` per stage via ``DescriptiveStats.from_runs`` — the
same statistical machinery as every other benchmark in the suite
(bootstrap CIs for median / P95 / P99).

Protocol (identical node topology to ``tick_latency.py`` so the
aggregate numbers are directly comparable):
  - 3 SenseNodes returning scalar values
  - 2 InstinctNodes (1 normal + 1 reflex, neither fires)
  - 1 WeightedDecisionNode
  - 1 ActionNode (no-op)
  - 1,000 warm-up ticks discarded; 10,000 measurement ticks recorded
  - Metric: wall-clock duration of each tick stage, in milliseconds.

Run:
    python benchmarks/stage_breakdown.py
    python benchmarks/stage_breakdown.py --runs 5 --ticks 2000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from dataclasses import asdict
from pathlib import Path

from arachnite import (
    TICK_STAGE_NAMES,
    ActionMasterNode,
    ArachniteRuntime,
    ContextNode,
    DecisionMasterNode,
    InstinctMasterNode,
    SenseMasterNode,
    SignalBus,
    WeightedDecisionNode,
)
from benchmarks.stats import DescriptiveStats, format_stats_table
from benchmarks.tick_latency import (
    _NopAction,
    _NopInstinct,
    _NopReflex,
    _ScalarSense,
    _ScalarSense1,
    _ScalarSense2,
)

_WARMUP = 1_000
_TICKS = 10_000


# ── Benchmark-private collector ──────────────────────────────────────────────
#
# This class implements the ``TickInstrumenter`` structural protocol but is
# **not** exported from ``arachnite/__init__.py``. Benchmark-side utilities
# are not part of the framework's public surface — users who need a
# collector write their own (the protocol is the public contract).


class StageTimingCollector:
    """Append-only per-stage timing sink for ``ArachniteRuntime``.

    Each call to ``on_stage(name, duration_s)`` appends the duration (in
    *milliseconds* — the unit consumed by ``DescriptiveStats``) to the
    per-stage list. ``on_tick_complete(tick, total_s)`` appends the total
    to ``totals`` so the caller can sanity-check that the stage deltas sum
    to approximately the total (a small positive drift is expected — it
    reflects the cost of the instrumenter calls themselves and the handful
    of lines between stages).
    """

    stage_names: tuple[str, ...] = TICK_STAGE_NAMES

    def __init__(self) -> None:
        self.samples: dict[str, list[float]] = {name: [] for name in self.stage_names}
        self.totals: list[float] = []

    def on_stage(self, stage: str, duration_s: float) -> None:
        # Defensive: ignore unknown stage names rather than raising. The
        # runtime's contract guarantees only names from TICK_STAGE_NAMES,
        # but a collector that never raises is easier to debug.
        bucket = self.samples.get(stage)
        if bucket is not None:
            bucket.append(duration_s * 1_000.0)

    def on_tick_complete(self, tick_index: int, total_s: float) -> None:
        self.totals.append(total_s * 1_000.0)

    def reset(self) -> None:
        """Clear all collected samples (used to discard warm-up ticks)."""
        for bucket in self.samples.values():
            bucket.clear()
        self.totals.clear()


# ── Benchmark runner ─────────────────────────────────────────────────────────


async def run(ticks: int = _TICKS, warmup: int = _WARMUP) -> dict[str, list[float]]:
    """Run a single stage-breakdown measurement and return per-stage samples.

    Returns a dict with one ``list[float]`` per stage name plus a ``totals``
    entry. All values are in milliseconds, index-aligned across stages.
    """
    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
    am = ActionMasterNode(bus=bus)

    for cls in (_ScalarSense, _ScalarSense1, _ScalarSense2):
        sm.register(cls(bus=bus))
    im.register(_NopInstinct(bus=bus))
    im.register(_NopReflex(bus=bus))
    am.register(_NopAction(bus=bus))

    collector = StageTimingCollector()

    rt = ArachniteRuntime(
        sense_master=sm,
        context=ContextNode(),
        instinct_master=im,
        decision_master=dm,
        action_master=am,
        bus=bus,
        tick_rate_hz=10_000.0,
        tick_instrumenter=collector,
    )

    # Bypass background tick loop — same discipline as tick_latency.py.
    for m in (sm, im, dm, am):
        await m.setup()

    # Warm-up (samples discarded wholesale after the loop)
    for _ in range(warmup):
        await rt.tick()
    collector.reset()

    # Measurement
    for _ in range(ticks):
        await rt.tick()

    for m in (am, dm, im, sm):
        await m.teardown()

    result: dict[str, list[float]] = {
        name: list(collector.samples[name]) for name in TICK_STAGE_NAMES
    }
    result["totals"] = list(collector.totals)
    return result


# ── Multi-run aggregation ────────────────────────────────────────────────────


async def multi_run(
    n_runs: int, ticks: int = _TICKS, warmup: int = _WARMUP,
) -> dict[str, DescriptiveStats]:
    """Run the benchmark ``n_runs`` times and produce per-stage stats.

    Each run feeds ``DescriptiveStats.from_runs`` its own median and full
    sample list, exactly as ``tick_latency`` does; the ``run_samples``
    kwarg enables Bench-4's per-run P95/P99 bootstrap.
    """
    # Collect per-run per-stage samples
    run_samples_by_stage: dict[str, list[list[float]]] = {
        name: [] for name in TICK_STAGE_NAMES
    }
    run_medians_by_stage: dict[str, list[float]] = {
        name: [] for name in TICK_STAGE_NAMES
    }
    pooled_by_stage: dict[str, list[float]] = {
        name: [] for name in TICK_STAGE_NAMES
    }

    for i in range(n_runs):
        samples_dict = await run(ticks=ticks, warmup=warmup)
        for name in TICK_STAGE_NAMES:
            stage_samples = samples_dict[name]
            run_samples_by_stage[name].append(stage_samples)
            run_medians_by_stage[name].append(statistics.median(stage_samples))
            pooled_by_stage[name].extend(stage_samples)
        # Per-run summary for console output
        medians = {
            name: statistics.median(samples_dict[name]) for name in TICK_STAGE_NAMES
        }
        line = f"    Run {i + 1:>3d}/{n_runs}: " + "  ".join(
            f"{name}={medians[name]:.3f} ms" for name in TICK_STAGE_NAMES
        )
        print(line)

    stats_by_stage: dict[str, DescriptiveStats] = {}
    for name in TICK_STAGE_NAMES:
        stats_by_stage[name] = DescriptiveStats.from_runs(
            run_medians_by_stage[name],
            pooled_by_stage[name],
            ticks,
            run_samples=run_samples_by_stage[name],
        )
    return stats_by_stage


# ── Reporting ────────────────────────────────────────────────────────────────


def report(stats_by_stage: dict[str, DescriptiveStats]) -> None:
    print()
    print("Per-stage tick latency breakdown (ms)")
    print("-" * 44)
    for name in TICK_STAGE_NAMES:
        print(format_stats_table(name, stats_by_stage[name], "ms"))


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Per-stage tick latency breakdown.",
    )
    parser.add_argument(
        "--runs", "-n", type=int, default=5,
        help="Independent runs (default: 5).",
    )
    parser.add_argument(
        "--ticks", "-t", type=int, default=_TICKS,
        help=f"Measurement ticks per run (default: {_TICKS}).",
    )
    parser.add_argument(
        "--warmup", "-w", type=int, default=_WARMUP,
        help=f"Warm-up ticks per run (default: {_WARMUP}).",
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default="benchmarks/results",
        help="Directory for the JSON output file.",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Arachnite stage-breakdown benchmark")
    print(
        f"Warm-up: {args.warmup} ticks  |  Measurement: {args.ticks} ticks  "
        f"|  Runs: {args.runs}"
    )
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print("-" * 44)

    stats_by_stage = asyncio.run(multi_run(args.runs, args.ticks, args.warmup))
    report(stats_by_stage)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"stage_breakdown_{timestamp}.json"
    payload = {
        "benchmark": "stage_breakdown",
        "unit": "ms",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "n_runs": args.runs,
        "samples_per_run": args.ticks,
        "stages": {name: asdict(stats_by_stage[name]) for name in TICK_STAGE_NAMES},
    }
    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nResults saved to {out_file}")


if __name__ == "__main__":
    main()
