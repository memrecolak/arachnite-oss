"""
benchmarks/reflex_stimulus_latency.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Stimulus-onset-to-action reflex latency under a live tick loop.

Motivation (revision experiment E1): ``reflex_latency.py`` starts its
clock inside ``SenseNode.read()``, so it measures the *pipeline-internal*
reflex latency from the sampling instant — it deliberately excludes the
time a stimulus waits between ticks before the next sense poll observes
it. At the default 10 Hz that wait is up to 100 ms and dominates the
end-to-end reaction. This benchmark measures from **stimulus onset**:

  T_total    = action.execute() entry − stimulus onset
  T_detect   = SenseNode.read() sampling instant − stimulus onset
               (the tick-quantization wait; ~Uniform(0, tick period))
  T_pipeline = action.execute() entry − sampling instant
               (identical measurand to reflex_latency.py)

so that T_total = T_detect + T_pipeline, and the worst-case
stimulus-to-action bound is (tick period + T_pipeline_p99).

Protocol:
  - Runtime started with its real background tick loop at ``--tick-rate``
    (default 10 Hz, the paper's reference rate).
  - A driver task sets a consume-once stimulus flag at a random phase
    within the tick period (seeded PRNG, uniform 0.2–0.8 of the period
    after an idle gap) and records the onset timestamp.
  - The next sense poll observes the flag, emits a collision signal, and
    records the sampling timestamp; the reflex bypasses the decision
    layer and the action records its entry timestamp.
  - One trial per stimulus; an inter-trial gap of one tick period ensures
    trials never overlap.

Caveat: on Windows the ~15.6 ms asyncio timer granularity phase-locks the
driver's random-offset sleep to the tick loop, skewing ``T_detect`` toward
0. Run reported measurements on Linux (~1 ms granularity), where onset
phases are effectively uniform over the tick period.

Run:
    python benchmarks/reflex_stimulus_latency.py
    python benchmarks/reflex_stimulus_latency.py --runs 5 --trials 200 --tick-rate 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import random
import statistics
import sys
import time
from dataclasses import asdict
from pathlib import Path

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
from benchmarks.stats import DescriptiveStats, format_stats_table

_RUNS = 5
_TRIALS = 200
_TICK_RATE_HZ = 10.0
_SEED = 20260810
# Onset phase window within the tick period (fraction of the period after the
# inter-trial gap). The campaign default 0.2-0.8 keeps every onset clear of the
# tick boundaries; 0.0-1.0 samples the whole period, boundaries included.
_PHASE_DEFAULT = (0.2, 0.8)

SERIES: tuple[str, ...] = ("total", "detect_wait", "pipeline")


# ── Instrumented stub nodes ──────────────────────────────────────────────────

class _StimulusSense(BaseSenseNode):
    """Consume-once stimulus flag; records the sampling instant."""

    node_id = "StimulusSense"
    signal_kind = "proximity"
    poll_interval_s = 0.0  # no throttle — read every tick

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.stim_pending = False
        self.read_time: float = 0.0

    async def read(self) -> Signal:
        if self.stim_pending:
            self.stim_pending = False  # consume once
            self.read_time = time.perf_counter()
            value = 0.02
        else:
            value = 1.0
        return Signal(source=self.node_id, kind=self.signal_kind,
                      value=value, confidence=1.0,
                      timestamp=time.perf_counter())


class _TimedReflex(BaseReflexInstinctNode):
    node_id = "TimedReflex"
    priority = 250

    async def evaluate(self, ctx: Context) -> Proposal | None:
        prox = [s for s in ctx.signals if s.kind == "proximity"]
        if prox and prox[-1].value < 0.05:
            return Proposal(instinct_id=self.node_id,
                            action_id="TimedRetract",
                            priority=self.priority, urgency=1.0)
        return None


class _TimedRetract(BaseActionNode):
    """Records the timestamp at the start of execute() and signals the driver."""

    node_id = "TimedRetract"
    timeout_s = 1.0

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.exec_time: float = 0.0
        self.fired: asyncio.Event = asyncio.Event()

    async def execute(self, proposal: Proposal) -> Result:
        self.exec_time = time.perf_counter()
        self.fired.set()
        return Result(action_id=self.node_id, success=True)


# ── Benchmark runner ─────────────────────────────────────────────────────────

async def run(
    trials: int = _TRIALS,
    tick_rate_hz: float = _TICK_RATE_HZ,
    rng: random.Random | None = None,
    phase: tuple[float, float] = _PHASE_DEFAULT,
) -> tuple[dict[str, list[float]], int]:
    """One independent run under a live tick loop.

    Returns ({series: samples_ms}, missed_trials). A trial is missed if
    the reflex action does not fire within five tick periods (never
    expected; counted rather than silently dropped).
    """
    rng = rng or random.Random(_SEED)
    period_s = 1.0 / tick_rate_hz

    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am = ActionMasterNode(bus=bus)
    sense = _StimulusSense(bus=bus)
    reflex = _TimedReflex(bus=bus)
    action = _TimedRetract(bus=bus)

    sm.register(sense)
    im.register(reflex)
    am.register(action)

    rt = ArachniteRuntime(
        sense_master=sm,
        context=ContextNode(),
        instinct_master=im,
        decision_master=dm,
        action_master=am,
        bus=bus,
        tick_rate_hz=tick_rate_hz,
    )

    samples: dict[str, list[float]] = {name: [] for name in SERIES}
    missed = 0

    await rt.start()
    try:
        # Let the loop settle before the first stimulus.
        await asyncio.sleep(period_s * 3)

        for _ in range(trials):
            # Random phase within the tick period so onsets are uniformly
            # distributed relative to the sense poll.
            lo, hi = phase
            await asyncio.sleep(period_s * (lo + (hi - lo) * rng.random()))

            action.fired.clear()
            action.exec_time = 0.0
            sense.read_time = 0.0
            onset = time.perf_counter()
            sense.stim_pending = True

            try:
                await asyncio.wait_for(action.fired.wait(),
                                       timeout=period_s * 5)
            except asyncio.TimeoutError:
                missed += 1
                sense.stim_pending = False
                continue

            samples["total"].append((action.exec_time - onset) * 1_000.0)
            samples["detect_wait"].append((sense.read_time - onset) * 1_000.0)
            samples["pipeline"].append(
                (action.exec_time - sense.read_time) * 1_000.0
            )

            # Inter-trial gap: one full period so trials never overlap.
            await asyncio.sleep(period_s)
    finally:
        await rt.stop()

    return samples, missed


async def multi_run(
    n_runs: int,
    trials: int = _TRIALS,
    tick_rate_hz: float = _TICK_RATE_HZ,
    phase: tuple[float, float] = _PHASE_DEFAULT,
) -> tuple[dict[str, DescriptiveStats], int]:
    run_samples_by_name: dict[str, list[list[float]]] = {n: [] for n in SERIES}
    run_medians_by_name: dict[str, list[float]] = {n: [] for n in SERIES}
    pooled_by_name: dict[str, list[float]] = {n: [] for n in SERIES}
    total_missed = 0

    for i in range(n_runs):
        # Distinct seed per run — independent stimulus phases, reproducible.
        rng = random.Random(_SEED + i)
        samples, missed = await run(trials, tick_rate_hz, rng, phase)
        total_missed += missed
        for name in SERIES:
            if not samples[name]:
                continue
            run_samples_by_name[name].append(samples[name])
            run_medians_by_name[name].append(statistics.median(samples[name]))
            pooled_by_name[name].extend(samples[name])
        print(
            f"    Run {i + 1:>3d}/{n_runs}: "
            f"total={statistics.median(samples['total']):.1f} ms  "
            f"detect={statistics.median(samples['detect_wait']):.1f} ms  "
            f"pipeline={statistics.median(samples['pipeline']):.3f} ms  "
            f"missed={missed}"
        )

    stats_by_name = {
        name: DescriptiveStats.from_runs(
            run_medians_by_name[name],
            pooled_by_name[name],
            trials,
            run_samples=run_samples_by_name[name],
        )
        for name in SERIES
        if pooled_by_name[name]
    }
    return stats_by_name, total_missed


def report(
    stats_by_name: dict[str, DescriptiveStats],
    tick_rate_hz: float,
    missed: int,
) -> None:
    period_ms = 1_000.0 / tick_rate_hz
    print()
    print(f"Stimulus-onset reflex latency @ {tick_rate_hz:g} Hz "
          f"(tick period {period_ms:.1f} ms)")
    print("-" * 72)
    for name in SERIES:
        if name in stats_by_name:
            print(format_stats_table(name, stats_by_name[name], "ms"))
    if missed:
        print(f"  MISSED TRIALS: {missed} (reflex did not fire within 5 periods)")
    if "pipeline" in stats_by_name:
        bound = period_ms + stats_by_name["pipeline"].p99
        print(f"  Worst-case stimulus-to-action bound "
              f"(period + pipeline P99): {bound:.1f} ms")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Stimulus-onset-to-action reflex latency under a live tick "
            "loop; decomposes total reaction into tick-quantization wait "
            "and pipeline-internal reflex latency."
        ),
    )
    parser.add_argument(
        "--runs", "-n", type=int, default=_RUNS,
        help=f"Independent runs (default: {_RUNS}).",
    )
    parser.add_argument(
        "--trials", "-t", type=int, default=_TRIALS,
        help=f"Stimulus trials per run (default: {_TRIALS}).",
    )
    parser.add_argument(
        "--tick-rate", type=float, default=_TICK_RATE_HZ,
        help=f"Runtime tick rate in Hz (default: {_TICK_RATE_HZ:g}).",
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default="benchmarks/results",
        help="Directory for the JSON output file.",
    )
    parser.add_argument(
        "--phase-min", type=float, default=_PHASE_DEFAULT[0],
        help=f"Earliest onset phase within the tick period (default: {_PHASE_DEFAULT[0]:g}).",
    )
    parser.add_argument(
        "--phase-max", type=float, default=_PHASE_DEFAULT[1],
        help=f"Latest onset phase within the tick period (default: {_PHASE_DEFAULT[1]:g}).",
    )
    args = parser.parse_args()
    if not 0.0 <= args.phase_min < args.phase_max <= 1.0:
        parser.error("--phase-min/--phase-max must satisfy 0 <= min < max <= 1")
    phase = (args.phase_min, args.phase_max)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    period_ms = 1_000.0 / args.tick_rate
    est_s = args.runs * args.trials * (period_ms / 1_000.0) * 2.1
    print("Arachnite stimulus-onset reflex latency benchmark")
    print(f"Trials per run: {args.trials}  |  Runs: {args.runs}  |  "
          f"Tick rate: {args.tick_rate:g} Hz  |  Onset phase: "
          f"{phase[0]:g}-{phase[1]:g}")
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print(f"Estimated duration: ~{est_s / 60.0:.1f} min "
          "(trials are paced by the live tick loop)")
    print("-" * 72)

    stats_by_name, missed = asyncio.run(
        multi_run(args.runs, args.trials, args.tick_rate, phase)
    )
    report(stats_by_name, args.tick_rate, missed)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"reflex_stimulus_latency_{timestamp}.json"
    payload = {
        "benchmark": "reflex_stimulus_latency",
        "unit": "ms",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "n_runs": args.runs,
        "trials_per_run": args.trials,
        "tick_rate_hz": args.tick_rate,
        "tick_period_ms": period_ms,
        "seed": _SEED,
        "onset_phase": [phase[0], phase[1]],
        "missed_trials": missed,
        "series": {
            name: asdict(stats) for name, stats in stats_by_name.items()
        },
        "worst_case_bound_ms": (
            period_ms + stats_by_name["pipeline"].p99
            if "pipeline" in stats_by_name else None
        ),
    }
    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nResults saved to {out_file}")


if __name__ == "__main__":
    main()
