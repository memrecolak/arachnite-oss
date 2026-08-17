"""
benchmarks/gil_contention.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reflex latency under co-resident CPU-bound load (GIL contention).

Motivation (revision experiment E3): in a CPython asyncio runtime, a
regular node performing CPU-bound work (e.g. parsing a large JSON sensor
payload) holds the GIL and stalls the event loop, delaying every other
coroutine — including the reflex path. This benchmark quantifies that
degradation instead of arguing it qualitatively. It reuses the
stimulus-onset rig from ``reflex_stimulus_latency.py`` (same decomposition:
``total = detect_wait + pipeline``) and adds a co-resident instinct node
that performs the CPU-bound work once per tick, in three arms:

  no_load    — the load node returns immediately (baseline; should
               reproduce reflex_stimulus_latency.py numbers).
  naive      — the load node parses the payload synchronously inside its
               async ``evaluate()``, blocking the event loop for the full
               parse duration every tick.
  to_thread  — the identical parse is delegated via ``asyncio.to_thread()``,
               the mitigation the framework documentation prescribes; the
               event loop stays responsive while the worker thread holds
               the GIL only between bytecode boundaries.

The payload is a synthetic camera/detection JSON document of ``--payload-kb``
kilobytes, built once and parsed ``--load-reps`` times per tick. Parse cost
is calibrated (median of 20 solo parses); by default the rep count is derived
from ``--target-load-pct`` (stall as a fraction of the tick period), so the
*relative* injected load is comparable across hosts of different speeds. The
calibrated per-parse cost and derived reps are recorded in the output.

Run:
    python benchmarks/gil_contention.py
    python benchmarks/gil_contention.py --runs 5 --trials 200 --payload-kb 512
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
    BaseInstinctNode,
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
_PAYLOAD_KB = 512
_TARGET_LOAD_PCT = 20.0  # naive-arm stall as % of the tick period
_SEED = 20260812

ARMS: tuple[str, ...] = ("no_load", "naive", "to_thread")
SERIES: tuple[str, ...] = ("total", "detect_wait", "pipeline")


# ── Synthetic CPU-bound payload ──────────────────────────────────────────────

def build_payload(payload_kb: int, rng: random.Random) -> str:
    """A realistic sensor JSON document of ~payload_kb kilobytes: a float
    array (camera/depth frame) plus nested detection metadata."""
    target = payload_kb * 1024
    # ~10 bytes per serialized float entry ("1.234567,"); guard tiny payloads.
    n_floats = max(64, target // 10)
    doc = {
        "frame_id": 1,
        "stamp": 1234567890.123456,
        "detections": [
            {"cls": "obstacle", "conf": 0.97, "bbox": [0.1, 0.2, 0.3, 0.4]}
            for _ in range(32)
        ],
        "depth": [round(rng.random() * 10.0, 6) for _ in range(n_floats)],
    }
    return json.dumps(doc)


def calibrate_parse_ms(payload: str) -> float:
    """Median duration of one solo json.loads() of the payload."""
    times = []
    for _ in range(20):
        t0 = time.perf_counter()
        json.loads(payload)
        times.append((time.perf_counter() - t0) * 1_000.0)
    return statistics.median(times)


# ── Instrumented stub nodes (rig shared with reflex_stimulus_latency) ────────

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


class _PayloadLoadInstinct(BaseInstinctNode):
    """Co-resident CPU-bound load: parses the payload every tick.

    ``mode`` selects the arm; the parsed result is discarded — only the
    CPU/GIL cost matters. Never proposes an action.
    """

    node_id = "PayloadLoad"
    priority = 10

    def __init__(self, payload: str, reps: int, mode: str,
                 **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._payload = payload
        self._reps = reps
        self.mode = mode

    def _parse(self) -> None:
        for _ in range(self._reps):
            json.loads(self._payload)

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if self.mode == "naive":
            self._parse()
        elif self.mode == "to_thread":
            await asyncio.to_thread(self._parse)
        # "no_load": fall straight through
        return None


# ── Benchmark runner ─────────────────────────────────────────────────────────

async def run_arm(
    mode: str,
    payload: str,
    load_reps: int,
    trials: int = _TRIALS,
    tick_rate_hz: float = _TICK_RATE_HZ,
    rng: random.Random | None = None,
) -> tuple[dict[str, list[float]], int]:
    """One independent run of one arm under a live tick loop.

    Returns ({series: samples_ms}, missed_trials). A trial is missed if
    the reflex action does not fire within five tick periods.
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
    load = _PayloadLoadInstinct(payload, load_reps, mode, bus=bus)
    action = _TimedRetract(bus=bus)

    sm.register(sense)
    im.register(reflex)
    im.register(load)
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
        # Let the loop settle (and the load node reach steady state).
        await asyncio.sleep(period_s * 3)

        for _ in range(trials):
            # Random phase within the tick period so onsets land uniformly —
            # including mid-parse in the naive arm, which is the case the
            # benchmark exists to expose.
            await asyncio.sleep(period_s * (0.2 + 0.6 * rng.random()))

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
    payload: str,
    load_reps: int,
    trials: int = _TRIALS,
    tick_rate_hz: float = _TICK_RATE_HZ,
) -> tuple[dict[str, dict[str, DescriptiveStats]], dict[str, int]]:
    stats_by_arm: dict[str, dict[str, DescriptiveStats]] = {}
    missed_by_arm: dict[str, int] = {}

    for mode in ARMS:
        print(f"  Arm: {mode}")
        run_samples_by_name: dict[str, list[list[float]]] = {n: [] for n in SERIES}
        run_medians_by_name: dict[str, list[float]] = {n: [] for n in SERIES}
        pooled_by_name: dict[str, list[float]] = {n: [] for n in SERIES}
        total_missed = 0

        for i in range(n_runs):
            # Distinct seed per run — independent phases, reproducible;
            # identical seed sequence across arms for paired comparison.
            rng = random.Random(_SEED + i)
            samples, missed = await run_arm(
                mode, payload, load_reps, trials, tick_rate_hz, rng
            )
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

        stats_by_arm[mode] = {
            name: DescriptiveStats.from_runs(
                run_medians_by_name[name],
                pooled_by_name[name],
                trials,
                run_samples=run_samples_by_name[name],
            )
            for name in SERIES
            if pooled_by_name[name]
        }
        missed_by_arm[mode] = total_missed

    return stats_by_arm, missed_by_arm


def report(
    stats_by_arm: dict[str, dict[str, DescriptiveStats]],
    missed_by_arm: dict[str, int],
    tick_rate_hz: float,
    parse_ms: float,
    load_reps: int,
) -> None:
    period_ms = 1_000.0 / tick_rate_hz
    print()
    print(f"Reflex latency under co-resident CPU-bound load @ "
          f"{tick_rate_hz:g} Hz (tick period {period_ms:.1f} ms)")
    print(f"Injected load: {parse_ms:.2f} ms/parse x {load_reps} rep(s) "
          f"per tick (calibrated solo)")
    print("=" * 72)
    for mode in ARMS:
        print(f"\n[{mode}]")
        print("-" * 72)
        for name in SERIES:
            if name in stats_by_arm.get(mode, {}):
                print(format_stats_table(name, stats_by_arm[mode][name], "ms"))
        if missed_by_arm.get(mode):
            print(f"  MISSED TRIALS: {missed_by_arm[mode]} "
                  "(reflex did not fire within 5 periods)")

    base = stats_by_arm.get("no_load", {}).get("pipeline")
    if base and base.p99:
        print()
        print("Pipeline P99 degradation vs no_load:")
        for mode in ("naive", "to_thread"):
            s = stats_by_arm.get(mode, {}).get("pipeline")
            if s:
                print(f"  {mode:<10} {s.p99:>8.3f} ms  "
                      f"({s.p99 / base.p99:.1f}x)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Reflex latency percentiles while a co-resident node performs "
            "CPU-bound JSON parsing: no load vs naive in-loop vs "
            "asyncio.to_thread delegation."
        ),
    )
    parser.add_argument(
        "--runs", "-n", type=int, default=_RUNS,
        help=f"Independent runs per arm (default: {_RUNS}).",
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
        "--payload-kb", type=int, default=_PAYLOAD_KB,
        help=f"Synthetic JSON payload size in KB (default: {_PAYLOAD_KB}).",
    )
    parser.add_argument(
        "--load-reps", type=int, default=0,
        help="Payload parses per tick; 0 (default) derives the count from "
             "--target-load-pct via calibration.",
    )
    parser.add_argument(
        "--target-load-pct", type=float, default=_TARGET_LOAD_PCT,
        help="Stall injected per tick as %% of the tick period, used to "
             f"derive --load-reps when it is 0 (default: {_TARGET_LOAD_PCT:g}). "
             "Calibration-based, so the relative load is comparable across "
             "hosts of different speeds.",
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default="benchmarks/results",
        help="Directory for the JSON output file.",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = build_payload(args.payload_kb, random.Random(_SEED))
    parse_ms = calibrate_parse_ms(payload)

    period_ms = 1_000.0 / args.tick_rate
    load_reps = args.load_reps
    if load_reps <= 0:
        target_ms = period_ms * args.target_load_pct / 100.0
        load_reps = max(1, round(target_ms / parse_ms))
    args.load_reps = load_reps

    est_s = len(ARMS) * args.runs * args.trials * (period_ms / 1_000.0) * 2.1
    print("Arachnite GIL-contention reflex latency benchmark")
    print(f"Arms: {', '.join(ARMS)}  |  Runs/arm: {args.runs}  |  "
          f"Trials: {args.trials}  |  Tick rate: {args.tick_rate:g} Hz")
    print(f"Payload : {len(payload) / 1024:.0f} KB JSON, "
          f"{parse_ms:.2f} ms/parse solo, {load_reps} rep(s)/tick "
          f"(~{parse_ms * load_reps:.1f} ms = "
          f"{parse_ms * load_reps / period_ms * 100.0:.0f}% of the period)")
    print(f"Platform: {sys.platform} / CPython {platform.python_version()}")
    print(f"Estimated duration: ~{est_s / 60.0:.1f} min "
          "(trials are paced by the live tick loop)")
    print("-" * 72)

    stats_by_arm, missed_by_arm = asyncio.run(
        multi_run(args.runs, payload, args.load_reps,
                  args.trials, args.tick_rate)
    )
    report(stats_by_arm, missed_by_arm, args.tick_rate, parse_ms,
           args.load_reps)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"gil_contention_{timestamp}.json"
    payload_doc = {
        "benchmark": "gil_contention",
        "unit": "ms",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "n_runs": args.runs,
        "trials_per_run": args.trials,
        "tick_rate_hz": args.tick_rate,
        "tick_period_ms": period_ms,
        "payload_kb": args.payload_kb,
        "payload_bytes": len(payload),
        "load_reps_per_tick": args.load_reps,
        "calibrated_parse_ms": parse_ms,
        "seed": _SEED,
        "arms": {
            mode: {
                "missed_trials": missed_by_arm.get(mode, 0),
                "series": {
                    name: asdict(stats)
                    for name, stats in stats_by_arm.get(mode, {}).items()
                },
            }
            for mode in ARMS
        },
    }
    with open(out_file, "w") as f:
        json.dump(payload_doc, f, indent=2)
    print(f"\nResults saved to {out_file}")


if __name__ == "__main__":
    main()
