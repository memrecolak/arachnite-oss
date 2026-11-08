"""
benchmarks/reflex_depth_sweep.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reflex latency as a function of deliberative pipeline depth.

Same rig and measurand as ``reflex_latency.py`` (T_reflex = sense.read()
returning the collision signal -> EmergencyRetract.execute() entry; 1 timed
sense, 1 timed reflex, 1 timed action; two manual ticks per trial, no
warm-up), with a population of deliberative instinct nodes added to the
instinct master. The deliberative instincts never fire; they are evaluated
by the ordinary instinct stage *after* the reflex stage on every tick, so
they model the "deliberative pipeline depth" the reflex path short-circuits.

A second, smaller axis adds extra sense nodes while the instinct count is
zero: ``ContextNode.update()`` and the reflex evaluation iterate over the
signals of the tick, so the reflex path is expected to depend on the number
of *signals*, not on the number of deliberative instincts.

Configurations (default):
    instincts 0 / 10 / 50 / 150 with 1 sense node
    senses    +10 / +50 extra sense nodes with 0 deliberative instincts

Run:
    python benchmarks/reflex_depth_sweep.py --runs 30 --trials 1000 -o <dir>
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
from datetime import datetime
from pathlib import Path
from typing import Any

from arachnite import (
    ActionMasterNode,
    ArachniteRuntime,
    BaseInstinctNode,
    BaseSenseNode,
    Context,
    ContextNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    InstinctMasterNode,
    Proposal,
    SenseMasterNode,
    Signal,
    SignalBus,
)
from benchmarks.reflex_latency import _TimedProxSense, _TimedReflex, _TimedRetract
from benchmarks.stats import DescriptiveStats, percentile

_INSTINCT_LEVELS: tuple[int, ...] = (0, 10, 50, 150)
_SENSE_LEVELS: tuple[int, ...] = (10, 50)


def _make_instinct_class(idx: int) -> type[BaseInstinctNode]:
    class _Instinct(BaseInstinctNode):
        node_id = f"DelibInstinct_{idx}"
        priority = 50

        async def evaluate(self, ctx: Context) -> Proposal | None:
            # Touch the signals the way a real instinct would, never fire.
            for s in ctx.signals:
                if s.kind == "never":
                    return Proposal(instinct_id=self.node_id, action_id="TimedRetract",
                                    priority=self.priority, urgency=0.1)
            return None
    return _Instinct


def _make_sense_class(idx: int) -> type[BaseSenseNode]:
    class _Sense(BaseSenseNode):
        node_id = f"ExtraSense_{idx}"
        signal_kind = "bench"
        poll_interval_s = 0.0

        async def read(self) -> Signal:
            return Signal(source=self.node_id, kind=self.signal_kind,
                          value=1.0, confidence=1.0, timestamp=time.monotonic())
    return _Sense


async def run(n_instincts: int, n_extra_senses: int, trials: int) -> list[float]:
    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am = ActionMasterNode(bus=bus)
    sense = _TimedProxSense(bus=bus)
    reflex = _TimedReflex(bus=bus)
    action = _TimedRetract(bus=bus)

    sm.register(sense)
    for i in range(n_extra_senses):
        sm.register(_make_sense_class(i)(bus=bus))
    im.register(reflex)
    for i in range(n_instincts):
        im.register(_make_instinct_class(i)(bus=bus))
    am.register(action)

    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(), instinct_master=im,
        decision_master=dm, action_master=am, bus=bus, tick_rate_hz=10_000.0,
    )
    for m in (sm, im, dm, am):
        await m.setup()

    samples: list[float] = []
    for _ in range(trials):
        sense.fire = False
        await rt.tick()
        sense.fire = True
        action.exec_time = 0.0
        await rt.tick()
        if action.exec_time > 0:
            samples.append((action.exec_time - sense.read_time) * 1_000_000)
        sense.fire = False
        await rt.wait_idle()

    for m in (am, dm, im, sm):
        await m.teardown()
    return samples


async def run_config(n_instincts: int, n_extra_senses: int, n_runs: int,
                     trials: int) -> dict[str, Any]:
    run_medians: list[float] = []
    all_samples: list[float] = []
    run_samples: list[list[float]] = []
    t0 = time.perf_counter()
    for _ in range(n_runs):
        s = await run(n_instincts, n_extra_senses, trials)
        run_medians.append(statistics.median(s))
        run_samples.append(s)
        all_samples.extend(s)
    stats = DescriptiveStats.from_runs(run_medians, all_samples, trials, run_samples)
    return {
        "n_instincts": n_instincts,
        "n_extra_senses": n_extra_senses,
        "n_nodes_total": 3 + n_instincts + n_extra_senses,
        "n_runs": n_runs,
        "samples_per_run": trials,
        "n_samples": len(all_samples),
        "wall_s": round(time.perf_counter() - t0, 1),
        "stats": asdict(stats),
        "run_medians": run_medians,
        "run_p99s": [percentile(s, 99.0) for s in run_samples],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--trials", type=int, default=1_000)
    parser.add_argument("--instincts", type=str, default=",".join(map(str, _INSTINCT_LEVELS)),
                        help="comma-separated deliberative instinct counts (1 sense node)")
    parser.add_argument("--senses", type=str, default=",".join(map(str, _SENSE_LEVELS)),
                        help="comma-separated extra sense counts (0 deliberative instincts); "
                             "empty string disables the axis")
    parser.add_argument("--output-dir", "-o", type=str, default="benchmarks/results")
    args = parser.parse_args()

    configs: list[tuple[int, int]] = [(int(x), 0) for x in args.instincts.split(",") if x.strip()]
    configs += [(0, int(x)) for x in args.senses.split(",") if x.strip()]

    print("Arachnite reflex latency vs deliberative depth")
    print("Measures: sense.read() -> reflex fires -> action.execute() entry (us)")
    print(f"Runs: {args.runs} x {args.trials} trials per configuration | "
          f"Platform: {sys.platform} / CPython {platform.python_version()}")
    print("-" * 78)
    print(f"{'instincts':>9} {'+senses':>8} {'median':>9} {'P95':>9} {'P99':>9} "
          f"{'max':>9} {'CI95 median':>19} {'wall':>7}")

    results: list[dict[str, Any]] = []
    for n_i, n_s in configs:
        r = asyncio.run(run_config(n_i, n_s, args.runs, args.trials))
        st = r["stats"]
        print(f"{n_i:>9} {n_s:>8} {st['median']:>9.1f} {st['p95']:>9.1f} {st['p99']:>9.1f} "
              f"{st['max_val']:>9.1f} [{st['ci_lower']:>8.1f},{st['ci_upper']:>8.1f}] "
              f"{r['wall_s']:>6.1f}s", flush=True)
        results.append(r)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    host = platform.node().split(".")[0]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = out_dir / f"reflex_depth_sweep_{host}_py{platform.python_version()}_{stamp}.json"
    try:
        from arachnite import __version__ as ver
    except ImportError:  # pragma: no cover
        ver = "unknown"
    payload = {
        "benchmark": "reflex_depth_sweep",
        "unit": "us",
        "platform": platform.platform(),
        "hostname": host,
        "python_version": platform.python_version(),
        "arachnite_version": ver,
        "protocol": "1 timed sense + N deliberative instincts (never fire) + 1 timed reflex + "
                    "1 timed action; two manual ticks per trial; no warm-up; "
                    "extra-sense axis with 0 deliberative instincts",
        "configs": results,
    }
    out.write_text(json.dumps(payload, indent=2))
    print(f"\nResults written to {out}")


if __name__ == "__main__":
    main()
