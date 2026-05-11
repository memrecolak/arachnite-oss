"""
benchmarks/active_inference_comparison.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Decision-strategy comparison: Greedy vs Weighted vs Random vs ActiveInference.

Benchmarks ``ActiveInferenceDecisionNode`` against the three baseline
strategies on:

  1. The pick-and-place case study (``examples/robot_arm``).  Measures the
     per-tick latency overhead each strategy adds to a real workload.  The
     case study only routes one normal instinct (GraspInstinct) through the
     decision layer at a time, so this isolates *overhead*, not selection
     quality.

  2. A synthetic competing-proposal workload where four candidate proposals
     with varying ``priority``, ``urgency`` and per-evidence ``confidence``
     are presented every tick.  This isolates *selection behaviour* — which
     proposal wins — and lets each strategy reveal its bias.

Run::

    python -m benchmarks.active_inference_comparison [--runs 30] [--ticks 5000]

Output::

    benchmarks/results/active_inference_comparison_<DEVICE>_py<PYVER>_<TIMESTAMP>.json

When invoked from ``benchmarks/suite.py`` the same dict is folded into
the suite-wide report under ``benchmarks.active_inference_comparison``;
no separate file is written.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import platform
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

from arachnite import (
    ActionMasterNode,
    ArachniteRuntime,
    ContextNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    InstinctMasterNode,
    Proposal,
    RandomDecisionNode,
    SenseMasterNode,
    SignalBus,
    WeightedDecisionNode,
)
from arachnite.nodes.active_inference import ActiveInferenceDecisionNode
from arachnite.nodes.decision import BaseDecisionNode
from benchmarks.stats import bootstrap_ci, cliffs_delta, percentile, wilcoxon_signed_rank

# Suite / CI defaults. Standalone runs use the larger CLI defaults
# (--runs 5 --ticks 2000 --warmup 200); the suite runner invokes
# _run_async() with these tighter values so a full suite invocation
# stays reasonable. Each strategy × workload combination pays
# n_runs × n_ticks ticks, so the cell budget is 4 strategies × 2
# workloads × n_runs × n_ticks.
_QUICK_TICKS  = 500
_QUICK_WARMUP = 100


# ── Strategy registry ────────────────────────────────────────────────────────


def _make_strategies(bus: SignalBus) -> dict[str, BaseDecisionNode]:
    """Construct one fresh instance of each strategy.

    ActiveInference is exercised in two regimes:
      - beta=1.0: balanced exploration / exploitation
      - beta=0.0: equivalent to WeightedDecisionNode (sanity check)
    """
    return {
        "Greedy":           GreedyDecisionNode(bus=bus),
        "Weighted":         WeightedDecisionNode(bus=bus),
        "Random":           RandomDecisionNode(bus=bus),
        "ActiveInference":  ActiveInferenceDecisionNode(bus=bus, beta=1.0),
    }


# ── Workload 1: pick-and-place case study ────────────────────────────────────


async def bench_case_study(
    strategy_name: str,
    n_ticks: int,
    warmup: int,
) -> dict[str, Any]:
    """Run the robot-arm case study with the named strategy."""
    from examples.robot_arm import nodes as _example_nodes
    from examples.robot_arm.nodes import (
        SIM,
        ArmState,
        CollisionReflex,
        EmergencyRetractAction,
        GraspInstinct,
        JointPositionSenseNode,
        ObjectDetectionSenseNode,
        PickAndPlaceAction,
        ProximitySenseNode,
    )

    # Strip simulated-hardware sleeps so per-strategy latency reflects
    # decision-strategy overhead, not robot-arm timing.
    _example_nodes.BENCHMARK_MODE = True

    # Sense nodes default to poll_interval_s=0.1 (10 Hz). At benchmark tick
    # rates (~10 kHz) that throttles reads to 1-per-1000-ticks, so picks
    # never accumulate in a quick-mode measurement window. Read every tick.
    ProximitySenseNode.poll_interval_s = 0.0
    ObjectDetectionSenseNode.poll_interval_s = 0.0
    JointPositionSenseNode.poll_interval_s = 0.0

    # Reset deterministic physics. Going through the class (rather than
    # `SIM.__init__()`) avoids mypy's "instance.__init__ unsound" warning
    # while preserving the singleton identity that all node modules import.
    ArmState.__init__(SIM)

    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    strategy = _make_strategies(bus)[strategy_name]
    dm = DecisionMasterNode(bus=bus, strategy=strategy)
    am = ActionMasterNode(bus=bus)

    sm.register(ProximitySenseNode(bus=bus))
    sm.register(ObjectDetectionSenseNode(bus=bus))
    sm.register(JointPositionSenseNode(bus=bus))
    im.register(CollisionReflex(bus=bus))
    im.register(GraspInstinct(bus=bus))
    am.register(PickAndPlaceAction(bus=bus))
    am.register(EmergencyRetractAction(bus=bus))

    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(),
        instinct_master=im, decision_master=dm,
        action_master=am, bus=bus, tick_rate_hz=10_000.0,
    )
    # Bypass the background tick loop.
    for m in (sm, im, dm, am):
        await m.setup()

    # try/finally guarantees teardown runs even if a tick raises during
    # warmup or measurement, preventing master leakage from one strategy
    # into the next (`_run_async` calls `bench_case_study` per strategy).
    try:
        # Warmup
        for _ in range(warmup):
            await rt.tick()

        ArmState.__init__(SIM)  # reset after warmup so picks/emergencies count from zero

        samples: list[float] = []
        for _ in range(n_ticks):
            t0 = time.perf_counter()
            await rt.tick()
            samples.append((time.perf_counter() - t0) * 1_000.0)
    finally:
        for m in (am, dm, im, sm):
            await m.teardown()

    return {
        "samples_ms": samples,
        "picks": SIM.pick_count,
        "emergencies": SIM.emergency_count,
    }


# ── Workload 2: synthetic competing proposals ────────────────────────────────


def _competing_proposals(tick: int) -> list[Proposal]:
    """Build a deterministic but variable set of 4 competing proposals.

    The varying ``confidence`` values in ``evidence`` are what
    ActiveInferenceDecisionNode reads when computing epistemic value;
    the other strategies ignore them.
    """
    # Phase rotates every 4 ticks so each strategy gets exposed to all orderings
    phase = tick % 4

    base = [
        # (action_id,        priority, urgency, confidence)
        ("HighGoalLowConf",        90,    0.6,        0.30),
        ("MidGoalHighConf",        60,    0.9,        0.95),
        ("LowGoalHighConf",        30,    0.5,        0.99),
        ("UrgentLowGoalLowConf",   20,    1.0,        0.20),
    ]
    # Light per-tick perturbation to keep ties from being identical
    perturbed = []
    for i, (aid, pr, ur, _conf) in enumerate(base):
        # Rotate confidences by phase so the "uncertain" proposal moves around
        rotated_conf = base[(i + phase) % len(base)][3]
        perturbed.append(
            Proposal(
                instinct_id=f"Inst{i}",
                action_id=aid,
                priority=pr,
                urgency=ur,
                evidence={"signal_confidence": rotated_conf},
            )
        )
    return perturbed


async def bench_synthetic(
    strategy_name: str,
    n_ticks: int,
    warmup: int,
) -> dict[str, Any]:
    """Run only the decision layer with synthetic competing proposals."""
    bus = SignalBus()
    strategy = _make_strategies(bus)[strategy_name]

    # Warmup (also serves to JIT-warm any code paths)
    for tick in range(warmup):
        await strategy.decide(_competing_proposals(tick))

    samples: list[float] = []
    winners: Counter[str] = Counter()
    for tick in range(n_ticks):
        proposals = _competing_proposals(tick)
        t0 = time.perf_counter()
        chosen = await strategy.decide(proposals)
        samples.append((time.perf_counter() - t0) * 1_000_000.0)  # microseconds
        if chosen is not None:
            winners[chosen.action_id] += 1

    return {
        "samples_us": samples,
        "winners": dict(winners),
    }


# ── Aggregation ──────────────────────────────────────────────────────────────


def _summarise(samples: list[float], run_medians: list[float]) -> dict[str, Any]:
    """Bootstrap a 95 % median CI from the per-run medians, not pooled samples.

    `run_medians` is mandatory — bootstrapping `statistics.median` over
    the pooled sample array (60k+ entries at high run counts) stalls the
    process. Callers must supply the per-run medians; an empty list
    raises rather than silently falling through to the pooled-sample path.
    """
    if not run_medians:
        raise ValueError("_summarise(): run_medians must be a non-empty list")
    s = sorted(samples)
    n = len(s)
    # bootstrap_ci on a single replicate is degenerate (every resample picks
    # the only element, so lo == hi == median). Emit explicit nulls when the
    # caller hasn't supplied at least two run_medians.
    ci_lo: float | None
    ci_hi: float | None
    if len(run_medians) < 2:
        ci_lo = ci_hi = None
    else:
        ci_lo, ci_hi = bootstrap_ci(run_medians, stat_fn=statistics.median)
    return {
        "n": n,
        "mean": statistics.mean(s),
        "median": statistics.median(s),
        "p95": percentile(s, 95.0),
        "p99": percentile(s, 99.0),
        "std_dev": statistics.stdev(s) if n > 1 else 0.0,
        "ci_lower": ci_lo,
        "ci_upper": ci_hi,
    }


def _pairwise_vs_weighted(
    run_medians_by_strategy: dict[str, list[float]],
) -> dict[str, dict[str, Any]]:
    """Wilcoxon + Cliff's delta comparing each strategy against Weighted.

    Operates on per-run medians (R values), not pooled raw samples.
    Weighted is the natural baseline because ActiveInference reduces to it
    when beta = 0.
    """
    if "Weighted" not in run_medians_by_strategy:
        return {}
    ref = run_medians_by_strategy["Weighted"]
    out: dict[str, dict[str, Any]] = {}
    for name, meds in run_medians_by_strategy.items():
        if name == "Weighted":
            continue
        n = min(len(ref), len(meds))
        w, p = wilcoxon_signed_rank(ref[:n], meds[:n])
        delta, mag = cliffs_delta(meds, ref)
        # wilcoxon_signed_rank() returns NaN when n < 10 (normal-approximation
        # threshold). RFC 8259 forbids NaN in JSON, so emit null instead.
        out[name] = {
            "wilcoxon_W": w,
            "wilcoxon_p": p if math.isfinite(p) else None,
            "cliffs_delta_vs_weighted": delta,
            "effect_magnitude": mag,
        }
    return out


# ── CLI ──────────────────────────────────────────────────────────────────────


def _print_strategy_table(label: str, unit: str, stats: dict[str, dict[str, Any]]) -> None:
    print(f"\n  {label}")
    print("  " + "-" * 80)
    print(f"  {'Strategy':<18}  {'Mean':>10}  {'Median':>10}  "
          f"{'P95':>10}  {'P99':>10}  {'95% CI':>22}")
    print("  " + "-" * 80)
    for name, s in stats.items():
        if s["ci_lower"] is None or s["ci_upper"] is None:
            ci = "[n/a, n/a]"
        else:
            ci = f"[{s['ci_lower']:.3f}, {s['ci_upper']:.3f}]"
        print(f"  {name:<18}  "
              f"{s['mean']:>8.3f} {unit}  "
              f"{s['median']:>8.3f} {unit}  "
              f"{s['p95']:>8.3f} {unit}  "
              f"{s['p99']:>8.3f} {unit}  "
              f"{ci:>22}")


async def run_async(
    n_runs: int, n_ticks: int, warmup: int,
) -> dict[str, Any]:
    """Run both workloads × all 4 strategies and assemble the report dict.

    Pulled out of the CLI ``main()`` so the suite runner
    (``benchmarks/suite.py::run_active_inference_comparison``) can drive
    the same code path with its own (typically quicker) tick/run budget.
    """
    strategy_names = ["Greedy", "Weighted", "Random", "ActiveInference"]

    # ── Workload 1 ────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print(f"  WORKLOAD 1: pick-and-place case study  "
          f"({n_runs} runs × {n_ticks} ticks)")
    print("=" * 80)
    case_study: dict[str, dict[str, Any]] = {}
    case_pooled: dict[str, list[float]] = {n: [] for n in strategy_names}
    case_run_medians: dict[str, list[float]] = {n: [] for n in strategy_names}
    case_picks: dict[str, list[int]] = {n: [] for n in strategy_names}
    case_emerg: dict[str, list[int]] = {n: [] for n in strategy_names}

    for name in strategy_names:
        print(f"\n  {name}")
        for run in range(n_runs):
            r = await bench_case_study(name, n_ticks, warmup)
            run_med = statistics.median(r["samples_ms"])
            case_pooled[name].extend(r["samples_ms"])
            case_run_medians[name].append(run_med)
            case_picks[name].append(r["picks"])
            case_emerg[name].append(r["emergencies"])
            print(f"    run {run+1:2d}/{n_runs}: "
                  f"median={run_med:.4f} ms  "
                  f"picks={r['picks']}  emergencies={r['emergencies']}")
        case_study[name] = _summarise(case_pooled[name], case_run_medians[name])
        case_study[name]["picks_per_run"]       = case_picks[name]
        case_study[name]["emergencies_per_run"] = case_emerg[name]
        case_study[name]["picks_mean"]          = statistics.mean(case_picks[name])
        case_study[name]["emergencies_mean"]    = statistics.mean(case_emerg[name])

    _print_strategy_table(
        "Case study tick latency (ms)", "ms",
        {n: case_study[n] for n in strategy_names},
    )
    print("\n  Decision-quality (case study, mean per run):")
    print(f"  {'Strategy':<18}  {'picks':>8}  {'emergencies':>12}")
    for name in strategy_names:
        print(f"  {name:<18}  "
              f"{case_study[name]['picks_mean']:>8.1f}  "
              f"{case_study[name]['emergencies_mean']:>12.1f}")

    # ── Workload 2 ────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print(f"  WORKLOAD 2: synthetic competing proposals  "
          f"({n_runs} runs × {n_ticks} ticks)")
    print("=" * 80)
    synth: dict[str, dict[str, Any]] = {}
    synth_pooled: dict[str, list[float]] = {n: [] for n in strategy_names}
    synth_run_medians: dict[str, list[float]] = {n: [] for n in strategy_names}
    synth_winners: dict[str, Counter[str]] = {n: Counter() for n in strategy_names}

    for name in strategy_names:
        print(f"\n  {name}")
        for run in range(n_runs):
            r = await bench_synthetic(name, n_ticks, warmup)
            run_med = statistics.median(r["samples_us"])
            synth_pooled[name].extend(r["samples_us"])
            synth_run_medians[name].append(run_med)
            synth_winners[name].update(r["winners"])
            print(f"    run {run+1:2d}/{n_runs}: "
                  f"median={run_med:.2f} us")
        synth[name] = _summarise(synth_pooled[name], synth_run_medians[name])
        synth[name]["winners"] = dict(synth_winners[name])

    _print_strategy_table(
        "Synthetic decision latency (us)", "us",
        {n: synth[n] for n in strategy_names},
    )
    print("\n  Selection bias (winning action_id over all runs):")
    all_actions = sorted({a for w in synth_winners.values() for a in w})
    print(f"  {'Strategy':<18}  " + "  ".join(f"{a:>22}" for a in all_actions))
    for name in strategy_names:
        cells = []
        total = sum(synth_winners[name].values()) or 1
        for a in all_actions:
            count = synth_winners[name].get(a, 0)
            cells.append(f"{count:>5d} ({100*count/total:>5.1f}%)" + " " * 8)
        print(f"  {name:<18}  " + "  ".join(cells))

    # ── Pairwise statistics (latency, vs Weighted as reference) ───────────
    case_pairwise = _pairwise_vs_weighted(case_run_medians)
    synth_pairwise = _pairwise_vs_weighted(synth_run_medians)

    print("\n  Pairwise latency tests (vs Weighted):")
    print("  " + "-" * 70)
    for label, table in (("Case study", case_pairwise),
                         ("Synthetic ", synth_pairwise)):
        for name, comp in table.items():
            p_val = comp["wilcoxon_p"]
            p_str = f"{p_val:.4g}" if p_val is not None else "n/a"
            print(f"  {label} | {name:<18}  "
                  f"W={comp['wilcoxon_W']:>8.1f}  "
                  f"p={p_str:>8}  "
                  f"delta={comp['cliffs_delta_vs_weighted']:+.3f} "
                  f"({comp['effect_magnitude']})")

    return {
        "benchmark": "active_inference_comparison",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "machine":  platform.node(),
        "args": {
            "runs":   n_runs,
            "ticks":  n_ticks,
            "warmup": warmup,
        },
        "case_study": case_study,
        "synthetic": synth,
        "pairwise_vs_weighted": {
            "case_study": case_pairwise,
            "synthetic":  synth_pairwise,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Decision-strategy comparison (Q1-6 active inference benchmark)",
    )
    parser.add_argument("--runs",   type=int, default=5,
                        help="Independent runs per strategy (default: 5)")
    parser.add_argument("--ticks",  type=int, default=2_000,
                        help="Ticks per run (default: 2000)")
    parser.add_argument("--warmup", type=int, default=200,
                        help="Warmup ticks per run (default: 200)")
    parser.add_argument("--output-dir", type=str, default="benchmarks/results")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.runs < 10:
        print(f"  WARNING: --runs={args.runs} is below the n>=10 threshold for "
              "the Wilcoxon normal approximation; wilcoxon_p will be null. "
              "Use --runs 10 or more for publication-grade comparisons.")

    result = asyncio.run(run_async(args.runs, args.ticks, args.warmup))

    # Hostname + Python tag mirrors the suite/compare conventions so
    # results from different machines / interpreters never collide.
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    device_tag = platform.node().replace(" ", "_") or "unknown"
    py_tag = f"py{platform.python_version()}"
    out_file = out_dir / (
        f"active_inference_comparison_{device_tag}_{py_tag}_{timestamp}.json"
    )
    with open(out_file, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n  Results saved to {out_file}")


if __name__ == "__main__":
    main()
