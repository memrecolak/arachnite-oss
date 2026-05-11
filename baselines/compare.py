"""
baselines/compare.py
~~~~~~~~~~~~~~~~~~~~
Cross-framework comparison benchmark.

Runs the pick-and-place scenario on all available frameworks and produces
a comparison table suitable for the paper.

Run:
    python -m baselines.compare [--runs N] [--ticks N]

Output:
    - Console table with latency, memory, and LoC comparison
    - JSON results in baselines/results/

Frameworks tested:
    - Arachnite (always available)
    - py_trees (if installed: pip install py-trees)
    - Jason (if JASON_HOME is set and baselines/jason/build/ is compiled —
        see baselines/jason/README.md)
    - ROS 2 (if rclpy is importable, i.e. a ROS 2 distro is sourced —
        see baselines/ros2/README.md)

Frameworks for which the runtime benchmark cannot run on this machine
contribute LoC only and are reported with a [SKIP] line at the top of the
output.
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
from collections.abc import Callable
from pathlib import Path
from typing import Any

from benchmarks.stats import (
    bootstrap_ci,
    cliffs_delta,
    percentile,
    wilcoxon_signed_rank,
)

# ── Lines of code counter ───────────────────────────────────────────────────

def count_loc(
    paths: list[str],
    extensions: set[str] | None = None,
) -> int:
    """Count non-blank, non-comment lines across files."""
    if extensions is None:
        extensions = {".py", ".asl", ".java", ".xml", ".yaml"}
    total = 0
    for path_str in paths:
        p = Path(path_str)
        if not p.exists():
            continue
        if p.suffix not in extensions:
            continue
        with open(p, encoding="utf-8", errors="replace") as f:
            for line in f:
                stripped = line.strip()
                if stripped and not stripped.startswith("#") and not stripped.startswith("//"):
                    total += 1
    return total


# ── Arachnite benchmark ─────────────────────────────────────────────────────

async def bench_arachnite(n_ticks: int, warmup: int) -> dict[str, Any]:
    """Run Arachnite pick-and-place and return latency samples."""
    from arachnite import (
        ActionMasterNode,
        ArachniteRuntime,
        ContextNode,
        DecisionMasterNode,
        InstinctMasterNode,
        SenseMasterNode,
        SignalBus,
        WeightedDecisionNode,
    )
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

    # Strip simulated-hardware sleeps so per-tick latency reflects framework
    # overhead at parity with the sleep-free py_trees baseline. See
    # docs/audits/2026-05-04-architecture-and-fairness-audit.md, finding #1.
    _example_nodes.BENCHMARK_MODE = True

    # Sense nodes default to poll_interval_s=0.1 (10 Hz). At the benchmark's
    # 10 kHz tick rate that throttles reads to ~1 per 1000 ticks, so the
    # number of completed picks ends up gated by wall-clock time rather than
    # tick count — faster hosts complete the same 300k ticks in fewer seconds
    # and therefore record fewer picks. Read every tick to make pick count
    # host-independent and to expose the full per-tick pipeline cost.
    # Mirrors benchmarks/active_inference_comparison.py:120-125.
    ProximitySenseNode.poll_interval_s = 0.0
    ObjectDetectionSenseNode.poll_interval_s = 0.0
    JointPositionSenseNode.poll_interval_s = 0.0

    # Reset simulation in place. Going through the class (rather than
    # `SIM.__init__()`) avoids mypy's "instance.__init__ unsound" warning
    # while preserving the singleton identity that all node modules import.
    ArmState.__init__(SIM)

    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
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
    # Bypass background tick loop (Bug B — see audit 2026-04-16).
    for m in (sm, im, dm, am):
        await m.setup()

    # try/finally guarantees teardown runs even if a tick raises during
    # warmup or measurement, preventing master leakage into subsequent runs.
    # See docs/audits/2026-05-04-architecture-and-fairness-audit.md, finding #3.
    try:
        # Warmup
        for _ in range(warmup):
            await rt.tick()

        ArmState.__init__(SIM)  # Reset after warmup (see note above)

        # Measure
        samples = []
        for _tick in range(n_ticks):
            t0 = time.perf_counter()
            await rt.tick()
            samples.append((time.perf_counter() - t0) * 1_000)
    finally:
        for m in (am, dm, im, sm):
            await m.teardown()

    # Memory
    rss = 0.0
    try:
        import psutil
        rss = psutil.Process().memory_info().rss / (1024 * 1024)
    except ImportError:
        pass

    return {
        "framework": "Arachnite",
        "samples": samples,
        "picks": SIM.pick_count,
        "pick_durations_ms": list(SIM.pick_durations_ms),
        "emergencies": SIM.emergency_count,
        "memory_mb": rss,
    }


# ── py_trees benchmark ──────────────────────────────────────────────────────

def bench_py_trees(n_ticks: int, warmup: int) -> dict[str, Any] | None:
    """Run py_trees pick-and-place. Returns None if py_trees not installed."""
    try:
        import py_trees  # noqa: F401
    except ImportError:
        print("  [SKIP] py_trees not installed (pip install py-trees)")
        return None

    # Pin the conveyor model at the grasp window so the pick path actually
    # runs every tick, mirroring Arachnite's BENCHMARK_MODE and Jason's
    # ArmState.java. Without this the wall-clock drift never reaches the
    # 0.10–0.35 m window during a 10k-tick run, picks_completed stays at 0,
    # and the latency comparison is apples-to-oranges. See audit
    # 2026-05-04 #1 (alternative-fix branch).
    import baselines.shared_sim as _shared_sim
    _shared_sim.BENCHMARK_MODE = True

    from baselines.py_trees.robot_arm import run_benchmark
    result = run_benchmark(n_ticks=n_ticks, warmup=warmup)

    return {
        "framework": "py_trees",
        "samples": result.tick_latencies_ms,
        "picks": result.picks_completed,
        "pick_durations_ms": list(result.pick_durations_ms),
        "emergencies": result.emergencies_handled,
        "memory_mb": result.memory_rss_mb,
    }


# ── Jason benchmark ─────────────────────────────────────────────────────────

def bench_jason(n_ticks: int, warmup: int) -> dict[str, Any] | None:
    """Run Jason BDI baseline. Returns None if Jason or JVM is unavailable."""
    from baselines.jason.run_jason import jason_available, run_benchmark

    ok, why = jason_available()
    if not ok:
        print(f"  [SKIP] Jason not available: {why}")
        return None

    result = run_benchmark(n_ticks=n_ticks, warmup=warmup)
    return {
        "framework": "Jason",
        "samples": result.tick_latencies_ms,
        "picks": result.picks_completed,
        "pick_durations_ms": list(result.pick_durations_ms),
        "emergencies": result.emergencies_handled,
        "memory_mb": result.memory_rss_mb,
    }


# ── ROS 2 benchmark ─────────────────────────────────────────────────────────

def bench_ros2(n_ticks: int, warmup: int) -> dict[str, Any] | None:
    """Run ROS 2 + py_trees baseline. Returns None if rclpy is unavailable."""
    from baselines.ros2.robot_arm import ros2_available, run_benchmark

    ok, why = ros2_available()
    if not ok:
        print(f"  [SKIP] ROS 2 not available: {why}")
        return None

    # Match the workload of the other frameworks — see bench_py_trees note.
    import baselines.shared_sim as _shared_sim
    _shared_sim.BENCHMARK_MODE = True

    result = run_benchmark(n_ticks=n_ticks, warmup=warmup)
    return {
        "framework": "ROS 2 BT",
        "samples": result.tick_latencies_ms,
        "picks": result.picks_completed,
        "pick_durations_ms": list(result.pick_durations_ms),
        "emergencies": result.emergencies_handled,
        "memory_mb": result.memory_rss_mb,
    }


# ── Comparison analysis ─────────────────────────────────────────────────────

def analyze(results: list[dict[str, Any]], n_runs: int) -> dict[str, Any]:
    """Compute descriptive stats and pairwise comparisons.

    Each entry in `results` must carry a `run_medians` list — the per-run
    median latencies that drive the bootstrap CI, Wilcoxon test, and
    Cliff's delta. The previous fallback of "use pooled samples if
    `run_medians` is missing" is a perf-cliff trap: at paper-grade run
    counts the pooled sample array runs to 300k+ entries, which turns
    `bootstrap_ci(..., stat_fn=statistics.median)` into ~10k median
    computations on 300k-element lists (effectively a hang). The
    contract is enforced loudly so a future refactor that calls
    `analyze()` without populating `run_medians` fails fast instead of
    locking the process. Audit: 2026-05-04 #2.
    """
    missing = [r.get("framework", "<unknown>") for r in results if not r.get("run_medians")]
    if missing:
        raise ValueError(
            "analyze(): every result must carry a non-empty 'run_medians' "
            f"list; missing for: {missing}"
        )

    analysis: dict[str, Any] = {}

    for r in results:
        name = r["framework"]
        s = sorted(r["samples"])
        n = len(s)
        # bootstrap_ci on a single replicate is mathematically degenerate
        # (every resample picks the only element, so lo == hi == median).
        # Emit explicit nulls so paper-grade tooling can detect under-replicated
        # runs instead of mistaking the point estimate for a tight interval.
        ci_lo: float | None
        ci_hi: float | None
        if len(r["run_medians"]) < 2:
            ci_lo = ci_hi = None
        else:
            ci_lo, ci_hi = bootstrap_ci(r["run_medians"], stat_fn=statistics.median)

        # Per-pick wall-clock distribution (object-detected → object-released).
        # This is the column the paper compares across frameworks: per-tick
        # latency measures different units of work per framework (Arachnite =
        # one stage of a MultiStepAction; py_trees/ROS 2 = one full Sequence
        # traversal; Jason = one reasoning cycle), but per-pick wall-clock
        # measures the same end-to-end task in all four. n/a if a framework
        # didn't complete a single pick (under-replicated or broken setup).
        picks_ms = sorted(r.get("pick_durations_ms") or [])
        per_pick_n = len(picks_ms)
        if per_pick_n == 0:
            per_pick_median: float | None = None
            per_pick_p95: float | None = None
            per_pick_p99: float | None = None
        else:
            per_pick_median = statistics.median(picks_ms)
            per_pick_p95 = percentile(picks_ms, 95.0)
            per_pick_p99 = percentile(picks_ms, 99.0)

        analysis[name] = {
            "mean": statistics.mean(s),
            "median": statistics.median(s),
            "p95": percentile(s, 95.0),
            "p99": percentile(s, 99.0),
            "std_dev": statistics.stdev(s) if n > 1 else 0.0,
            "ci_lower": ci_lo,
            "ci_upper": ci_hi,
            "memory_mb": r["memory_mb"],
            "picks": r["picks"],
            "per_pick_median_ms": per_pick_median,
            "per_pick_p95_ms": per_pick_p95,
            "per_pick_p99_ms": per_pick_p99,
            "per_pick_n": per_pick_n,
            "emergencies": r["emergencies"],
            "n_samples": n,
        }

    # Pairwise comparisons (Arachnite vs each baseline)
    # Use per-run medians (not pooled samples) for Wilcoxon and Cliff's delta:
    # each run is an independent replicate, so run-level medians are the
    # correct unit of analysis. Pooled samples are 300k+ entries and make
    # cliffs_delta() O(n²) = infeasible.
    #
    # Two parallel paired comparisons are emitted per baseline:
    #   per-tick metric  → wilcoxon_W / wilcoxon_p / cliffs_delta
    #   per-pick metric  → per_pick_wilcoxon_W / per_pick_wilcoxon_p /
    #                       per_pick_cliffs_delta
    # The per-pick paired sample drops any run index where either framework
    # produced zero picks (median undefined for that run).
    if "Arachnite" in analysis:
        arachnite_r = [r for r in results if r["framework"] == "Arachnite"][0]
        arachnite_medians = arachnite_r["run_medians"]
        arachnite_pick_medians = arachnite_r.get("pick_run_medians", [])
        comparisons: dict[str, dict[str, Any]] = {}

        for r in results:
            if r["framework"] == "Arachnite":
                continue

            baseline_medians = r["run_medians"]
            min_len = min(len(arachnite_medians), len(baseline_medians))
            a_paired = arachnite_medians[:min_len]
            b_paired = baseline_medians[:min_len]

            w, p = wilcoxon_signed_rank(a_paired, b_paired)
            delta, mag = cliffs_delta(arachnite_medians, baseline_medians)

            # Per-pick paired test. Both vectors carry one entry per run;
            # entries are None when that run produced zero picks. Drop
            # paired entries where either side is None before testing.
            baseline_pick_medians = r.get("pick_run_medians", [])
            paired_picks: list[tuple[float, float]] = []
            for a, b in zip(arachnite_pick_medians, baseline_pick_medians, strict=False):
                if a is not None and b is not None:
                    paired_picks.append((a, b))

            if len(paired_picks) >= 1:
                a_pick = [p_[0] for p_ in paired_picks]
                b_pick = [p_[1] for p_ in paired_picks]
                pp_w, pp_p = wilcoxon_signed_rank(a_pick, b_pick)
                pp_delta, pp_mag = cliffs_delta(a_pick, b_pick)
                pp_n = len(paired_picks)
            else:
                pp_w = float("nan")
                pp_p = float("nan")
                pp_delta = float("nan")
                pp_mag = "n/a"
                pp_n = 0

            # wilcoxon_signed_rank() returns NaN when the paired sample is too
            # small for the normal approximation (n < 10). RFC 8259 forbids the
            # NaN literal in JSON, so emit null instead — keeps the file parseable
            # by jq / JSON.parse and signals "test not run" explicitly.
            comparisons[r["framework"]] = {
                "wilcoxon_W": w,
                "wilcoxon_p": p if math.isfinite(p) else None,
                "cliffs_delta": delta,
                "effect_magnitude": mag,
                "per_pick_wilcoxon_W": pp_w if math.isfinite(pp_w) else None,
                "per_pick_wilcoxon_p": pp_p if math.isfinite(pp_p) else None,
                "per_pick_cliffs_delta": pp_delta if math.isfinite(pp_delta) else None,
                "per_pick_effect_magnitude": pp_mag,
                "per_pick_paired_n": pp_n,
            }

        analysis["_comparisons"] = comparisons

    return analysis


# ── Lines of code analysis ──────────────────────────────────────────────────

def loc_analysis() -> dict[str, int]:
    """Count lines of code for each framework's implementation."""
    base = Path(__file__).parent.parent

    frameworks = {
        "Arachnite": count_loc([
            str(base / "examples/robot_arm/nodes.py"),
            str(base / "examples/robot_arm/simulate.py"),
            str(base / "examples/robot_arm/manifest.yaml"),
        ]),
        "py_trees": count_loc([
            str(base / "baselines/py_trees/robot_arm.py"),
        ]),
        "Jason": count_loc([
            str(base / "baselines/jason/robot_arm.asl"),
            str(base / "baselines/jason/RobotArmEnv.java"),
        ]),
        "ROS 2 BT": count_loc([
            str(base / "baselines/ros2/robot_arm_bt.py"),
        ]),
    }

    return frameworks


def loc_table(loc: dict[str, int]) -> None:
    """Print LoC comparison table."""
    print("\n  Lines of Code Comparison (pick-and-place scenario)")
    print("  " + "-" * 55)
    print(f"  {'Framework':<20} {'LoC':>6}  Features included")
    print("  " + "-" * 55)

    features = {
        "Arachnite": "reflex, FAP, rollback, distributed",
        "py_trees": "BT tick, no reflex/FAP/rollback",
        "Jason": "BDI plans, no reflex/FAP/rollback",
        "ROS 2 BT": "BT + DDS, no reflex/FAP/rollback",
    }

    for name, lines in loc.items():
        feat = features.get(name, "")
        print(f"  {name:<20} {lines:>6}  {feat}")
    print("  " + "-" * 55)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-framework comparison")
    parser.add_argument("--runs", type=int, default=5, help="Independent runs per framework")
    parser.add_argument("--ticks", type=int, default=10_000, help="Ticks per run")
    parser.add_argument("--warmup", type=int, default=1_000, help="Warmup ticks")
    parser.add_argument("--output-dir", type=str, default="baselines/results")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  CROSS-FRAMEWORK COMPARISON BENCHMARK")
    print("=" * 60)

    # Lines of code (always available)
    loc = loc_analysis()
    loc_table(loc)

    # Runtime benchmarks. Each entry is (name, callable -> dict | None).
    # Callables either return a result dict or print a [SKIP] line and
    # return None. Order matters only for the printed report.
    baselines: list[tuple[str, Callable[[], dict[str, Any] | None]]] = [
        ("Arachnite", lambda: asyncio.run(bench_arachnite(args.ticks, args.warmup))),
        ("py_trees",  lambda: bench_py_trees(args.ticks, args.warmup)),
        ("Jason",     lambda: bench_jason(args.ticks, args.warmup)),
        ("ROS 2 BT",  lambda: bench_ros2(args.ticks, args.warmup)),
    ]

    print(f"\n  Running {args.runs} independent runs, {args.ticks} ticks each...")
    if args.runs < 10:
        print(f"  WARNING: --runs={args.runs} is below the n>=10 threshold for "
              "the Wilcoxon normal approximation; wilcoxon_p will be null. "
              "Paper-grade comparisons require --runs 10 or more.")

    # Results indexed by framework name so we accumulate run_medians across
    # runs without depending on stable list positions.
    accum: dict[str, dict[str, Any]] = {}
    skipped: set[str] = set()

    for run_idx in range(args.runs):
        print(f"\n  --- Run {run_idx + 1}/{args.runs} ---")

        for name, fn in baselines:
            if name in skipped:
                continue
            print(f"  {name}...", end=" ", flush=True)
            try:
                res = fn()
            except RuntimeError as exc:
                print(f"[SKIP] {exc}")
                skipped.add(name)
                continue
            if res is None:
                # Callable already printed a [SKIP] line.
                skipped.add(name)
                continue
            median = statistics.median(res["samples"])
            print(f"median={median:.3f} ms")
            # Per-run per-pick median, used for paired Wilcoxon / Cliff's
            # delta on the per-pick wall-clock metric. None when this run
            # produced zero picks — the analyze() step drops paired entries
            # where either side is None.
            run_picks = res.get("pick_durations_ms") or []
            pick_median_this_run: float | None = (
                statistics.median(run_picks) if run_picks else None
            )

            if name not in accum:
                res["run_medians"] = [median]
                res["pick_run_medians"] = [pick_median_this_run]
                # Normalise: every accum entry must have a list, even when
                # the framework didn't return one (older skip paths).
                res.setdefault("pick_durations_ms", [])
                accum[name] = res
            else:
                accum[name]["samples"].extend(res["samples"])
                accum[name]["run_medians"].append(median)
                accum[name]["pick_run_medians"].append(pick_median_this_run)
                accum[name]["pick_durations_ms"].extend(
                    res.get("pick_durations_ms") or []
                )
                # Keep the latest run's pick_count / memory snapshot —
                # picks are per-run counts, but the JSON only has one slot.
                # Sum picks across runs so the reported "picks" matches the
                # length of the per-pick distribution.
                accum[name]["picks"] = accum[name].get("picks", 0) + res.get("picks", 0)
                accum[name]["memory_mb"] = res.get("memory_mb", accum[name]["memory_mb"])

    all_results = list(accum.values())
    if len(all_results) < 2:
        # A "comparison of one" cannot produce pairwise comparisons; emitting
        # a comparison_*.json with comparisons={} would silently look like a
        # successful run. Fail loudly so CI/operator notices missing baselines.
        # See docs/audits/2026-05-04-architecture-and-fairness-audit.md, finding #4.
        ran = [r["framework"] for r in all_results] or ["<none>"]
        print(
            f"\n  Cross-framework comparison requires >= 2 frameworks; "
            f"only ran: {', '.join(ran)}. Skipped: {', '.join(sorted(skipped)) or '<none>'}."
        )
        sys.exit(1)

    # Analysis
    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)

    analysis = analyze(all_results, args.runs)

    print(f"\n  {'Metric':<25} ", end="")
    for name in analysis:
        if not name.startswith("_"):
            print(f"{name:>15}", end="")
    print()
    print("  " + "-" * 70)

    for metric in [
        "mean", "median", "p95", "p99", "std_dev",
        "ci_lower", "ci_upper", "memory_mb",
        "per_pick_median_ms", "per_pick_p95_ms", "per_pick_p99_ms",
    ]:
        if metric == "memory_mb":
            unit = "MB"
        elif metric.startswith("per_pick_"):
            unit = "ms/pick"
        else:
            unit = "ms"
        print(f"  {metric + ' (' + unit + ')':<25} ", end="")
        for name, stats in analysis.items():
            if not name.startswith("_"):
                value = stats[metric]
                cell = f"{value:>15.3f}" if value is not None else f"{'n/a':>15}"
                print(cell, end="")
        print()

    # Per-pick sample counts on their own row — distinct unit from the latencies.
    print(f"  {'per_pick_n':<25} ", end="")
    for name, stats in analysis.items():
        if not name.startswith("_"):
            print(f"{stats['per_pick_n']:>15d}", end="")
    print()

    # Pairwise comparisons
    if "_comparisons" in analysis:
        print("\n  Pairwise comparisons (Arachnite vs baseline):")
        print("  " + "-" * 60)
        for baseline, comp in analysis["_comparisons"].items():
            p_val = comp["wilcoxon_p"]
            p_str = f"{p_val:.6f}" if p_val is not None else "n/a"
            print(f"  vs {baseline}:")
            print(f"    Wilcoxon W = {comp['wilcoxon_W']:.1f}, p = {p_str}")
            print(f"    Cliff's delta = {comp['cliffs_delta']:.3f} ({comp['effect_magnitude']})")

    # LoC in results
    analysis["lines_of_code"] = loc

    # Save. Tag the filename with hostname + Python version so successive
    # runs from different machines or interpreters don't overwrite each
    # other in the shared results directory.
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    device_tag = platform.node().replace(" ", "_") or "unknown"
    py_tag = f"py{platform.python_version()}"
    out_file = out_dir / f"comparison_{device_tag}_{py_tag}_{timestamp}.json"

    # Convert samples to stats only (too large for JSON)
    save_analysis = {k: v for k, v in analysis.items() if k != "_comparisons"}
    if "_comparisons" in analysis:
        save_analysis["comparisons"] = analysis["_comparisons"]

    with open(out_file, "w") as f:
        json.dump(save_analysis, f, indent=2, default=str)

    print(f"\n  Results saved to {out_file}")


if __name__ == "__main__":
    main()
