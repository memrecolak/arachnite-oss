"""
benchmarks/stats.py
~~~~~~~~~~~~~~~~~~~
Statistical analysis utilities for Arachnite benchmarks.

Provides the rigour expected by Q1 SE journals (IEEE TSE, ACM TOSEM):
  - 95% bootstrap confidence intervals
  - Wilcoxon signed-rank test for paired comparisons
  - Cliff's delta effect size
  - Bonferroni correction for multiple comparisons
  - Multi-run aggregation across independent process invocations

References:
  - Kitchenham & Charters, "Guidelines for Performing Systematic Literature
    Reviews in Software Engineering," EBSE-2007-01, 2007.
  - Kampenes et al., "A Systematic Review of Effect Size in Software
    Engineering Experiments," IST, vol. 49, no. 11-12, 2007.
  - Wohlin et al., Experimentation in Software Engineering, Springer, 2012.

Run:
    python benchmarks/stats.py --benchmark tick --runs 30
    python benchmarks/stats.py --benchmark reflex --runs 30
    python benchmarks/stats.py --benchmark scalability --runs 30
    python benchmarks/stats.py --benchmark memory --runs 10
    python benchmarks/stats.py --benchmark all --runs 30

Per-benchmark execution delegates to the runners in `benchmarks.suite`, so
this CLI and `benchmarks/suite.py --only <name>` produce equivalent data;
the difference is packaging — `stats.py` writes one JSON file per benchmark,
`suite.py` writes a combined report for the whole run.

Available choices: tick, stage_breakdown, reflex, memory, scalability,
extended, multistep_action_latency, soak_test, all.

Output:
    JSON file in benchmarks/results/ with per-run raw data and aggregated
    statistics including CIs, and a human-readable summary to stdout.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import platform
import random
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

# ── Statistical primitives ──────────────────────────────────────────────────


def percentile(data: Sequence[float], p: float) -> float:
    """Return the p-th percentile of data (0 <= p <= 100).

    Uses the nearest-rank method consistent with the rest of the benchmark
    suite (`all_sorted[int(n * p/100)]`). Raises ValueError on empty input.
    """
    if not data:
        raise ValueError("percentile() requires non-empty data")
    if not 0.0 <= p <= 100.0:
        raise ValueError(f"percentile p must be in [0, 100], got {p}")
    ordered = sorted(data)
    n = len(ordered)
    idx = int(n * (p / 100.0))
    if idx >= n:
        idx = n - 1
    return ordered[idx]


def bootstrap_ci(
    data: list[float],
    stat_fn: Callable[[Sequence[float]], float] = statistics.mean,
    n_bootstrap: int = 10_000,
    alpha: float = 0.05,
    seed: int = 42,
) -> tuple[float, float]:
    """Compute a (1-alpha) bootstrap confidence interval for stat_fn.

    Parameters
    ----------
    data:
        Observations to resample.
    stat_fn:
        Statistic to compute on each bootstrap sample. Defaults to
        `statistics.mean`. Any callable accepting a sequence of floats and
        returning a float is accepted, e.g. `statistics.median` or
        `lambda s: percentile(s, 95.0)` for a P95 CI.
    n_bootstrap:
        Number of bootstrap resamples (default 10,000).
    alpha:
        Significance level; the CI is (1 - alpha) wide (default 0.05 → 95% CI).
    seed:
        Seed for the bootstrap RNG (default 42 for reproducibility per §22.1.3).

    Returns
    -------
    (lower, upper) bounds of the CI. Returns (nan, nan) if `data` is empty.
    """
    n = len(data)
    if n == 0:
        return (float("nan"), float("nan"))
    rng = random.Random(seed)
    boot_stats: list[float] = []
    for _ in range(n_bootstrap):
        sample = [data[rng.randint(0, n - 1)] for _ in range(n)]
        boot_stats.append(stat_fn(sample))
    boot_stats.sort()
    lo = int((alpha / 2) * n_bootstrap)
    hi = int((1 - alpha / 2) * n_bootstrap) - 1
    return boot_stats[lo], boot_stats[hi]


def wilcoxon_signed_rank(x: list[float], y: list[float]) -> tuple[float, float]:
    """Wilcoxon signed-rank test (two-sided) for paired samples.

    Returns (W_statistic, approximate_p_value).
    Uses normal approximation for n >= 10.
    """
    assert len(x) == len(y), "Paired samples must have equal length"
    raw_diffs = [xi - yi for xi, yi in zip(x, y, strict=False)]
    # Remove zero differences; pair each remaining diff with |diff| for sorting.
    pairs: list[tuple[float, float]] = [(abs(d), d) for d in raw_diffs if d != 0.0]
    if not pairs:
        return 0.0, 1.0

    # Rank by absolute value
    pairs.sort(key=lambda t: t[0])
    ranks: list[float] = [float(r) for r in range(1, len(pairs) + 1)]

    # Handle ties: average ranks for tied absolute values
    i = 0
    while i < len(pairs):
        j = i + 1
        while j < len(pairs) and pairs[j][0] == pairs[i][0]:
            j += 1
        if j > i + 1:
            avg_rank = sum(ranks[i:j]) / (j - i)
            for k in range(i, j):
                ranks[k] = avg_rank
        i = j

    # Sum of ranks for positive and negative differences
    w_plus = sum(r for r, (_, d) in zip(ranks, pairs, strict=False) if d > 0)
    w_minus = sum(r for r, (_, d) in zip(ranks, pairs, strict=False) if d < 0)
    w = min(w_plus, w_minus)

    nr = len(pairs)
    if nr < 10:
        # Too few samples for normal approximation
        return w, float("nan")

    # Normal approximation
    mean_w = nr * (nr + 1) / 4
    std_w = math.sqrt(nr * (nr + 1) * (2 * nr + 1) / 24)
    if std_w == 0:
        return w, 1.0
    z = (w - mean_w) / std_w
    # Two-sided p-value from z using error function approximation
    p = 2 * (1 - _norm_cdf(abs(z)))
    return w, p


def _norm_cdf(z: float) -> float:
    """Standard normal CDF via the error function."""
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def cliffs_delta(x: list[float], y: list[float]) -> tuple[float, str]:
    """Cliff's delta effect size (non-parametric).

    Returns (delta, magnitude) where magnitude is one of:
    negligible (|d| < 0.147), small (< 0.33), medium (< 0.474), large (>= 0.474).
    Thresholds from Romano et al. (2006).
    """
    n_x, n_y = len(x), len(y)
    count = 0
    for xi in x:
        for yi in y:
            if xi > yi:
                count += 1
            elif xi < yi:
                count -= 1
    delta = count / (n_x * n_y)
    abs_d = abs(delta)
    if abs_d < 0.147:
        mag = "negligible"
    elif abs_d < 0.33:
        mag = "small"
    elif abs_d < 0.474:
        mag = "medium"
    else:
        mag = "large"
    return delta, mag


def bonferroni_adjust(p_values: list[float]) -> list[float]:
    """Bonferroni correction: multiply each p-value by the number of tests."""
    m = len(p_values)
    return [min(p * m, 1.0) for p in p_values]


# ── Descriptive statistics ──────────────────────────────────────────────────


@dataclass
class DescriptiveStats:
    """Summary statistics for a single benchmark configuration.

    The median CI (`ci_lower`, `ci_upper`) is the legacy field pair and
    remains the canonical 95% CI for the central-tendency estimate. P95 and
    P99 CIs (`p95_ci_lower`/`p95_ci_upper`, `p99_ci_lower`/`p99_ci_upper`)
    were added so callers can report tail-latency confidence bounds. All
    new fields default to 0.0 so existing callers that construct
    `DescriptiveStats(...)` positionally or by keyword remain compatible.
    """
    n_runs: int = 0
    n_samples_per_run: int = 0
    mean: float = 0.0
    median: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    max_val: float = 0.0
    std_dev: float = 0.0
    ci_lower: float = 0.0
    ci_upper: float = 0.0
    ci_level: float = 0.95
    # Tail-latency bootstrap CIs (added 2026-04-16, Bench-4).
    p95_ci_lower: float = 0.0
    p95_ci_upper: float = 0.0
    p99_ci_lower: float = 0.0
    p99_ci_upper: float = 0.0

    @classmethod
    def from_runs(
        cls,
        run_medians: list[float],
        all_samples: list[float],
        n_samples_per_run: int,
        run_samples: list[list[float]] | None = None,
    ) -> DescriptiveStats:
        """Compute stats from multiple independent runs.

        Parameters
        ----------
        run_medians:
            Median of each independent run (used for the median CI).
        all_samples:
            Pooled samples across all runs (used for percentiles).
        n_samples_per_run:
            Number of samples in each individual run.
        run_samples:
            Optional per-run raw sample lists. When supplied, P95/P99 CIs
            are bootstrapped over the per-run P95/P99 estimates, mirroring
            the methodology used for the median CI (spec §22.1.3). When
            omitted, P95/P99 CIs are bootstrapped from the pooled sample
            array directly — this is the backwards-compatible path for
            callers that do not retain per-run samples (Bench-4).

        Returns
        -------
        Populated `DescriptiveStats`. If `all_samples` is empty, returns a
        zeroed instance with `n_runs=len(run_medians)` so callers get a
        well-formed object to serialise even in degenerate configurations.
        """
        if not all_samples:
            return cls(
                n_runs=len(run_medians),
                n_samples_per_run=n_samples_per_run,
            )

        all_sorted = sorted(all_samples)
        n = len(all_sorted)
        ci_lo, ci_hi = bootstrap_ci(run_medians, stat_fn=statistics.median)

        if run_samples:
            # Methodologically preferred path: one P95/P99 estimate per run,
            # then bootstrap over those R estimates (matches spec §22.1.3).
            run_p95s = [percentile(s, 95.0) for s in run_samples if s]
            run_p99s = [percentile(s, 99.0) for s in run_samples if s]
            p95_ci_lo, p95_ci_hi = bootstrap_ci(run_p95s, stat_fn=statistics.median)
            p99_ci_lo, p99_ci_hi = bootstrap_ci(run_p99s, stat_fn=statistics.median)
        else:
            # Fallback: bootstrap the pooled sample array and take the
            # percentile on each resample. Each resample is the same size
            # as the input, so this is a valid percentile CI; it is simply
            # less granular than the per-run approach above.
            p95_ci_lo, p95_ci_hi = bootstrap_ci(
                all_samples, stat_fn=lambda s: percentile(s, 95.0)
            )
            p99_ci_lo, p99_ci_hi = bootstrap_ci(
                all_samples, stat_fn=lambda s: percentile(s, 99.0)
            )

        return cls(
            n_runs=len(run_medians),
            n_samples_per_run=n_samples_per_run,
            mean=statistics.mean(all_samples),
            median=statistics.median(all_samples),
            p95=all_sorted[int(n * 0.95)] if n > 20 else all_sorted[-1],
            p99=all_sorted[int(n * 0.99)] if n > 100 else all_sorted[-1],
            max_val=all_sorted[-1],
            std_dev=statistics.stdev(all_samples) if n > 1 else 0.0,
            ci_lower=ci_lo,
            ci_upper=ci_hi,
            p95_ci_lower=p95_ci_lo,
            p95_ci_upper=p95_ci_hi,
            p99_ci_lower=p99_ci_lo,
            p99_ci_upper=p99_ci_hi,
        )


def format_stats_table(
    label: str,
    stats: DescriptiveStats,
    unit: str = "ms",
) -> str:
    """Format a single stats row for console output."""
    return (
        f"  {label:<20s}  "
        f"Mean={stats.mean:8.3f} {unit}  "
        f"Median={stats.median:8.3f} {unit}  "
        f"P95={stats.p95:8.3f} {unit}  "
        f"P99={stats.p99:8.3f} {unit}  "
        f"SD={stats.std_dev:8.3f} {unit}  "
        f"95%CI=[{stats.ci_lower:.3f}, {stats.ci_upper:.3f}]"
    )


# ── CLI entry point ─────────────────────────────────────────────────────────
#
# The actual benchmark runners live in `benchmarks/suite.py` (the registry
# `suite.BENCHMARKS`). This file kept its own copies for several releases,
# which slowly drifted: by 2026-05 stats.py only listed 4 of the 9 benchmarks
# and silently dropped the rest under `--benchmark all`. The CLI now delegates
# to suite.BENCHMARKS so both runners stay in lock-step (audit 2026-05-04, #3).
#
# Note: suite.py imports the statistical primitives above from this module, so
# `from benchmarks.suite import BENCHMARKS` must remain a *lazy* import inside
# main() to avoid a circular import at module-load time.


def main() -> None:
    # Lazy import — `suite.py` imports the statistical primitives above from
    # this module, so importing it at file scope would create a cycle.
    from benchmarks.suite import BENCHMARKS

    benchmark_keys = list(BENCHMARKS.keys())

    parser = argparse.ArgumentParser(
        description=(
            "Run Arachnite benchmarks with statistical rigour. "
            "Per-benchmark runners are shared with `benchmarks/suite.py`."
        ),
    )
    parser.add_argument(
        "--benchmark", "-b",
        choices=[*benchmark_keys, "all"],
        default="all",
        help="Which benchmark to run (default: all)",
    )
    parser.add_argument(
        "--runs", "-n",
        type=int, default=30,
        help="Number of independent runs (default: 30)",
    )
    parser.add_argument(
        "--output-dir", "-o",
        type=str, default="benchmarks/results",
        help="Directory for JSON output files",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    targets = benchmark_keys if args.benchmark == "all" else [args.benchmark]
    platform_str = f"{sys.platform} / CPython {platform.python_version()}"

    async def run_selected() -> None:
        for name in targets:
            label, runner = BENCHMARKS[name]
            print(f"\n{'=' * 60}")
            print(f"  {label.upper()}  ({args.runs} runs)")
            print(f"{'=' * 60}")

            result = await runner(args.runs)
            wrapped = {
                "benchmark": name,
                "label": label,
                "platform": platform_str,
                "machine": platform.node(),
                "n_runs": args.runs,
                "result": result,
            }

            out_file = out_dir / f"{name}_{timestamp}.json"
            with open(out_file, "w") as f:
                json.dump(wrapped, f, indent=2)
            print(f"\n  Results saved to {out_file}")

    asyncio.run(run_selected())
    print(f"\nDone. All results in {out_dir}/")


if __name__ == "__main__":
    main()
