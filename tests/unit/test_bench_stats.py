"""
Unit tests for benchmarks/stats.py statistical primitives (spec §22.9).

Covers:
  - `bootstrap_ci()` with custom `stat_fn` (mean, median, percentile)
  - `percentile()` helper used by the P95/P99 CI path
  - `DescriptiveStats.from_runs()` backwards compatibility (median CI)
  - `DescriptiveStats.from_runs()` P95 and P99 bootstrap CIs (Bench-4)
  - Empty / degenerate input handling
"""

from __future__ import annotations

import statistics

import pytest

from benchmarks.stats import (
    DescriptiveStats,
    bootstrap_ci,
    percentile,
)

# ── percentile() ────────────────────────────────────────────────────────────


def test_percentile_basic() -> None:
    data = list(range(1, 101))  # 1..100
    # nearest-rank: index = int(100 * 0.95) = 95 → data[95] = 96
    assert percentile(data, 95.0) == 96
    # index = int(100 * 0.99) = 99 → data[99] = 100
    assert percentile(data, 99.0) == 100
    assert percentile(data, 50.0) == 51


def test_percentile_handles_unsorted_input() -> None:
    data = [5.0, 1.0, 4.0, 2.0, 3.0]
    assert percentile(data, 50.0) == 3.0


def test_percentile_rejects_empty() -> None:
    with pytest.raises(ValueError):
        percentile([], 95.0)


def test_percentile_rejects_out_of_range() -> None:
    with pytest.raises(ValueError):
        percentile([1.0, 2.0], -1.0)
    with pytest.raises(ValueError):
        percentile([1.0, 2.0], 101.0)


def test_percentile_100_returns_max() -> None:
    # idx = int(n * 1.0) == n, which the helper clamps to n-1 (the max).
    data = [10.0, 20.0, 30.0]
    assert percentile(data, 100.0) == 30.0


# ── bootstrap_ci() with custom statistics ──────────────────────────────────


def test_bootstrap_ci_default_mean_backwards_compatible() -> None:
    """Default stat_fn is still statistics.mean (regression guard)."""
    rng_data = [float(i) for i in range(1, 101)]
    lo, hi = bootstrap_ci(rng_data, n_bootstrap=500)
    # Sample mean is 50.5; CI must bracket it.
    assert lo < 50.5 < hi
    # Reasonable tightness for n=100 (uniform 1..100 has SD ≈ 28.9, so the
    # mean's 95% CI half-width should be well under ~6, total width < 15).
    assert hi - lo < 15.0


def test_bootstrap_ci_accepts_median() -> None:
    data = [float(i) for i in range(1, 101)]
    lo, hi = bootstrap_ci(data, stat_fn=statistics.median, n_bootstrap=500)
    # Sample median is 50.5; CI must bracket it.
    assert lo < 50.5 < hi


def test_bootstrap_ci_accepts_percentile_lambda() -> None:
    """Reviewers expect P95/P99 CIs — verify the API supports them."""
    data = [float(i) for i in range(1, 101)]  # P95 ≈ 96
    lo, hi = bootstrap_ci(
        data, stat_fn=lambda s: percentile(s, 95.0), n_bootstrap=500
    )
    # Nearest-rank P95 on 1..100 is 96; bootstrap CI must bracket a plausible
    # range around it (not too tight, not too loose).
    assert 90.0 <= lo <= 96.0
    assert 96.0 <= hi <= 100.0


def test_bootstrap_ci_is_reproducible_with_same_seed() -> None:
    data = [float(i) for i in range(50)]
    a = bootstrap_ci(data, seed=7, n_bootstrap=200)
    b = bootstrap_ci(data, seed=7, n_bootstrap=200)
    assert a == b


def test_bootstrap_ci_empty_input_returns_nan() -> None:
    lo, hi = bootstrap_ci([])
    assert lo != lo  # NaN
    assert hi != hi  # NaN


# ── DescriptiveStats.from_runs() — legacy median CI (regression guard) ─────


def test_from_runs_median_ci_still_populated() -> None:
    """Existing callers must still get a median CI on the legacy fields."""
    run_medians = [1.0, 1.05, 0.95, 1.02, 0.98, 1.01, 0.99, 1.03, 0.97, 1.04]
    all_samples = [v for m in run_medians for v in (m - 0.1, m, m + 0.1)]
    stats = DescriptiveStats.from_runs(run_medians, all_samples, 3)

    assert stats.n_runs == 10
    assert stats.n_samples_per_run == 3
    # Median CI brackets the true median (≈1.0).
    assert stats.ci_lower <= 1.0 <= stats.ci_upper
    # CI is finite.
    assert stats.ci_lower == stats.ci_lower  # not NaN
    assert stats.ci_upper == stats.ci_upper


def test_from_runs_preserves_mean_median_p95_p99() -> None:
    """Numeric fields unrelated to Bench-4 remain correct."""
    samples = [float(i) for i in range(1, 1001)]
    stats = DescriptiveStats.from_runs([500.5] * 5, samples, 200)
    assert stats.mean == pytest.approx(statistics.mean(samples))
    assert stats.median == pytest.approx(statistics.median(samples))
    # nearest-rank percentiles consistent with the pre-Bench-4 rules.
    assert stats.p95 == samples[int(1000 * 0.95)]
    assert stats.p99 == samples[int(1000 * 0.99)]
    assert stats.max_val == 1000.0


# ── DescriptiveStats.from_runs() — P95 CI (Bench-4) ────────────────────────


def test_from_runs_p95_ci_reasonable_on_known_distribution() -> None:
    """
    On a uniform(0, 100) distribution, the true P95 ≈ 95. Bootstrap CI
    from ~3k samples should bracket 95 and be tight (well under ±10).
    """
    # Seed the synthetic data for determinism. We use `random.Random` so the
    # test doesn't affect the global rng state.
    import random as _random
    rng = _random.Random(1234)
    run_samples = [
        [rng.uniform(0.0, 100.0) for _ in range(300)]
        for _ in range(10)
    ]
    run_medians = [statistics.median(s) for s in run_samples]
    all_samples = [v for s in run_samples for v in s]

    stats = DescriptiveStats.from_runs(
        run_medians, all_samples, 300, run_samples=run_samples
    )

    # Finite and ordered.
    assert stats.p95_ci_lower <= stats.p95_ci_upper
    # CI must bracket the true P95 (95.0) with wide enough tolerance for the
    # sample size. Uniform P95 with n=300 has low sampling error.
    assert 85.0 <= stats.p95_ci_lower <= 95.0
    assert 95.0 <= stats.p95_ci_upper <= 100.0
    # CI should be well inside the data range.
    assert stats.p95_ci_lower > 0.0


def test_from_runs_p95_ci_without_run_samples_still_populated() -> None:
    """
    Backwards-compat path: when callers don't pass `run_samples`, the
    pooled-bootstrap fallback still returns a finite, sensible P95 CI.
    """
    import random as _random
    rng = _random.Random(99)
    all_samples = [rng.uniform(0.0, 100.0) for _ in range(600)]
    run_medians = [statistics.median(all_samples)]

    stats = DescriptiveStats.from_runs(run_medians, all_samples, 600)
    assert stats.p95_ci_lower <= stats.p95_ci_upper
    # Wide tolerance: pooled fallback is less precise than per-run, but
    # must still bracket the neighborhood of the true P95 (~95).
    assert 80.0 <= stats.p95_ci_lower <= 100.0
    assert 90.0 <= stats.p95_ci_upper <= 100.0


# ── DescriptiveStats.from_runs() — P99 CI (Bench-4) ────────────────────────


def test_from_runs_p99_ci_reasonable_on_known_distribution() -> None:
    """
    On a uniform(0, 100) distribution, the true P99 ≈ 99. Bootstrap CI
    over per-run P99s should bracket 99 and be ordered (lower <= upper).
    """
    import random as _random
    rng = _random.Random(5678)
    run_samples = [
        [rng.uniform(0.0, 100.0) for _ in range(500)]
        for _ in range(15)
    ]
    run_medians = [statistics.median(s) for s in run_samples]
    all_samples = [v for s in run_samples for v in s]

    stats = DescriptiveStats.from_runs(
        run_medians, all_samples, 500, run_samples=run_samples
    )

    assert stats.p99_ci_lower <= stats.p99_ci_upper
    # True P99 of uniform(0,100) is 99.
    assert 95.0 <= stats.p99_ci_lower <= 99.0
    assert 99.0 <= stats.p99_ci_upper <= 100.5
    # P99 CI should sit above the P95 CI (tail is higher than upper-quantile).
    assert stats.p99_ci_lower >= stats.p95_ci_lower


# ── Empty / degenerate inputs ──────────────────────────────────────────────


def test_from_runs_empty_samples_returns_zeroed_stats() -> None:
    """Degenerate input must not raise; produces a zero-filled instance."""
    stats = DescriptiveStats.from_runs([], [], 0)
    assert stats.n_runs == 0
    assert stats.n_samples_per_run == 0
    assert stats.mean == 0.0
    assert stats.median == 0.0
    assert stats.p95 == 0.0
    assert stats.p99 == 0.0
    # Defaults for new CI fields stay at 0.0 so JSON-serialising the result
    # always yields a structurally valid object.
    assert stats.p95_ci_lower == 0.0
    assert stats.p95_ci_upper == 0.0
    assert stats.p99_ci_lower == 0.0
    assert stats.p99_ci_upper == 0.0


def test_from_runs_single_sample_does_not_crash() -> None:
    """Exactly one sample is a common edge case for smoke tests."""
    stats = DescriptiveStats.from_runs([42.0], [42.0], 1)
    assert stats.n_runs == 1
    assert stats.mean == 42.0
    assert stats.median == 42.0
    assert stats.std_dev == 0.0
    # CI fields are finite (all resamples are [42.0], so CI collapses to 42).
    assert stats.ci_lower == 42.0
    assert stats.ci_upper == 42.0
    assert stats.p95_ci_lower == 42.0
    assert stats.p95_ci_upper == 42.0
    assert stats.p99_ci_lower == 42.0
    assert stats.p99_ci_upper == 42.0


def test_from_runs_constructor_default_zeroed() -> None:
    """Legacy callers that construct DescriptiveStats() with no args still work."""
    stats = DescriptiveStats()
    assert stats.n_runs == 0
    assert stats.p95_ci_lower == 0.0
    assert stats.p99_ci_upper == 0.0
    assert stats.ci_level == 0.95
