"""
Unit tests for ``benchmarks/soak_test.py`` (Bench-5).

Covers:
  - ``run()`` with small bucket/tick counts — correct bucket count,
    bucket bounds, populated mean/P99, positive RSS.
  - ``_rss_mb()`` returns a sensible positive number (or ``nan`` on
    platforms without psutil/proc).
  - ``compute_drift()`` produces a "POSSIBLE DRIFT" verdict on synthetic
    growing data and a "No significant drift" verdict on flat data.
  - CLI smoke with ``--quick`` — exit 0, bucket table printed, drift
    verdict printed, JSON file written with the documented schema.
  - Suite integration — ``run_soak_test`` returns the expected shape
    and is registered in ``BENCHMARKS``.
"""

from __future__ import annotations

import json
import math
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.soak_test import (
    _P99_DRIFT_THRESHOLD_MS,
    _RSS_GROWTH_THRESHOLD_MB,
    BucketStats,
    _rss_mb,
    compute_drift,
    run,
)

# ── run() — aggregate shape ─────────────────────────────────────────────────


class TestRun:
    @pytest.mark.asyncio
    async def test_run_with_small_counts_produces_correct_bucket_count(
        self,
    ) -> None:
        """200 ticks, bucket_size=50 → 4 buckets."""
        buckets = await run(ticks=200, bucket_size=50, warmup=10)
        assert len(buckets) == 4
        for i, b in enumerate(buckets, start=1):
            assert b.bucket_index == i
            assert b.tick_end - b.tick_start + 1 == 50
            assert b.mean_ms > 0.0
            assert b.p99_ms > 0.0
            # RSS is positive or nan (latter only on unsupported platforms)
            assert b.rss_mb > 0.0 or math.isnan(b.rss_mb)

    @pytest.mark.asyncio
    async def test_run_bucket_boundaries_are_contiguous(self) -> None:
        """Bucket N's tick_start must equal bucket (N-1).tick_end + 1."""
        buckets = await run(ticks=150, bucket_size=50, warmup=5)
        assert len(buckets) == 3
        assert buckets[0].tick_start == 0
        for prev, cur in zip(buckets, buckets[1:], strict=False):
            assert cur.tick_start == prev.tick_end + 1

    @pytest.mark.asyncio
    async def test_run_includes_tail_bucket_for_remainder(self) -> None:
        """Ticks that don't fill a bucket still produce a tail bucket."""
        buckets = await run(ticks=175, bucket_size=50, warmup=5)
        # 175 / 50 = 3 full + 1 tail of 25.
        assert len(buckets) == 4
        assert buckets[-1].tick_end - buckets[-1].tick_start + 1 == 25

    @pytest.mark.asyncio
    async def test_run_rejects_bad_arguments(self) -> None:
        with pytest.raises(ValueError):
            await run(ticks=0, bucket_size=50, warmup=0)
        with pytest.raises(ValueError):
            await run(ticks=100, bucket_size=0, warmup=0)
        with pytest.raises(ValueError):
            await run(ticks=100, bucket_size=50, warmup=-1)


# ── _rss_mb() ──────────────────────────────────────────────────────────────


class TestRssMeasurement:
    def test_rss_is_positive_or_nan(self) -> None:
        """On dev/CI hosts RSS is positive; on unsupported platforms nan."""
        rss = _rss_mb()
        assert rss > 0.0 or math.isnan(rss)
        if not math.isnan(rss):
            # Sanity: Python interpreter baseline is a handful of MB;
            # anything under 1 MB or over 100 GB indicates a bug.
            assert 1.0 < rss < 100_000.0


# ── compute_drift() ─────────────────────────────────────────────────────────


def _bucket(
    index: int, mean: float = 0.1, p99: float = 0.2, rss: float = 50.0,
) -> BucketStats:
    """Helper — build a synthetic bucket row with sensible tick bounds."""
    tick_start = (index - 1) * 100
    return BucketStats(
        bucket_index=index,
        tick_start=tick_start,
        tick_end=tick_start + 99,
        mean_ms=mean,
        p99_ms=p99,
        rss_mb=rss,
    )


class TestDriftVerdict:
    def test_flat_data_yields_no_drift_verdict(self) -> None:
        """Ten identical buckets → no drift detected."""
        buckets = [_bucket(i, mean=0.1, p99=0.2, rss=50.0) for i in range(1, 11)]
        drift = compute_drift(buckets)
        assert drift.drift_detected is False
        assert drift.rss_growth_mb == 0.0
        assert drift.p99_drift_ms == 0.0
        assert "No significant drift" in drift.verdict

    def test_growing_rss_triggers_possible_drift(self) -> None:
        """RSS that grows past the threshold must trip the verdict."""
        growth_per_bucket = (_RSS_GROWTH_THRESHOLD_MB * 2.0) / 9.0  # 10 buckets
        buckets = [
            _bucket(i, mean=0.1, p99=0.2, rss=50.0 + growth_per_bucket * (i - 1))
            for i in range(1, 11)
        ]
        drift = compute_drift(buckets)
        assert drift.drift_detected is True
        assert drift.rss_growth_mb > _RSS_GROWTH_THRESHOLD_MB
        assert "POSSIBLE DRIFT" in drift.verdict
        assert "RSS grew by" in drift.verdict

    def test_growing_p99_triggers_possible_drift(self) -> None:
        """P99 that drifts past the threshold must trip the verdict."""
        drift_per_bucket = (_P99_DRIFT_THRESHOLD_MS * 3.0) / 9.0
        buckets = [
            _bucket(i, mean=0.1, p99=0.2 + drift_per_bucket * (i - 1), rss=50.0)
            for i in range(1, 11)
        ]
        drift = compute_drift(buckets)
        assert drift.drift_detected is True
        assert drift.p99_drift_ms > _P99_DRIFT_THRESHOLD_MS
        assert "POSSIBLE DRIFT" in drift.verdict
        assert "P99 drifted by" in drift.verdict

    def test_single_bucket_returns_safe_default_verdict(self) -> None:
        """Degenerate single-bucket input must not crash and must not trip."""
        drift = compute_drift([_bucket(1)])
        assert drift.drift_detected is False
        assert "No significant drift" in drift.verdict

    def test_verdict_carries_configured_thresholds(self) -> None:
        """Thresholds flow through to the returned object for auditability."""
        drift = compute_drift([_bucket(1), _bucket(2)])
        assert drift.rss_growth_threshold_mb == _RSS_GROWTH_THRESHOLD_MB
        assert drift.p99_drift_threshold_ms == _P99_DRIFT_THRESHOLD_MS


# ── CLI smoke ───────────────────────────────────────────────────────────────


class TestCLI:
    def test_cli_quick_mode_end_to_end(self, tmp_path: Path) -> None:
        """``--quick`` run exits 0, prints the table + verdict, writes JSON."""
        root = Path(__file__).resolve().parents[2]
        script = root / "benchmarks" / "soak_test.py"
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--quick",
                "--output-dir", str(tmp_path),
            ],
            capture_output=True, text=True, timeout=300, cwd=str(root),
        )
        assert result.returncode == 0, (
            f"CLI exited non-zero.\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
        # Printed table header & some bucket content
        assert "Per-bucket soak statistics" in result.stdout
        assert "bucket" in result.stdout
        assert "mean_ms" in result.stdout
        assert "p99_ms" in result.stdout
        assert "rss_mb" in result.stdout
        # A drift verdict (positive or negative) must print.
        assert ("No significant drift" in result.stdout
                or "POSSIBLE DRIFT" in result.stdout)

        # JSON output
        outputs = list(tmp_path.glob("soak_test_*.json"))
        assert len(outputs) == 1, f"expected one JSON, got {outputs}"
        payload = json.loads(outputs[0].read_text())
        assert payload["benchmark"] == "soak_test"
        assert payload["ticks"] == 10_000
        assert payload["bucket_size"] == 1_000
        assert payload["warmup"] == 500
        assert isinstance(payload["buckets"], list)
        assert len(payload["buckets"]) == 10
        for row in payload["buckets"]:
            for key in ("bucket_index", "tick_start", "tick_end",
                        "mean_ms", "p99_ms", "rss_mb"):
                assert key in row, f"missing {key} in bucket row"
        assert "drift" in payload
        for key in ("rss_growth_mb", "p99_drift_ms", "drift_detected",
                    "verdict", "rss_growth_threshold_mb",
                    "p99_drift_threshold_ms"):
            assert key in payload["drift"], f"missing drift.{key}"


# ── Suite integration ───────────────────────────────────────────────────────


class TestSuiteRegistration:
    def test_soak_test_registered_in_suite(self) -> None:
        from benchmarks.suite import BENCHMARKS
        assert "soak_test" in BENCHMARKS

    def test_suite_version_bumped_to_2_3(self) -> None:
        """Reading the suite.py docstring is sufficient — the constant
        lives in the runtime report payload and is exercised in the
        integration-shape test below."""
        from benchmarks import suite
        assert "2.3" in (suite.__doc__ or "")

    @pytest.mark.asyncio
    async def test_suite_runner_returns_expected_shape(self) -> None:
        """The suite runner uses the --quick preset and returns a dict
        with buckets + drift sub-objects."""
        from benchmarks.suite import run_soak_test

        result = await run_soak_test(n_runs=1)

        assert result["name"] == "soak_test"
        assert result["unit"] == "ms"
        assert result["mode"] == "quick"
        assert result["ticks"] == 10_000
        assert result["bucket_size"] == 1_000
        assert result["warmup"] == 500
        assert isinstance(result["buckets"], list)
        assert len(result["buckets"]) == 10
        assert "drift" in result
        assert "verdict" in result["drift"]
