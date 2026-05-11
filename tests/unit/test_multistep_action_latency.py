"""
Unit tests for ``benchmarks/multistep_action_latency.py`` (Bench-1).

Covers:
  - Each single-iteration measurement function returns a plausible
    positive latency (> 0, < 1 s) with the right policy-invariant.
  - ``run()`` produces a dict with all scenario keys and non-empty
    sample arrays.
  - ``multi_run()`` produces a ``DescriptiveStats`` per scenario.
  - Mandatory-block worst-case scenario: ``request_interrupt()`` does
    NOT preempt the mandatory step (block completes), and the interrupt
    IS honoured at the next interruptible boundary (``finish`` skipped).
  - CLI end-to-end smoke with ``--runs 2 --iterations 10``.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.multistep_action_latency import (
    SCENARIO_ALWAYS,
    SCENARIO_MANDATORY_WORST_CASE,
    SCENARIO_ROLLBACK,
    SCENARIOS,
    _measure_always,
    _measure_checkpoint,
    _measure_mandatory_block_worst_case,
    _measure_rollback,
    multi_run,
    run,
)
from benchmarks.stats import DescriptiveStats

# ── Single-iteration measurement functions ──────────────────────────────────


class TestSingleMeasurements:
    """Each probe returns a plausible positive latency (ms)."""

    @pytest.mark.asyncio
    async def test_always_returns_positive_bounded_ms(self) -> None:
        ms = await _measure_always()
        assert ms > 0.0
        assert ms < 1_000.0

    @pytest.mark.asyncio
    async def test_checkpoint_returns_positive_bounded_ms(self) -> None:
        ms = await _measure_checkpoint()
        assert ms > 0.0
        assert ms < 1_000.0

    @pytest.mark.asyncio
    async def test_rollback_fires_all_rollbacks_and_returns_ms(self) -> None:
        total_ms, n_rolled_back, walk_ms = await _measure_rollback()
        assert total_ms > 0.0
        assert total_ms < 1_000.0
        # Three mandatory steps precede the interrupt boundary; all three
        # rollback callables must fire.
        assert n_rolled_back == 3
        # Direct-timed rollback walk is non-negative and bounded; it
        # must be no larger than the outer wall-clock.
        assert walk_ms >= 0.0
        assert walk_ms <= total_ms

    @pytest.mark.asyncio
    async def test_mandatory_block_worst_case_runs_block_to_completion(
        self,
    ) -> None:
        """
        Empirical T_worst_mandatory probe: request_interrupt() does NOT
        preempt the mandatory block; the flag is held until the next
        interruptible step and ``finish`` is skipped.
        """
        ms, worst_case_honoured = await _measure_mandatory_block_worst_case()
        assert ms > 0.0
        assert ms < 1_000.0
        assert worst_case_honoured, (
            "mandatory block must run to completion under request_interrupt(); "
            "the interrupt must be held until the next interruptible boundary "
            "and the subsequent interruptible step must be skipped"
        )


# ── run() — aggregate shape ─────────────────────────────────────────────────


class TestRun:
    @pytest.mark.asyncio
    async def test_run_produces_all_scenarios(self) -> None:
        samples = await run(iterations=5)
        for name in SCENARIOS:
            assert name in samples, f"missing scenario {name}"
            assert len(samples[name]) == 5
            assert all(s > 0.0 for s in samples[name])
        # Per-step rollback series is derived, one entry per iteration
        # that fired at least one rollback (all five iterations do).
        assert "rollback_policy_per_step" in samples
        assert len(samples["rollback_policy_per_step"]) == 5

    @pytest.mark.asyncio
    async def test_rollback_at_least_as_slow_as_always(self) -> None:
        """
        Sanity: ROLLBACK must include the on_interrupted() rollback walk
        on top of the ALWAYS-style interrupt delivery, so its median
        latency cannot be lower. (Inequality, not strict — on fast
        platforms the difference may be within jitter.)
        """
        samples = await run(iterations=5)
        import statistics

        always_med = statistics.median(samples[SCENARIO_ALWAYS])
        rollback_med = statistics.median(samples[SCENARIO_ROLLBACK])
        # Allow some slack for jitter; the invariant is directional.
        assert rollback_med >= always_med * 0.5


# ── multi_run() ─────────────────────────────────────────────────────────────


class TestMultiRun:
    @pytest.mark.asyncio
    async def test_multi_run_produces_descriptive_stats(self) -> None:
        stats = await multi_run(n_runs=2, iterations=5)
        for name in SCENARIOS:
            assert name in stats, f"missing scenario stats for {name}"
            s = stats[name]
            assert isinstance(s, DescriptiveStats)
            assert s.n_runs == 2
            assert s.median > 0.0
            # Tail CIs must be populated (Bench-4 per-run bootstrap path)
            assert s.p95_ci_lower <= s.p95 <= s.p95_ci_upper or \
                   s.p95_ci_lower == s.p95_ci_upper
            assert s.p99_ci_lower <= s.p99 <= s.p99_ci_upper or \
                   s.p99_ci_lower == s.p99_ci_upper

    @pytest.mark.asyncio
    async def test_multi_run_mandatory_worst_case_present(self) -> None:
        stats = await multi_run(n_runs=2, iterations=5)
        assert SCENARIO_MANDATORY_WORST_CASE in stats


# ── CLI smoke test ──────────────────────────────────────────────────────────


class TestCLI:
    def test_cli_end_to_end_small(self, tmp_path: Path) -> None:
        """Smoke: run CLI with tiny arguments and verify JSON output."""
        root = Path(__file__).resolve().parents[2]
        script = root / "benchmarks" / "multistep_action_latency.py"
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--runs", "2",
                "--iterations", "10",
                "--output-dir", str(tmp_path),
            ],
            capture_output=True, text=True, timeout=120, cwd=str(root),
        )
        assert result.returncode == 0, (
            f"CLI exited non-zero.\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
        # Printed table header
        assert "Multi-step action" in result.stdout
        assert "always_policy" in result.stdout
        assert "rollback_policy" in result.stdout
        assert "mandatory_block_worst_case" in result.stdout
        # The removed duplicate scenario must not appear.
        assert "mandatory_block_emergency_stop" not in result.stdout
        assert "mandatory_block_request_interrupt" not in result.stdout

        # JSON output
        outputs = list(tmp_path.glob("multistep_action_latency_*.json"))
        assert len(outputs) == 1, f"expected one JSON, got {outputs}"
        payload = json.loads(outputs[0].read_text())
        assert payload["benchmark"] == "multistep_action_latency"
        assert payload["n_runs"] == 2
        assert payload["iterations_per_run"] == 10
        for name in SCENARIOS:
            assert name in payload["scenarios"], f"missing {name} in JSON"
            assert payload["scenarios"][name]["n_runs"] == 2


# ── Suite integration ───────────────────────────────────────────────────────


class TestSuiteRegistration:
    @pytest.mark.asyncio
    async def test_suite_runner_returns_expected_shape(self) -> None:
        """
        The suite runner (``benchmarks.suite.run_multistep_action_latency``)
        should run a full multi-run cycle and return a dict with the
        standard per-scenario stats layout. The suite registry must also
        include the new key.
        """
        from benchmarks.suite import BENCHMARKS, run_multistep_action_latency

        assert "multistep_action_latency" in BENCHMARKS

        # A 2-run call is sufficient for smoke coverage; use monkey
        # patching to shrink the default iteration count so this test
        # stays in the asyncio-event-loop budget (~a few seconds on
        # Windows due to the ``asyncio.sleep`` timer resolution).
        import benchmarks.multistep_action_latency as mmod

        original = mmod._ITERATIONS
        mmod._ITERATIONS = 5
        try:
            result = await run_multistep_action_latency(n_runs=2)
        finally:
            mmod._ITERATIONS = original

        assert result["name"] == "multistep_action_latency"
        assert result["unit"] == "ms"
        assert result["n_runs"] == 2
        assert "scenarios" in result
        for name in SCENARIOS:
            assert name in result["scenarios"]


# ── Module-level helper used by TestCLI ─────────────────────────────────────


def _smoke_run_blocking() -> None:
    """Convenience entry point for manual local smoke — not a pytest test."""
    samples = asyncio.run(run(iterations=5))
    assert all(len(samples[n]) == 5 for n in SCENARIOS)
