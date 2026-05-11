"""
Unit tests for ``benchmarks/transport_latency.py`` (Bench-2).

Covers:
  - ``run()`` with ``LocalTransport`` and tiny iteration count produces
    populated samples and a well-formed report.
  - Env-var-unset case for each broker transport produces
    ``status="skipped"`` with a sensible note (and stays skipped without
    requiring the optional dep).
  - Env-var-set-but-dep-missing produces a loud failure (simulated by
    monkeypatching the ``_*_AVAILABLE`` flag on the transport module to
    ``False`` so the constructor raises ``ImportError`` per ADR §1).
  - Payload-size sweep runs all three sizes (8 B / 1 KB / 64 KB).
  - ``--quick`` flag uses the documented ``_QUICK_ITERATIONS_*`` defaults.
  - CLI smoke (``--quick`` with no env vars) — exits 0, prints both
    tables, writes JSON, broker entries marked ``"skipped"``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.transport_latency import (
    _BENCH_SIGNAL_KIND,
    _ENV_VAR_MQTT,
    _ENV_VAR_NATS,
    _ENV_VAR_REDIS,
    _PAYLOAD_SIZES_B,
    _QUICK_ITERATIONS_LOCAL,
    _QUICK_ITERATIONS_MQTT,
    _QUICK_ITERATIONS_NATS,
    _QUICK_ITERATIONS_REDIS,
    _STATUS_MEASURED,
    _STATUS_SKIPPED,
    _make_payload,
    _run_local,
    assemble_report,
    run,
)

# ── Payload helpers ─────────────────────────────────────────────────────────


class TestPayloadHelpers:
    def test_make_payload_returns_exact_size(self) -> None:
        for size in (8, 1024, 65536):
            buf = _make_payload(size)
            assert isinstance(buf, bytes)
            assert len(buf) == size


# ── _run_local — single-transport drive ─────────────────────────────────────


class TestRunLocal:
    @pytest.mark.asyncio
    async def test_local_runs_all_payload_sizes(self) -> None:
        report = await _run_local(iterations=20, n_runs=2)
        assert report.transport == "LocalTransport"
        assert report.status == _STATUS_MEASURED
        # One cell per documented payload size.
        sizes = [c.payload_size_b for c in report.cells]
        assert sizes == list(_PAYLOAD_SIZES_B)

    @pytest.mark.asyncio
    async def test_local_cells_carry_populated_stats(self) -> None:
        report = await _run_local(iterations=30, n_runs=2)
        for cell in report.cells:
            # Every sample is a positive ms latency; the median must be
            # > 0 even on the fastest in-process delivery path.
            assert cell.iterations_per_run == 30
            assert cell.n_runs == 2
            assert cell.stats.median > 0.0
            assert cell.stats.mean > 0.0
            # Sample count carried through unchanged.
            assert cell.stats.n_samples_per_run == 30

    @pytest.mark.asyncio
    async def test_local_p99_ci_populated_when_per_run_supplied(self) -> None:
        """run_samples is wired through, so P95/P99 CIs are non-zero."""
        report = await _run_local(iterations=60, n_runs=3)
        first = report.cells[0]
        # At least one of the bootstrap-CI bounds is positive once we
        # have enough samples for the percentile to be meaningful.
        assert first.stats.p95 > 0.0
        assert first.stats.p99 > 0.0


# ── run() — full driver, env-var-unset case ─────────────────────────────────


class TestRunNoBrokerEnv:
    """When no broker env vars are set, only LocalTransport measures."""

    @pytest.mark.asyncio
    async def test_run_skips_brokers_silently_when_env_vars_unset(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Strip any operator-side env to make this test deterministic.
        monkeypatch.delenv(_ENV_VAR_MQTT,  raising=False)
        monkeypatch.delenv(_ENV_VAR_NATS,  raising=False)
        monkeypatch.delenv(_ENV_VAR_REDIS, raising=False)

        reports = await run(iterations_override=10, n_runs=2)

        by_name = {r.transport: r for r in reports}
        assert by_name["LocalTransport"].status == _STATUS_MEASURED
        assert by_name["LocalTransport"].cells, "Local cells must populate"

        for name in ("MQTTTransport", "NATSTransport", "RedisTransport"):
            tr = by_name[name]
            assert tr.status == _STATUS_SKIPPED
            assert tr.cells == []
            # Sensible note string mentioning the env var name.
            assert "ARACHNITE_TEST_" in tr.note

    @pytest.mark.asyncio
    async def test_run_iterations_override_applies_uniformly(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An explicit --iterations flag overrides per-transport defaults."""
        monkeypatch.delenv(_ENV_VAR_MQTT,  raising=False)
        monkeypatch.delenv(_ENV_VAR_NATS,  raising=False)
        monkeypatch.delenv(_ENV_VAR_REDIS, raising=False)
        reports = await run(iterations_override=15, n_runs=1)
        local = next(r for r in reports if r.transport == "LocalTransport")
        for cell in local.cells:
            assert cell.iterations_per_run == 15

    @pytest.mark.asyncio
    async def test_run_quick_uses_documented_iteration_counts(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """--quick (no override) selects the _QUICK_ITERATIONS_LOCAL value."""
        monkeypatch.delenv(_ENV_VAR_MQTT,  raising=False)
        monkeypatch.delenv(_ENV_VAR_NATS,  raising=False)
        monkeypatch.delenv(_ENV_VAR_REDIS, raising=False)
        reports = await run(iterations_override=None, n_runs=1, quick=True)
        local = next(r for r in reports if r.transport == "LocalTransport")
        assert local.iterations_per_run == _QUICK_ITERATIONS_LOCAL
        for cell in local.cells:
            assert cell.iterations_per_run == _QUICK_ITERATIONS_LOCAL


# ── Env-var-set-but-dep-missing → loud failure ──────────────────────────────


class TestBrokerLoudFailureOnMissingDep:
    """ADR §1: env var set but optional dep missing → raise loud."""

    @pytest.mark.asyncio
    async def test_mqtt_loud_failure_when_aiomqtt_missing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Simulate aiomqtt not being installed by flipping the
        # availability flag on the transport module. The transport
        # constructor checks this flag and raises ImportError —
        # surfacing as a loud failure per ADR §1, not a silent skip.
        import arachnite.transport.mqtt as mqtt_mod
        monkeypatch.setattr(mqtt_mod, "_AIOMQTT_AVAILABLE", False)
        monkeypatch.setenv(_ENV_VAR_MQTT, "mqtt://localhost:1883")
        monkeypatch.delenv(_ENV_VAR_NATS,  raising=False)
        monkeypatch.delenv(_ENV_VAR_REDIS, raising=False)

        with pytest.raises(ImportError, match="aiomqtt"):
            await run(iterations_override=5, n_runs=1)

    @pytest.mark.asyncio
    async def test_nats_loud_failure_when_nats_missing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import arachnite.transport.nats as nats_mod
        monkeypatch.setattr(nats_mod, "_NATS_AVAILABLE", False)
        monkeypatch.setenv(_ENV_VAR_NATS, "nats://localhost:4222")
        monkeypatch.delenv(_ENV_VAR_MQTT,  raising=False)
        monkeypatch.delenv(_ENV_VAR_REDIS, raising=False)

        with pytest.raises(ImportError, match="nats-py"):
            await run(iterations_override=5, n_runs=1)

    @pytest.mark.asyncio
    async def test_redis_loud_failure_when_redis_missing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import arachnite.transport.redis as redis_mod
        monkeypatch.setattr(redis_mod, "_REDIS_AVAILABLE", False)
        monkeypatch.setenv(_ENV_VAR_REDIS, "redis://localhost:6379")
        monkeypatch.delenv(_ENV_VAR_MQTT, raising=False)
        monkeypatch.delenv(_ENV_VAR_NATS, raising=False)

        with pytest.raises(ImportError, match="redis"):
            await run(iterations_override=5, n_runs=1)


# ── JSON serialisation ──────────────────────────────────────────────────────


class TestAssembleReport:
    @pytest.mark.asyncio
    async def test_assembled_report_carries_local_and_skipped_brokers(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv(_ENV_VAR_MQTT,  raising=False)
        monkeypatch.delenv(_ENV_VAR_NATS,  raising=False)
        monkeypatch.delenv(_ENV_VAR_REDIS, raising=False)
        reports = await run(iterations_override=8, n_runs=1)
        payload = assemble_report(reports, n_runs=1, elapsed_s=0.5)

        assert payload["benchmark"] == "transport_latency"
        assert payload["unit"] == "ms"
        assert payload["payload_sizes_b"] == list(_PAYLOAD_SIZES_B)
        assert payload["n_runs"] == 1

        transports = payload["transports"]
        local = transports["LocalTransport"]
        assert local["status"] == _STATUS_MEASURED
        assert "by_size" in local
        # Three documented payload sizes, all populated.
        assert set(local["by_size"].keys()) == {
            str(s) for s in _PAYLOAD_SIZES_B
        }

        for name in ("MQTTTransport", "NATSTransport", "RedisTransport"):
            entry = transports[name]
            assert entry["status"] == _STATUS_SKIPPED
            assert "ARACHNITE_TEST_" in entry["note"]
            assert "by_size" not in entry


# ── Suite-quick-iteration constants ─────────────────────────────────────────


class TestQuickConstants:
    def test_quick_iterations_within_documented_scaling(self) -> None:
        """ADR §4: --quick scales every default by ~0.1x."""
        # Local: 50,000 -> 5,000 (exact 0.1x).
        assert _QUICK_ITERATIONS_LOCAL == 5_000
        # Brokers: scaled to 200 (close to 0.1x of MQTT 2_000;
        # ~0.04x of NATS/Redis 5_000 — ADR allows "~0.1x").
        assert _QUICK_ITERATIONS_MQTT == 200
        assert _QUICK_ITERATIONS_NATS == 200
        assert _QUICK_ITERATIONS_REDIS == 200


# ── CLI smoke ───────────────────────────────────────────────────────────────


class TestCLI:
    def test_cli_quick_smoke_no_env_vars(self, tmp_path: Path) -> None:
        """``--quick`` with no broker env vars: exit 0, JSON written."""
        root = Path(__file__).resolve().parents[2]
        script = root / "benchmarks" / "transport_latency.py"

        # Strip any operator-side env so the smoke run is deterministic.
        env = os.environ.copy()
        for var in (_ENV_VAR_MQTT, _ENV_VAR_NATS, _ENV_VAR_REDIS):
            env.pop(var, None)

        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--quick",
                "--runs", "2",
                "--iterations", "20",
                "--output-dir", str(tmp_path),
            ],
            capture_output=True, text=True, timeout=300, cwd=str(root),
            env=env,
        )
        assert result.returncode == 0, (
            f"CLI exited non-zero.\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

        # Both tables must print, and broker entries say "skipped".
        assert "LocalTransport publish-to-wake latency" in result.stdout
        assert "Broker transport publish-to-deliver latency" in result.stdout
        assert "MQTTTransport: skipped" in result.stdout
        assert "NATSTransport: skipped" in result.stdout
        assert "RedisTransport: skipped" in result.stdout

        # JSON output: exactly one file, with the documented schema.
        outputs = list(tmp_path.glob("transport_latency_*.json"))
        assert len(outputs) == 1, f"expected one JSON, got {outputs}"
        payload = json.loads(outputs[0].read_text())
        assert payload["benchmark"] == "transport_latency"
        assert payload["unit"] == "ms"
        assert payload["n_runs"] == 2
        assert payload["payload_sizes_b"] == list(_PAYLOAD_SIZES_B)

        local = payload["transports"]["LocalTransport"]
        assert local["status"] == _STATUS_MEASURED
        assert local["iterations_per_run"] == 20
        assert "by_size" in local
        assert set(local["by_size"].keys()) == {
            str(s) for s in _PAYLOAD_SIZES_B
        }

        for name in ("MQTTTransport", "NATSTransport", "RedisTransport"):
            entry = payload["transports"][name]
            assert entry["status"] == _STATUS_SKIPPED
            assert "ARACHNITE_TEST_" in entry["note"]


# ── Smoke: signal kind constant is stable ───────────────────────────────────


class TestSignalKindConstant:
    def test_bench_signal_kind_is_distinct(self) -> None:
        """The benchmark uses its own kind to avoid colliding with framework
        traffic; this guards against accidental rename."""
        assert _BENCH_SIGNAL_KIND == "transport_bench"
