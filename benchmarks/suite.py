"""
benchmarks/suite.py
~~~~~~~~~~~~~~~~~~~
Unified benchmark suite for Arachnite.

Collects device information, runs all benchmarks, and writes a single
JSON report file with everything.

Run:
    python benchmarks/suite.py
    python benchmarks/suite.py --runs 5           # quick run (5 instead of 30)
    python benchmarks/suite.py --skip memory      # skip specific benchmarks
    python benchmarks/suite.py --only tick reflex  # run only specific ones
    python benchmarks/suite.py -o results/         # custom output directory

Output:
    A single JSON file:
        benchmarks/results/suite_<DEVICE>_py<PYVER>_<TIMESTAMP>.json
    where ``<DEVICE>`` is ``platform.node()`` (the operator's hostname),
    ``<PYVER>`` is ``platform.python_version()`` (e.g. ``3.12.4``), and
    ``<TIMESTAMP>`` is ``YYYYMMDD_HHMMSS``. The hostname + Python tag
    keeps results from different machines / interpreters from
    overwriting each other when the suite is re-run.
    The report carries a top-level ``suite_version`` field (see below) so
    consumers can tell v1 results apart from v2 results.

Suite version history
---------------------
v2.7 (2026-05-06) — current
    Adds the ``active_inference`` benchmark to the registry. The
    standalone driver
    (``benchmarks/active_inference_comparison.py``) was previously only
    invocable directly, so suite-mode runs silently omitted the
    decision-strategy comparison from the consolidated report. The
    suite invokes ``run_async`` with the
    ``_QUICK_TICKS`` / ``_QUICK_WARMUP`` defaults exported by the
    driver (500 ticks, 100 warmup) so a full suite run remains
    reasonable — at default ``--runs 30`` this adds
    4 strategies × 2 workloads × 30 × 500 = 120k ticks of work,
    comparable to ``scalability_extended``. Operators wanting full-N
    runs (publication-grade decision-quality numbers) should keep invoking
    ``python -m benchmarks.active_inference_comparison`` directly with
    ``--ticks 2000 --warmup 200``. Schema gains one optional top-level
    benchmark entry (``benchmarks.active_inference_comparison``); no
    existing field changes.

v2.6 (2026-05-05)
    Fixes ``memory_footprint``. Both the standalone driver
    (``benchmarks/memory_footprint.py``) and the ``run_memory_footprint``
    cell in this file used to take their N readings inside a single
    parent process: re-call ``_rss_mb()`` for the baseline, then
    rebuild a fresh ``ArachniteRuntime`` per reading for ``minimal`` /
    ``robot_arm``. Two bugs fell out of that:
    (a) **zero-width CI** — back-to-back ``_rss_mb()`` calls on a
        stable heap return the same scalar, so std_dev collapsed to 0
        and the bootstrap CI to a point. Symptom in v2.5 outputs:
        ``configs.*.std_dev = 0.0`` and ``ci_lower == ci_upper`` for
        every run.
    (b) **contaminated baseline** — when the suite ran
        ``memory_footprint`` after ``tick_latency`` /
        ``stage_breakdown`` / ``reflex_latency``, the parent's
        baseline included un-GC'd churn from those benchmarks, so
        ``RSS(baseline) > RSS(minimal)``. Symptom in v2.5 outputs:
        baseline ≈ 81–92 MB on a Linux VM with minimal ≈ baseline,
        instead of the expected baseline << minimal ordering.
    Fix: each measurement now runs in a fresh subprocess
    (``--measure-once <config>``). N readings → N independent Python
    startups, so std_dev reflects real run-to-run variance and the
    baseline is unaffected by which suite benchmark ran before it.
    The schema is unchanged — only the values inside
    ``benchmarks.memory_footprint.configs.*`` change. Any historical
    ``memory_footprint`` data from v2.0–v2.5 reports needs
    regeneration.

v2.5 (2026-05-04)
    Fixes the ``bus_throughput`` cell of ``scalability_extended``. Both
    ``benchmarks/scalability_extended.py::bench_bus_throughput`` and the
    inline ``run_scalability_extended`` body in this file defined a
    single ``async def _noop`` once and then called
    ``bus.subscribe("bench", _noop)`` ``n_subs`` times.
    ``SignalBus.subscribe()`` deduplicates by callback identity (see
    ``arachnite/bus.py``: ``_subscriber_set: dict[str, set[Callback]]``),
    so the second-through-Nth registrations were silently dropped — the
    ``1 / 10 / 50 / 100 / 500`` "subscriber count" rows were all
    measuring exactly one subscriber. Symptom in v2.0–v2.4 outputs: all
    five rows reported essentially identical throughput
    (~108k–112k sig/s on a typical desktop) instead of the expected
    inverse curve. Fix: build distinct closures per registration via a
    small ``_make_noop()`` factory in both code paths. After the fix the
    series shows the expected ~120× degradation from 1→500 subscribers
    (~110k sig/s → ~1k sig/s on the reference machine). The schema is
    unchanged — only the values inside
    ``benchmarks.scalability_extended.results.bus_throughput.*`` change.
    Any historical ``bus_throughput`` data from v2.0–v2.4 reports
    needs regeneration.

v2.4 (2026-04-16)
    Bench-2 addition. Adds the ``transport_latency`` benchmark — measures
    publish-to-deliver latency for ``LocalTransport`` (always available)
    and ``MQTTTransport`` / ``NATSTransport`` / ``RedisTransport`` (each
    gated by its own env var: ``ARACHNITE_TEST_MQTT_URL`` /
    ``_NATS_URL`` / ``_REDIS_URL``). Per ADR 0004, broker transports
    skip silently when their env var is unset (``status: "skipped"`` in
    the JSON) and fail loudly when the var is set but the optional dep
    is missing or the broker is unreachable. The suite invokes the
    ``--quick`` preset (Local 5_000 iterations, brokers 200) so a full
    suite run remains sub-minute on Local-only hosts. The schema gains
    one optional top-level benchmark entry
    (``benchmarks.transport_latency``) with per-(transport, payload-size)
    cells; payload sweep is 8 B / 1 KB / 64 KB.

v2.3 (2026-04-16)
    Bench-5 addition. Adds the ``soak_test`` benchmark — a long-horizon
    stability probe (default 1,000,000 ticks, 100,000-tick buckets)
    reporting per-bucket mean / P99 latency, end-of-bucket RSS, and a
    single-line drift verdict (RSS growth vs. P99 drift thresholds).
    Supports stability claims directly rather than by inference from
    short runs. Because the full 1M-tick default
    takes several minutes, the suite runner invokes it via the
    ``--quick`` preset (10,000 ticks, 1,000-tick buckets, 500-tick
    warmup); operators running the full soak should invoke
    ``python benchmarks/soak_test.py`` directly. The schema gains one
    optional top-level benchmark entry (``benchmarks.soak_test``); no
    existing field changes.

v2.2 (2026-04-16)
    Bench-1 addition. Adds the ``multistep_action_latency`` benchmark
    exercising ``MultiStepActionNode``'s four interrupt policies
    (ALWAYS / CHECKPOINT / ROLLBACK / mandatory-block) plus an
    ``emergency_stop``-equivalent probe. Directly supports the
    ``T_worst_mandatory`` formal bound. The schema gains one optional
    top-level benchmark entry
    (``benchmarks.multistep_action_latency``); no existing field changes.

v2.1 (2026-04-16)
    Bench-3 addition (tick-stage instrumentation).
    Adds the ``stage_breakdown`` benchmark under the new
    ``TickInstrumenter`` protocol hook in ``arachnite.runtime``, slicing
    the tick latency into the six pipeline stages defined by
    ``TICK_STAGE_NAMES`` (``sense``, ``context``, ``reflex``,
    ``instinct``, ``decide``, ``act``). The schema gains one optional
    top-level benchmark entry (``benchmarks.stage_breakdown``); no
    existing field changes.

v2.0 (2026-04-16)
    Bug B fix: all benchmark drivers now bypass the runtime's background tick loop:
    instead of ``await rt.start()`` / ``await rt.stop()``, they call
    ``setup()`` / ``teardown()`` on the four masters directly and drive
    the loop with manual ``await rt.tick()`` calls.

    Why it matters: under v1 the background loop was running in parallel
    with the manual ``rt.tick()`` calls used by the driver, which caused
    (a) double tick counting, (b) scheduling jitter from the contending
    coroutines, and (c) a flood of "Tick overrun" warnings that drowned
    benchmark output. Numbers from v1 reports are systematically biased
    high (latency) and noisy (variance) compared to v2 — they should not
    be combined or directly compared with v2 numbers in tables.

    Affected drivers: tick_latency, reflex_latency, memory_footprint,
    scalability_sweep, scalability_extended (concurrent-action +
    history-depth inline configs in this file).

v1.0 — initial release
    Used ``await rt.start()`` / ``await rt.stop()`` around manual
    ``rt.tick()`` calls. Contaminated by the background loop (see above).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import struct
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from benchmarks.stats import (
    DescriptiveStats,
    bootstrap_ci,
    format_stats_table,
    percentile,
)

# ── Device information ───────────────────────────────────────────────────────


def collect_device_info() -> dict[str, Any]:
    """Gather hardware and software information about the current device."""
    info: dict[str, Any] = {
        "hostname": platform.node(),
        "platform": sys.platform,
        "os": platform.platform(),
        "os_version": platform.version(),
        "architecture": platform.machine(),
        "pointer_size_bits": struct.calcsize("P") * 8,
        "cpu": _get_cpu_name(),
        "cpu_cores_physical": _get_physical_cores(),
        "cpu_cores_logical": os.cpu_count() or 0,
        "ram_total_gb": round(_get_total_ram_gb(), 1),
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_compiler": platform.python_compiler(),
    }

    # Arachnite version
    try:
        import arachnite
        info["arachnite_version"] = arachnite.__version__
    except Exception:
        info["arachnite_version"] = "unknown"

    return info


def _get_cpu_name() -> str:
    """Best-effort CPU model name."""
    # Windows: registry gives the best name (e.g. "AMD Ryzen 9 7950X 16-Core Processor")
    if sys.platform == "win32":
        try:
            import winreg
            key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"HARDWARE\DESCRIPTION\System\CentralProcessor\0",
            )
            name, _ = winreg.QueryValueEx(key, "ProcessorNameString")
            winreg.CloseKey(key)
            if name and str(name).strip():
                return str(name).strip()
        except Exception:
            pass
        try:
            import subprocess
            result = subprocess.run(
                ["wmic", "cpu", "get", "Name", "/value"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.splitlines():
                if line.startswith("Name="):
                    return line.split("=", 1)[1].strip()
        except Exception:
            pass

    # Linux: /proc/cpuinfo
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except FileNotFoundError:
        pass

    # Try platform.processor() as fallback
    proc = platform.processor()
    if proc and proc.strip() and proc.strip() not in ("", "x86_64", "AMD64", "aarch64"):
        return proc.strip()

    # macOS: sysctl
    if sys.platform == "darwin":
        try:
            import subprocess
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True, timeout=5,
            )
            if result.stdout.strip():
                return result.stdout.strip()
        except Exception:
            pass

    return platform.processor() or "unknown"


def _get_physical_cores() -> int:
    """Physical CPU core count."""
    try:
        import psutil
        return psutil.cpu_count(logical=False) or 0
    except ImportError:
        pass
    # Fallback: assume logical = physical (no hyperthreading info)
    return os.cpu_count() or 0


def _get_total_ram_gb() -> float:
    """Total system RAM in GB."""
    try:
        import psutil
        return float(psutil.virtual_memory().total) / (1024 ** 3)
    except ImportError:
        pass

    # Linux fallback
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return kb / (1024 * 1024)
    except FileNotFoundError:
        pass

    # Windows fallback
    if sys.platform == "win32":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            c_ulonglong = ctypes.c_ulonglong

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", c_ulonglong),
                    ("ullAvailPhys", c_ulonglong),
                    ("ullTotalPageFile", c_ulonglong),
                    ("ullAvailPageFile", c_ulonglong),
                    ("ullTotalVirtual", c_ulonglong),
                    ("ullAvailVirtual", c_ulonglong),
                    ("ullAvailExtendedVirtual", c_ulonglong),
                ]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(stat)
            kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return float(stat.ullTotalPhys) / (1024 ** 3)
        except Exception:
            pass

    return 0.0


def print_device_info(info: dict[str, Any]) -> None:
    """Print device info to console."""
    print(f"  Hostname:     {info['hostname']}")
    print(f"  OS:           {info['os']}")
    print(f"  CPU:          {info['cpu']}")
    print(f"  Cores:        {info['cpu_cores_physical']} physical, "
          f"{info['cpu_cores_logical']} logical")
    print(f"  RAM:          {info['ram_total_gb']} GB")
    print(f"  Python:       {info['python_implementation']} {info['python_version']}")
    print(f"  Arachnite:    {info.get('arachnite_version', '?')}")


# ── Benchmark runners ────────────────────────────────────────────────────────
# Each returns a dict with results. They reuse the existing benchmark modules.


async def run_tick_latency(n_runs: int) -> dict[str, Any]:
    """Tick latency benchmark (§8.2.1)."""
    from benchmarks.tick_latency import _TICKS
    from benchmarks.tick_latency import run as single_run

    run_medians: list[float] = []
    all_samples: list[float] = []
    run_samples: list[list[float]] = []
    raw_runs: list[dict[str, Any]] = []

    for i in range(n_runs):
        samples = await single_run()
        med = statistics.median(samples)
        run_medians.append(med)
        run_samples.append(samples)
        all_samples.extend(samples)
        raw_runs.append({
            "run": i + 1,
            "median": round(med, 4),
            "mean": round(statistics.mean(samples), 4),
            "p99": round(percentile(samples, 99.0), 4),
        })
        print(f"    Run {i + 1:3d}/{n_runs}: median = {med:.3f} ms")

    stats = DescriptiveStats.from_runs(run_medians, all_samples, _TICKS, run_samples=run_samples)
    print(format_stats_table("Tick latency", stats, "ms"))

    return {
        "name": "tick_latency",
        "unit": "ms",
        "protocol": "3 sense, 1 instinct, 1 reflex, 1 weighted decision, 1 action; "
                    "1000 warmup, 10000 measurement ticks per run",
        "n_runs": n_runs,
        "samples_per_run": _TICKS,
        "stats": asdict(stats),
        "runs": raw_runs,
    }


async def run_stage_breakdown(n_runs: int) -> dict[str, Any]:
    """Per-stage tick latency breakdown (§8.2.1, ADR 0002)."""
    from arachnite.runtime import TICK_STAGE_NAMES
    from benchmarks.stage_breakdown import _TICKS, _WARMUP
    from benchmarks.stage_breakdown import run as single_run

    run_samples_by_stage: dict[str, list[list[float]]] = {
        name: [] for name in TICK_STAGE_NAMES
    }
    run_medians_by_stage: dict[str, list[float]] = {
        name: [] for name in TICK_STAGE_NAMES
    }
    pooled_by_stage: dict[str, list[float]] = {
        name: [] for name in TICK_STAGE_NAMES
    }

    for i in range(n_runs):
        samples_dict = await single_run(ticks=_TICKS, warmup=_WARMUP)
        medians: dict[str, float] = {}
        for name in TICK_STAGE_NAMES:
            stage_samples = samples_dict[name]
            run_samples_by_stage[name].append(stage_samples)
            m = statistics.median(stage_samples)
            run_medians_by_stage[name].append(m)
            pooled_by_stage[name].extend(stage_samples)
            medians[name] = m
        line = f"    Run {i + 1:3d}/{n_runs}: " + "  ".join(
            f"{name}={medians[name]:.3f}" for name in TICK_STAGE_NAMES
        )
        print(line)

    stages_out: dict[str, dict[str, Any]] = {}
    for name in TICK_STAGE_NAMES:
        stats = DescriptiveStats.from_runs(
            run_medians_by_stage[name],
            pooled_by_stage[name],
            _TICKS,
            run_samples=run_samples_by_stage[name],
        )
        stages_out[name] = asdict(stats)
        print(format_stats_table(f"stage:{name}", stats, "ms"))

    return {
        "name": "stage_breakdown",
        "unit": "ms",
        "protocol": (
            "same node topology as tick_latency; durations measured at the "
            "six stage boundaries via TickInstrumenter (ADR 0002)"
        ),
        "n_runs": n_runs,
        "samples_per_run": _TICKS,
        "stages": stages_out,
    }


async def run_multistep_action_latency(n_runs: int) -> dict[str, Any]:
    """Multi-step action interrupt / rollback / mandatory-block latency (§8.2.7)."""
    from benchmarks.multistep_action_latency import (
        _ITERATIONS,
        SCENARIOS,
    )
    from benchmarks.multistep_action_latency import (
        run as single_run,
    )

    all_names = list(SCENARIOS) + ["rollback_policy_per_step"]
    run_samples_by_name: dict[str, list[list[float]]] = {n: [] for n in all_names}
    run_medians_by_name: dict[str, list[float]] = {n: [] for n in all_names}
    pooled_by_name: dict[str, list[float]] = {n: [] for n in all_names}

    for i in range(n_runs):
        samples_dict = await single_run(iterations=_ITERATIONS)
        for name in all_names:
            samples = samples_dict.get(name, [])
            if not samples:
                continue
            run_samples_by_name[name].append(samples)
            run_medians_by_name[name].append(statistics.median(samples))
            pooled_by_name[name].extend(samples)
        medians = {
            n: statistics.median(samples_dict[n])
            for n in SCENARIOS if samples_dict.get(n)
        }
        line = f"    Run {i + 1:3d}/{n_runs}: " + "  ".join(
            f"{n.split('_')[0]}={medians[n]:.3f}" for n in SCENARIOS if n in medians
        )
        print(line)

    scenarios_out: dict[str, dict[str, Any]] = {}
    for name in all_names:
        if not pooled_by_name[name]:
            continue
        stats = DescriptiveStats.from_runs(
            run_medians_by_name[name],
            pooled_by_name[name],
            _ITERATIONS,
            run_samples=run_samples_by_name[name],
        )
        scenarios_out[name] = asdict(stats)
        print(format_stats_table(f"scenario:{name}", stats, "ms"))

    return {
        "name": "multistep_action_latency",
        "unit": "ms",
        "protocol": (
            "MultiStepActionNode with 5-step ALWAYS / 5-step CHECKPOINT / "
            "5-step ROLLBACK / 3-step mandatory-block actions; "
            "interrupt fired mid-execution via request_interrupt() or "
            "emergency_stop()-equivalent; wall-clock from interrupt to "
            "execute() return"
        ),
        "n_runs": n_runs,
        "iterations_per_run": _ITERATIONS,
        "scenarios": scenarios_out,
    }


async def run_soak_test(n_runs: int) -> dict[str, Any]:
    """Soak / stability benchmark (§8.2.8, Bench-5).

    The suite runner always uses the ``--quick`` preset (10,000 ticks,
    1,000-tick buckets, 500-tick warmup) so a full suite invocation
    remains sub-minute. The full 1M-tick soak is operator-driven and
    run via ``python benchmarks/soak_test.py`` directly — see the
    suite-version-history notes in this module's docstring for the
    reasoning.

    ``n_runs`` is currently ignored (a soak is a single long run, not a
    statistical sample); the parameter is kept for signature symmetry
    with the other runners in this registry.
    """
    from benchmarks.soak_test import (
        _QUICK_BUCKET_SIZE,
        _QUICK_TICKS,
        _QUICK_WARMUP,
        compute_drift,
    )
    from benchmarks.soak_test import (
        run as single_run,
    )

    del n_runs  # see docstring

    buckets = await single_run(
        ticks=_QUICK_TICKS,
        bucket_size=_QUICK_BUCKET_SIZE,
        warmup=_QUICK_WARMUP,
    )
    drift = compute_drift(buckets)

    for b in buckets:
        print(
            f"    bucket {b.bucket_index:>3d} "
            f"[{b.tick_start:>6d}-{b.tick_end:>6d}]: "
            f"mean={b.mean_ms:.4f} ms  P99={b.p99_ms:.4f} ms  "
            f"RSS={b.rss_mb:.3f} MB"
        )
    print(f"    {drift.verdict}")

    return {
        "name": "soak_test",
        "unit": "ms",
        "protocol": (
            "minimal rig (1 sense, 1 instinct, 1 action); quick-preset "
            "soak (10,000 ticks, 1,000-tick buckets, 500-tick warmup) "
            "invoked via the suite runner. Full 1M-tick soak is "
            "operator-driven via `python benchmarks/soak_test.py`."
        ),
        "mode": "quick",
        "ticks": _QUICK_TICKS,
        "bucket_size": _QUICK_BUCKET_SIZE,
        "warmup": _QUICK_WARMUP,
        "buckets": [asdict(b) for b in buckets],
        "drift": asdict(drift),
    }


async def run_reflex_latency(n_runs: int) -> dict[str, Any]:
    """Reflex latency benchmark (§8.2.2)."""
    from benchmarks.reflex_latency import _TRIALS
    from benchmarks.reflex_latency import run as single_run

    run_medians: list[float] = []
    all_samples: list[float] = []
    run_samples: list[list[float]] = []
    raw_runs: list[dict[str, Any]] = []

    for i in range(n_runs):
        samples = await single_run()
        med = statistics.median(samples)
        run_medians.append(med)
        run_samples.append(samples)
        all_samples.extend(samples)
        raw_runs.append({
            "run": i + 1,
            "median": round(med, 2),
            "mean": round(statistics.mean(samples), 2),
            "p99": round(percentile(samples, 99.0), 2),
        })
        print(f"    Run {i + 1:3d}/{n_runs}: median = {med:.1f} us")

    stats = DescriptiveStats.from_runs(run_medians, all_samples, _TRIALS, run_samples=run_samples)
    print(format_stats_table("Reflex latency", stats, "us"))

    return {
        "name": "reflex_latency",
        "unit": "us",
        "protocol": "1 sense, 1 reflex instinct, 1 action; "
                    "1000 trials per run",
        "n_runs": n_runs,
        "samples_per_run": _TRIALS,
        "stats": asdict(stats),
        "runs": raw_runs,
    }


async def run_memory_footprint(n_runs: int) -> dict[str, Any]:
    """Memory footprint benchmark (§8.2.3).

    Each measurement is taken in a fresh subprocess (see
    ``benchmarks/memory_footprint.py`` module docstring). This
    (a) gives genuine sample-to-sample independence — repeated
    in-process ``_rss_mb()`` calls on a stable heap return identical
    scalars and collapse the bootstrap CI to zero width — and
    (b) makes the baseline honest: prior suite benchmarks
    (tick_latency / stage_breakdown / reflex_latency) leave
    allocator churn in the parent process that an in-process baseline
    would inherit, inflating RSS(baseline) above RSS(minimal).
    """
    from benchmarks.memory_footprint import (
        RobotArmUnavailable,
        measure_n_subprocess,
    )

    def _summarise(readings: list[float], description: str) -> dict[str, Any]:
        ci_lo, ci_hi = bootstrap_ci(readings)
        return {
            "description": description,
            "mean_mb": round(statistics.mean(readings), 3),
            "median_mb": round(statistics.median(readings), 3),
            "std_dev": round(
                statistics.stdev(readings) if len(readings) > 1 else 0.0, 3,
            ),
            "ci_lower": round(ci_lo, 3),
            "ci_upper": round(ci_hi, 3),
            "readings": [round(r, 3) for r in readings],
        }

    # subprocess.run is blocking; offload to a worker thread so the
    # asyncio event loop running the suite stays responsive.
    def _measure_blocking(config: str) -> list[float]:
        return measure_n_subprocess(config, n_runs, quiet=False)

    configs: dict[str, dict[str, Any]] = {}

    print("    baseline (subprocess-per-measurement)...")
    baseline = await asyncio.to_thread(_measure_blocking, "baseline")
    configs["baseline"] = _summarise(
        baseline, "import only (Python interpreter overhead)",
    )

    print("    minimal (subprocess-per-measurement)...")
    minimal = await asyncio.to_thread(_measure_blocking, "minimal")
    configs["minimal"] = _summarise(
        minimal, "1 sense, 1 instinct, 1 action (100 ticks steady state)",
    )

    print("    robot_arm (subprocess-per-measurement)...")
    try:
        robot_arm = await asyncio.to_thread(_measure_blocking, "robot_arm")
        configs["robot_arm"] = _summarise(
            robot_arm,
            "3 sense, 2 instinct, 2 action (robot arm case study)",
        )
    except RobotArmUnavailable:
        print("    (robot_arm nodes not available, skipping)")

    return {
        "name": "memory_footprint",
        "unit": "MB",
        "protocol": "RSS measured via psutil after 100 ticks steady state; "
                    "each sample taken in a fresh subprocess",
        "n_runs": n_runs,
        "configs": configs,
    }


async def run_scalability_sweep(n_runs: int) -> dict[str, Any]:
    """Node-count scalability benchmark (§8.2.4)."""
    from benchmarks.scalability_sweep import _TICKS, run_config

    configs_list = [
        (1, "3 nodes"), (3, "9 nodes"), (5, "15 nodes"),
        (10, "30 nodes"), (25, "75 nodes"), (50, "150 nodes"),
    ]
    configs: dict[str, dict[str, Any]] = {}

    for n_per_type, label in configs_list:
        print(f"    Config: {label}")
        run_medians: list[float] = []
        all_samples: list[float] = []
        run_samples: list[list[float]] = []
        for i in range(n_runs):
            samples = await run_config(n_per_type)
            med = statistics.median(samples)
            run_medians.append(med)
            run_samples.append(samples)
            all_samples.extend(samples)
            print(f"      Run {i + 1:3d}/{n_runs}: median = {med:.3f} ms")

        stats = DescriptiveStats.from_runs(
            run_medians, all_samples, _TICKS, run_samples=run_samples
        )
        configs[label] = asdict(stats)
        print(format_stats_table(label, stats, "ms"))

    return {
        "name": "scalability_sweep",
        "unit": "ms",
        "protocol": "N sense + N instinct + N action; 1000 warmup, "
                    "5000 measurement ticks per run",
        "n_runs": n_runs,
        "configs": configs,
    }


async def run_scalability_extended(n_runs: int) -> dict[str, Any]:
    """Extended scalability benchmarks (§8.2.5)."""
    results: dict[str, Any] = {}

    # 1. SignalBus throughput
    print("    SignalBus throughput:")
    from arachnite import Signal, SignalBus
    bus_results: dict[str, dict[str, Any]] = {}

    from collections.abc import Awaitable, Callable

    def _make_noop() -> Callable[[Signal], Awaitable[None]]:
        # Distinct closure per call so SignalBus.subscribe (identity-based
        # dedup) registers each as a separate subscriber.
        async def _noop(sig: Signal) -> None:
            pass
        return _noop

    for n_subs in [1, 10, 50, 100, 500]:
        throughputs: list[float] = []
        latencies: list[float] = []
        for _ in range(n_runs):
            bus = SignalBus()

            for _ in range(n_subs):
                bus.subscribe("bench", _make_noop())

            n_signals = 10_000
            t0 = time.perf_counter()
            for j in range(n_signals):
                await bus.publish(Signal(
                    source="bench", kind="bench", value=float(j),
                    confidence=1.0, timestamp=time.monotonic(),
                ))
            elapsed = time.perf_counter() - t0
            throughputs.append(n_signals / elapsed)
            latencies.append((elapsed / n_signals) * 1_000_000)

        bus_results[f"{n_subs}_subscribers"] = {
            "throughput_mean": round(statistics.mean(throughputs), 0),
            "throughput_unit": "signals/s",
            "per_publish_us_mean": round(statistics.mean(latencies), 1),
        }
        print(f"      {n_subs:>4} subs: {statistics.mean(throughputs):,.0f} sig/s, "
              f"{statistics.mean(latencies):.1f} us/pub")

    results["bus_throughput"] = bus_results

    # 2. Concurrent action dispatch
    print("    Concurrent action dispatch:")
    from arachnite import (
        ActionMasterNode,
        ArachniteRuntime,
        ContextNode,
        DecisionMasterNode,
        InstinctMasterNode,
        SenseMasterNode,
        WeightedDecisionNode,
    )
    from benchmarks.scalability_extended import (
        _make_action,
        _make_instinct,
        _make_sense,
    )

    action_results: dict[str, dict[str, Any]] = {}
    for n_actions in [1, 5, 10, 25, 50]:
        run_medians_a: list[float] = []
        all_samples_a: list[float] = []
        run_samples_a: list[list[float]] = []
        for _r in range(n_runs):
            bus = SignalBus()
            sm = SenseMasterNode(bus=bus)
            im = InstinctMasterNode(bus=bus)
            dm = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
            am = ActionMasterNode(bus=bus)
            sm.register(_make_sense(0)(bus=bus))
            for k in range(n_actions):
                im.register(_make_instinct(k, f"ScaleAction_{k}")(bus=bus))
                am.register(_make_action(k)(bus=bus))
            rt = ArachniteRuntime(
                sense_master=sm, context=ContextNode(),
                instinct_master=im, decision_master=dm,
                action_master=am, bus=bus, tick_rate_hz=10_000.0,
            )
            # Bypass background tick loop (Bug B — see audit 2026-04-16).
            for m in (sm, im, dm, am):
                await m.setup()
            for _ in range(500):
                await rt.tick()
            samples: list[float] = []
            for _ in range(2_000):
                t0 = time.perf_counter()
                await rt.tick()
                samples.append((time.perf_counter() - t0) * 1_000)
            for m in (am, dm, im, sm):
                await m.teardown()
            run_medians_a.append(statistics.median(samples))
            run_samples_a.append(samples)
            all_samples_a.extend(samples)

        stats = DescriptiveStats.from_runs(
            run_medians_a, all_samples_a, 2_000, run_samples=run_samples_a
        )
        action_results[f"{n_actions}_actions"] = asdict(stats)
        print(f"      {n_actions:>3} actions: median = {stats.median:.3f} ms, "
              f"P99 = {stats.p99:.3f} ms")

    results["concurrent_actions"] = action_results

    # 3. Context history depth
    print("    Context history depth:")
    history_results: dict[str, dict[str, Any]] = {}
    for depth in [1, 10, 50, 100, 500]:
        run_medians_h: list[float] = []
        all_samples_h: list[float] = []
        run_samples_h: list[list[float]] = []
        for _r in range(n_runs):
            bus = SignalBus()
            sm = SenseMasterNode(bus=bus)
            im = InstinctMasterNode(bus=bus)
            dm = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
            am = ActionMasterNode(bus=bus)
            sm.register(_make_sense(0)(bus=bus))
            im.register(_make_instinct(0)(bus=bus))
            am.register(_make_action(0)(bus=bus))
            rt = ArachniteRuntime(
                sense_master=sm, context=ContextNode(history_length=depth),
                instinct_master=im, decision_master=dm,
                action_master=am, bus=bus, tick_rate_hz=10_000.0,
            )
            # Bypass background tick loop (Bug B — see audit 2026-04-16).
            for m in (sm, im, dm, am):
                await m.setup()
            for _ in range(depth + 100):
                await rt.tick()
            samples = []
            for _ in range(2_000):
                t0 = time.perf_counter()
                await rt.tick()
                samples.append((time.perf_counter() - t0) * 1_000)
            for m in (am, dm, im, sm):
                await m.teardown()
            run_medians_h.append(statistics.median(samples))
            run_samples_h.append(samples)
            all_samples_h.extend(samples)

        stats = DescriptiveStats.from_runs(
            run_medians_h, all_samples_h, 2_000, run_samples=run_samples_h
        )
        history_results[f"depth_{depth}"] = asdict(stats)
        print(f"      depth {depth:>4}: median = {stats.median:.3f} ms, "
              f"P99 = {stats.p99:.3f} ms")

    results["history_depth"] = history_results

    return {
        "name": "scalability_extended",
        "unit": "mixed",
        "protocol": "Bus: 10k signals per config; Actions/History: "
                    "500 warmup, 2000 measurement ticks per run",
        "n_runs": n_runs,
        "results": results,
    }


async def run_transport_latency(n_runs: int) -> dict[str, Any]:
    """Transport publish-to-deliver latency benchmark (§8.2.9, Bench-2).

    Always invokes the ``--quick`` preset (Local 5,000 iterations,
    brokers 200) so a full suite run remains sub-minute on Local-only
    hosts. Operators wanting full-N runs should invoke
    ``python benchmarks/transport_latency.py`` directly with
    ``--runs N`` and any ``ARACHNITE_TEST_*_URL`` env vars set. Broker
    transports skip silently when their env var is unset (status
    ``"skipped"`` in the result); a misconfigured broker (env var set,
    dep missing, or connect fails) raises and aborts the suite per
    ADR 0004 §1.

    ``n_runs`` is forwarded to the benchmark as the per-cell run count
    (each ``DescriptiveStats`` is computed from this many independent
    runs).
    """
    from benchmarks.transport_latency import (
        _PAYLOAD_SIZES_B,
        _QUICK_ITERATIONS_LOCAL,
        _QUICK_ITERATIONS_MQTT,
        _QUICK_ITERATIONS_NATS,
        _QUICK_ITERATIONS_REDIS,
        _report_to_json,
    )
    from benchmarks.transport_latency import (
        run as single_run,
    )

    reports = await single_run(
        iterations_override=None, n_runs=n_runs, quick=True,
    )
    transports_out = {tr.transport: _report_to_json(tr) for tr in reports}
    for tr in reports:
        if tr.status == "skipped":
            print(f"    {tr.transport}: skipped ({tr.note})")
            continue
        if tr.status == "failed":
            print(f"    {tr.transport}: FAILED ({tr.note})")
            continue
        for cell in tr.cells:
            print(
                f"    {tr.transport} {cell.payload_size_b:>6d} B: "
                f"median={cell.stats.median:.4f} ms  "
                f"P99={cell.stats.p99:.4f} ms"
            )

    return {
        "name": "transport_latency",
        "unit": "ms",
        "protocol": (
            "in-process loopback (single BaseTransport instance acts as "
            "both publisher and subscriber); per-payload sweep "
            "8 B / 1 KB / 64 KB; LocalTransport always runs, broker "
            "transports gated by ARACHNITE_TEST_<MQTT|NATS|REDIS>_URL "
            "(unset → skipped; set but unreachable → loud failure per "
            "ADR 0004 §1). Suite invocation uses --quick: Local "
            f"{_QUICK_ITERATIONS_LOCAL:,} iterations, brokers "
            f"{_QUICK_ITERATIONS_MQTT:,} iterations."
        ),
        "mode": "quick",
        "n_runs": n_runs,
        "payload_sizes_b": list(_PAYLOAD_SIZES_B),
        "quick_iterations": {
            "local": _QUICK_ITERATIONS_LOCAL,
            "mqtt":  _QUICK_ITERATIONS_MQTT,
            "nats":  _QUICK_ITERATIONS_NATS,
            "redis": _QUICK_ITERATIONS_REDIS,
        },
        "transports": transports_out,
    }


async def run_active_inference_comparison(n_runs: int) -> dict[str, Any]:
    """Decision-strategy comparison benchmark.

    Invokes ``benchmarks.active_inference_comparison.run_async`` with the
    driver-exported ``_QUICK_TICKS`` / ``_QUICK_WARMUP`` defaults so a
    full suite run remains tractable. The standalone driver
    (``python -m benchmarks.active_inference_comparison --ticks 2000``)
    is the right entry point for publication-grade decision-quality
    numbers. ``n_runs`` is forwarded verbatim — Wilcoxon comparison
    needs ``n_runs ≥ 10``.
    """
    from benchmarks.active_inference_comparison import (
        _QUICK_TICKS,
        _QUICK_WARMUP,
        run_async,
    )

    result = await run_async(
        n_runs=n_runs, n_ticks=_QUICK_TICKS, warmup=_QUICK_WARMUP,
    )

    # Strip the redundant top-level metadata that the standalone driver
    # writes (``benchmark`` / ``platform`` / ``machine`` / ``args``);
    # the suite report already carries equivalent fields under
    # ``device`` / ``timestamp`` / ``runs_per_benchmark``. Keep the
    # measurement payload (``case_study`` / ``synthetic`` /
    # ``pairwise_vs_weighted``) so the per-benchmark structure is
    # preserved for downstream tooling.
    return {
        "name": "active_inference_comparison",
        "unit": "mixed",  # ms (case study) + us (synthetic)
        "protocol": (
            "Greedy / Weighted / Random / ActiveInference compared on "
            "two workloads — pick-and-place case study (latency in ms) "
            "and synthetic competing-proposal decision layer (latency "
            "in us). Suite invocation uses quick defaults: "
            f"{_QUICK_TICKS} ticks per run, {_QUICK_WARMUP} warmup "
            "ticks. Pairwise statistics use Weighted as the reference."
        ),
        "mode": "quick",
        "n_runs": n_runs,
        "ticks_per_run": _QUICK_TICKS,
        "warmup": _QUICK_WARMUP,
        "case_study": result["case_study"],
        "synthetic":  result["synthetic"],
        "pairwise_vs_weighted": result["pairwise_vs_weighted"],
    }


# ── Registry ────────────────────────────────────────────────────────────────

BENCHMARKS = {
    "tick": ("Tick Latency (§8.2.1)", run_tick_latency),
    "stage_breakdown": ("Stage Breakdown (§8.2.1)", run_stage_breakdown),
    "reflex": ("Reflex Latency (§8.2.2)", run_reflex_latency),
    "memory": ("Memory Footprint (§8.2.3)", run_memory_footprint),
    "scalability": ("Node-Count Scalability (§8.2.4)", run_scalability_sweep),
    "extended": ("Extended Scalability (§8.2.5)", run_scalability_extended),
    "multistep_action_latency": (
        "Multi-Step Action Latency (§8.2.7)",
        run_multistep_action_latency,
    ),
    "soak_test": ("Soak / Stability (§8.2.8, quick mode)", run_soak_test),
    "transport_latency": (
        "Transport Latency (§8.2.9, quick mode)",
        run_transport_latency,
    ),
    "active_inference": (
        "Active Inference Comparison (§9.2, quick mode)",
        run_active_inference_comparison,
    ),
}


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Arachnite benchmark suite — run all benchmarks and produce a single report",
    )
    parser.add_argument(
        "--runs", "-n", type=int, default=30,
        help="Independent runs per benchmark (default: 30)",
    )
    parser.add_argument(
        "--only", nargs="+", choices=list(BENCHMARKS.keys()),
        help="Run only these benchmarks",
    )
    parser.add_argument(
        "--skip", nargs="+", choices=list(BENCHMARKS.keys()),
        help="Skip these benchmarks",
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default="benchmarks/results",
        help="Output directory (default: benchmarks/results)",
    )
    args = parser.parse_args()

    # Determine which benchmarks to run
    targets = args.only or list(BENCHMARKS.keys())
    if args.skip:
        targets = [t for t in targets if t not in args.skip]

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    async def run_suite() -> None:
        suite_start = time.time()

        # Collect device info
        print("=" * 70)
        print("  ARACHNITE BENCHMARK SUITE")
        print("=" * 70)
        print()
        print("  Device Information")
        print("  " + "-" * 40)
        device_info = collect_device_info()
        print_device_info(device_info)
        print()

        # Run benchmarks
        benchmark_results: list[dict[str, Any]] = []
        for key in targets:
            label, runner = BENCHMARKS[key]
            print("-" * 70)
            print(f"  {label}  ({args.runs} runs)")
            print("-" * 70)
            result = await runner(args.runs)
            benchmark_results.append(result)
            print()

        # Assemble report
        elapsed = time.time() - suite_start
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        device_tag = device_info["hostname"].replace(" ", "_")
        py_tag = f"py{device_info['python_version']}"

        report = {
            "suite_version": "2.7",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "elapsed_s": round(elapsed, 1),
            "runs_per_benchmark": args.runs,
            "device": device_info,
            "benchmarks": {r["name"]: r for r in benchmark_results},
        }

        # Write output
        out_file = out_dir / f"suite_{device_tag}_{py_tag}_{timestamp}.json"
        with open(out_file, "w") as f:
            json.dump(report, f, indent=2)

        # Summary
        print("=" * 70)
        print(f"  COMPLETE  ({elapsed:.1f}s elapsed)")
        print(f"  Report:   {out_file}")
        print(f"  Device:   {device_info['cpu']}")
        print(f"  Platform: {device_info['os']}")
        print(f"  Python:   {device_info['python_implementation']} "
              f"{device_info['python_version']}")
        print(f"  Benchmarks: {len(benchmark_results)} run, "
              f"{args.runs} independent runs each")
        print("=" * 70)

    asyncio.run(run_suite())


if __name__ == "__main__":
    main()
