"""
baselines/jason/run_jason.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Python harness for the Jason BDI baseline. Spawns a JVM running JasonBench,
parses the JSONL latency stream emitted by RobotArmEnv, and returns a
BenchmarkResult shaped exactly like the py_trees baseline so compare.py can
treat all baselines uniformly.

Defines availability via ``jason_available()`` so callers can skip cleanly
when the JVM, Jason, or the compiled .class files are missing.

Tick-equivalent definition for Jason:
    Jason has no fixed tick rate. Each reasoning cycle in the basic
    centralised infrastructure produces at most one external action. We
    measure wall-clock between two consecutive ``executeAction`` calls in
    the env. Over a long run this approximates per-cycle latency for the
    cycles that produce work — cycles that don't fire any plan are
    invisible (Jason elides them), but those are not the cycles a fair
    comparison should count.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from baselines.shared_sim import BenchmarkResult


_HERE = Path(__file__).resolve().parent
_BUILD_DIR = _HERE / "build"


def _jason_jars() -> str | None:
    """Return classpath fragment matching libs/* under JASON_HOME, or None."""
    home = os.environ.get("JASON_HOME")
    if not home:
        return None
    libs = Path(home) / "libs"
    if not (libs / "jason.jar").exists():
        return None
    sep = ";" if os.name == "nt" else ":"
    return sep.join(str(p) for p in libs.glob("*.jar"))


def jason_available() -> tuple[bool, str]:
    """Return (ok, reason) — reason is empty on success, explanation on miss."""
    if shutil.which("java") is None:
        return False, "java not on PATH"
    jars = _jason_jars()
    if jars is None:
        return False, "JASON_HOME unset or libs/jason.jar missing"
    if not (_BUILD_DIR / "JasonBench.class").exists():
        return False, "baselines/jason/build/ not compiled (run build.sh / build.ps1)"
    return True, ""


def run_benchmark(
    n_ticks: int = 10_000,
    warmup: int = 1_000,
    inject_collision_at: int | None = None,
    timeout_s: float = 600.0,
) -> BenchmarkResult:
    """Run the Jason BDI baseline. Raises RuntimeError on misconfiguration."""
    from baselines.shared_sim import BenchmarkResult

    ok, why = jason_available()
    if not ok:
        raise RuntimeError(f"Jason baseline not available: {why}")

    jars = _jason_jars()
    assert jars is not None  # narrowed by jason_available()
    sep = ";" if os.name == "nt" else ":"
    classpath = f"{_BUILD_DIR}{sep}{jars}"

    out_file = Path(tempfile.mkstemp(prefix="jason_lat_", suffix=".jsonl")[1])
    try:
        cmd = [
            "java",
            "-Djava.awt.headless=true",
            "-cp", classpath,
            "JasonBench",
            "--out", str(out_file),
            "--cycles", str(n_ticks),
            "--warmup", str(warmup),
            "--asl-dir", str(_HERE),
            "--asl", "robot_arm.asl",
        ]
        if inject_collision_at is not None:
            cmd += ["--inject", str(inject_collision_at)]

        completed = subprocess.run(
            cmd,
            capture_output=True,
            timeout=timeout_s,
            check=False,
            cwd=str(_HERE),
        )

        # JasonBench's RobotArmEnv calls System.exit(0) when it has
        # collected enough cycles; any other exit code is a real failure.
        if completed.returncode != 0:
            raise RuntimeError(
                f"JasonBench exited {completed.returncode}\n"
                f"stderr (last 1k chars):\n{completed.stderr.decode(errors='replace')[-1000:]}"
            )

        result = BenchmarkResult(framework="Jason")
        with out_file.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                # ``RobotArmEnv`` writes a final ``{"summary": true, ...}``
                # record carrying the live ArmState counters before the JVM
                # exits, since the parent has no other channel to read them
                # back. Per-cycle records have ``latency_ms`` instead.
                if rec.get("summary"):
                    result.picks_completed = int(rec.get("pick_count", 0))
                    result.emergencies_handled = int(rec.get("emergency_count", 0))
                    # pick_durations_ms is missing from older RobotArmEnv
                    # builds (pre-2026-05-09); leave the list empty in that
                    # case so the parent harness reports per_pick_n=0
                    # rather than crashing.
                    result.pick_durations_ms = [
                        float(x) for x in rec.get("pick_durations_ms", [])
                    ]
                    continue
                result.tick_latencies_ms.append(float(rec["latency_ms"]))

        if not result.tick_latencies_ms:
            raise RuntimeError(
                "JasonBench produced no latency records; check the agent or env"
            )

        result.total_ticks = len(result.tick_latencies_ms)

        try:
            import psutil
            # JVM RSS at the moment the parent process woke up — measured
            # in the parent's own context; for cross-framework parity we
            # report the same metric as py_trees (parent RSS at run-end).
            result.memory_rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
        except ImportError:
            result.memory_rss_mb = 0.0

        return result
    finally:
        with contextlib.suppress(OSError):
            out_file.unlink()


if __name__ == "__main__":
    import statistics

    ok, why = jason_available()
    if not ok:
        print(f"Jason not available: {why}", file=sys.stderr)
        sys.exit(2)

    print("Jason BDI pick-and-place benchmark")
    print("-" * 50)
    r = run_benchmark(n_ticks=10_000, warmup=1_000)
    s = sorted(r.tick_latencies_ms)
    n = len(s)
    print("Framework  : Jason")
    print(f"Cycles     : {r.total_ticks}")
    print("Cycle latency (ms):")
    print(f"  Mean    : {statistics.mean(s):7.3f}")
    print(f"  Median  : {statistics.median(s):7.3f}")
    print(f"  P95     : {s[int(n * 0.95)]:7.3f}")
    print(f"  P99     : {s[int(n * 0.99)]:7.3f}")
    print(f"  Std Dev : {statistics.stdev(s):7.3f}")
    print(f"Memory     : {r.memory_rss_mb:.1f} MB (parent process RSS)")
