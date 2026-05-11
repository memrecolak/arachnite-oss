"""
benchmarks/memory_footprint.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Measures RSS memory footprint of an Arachnite runtime.

Three configurations measured:

  baseline  Python interpreter + Arachnite imports only (no runtime)
  minimal   1 SenseNode, 1 InstinctNode, 1 ActionNode, LocalTransport
  robot_arm Full robot arm case study (3 sense, 2 instinct, 2 action)

Metric: RSS (Resident Set Size) in MB, read from /proc/self/status on
Linux or psutil on Windows/macOS. Measured after the runtime has started
and completed 100 ticks (steady state), with `gc.collect()` immediately
before the read so all three configurations are sampled at comparable
allocator states.

Sampling strategy
-----------------
Each of the N independent measurements per configuration runs in a
**fresh Python subprocess**. Without this, every in-process reading
after the first collapses to the same scalar (RSS does not change
between back-to-back `_rss_mb()` calls on a stable heap) — std_dev=0
and the bootstrap CI is zero-width. Subprocess invocation gives genuine
sample-to-sample independence: each child has its own Python startup,
allocator state, and page mapping, so std_dev reflects real run-to-run
variance rather than measurement instrument noise.

Subprocess invocation also makes the *baseline* honest. When the suite
runs memory_footprint after tick_latency / stage_breakdown / reflex_latency,
the parent process has already allocated and partially released
benchmark state. An in-process baseline measured at that point is "Python
+ Arachnite imports + leftover churn from prior benchmarks", inflating
RSS(baseline) above RSS(minimal) (which gets a fresh `gc.collect()`).
A subprocess baseline is "fresh Python + Arachnite imports + nothing
else" — independent of prior benchmark history.

Run:
    python benchmarks/memory_footprint.py
    python benchmarks/memory_footprint.py --runs 30
    python benchmarks/memory_footprint.py --runs 5 --output-dir benchmarks/results

    # Internal: single measurement in the current process — used by the
    # parent's subprocess invocations (and handy for manual probing).
    python benchmarks/memory_footprint.py --measure-once baseline
    python benchmarks/memory_footprint.py --measure-once minimal
    python benchmarks/memory_footprint.py --measure-once robot_arm

Output:
    benchmarks/results/memory_footprint_<timestamp>.json

Note: psutil is an optional dependency. Install with:
    pip install psutil
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from arachnite import (
    ActionMasterNode,
    ArachniteRuntime,
    BaseActionNode,
    BaseInstinctNode,
    BaseSenseNode,
    Context,
    ContextNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    InstinctMasterNode,
    Proposal,
    Result,
    SenseMasterNode,
    Signal,
    SignalBus,
)

# Throwaway warmup runs before measurement. With subprocess invocation,
# the first 1-2 children may pay disk-cache cold-start cost on `import
# arachnite` (the .pyc files have to be read from disk). After warmup,
# subsequent children hit warm filesystem cache and produce stable RSS.
_WARMUP_RUNS = 3

# Subprocess timeout per measurement. Even on a slow Pi, one measurement
# is import + 100 ticks + gc + RSS read — well under 30 s. 60 s gives
# headroom for first-cold-start importers.
_SUBPROCESS_TIMEOUT_S = 60.0

_THIS_FILE = Path(__file__).resolve()
_PROJECT_ROOT = _THIS_FILE.parents[1]

# Valid config names accepted by --measure-once and the parent dispatcher.
_VALID_CONFIGS = ("baseline", "minimal", "robot_arm")


# ── RSS helper ────────────────────────────────────────────────────────────────

def _rss_mb() -> float:
    """Return current RSS in MB. Uses psutil if available, else /proc."""
    try:
        import psutil
        rss_bytes: int = psutil.Process().memory_info().rss
        return rss_bytes / (1024 * 1024)
    except ImportError:
        pass
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except FileNotFoundError:
        pass
    return float("nan")


# ── Stub nodes ────────────────────────────────────────────────────────────────

class _S(BaseSenseNode):
    node_id     = "MemSense"
    signal_kind = "mem"
    async def read(self) -> Signal:
        return Signal(source=self.node_id, kind=self.signal_kind,
                      value=0.0, confidence=1.0, timestamp=time.monotonic())

class _I(BaseInstinctNode):
    node_id  = "MemInstinct"
    priority = 50
    async def evaluate(self, ctx: Context) -> Proposal | None:
        return None

class _A(BaseActionNode):
    node_id   = "MemAction"
    timeout_s = 1.0
    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


# ── In-process single-measurement helpers ────────────────────────────────────

async def measure(
    label: str,
    sense_nodes: list[BaseSenseNode],
    instinct_nodes: list[BaseInstinctNode],
    action_nodes: list[BaseActionNode],
    quiet: bool = False,
) -> float:
    """Build the given runtime topology, tick 100 times, gc, return RSS in MB."""
    bus = SignalBus()
    sm  = SenseMasterNode(bus=bus)
    im  = InstinctMasterNode(bus=bus)
    dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am  = ActionMasterNode(bus=bus)

    for sn in sense_nodes:
        sm.register(sn)
    for in_ in instinct_nodes:
        im.register(in_)
    for an in action_nodes:
        am.register(an)

    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(),
        instinct_master=im, decision_master=dm,
        action_master=am, bus=bus, tick_rate_hz=1000.0,
    )
    # Bypass background tick loop (Bug B — see audit 2026-04-16).
    for m in (sm, im, dm, am):
        await m.setup()
    for _ in range(100):
        await rt.tick()

    gc.collect()
    rss = _rss_mb()
    for m in (am, dm, im, sm):
        await m.teardown()
    if not quiet:
        print(f"  {label:<20}  {rss:8.3f} MB  ({len(sense_nodes)}S "
              f"{len(instinct_nodes)}I {len(action_nodes)}A)")
    return rss


async def _measure_baseline_async() -> float:
    """Baseline = imports only, no runtime. gc.collect() before read so the
    sample is taken at the same allocator state as the other configs."""
    gc.collect()
    return _rss_mb()


async def _measure_minimal_async() -> float:
    bus = SignalBus()
    return await measure(
        "minimal", [_S(bus=bus)], [_I(bus=bus)], [_A(bus=bus)], quiet=True,
    )


async def _measure_robot_arm_async() -> float:
    from examples.robot_arm.nodes import (
        CollisionReflex,
        EmergencyRetractAction,
        GraspInstinct,
        JointPositionSenseNode,
        ObjectDetectionSenseNode,
        PickAndPlaceAction,
        ProximitySenseNode,
    )
    bus = SignalBus()
    return await measure(
        "robot_arm",
        [ProximitySenseNode(bus=bus), ObjectDetectionSenseNode(bus=bus),
         JointPositionSenseNode(bus=bus)],
        [CollisionReflex(bus=bus), GraspInstinct(bus=bus)],
        [EmergencyRetractAction(bus=bus), PickAndPlaceAction(bus=bus)],
        quiet=True,
    )


def measure_once(config: str) -> float:
    """Run a single in-process RSS measurement of one configuration.

    Used both by the `--measure-once` CLI mode (so the parent can spawn
    a subprocess that prints one reading) and by tests that want to
    exercise one configuration without the subprocess machinery.
    """
    if config == "baseline":
        return asyncio.run(_measure_baseline_async())
    if config == "minimal":
        return asyncio.run(_measure_minimal_async())
    if config == "robot_arm":
        return asyncio.run(_measure_robot_arm_async())
    raise ValueError(
        f"unknown config: {config!r} (valid: {', '.join(_VALID_CONFIGS)})",
    )


# ── Subprocess-driven N-sample measurement ──────────────────────────────────

class RobotArmUnavailable(Exception):
    """Raised when the robot_arm config is requested but the example package
    isn't importable in the child subprocess."""


def _spawn_one(config: str) -> float:
    """Spawn one child process that prints a single RSS reading for `config`.

    The child is invoked via ``-m benchmarks.memory_footprint`` (not the
    raw script path) so that the project root, not ``benchmarks/``, sits
    at ``sys.path[0]``. Otherwise ``from examples.robot_arm.nodes import
    ...`` in the child fails because ``examples`` is a PEP 420 namespace
    package only resolvable from the project root.
    """
    result = subprocess.run(
        [sys.executable, "-m", "benchmarks.memory_footprint",
         "--measure-once", config],
        capture_output=True,
        text=True,
        cwd=str(_PROJECT_ROOT),
        timeout=_SUBPROCESS_TIMEOUT_S,
    )
    if result.returncode != 0:
        # robot_arm depends on examples/robot_arm/nodes.py. Detect import
        # failures broadly: ModuleNotFoundError mentions "examples" or
        # "robot_arm"; ImportError on a missing symbol from the package
        # also names the same module path. The previous narrow
        # ``"examples.robot_arm" in stderr`` check missed the error
        # entirely when Python printed the qualified name with a different
        # separator.
        stderr = result.stderr
        if config == "robot_arm" and (
            "ModuleNotFoundError" in stderr and (
                "examples" in stderr or "robot_arm" in stderr
            )
            or "examples.robot_arm" in stderr
            or "examples/robot_arm" in stderr
        ):
            raise RobotArmUnavailable(stderr.strip())
        raise RuntimeError(
            f"memory_footprint child for config={config!r} exited "
            f"{result.returncode}\nstderr:\n{result.stderr}",
        )
    lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
    if not lines:
        raise RuntimeError(
            f"memory_footprint child for config={config!r} produced no "
            f"output\nstderr:\n{result.stderr}",
        )
    return float(lines[-1].strip())


def measure_n_subprocess(
    config: str,
    n: int,
    *,
    warmup: int = _WARMUP_RUNS,
    quiet: bool = False,
) -> list[float]:
    """Measure RSS for `config` n times, each in a fresh subprocess.

    Each child has its own Python startup, allocator state, and page
    mapping, so the n readings reflect genuine run-to-run variance.
    Repeating the in-process measurement n times instead would yield
    n identical scalars (zero-width CI, std_dev=0).
    """
    for _ in range(warmup):
        _spawn_one(config)
    readings: list[float] = []
    for i in range(n):
        rss = _spawn_one(config)
        readings.append(rss)
        if not quiet:
            print(f"  {config:<20}  {rss:8.3f} MB  (run {i + 1}/{n})")
    return readings


# ── Statistics helper ─────────────────────────────────────────────────────────

def _summarise_readings(
    readings: list[float], description: str,
) -> dict[str, Any]:
    from benchmarks.stats import bootstrap_ci
    ci_lo, ci_hi = bootstrap_ci(readings)
    return {
        "description": description,
        "n_runs": len(readings),
        "mean_mb": round(statistics.mean(readings), 3),
        "median_mb": round(statistics.median(readings), 3),
        "std_dev": round(statistics.stdev(readings) if len(readings) > 1 else 0.0, 3),
        "ci_lower": round(ci_lo, 3),
        "ci_upper": round(ci_hi, 3),
        "readings": [round(r, 3) for r in readings],
    }


# ── Entry points ─────────────────────────────────────────────────────────────

def _run_parent(args: argparse.Namespace) -> None:
    configs: dict[str, dict[str, Any]] = {}

    baseline = measure_n_subprocess("baseline", args.runs)
    configs["baseline"] = _summarise_readings(
        baseline, "import only (Python interpreter overhead)",
    )
    print()

    minimal = measure_n_subprocess("minimal", args.runs)
    configs["minimal"] = _summarise_readings(
        minimal, "1 sense, 1 instinct, 1 action (100 ticks steady state)",
    )
    print()

    try:
        robot_arm = measure_n_subprocess("robot_arm", args.runs)
        configs["robot_arm"] = _summarise_readings(
            robot_arm,
            "3 sense, 2 instinct, 2 action (robot arm case study)",
        )
    except RobotArmUnavailable as exc:
        print(f"  (robot_arm nodes not available, skipping: {exc})")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "benchmark": "memory_footprint",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "machine": platform.node(),
        "args": {
            "runs": args.runs,
            "warmup_runs": _WARMUP_RUNS,
            "sampling": "subprocess-per-measurement",
        },
        "configs": configs,
    }
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"memory_footprint_{timestamp}.json"
    with open(out_file, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n  Results saved to {out_file}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Arachnite memory footprint benchmark",
    )
    parser.add_argument(
        "--runs", type=int, default=1,
        help="Independent measurement runs per configuration (default: 1)",
    )
    parser.add_argument(
        "--output-dir", type=str, default="benchmarks/results",
        help="Output directory for JSON results (default: benchmarks/results)",
    )
    parser.add_argument(
        "--measure-once",
        choices=_VALID_CONFIGS,
        default=None,
        help=(
            "Internal: run one measurement of the named config in the "
            "current process and print RSS (MB) to stdout. Used by the "
            "parent's subprocess invocations."
        ),
    )
    args = parser.parse_args()

    if args.measure_once is not None:
        # Child mode: print one RSS reading and exit. Stderr stays empty
        # on the happy path; the parent reads the last stdout line as a
        # float.
        rss = measure_once(args.measure_once)
        print(f"{rss:.6f}")
        return

    print("Arachnite memory footprint benchmark")
    print(f"Platform: {sys.platform}")
    print(f"Sampling: subprocess-per-measurement "
          f"({_WARMUP_RUNS} warmup + {args.runs} measured per config)")
    print("-" * 50)
    print(f"  {'configuration':<20}  {'RSS':>6}")
    print("-" * 50)
    _run_parent(args)
    print("-" * 50)
    print("Note: RSS includes Python interpreter overhead.")
    print("      Framework-only delta = robot_arm RSS - baseline RSS.")


if __name__ == "__main__":
    main()
