"""
benchmarks/scalability_extended.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Extended scalability benchmarks beyond the node-count sweep in §7.2.4.

Measures:
  1. SignalBus throughput under high signal load
  2. Concurrent action dispatch scaling
  3. Context history depth impact on tick latency

Run:
    python benchmarks/scalability_extended.py
    python benchmarks/scalability_extended.py --runs 30
    python benchmarks/scalability_extended.py --runs 5 --output-dir benchmarks/results

Output:
    benchmarks/results/scalability_extended_<timestamp>.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from collections.abc import Awaitable, Callable
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
    InstinctMasterNode,
    Proposal,
    Result,
    SenseMasterNode,
    Signal,
    SignalBus,
    WeightedDecisionNode,
)

# ── Stub nodes ───────────────────────────────────────────────────────────────

def _make_sense(idx: int) -> type[BaseSenseNode]:
    class S(BaseSenseNode):
        node_id = f"ScaleSense_{idx}"
        signal_kind = "scale"
        async def read(self) -> Signal:
            return Signal(source=self.node_id, kind=self.signal_kind,
                          value=float(idx), confidence=1.0, timestamp=time.monotonic())
    return S

def _make_instinct(idx: int, action_id: str = "ScaleAction_0") -> type[BaseInstinctNode]:
    class Instinct(BaseInstinctNode):
        node_id = f"ScaleInstinct_{idx}"
        priority = 50
        async def evaluate(self, ctx: Context) -> Proposal | None:
            return Proposal(instinct_id=self.node_id, action_id=action_id,
                            priority=self.priority, urgency=0.5)
    return Instinct

def _make_action(idx: int) -> type[BaseActionNode]:
    class A(BaseActionNode):
        node_id = f"ScaleAction_{idx}"
        timeout_s = 1.0
        async def execute(self, proposal: Proposal) -> Result:
            return Result(action_id=self.node_id, success=True)
    return A


# ── 1. SignalBus throughput ──────────────────────────────────────────────────

# SignalBus.subscribe deduplicates by callback identity, so a single shared
# `_noop` would register only once regardless of `n_subs`. The factory
# returns a fresh closure on every call, giving `n_subs` distinct callback
# identities. Hoisted to module scope so it isn't rebuilt
# `len(n_subs_configs) × n_runs` times in the inner loop (audit 2026-05-04 #4).
def _make_noop() -> Callable[[Signal], Awaitable[None]]:
    async def _noop(sig: Signal) -> None:
        pass
    return _noop


async def bench_bus_throughput(n_runs: int = 1) -> dict[str, dict[str, Any]]:
    """Measure SignalBus publish throughput with increasing subscriber counts."""
    print("\n  1. SignalBus Throughput")
    print("  " + "-" * 55)
    print(f"  {'Subscribers':>12}  {'Throughput':>14}  {'Per-publish':>12}")

    results: dict[str, dict[str, Any]] = {}
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

        tp_mean = statistics.mean(throughputs)
        lat_mean = statistics.mean(latencies)
        tp_std = statistics.stdev(throughputs) if n_runs > 1 else 0.0
        results[f"{n_subs}_subscribers"] = {
            "throughput_mean": round(tp_mean, 0),
            "throughput_std": round(tp_std, 0),
            "throughput_unit": "signals/s",
            "per_publish_us_mean": round(lat_mean, 2),
            "n_runs": n_runs,
        }
        print(f"  {n_subs:>12}  {tp_mean:>12.0f}/s  {lat_mean:>10.1f} µs")

    return results


# ── 2. Concurrent action scaling ────────────────────────────────────────────

async def bench_concurrent_actions(n_runs: int = 1) -> dict[str, dict[str, Any]]:
    """Measure dispatch latency with increasing concurrent actions."""
    print("\n  2. Concurrent Action Dispatch")
    print("  " + "-" * 55)
    print(f"  {'Actions':>8}  {'Mean':>8}  {'Median':>8}  {'P99':>8}  (ms)")

    results: dict[str, dict[str, Any]] = {}
    for n_actions in [1, 5, 10, 25, 50]:
        run_medians: list[float] = []
        run_means: list[float] = []
        run_p99s: list[float] = []
        for _ in range(n_runs):
            bus = SignalBus()
            sm = SenseMasterNode(bus=bus)
            im = InstinctMasterNode(bus=bus)
            dm = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
            am = ActionMasterNode(bus=bus)
            sm.register(_make_sense(0)(bus=bus))
            for i in range(n_actions):
                im.register(_make_instinct(i, f"ScaleAction_{i}")(bus=bus))
                am.register(_make_action(i)(bus=bus))
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
            s = sorted(samples)
            n = len(s)
            run_medians.append(statistics.median(s))
            run_means.append(statistics.mean(s))
            run_p99s.append(s[int(n * 0.99)])

        mean_val = statistics.mean(run_means)
        median_val = statistics.mean(run_medians)
        p99_val = statistics.mean(run_p99s)
        results[f"{n_actions}_actions"] = {
            "mean_ms": round(mean_val, 4),
            "median_ms": round(median_val, 4),
            "p99_ms": round(p99_val, 4),
            "n_runs": n_runs,
        }
        print(f"  {n_actions:>8}  {mean_val:>8.3f}  {median_val:>8.3f}  {p99_val:>8.3f}")

    return results


# ── 3. Context history depth ────────────────────────────────────────────────

async def bench_history_depth(n_runs: int = 1) -> dict[str, dict[str, Any]]:
    """Measure tick latency with increasing context history depth."""
    print("\n  3. Context History Depth Impact")
    print("  " + "-" * 55)
    print(f"  {'History':>8}  {'Mean':>8}  {'Median':>8}  {'P99':>8}  (ms)")

    results: dict[str, dict[str, Any]] = {}
    for history_len in [1, 10, 50, 100, 500]:
        run_medians: list[float] = []
        run_means: list[float] = []
        run_p99s: list[float] = []
        for _ in range(n_runs):
            bus = SignalBus()
            sm = SenseMasterNode(bus=bus)
            im = InstinctMasterNode(bus=bus)
            dm = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
            am = ActionMasterNode(bus=bus)
            sm.register(_make_sense(0)(bus=bus))
            im.register(_make_instinct(0)(bus=bus))
            am.register(_make_action(0)(bus=bus))
            rt = ArachniteRuntime(
                sense_master=sm, context=ContextNode(history_length=history_len),
                instinct_master=im, decision_master=dm,
                action_master=am, bus=bus, tick_rate_hz=10_000.0,
            )
            # Bypass background tick loop (Bug B — see audit 2026-04-16).
            for m in (sm, im, dm, am):
                await m.setup()
            for _ in range(history_len + 100):
                await rt.tick()
            samples = []
            for _ in range(2_000):
                t0 = time.perf_counter()
                await rt.tick()
                samples.append((time.perf_counter() - t0) * 1_000)
            for m in (am, dm, im, sm):
                await m.teardown()
            s = sorted(samples)
            n = len(s)
            run_medians.append(statistics.median(s))
            run_means.append(statistics.mean(s))
            run_p99s.append(s[int(n * 0.99)])

        mean_val = statistics.mean(run_means)
        median_val = statistics.mean(run_medians)
        p99_val = statistics.mean(run_p99s)
        results[f"depth_{history_len}"] = {
            "mean_ms": round(mean_val, 4),
            "median_ms": round(median_val, 4),
            "p99_ms": round(p99_val, 4),
            "n_runs": n_runs,
        }
        print(f"  {history_len:>8}  {mean_val:>8.3f}  {median_val:>8.3f}  {p99_val:>8.3f}")

    return results


# ── Main ─────────────────────────────────────────────────────────────────────

async def _run(args: argparse.Namespace) -> dict[str, Any]:
    print("Arachnite Extended Scalability Benchmarks")
    print(f"Platform: {sys.platform} / CPython {platform.python_version()}")
    print("=" * 60)

    bus_throughput = await bench_bus_throughput(args.runs)
    concurrent_actions = await bench_concurrent_actions(args.runs)
    history_depth = await bench_history_depth(args.runs)

    print("\n" + "=" * 60)
    print("Done.")

    return {
        "benchmark": "scalability_extended",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "machine": platform.node(),
        "args": {"runs": args.runs},
        "results": {
            "bus_throughput": bus_throughput,
            "concurrent_actions": concurrent_actions,
            "history_depth": history_depth,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Arachnite extended scalability benchmarks",
    )
    parser.add_argument(
        "--runs", type=int, default=1,
        help="Independent runs per configuration (default: 1)",
    )
    parser.add_argument(
        "--output-dir", type=str, default="benchmarks/results",
        help="Output directory (default: benchmarks/results)",
    )
    args = parser.parse_args()

    result = asyncio.run(_run(args))

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"scalability_extended_{timestamp}.json"
    with open(out_file, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n  Results saved to {out_file}")


if __name__ == "__main__":
    main()
