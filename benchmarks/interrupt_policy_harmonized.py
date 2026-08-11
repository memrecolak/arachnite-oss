"""
benchmarks/interrupt_policy_harmonized.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Apples-to-apples interrupt-policy latency: one identical action, four policies.

Motivation (revision experiment E6): the per-policy scenarios in
``multistep_action_latency.py`` each use a *different* action (different
step flags, sleeps, and interrupt fire-points), so their rows measure
different semantic quantities and are not mutually comparable — e.g.
checkpoint (2.39 ms) reads slower than rollback (1.23 ms) purely because
its scenario fires the interrupt earlier and must carry through more
scheduled step work. This benchmark removes every free variable except
the policy itself:

  - ONE action shape shared by all four policies:
      step0  interruptible=False, rollback callable   (completes pre-fire)
      step1  interruptible=True
      step2  interruptible=True
      step3  interruptible=True,  checkpoint=True
      step4  interruptible=True,  checkpoint=True
    Every step performs the same ``asyncio.sleep(_STEP_SLEEP_S)``.
  - ONE interrupt fire-point for all policies: the interrupt task is
    unblocked by an event set at the end of step0, so the request lands
    during step1 for every policy, deterministically.
  - The ONLY varied factor is ``interrupt_policy``.

Expected honour points (step-boundary semantics, spec §17.4):
  ALWAYS   → boundary before step2 (first boundary after the request)
  ROLLBACK → same boundary + rollback walk of completed step0
  CHECKPOINT → held until step3 (first checkpoint)
  NEVER    → request ignored; action runs to completion (measured as
             fire-to-return for comparability; asserted un-interrupted)

Measured window per iteration: wall-clock from the moment
``request_interrupt()`` is called to the moment ``execute()`` returns
(identical to the multistep benchmark's window).

Run:
    python benchmarks/interrupt_policy_harmonized.py
    python benchmarks/interrupt_policy_harmonized.py --runs 5 --iterations 200
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path

from arachnite import (
    ActionStep,
    InterruptPolicy,
    InterruptRequest,
    MultiStepActionNode,
    Proposal,
    SignalBus,
    StepResult,
)
from benchmarks.stats import DescriptiveStats, format_stats_table

_RUNS = 5
_ITERATIONS = 500

# Same per-step busy time as multistep_action_latency.py so the two
# benchmarks' magnitudes are directly relatable.
_STEP_SLEEP_S = 0.0005

POLICIES: tuple[InterruptPolicy, ...] = (
    InterruptPolicy.ALWAYS,
    InterruptPolicy.NEVER,
    InterruptPolicy.CHECKPOINT,
    InterruptPolicy.ROLLBACK,
)


def _proposal(action_id: str) -> Proposal:
    return Proposal(
        instinct_id="bench",
        action_id=action_id,
        priority=100,
        urgency=0.5,
    )


def _interrupt_request() -> InterruptRequest:
    return InterruptRequest(
        new_proposal=_proposal("__bench_other__"),
        requesting_instinct_id="bench",
        reason="benchmark",
    )


class _HarmonizedAction(MultiStepActionNode):
    """The single shared action shape; only ``interrupt_policy`` varies.

    The policy is assigned per instance in ``__init__`` — ``execute()``
    reads ``self.interrupt_policy``, so an instance attribute cleanly
    overrides the class default without needing four subclasses (which
    would defeat the purpose of an identical action).
    """

    node_id = "BenchHarmonizedAction"
    timeout_s = 5.0

    def __init__(self, policy: InterruptPolicy, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.interrupt_policy = policy
        self.rolled_back: list[str] = []
        # Set at the end of step0 so the interrupt task fires at the same
        # deterministic graph position for every policy.
        self.step0_done: asyncio.Event = asyncio.Event()

    async def _rb_step0(self) -> None:
        self.rolled_back.append("step0")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step0", interruptible=False, rollback=self._rb_step0),
            ActionStep("step1", interruptible=True),
            ActionStep("step2", interruptible=True),
            ActionStep("step3", interruptible=True, checkpoint=True),
            ActionStep("step4", interruptible=True, checkpoint=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        await asyncio.sleep(_STEP_SLEEP_S)
        if step.name == "step0":
            self.step0_done.set()
        return StepResult(step_name=step.name, success=True)


async def _measure(policy: InterruptPolicy) -> tuple[float, str]:
    """One iteration for one policy.

    Returns (latency_ms, honour_point) where honour_point is the step the
    action stopped at (or "completed" for a run-to-completion result).
    """
    bus = SignalBus()
    action = _HarmonizedAction(policy, bus=bus)
    fire_ts: list[float] = []

    async def _fire_interrupt() -> None:
        await action.step0_done.wait()
        fire_ts.append(time.perf_counter())
        action.request_interrupt(_interrupt_request())

    task = asyncio.create_task(_fire_interrupt())
    result = await action.execute(_proposal(action.node_id))
    return_ts = time.perf_counter()
    await task

    # Per-policy invariants — the semantic proof that only the policy
    # changed the outcome.
    if policy == InterruptPolicy.NEVER:
        assert result.success and not result.interrupted, \
            "NEVER must ignore the interrupt and complete"
    elif policy == InterruptPolicy.ROLLBACK:
        assert result.interrupted and result.rolled_back, \
            "ROLLBACK must interrupt and roll back completed mandatory steps"
        assert action.rolled_back == ["step0"], "step0 rollback must fire"
    elif policy == InterruptPolicy.ALWAYS:
        assert result.interrupted, "ALWAYS must interrupt"
    else:  # CHECKPOINT — rare timing slips may let the action complete
        assert result.interrupted or result.success, "CHECKPOINT invariant"

    honour_point = result.stopped_at_step if result.interrupted else "completed"
    return (return_ts - fire_ts[0]) * 1_000.0, honour_point or "unknown"


async def run(iterations: int = _ITERATIONS) -> tuple[
    dict[str, list[float]], dict[str, Counter[str]],
]:
    """One independent run: per-policy sample arrays + honour-point tallies."""
    samples: dict[str, list[float]] = {p.value: [] for p in POLICIES}
    honour: dict[str, Counter[str]] = {p.value: Counter() for p in POLICIES}
    for _ in range(iterations):
        for policy in POLICIES:
            ms, point = await _measure(policy)
            samples[policy.value].append(ms)
            honour[policy.value][point] += 1
    return samples, honour


async def multi_run(
    n_runs: int, iterations: int = _ITERATIONS,
) -> tuple[dict[str, DescriptiveStats], dict[str, Counter[str]]]:
    names = [p.value for p in POLICIES]
    run_samples_by_name: dict[str, list[list[float]]] = {n: [] for n in names}
    run_medians_by_name: dict[str, list[float]] = {n: [] for n in names}
    pooled_by_name: dict[str, list[float]] = {n: [] for n in names}
    honour_total: dict[str, Counter[str]] = {n: Counter() for n in names}

    for i in range(n_runs):
        samples, honour = await run(iterations=iterations)
        for name in names:
            run_samples_by_name[name].append(samples[name])
            run_medians_by_name[name].append(statistics.median(samples[name]))
            pooled_by_name[name].extend(samples[name])
            honour_total[name].update(honour[name])
        line = f"    Run {i + 1:>3d}/{n_runs}: " + "  ".join(
            f"{name}={statistics.median(samples[name]):.3f} ms" for name in names
        )
        print(line)

    stats_by_name = {
        name: DescriptiveStats.from_runs(
            run_medians_by_name[name],
            pooled_by_name[name],
            iterations,
            run_samples=run_samples_by_name[name],
        )
        for name in names
    }
    return stats_by_name, honour_total


def report(
    stats_by_name: dict[str, DescriptiveStats],
    honour_total: dict[str, Counter[str]],
) -> None:
    print()
    print("Harmonized interrupt-policy latency - identical action, policy is the")
    print("only varied factor (ms, fire-to-return)")
    print("-" * 72)
    for policy in POLICIES:
        name = policy.value
        print(format_stats_table(name, stats_by_name[name], "ms"))
        points = ", ".join(
            f"{point}: {count}" for point, count in honour_total[name].most_common()
        )
        print(f"    honour points: {points}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Apples-to-apples interrupt-policy latency: one identical "
            "multi-step action, one fire-point, four policies."
        ),
    )
    parser.add_argument(
        "--runs", "-n", type=int, default=_RUNS,
        help=f"Independent runs (default: {_RUNS}).",
    )
    parser.add_argument(
        "--iterations", "-t", type=int, default=_ITERATIONS,
        help=f"Measurement iterations per run (default: {_ITERATIONS}).",
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default="benchmarks/results",
        help="Directory for the JSON output file.",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Arachnite harmonized interrupt-policy benchmark")
    print(f"Iterations per run: {args.iterations}  |  Runs: {args.runs}")
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print("-" * 72)

    stats_by_name, honour_total = asyncio.run(
        multi_run(args.runs, args.iterations)
    )
    report(stats_by_name, honour_total)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"interrupt_policy_harmonized_{timestamp}.json"
    payload = {
        "benchmark": "interrupt_policy_harmonized",
        "unit": "ms",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "n_runs": args.runs,
        "iterations_per_run": args.iterations,
        "step_sleep_s": _STEP_SLEEP_S,
        "action_shape": [
            "step0 interruptible=False rollback",
            "step1 interruptible=True",
            "step2 interruptible=True",
            "step3 interruptible=True checkpoint=True",
            "step4 interruptible=True checkpoint=True",
        ],
        "fire_point": "event set at end of step0 (request lands during step1)",
        "policies": {
            name: asdict(stats) for name, stats in stats_by_name.items()
        },
        "honour_points": {
            name: dict(counter) for name, counter in honour_total.items()
        },
    }
    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nResults saved to {out_file}")


if __name__ == "__main__":
    main()
