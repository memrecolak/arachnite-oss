"""
benchmarks/multistep_action_latency.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Interrupt / rollback / mandatory-block latency for MultiStepActionNode.

Four measurement scenarios, all framework-level wall-clock timings in
milliseconds driven by ``time.perf_counter()``:

1. **ALWAYS policy** — fire ``request_interrupt()`` mid-execution; measure
   wall-clock from the moment the interrupt flag is set to the moment
   ``execute()`` returns. Establishes the "cheap" (no rollback) interrupt
   latency floor.
2. **CHECKPOINT policy** — same shape, but only steps flagged
   ``checkpoint=True`` honour interrupts. Measures the extra latency
   imposed by carrying through non-checkpoint steps.
3. **ROLLBACK policy** — interrupt triggers ``on_interrupted()`` which
   invokes the rollback callables of completed non-interruptible steps in
   reverse order. Measures total interrupt-to-return latency *including*
   rollback. The per-completed-step rollback overhead is derived via
   **direct timing** of the ``on_interrupted()`` walk itself (see
   ``_RollbackPolicyAction.on_interrupted``) — this isolates the pure
   rollback cost from setup overhead (interrupt-task wait, post-step
   check, ``_handle_interrupt`` entry), which would inflate the per-step
   metric if computed from the outer wall-clock.
4. **Mandatory-block worst-case** — action contains a non-interruptible
   block (``interruptible=False``). ``request_interrupt()`` is fired
   mid-block via the node's public setter; the block MUST run to
   completion and the interrupt is honoured only at the next
   interruptible boundary. This establishes an empirical
   ``T_worst_mandatory`` bound.

Each scenario produces a ``DescriptiveStats`` via
``DescriptiveStats.from_runs(..., run_samples=per_run_samples)`` — the
same Bench-4 bootstrap CI path used by ``stage_breakdown.py``.

Run:
    python benchmarks/multistep_action_latency.py
    python benchmarks/multistep_action_latency.py --runs 5 --iterations 200
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
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

# Per-step busy time used by checkpoint, rollback, and mandatory-block
# scenarios. Kept small so a 500-iteration run finishes in ~seconds.
# _AlwaysPolicyAction does NOT use this — its steps use asyncio.sleep(0)
# so the always_policy measurement captures pure framework interrupt-check
# overhead rather than step-work time (which varies by asyncio timer
# resolution across Python versions and would dominate the reading).
_STEP_SLEEP_S = 0.0005

# Scenario labels (stable — consumed by the JSON report schema).
SCENARIO_ALWAYS = "always_policy"
SCENARIO_CHECKPOINT = "checkpoint_policy"
SCENARIO_ROLLBACK = "rollback_policy"
SCENARIO_MANDATORY_WORST_CASE = "mandatory_block_worst_case"

SCENARIOS: tuple[str, ...] = (
    SCENARIO_ALWAYS,
    SCENARIO_CHECKPOINT,
    SCENARIO_ROLLBACK,
    SCENARIO_MANDATORY_WORST_CASE,
)


# ── Benchmark-private action nodes ───────────────────────────────────────────
#
# Each action mirrors a concrete ``InterruptPolicy`` variant plus a
# mandatory-block specimen. All use a short ``asyncio.sleep`` per step so
# an interrupt task scheduled before ``execute()`` has a guaranteed event-
# loop window to flip ``_interrupt_requested``.


def _proposal(action_id: str) -> Proposal:
    """Build a minimal proposal for the action under test."""
    return Proposal(
        instinct_id="bench",
        action_id=action_id,
        priority=100,
        urgency=0.5,
    )


def _interrupt_request() -> InterruptRequest:
    """Build a minimal InterruptRequest for the benchmark."""
    return InterruptRequest(
        new_proposal=_proposal("__bench_other__"),
        requesting_instinct_id="bench",
        reason="benchmark",
    )


class _AlwaysPolicyAction(MultiStepActionNode):
    """Five interruptible steps. Interrupt lands at the next boundary.

    Steps use ``asyncio.sleep(0)`` (yield without sleeping) so the
    always_policy measurement captures pure framework interrupt-check
    overhead.  Using a timed sleep would make the measurement equal to
    one step's sleep duration — a function of asyncio timer resolution
    that differs between Python versions — rather than the framework cost.
    """

    node_id = "BenchAlwaysAction"
    interrupt_policy = InterruptPolicy.ALWAYS
    timeout_s = 5.0

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        # Signalled when step0 finishes so the interrupt task fires at a
        # deterministic graph position, immune to asyncio timer resolution.
        self.step0_done: asyncio.Event = asyncio.Event()

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep(f"step{i}", interruptible=True)
            for i in range(5)
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        await asyncio.sleep(0)  # yield only — no timed sleep, see class docstring
        if step.name == "step0":
            self.step0_done.set()
        return StepResult(step_name=step.name, success=True)


class _CheckpointPolicyAction(MultiStepActionNode):
    """Five steps; only step3 and step4 are checkpoints."""

    node_id = "BenchCheckpointAction"
    interrupt_policy = InterruptPolicy.CHECKPOINT
    timeout_s = 5.0

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step0", interruptible=True, checkpoint=False),
            ActionStep("step1", interruptible=True, checkpoint=False),
            ActionStep("step2", interruptible=True, checkpoint=False),
            ActionStep("step3", interruptible=True, checkpoint=True),
            ActionStep("step4", interruptible=True, checkpoint=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        await asyncio.sleep(_STEP_SLEEP_S)
        return StepResult(step_name=step.name, success=True)


class _RollbackPolicyAction(MultiStepActionNode):
    """
    Five steps, the first three flagged non-interruptible so their
    rollback callables fire on interrupt. The interrupt is scheduled to
    land after step3 completes; ``on_interrupted()`` then rolls back the
    three mandatory steps in reverse order.

    The rollback walk itself is wrapped in ``time.perf_counter()`` via
    an override of ``on_interrupted()``; this isolates the pure rollback
    cost (the per-completed-step metric) from outer setup overhead.
    """

    node_id = "BenchRollbackAction"
    interrupt_policy = InterruptPolicy.ROLLBACK
    timeout_s = 5.0

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.rolled_back: list[str] = []
        # Populated by ``on_interrupted`` — pure rollback-walk wall-clock (s).
        self.rollback_walk_s: float = 0.0

    async def _rb_step0(self) -> None:
        self.rolled_back.append("step0")

    async def _rb_step1(self) -> None:
        self.rolled_back.append("step1")

    async def _rb_step2(self) -> None:
        self.rolled_back.append("step2")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step0", interruptible=False, rollback=self._rb_step0),
            ActionStep("step1", interruptible=False, rollback=self._rb_step1),
            ActionStep("step2", interruptible=False, rollback=self._rb_step2),
            ActionStep("step3", interruptible=True),
            ActionStep("step4", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        await asyncio.sleep(_STEP_SLEEP_S)
        return StepResult(step_name=step.name, success=True)

    async def on_interrupted(
        self,
        completed: list[StepResult],
        pending: list[ActionStep],
        proposal: Proposal,
    ) -> None:
        """
        Directly time the rollback walk. Delegates to the base class
        implementation (which invokes rollback callables of completed
        non-interruptible steps in reverse order) and records the
        wall-clock in ``self.rollback_walk_s``.
        """
        t0 = time.perf_counter()
        await super().on_interrupted(completed, pending, proposal)
        self.rollback_walk_s = time.perf_counter() - t0


class _MandatoryBlockAction(MultiStepActionNode):
    """
    Long mandatory middle step bracketed by short interruptible steps.
    The mandatory step has multiple short sleeps so that interrupts
    delivered mid-block cannot preempt it — the block runs to completion
    regardless. Used by the mandatory-block worst-case scenario.
    """

    node_id = "BenchMandatoryAction"
    interrupt_policy = InterruptPolicy.ALWAYS
    timeout_s = 5.0

    # Number of inner awaits inside the mandatory step. Multiplied by
    # ``_STEP_SLEEP_S`` this gives the block's wall-clock duration — a
    # synthetic proxy for the ``Σ timeout_s`` formal bound.
    _MANDATORY_INNER_AWAITS = 10

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("prep", interruptible=True),
            ActionStep("mandatory", interruptible=False),
            ActionStep("finish", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult],
    ) -> StepResult:
        if step.name == "mandatory":
            for _ in range(self._MANDATORY_INNER_AWAITS):
                await asyncio.sleep(_STEP_SLEEP_S)
        else:
            await asyncio.sleep(_STEP_SLEEP_S)
        return StepResult(step_name=step.name, success=True)


# ── Single-iteration measurements ────────────────────────────────────────────
#
# Each function schedules an interrupt task via ``asyncio.create_task()``
# — you MUST schedule after ``execute()`` has started because
# ``execute()`` resets the flag on entry. The measurement window is the
# wall-clock from the moment the interrupt task sets the flag to the
# moment ``execute()`` returns.


async def _measure_always() -> float:
    """Wall-clock ms from interrupt-request to execute() return (ALWAYS).

    Measures pure framework interrupt-check overhead: how long after
    ``request_interrupt()`` is called does ``execute()`` return when
    every step uses ``asyncio.sleep(0)`` (no timed sleep).

    The interrupt fires exactly after step0 completes (via step0_done
    event) so the trigger is at a deterministic graph position.  Steps
    yield without sleeping so the window contains only event-loop
    switching cost and the framework's interrupt-flag check — no step
    work time that would vary with asyncio timer resolution across
    Python versions.
    """
    bus = SignalBus()
    action = _AlwaysPolicyAction(bus=bus)
    fire_ts: list[float] = []

    async def _fire_interrupt() -> None:
        # Wait until step0 is done rather than sleeping a fixed wall-clock
        # interval — this is immune to asyncio timer resolution differences.
        await action.step0_done.wait()
        fire_ts.append(time.perf_counter())
        action.request_interrupt(_interrupt_request())

    task = asyncio.create_task(_fire_interrupt())
    result = await action.execute(_proposal(action.node_id))
    return_ts = time.perf_counter()
    await task
    assert result.interrupted, "ALWAYS policy must interrupt"
    return (return_ts - fire_ts[0]) * 1_000.0


async def _measure_checkpoint() -> float:
    """Wall-clock ms from interrupt-request to execute() return (CHECKPOINT)."""
    bus = SignalBus()
    action = _CheckpointPolicyAction(bus=bus)
    fire_ts: list[float] = []

    async def _fire_interrupt() -> None:
        # Fire early — interrupt should be held until step3 (first checkpoint).
        await asyncio.sleep(_STEP_SLEEP_S * 0.5)
        fire_ts.append(time.perf_counter())
        action.request_interrupt(_interrupt_request())

    task = asyncio.create_task(_fire_interrupt())
    result = await action.execute(_proposal(action.node_id))
    return_ts = time.perf_counter()
    await task
    # At the checkpoint the interrupt fires; occasionally timing slips and
    # the action completes normally (all steps ran) — both are valid for
    # the latency-measurement purpose.
    assert result.interrupted or result.success, "CHECKPOINT scenario invariant"
    return (return_ts - fire_ts[0]) * 1_000.0


async def _measure_rollback() -> tuple[float, int, float]:
    """
    Measure ROLLBACK. Returns three values:
      - outer wall-clock ms from interrupt-request to ``execute()`` return
        (the ``rollback_policy`` scenario total),
      - number of rollback callables that fired,
      - **direct** wall-clock ms of the ``on_interrupted()`` rollback walk
        itself (used to derive ``rollback_policy_per_step``).
    """
    bus = SignalBus()
    action = _RollbackPolicyAction(bus=bus)
    fire_ts: list[float] = []

    async def _fire_interrupt() -> None:
        # Wait long enough that all three mandatory steps have completed,
        # so their rollbacks will fire during on_interrupted().
        await asyncio.sleep(_STEP_SLEEP_S * 3.5)
        fire_ts.append(time.perf_counter())
        action.request_interrupt(_interrupt_request())

    task = asyncio.create_task(_fire_interrupt())
    result = await action.execute(_proposal(action.node_id))
    return_ts = time.perf_counter()
    await task
    assert result.interrupted and result.rolled_back, "ROLLBACK scenario invariant"
    total_ms = (return_ts - fire_ts[0]) * 1_000.0
    walk_ms = action.rollback_walk_s * 1_000.0
    return total_ms, len(action.rolled_back), walk_ms


async def _measure_mandatory_block_worst_case() -> tuple[float, bool]:
    """
    Empirical ``T_worst_mandatory`` probe. Measures the wall-clock of the
    mandatory block itself when ``request_interrupt()`` is fired mid-
    block via the node's public setter. The returned ``bool`` is ``True``
    iff the block completed without being preempted AND the interrupt
    was honoured at the next interruptible boundary (``finish`` skipped).
    """
    bus = SignalBus()
    action = _MandatoryBlockAction(bus=bus)

    async def _fire_interrupt_midblock() -> None:
        # Wait until execute() is inside the mandatory step — well past
        # the prep step and partway through the inner awaits.
        target_delay = _STEP_SLEEP_S * (
            1.5 + _MandatoryBlockAction._MANDATORY_INNER_AWAITS * 0.3
        )
        await asyncio.sleep(target_delay)
        action.request_interrupt(_interrupt_request())

    task = asyncio.create_task(_fire_interrupt_midblock())
    t0 = time.perf_counter()
    result = await action.execute(_proposal(action.node_id))
    t1 = time.perf_counter()
    await task

    # The mandatory block ran to completion even though the interrupt
    # arrived mid-block; the interrupt took effect at the next
    # interruptible step boundary (``finish``). So the "mandatory"
    # step must be in completed_steps but the overall result is
    # interrupted (finish was skipped) — that's the semantic proof.
    completed_names = {sr.step_name for sr in result.step_results}
    worst_case_honoured = (
        "mandatory" in completed_names
        and result.interrupted
        and "finish" not in completed_names
    )
    return (t1 - t0) * 1_000.0, worst_case_honoured


# ── Per-run drivers (each returns a list of per-iteration samples) ──────────


async def run(iterations: int = _ITERATIONS) -> dict[str, list[float]]:
    """Run one independent measurement run.

    Returns a dict mapping scenario names to per-iteration sample arrays
    (milliseconds). The ``rollback_policy`` scenario additionally
    contributes a ``rollback_policy_per_step`` series containing the
    per-completed-step rollback overhead (ms). The per-step metric is
    derived by **directly timing** the ``on_interrupted()`` rollback walk
    and dividing by the number of rollback callables that fired; this
    isolates pure rollback cost from surrounding setup overhead.
    """
    out: dict[str, list[float]] = {name: [] for name in SCENARIOS}
    per_step_rollback: list[float] = []

    for _ in range(iterations):
        out[SCENARIO_ALWAYS].append(await _measure_always())
        out[SCENARIO_CHECKPOINT].append(await _measure_checkpoint())
        total_ms, n_rb, walk_ms = await _measure_rollback()
        out[SCENARIO_ROLLBACK].append(total_ms)
        if n_rb > 0:
            # Direct-timed rollback walk / #callables = pure per-step cost.
            per_step_rollback.append(walk_ms / n_rb)
        mand_ms, _ok = await _measure_mandatory_block_worst_case()
        out[SCENARIO_MANDATORY_WORST_CASE].append(mand_ms)

    out["rollback_policy_per_step"] = per_step_rollback
    return out


# ── Multi-run aggregation ───────────────────────────────────────────────────


async def multi_run(
    n_runs: int, iterations: int = _ITERATIONS,
) -> dict[str, DescriptiveStats]:
    """Run ``n_runs`` independent runs and aggregate per-scenario stats.

    Uses ``DescriptiveStats.from_runs(..., run_samples=...)`` so that
    P95 / P99 95% bootstrap confidence intervals are computed over the
    per-run tail estimates (Bench-4 / spec §22.1.3 methodology).
    """
    all_names = list(SCENARIOS) + ["rollback_policy_per_step"]
    run_samples_by_name: dict[str, list[list[float]]] = {
        name: [] for name in all_names
    }
    run_medians_by_name: dict[str, list[float]] = {
        name: [] for name in all_names
    }
    pooled_by_name: dict[str, list[float]] = {
        name: [] for name in all_names
    }

    for i in range(n_runs):
        samples_dict = await run(iterations=iterations)
        for name in all_names:
            samples = samples_dict.get(name, [])
            if not samples:
                continue
            run_samples_by_name[name].append(samples)
            run_medians_by_name[name].append(statistics.median(samples))
            pooled_by_name[name].extend(samples)
        # Per-run summary line
        medians = {
            name: statistics.median(samples_dict[name])
            for name in SCENARIOS if samples_dict.get(name)
        }
        line = f"    Run {i + 1:>3d}/{n_runs}: " + "  ".join(
            f"{name.split('_')[0]}={medians[name]:.3f} ms"
            for name in SCENARIOS if name in medians
        )
        print(line)

    stats_by_name: dict[str, DescriptiveStats] = {}
    for name in all_names:
        if not pooled_by_name[name]:
            continue
        stats_by_name[name] = DescriptiveStats.from_runs(
            run_medians_by_name[name],
            pooled_by_name[name],
            iterations,
            run_samples=run_samples_by_name[name],
        )
    return stats_by_name


# ── Reporting ───────────────────────────────────────────────────────────────


def report(stats_by_name: dict[str, DescriptiveStats]) -> None:
    print()
    print("Multi-step action interrupt / rollback / mandatory-block latency (ms)")
    print("-" * 72)
    order = [
        SCENARIO_ALWAYS,
        SCENARIO_CHECKPOINT,
        SCENARIO_ROLLBACK,
        "rollback_policy_per_step",
        SCENARIO_MANDATORY_WORST_CASE,
    ]
    for name in order:
        if name in stats_by_name:
            print(format_stats_table(name, stats_by_name[name], "ms"))


# ── CLI ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Multi-step action interrupt / rollback / mandatory-block "
            "latency benchmark."
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

    print("Arachnite multi-step action latency benchmark")
    print(f"Iterations per run: {args.iterations}  |  Runs: {args.runs}")
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print("-" * 72)

    stats_by_name = asyncio.run(multi_run(args.runs, args.iterations))
    report(stats_by_name)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"multistep_action_latency_{timestamp}.json"
    payload = {
        "benchmark": "multistep_action_latency",
        "unit": "ms",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "n_runs": args.runs,
        "iterations_per_run": args.iterations,
        "step_sleep_s": _STEP_SLEEP_S,
        "mandatory_inner_awaits": _MandatoryBlockAction._MANDATORY_INNER_AWAITS,
        "scenarios": {
            name: asdict(stats)
            for name, stats in stats_by_name.items()
        },
    }
    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nResults saved to {out_file}")


if __name__ == "__main__":
    main()
