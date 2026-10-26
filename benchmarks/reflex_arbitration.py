"""
benchmarks/reflex_arbitration.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reflex response while an action is running (ADR 0005; experiments E9, E10).

Two experiments share one instrumented rig: an arm actuator that logs every
motion, flags overlapping writers and detects a dropped object, a hazard
sense node, a collision reflex and its retract action.

E9 — stage-wise collision injection
    A six-step pick-and-place (approach | lower, close, raise | move,
    release; the middle three form a mandatory block, ROLLBACK policy) runs,
    and a latched collision stimulus is raised at a uniformly random point
    inside one stage. The reflex's retract action needs the arm the pick
    owns. Per trial: stimulus onset → retract ``execute()`` entry latency,
    the pick's fate (completed / interrupted / preempted, rolled back or
    not, where it stopped), steps the pick began after the onset, safe-stop
    duration, actuator overlaps, and whether the object was dropped.

E10 — reflex latency under a long-running action
    A conveyor action on a disjoint resource is re-dispatched back to back
    with a fixed duration D; each stimulus arrives at a uniformly random
    point inside a running conveyor action (D = 0: a random tick phase).
    Per trial: onset → retract entry latency. Per cell: tick pacing (achieved tick
    interval, overruns) and the number of ticks that ran during each
    conveyor action.

Arms (chosen automatically from the installed arachnite):
    blocking  arachnite without non-blocking dispatch (≤ 0.12): the tick
              awaits every action, so a reflex waits for the running one.
    preempt   tick-lane reflex, ReflexPreemption.PREEMPT (default).
    wait      tick-lane reflex, ReflexPreemption.WAIT_FOR_BLOCK (E9 only).
    event     event-lane reflex (reflex_mode="event"), the sense node pushes
              the hazard the moment it occurs; PREEMPT.
              In E10 "preempt" is reported as "tick".

The rig uses only API present in both 0.12.x and 0.13, so the same file
produces the "before" arm when run in a 0.12 environment (class attributes
the old version does not know, e.g. ``resources``, are simply ignored).

Every trial gets a fresh runtime (E9) or a fresh cell runtime (E10); onsets
are drawn from a seeded PRNG. On Windows the ~15.6 ms asyncio timer
granularity quantises onset phases; report numbers from Linux hosts.

Run:
    python benchmarks/reflex_arbitration.py e9 --trials 200
    python benchmarks/reflex_arbitration.py e10 --trials 200
    python benchmarks/reflex_arbitration.py e9 --trials 5 --arms preempt,event
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import platform
import random
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import arachnite
from arachnite import (
    ActionMasterNode,
    ActionStep,
    ArachniteRuntime,
    BaseActionNode,
    BaseInstinctNode,
    BaseReflexInstinctNode,
    BaseSenseNode,
    Context,
    ContextNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    InstinctMasterNode,
    InterruptPolicy,
    MultiStepActionNode,
    Proposal,
    Result,
    SenseMasterNode,
    Signal,
    SignalBus,
    StepResult,
)

try:
    from arachnite import ReflexPreemption
except ImportError:  # arachnite ≤ 0.12: no arbitration, the tick blocks
    ReflexPreemption = None  # type: ignore[assignment,misc]

HAS_ARBITRATION = ReflexPreemption is not None
#: Dispatch model of the installed arachnite (the branch keeps its old
#: version string until the release bump, so label by capability).
DISPATCH = "nonblocking" if HAS_ARBITRATION else "blocking"

clock = time.perf_counter

_SEED = 20261026
_TICK_RATE_HZ = 10.0
_TRIALS = 200

# Pick-and-place step durations (s): the robot_arm example's realistic
# timings scaled by 0.5, so the mandatory block (lower + close + raise)
# lasts 1.0 s = 10 ticks at 10 Hz.
PICK_STEPS: tuple[tuple[str, bool, float], ...] = (
    ("approach", True, 0.50),
    ("lower", False, 0.40),
    ("close", False, 0.20),
    ("raise", False, 0.40),
    ("move", True, 0.50),
    ("release", True, 0.10),
)
STEP_DUR = {name: dur for name, _, dur in PICK_STEPS}
PICK_TOTAL_S = sum(STEP_DUR.values())
UNDO_S = 0.10          # each rollback motion
SAFE_STOP_S = 0.02     # on_preempted(): brake the arm, keep the grip
RETRACT_S = 0.15       # reflex retract motion

E9_STAGES = ("idle",) + tuple(name for name, _, _ in PICK_STEPS)
E10_DURATIONS = (0.0, 0.1, 0.5, 2.5)


# ── Per-trial state (the rig's "hardware") ───────────────────────────────────

@dataclass
class Trial:
    """Hardware state and observations of one trial; reset by the driver."""

    hazard: bool = False
    onset: float | None = None
    detect: float | None = None
    retract_entry: float | None = None
    retract_done: asyncio.Event = field(default_factory=asyncio.Event)
    start_pick: bool = False
    pick_started: bool = False
    pick_finished: asyncio.Event = field(default_factory=asyncio.Event)
    step_events: dict[str, asyncio.Event] = field(
        default_factory=lambda: {n: asyncio.Event() for n in STEP_DUR})
    step_starts: list[tuple[str, float]] = field(default_factory=list)
    current_step: str | None = None
    fate: str = "none"
    rolled_back: bool = False
    stopped_at: str | None = None
    preempted_hook: bool = False
    safe_stop_ms: float | None = None
    # E10 conveyor
    conveyor_d: float = 0.0
    conveyor_idle: bool = True
    conveyor_started: asyncio.Event = field(default_factory=asyncio.Event)
    conveyor_runs: list[tuple[float, float]] = field(default_factory=list)
    tick_times: list[float] = field(default_factory=list)


TRIAL = Trial()


class Arm:
    """Instrumented actuator: logs motions, overlapping writers and drops."""

    def __init__(self) -> None:
        self.state: dict[str, Any] = {
            "z": "up", "gripper": "open", "holding": False, "pose": "home"}
        self.active: dict[str, str] = {}
        self.overlaps = 0
        self.dropped = False

    async def command(self, writer: str, cmd: str, duration: float,
                      **final: Any) -> None:
        key = f"{writer}:{cmd}"
        if self.active:
            self.overlaps += 1
        self.active[key] = cmd
        try:
            if duration > 0:
                await asyncio.sleep(duration)
        finally:
            self.active.pop(key, None)
        if (final.get("gripper") == "open" and self.state["holding"]
                and self.state["z"] == "up"
                and self.state["pose"] != "over_target"):
            self.dropped = True  # released mid-air, away from both sites
        self.state.update(final)


ARM = Arm()


# ── Nodes ────────────────────────────────────────────────────────────────────

class HazardSense(BaseSenseNode):
    """Proximity sensor over the latched collision flag."""

    node_id = "HazardSense"
    signal_kind = "proximity"
    poll_interval_s = 0.0

    def hazard_signal(self) -> Signal:
        return Signal(source=self.node_id, kind=self.signal_kind, value=0.01,
                      confidence=1.0, timestamp=clock())

    async def read(self) -> Signal:
        if TRIAL.hazard:
            if TRIAL.detect is None:
                TRIAL.detect = clock()
            return self.hazard_signal()
        return Signal(source=self.node_id, kind=self.signal_kind, value=1.0,
                      confidence=1.0, timestamp=clock())


class CollisionReflex(BaseReflexInstinctNode):
    node_id = "CollisionReflex"
    priority = 250

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if any(s.kind == HazardSense.signal_kind and s.value < 0.05
               for s in ctx.signals):
            return Proposal(instinct_id=self.node_id,
                            action_id=Retract.node_id,
                            priority=self.priority, urgency=1.0)
        return None


class StartPick(BaseInstinctNode):
    """Proposes the pick once, when the driver asks for it."""

    node_id = "StartPick"
    priority = 80

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if not TRIAL.start_pick or TRIAL.pick_started:
            return None
        TRIAL.pick_started = True
        return Proposal(instinct_id=self.node_id, action_id=PickPlace.node_id,
                        priority=self.priority, urgency=0.5)


class RunConveyor(BaseInstinctNode):
    """Re-proposes the conveyor whenever it is idle."""

    node_id = "RunConveyor"
    priority = 60

    async def evaluate(self, ctx: Context) -> Proposal | None:
        if not TRIAL.conveyor_idle:
            return None
        TRIAL.conveyor_idle = False
        return Proposal(instinct_id=self.node_id, action_id=Conveyor.node_id,
                        priority=self.priority, urgency=0.5)


_FINAL_STATE: dict[str, dict[str, Any]] = {
    "approach": {"pose": "over_object"},
    "lower": {"z": "down"},
    "close": {"gripper": "closed", "holding": True},
    "raise": {"z": "up"},
    "move": {"pose": "over_target"},
    "release": {"gripper": "open", "holding": False},
}


class PickPlace(MultiStepActionNode):
    node_id = "PickPlace"
    timeout_s = 10.0
    interrupt_policy = InterruptPolicy.ROLLBACK
    resources = frozenset({"arm", "gripper"})

    def steps(self) -> list[ActionStep]:
        undo = {"lower": self._undo_lower, "close": self._undo_close,
                "raise": self._undo_raise}
        return [
            ActionStep(name, interruptible=intr, rollback=undo.get(name),
                       timeout_s=2.0)
            for name, intr, _ in PICK_STEPS
        ]

    async def execute(self, proposal: Proposal) -> Result:
        try:
            result = await super().execute(proposal)
        except asyncio.CancelledError:
            # on_preempted() runs after the cancel and upgrades this.
            TRIAL.fate = "cancelled"
            TRIAL.stopped_at = TRIAL.current_step
            TRIAL.pick_finished.set()
            raise
        if getattr(result, "preempted", False) or TRIAL.preempted_hook:
            TRIAL.fate = "preempted"
        elif result.interrupted:
            TRIAL.fate = "interrupted"
        else:
            TRIAL.fate = "completed" if result.success else "failed"
        TRIAL.rolled_back = TRIAL.rolled_back or result.rolled_back
        TRIAL.stopped_at = result.stopped_at_step
        TRIAL.pick_finished.set()
        return result

    async def execute_step(self, step: ActionStep, proposal: Proposal,
                           completed: list[StepResult]) -> StepResult:
        TRIAL.current_step = step.name
        TRIAL.step_starts.append((step.name, clock()))
        TRIAL.step_events[step.name].set()
        await ARM.command(self.node_id, step.name, STEP_DUR[step.name],
                          **_FINAL_STATE[step.name])
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal: Proposal,
                           preempted_by: Proposal) -> None:
        TRIAL.preempted_hook = True
        TRIAL.fate = "preempted"
        t0 = clock()
        await ARM.command(self.node_id, "hold", SAFE_STOP_S)  # keep the grip
        TRIAL.safe_stop_ms = (clock() - t0) * 1_000.0

    async def _undo_lower(self) -> None:
        TRIAL.rolled_back = True
        await ARM.command(self.node_id, "undo_lower", UNDO_S, z="up")

    async def _undo_close(self) -> None:
        TRIAL.rolled_back = True
        await ARM.command(self.node_id, "undo_close", UNDO_S,
                          gripper="open", holding=False)

    async def _undo_raise(self) -> None:
        TRIAL.rolled_back = True
        await ARM.command(self.node_id, "undo_raise", UNDO_S, z="down")


class Retract(BaseActionNode):
    """Reflex response: pull the arm back home, keeping any grip."""

    node_id = "Retract"
    timeout_s = 2.0
    resources = frozenset({"arm"})

    async def execute(self, proposal: Proposal) -> Result:
        if TRIAL.retract_entry is None:
            TRIAL.retract_entry = clock()
        await ARM.command(self.node_id, "retract", RETRACT_S,
                          z="up", pose="home")
        TRIAL.hazard = False
        TRIAL.retract_done.set()
        return Result(action_id=self.node_id, success=True)


class Conveyor(BaseActionNode):
    """Long-running action on a resource the reflex does not need."""

    node_id = "Conveyor"
    timeout_s = 10.0
    resources = frozenset({"conveyor"})

    async def execute(self, proposal: Proposal) -> Result:
        start = clock()
        TRIAL.conveyor_started.set()
        try:
            if TRIAL.conveyor_d > 0:
                await asyncio.sleep(TRIAL.conveyor_d)
        finally:
            TRIAL.conveyor_runs.append((start, clock()))
            TRIAL.conveyor_idle = True
        return Result(action_id=self.node_id, success=True)


# ── Rig construction ─────────────────────────────────────────────────────────

def available_arms(experiment: str) -> list[str]:
    if not HAS_ARBITRATION:
        return ["blocking"]
    return ["preempt", "wait", "event"] if experiment == "e9" else ["tick", "event"]


def build(arm: str, tick_hz: float, deliberative: list[type[BaseInstinctNode]],
          actions: list[type[BaseActionNode]],
          ) -> tuple[ArachniteRuntime, HazardSense]:
    """Fresh runtime for ``arm`` with the rig's nodes registered."""
    CollisionReflex.trigger_on_signals = (
        [HazardSense.signal_kind] if arm == "event" else None)
    if HAS_ARBITRATION:
        CollisionReflex.preemption = (
            ReflexPreemption.WAIT_FOR_BLOCK if arm == "wait"
            else ReflexPreemption.PREEMPT)

    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am = ActionMasterNode(bus=bus)
    sense = HazardSense(bus=bus)
    sm.register(sense)
    im.register(CollisionReflex(bus=bus))
    for instinct in deliberative:
        im.register(instinct(bus=bus))
    am.register(Retract(bus=bus))
    for action in actions:
        am.register(action(bus=bus))

    kwargs: dict[str, Any] = {}
    if arm == "event":
        kwargs["reflex_mode"] = "event"
    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(), instinct_master=im,
        decision_master=dm, action_master=am, bus=bus, tick_rate_hz=tick_hz,
        context_observers=[lambda _ctx: TRIAL.tick_times.append(clock())],
        **kwargs,
    )
    return rt, sense


async def inject(arm: str, sense: HazardSense) -> float:
    """Raise the latched collision flag; an event-driven sensor pushes it."""
    TRIAL.hazard = True
    TRIAL.onset = onset = clock()
    if arm == "event":
        TRIAL.detect = onset
        await sense.push(sense.hazard_signal())
    return onset


def reset_trial() -> None:
    global TRIAL, ARM
    TRIAL = Trial()
    ARM = Arm()


def _ms(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else (a - b) * 1_000.0


# ── E9 ───────────────────────────────────────────────────────────────────────

async def e9_trial(arm: str, stage: str, rng: random.Random,
                   tick_hz: float) -> dict[str, Any]:
    reset_trial()
    period = 1.0 / tick_hz
    rt, sense = build(arm, tick_hz, [StartPick], [PickPlace])
    missed = False
    offset: float | None = None
    onset: float | None = None
    await rt.start()
    try:
        if stage == "idle":
            await asyncio.sleep(period * (2.0 + rng.random()))
        else:
            TRIAL.start_pick = True
            await asyncio.wait_for(TRIAL.step_events[stage].wait(),
                                   timeout=PICK_TOTAL_S + 2.0)
            offset = STEP_DUR[stage] * (0.1 + 0.8 * rng.random())
            await asyncio.sleep(offset)
        onset = await inject(arm, sense)
        await asyncio.wait_for(TRIAL.retract_done.wait(),
                               timeout=PICK_TOTAL_S + 2.0)
        if stage != "idle":
            await asyncio.wait_for(TRIAL.pick_finished.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        missed = True
    finally:
        await rt.stop()

    steps_after = [n for n, t in TRIAL.step_starts
                   if onset is not None and t > onset]
    return {
        "arm": arm, "stage": stage,
        "offset_in_stage_s": offset,
        "latency_ms": _ms(TRIAL.retract_entry, onset),
        "detect_ms": _ms(TRIAL.detect, onset),
        "fate": TRIAL.fate if stage != "idle" else "none",
        "rolled_back": TRIAL.rolled_back,
        "stopped_at": TRIAL.stopped_at,
        "steps_begun_after_onset": steps_after,
        "safe_stop_ms": TRIAL.safe_stop_ms,
        "overlaps": ARM.overlaps,
        "dropped": ARM.dropped,
        "final_state": dict(ARM.state),
        "missed": missed,
    }


async def run_e9(arms: list[str], stages: list[str], trials: int,
                 tick_hz: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for arm in arms:
        for s_idx, stage in enumerate(stages):
            rng = random.Random(_SEED + 1000 * s_idx)  # same onsets per arm
            t0 = clock()
            cell = [await e9_trial(arm, stage, rng, tick_hz)
                    for _ in range(trials)]
            for i, row in enumerate(cell):
                row["trial"] = i
            rows.extend(cell)
            lat = [r["latency_ms"] for r in cell if r["latency_ms"] is not None]
            fates = _count(r["fate"] for r in cell)
            print(f"  {arm:<9}{stage:<10} n={len(lat):>4}  "
                  f"median={_fmt(_median(lat))}  p99={_fmt(_pct(lat, 99))}  "
                  f"max={_fmt(max(lat) if lat else None)} ms  fates={fates}  "
                  f"overlaps={sum(r['overlaps'] for r in cell)}  "
                  f"dropped={sum(r['dropped'] for r in cell)}  "
                  f"missed={sum(r['missed'] for r in cell)}  "
                  f"({clock() - t0:.0f} s)", flush=True)
    return rows


# ── E10 ──────────────────────────────────────────────────────────────────────

async def e10_cell(arm: str, duration: float, trials: int, rng: random.Random,
                   tick_hz: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    reset_trial()
    period = 1.0 / tick_hz
    TRIAL.conveyor_d = duration
    rt, sense = build(arm, tick_hz, [RunConveyor], [Conveyor])
    rows: list[dict[str, Any]] = []
    await rt.start()
    t_start = clock()
    try:
        await asyncio.sleep(period * 3)
        for i in range(trials):
            missed = False
            offset: float | None = None
            if duration > 0:
                # Onset at a uniformly random point inside a running action.
                TRIAL.conveyor_started.clear()
                try:
                    await asyncio.wait_for(TRIAL.conveyor_started.wait(),
                                           timeout=duration + 2.0)
                except asyncio.TimeoutError:
                    missed = True
                offset = duration * (0.05 + 0.9 * rng.random())
                await asyncio.sleep(offset)
            else:
                await asyncio.sleep(period * rng.random())
            TRIAL.retract_entry = None
            TRIAL.detect = None
            TRIAL.retract_done.clear()
            busy = not TRIAL.conveyor_idle
            onset = await inject(arm, sense)
            try:
                await asyncio.wait_for(TRIAL.retract_done.wait(),
                                       timeout=duration + 2.0)
            except asyncio.TimeoutError:
                missed = True
                TRIAL.hazard = False
            rows.append({
                "arm": arm, "duration_s": duration, "trial": i,
                "offset_in_action_s": offset,
                "conveyor_busy_at_onset": busy,
                "latency_ms": _ms(TRIAL.retract_entry, onset),
                "detect_ms": _ms(TRIAL.detect, onset),
                "missed": missed,
            })
            await asyncio.sleep(period)
    finally:
        t_end = clock()
        await rt.stop()

    ticks = [t for t in TRIAL.tick_times if t_start <= t <= t_end]
    intervals = [(b - a) * 1_000.0 for a, b in zip(ticks, ticks[1:], strict=False)]
    period_ms = period * 1_000.0
    ticks_during = [sum(1 for t in ticks if s < t < e)
                    for s, e in TRIAL.conveyor_runs if e - s >= period]
    pacing = {
        "arm": arm, "duration_s": duration,
        "ticks": len(ticks),
        "tick_interval_median_ms": _median(intervals),
        "tick_interval_max_ms": max(intervals) if intervals else None,
        "overruns": sum(1 for x in intervals if x > 1.2 * period_ms),
        "conveyor_runs": len(TRIAL.conveyor_runs),
        "ticks_during_action_median": _median([float(x) for x in ticks_during]),
        "ticks_during_action_expected": duration / period,
    }
    return rows, pacing


async def run_e10(arms: list[str], durations: list[float], trials: int,
                  tick_hz: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    pacing: list[dict[str, Any]] = []
    for arm in arms:
        for d_idx, duration in enumerate(durations):
            rng = random.Random(_SEED + 1000 * d_idx)
            t0 = clock()
            cell, pace = await e10_cell(arm, duration, trials, rng, tick_hz)
            rows.extend(cell)
            pacing.append(pace)
            lat = [r["latency_ms"] for r in cell if r["latency_ms"] is not None]
            print(f"  {arm:<9}D={duration:<5g} n={len(lat):>4}  "
                  f"median={_fmt(_median(lat))}  p99={_fmt(_pct(lat, 99))}  "
                  f"max={_fmt(max(lat) if lat else None)} ms  "
                  f"tick median={_fmt(pace['tick_interval_median_ms'])} ms  "
                  f"overruns={pace['overruns']}  "
                  f"ticks/action={_fmt(pace['ticks_during_action_median'])}  "
                  f"missed={sum(r['missed'] for r in cell)}  "
                  f"({clock() - t0:.0f} s)", flush=True)
    return rows, pacing


# ── Summaries ────────────────────────────────────────────────────────────────

def _median(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


def _pct(xs: list[float], p: int) -> float | None:
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    return statistics.quantiles(xs, n=100, method="inclusive")[p - 1]


def _fmt(x: float | None) -> str:
    return "-" if x is None else f"{x:.1f}"


def _count(items: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for item in items:
        out[str(item)] = out.get(str(item), 0) + 1
    return out


def summarise(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    cells: dict[tuple[str, Any], list[dict[str, Any]]] = {}
    for row in rows:
        cells.setdefault((row["arm"], row[key]), []).append(row)
    out = []
    for (arm, k), cell in cells.items():
        lat = [r["latency_ms"] for r in cell if r["latency_ms"] is not None]
        det = [r["detect_ms"] for r in cell if r.get("detect_ms") is not None]
        entry: dict[str, Any] = {
            "arm": arm, key: k, "n": len(cell),
            "missed": sum(r["missed"] for r in cell),
            "latency_ms": {
                "median": _median(lat), "p95": _pct(lat, 95),
                "p99": _pct(lat, 99), "max": max(lat) if lat else None,
                "mean": statistics.fmean(lat) if lat else None,
            },
            "detect_ms_median": _median(det),
        }
        if "fate" in cell[0]:
            ss = [r["safe_stop_ms"] for r in cell if r["safe_stop_ms"] is not None]
            entry.update({
                "fates": _count(r["fate"] for r in cell),
                "rolled_back": sum(r["rolled_back"] for r in cell),
                "stopped_at": _count(r["stopped_at"] for r in cell),
                "steps_begun_after_onset_median": _median(
                    [float(len(r["steps_begun_after_onset"])) for r in cell]),
                "safe_stop_ms_median": _median(ss),
                "overlaps": sum(r["overlaps"] for r in cell),
                "dropped": sum(r["dropped"] for r in cell),
                "final_holding": sum(bool(r["final_state"]["holding"]) for r in cell),
            })
        out.append(entry)
    return out


def git_commit() -> str | None:
    """Commit of the arachnite checkout under test, if it is a git tree."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(arachnite.__file__).resolve().parent,
            capture_output=True, text=True, timeout=5, check=True)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reflex response while an action is running (E9, E10).")
    parser.add_argument("experiment", choices=("e9", "e10"))
    parser.add_argument("--trials", "-t", type=int, default=_TRIALS,
                        help=f"Trials per cell (default: {_TRIALS}).")
    parser.add_argument("--tick-rate", type=float, default=_TICK_RATE_HZ,
                        help=f"Tick rate in Hz (default: {_TICK_RATE_HZ:g}).")
    parser.add_argument("--arms", type=str, default="auto",
                        help="Comma-separated arms, or 'auto' for every arm "
                             "the installed arachnite supports.")
    parser.add_argument("--stages", type=str, default=",".join(E9_STAGES),
                        help="E9 stages (comma-separated).")
    parser.add_argument("--durations", type=str,
                        default=",".join(f"{d:g}" for d in E10_DURATIONS),
                        help="E10 action durations in seconds (comma-separated).")
    parser.add_argument("--label", type=str, default="",
                        help="Host label for the output file name.")
    parser.add_argument("--output-dir", "-o", type=str,
                        default="benchmarks/results")
    args = parser.parse_args()

    logging.disable(logging.CRITICAL)
    supported = available_arms(args.experiment)
    arms = supported if args.arms == "auto" else args.arms.split(",")
    unknown = [a for a in arms if a not in supported]
    if unknown:
        parser.error(f"arms {unknown} not available with arachnite "
                     f"{arachnite.__version__}; supported: {supported}")

    version = arachnite.__version__
    commit = git_commit()
    print(f"Arachnite reflex arbitration - {args.experiment.upper()}  "
          f"(arachnite {version} @ {commit}, {DISPATCH} dispatch, "
          f"CPython {platform.python_version()}, {sys.platform})")
    print(f"Arms: {arms}  |  trials/cell: {args.trials}  |  "
          f"tick: {args.tick_rate:g} Hz")
    print("-" * 78)

    t0 = clock()
    payload: dict[str, Any] = {
        "benchmark": f"reflex_arbitration_{args.experiment}",
        "arachnite_version": version,
        "arachnite_commit": commit,
        "dispatch": DISPATCH,
        "python": platform.python_version(),
        "platform": f"{sys.platform} / {platform.machine()} / {platform.processor()}",
        "host_label": args.label,
        "tick_rate_hz": args.tick_rate,
        "trials_per_cell": args.trials,
        "seed": _SEED,
        "arms": arms,
    }
    if args.experiment == "e9":
        stages = args.stages.split(",")
        rows = asyncio.run(run_e9(arms, stages, args.trials, args.tick_rate))
        payload.update({
            "pick_steps": [{"name": n, "interruptible": i, "duration_s": d}
                           for n, i, d in PICK_STEPS],
            "undo_s": UNDO_S, "safe_stop_s": SAFE_STOP_S, "retract_s": RETRACT_S,
            "summary": summarise(rows, "stage"),
            "trials": rows,
        })
    else:
        durations = [float(x) for x in args.durations.split(",")]
        rows, pacing = asyncio.run(
            run_e10(arms, durations, args.trials, args.tick_rate))
        payload.update({
            "retract_s": RETRACT_S,
            "summary": summarise(rows, "duration_s"),
            "pacing": pacing,
            "trials": rows,
        })
    payload["wall_s"] = clock() - t0

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    label = f"_{args.label}" if args.label else ""
    out = out_dir / (f"reflex_arbitration_{args.experiment}{label}"
                     f"_{DISPATCH}_{stamp}.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1)
    print(f"\nDone in {payload['wall_s'] / 60:.1f} min. Results saved to {out}")


if __name__ == "__main__":
    main()
