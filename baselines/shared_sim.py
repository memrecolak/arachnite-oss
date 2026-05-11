"""
baselines/shared_sim.py
~~~~~~~~~~~~~~~~~~~~~~~
Shared physics stub and timing instrumentation for all baseline
implementations. Ensures identical simulation behaviour across frameworks.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

# When True, ``ArmState.update_distance`` pins the (non-collision, non-holding)
# distance at 0.20 m so the grasp window (0.10–0.35 m) is satisfied every tick.
# Mirrors ``examples.robot_arm.nodes.BENCHMARK_MODE`` and Jason's
# ``ArmState.java::updateDistance``: at tick rates of 10 kHz the wall-clock
# conveyor model (1.5 → 0.10 m over ~20 s) never reaches the grasp window
# inside a 10 000-tick run, so the BT/executor short-circuits on
# ``CheckObjectDetected`` and the pick path never executes — making the
# Arachnite-vs-baseline latency comparison apples-to-oranges. Set this to True
# from the benchmark harness so all four frameworks exercise the same path.
# See docs/audits/2026-05-04-architecture-and-fairness-audit.md, finding #1.
BENCHMARK_MODE: bool = False


@dataclass
class ArmState:
    """Deterministic physics stub shared across all baselines."""
    joints:              list[float] = field(default_factory=lambda: [0.0] * 6)
    gripper:             float       = 0.0
    object_distance:     float       = 1.5
    holding:             bool        = False
    collision_imminent:  bool        = False
    start_time:          float       = field(default_factory=time.monotonic)
    pick_count:          int         = 0
    emergency_count:     int         = 0
    # Per-pick wall-clock instrumentation. ``pick_start_ns`` is set the
    # first time the object enters the grasp window with ``holding=False``
    # and no pick already in flight; ``pick_complete`` records the elapsed
    # ms in ``pick_durations_ms`` and resets ``pick_start_ns`` to -1. This
    # gives a per-framework distribution of "object-detected → object-
    # released" wall times that *is* directly comparable across frameworks
    # (unlike per-tick latency, which counts a different unit of work per
    # framework — see audit 2026-05-04 #1 follow-up).
    pick_start_ns:       int         = -1
    pick_durations_ms:   list[float] = field(default_factory=list)

    def update_distance(self) -> float:
        """Simulate conveyor drift: 1.5 m -> 0.10 m over ~20 s.

        In ``BENCHMARK_MODE`` the (non-collision, non-holding) distance is
        pinned at 0.20 m so picks fire on every cycle.

        Side effect: starts the per-pick stopwatch the first time the
        object becomes visibly graspable.
        """
        if self.collision_imminent:
            self.object_distance = 0.02
        elif self.holding:
            self.object_distance = 1.5
        elif BENCHMARK_MODE:
            self.object_distance = 0.20
        else:
            elapsed = time.monotonic() - self.start_time
            self.object_distance = max(0.10, 1.5 - elapsed * 0.067)
        # Pick-start detection: object in grasp window, hand empty, and
        # the previous pick has been recorded (pick_start_ns < 0). The
        # 0.10–0.35 m window matches CheckObjectDetected and GraspInstinct.
        if (
            self.pick_start_ns < 0
            and not self.holding
            and 0.10 <= self.object_distance <= 0.35
        ):
            self.pick_start_ns = time.monotonic_ns()
        return self.object_distance

    def emergency_retract(self) -> None:
        self.joints = [0.0] * 6
        self.gripper = 0.0
        self.holding = False
        self.collision_imminent = False
        self.start_time = time.monotonic()
        self.emergency_count += 1
        # Discard the in-flight pick stopwatch — the pick was aborted.
        self.pick_start_ns = -1

    def pick_complete(self) -> None:
        self.gripper = 0.0
        self.holding = False
        self.pick_count += 1
        self.start_time = time.monotonic()
        if self.pick_start_ns >= 0:
            self.pick_durations_ms.append(
                (time.monotonic_ns() - self.pick_start_ns) / 1e6
            )
            self.pick_start_ns = -1


@dataclass
class BenchmarkResult:
    """Standardised result for cross-framework comparison."""
    framework: str
    tick_latencies_ms: list[float] = field(default_factory=list)
    # Per-pick wall-clock durations (object-detected → object-released).
    # Unlike ``tick_latencies_ms``, this metric measures the same unit of
    # work across all four frameworks and is the right column to compare
    # in the paper's throughput table.
    pick_durations_ms: list[float] = field(default_factory=list)
    safety_response_us: list[float] = field(default_factory=list)
    memory_rss_mb: float = 0.0
    lines_of_code: int = 0
    picks_completed: int = 0
    emergencies_handled: int = 0
    total_ticks: int = 0
