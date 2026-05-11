"""
baselines/py_trees/robot_arm.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Equivalent pick-and-place robot arm controller using py_trees.

This implements the same scenario as examples/robot_arm/ using py_trees'
behaviour tree framework. The tree structure is:

    Root (Selector)
    ├── CollisionGuard (Sequence)
    │   ├── CheckCollision (Condition)
    │   └── EmergencyRetract (Action)
    └── PickAndPlace (Sequence)
        ├── CheckObjectDetected (Condition)
        ├── MoveToObject (Action)
        ├── LowerGripper (Action)       ← no mandatory block concept
        ├── CloseGripper (Action)       ← can be interrupted between any steps
        ├── RaiseGripper (Action)
        ├── MoveToTarget (Action)
        └── ReleaseGripper (Action)

Key differences from Arachnite:
  - No reflex arc: collision handling is a high-priority branch in the tree,
    but it competes in the same tick traversal as all other behaviours.
  - No mandatory completion blocks: any step can be interrupted between ticks.
  - No rollback semantics: if interrupted mid-sequence, no undo occurs.
  - Single-process only: no distributed deployment support.

Requirements:
    pip install py-trees
"""

from __future__ import annotations

import time

import py_trees

from baselines.shared_sim import ArmState, BenchmarkResult

SIM = ArmState()


# ── Condition behaviours ─────────────────────────────────────────────────────

class CheckCollision(py_trees.behaviour.Behaviour):
    """Returns SUCCESS if collision is imminent (distance < 0.05 m)."""

    def __init__(self, name: str = "CheckCollision"):
        super().__init__(name)

    def update(self) -> py_trees.common.Status:
        SIM.update_distance()
        if SIM.collision_imminent or SIM.object_distance < 0.05:
            return py_trees.common.Status.SUCCESS
        return py_trees.common.Status.FAILURE


class CheckObjectDetected(py_trees.behaviour.Behaviour):
    """Returns SUCCESS if a graspable object is within reach."""

    def __init__(self, name: str = "CheckObjectDetected"):
        super().__init__(name)

    def update(self) -> py_trees.common.Status:
        SIM.update_distance()
        if 0.10 <= SIM.object_distance <= 0.35 and not SIM.holding:
            return py_trees.common.Status.SUCCESS
        return py_trees.common.Status.FAILURE


# ── Action behaviours ────────────────────────────────────────────────────────

class EmergencyRetract(py_trees.behaviour.Behaviour):
    """Immediately homes all joints and opens gripper."""

    def __init__(self, name: str = "EmergencyRetract"):
        super().__init__(name)
        self.exec_time: float = 0.0

    def update(self) -> py_trees.common.Status:
        self.exec_time = time.perf_counter()
        SIM.emergency_retract()
        return py_trees.common.Status.SUCCESS


class MoveToObject(py_trees.behaviour.Behaviour):
    def __init__(self, name: str = "MoveToObject"):
        super().__init__(name)

    def update(self) -> py_trees.common.Status:
        SIM.joints = [10.0, -20.0, 30.0, -10.0, 5.0, 0.0]
        return py_trees.common.Status.SUCCESS


class LowerGripper(py_trees.behaviour.Behaviour):
    def __init__(self, name: str = "LowerGripper"):
        super().__init__(name)

    def update(self) -> py_trees.common.Status:
        SIM.joints[1] -= 15.0
        return py_trees.common.Status.SUCCESS


class CloseGripper(py_trees.behaviour.Behaviour):
    def __init__(self, name: str = "CloseGripper"):
        super().__init__(name)

    def update(self) -> py_trees.common.Status:
        SIM.gripper = 1.0
        SIM.holding = True
        return py_trees.common.Status.SUCCESS


class RaiseGripper(py_trees.behaviour.Behaviour):
    def __init__(self, name: str = "RaiseGripper"):
        super().__init__(name)

    def update(self) -> py_trees.common.Status:
        SIM.joints[1] += 15.0
        return py_trees.common.Status.SUCCESS


class MoveToTarget(py_trees.behaviour.Behaviour):
    def __init__(self, name: str = "MoveToTarget"):
        super().__init__(name)

    def update(self) -> py_trees.common.Status:
        SIM.joints = [0.0] * 6
        return py_trees.common.Status.SUCCESS


class ReleaseGripper(py_trees.behaviour.Behaviour):
    def __init__(self, name: str = "ReleaseGripper"):
        super().__init__(name)

    def update(self) -> py_trees.common.Status:
        SIM.pick_complete()
        return py_trees.common.Status.SUCCESS


# ── Tree construction ────────────────────────────────────────────────────────

def create_tree() -> py_trees.trees.BehaviourTree:
    """Build the pick-and-place behaviour tree."""
    # Collision branch (higher priority via Selector ordering)
    collision_guard = py_trees.composites.Sequence(
        name="CollisionGuard",
        memory=False,
        children=[
            CheckCollision(),
            EmergencyRetract(),
        ],
    )

    # Pick-and-place branch
    pick_and_place = py_trees.composites.Sequence(
        name="PickAndPlace",
        memory=True,
        children=[
            CheckObjectDetected(),
            MoveToObject(),
            LowerGripper(),
            CloseGripper(),
            RaiseGripper(),
            MoveToTarget(),
            ReleaseGripper(),
        ],
    )

    root = py_trees.composites.Selector(
        name="Root",
        memory=False,
        children=[collision_guard, pick_and_place],
    )

    return py_trees.trees.BehaviourTree(root=root)


# ── Benchmark runner ─────────────────────────────────────────────────────────

def run_benchmark(
    n_ticks: int = 10_000,
    warmup: int = 1_000,
    inject_collision_at: int | None = None,
) -> BenchmarkResult:
    """Run the py_trees pick-and-place benchmark.

    Returns tick latencies and safety response times.
    """
    global SIM
    SIM = ArmState()

    tree = create_tree()
    tree.setup()

    result = BenchmarkResult(framework="py_trees")

    # Find the EmergencyRetract node for timing
    retract_node = None
    for node in tree.root.iterate():
        if isinstance(node, EmergencyRetract):
            retract_node = node

    # Warm-up
    for _ in range(warmup):
        tree.tick()

    SIM = ArmState()  # Reset after warmup

    # Measurement
    for tick in range(n_ticks):
        if inject_collision_at is not None and tick == inject_collision_at:
            SIM.collision_imminent = True

        t0 = time.perf_counter()
        tree.tick()
        tick_ms = (time.perf_counter() - t0) * 1_000
        result.tick_latencies_ms.append(tick_ms)

        # If collision was injected this tick and retract executed,
        # measure safety response time
        if (retract_node is not None and retract_node.exec_time > 0
                and SIM.emergency_count > result.emergencies_handled):
            # Safety response = full tick time (no bypass possible in BTs)
            result.safety_response_us.append(tick_ms * 1_000)
            result.emergencies_handled = SIM.emergency_count

    result.picks_completed = SIM.pick_count
    result.pick_durations_ms = list(SIM.pick_durations_ms)
    result.total_ticks = n_ticks

    # Memory footprint
    try:
        import psutil
        result.memory_rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
    except ImportError:
        result.memory_rss_mb = 0.0

    tree.shutdown()
    return result


if __name__ == "__main__":
    import statistics

    print("py_trees pick-and-place benchmark")
    print("-" * 50)

    r = run_benchmark(n_ticks=10_000, warmup=1_000, inject_collision_at=5_000)
    s = sorted(r.tick_latencies_ms)
    n = len(s)

    print("Framework : py_trees")
    print(f"Ticks     : {r.total_ticks}")
    print(f"Picks     : {r.picks_completed}")
    print(f"Emergencies: {r.emergencies_handled}")
    print("Tick latency:")
    print(f"  Mean    : {statistics.mean(s):7.3f} ms")
    print(f"  Median  : {statistics.median(s):7.3f} ms")
    print(f"  P95     : {s[int(n * 0.95)]:7.3f} ms")
    print(f"  P99     : {s[int(n * 0.99)]:7.3f} ms")
    print(f"  Std Dev : {statistics.stdev(s):7.3f} ms")
    print(f"Memory    : {r.memory_rss_mb:.1f} MB")
