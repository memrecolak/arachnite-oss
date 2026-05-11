"""
baselines/ros2/robot_arm.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Runnable ROS 2 baseline for the cross-framework comparison.

Same scenario as ``baselines/py_trees/robot_arm.py`` (so the Arachnite vs
ROS 2 column in §8.4 is apples-to-apples), but the per-tick measurement
also pays the DDS callback-dispatch cost. Concretely, each measured tick
runs:

    executor.spin_once(timeout_sec=0)   # drain pending DDS callbacks
    tree.tick()                         # py_trees BT traversal

so the latency captures the BT cost plus whatever the rclpy executor adds
for that tick. This is the cost a real ROS 2 robot pays even when the BT
itself is identical.

Architectural notes (also in ``robot_arm_bt.py``):
  - No reflex arc: collision check is a tree branch, dispatched by DDS.
  - No mandatory completion blocks: cancel_goal is the only interrupt.
  - No rollback: cancelled actions have no structured undo.

Requirements:
  - rclpy (i.e. a sourced ROS 2 distro on the environment)
  - py_trees
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from baselines.shared_sim import BenchmarkResult


def ros2_available() -> tuple[bool, str]:
    """Return (ok, reason). Mirrors jason_available() shape."""
    try:
        import rclpy  # noqa: F401
    except ImportError as exc:
        return False, f"rclpy not importable ({exc})"
    try:
        import py_trees  # noqa: F401
    except ImportError as exc:
        return False, f"py_trees not importable ({exc})"
    return True, ""


def run_benchmark(
    n_ticks: int = 10_000,
    warmup: int = 1_000,
    inject_collision_at: int | None = None,
) -> BenchmarkResult:
    """Run the ROS 2 + py_trees pick-and-place benchmark."""
    ok, why = ros2_available()
    if not ok:
        raise RuntimeError(f"ROS 2 baseline not available: {why}")

    import py_trees
    import rclpy
    from rclpy.executors import SingleThreadedExecutor
    from rclpy.node import Node
    from std_msgs.msg import Bool, Float32

    # Reset the SIM that the py_trees behaviours mutate. They import
    # baselines.py_trees.robot_arm.SIM by closure, so we have to rebind it
    # there (same pattern as run_benchmark in that module).
    import baselines.py_trees.robot_arm as pt_mod
    from baselines.py_trees.robot_arm import (
        CheckCollision,
        CheckObjectDetected,
        CloseGripper,
        EmergencyRetract,
        LowerGripper,
        MoveToObject,
        MoveToTarget,
        RaiseGripper,
        ReleaseGripper,
    )
    from baselines.shared_sim import ArmState, BenchmarkResult
    pt_mod.SIM = ArmState()

    rclpy.init()
    try:
        sim_ref = pt_mod.SIM

        class SensorBridge(Node):
            """Publishes the simulator's state at every tick.

            Real ROS 2 deployments have one publisher per sensor; we
            collapse that here because the latency we measure is the
            executor dispatch cost per tick, not the sensor count.
            """

            def __init__(self) -> None:
                super().__init__("sensor_bridge")
                self.dist_pub = self.create_publisher(Float32, "/object_distance", 10)
                self.col_pub = self.create_publisher(Bool, "/collision_imminent", 10)

            def publish(self) -> None:
                d = Float32()
                d.data = sim_ref.update_distance()
                self.dist_pub.publish(d)
                c = Bool()
                c.data = sim_ref.collision_imminent or sim_ref.object_distance < 0.05
                self.col_pub.publish(c)

        class CollisionMirror(Node):
            """Subscribes to /collision_imminent — adds one DDS hop per tick.

            The data is unused by the BT (which reads SIM directly via
            CheckCollision); the subscriber exists so the executor has
            real callbacks to dispatch on every spin_once.
            """

            def __init__(self) -> None:
                super().__init__("collision_mirror")
                self._sub = self.create_subscription(
                    Bool, "/collision_imminent", self._cb, 10,
                )
                self.last: bool = False

            def _cb(self, msg: Bool) -> None:
                self.last = bool(msg.data)

        sensor = SensorBridge()
        mirror = CollisionMirror()

        executor = SingleThreadedExecutor()
        executor.add_node(sensor)
        executor.add_node(mirror)

        # Build the same tree structure as the py_trees baseline.
        collision_guard = py_trees.composites.Sequence(
            name="CollisionGuard",
            memory=False,
            children=[CheckCollision(), EmergencyRetract()],
        )
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
        tree = py_trees.trees.BehaviourTree(root=root)
        tree.setup()

        result = BenchmarkResult(framework="ROS 2 BT")

        # Find the EmergencyRetract node for safety-response timing.
        retract_node = None
        for node in tree.root.iterate():
            if isinstance(node, EmergencyRetract):
                retract_node = node

        # Warm-up.
        for _ in range(warmup):
            sensor.publish()
            executor.spin_once(timeout_sec=0)
            tree.tick()

        pt_mod.SIM = ArmState()
        sim_ref = pt_mod.SIM  # rebind for closures

        # Measurement.
        for tick in range(n_ticks):
            if inject_collision_at is not None and tick == inject_collision_at:
                sim_ref.collision_imminent = True

            sensor.publish()

            t0 = time.perf_counter()
            executor.spin_once(timeout_sec=0)
            tree.tick()
            tick_ms = (time.perf_counter() - t0) * 1_000
            result.tick_latencies_ms.append(tick_ms)

            if (
                retract_node is not None
                and retract_node.exec_time > 0
                and sim_ref.emergency_count > result.emergencies_handled
            ):
                # No reflex bypass exists in ROS 2: the safety response
                # rides the same executor, so its latency is the same
                # tick latency we just measured.
                result.safety_response_us.append(tick_ms * 1_000)
                result.emergencies_handled = sim_ref.emergency_count

        result.picks_completed = sim_ref.pick_count
        result.pick_durations_ms = list(sim_ref.pick_durations_ms)
        result.total_ticks = n_ticks

        try:
            import psutil
            result.memory_rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
        except ImportError:
            result.memory_rss_mb = 0.0

        tree.shutdown()
        executor.remove_node(sensor)
        executor.remove_node(mirror)
        sensor.destroy_node()
        mirror.destroy_node()
        return result
    finally:
        rclpy.shutdown()


if __name__ == "__main__":
    import statistics
    import sys

    ok, why = ros2_available()
    if not ok:
        print(f"ROS 2 not available: {why}", file=sys.stderr)
        sys.exit(2)

    print("ROS 2 BT pick-and-place benchmark")
    print("-" * 50)
    r = run_benchmark(n_ticks=10_000, warmup=1_000, inject_collision_at=5_000)
    s = sorted(r.tick_latencies_ms)
    n = len(s)
    print("Framework  : ROS 2 BT")
    print(f"Ticks      : {r.total_ticks}")
    print(f"Picks      : {r.picks_completed}")
    print(f"Emergencies: {r.emergencies_handled}")
    print("Tick latency (ms):")
    print(f"  Mean    : {statistics.mean(s):7.3f}")
    print(f"  Median  : {statistics.median(s):7.3f}")
    print(f"  P95     : {s[int(n * 0.95)]:7.3f}")
    print(f"  P99     : {s[int(n * 0.99)]:7.3f}")
    print(f"  Std Dev : {statistics.stdev(s):7.3f}")
    print(f"Memory     : {r.memory_rss_mb:.1f} MB")
