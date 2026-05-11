# ROS 2 baseline

Two files in this directory:

| File | Role |
|---|---|
| `robot_arm.py` | **Runnable** ROS 2 + py_trees benchmark — exports `run_benchmark(n_ticks, warmup) -> BenchmarkResult` |
| `robot_arm_bt.py` | Architectural reference — Nav2-style BT XML + node sketches; not executable, kept for the LoC table and the `0.16 ms safety cost` discussion in §8.4 |

## Install

1. Install ROS 2. Humble (LTS) is the supported target. <https://docs.ros.org/en/humble/Installation.html>
2. Source the setup file in the shell that will run the comparison:
   - Linux: `source /opt/ros/humble/setup.bash`
   - Windows: `call C:\dev\ros2_humble\local_setup.bat` from cmd, or use the ROS 2 Cmd Prompt shortcut
3. `pip install py-trees` (the BT framework — same dep as the py_trees baseline).
4. `pip install psutil` (recommended; needed for the `memory_mb` column).

## Run

```bash
# Just the ROS 2 baseline:
python -m baselines.ros2.robot_arm

# The full cross-framework comparison:
python -m baselines.compare --runs 30 --ticks 10000
```

`baselines/compare.py` calls `ros2_available()` first and skips the ROS 2
benchmark with an explanatory message if `rclpy` or `py_trees` isn't
importable.

## Tick definition

For each measured tick the harness runs:

```python
sensor.publish()                            # DDS publish
t0 = perf_counter()
executor.spin_once(timeout_sec=0)           # drain pending DDS callbacks
tree.tick()                                 # py_trees BT traversal
tick_ms = (perf_counter() - t0) * 1000
```

The window deliberately includes `spin_once` because that is the cost a
real ROS 2 robot pays per control cycle that the pure py_trees baseline
does not. Without it, the ROS 2 number would be indistinguishable from
plain py_trees and the comparison would say nothing about ROS 2's
deployment cost.

## Notes for the paper

The runnable ROS 2 baseline is single-process. Multi-process DDS would
add inter-process IPC latency on top of the in-process spin_once cost
captured here — that scenario is what `robot_arm_bt.py` documents but
isn't measured. If a reviewer asks for the multi-process number, the
honest framing is: "in-process is a lower bound for ROS 2 overhead;
multi-process adds DDS network-stack cost that varies with the chosen RMW."
