# Baselines — cross-framework comparison artifacts

**These files are not part of the Arachnite framework.** They are
comparison harnesses used to benchmark Arachnite against other reactive
agent / behaviour-tree frameworks for the project's paper and evaluation
write-up.

`pip install arachnite` does **not** include this directory — it is
excluded from the wheel and the source distribution. The framework
itself has no runtime dependency on anything in here.

## What's here

| Subdirectory | Framework | Language | External requirement |
|---|---|---|---|
| `py_trees/` | [py_trees](https://github.com/splintered-reality/py_trees) behaviour trees | Python | `pip install py-trees` |
| `ros2/`     | [ROS 2](https://docs.ros.org/) + py_trees | Python | ROS 2 Humble + `rclpy` sourced into the shell |
| `jason/`    | [Jason](https://jason-lang.github.io/) AgentSpeak BDI | Java + Python harness | JDK 11+, Jason 3.x, `JASON_HOME` |

Top-level files:

- `compare.py` — orchestrator that runs each available baseline plus
  Arachnite on the shared pick-and-place workload and emits a JSON report.
  Skips any baseline whose external dependency is missing, with an
  explanatory message.
- `shared_sim.py` — `ArmState` physics stub shared by all baselines so
  every framework drives the same simulated environment.

## Running

The orchestrator is a Python module:

```bash
# Full comparison — skips baselines whose deps aren't installed
python -m baselines.compare --runs 30 --ticks 10000

# A single baseline:
python -m baselines.py_trees.robot_arm
python -m baselines.ros2.robot_arm
python -m baselines.jason.run_jason
```

Per-baseline setup notes live in each subdirectory's `README.md`
(`baselines/jason/README.md`, `baselines/ros2/README.md`).

## Licensing

These harnesses are MIT-licensed alongside the rest of the repository,
but they invoke or compile against third-party frameworks that carry
their own licenses (LGPL for Jason, BSD for py_trees, Apache-2.0 for ROS
2). When redistributing modified versions of those frameworks' source
files, follow the upstream project's terms.
