# Jason BDI baseline

Runnable Jason 3.x baseline for the cross-framework comparison in `baselines/compare.py`.
Mirrors the existing `baselines/py_trees/robot_arm.py` structure: exports a
`run_benchmark(n_ticks, warmup) -> BenchmarkResult` from `run_jason.py`.

## Files

| File | Role |
|---|---|
| `robot_arm.asl` | AgentSpeak agent (BDI plans for pick-and-place + emergency retract) |
| `RobotArmEnv.java` | Jason environment — owns `ArmState`, emits per-cycle latency JSONL |
| `ArmState.java` | Java port of `baselines/shared_sim.py:ArmState` for parity |
| `JasonBench.java` | JVM entry point — substitutes runtime args into a temp `.mas2j` and hands off to `RunCentralisedMAS` |
| `robot_arm.mas2j` | Reference project file (not used by the harness — `JasonBench` generates a parameterised one at runtime) |
| `run_jason.py` | Python harness — subprocesses the JVM, parses JSONL latencies |
| `build.sh`, `build.ps1` | Compile the three `.java` files |

## Install

1. Install **JDK 11+**. Verify `javac -version` and `java -version`.
2. Download Jason 3.x from <https://github.com/jason-lang/jason/releases>
   (pick the `jason-bin-3.x.zip` archive).
3. Unzip somewhere stable, e.g. `~/jason-3.4.0`.
4. Set `JASON_HOME`:
   - bash: `export JASON_HOME=~/jason-3.4.0`
   - PowerShell: `$env:JASON_HOME = "$HOME\jason-3.4.0"`

## Build

```bash
bash baselines/jason/build.sh
# or on Windows:
pwsh baselines/jason/build.ps1
```

This writes `.class` files into `baselines/jason/build/`.

## Run

The Python harness drives everything:

```bash
python -m baselines.jason.run_jason
```

Or as part of the full cross-framework comparison:

```bash
python -m baselines.compare --runs 30 --ticks 10000
```

`baselines/compare.py` calls `jason_available()` first and skips the Jason
benchmark with an explanatory message if `JASON_HOME`, `java`, or the
compiled classes are missing — exactly the same skip pattern as the
existing py_trees baseline.

## Tick-equivalent definition

Jason has no fixed tick rate. In the basic centralised infrastructure each
reasoning cycle produces at most one external action via `executeAction`.
`RobotArmEnv` records wall-clock between two consecutive `executeAction`
calls, which is the per-cycle latency for any cycle that fires a plan.
Cycles that do nothing are elided by Jason itself and don't enter the
measurement — which is the right semantics for a "framework overhead per
work unit" comparison.

