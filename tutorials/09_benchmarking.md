# Lesson 9 — Benchmarking Your Agent

You've built agents, used reflexes, wired up the tick loop, and even built a
smart lamp. Now let's answer a practical question: **how fast is it?**

Arachnite ships with a complete benchmark suite that measures framework
overhead so you can plan your tick rate, estimate headroom, and compare
performance across devices.

## Why benchmark?

When you deploy an agent on real hardware, you need to know:

- **How much time does the framework itself use per tick?** This is time your
  node logic *can't* use. If you set `tick_rate_hz=10` (100 ms budget) and
  the framework overhead is 2 ms, you have 98 ms left for your sense reads,
  instinct evaluations, and actions.
- **How fast is the reflex path?** If a collision sensor fires, how many
  microseconds until the emergency stop action begins?
- **How much memory does my agent use?** On a Jetson Nano with 4 GB RAM,
  every megabyte counts.
- **Does performance degrade as I add more nodes?** Going from 3 nodes to 30
  should be predictable, not surprising.

## The six benchmarks

Arachnite provides six benchmarks, each measuring a different aspect:

| Benchmark | What it measures | Unit |
|-----------|-----------------|------|
| **Tick latency** | Time for one `runtime.tick()` call | milliseconds |
| **Reflex latency** | Sensor read to action execute entry | microseconds |
| **Memory footprint** | RSS (Resident Set Size) at steady state | megabytes |
| **Node-count scalability** | Tick latency vs. number of nodes | milliseconds |
| **Extended scalability** | Bus throughput, action dispatch, history depth | mixed |
| **Active inference comparison** | Decision-strategy A/B (Greedy / Weighted / Random / ActiveInference) — latency + selection bias | mixed |

## Running the suite

The easiest way to benchmark is the unified suite:

```bash
# Run all benchmarks (30 independent runs each — takes a few minutes)
python benchmarks/suite.py

# Quick run for development (5 runs each — much faster)
python benchmarks/suite.py --runs 5

# Run only specific benchmarks
python benchmarks/suite.py --only tick reflex

# Skip the slow extended benchmarks
python benchmarks/suite.py --skip extended
```

The suite automatically detects your hardware (CPU, RAM, OS, Python version)
and writes a JSON report to `benchmarks/results/`.

## Running individual benchmarks

You can also run each benchmark standalone:

```bash
python benchmarks/tick_latency.py
python benchmarks/reflex_latency.py
python benchmarks/memory_footprint.py
python benchmarks/scalability_sweep.py
python benchmarks/scalability_extended.py
python -m benchmarks.active_inference_comparison
```

## Reading the output

Here's what a tick latency result looks like:

```
Platform : win32 / CPython 3.12.0
Ticks    : 10000
Mean     :  0.312 ms
Median   :  0.298 ms
P95      :  0.441 ms
P99      :  0.613 ms
Max      :  2.104 ms
Std Dev  :  0.072 ms
```

What do these numbers mean?

- **Mean** — the average tick time. Useful for overall planning, but can be
  skewed by outliers.
- **Median** — the "typical" tick time (50th percentile). Usually more
  representative than the mean.
- **P95** — 95% of ticks were faster than this. Good for soft real-time
  budgeting.
- **P99** — 99% of ticks were faster than this. The "almost worst case."
- **Max** — the single slowest tick. Usually an outlier caused by OS
  scheduling, garbage collection, or another process stealing CPU time.
- **Std Dev** — how spread out the measurements are. Low = consistent,
  high = variable.

## The tick budget

Here's how to think about your tick budget. If you set `tick_rate_hz=10`,
you have a 100 ms budget per tick:

```
|<---------- 100 ms tick budget ---------->|
|-- framework overhead --|-- your code ----|
|       ~0.3 ms          |   ~99.7 ms      |
```

The benchmark measures only the framework overhead part. Everything else
is available for your `read()`, `evaluate()`, and `execute()` methods.

**Rule of thumb:** If the framework P99 is under 5% of your tick budget,
you have plenty of headroom. If it exceeds 20%, consider reducing tick rate
or optimising your node logic.

## Reflex latency — what it means

Reflex latency measures the time from a sensor detecting danger to the
emergency action starting. This is the framework's safety response overhead:

```
Sensor.read()         ContextNode.update()
    |                       |
    v                       v
  t_sense ──> signal ──> context ──> evaluate_reflexes() ──> dispatch() ──> t_action
                                                                              |
                                                                    T_reflex = t_action - t_sense
```

On a modern workstation, this is typically 30-50 microseconds. On a Jetson
Nano, about 850 microseconds. These are framework-only numbers — your actual
hardware read and actuator write times add to this.

> **Important: `poll_interval_s`**
>
> If your sensor's `poll_interval_s` is higher than 0 (the default is 0.1
> seconds), the framework may skip reading it on some ticks. For safety-
> critical sensors like collision detectors, set `poll_interval_s = 0.0`
> to ensure the sensor is read on every single tick.

## Understanding scalability

The node-count scalability benchmark shows how tick latency grows as you
add nodes:

```
Nodes    Median tick latency
  3      0.042 ms
  9      0.072 ms    (1.7x with 3x nodes)
 15      0.110 ms    (2.6x with 5x nodes)
 30      0.191 ms    (4.5x with 10x nodes)
 75      0.409 ms    (9.7x with 25x nodes)
150      0.763 ms   (18.2x with 50x nodes)
```

Notice that 50x more nodes only produce an 18x increase in latency — that's
**sub-linear scaling**. This happens because `asyncio.gather()` runs sense
reads and instinct evaluations concurrently.

## Comparing decision strategies

If you're choosing between `GreedyDecisionNode`, `WeightedDecisionNode`,
`RandomDecisionNode`, or `ActiveInferenceDecisionNode`, the active-inference
benchmark gives you both the per-tick cost and the selection bias of each:

```bash
# Quick run (5 runs × 2,000 ticks)
python -m benchmarks.active_inference_comparison

# Publication-grade (30 runs)
python -m benchmarks.active_inference_comparison --runs 30
```

It runs two workloads:

- **Case-study workload** — the pick-and-place scenario with one proposal per
  tick. Isolates the *per-tick overhead* of each strategy. In practice all four
  strategies land within ~1 % of each other on this workload.
- **Synthetic workload** — four competing proposals per tick with varying
  `priority`, `urgency`, and `confidence`. Isolates *selection bias*. Greedy
  and Weighted always pick the highest-priority proposal; ActiveInference
  often picks a lower-priority but higher-confidence proposal because
  expected free energy trades pragmatic value against epistemic uncertainty.

This is the right benchmark to run when you're deciding which strategy to
hand to `DecisionMasterNode(strategy=...)`.

## Statistical rigour

When you run benchmarks for a report or publication, use the stats module:

```bash
python benchmarks/stats.py --benchmark tick --runs 30
python benchmarks/stats.py --benchmark reflex --runs 30
```

This gives you:

- **95% bootstrap confidence intervals** — how much the median varies across
  independent runs. Narrow CI = reproducible results.
- **Wilcoxon signed-rank test** — for comparing two configurations (e.g.,
  "is 30 nodes significantly slower than 15 nodes?").
- **Cliff's delta** — how big is the difference? Negligible, small, medium,
  or large.

## Tips for accurate benchmarks

1. **Close other applications.** Background processes steal CPU time and
   inflate your measurements.
2. **Use at least 30 runs.** Fewer runs make confidence intervals wider
   and results less reproducible.
3. **Don't trust a single run.** Always look at the range across runs,
   not just one snapshot.
4. **Compare medians, not means.** Means are skewed by OS scheduling
   outliers. Medians are more stable.
5. **Set `poll_interval_s = 0.0` on benchmark stubs.** Otherwise the
   default 0.1 s throttle will cause sensors to be silently skipped,
   producing misleading results (see Lesson 3 and 4 for details).

## The JSON report

The suite outputs a JSON file you can process with any tool:

```python
import json

with open("benchmarks/results/suite_MyPC_20260413_140000.json") as f:
    report = json.load(f)

# Device info
print(report["device"]["cpu"])
print(report["device"]["ram_total_gb"], "GB")

# Tick latency median
tick = report["benchmarks"]["tick_latency"]["stats"]
print(f"Median tick: {tick['median']:.3f} ms")
```

## What's next?

You now know how to measure your agent's performance. In practice:

- Run benchmarks on your **target hardware** (not just your dev laptop).
- Compare results across Python versions — CPython 3.14 can be noticeably
  faster than 3.10 on the same hardware.
- Use the JSON reports to track performance over time as you add features.

For the full statistical methodology, see Section 22 of the
[spec](../spec/08_benchmarks.md).
