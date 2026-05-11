# Lesson 13 — Benchmarking Your Agent

When deploying agents on real hardware — especially constrained devices like
the Jetson Nano — you need to know exactly how much overhead the framework
adds. Arachnite ships with a complete benchmarking suite.

## What to Measure

| Metric | Why it matters | Tool |
|--------|---------------|------|
| Tick latency | Framework overhead per cycle | `benchmarks/tick_latency.py` |
| Reflex latency | Safety response time | `benchmarks/reflex_latency.py` |
| Memory footprint | RAM usage on constrained devices | `benchmarks/memory_footprint.py` |
| Node-count scaling | How many nodes before it slows down | `benchmarks/scalability_sweep.py` |
| Bus throughput | SignalBus capacity | `benchmarks/scalability_extended.py` |

## Quick Start: Tick Latency

The simplest benchmark measures how long one `tick()` call takes:

```bash
python benchmarks/tick_latency.py
```

Output:
```
Platform : win32 / CPython 3.14.4
Ticks    : 10000
Mean     :  0.075 ms
Median   :  0.071 ms
P95      :  0.082 ms
P99      :  0.116 ms
Max      :  0.311 ms
Std Dev  :  0.012 ms
```

The P99 is the number that matters for real-time: 99% of ticks complete within
this time. At 10 Hz (100 ms budget), a P99 of 0.116 ms uses only 0.12% of the
budget.

## Reflex Latency

This measures the safety-critical path: how long from sensor read to reflex
action execution?

```bash
python benchmarks/reflex_latency.py
```

The reflex path is faster than a full tick because it skips the instinct
evaluation and decision layers.

## Statistically Rigorous Benchmarks

For research papers or production deployment decisions, use the statistical
harness that runs multiple independent trials:

```bash
python -m benchmarks.stats --benchmark all --runs 30
```

This produces:
- 30 independent runs per benchmark
- 95% bootstrap confidence intervals
- JSON output in `benchmarks/results/` with raw data

### Reading the Output

```bash
python -m benchmarks.stats --benchmark tick --runs 30
```

Each run creates a fresh runtime to avoid warm-cache effects. The results
include confidence intervals so you can make statistically valid claims.

## Extended Scalability

Three additional dimensions beyond node count:

```bash
python benchmarks/scalability_extended.py
```

1. **SignalBus throughput** — how many signals/second with 1-500 subscribers
2. **Concurrent actions** — tick latency with 1-50 simultaneous actions
3. **Context history depth** — impact of keeping 1 vs 500 ticks of history

## Writing Your Own Benchmarks

The pattern is simple: create a runtime, warm up, measure, report.

```python
import asyncio
import time
import statistics
from arachnite import (
    ArachniteRuntime, SignalBus, ContextNode,
    SenseMasterNode, InstinctMasterNode,
    DecisionMasterNode, GreedyDecisionNode,
    ActionMasterNode,
)

async def bench():
    # 1. Build your runtime (same as always)
    bus = SignalBus()
    # ... register your nodes ...

    rt = ArachniteRuntime(
        sense_master=sm, context=ContextNode(),
        instinct_master=im, decision_master=dm,
        action_master=am, bus=bus,
        tick_rate_hz=10_000.0,  # as fast as possible
    )
    await rt.start()

    # 2. Warm up (discard first N ticks)
    for _ in range(1_000):
        await rt.tick()

    # 3. Measure
    samples = []
    for _ in range(10_000):
        t0 = time.perf_counter()
        await rt.tick()
        samples.append((time.perf_counter() - t0) * 1_000)  # ms

    await rt.stop()

    # 4. Report
    s = sorted(samples)
    n = len(s)
    print(f"Mean   : {statistics.mean(s):.3f} ms")
    print(f"Median : {statistics.median(s):.3f} ms")
    print(f"P99    : {s[int(n * 0.99)]:.3f} ms")

asyncio.run(bench())
```

## Key Numbers to Watch

| Metric | Good | Concerning | Action needed |
|--------|------|-----------|---------------|
| Tick P99 | < 10% of tick budget | 10-50% | Optimize node logic |
| Tick P99 | > 50% of tick budget | | Reduce tick rate or split nodes |
| Reflex P99 | < 1 ms | 1-5 ms | Check asyncio event loop load |
| Memory delta | < 1 MB above baseline | > 10 MB | Check for signal/history leaks |

## Statistical Tools

The `benchmarks/stats.py` module provides building blocks for your own analysis:

```python
from benchmarks.stats import bootstrap_ci, wilcoxon_signed_rank, cliffs_delta

# 95% confidence interval
ci_lo, ci_hi = bootstrap_ci(samples, n_bootstrap=10_000)

# Compare two frameworks (paired samples)
W, p = wilcoxon_signed_rank(arachnite_samples, baseline_samples)

# Effect size
delta, magnitude = cliffs_delta(arachnite_samples, baseline_samples)
print(f"Cliff's delta: {delta:.3f} ({magnitude})")
```

## Tips

- Always **warm up** before measuring. The first ticks include JIT effects and
  cache warming.
- Use `tick_rate_hz=10_000.0` for benchmarks so `asyncio.sleep()` is skipped
  and you measure pure framework overhead.
- Run on a **quiet system** — close other applications, disable background
  services if possible.
- For publication-grade results, use `--runs 30` and report confidence intervals.
- The `benchmarks/results/` directory stores JSON files with raw data for
  reproducibility.
