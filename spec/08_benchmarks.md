<!-- Arachnite SPEC §22 -->

# __22\. Benchmarking__

This section specifies the benchmark suite that accompanies the framework. All benchmarks are deterministic software-only experiments: sensor and actuator I/O is replaced by no-op stubs, so that the measured overhead is pure framework overhead. The benchmark suite is located in `benchmarks/` and produces machine-readable JSON reports.

## __22\.1 Statistical Methodology__

Benchmarks must produce results that are reproducible and suitable for publication in peer-reviewed venues. The following methodology applies to all benchmarks unless stated otherwise.

### 22\.1\.1 Independent Runs

Each benchmark is executed as *R* independent runs (default *R* = 30). Each run creates a fresh `ArachniteRuntime` instance to eliminate cross-run state leakage. The number of runs is configurable via `--runs N`.

### 22\.1\.2 Descriptive Statistics

For each benchmark, the following statistics are computed over the pooled samples from all runs:

| Statistic | Formula |
|-----------|---------|
| Mean | x̄ = (1/n) Σᵢ xᵢ |
| Median | x̃ = middle value of sorted samples |
| P95 | x₍₀.₉₅ₙ₎ of the sorted sample array |
| P99 | x₍₀.₉₉ₙ₎ of the sorted sample array |
| Max | max(x₁, x₂, ..., xₙ) |
| Standard deviation | σ = √[(1/(n−1)) Σᵢ (xᵢ − x̄)²] |

### 22\.1\.3 Bootstrap Confidence Intervals

95% confidence intervals are computed over the per-run medians using the bootstrap resampling method:

1. Let M = {m₁, m₂, ..., m_R} be the per-run medians.
2. For *B* = 10,000 iterations, draw a sample of size *R* with replacement from M and compute the median of the resample.
3. Sort the *B* bootstrap medians.
4. The 95% CI is [bootstrap₍₀.₀₂₅B₎, bootstrap₍₀.₉₇₅B₎].

The bootstrap seed is fixed at 42 for reproducibility.

`DescriptiveStats` reports 95% bootstrap CIs not only for the median but also for the P95 and P99 percentile statistics (fields `p95_ci_lower`/`p95_ci_upper`, `p99_ci_lower`/`p99_ci_upper`). When per-run sample arrays are available, the P95/P99 CIs bootstrap over the per-run P95/P99 estimates using the same methodology as step (2) above; when only pooled samples are retained, the CI is computed by applying the percentile statistic to each bootstrap resample of the pooled array.

### 22\.1\.4 Hypothesis Testing

For cross-platform or cross-configuration comparisons, we use:

__Wilcoxon Signed-Rank Test__ (two-sided, α = 0.05). Given paired samples (xᵢ, yᵢ):

1. Compute differences dᵢ = xᵢ − yᵢ. Discard pairs where dᵢ = 0.
2. Rank |dᵢ| from smallest to largest (average ties).
3. W⁺ = sum of ranks where dᵢ > 0; W⁻ = sum of ranks where dᵢ < 0.
4. Test statistic: W = min(W⁺, W⁻).
5. For n ≥ 10, use the normal approximation: z = (W − μ_W) / σ_W where μ_W = n(n+1)/4 and σ_W = √[n(n+1)(2n+1)/24].
6. Two-sided p-value: p = 2 · Φ(−|z|) where Φ is the standard normal CDF.

__Bonferroni Correction.__ When making *k* pairwise comparisons, multiply each p-value by *k* and cap at 1.0: p_adj = min(p · k, 1.0).

### 22\.1\.5 Effect Size

__Cliff's Delta__ (non-parametric effect size) is used for unpaired comparisons:

δ = (1 / n_x · n_y) Σᵢ Σⱼ sign(xᵢ − yⱼ)

where sign(d) = +1 if d > 0, −1 if d < 0, 0 if d = 0.

Magnitude thresholds (Romano et al., 2006):

| |δ| range | Magnitude |
|-----------|-----------|
| < 0.147 | Negligible |
| 0.147 – 0.329 | Small |
| 0.330 – 0.473 | Medium |
| ≥ 0.474 | Large |

### 22\.1\.6 Timing

All latency measurements use `time.perf_counter()`, which provides sub-microsecond resolution on modern hardware. Warm-up iterations are discarded before measurement begins to eliminate JIT compilation and cache-warming effects.

## __22\.2 Tick Latency__

Measures the wall-clock duration of a single `ArachniteRuntime.tick()` call — the framework's per-cycle overhead.

### 22\.2\.1 Formal Definition

Let t₀ be the `time.perf_counter()` reading immediately before `tick()` is called, and t₁ the reading immediately after it returns. The tick latency for a single measurement is:

L_tick = t₁ − t₀

This excludes `asyncio.sleep()` scheduling overhead from the tick loop. The metric captures only the time spent inside the pipeline: sense → context → reflex → instinct → decide → act.

### 22\.2\.2 Protocol

| Parameter | Value |
|-----------|-------|
| SenseNodes | 3 (returning scalar float values) |
| InstinctNodes | 1 normal + 1 reflex (neither fires) |
| DecisionNode | WeightedDecisionNode |
| ActionNodes | 1 (no-op, returns `Result(success=True)`) |
| Warm-up ticks | 1,000 (discarded) |
| Measurement ticks | 10,000 per run |
| Default runs | 30 (300,000 total samples) |
| Timer | `time.perf_counter()` |

### 22\.2\.3 Per-Tick Complexity

The theoretical per-tick complexity of the pipeline is:

| Stage | Complexity |
|-------|-----------|
| SenseMasterNode.read_all() | O(S) concurrent, wall time = max(read_i) |
| ContextNode.update() | O(S + H) where H = history_length |
| InstinctMasterNode.evaluate_reflexes() | O(R) concurrent |
| InstinctMasterNode.evaluate_all() | O(I) concurrent |
| DecisionMasterNode.decide() | O(P log P) where P = proposals |
| ActionMasterNode.dispatch() | O(1) lookup by node_id |
| SignalBus.publish() | O(K) where K = subscribers |

Total: O(S + R + I + P log P + K), dominated by the number of leaf nodes.

### 22\.2\.4 Source

`benchmarks/tick_latency.py`

## __22\.3 Reflex Latency__

Measures the end-to-end latency from a sensor detecting a critical condition to the reflex action beginning execution — the framework's safety response overhead.

### 22\.3\.1 Formal Definition

Let t_sense be the `time.perf_counter()` reading at the moment `SenseNode.read()` returns a signal that crosses the reflex threshold, and t_action the reading at the moment `ActionNode.execute()` is entered. The reflex latency for a single trial is:

T_reflex = t_action − t_sense

This covers the latency introduced by:

- `ContextNode.update()` — integrating the signal into the context snapshot
- `InstinctMasterNode.evaluate_reflexes()` — evaluating all reflex nodes
- `ActionMasterNode.dispatch()` — looking up and calling the target action

It excludes hardware I/O latency (sensor read time and actuator write time), which are hardware-dependent and not attributable to the framework.

### 22\.3\.2 Protocol

| Parameter | Value |
|-----------|-------|
| SenseNodes | 1 (emits collision signal on command) |
| ReflexInstinctNode | 1 (fires on collision) |
| ActionNodes | 1 (records `time.perf_counter()` at `execute()` entry) |
| Trials per run | 1,000 |
| Default runs | 30 (30,000 total trials) |
| Timer | `time.perf_counter()` |
| poll_interval_s | 0.0 (read every tick) |

Each trial consists of two ticks: a prime tick (no collision) followed by a fire tick (collision signal). The sensor's `poll_interval_s` must be set to `0.0` to ensure it is read on every tick (see §4 Sensor Throttling).

### 22\.3\.3 Bounded Worst-Case

For a reflex that fires when no MultiStepActionNode is running, the worst-case framework latency is one tick interval plus the reflex evaluation time:

T_worst = 1/tick_rate_hz + T_evaluate_reflexes + T_dispatch

When a MultiStepActionNode with a mandatory completion block is running, the worst-case is bounded by:

T_worst_mandatory = T_remaining_mandatory + T_evaluate_reflexes + T_dispatch

where T_remaining_mandatory = Σ timeout_s for all remaining non-interruptible steps. This bound is statically computable from the step definitions (see §17.5).

### 22\.3\.4 Source

`benchmarks/reflex_latency.py`

## __22\.4 Memory Footprint__

Measures the Resident Set Size (RSS) of the Python process hosting an Arachnite runtime at steady state.

### 22\.4\.1 Formal Definition

Let RSS_config be the RSS measured after the runtime has started and completed 100 ticks (to reach steady state). The framework-attributable memory delta is:

Δ_RSS = RSS_config − RSS_baseline

where RSS_baseline is the RSS after importing the framework but before starting any runtime.

### 22\.4\.2 Protocol

| Parameter | Value |
|-----------|-------|
| Configurations | baseline (import only), minimal (1S 1I 1A), robot_arm (3S 2I 2A) |
| Warm-up ticks | 100 (reach steady state) |
| Measurement | RSS via psutil (cross-platform) or /proc/self/status (Linux) |
| Default runs | 10 |

### 22\.4\.3 Source

`benchmarks/memory_footprint.py`

## __22\.5 Node-Count Scalability__

Measures how tick latency scales with the number of registered nodes.

### 22\.5\.1 Formal Definition

For a configuration with N nodes per type (N sense + N instinct + N action = 3N total), the median tick latency is:

L̃_tick(N) = median of all samples across all runs for configuration N

The scalability factor relative to the baseline configuration (N=1) is:

S(N) = L̃_tick(N) / L̃_tick(1)

Sub-linear growth (S(N) < N) is expected due to `asyncio.gather()` concurrency in sense reads and instinct evaluations.

### 22\.5\.2 Protocol

| Parameter | Value |
|-----------|-------|
| Configurations | N ∈ {1, 3, 5, 10, 25, 50} per type (3–150 total nodes) |
| Warm-up ticks | 1,000 (discarded) |
| Measurement ticks | 5,000 per run |
| Default runs | 30 (150,000 total samples per configuration) |
| Instinct behaviour | None fires (pure overhead measurement) |
| Decision strategy | WeightedDecisionNode |

### 22\.5\.3 Expected Behaviour

Tick latency should scale approximately linearly with node count: O(N). The sub-linear constant factor is attributable to asyncio concurrency. A super-linear increase would indicate a regression in the framework's concurrent dispatch.

### 22\.5\.4 Source

`benchmarks/scalability_sweep.py`

## __22\.6 Extended Scalability__

Three additional scalability dimensions beyond node count, measured on a single platform.

### 22\.6\.1 SignalBus Throughput

__Metric:__ Number of signals published per second with K subscribers per signal kind.

__Definition:__ Let T_elapsed be the wall-clock time to publish N_signals through the bus. Throughput and per-publish latency are:

throughput = N_signals / T_elapsed  (signals/s)

L_publish = T_elapsed / N_signals  (seconds per publish)

__Protocol:__

| Parameter | Value |
|-----------|-------|
| Subscriber counts | K ∈ {1, 10, 50, 100, 500} |
| Signals per config | 10,000 |
| Subscriber callback | no-op async function |

__Expected behaviour:__ Throughput should be approximately constant across subscriber counts because `asyncio.gather()` dispatches all callbacks concurrently.

### 22\.6\.2 Concurrent Action Dispatch

__Metric:__ Tick latency when the `DecisionMasterNode` selects multiple proposals for concurrent dispatch via `dispatch_many()`.

__Protocol:__

| Parameter | Value |
|-----------|-------|
| Concurrent actions | A ∈ {1, 5, 10, 25, 50} |
| Warm-up ticks | 500 |
| Measurement ticks | 2,000 per run |
| Each instinct | Always fires, targeting its own action |

### 22\.6\.3 Context History Depth

__Metric:__ Tick latency with increasing context history buffer depth.

__Protocol:__

| Parameter | Value |
|-----------|-------|
| History depth | D ∈ {1, 10, 50, 100, 500} |
| Pre-fill | D + 100 ticks (fill history buffer completely) |
| Measurement ticks | 2,000 per run |

__Expected behaviour:__ History depth should have no measurable impact on tick latency. The `ContextNode` uses a `deque` with `maxlen`, so append is O(1). Instinct nodes receive a reference to the deque, not a copy.

### 22\.6\.4 Source

`benchmarks/scalability_extended.py`

## __22\.7 Stage Breakdown__

Measures the per-stage wall-clock overhead of a single `ArachniteRuntime.tick()` call, sliced into the six pipeline stages (`sense` → `context` → `reflex` → `instinct` → `decide` → `act`). Complements §22.2 (tick latency) by attributing the aggregate overhead to individual stages. See §7.5 for the `TickInstrumenter` protocol and the stage boundary contract.

### 22\.7\.1 Formal Definition

For tick *i*, the runtime invokes `on_stage(name, duration_s)` once per stage in pipeline order. Let L_stage^s(i) be the reported duration in seconds for stage s ∈ TICK_STAGE_NAMES. The stage sum plus the small framework bookkeeping interval between stages equals the total tick duration delivered by `on_tick_complete(tick_index, total_s)`:

> L_tick(i) ≈ Σ_{s ∈ stages} L_stage^s(i) + ε_framework(i)

where ε_framework is the cost of the instrumenter calls themselves and inter-stage bookkeeping (typically well under 1% of L_tick).

### 22\.7\.2 Protocol

| Parameter | Value |
|-----------|-------|
| SenseNodes | 3 (scalar float values; reused from `tick_latency.py`) |
| InstinctNodes | 1 normal + 1 reflex (neither fires) |
| DecisionNode | `WeightedDecisionNode` |
| ActionNodes | 1 (no-op, returns `Result(success=True)`) |
| Warm-up ticks | 1,000 (discarded) |
| Measurement ticks | 10,000 per run |
| Default runs | 30 (300,000 total samples per stage) |
| Instrumentation | `TickInstrumenter` protocol; benchmark-private `StageTimingCollector` appends to six pre-allocated lists |
| Timer | Runtime-side `time.monotonic()` at stage boundaries; durations are framework-reported, not benchmark-reported |

### 22\.7\.3 Statistical Treatment

Per-stage samples are fed through `DescriptiveStats.from_runs(medians, samples, n_per_run, run_samples=per_run_per_stage_samples)` (see §22.9.1 and §22.1.3) — once per stage — producing per-stage mean, median, P95, P99, σ, and 95% bootstrap confidence intervals for median, P95, and P99. The `run_samples` argument supplies per-run stage samples so that P95/P99 CIs bootstrap over per-run P95/P99 estimates rather than over the pooled array (the methodologically preferred path when per-run samples are available).

### 22\.7\.4 `TickInstrumenter` Notes

The `reflex` stage fuses reflex evaluation and reflex dispatch: the reflex arc is a decision-bypass arc, so `evaluate_reflexes()` and the sequential dispatch loop execute as one logical stage, and the stage label in the output sub-table SHOULD be annotated "reflex (evaluate + dispatch)" to avoid asymmetry with the `instinct` stage, which measures `evaluate_all()` only (normal instinct proposals dispatch in the `act` stage). The `sense` stage charges the `notify_tick_start` gather and supervisor-signal-buffer drain; the `act` stage charges the `notify_tick_end` gather. Instrumenter exceptions are caught, logged at WARNING, and do not fail the tick; see §7.5 for the error-isolation contract (ADR 0003). Samples for failing stages may be dropped from the collector but the tick continues.

### 22\.7\.5 Source

`benchmarks/stage_breakdown.py`

## __22\.8 Benchmark Suite__

The unified benchmark suite (`benchmarks/suite.py`) orchestrates all benchmarks and produces a single JSON report.

**Setup.** Publication-targeted benchmark runs SHOULD install the `benchmarks` optional-dependency extra (`pip install -e ".[benchmarks]"`), which pulls in `psutil>=5.9` and enables first-class RSS measurement in `benchmarks/memory_footprint.py` (§22.4) and `benchmarks/soak_test.py` (§22.13) on all supported platforms (Windows, Linux, macOS). The extra is also rolled into the `all` extra, so `pip install -e ".[all,dev]"` covers it. Benchmarks remain runnable without the extra: the RSS reader follows a three-tier fallback (`psutil` → `/proc/self/status` → `float('nan')`), so latency numbers are unaffected, RSS columns simply render `nan` on platforms without `psutil` and without `/proc`, and the `soak_test` drift verdict gracefully degrades to a P99-only check on those hosts.

### 22\.8\.1 Device Information

The suite automatically collects:

| Field | Source |
|-------|--------|
| CPU model | winreg (Windows), /proc/cpuinfo (Linux), sysctl (macOS) |
| Physical/logical cores | psutil or os.cpu_count() |
| Total RAM | psutil, /proc/meminfo, or ctypes MEMORYSTATUSEX |
| OS, architecture | platform module |
| Python version | platform.python_version() |
| Arachnite version | arachnite.__version__ |

### 22\.8\.2 CLI

```bash
# Run all benchmarks (30 runs each)
python benchmarks/suite.py

# Quick run (5 runs each)
python benchmarks/suite.py --runs 5

# Run specific benchmarks only
python benchmarks/suite.py --only tick reflex

# Skip slow benchmarks
python benchmarks/suite.py --skip extended

# Custom output directory
python benchmarks/suite.py --output-dir results/
```

### 22\.8\.3 Output Format

The suite writes a JSON file to `benchmarks/results/suite_<hostname>_<timestamp>.json`:

```json
{
  "suite_version": "2.0",
  "timestamp": "2026-04-13T14:05:00+0300",
  "elapsed_s": 342.7,
  "runs_per_benchmark": 30,
  "device": { ... },
  "benchmarks": {
    "tick_latency": { "stats": { ... }, "runs": [ ... ] },
    "reflex_latency": { ... },
    "memory_footprint": { ... },
    "scalability_sweep": { ... },
    "scalability_extended": { ... }
  }
}
```

The top-level `suite_version` field tags the driver protocol used to produce the report:

- **`"2.0"`** (current, 2026-04-16) — drivers bypass the runtime background loop and call `setup()` / `teardown()` on the four masters directly while driving the loop with manual `await rt.tick()` calls.
- **`"1.0"`** — initial release. Used `await rt.start()` / `await rt.stop()` around manual `rt.tick()` calls; numbers were systematically biased high (latency) and noisy (variance) due to background-loop contention. Do not pool v1 and v2 results in the same table.

### 22\.8\.4 Benchmark Registry

| Key | Section | Description |
|-----|---------|-------------|
| tick | §22.2 | Tick latency |
| reflex | §22.3 | Reflex latency |
| memory | §22.4 | Memory footprint |
| scalability | §22.5 | Node-count scalability |
| extended | §22.6 | Extended scalability |
| stage_breakdown | §22.7 | Per-stage tick latency |
| multistep_action_latency | §22.12 | Multi-step action interrupt/rollback latency |
| soak_test | §22.13 | Soak / stability drift |
| transport_latency | §22.14 | Publish-to-wake latency across transports |

## __22\.9 Statistical Primitives__

All statistical functions are implemented in `benchmarks/stats.py` with no external dependencies beyond the Python standard library. The module provides:

| Function | Purpose |
|----------|---------|
| `bootstrap_ci(data, stat_fn, n_bootstrap, alpha, seed)` | (1−α) bootstrap confidence interval |
| `wilcoxon_signed_rank(x, y)` | Paired Wilcoxon signed-rank test (W, p-value) |
| `cliffs_delta(x, y)` | Cliff's δ effect size with magnitude label |
| `bonferroni_adjust(p_values)` | Bonferroni multiple-comparison correction |
| `DescriptiveStats.from_runs(medians, samples, n_per_run)` | Aggregated statistics from multiple runs |

### 22\.9\.1 DescriptiveStats

```
@dataclass
class DescriptiveStats:
    n_runs: int
    n_samples_per_run: int
    mean: float
    median: float
    p95: float
    p99: float
    max_val: float
    std_dev: float
    ci_lower: float         # 95% bootstrap CI lower bound (median)
    ci_upper: float         # 95% bootstrap CI upper bound (median)
    ci_level: float = 0.95
    p95_ci_lower: float     # 95% bootstrap CI lower bound (P95)
    p95_ci_upper: float     # 95% bootstrap CI upper bound (P95)
    p99_ci_lower: float     # 95% bootstrap CI lower bound (P99)
    p99_ci_upper: float     # 95% bootstrap CI upper bound (P99)
```

`DescriptiveStats.from_runs()` computes percentiles from the pooled sample array and confidence intervals from the per-run medians (for the median statistic) and from per-run P95/P99 estimates when `run_samples` are supplied — otherwise from the pooled sample array via a percentile statistic passed to `bootstrap_ci()`.

## __22\.10 Writing New Benchmarks__

When adding a new benchmark to the suite:

1. Create `benchmarks/<name>.py` with a module-level `async def run() -> list[float]` returning raw samples.
2. Add a runner function in `benchmarks/suite.py` that calls `run()` for each independent run and aggregates via `DescriptiveStats.from_runs()`.
3. Register the benchmark in the `BENCHMARKS` dict in `suite.py`.
4. Document the benchmark in this section (§22) with a formal definition, protocol table, and expected behaviour.

## __22\.11 Reproducing Results__

All benchmarks are fully reproducible:

1. Install: `pip install -e ".[all,dev]"` and `pip install psutil` (for memory benchmarks).
2. Close unnecessary applications to reduce OS scheduling noise.
3. Run: `python benchmarks/suite.py --runs 30`.
4. Compare the JSON output with published results.

For publication-grade results, run on a quiet system with minimal background processes. On Linux, consider using `taskset` to pin to specific cores and `nice -n -20` for highest scheduling priority.

## __22\.12 Multi-Step Action Latency__

Measures interrupt and rollback latency on `MultiStepActionNode` across the framework's three interrupt policies (`InterruptPolicy.ALWAYS`, `CHECKPOINT`, `ROLLBACK`) and empirically probes the `T_worst_mandatory` bound declared in §22.3.3 by firing `request_interrupt()` inside a non-interruptible block. The benchmark does NOT exercise `ArachniteRuntime.emergency_stop()` end-to-end: that method tears down all four master nodes synchronously with interrupt delivery, so wall-clock at the call site is dominated by teardown, not by interrupt-delivery semantics; isolating the two would require a framework-level hook.

### 22\.12\.1 Formal Definition

Four scenarios, all framework-level wall-clock measurements in milliseconds driven by `time.perf_counter()`:

__ALWAYS.__ Let t_req be the reading at the moment `request_interrupt(...)` is called on an action running under `InterruptPolicy.ALWAYS`, and t_ret the reading at the moment `execute()` returns. The single-iteration latency is:

> L_always = t_ret − t_req

__CHECKPOINT.__ Identical definition, but the action runs under `InterruptPolicy.CHECKPOINT` and the interrupt is held until the next step flagged `checkpoint=True`. This isolates the latency tax imposed by carrying non-checkpoint steps through after the interrupt request.

__ROLLBACK.__ For `InterruptPolicy.ROLLBACK`, two metrics are reported. The outer wall-clock:

> L_rollback_total = t_ret − t_req

is the same shape as the previous two. The per-completed-step rollback overhead is derived from a direct `time.perf_counter()` wrap of the action's `on_interrupted()` (which invokes the rollback callables of completed non-interruptible steps in reverse order):

> L_rollback_per_step = (t_rollback_end − t_rollback_start) / N_rolled_back

where N_rolled_back is the number of rollback callables that fired. Direct timing of the rollback walk isolates pure rollback cost from setup overhead (interrupt-task wait, post-step check, `_handle_interrupt` entry), which would inflate the per-step metric if computed from the outer wall-clock.

__Mandatory-block worst-case.__ The action contains three steps, the middle one flagged `interruptible=False`. `request_interrupt(...)` is fired via the node's public setter while the mandatory step is in flight. The benchmark records the wall-clock of the mandatory block itself plus a boolean probe asserting (i) the mandatory step completes (not preempted), (ii) the interrupt is honoured at the next interruptible boundary, and (iii) the post-mandatory `finish` step is skipped. The sample is the empirical analogue of the formal bound §22.3.3:

> T_worst_mandatory ≥ Σ timeout_s for all remaining non-interruptible steps

### 22\.12\.2 Protocol

| Parameter | Value |
|-----------|-------|
| Actions | 4 benchmark-private `MultiStepActionNode` subclasses (one per scenario) |
| Step busy time | `asyncio.sleep(0.0005)` per step (interruptible); 10 inner awaits inside the mandatory block |
| Interrupt scheduler | `asyncio.create_task()` fired *after* `execute()` is entered (`execute()` resets `_interrupt_requested = False` on entry, so early scheduling is discarded) |
| Iterations per run | 500 (default) |
| Default runs | 5 (publication runs use 30 per §22.1.1) |
| Timer | `time.perf_counter()` |
| Scenario keys | `always_policy`, `checkpoint_policy`, `rollback_policy`, `rollback_policy_per_step` (derived), `mandatory_block_worst_case` |

### 22\.12\.3 Statistical Treatment

Per-scenario samples are aggregated through `DescriptiveStats.from_runs(medians, samples, n_per_run, run_samples=per_run_samples)` (see §22.9.1 and §22.1.3) — once per scenario, with the derived `rollback_policy_per_step` series aggregated using the same call pattern. The `run_samples` argument supplies per-run iteration samples so that P95/P99 CIs bootstrap over per-run P95/P99 estimates rather than over the pooled array (the methodologically preferred path when per-run samples are available).

### 22\.12\.4 Notes

The `CHECKPOINT` scenario occasionally terminates without firing (all steps run to completion before the interrupt task schedules), which is a valid outcome for the latency measurement — the scenario invariant accepts `result.interrupted or result.success`. The `ROLLBACK` scenario's per-step metric is derived by direct timing of the `on_interrupted()` walk rather than subtracting scenario totals, because outer wall-clock includes interrupt-task scheduling, the post-step boundary check, and `_handle_interrupt` entry — overhead that is not rollback work and that would dominate the per-step metric for small N_rolled_back. The mandatory-block scenario uses `action.request_interrupt(...)` — the public setter — rather than `ArachniteRuntime.emergency_stop()` for the reason recorded in the opening paragraph; teardown-inclusive emergency-stop timing is out of scope for this benchmark.

### 22\.12\.5 Source

`benchmarks/multistep_action_latency.py`

## __22\.13 Soak / Stability Test__

Measures long-horizon framework stability by running a large fixed number of ticks against a minimal rig, partitioning them into equal-sized buckets, and reporting per-bucket mean / P99 latency and end-of-bucket RSS. Complements §22.2 (single-shot tick latency) by exposing slow drift, hidden unbounded-queue growth, and gradual P99 inflation that single-shot 10,000-tick runs cannot detect.

### 22\.13\.1 Formal Definition

Let *T* be the total number of measurement ticks (default *T* = 1,000,000), *B* the bucket size in ticks (default *B* = 100,000), and *W* the warm-up tick count (default *W* = 10,000, discarded wholesale before measurement begins). Measurement ticks are partitioned in arrival order into ⌈T / B⌉ buckets; bucket *k* (1-indexed) covers measurement ticks [(k − 1) · B, min(k · B, T) − 1]. For each bucket *k*, the reported statistics are:

| Statistic | Formula |
|-----------|---------|
| mean_ms(k) | (1 / |samples_k|) · Σ samples_k |
| p99_ms(k)  | nearest-rank P99 of samples_k via `benchmarks.stats.percentile(samples_k, 99.0)` |
| rss_mb(k)  | RSS at the moment the bucket's final tick is observed |

The drift verdict compares the last bucket against the first:

> rss_growth_mb = rss_mb(K) − rss_mb(1)
>
> p99_drift_ms  = p99_ms(K) − p99_ms(1)

where *K* is the final bucket index. Drift is declared (`drift_detected = True`) if `rss_growth_mb` exceeds `_RSS_GROWTH_THRESHOLD_MB` (default 5.0) OR `p99_drift_ms` exceeds `_P99_DRIFT_THRESHOLD_MS` (default 0.05). Both thresholds are tunable module-level constants in `benchmarks/soak_test.py` and flow through to the JSON `drift` sub-object as `rss_growth_threshold_mb` / `p99_drift_threshold_ms` for auditability. NaN propagation is explicit: if either RSS reading is NaN (host without `psutil` and without `/proc/self/status`), `rss_growth_mb` is NaN, the RSS clause of the verdict is rendered as "RSS unmeasured" rather than "RSS +nan MB", and the verdict is decided on the P99 differential alone.

### 22\.13\.2 Protocol

| Parameter | Value |
|-----------|-------|
| SenseNodes | 1 (`_ConstantSense` returning a scalar float) |
| InstinctNodes | 1 (no-op, never proposes) |
| DecisionNode | `WeightedDecisionNode` |
| ActionNodes | 1 (no-op, returns `Result(success=True)`) |
| Warm-up ticks | *W* = 10,000 (discarded; bucket 1 starts at measurement tick 0) |
| Measurement ticks | *T* = 1,000,000 (default; tunable via `--ticks`) |
| Bucket size | *B* = 100,000 (default; tunable via `--bucket-size`) |
| Tail bucket | If *T* mod *B* ≠ 0 the remainder forms a final bucket of size *T* mod *B* |
| Tick driver | Manual `setup()` / `tick()` / `teardown()` per the v2 driver discipline (audit 2026-04-16, Bug B) |
| Timer | `time.perf_counter()` |
| Quick preset | `--quick` overrides to *T* = 10,000, *B* = 1,000, *W* = 500 — used by `benchmarks/suite.py` to keep CI sub-minute |

### 22\.13\.3 Statistical Treatment

Per-bucket P99 is reported as a point estimate using the suite-wide nearest-rank `percentile()` primitive (`all_sorted[int(n · p / 100)]`), consistent with §22.1.2. Per-bucket mean is the arithmetic mean of bucket samples. Bootstrap confidence intervals are NOT reported here: the drift signal is the *bucket-to-bucket trend* over a single long run, not the precision of any single point estimate. Operators seeking CI-bracketed point estimates SHOULD use `tick_latency.py` plus the §22.1.3 `DescriptiveStats.from_runs(..., run_samples=...)` path, which supplies the appropriate machinery for that question.

### 22\.13\.4 Notes

The minimal rig (1 sense / 1 instinct / 1 action) is deliberately lower overhead than `tick_latency.py`'s 3-sense / 2-instinct topology so that any per-tick drift observed over 1M ticks is attributable to framework state, not fixture state. RSS measurement reuses the optional-`psutil` / `/proc/self/status` / `float('nan')` fallback pathway from `benchmarks/memory_footprint.py` so numbers from the two benchmarks are directly comparable; on hosts where neither path resolves, the per-bucket `rss_mb` column renders as `nan` in the table and the drift verdict treats RSS as unmeasured (verdict line shows "RSS unmeasured" rather than `+nan MB`). The 1M-tick default takes several minutes on commodity hardware and is intended for per-release regression checking rather than routine CI runs; the suite runner therefore invokes only the `--quick` preset (*T* = 10,000, *B* = 1,000, *W* = 500) so a full `python benchmarks/suite.py` invocation remains sub-minute. Operators producing publication-grade soak data MUST invoke `python benchmarks/soak_test.py` directly. Drift thresholds are intentionally conservative for a minimal rig on a quiet machine and SHOULD be tightened per platform if sharper bounds are required.

### 22\.13\.5 Source

`benchmarks/soak_test.py`

## __22\.14 Transport Latency__

Measures publish-to-wake latency across the framework's four transports (`LocalTransport`, `MQTTTransport`, `NATSTransport`, `RedisTransport`) and three payload sizes per transport.

### 22\.14\.1 Formal Definition

For each `(transport, payload_size)` pair, a single `BaseTransport` instance acts as both publisher and subscriber (the self-publish path supported by all four transports). Let t_send be the `time.monotonic()` reading immediately before `BaseTransport.publish(signal)` is called, and t_wake the reading captured by the subscriber callback at the moment it fires. The single-iteration latency in milliseconds is:

> L_wake = (t_wake − t_send) · 1000

Both timestamps are read from the same monotonic clock in the same process, so clock-skew correction is unnecessary. The metric captures the transport-internal round trip — for `LocalTransport`, the in-process dispatch overhead beyond the `asyncio` event loop; for broker transports, serialise → publish → broker round-trip → `_listen_loop` receive → codec decode → subscriber callback fire → `Event.set()` return, sharing one TCP connection per the single-instance loopback shape.

### 22\.14\.2 Protocol

| Parameter | Value |
|-----------|-------|
| Transports | 4: `LocalTransport` (always); `MQTTTransport` / `NATSTransport` / `RedisTransport` (env-var gated) |
| Env vars | `ARACHNITE_TEST_MQTT_URL`, `ARACHNITE_TEST_NATS_URL`, `ARACHNITE_TEST_REDIS_URL` |
| Payload sizes | 8 B (scalar), 1 KB (structured), 64 KB (large) |
| Iteration defaults | `LocalTransport`: 50,000  ·  `MQTTTransport`: 2,000  ·  `NATSTransport`: 5,000  ·  `RedisTransport`: 5,000 |
| `--quick` preset | `LocalTransport`: 5,000  ·  brokers: 200 (uniform across the three brokers) |
| `--iterations N` | Uniform override across all four transports (used by tests) |
| Default runs | 5 (publication runs use 30 per §22.1.1) |
| Concurrency shape | Single publisher → single subscriber, serialised (publisher awaits subscriber `asyncio.Event` before next sample) |
| Timer | `time.monotonic()` |
| Signal kind | `transport_bench` (routes through the `CodecRegistry` wildcard fallback, msgpack) |

Broker gating policy (per ADR 0004 §1):

| Env var set? | Optional dep installed? | Behaviour |
|---|---|---|
| no  | n/a | Skip silently (`status: "skipped"` in JSON; one stdout note line) |
| yes | no  | Fail loud (raise from the optional-dep import; non-zero exit) |
| yes | yes | Run; on broker connect failure, fail loud (`TransportConnectionError` propagates) |

### 22\.14\.3 Statistical Treatment

Per-`(transport, payload_size)` samples are aggregated through `DescriptiveStats.from_runs(medians, samples, n_per_run, run_samples=per_run_samples)` (see §22.9.1 and §22.1.3) — once per cell. The `run_samples` argument supplies per-run iteration samples so that P95/P99 confidence intervals bootstrap over per-run P95/P99 estimates rather than over the pooled array (the methodologically preferred path when per-run samples are available). The printed report uses two tables — `LocalTransport` separately from broker transports — because the order-of-magnitude separation between sub-µs `LocalTransport` wake latency and millisecond broker round-trips makes a single combined table illegible; the JSON shape is uniform across all four transports.

### 22\.14\.4 Notes

The optional-dep imports for `aiomqtt` (MQTT), `nats` (NATS), and `redis.asyncio` (Redis) are deferred into the per-transport `_construct_*` helpers and execute only after the corresponding env var is found, per the rule that transport modules SHALL NOT be imported at module load time (they are optional extras). The `RedisTransport` measurement carries an additional structural cost not present in `MQTTTransport` / `NATSTransport`: `redis.asyncio` Pub/Sub uses a two-client architecture (one publisher connection plus one subscriber connection running `LISTEN` in `_listen_loop`), so the single-instance loopback path covers a `LISTEN` + `PUBLISH` round trip across two physical connections rather than a single duplexed connection; broker comparisons across `MQTTTransport` / `NATSTransport` / `RedisTransport` should account for this when interpreting per-cell numbers. The cross-`AgentNode` case adds at most one extra TCP hop and is intentionally not separately benchmarked here. The unified suite (`benchmarks/suite.py`, version 2.4) registers this benchmark under the `"transport_latency"` key and invokes it with the `--quick` preset so a Local-only suite run remains sub-minute; operators producing publication-grade numbers MUST invoke `python benchmarks/transport_latency.py` directly (with the relevant `ARACHNITE_TEST_*_URL` env vars set for broker coverage) and SHOULD use `--runs 30` per §22.1.1.

### 22\.14\.5 Source

`benchmarks/transport_latency.py`
