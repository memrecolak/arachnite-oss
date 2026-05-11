"""
benchmarks/soak_test.py
~~~~~~~~~~~~~~~~~~~~~~~
Long-horizon stability / drift soak test for Arachnite.

Purpose
-------
The existing tick_latency / stage_breakdown benchmarks each run 10,000
ticks and report a single statistical summary. A 10k-tick single-shot
cannot detect slow memory leaks, hidden unbounded-queue growth, or
per-tick P99 drift. This benchmark addresses that gap by running one
million ticks against a minimal runtime rig, bucketing them into
100,000-tick windows, and reporting per-bucket mean / P99 latency and
end-of-bucket RSS. The resulting per-bucket table supports stability
claims directly (rather than by inference from short runs).

Protocol
--------
  - Same "minimal" topology as tick_latency.py — a single
    ``_ConstantSense`` emitting a scalar, one no-op instinct, one no-op
    action — so wall-clock drift reflects framework state, not fixture
    state.
  - ``--ticks`` measurement ticks (default 1,000,000).
  - ``--bucket-size`` ticks per bucket (default 100,000).
  - ``--warmup`` pre-soak ticks (default 10,000) are discarded; bucket 1
    starts at measurement tick 0.
  - Per-bucket metrics:
      * mean tick latency (ms)
      * P99 tick latency (ms) — computed via the same percentile helper
        used by ``DescriptiveStats`` (``benchmarks.stats.percentile``)
      * RSS at bucket end (MB) — read via the same pathway as
        ``benchmarks/memory_footprint.py`` (psutil when available,
        ``/proc/self/status`` on Linux, NaN otherwise)
  - Drift verdict: a single-line conclusion based on two differentials,
    ``rss_growth_mb = rss[last] - rss[first]`` and
    ``p99_drift_ms  = p99[last] - p99[first]``. Thresholds default to
    ``5.0 MB`` and ``0.05 ms`` respectively and are tunable via module
    constants (``_RSS_GROWTH_THRESHOLD_MB`` / ``_P99_DRIFT_THRESHOLD_MS``).

The 1M-tick default takes several minutes on commodity hardware. For
CI / smoke runs use ``--quick``, which sets ``--ticks=10_000
--bucket-size=1_000 --warmup=500`` so the full pipeline (rig setup,
bucketing, RSS readings, drift verdict, JSON emission) exercises in a
few seconds.

Run:
    python benchmarks/soak_test.py --quick
    python benchmarks/soak_test.py                 # full 1M-tick soak
    python benchmarks/soak_test.py --ticks=500000  # half-soak
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from arachnite import (
    ActionMasterNode,
    ArachniteRuntime,
    BaseActionNode,
    BaseInstinctNode,
    BaseSenseNode,
    ContextNode,
    DecisionMasterNode,
    InstinctMasterNode,
    Proposal,
    Result,
    SenseMasterNode,
    Signal,
    SignalBus,
    WeightedDecisionNode,
)
from benchmarks.stats import percentile

# ── Defaults / tunables ──────────────────────────────────────────────────────

_TICKS_DEFAULT = 1_000_000
_BUCKET_SIZE_DEFAULT = 100_000
_WARMUP_DEFAULT = 10_000

# Drift verdict thresholds. These defaults are intentionally conservative
# for a minimal rig on a quiet machine. Tune by platform if tighter bounds
# are required.
_RSS_GROWTH_THRESHOLD_MB = 5.0
_P99_DRIFT_THRESHOLD_MS = 0.05

# --quick override values — used for CI smoke and the suite runner.
_QUICK_TICKS = 10_000
_QUICK_BUCKET_SIZE = 1_000
_QUICK_WARMUP = 500


# ── RSS helper ───────────────────────────────────────────────────────────────


def _rss_mb() -> float:
    """Return current RSS in MB. psutil preferred, /proc fallback, else NaN.

    Mirrors the measurement pathway used by ``benchmarks/memory_footprint.py``
    so numbers from the two benchmarks are directly comparable.
    """
    try:
        import psutil
        rss_bytes: float = float(psutil.Process().memory_info().rss)
        return rss_bytes / (1024 * 1024)
    except ImportError:
        pass
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except FileNotFoundError:
        pass
    return float("nan")


# ── Minimal stub nodes ───────────────────────────────────────────────────────
#
# A deliberately-minimal topology: one ConstantSense + one NopInstinct +
# one NopAction. The rig is lower overhead than ``tick_latency.py``'s
# 3-sense / 2-instinct setup so that any per-tick drift observed over
# 1M ticks is attributable to the framework itself, not fixture noise.


class _ConstantSense(BaseSenseNode):
    node_id = "SoakSense"
    signal_kind = "soak"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=1.0, confidence=1.0, timestamp=time.monotonic(),
        )


class _NopInstinct(BaseInstinctNode):
    node_id = "SoakInstinct"
    priority = 50

    async def evaluate(self, ctx: object) -> Proposal | None:
        return None


class _NopAction(BaseActionNode):
    node_id = "SoakAction"
    timeout_s = 1.0

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


# ── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class BucketStats:
    """Per-bucket statistics row (values in ms; RSS in MB)."""
    bucket_index: int
    tick_start: int
    tick_end: int
    mean_ms: float
    p99_ms: float
    rss_mb: float


@dataclass
class DriftSummary:
    """Differential across buckets plus a verdict string.

    ``rss_growth_mb`` is ``buckets[-1].rss_mb - buckets[0].rss_mb`` and
    ``p99_drift_ms`` is the equivalent for the P99 latency column. The
    ``drift_detected`` flag and ``verdict`` sentence summarise whether
    either differential exceeds its configured threshold.
    """
    rss_growth_mb: float
    p99_drift_ms: float
    rss_growth_threshold_mb: float
    p99_drift_threshold_ms: float
    drift_detected: bool
    verdict: str


@dataclass
class SoakReport:
    """Top-level report object written to JSON."""
    benchmark: str = "soak_test"
    unit: str = "ms"
    platform: str = ""
    python_version: str = ""
    arachnite_version: str = ""
    ticks: int = 0
    bucket_size: int = 0
    warmup: int = 0
    buckets: list[BucketStats] = field(default_factory=list)
    drift: DriftSummary | None = None


# ── Drift verdict ────────────────────────────────────────────────────────────


def compute_drift(
    buckets: list[BucketStats],
    rss_threshold_mb: float = _RSS_GROWTH_THRESHOLD_MB,
    p99_threshold_ms: float = _P99_DRIFT_THRESHOLD_MS,
) -> DriftSummary:
    """Compute drift differentials and produce a human-readable verdict.

    Returns a ``DriftSummary`` even for degenerate single-bucket inputs so
    the caller always gets a well-formed object to serialise; in that case
    the differentials are 0.0 and ``drift_detected=False``.
    """
    if len(buckets) < 2:
        return DriftSummary(
            rss_growth_mb=0.0,
            p99_drift_ms=0.0,
            rss_growth_threshold_mb=rss_threshold_mb,
            p99_drift_threshold_ms=p99_threshold_ms,
            drift_detected=False,
            verdict="No significant drift (insufficient buckets for comparison)",
        )

    first = buckets[0]
    last = buckets[-1]

    # NaN-safe subtraction: if either RSS reading is NaN, the differential
    # is NaN and we treat it as "unmeasured" (not detected).
    rss_growth = last.rss_mb - first.rss_mb
    p99_drift = last.p99_ms - first.p99_ms

    rss_over = rss_growth == rss_growth and rss_growth > rss_threshold_mb
    p99_over = p99_drift == p99_drift and p99_drift > p99_threshold_ms
    detected = rss_over or p99_over

    rss_clause_detected = (
        f"RSS grew by {rss_growth:+.2f} MB (threshold {rss_threshold_mb:.2f} MB)"
        if rss_growth == rss_growth
        else "RSS unmeasured"
    )
    rss_clause_clean = (
        f"RSS {rss_growth:+.2f} MB"
        if rss_growth == rss_growth
        else "RSS unmeasured"
    )

    if detected:
        verdict = (
            f"POSSIBLE DRIFT: {rss_clause_detected}, "
            f"P99 drifted by {p99_drift:+.4f} ms "
            f"(threshold {p99_threshold_ms:.4f} ms)"
        )
    else:
        verdict = (
            f"No significant drift ({rss_clause_clean}, "
            f"P99 {p99_drift:+.4f} ms over {len(buckets)} buckets)"
        )

    return DriftSummary(
        rss_growth_mb=rss_growth,
        p99_drift_ms=p99_drift,
        rss_growth_threshold_mb=rss_threshold_mb,
        p99_drift_threshold_ms=p99_threshold_ms,
        drift_detected=detected,
        verdict=verdict,
    )


# ── Benchmark driver ─────────────────────────────────────────────────────────


async def run(
    ticks: int = _TICKS_DEFAULT,
    bucket_size: int = _BUCKET_SIZE_DEFAULT,
    warmup: int = _WARMUP_DEFAULT,
) -> list[BucketStats]:
    """Execute the soak and return per-bucket statistics.

    The tick loop is driven manually (``setup()`` + ``tick()`` +
    ``teardown()``) per the discipline established by the audit-2026-04-16
    Bug B fix: the background loop must not run concurrently with manual
    tick drives.
    """
    if ticks <= 0:
        raise ValueError(f"ticks must be > 0, got {ticks}")
    if bucket_size <= 0:
        raise ValueError(f"bucket_size must be > 0, got {bucket_size}")
    if warmup < 0:
        raise ValueError(f"warmup must be >= 0, got {warmup}")

    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=WeightedDecisionNode(bus=bus))
    am = ActionMasterNode(bus=bus)

    sm.register(_ConstantSense(bus=bus))
    im.register(_NopInstinct(bus=bus))
    am.register(_NopAction(bus=bus))

    rt = ArachniteRuntime(
        sense_master=sm,
        context=ContextNode(),
        instinct_master=im,
        decision_master=dm,
        action_master=am,
        bus=bus,
        tick_rate_hz=10_000.0,  # sleep skipped; as fast as the event loop allows
    )

    for m in (sm, im, dm, am):
        await m.setup()

    # Warm-up — discarded wholesale.
    for _ in range(warmup):
        await rt.tick()

    # Measurement — bucketed.
    buckets: list[BucketStats] = []
    bucket_samples: list[float] = []
    bucket_index = 1
    tick_start = 0

    for _i in range(ticks):
        t0 = time.perf_counter()
        await rt.tick()
        bucket_samples.append((time.perf_counter() - t0) * 1_000.0)

        if len(bucket_samples) >= bucket_size:
            tick_end = tick_start + len(bucket_samples) - 1
            buckets.append(
                BucketStats(
                    bucket_index=bucket_index,
                    tick_start=tick_start,
                    tick_end=tick_end,
                    mean_ms=statistics.mean(bucket_samples),
                    p99_ms=percentile(bucket_samples, 99.0),
                    rss_mb=_rss_mb(),
                )
            )
            bucket_index += 1
            tick_start = tick_end + 1
            bucket_samples = []

    # Tail bucket — any remaining samples that didn't fill a full bucket.
    if bucket_samples:
        tick_end = tick_start + len(bucket_samples) - 1
        buckets.append(
            BucketStats(
                bucket_index=bucket_index,
                tick_start=tick_start,
                tick_end=tick_end,
                mean_ms=statistics.mean(bucket_samples),
                p99_ms=percentile(bucket_samples, 99.0),
                rss_mb=_rss_mb(),
            )
        )

    for m in (am, dm, im, sm):
        await m.teardown()

    return buckets


# ── Reporting ────────────────────────────────────────────────────────────────


def format_bucket_table(buckets: list[BucketStats]) -> str:
    """Format the per-bucket table as a printable multi-line string."""
    lines = [
        "  bucket  tick_start   tick_end     mean_ms      p99_ms    rss_mb",
        "  ------  ----------  ---------  ----------  ----------  --------",
    ]
    for b in buckets:
        lines.append(
            f"  {b.bucket_index:>6d}  {b.tick_start:>10d}  {b.tick_end:>9d}  "
            f"{b.mean_ms:>10.4f}  {b.p99_ms:>10.4f}  {b.rss_mb:>8.1f}"
        )
    return "\n".join(lines)


def report(buckets: list[BucketStats], drift: DriftSummary) -> None:
    """Print the bucket table and drift verdict to stdout."""
    print()
    print(f"Per-bucket soak statistics ({len(buckets)} buckets)")
    print("-" * 72)
    print(format_bucket_table(buckets))
    print("-" * 72)
    print(f"  {drift.verdict}")


# ── CLI ──────────────────────────────────────────────────────────────────────


def _arachnite_version() -> str:
    try:
        import arachnite
        return str(getattr(arachnite, "__version__", "unknown"))
    except Exception:
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Arachnite soak / stability benchmark. "
            "Runs N ticks against a minimal runtime, bucketed into windows, "
            "reporting per-bucket mean / P99 latency, end-of-bucket RSS, and "
            "an overall drift verdict."
        ),
    )
    parser.add_argument(
        "--ticks", "-t", type=int, default=_TICKS_DEFAULT,
        help=f"Total measurement ticks (default: {_TICKS_DEFAULT:,})",
    )
    parser.add_argument(
        "--bucket-size", "-b", type=int, default=_BUCKET_SIZE_DEFAULT,
        help=f"Ticks per bucket (default: {_BUCKET_SIZE_DEFAULT:,})",
    )
    parser.add_argument(
        "--warmup", "-w", type=int, default=_WARMUP_DEFAULT,
        help=f"Pre-soak ticks discarded (default: {_WARMUP_DEFAULT:,})",
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default="benchmarks/results",
        help="Directory for the JSON output file (default: benchmarks/results)",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help=(
            f"Quick / CI mode: ticks={_QUICK_TICKS:,} "
            f"bucket-size={_QUICK_BUCKET_SIZE:,} "
            f"warmup={_QUICK_WARMUP:,}. Overrides --ticks / --bucket-size "
            "/ --warmup."
        ),
    )
    args = parser.parse_args()

    if args.quick:
        ticks = _QUICK_TICKS
        bucket_size = _QUICK_BUCKET_SIZE
        warmup = _QUICK_WARMUP
    else:
        ticks = args.ticks
        bucket_size = args.bucket_size
        warmup = args.warmup

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Arachnite soak / stability benchmark")
    print(
        f"Warm-up: {warmup:,} ticks  |  Measurement: {ticks:,} ticks  "
        f"|  Bucket: {bucket_size:,}"
    )
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print("-" * 72)

    t0 = time.perf_counter()
    buckets = asyncio.run(run(ticks=ticks, bucket_size=bucket_size, warmup=warmup))
    elapsed = time.perf_counter() - t0

    drift = compute_drift(buckets)
    report(buckets, drift)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"soak_test_{timestamp}.json"
    payload = {
        "benchmark": "soak_test",
        "unit": "ms",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "python_version": platform.python_version(),
        "arachnite_version": _arachnite_version(),
        "ticks": ticks,
        "bucket_size": bucket_size,
        "warmup": warmup,
        "elapsed_s": round(elapsed, 2),
        "buckets": [asdict(b) for b in buckets],
        "drift": asdict(drift),
    }
    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"\nElapsed: {elapsed:.2f} s")
    print(f"Results saved to {out_file}")


if __name__ == "__main__":
    main()
