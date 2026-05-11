"""
benchmarks/transport_latency.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Transport publish-to-deliver latency.

Purpose
-------
Measures **in-process loopback latency** for all four transports —
``LocalTransport`` (always), and ``MQTTTransport`` / ``NATSTransport`` /
``RedisTransport`` (each gated by its own env var).

Measurement shape
-----------------
A single ``BaseTransport`` instance acts as both
publisher and subscriber (the self-publish path that all four transports
support). The subscriber callback captures ``time.monotonic()`` and sets
an ``asyncio.Event``; the publisher records its send-side timestamp,
publishes, and awaits the event. The latency sample is
``recv_ts - send_ts``, both measured from the same monotonic clock in
the same process. Samples are serialised: the publisher waits for the
event before sending the next sample (no in-flight queueing
confounding).

For brokers, the publisher and subscriber share one ``BaseTransport``
instance and therefore one TCP connection. That matches the
single-AgentNode self-publish path; cross-AgentNode latency adds at most
one extra TCP hop and is **not** separately benchmarked here.

Gating policy (ADR 0004 §1)
---------------------------
Three independent gates evaluated per broker transport:

  - env var unset  -> skip silently (status="skipped" in JSON, one
    stdout line so the operator sees what was and was not measured)
  - env var set, optional dep missing -> raise loud (status="failed",
    non-zero exit). The operator clearly opted in by setting the var; a
    silent skip would hide the setup mistake.
  - env var set, dep present, connect fails -> raise loud (same).

``LocalTransport`` is never gated and always runs.

Payload-size sweep (ADR 0004 §3)
--------------------------------
Three sizes per transport: 8 B (scalar), 1 KB (structured),
64 KB (large). Each cell ``(transport, size)`` produces an independent
``DescriptiveStats`` with P95/P99 bootstrap CIs (Bench-4 path).

Iterations (ADR 0004 §4)
------------------------
Per-transport defaults (overridable uniformly via ``--iterations N``):

    LocalTransport : 50_000   (sub-µs per op; large N keeps bootstrap tight)
    MQTTTransport  :  2_000   (broker round-trip O(0.1-10 ms))
    NATSTransport  :  5_000   (faster than MQTT in practice)
    RedisTransport :  5_000   (comparable to NATS for Pub/Sub)

``--quick`` shrinks every default by ~10x for CI smoke runs.

Run
---
    python benchmarks/transport_latency.py --quick
    python benchmarks/transport_latency.py --runs 5
    ARACHNITE_TEST_REDIS_URL=redis://localhost:6379 \\
        python benchmarks/transport_latency.py --runs 30
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from arachnite.models import Signal
from arachnite.transport.base import BaseTransport
from arachnite.transport.local import LocalTransport
from benchmarks.stats import DescriptiveStats

# ── Defaults / tunables ──────────────────────────────────────────────────────

# Per-transport iteration defaults (ADR 0004 §4).
_ITERATIONS_LOCAL = 50_000
_ITERATIONS_MQTT  = 2_000
_ITERATIONS_NATS  = 5_000
_ITERATIONS_REDIS = 5_000

# --quick override values — used for CI smoke and the suite runner.
# ~0.1x scaling per ADR §4 ("--quick flag: scale everything by ~0.1x").
_QUICK_ITERATIONS_LOCAL = 5_000
_QUICK_ITERATIONS_MQTT  = 200
_QUICK_ITERATIONS_NATS  = 200
_QUICK_ITERATIONS_REDIS = 200

# Default number of independent runs (used when --runs is omitted).
_RUNS_DEFAULT = 5

# Payload sizes (bytes) — scalar / structured / large.
_PAYLOAD_SIZES_B: tuple[int, ...] = (8, 1024, 65536)

# Signal kind used for all benchmark traffic. Routes through the
# CodecRegistry wildcard fallback (msgpack), which natively encodes
# bytes payloads of arbitrary length.
_BENCH_SIGNAL_KIND = "transport_bench"

# Env var names per the ADR + matching the gated-integration-test TODO.
_ENV_VAR_MQTT  = "ARACHNITE_TEST_MQTT_URL"
_ENV_VAR_NATS  = "ARACHNITE_TEST_NATS_URL"
_ENV_VAR_REDIS = "ARACHNITE_TEST_REDIS_URL"

# Status sentinels carried in the JSON report.
_STATUS_MEASURED = "measured"
_STATUS_SKIPPED  = "skipped"
_STATUS_FAILED   = "failed"

# Per-iteration receive timeout. Brokers (especially MQTT QoS 1) can drop
# the very first publish if it lands before SUBACK is processed by the
# listener task. Without a timeout the benchmark hung indefinitely on
# ``await received.wait()`` whenever a sample was lost. The deadline is
# generous enough to cover slow loopback brokers (Redis/NATS/MQTT) but
# short enough to fail the run rather than hang the suite.
_RECV_TIMEOUT_S = 5.0

# Quiescence delay after subscribe. aiomqtt awaits SUBACK in
# ``client.subscribe()``, but message dispatch happens on a separate
# ``async for message in client.messages`` task. A publish issued
# immediately after subscribe can race that listener loop and arrive
# before the per-kind dispatcher is wired up. 500 ms is required for
# Mosquitto's listener task to be fully scheduled on slower hosts (Pi,
# Jetson Tegra) — at 50 ms the first publish landed before SUBACK was
# fully wired and produced 40+ second outliers on every host tested.
_SUBSCRIBE_SETTLE_S = 0.5

# Warmup iterations executed before the timed loop. These pay broker
# cold-start cost (first publish handshake, dispatcher task warm-up,
# Redis connection-pool initialisation) so it does not contaminate
# measured samples. Pre-fix, the first ~50 samples of every (transport,
# size) cell carried 40+ second tails on MQTT loopback; these are now
# absorbed by the warmup pass.
_WARMUP_ITERATIONS = 50


# ── Data classes ─────────────────────────────────────────────────────────────


@dataclass
class CellStats:
    """Per-(transport, payload-size) measurement cell."""
    payload_size_b: int
    iterations_per_run: int
    n_runs: int
    stats: DescriptiveStats


@dataclass
class TransportReport:
    """One transport's slice of the report (skipped / measured / failed)."""
    transport: str
    status: str
    note: str = ""
    iterations_per_run: int = 0
    broker_url: str = ""
    cells: list[CellStats] = field(default_factory=list)


# ── Helpers ─────────────────────────────────────────────────────────────────


def _make_payload(size_b: int) -> bytes:
    """Build a bytes payload of the requested size.

    Content is uniform (single-byte fill); the codec doesn't entropy-code
    so payload content has no measurable effect on serialised size.
    """
    return b"x" * size_b


def _signal_for(payload: bytes) -> Signal:
    """Build a Signal carrying *payload* as ``value``."""
    return Signal(
        source     = "bench",
        kind       = _BENCH_SIGNAL_KIND,
        value      = payload,
        confidence = 1.0,
        timestamp  = time.monotonic(),
    )


# ── Single-iteration measurement ────────────────────────────────────────────


async def _measure_loopback(
    transport: BaseTransport,
    payload: bytes,
    iterations: int,
) -> list[float]:
    """Drive *iterations* publish-to-wake samples, return millisecond list.

    The transport is assumed to be already connected and subscribed. The
    caller owns connect/disconnect — keeping that out of the hot loop
    means broker connect cost does not contaminate per-iteration timing.

    A single ``asyncio.Event`` is reused across iterations: the
    subscriber callback sets it after recording the receive timestamp,
    the publisher awaits it, then ``clear()``s it before the next sample.
    Because the publisher waits for the event before publishing the
    next sample, there is no in-flight queueing — each sample's wall
    clock isolates the per-signal wake latency.
    """
    received = asyncio.Event()
    recv_ts: list[float] = []

    async def _on_signal(sig: Signal) -> None:
        recv_ts.append(time.monotonic())
        received.set()

    await transport.subscribe(_BENCH_SIGNAL_KIND, _on_signal)
    # Let SUBACK / dispatcher wiring quiesce before the first publish.
    # See _SUBSCRIBE_SETTLE_S note above.
    await asyncio.sleep(_SUBSCRIBE_SETTLE_S)

    # Untimed warmup pass: pay broker cold-start cost (first PUBACK
    # handshake, listener-task scheduling, Redis pool init) before the
    # measured loop. See _WARMUP_ITERATIONS note above.
    for _ in range(_WARMUP_ITERATIONS):
        received.clear()
        recv_ts.clear()
        sig = _signal_for(payload)
        await transport.publish(sig)
        try:
            await asyncio.wait_for(received.wait(), timeout=_RECV_TIMEOUT_S)
        except asyncio.TimeoutError:
            # Broker still initialising; skip this warmup iteration and try the next.
            continue

    samples: list[float] = []
    try:
        for i in range(iterations):
            received.clear()
            recv_ts.clear()
            sig = _signal_for(payload)
            send_ts = time.monotonic()
            await transport.publish(sig)
            try:
                await asyncio.wait_for(received.wait(), timeout=_RECV_TIMEOUT_S)
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    f"transport_latency: no delivery within "
                    f"{_RECV_TIMEOUT_S:.1f}s on iteration {i + 1}/{iterations} "
                    f"(payload={len(payload)} B). Broker likely dropped the "
                    "message or the subscriber wiring is broken."
                ) from exc
            # Guard: if the broker dispatched twice, take the first arrival.
            samples.append((recv_ts[0] - send_ts) * 1_000.0)
    finally:
        await transport.unsubscribe(_BENCH_SIGNAL_KIND, _on_signal)

    return samples


# ── Per-transport runners ───────────────────────────────────────────────────


async def _run_transport_cell(
    transport: BaseTransport,
    payload_size_b: int,
    iterations: int,
    n_runs: int,
) -> CellStats:
    """Drive *n_runs* independent runs of *iterations* samples each.

    Each run resamples freshly. ``DescriptiveStats.from_runs`` is fed
    the per-run medians (for the median CI) and the per-run sample
    arrays (for the P95/P99 bootstrap CIs per Bench-4 / spec §22.1.3).
    """
    payload = _make_payload(payload_size_b)
    run_medians: list[float] = []
    pooled: list[float] = []
    run_samples: list[list[float]] = []

    for _ in range(n_runs):
        samples = await _measure_loopback(transport, payload, iterations)
        run_medians.append(statistics.median(samples))
        pooled.extend(samples)
        run_samples.append(samples)

    stats = DescriptiveStats.from_runs(
        run_medians, pooled, iterations, run_samples=run_samples,
    )
    return CellStats(
        payload_size_b=payload_size_b,
        iterations_per_run=iterations,
        n_runs=n_runs,
        stats=stats,
    )


async def _run_local(iterations: int, n_runs: int) -> TransportReport:
    """Run all payload sizes for ``LocalTransport``. Always runs."""
    transport = LocalTransport(agent_node_id="bench-local")
    await transport.connect()
    try:
        cells: list[CellStats] = []
        for size_b in _PAYLOAD_SIZES_B:
            cells.append(
                await _run_transport_cell(transport, size_b, iterations, n_runs)
            )
    finally:
        await transport.disconnect()
    return TransportReport(
        transport="LocalTransport",
        status=_STATUS_MEASURED,
        iterations_per_run=iterations,
        cells=cells,
    )


async def _run_broker(
    transport_name: str,
    env_var: str,
    iterations: int,
    n_runs: int,
    construct: _BrokerCtor,
) -> TransportReport:
    """Run all payload sizes for a broker transport (env-var gated).

    ``construct(broker_url)`` is a thin closure that imports the
    transport class lazily and returns a configured instance. It runs
    only after the env var is found, so the optional dep import is
    deferred (transports are optional).
    """
    broker_url = os.environ.get(env_var, "").strip()
    if not broker_url:
        note = f"{env_var} not set"
        print(f"  {transport_name}: skipped ({note})")
        return TransportReport(
            transport=transport_name,
            status=_STATUS_SKIPPED,
            note=note,
        )

    # Env var is set: any failure from here is a setup error and must
    # surface loudly per ADR 0004 §1.
    transport = construct(broker_url)
    await transport.connect()
    try:
        cells: list[CellStats] = []
        for size_b in _PAYLOAD_SIZES_B:
            cells.append(
                await _run_transport_cell(transport, size_b, iterations, n_runs)
            )
    finally:
        await transport.disconnect()

    return TransportReport(
        transport=transport_name,
        status=_STATUS_MEASURED,
        iterations_per_run=iterations,
        broker_url=broker_url,
        cells=cells,
    )


# Type alias for the broker constructor closures used by _run_broker.
_BrokerCtor = Any  # callable[[str], BaseTransport]


def _construct_mqtt(broker_url: str) -> BaseTransport:
    """Build an MQTTTransport from a ``mqtt://host:port`` URL.

    Import is deferred to here so the module doesn't fail to load on
    installations without ``aiomqtt``. A missing dep raises ImportError
    from the constructor, which propagates loudly per ADR §1.
    """
    from arachnite.transport.mqtt import MQTTTransport
    host, port = _parse_host_port(broker_url, default_port=1883)
    return MQTTTransport(
        broker_host=host,
        broker_port=port,
        agent_node_id="bench-mqtt",
        max_reconnect_attempts=1,  # fail fast on a misconfigured broker
    )


def _construct_nats(broker_url: str) -> BaseTransport:
    """Build a NATSTransport from a ``nats://host:port`` URL."""
    from arachnite.transport.nats import NATSTransport
    return NATSTransport(
        servers=broker_url,
        agent_node_id="bench-nats",
        max_reconnect_attempts=1,
    )


def _construct_redis(broker_url: str) -> BaseTransport:
    """Build a RedisTransport from a ``redis://host:port[/db]`` URL."""
    from arachnite.transport.redis import RedisTransport
    return RedisTransport(
        url=broker_url,
        agent_node_id="bench-redis",
        max_reconnect_attempts=1,
    )


def _parse_host_port(url: str, default_port: int) -> tuple[str, int]:
    """Extract (host, port) from a ``scheme://host[:port]`` URL.

    MQTT URLs come in as ``mqtt://broker:1883``; aiomqtt's Client takes
    host and port separately, so we parse them out. Anything malformed
    falls back to (url, default_port) so the broker connect call gets
    a chance to surface its own clearer error.
    """
    if "://" in url:
        url = url.split("://", 1)[1]
    if "/" in url:
        url = url.split("/", 1)[0]
    if ":" in url:
        host, port_str = url.rsplit(":", 1)
        try:
            return host, int(port_str)
        except ValueError:
            return url, default_port
    return url, default_port


# ── Top-level driver ────────────────────────────────────────────────────────


async def run(
    iterations_override: int | None = None,
    n_runs: int = _RUNS_DEFAULT,
    quick: bool = False,
) -> list[TransportReport]:
    """Run the full benchmark and return one report per transport.

    Parameters
    ----------
    iterations_override:
        If set, every transport uses this value (overriding per-transport
        defaults). Used by tests for fast smoke runs.
    n_runs:
        Independent runs per cell (forwarded to ``DescriptiveStats``).
    quick:
        If ``True`` and ``iterations_override`` is None, use the
        ``_QUICK_ITERATIONS_*`` per-transport scaled defaults.
    """
    if iterations_override is not None:
        iters_local = iters_mqtt = iters_nats = iters_redis = iterations_override
    elif quick:
        iters_local = _QUICK_ITERATIONS_LOCAL
        iters_mqtt  = _QUICK_ITERATIONS_MQTT
        iters_nats  = _QUICK_ITERATIONS_NATS
        iters_redis = _QUICK_ITERATIONS_REDIS
    else:
        iters_local = _ITERATIONS_LOCAL
        iters_mqtt  = _ITERATIONS_MQTT
        iters_nats  = _ITERATIONS_NATS
        iters_redis = _ITERATIONS_REDIS

    reports: list[TransportReport] = []
    reports.append(await _run_local(iters_local, n_runs))
    reports.append(await _run_broker(
        "MQTTTransport",  _ENV_VAR_MQTT,  iters_mqtt,  n_runs, _construct_mqtt,
    ))
    reports.append(await _run_broker(
        "NATSTransport",  _ENV_VAR_NATS,  iters_nats,  n_runs, _construct_nats,
    ))
    reports.append(await _run_broker(
        "RedisTransport", _ENV_VAR_REDIS, iters_redis, n_runs, _construct_redis,
    ))
    return reports


# ── Reporting ───────────────────────────────────────────────────────────────


def _format_cell_row(cell: CellStats) -> str:
    """One row of the printed transport-latency table."""
    s = cell.stats
    return (
        f"  {cell.payload_size_b:>7d} B  "
        f"mean={s.mean:>9.4f}  "
        f"median={s.median:>9.4f}  "
        f"P95={s.p95:>9.4f}  "
        f"P99={s.p99:>9.4f}  "
        f"SD={s.std_dev:>9.4f}  "
        f"95%CI=[{s.ci_lower:.4f}, {s.ci_upper:.4f}]"
    )


def report(reports: list[TransportReport]) -> None:
    """Print two tables (LocalTransport, then brokers).

    Order-of-magnitude separation between LocalTransport (sub-µs) and
    broker transports (ms) makes a single combined table illegible in
    print, and the two categories measure fundamentally different things
    ("framework dispatch overhead" vs "broker round-trip overhead").
    """
    by_name = {r.transport: r for r in reports}

    print()
    print("LocalTransport publish-to-wake latency (ms)")
    print("-" * 96)
    local = by_name.get("LocalTransport")
    if local and local.status == _STATUS_MEASURED:
        for cell in local.cells:
            print(_format_cell_row(cell))
    else:
        print(f"  LocalTransport: {local.status if local else 'absent'}")
    print("-" * 96)

    print()
    print("Broker transport publish-to-deliver latency (ms)")
    print("-" * 96)
    for name in ("MQTTTransport", "NATSTransport", "RedisTransport"):
        br = by_name.get(name)
        if br is None:
            print(f"  {name}: absent")
            continue
        if br.status == _STATUS_SKIPPED:
            print(f"  {name}: skipped ({br.note})")
            continue
        if br.status == _STATUS_FAILED:
            print(f"  {name}: FAILED ({br.note})")
            continue
        print(f"  {name}  [{br.broker_url}]")
        for cell in br.cells:
            print(_format_cell_row(cell))
    print("-" * 96)


# ── JSON serialisation ──────────────────────────────────────────────────────


def _report_to_json(tr: TransportReport) -> dict[str, Any]:
    """Serialise a TransportReport into the JSON shape from ADR §7."""
    out: dict[str, Any] = {
        "transport": tr.transport,
        "status": tr.status,
    }
    if tr.note:
        out["note"] = tr.note
    if tr.broker_url:
        out["broker_url"] = tr.broker_url
    if tr.cells:
        out["iterations_per_run"] = tr.iterations_per_run
        out["payload_sizes_b"] = list(_PAYLOAD_SIZES_B)
        out["by_size"] = {
            str(c.payload_size_b): asdict(c.stats) for c in tr.cells
        }
    return out


def assemble_report(
    reports: list[TransportReport], n_runs: int, elapsed_s: float,
) -> dict[str, Any]:
    """Build the top-level JSON payload."""
    return {
        "benchmark": "transport_latency",
        "unit": "ms",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "python_version": platform.python_version(),
        "arachnite_version": _arachnite_version(),
        "n_runs": n_runs,
        "payload_sizes_b": list(_PAYLOAD_SIZES_B),
        "elapsed_s": round(elapsed_s, 2),
        "transports": {tr.transport: _report_to_json(tr) for tr in reports},
    }


# ── CLI ─────────────────────────────────────────────────────────────────────


def _arachnite_version() -> str:
    try:
        import arachnite
        return str(getattr(arachnite, "__version__", "unknown"))
    except Exception:
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Arachnite transport publish-to-deliver latency benchmark. "
            "Always runs LocalTransport; "
            "broker transports light up when their respective env var is "
            f"set ({_ENV_VAR_MQTT} / {_ENV_VAR_NATS} / {_ENV_VAR_REDIS})."
        ),
    )
    parser.add_argument(
        "--iterations", "-t", type=int, default=None,
        help=(
            "Override per-transport iteration defaults uniformly. "
            f"Defaults: Local={_ITERATIONS_LOCAL:,}  "
            f"MQTT={_ITERATIONS_MQTT:,}  "
            f"NATS={_ITERATIONS_NATS:,}  "
            f"Redis={_ITERATIONS_REDIS:,}."
        ),
    )
    parser.add_argument(
        "--runs", "-n", type=int, default=_RUNS_DEFAULT,
        help=f"Independent runs per cell (default: {_RUNS_DEFAULT}).",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help=(
            f"Quick / CI mode: Local={_QUICK_ITERATIONS_LOCAL:,}  "
            f"brokers={_QUICK_ITERATIONS_MQTT:,}. Ignored if "
            "--iterations is given."
        ),
    )
    parser.add_argument(
        "--output-dir", "-o", type=str, default="benchmarks/results",
        help="Directory for the JSON output file (default: benchmarks/results).",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Arachnite transport latency benchmark")
    print(f"Runs: {args.runs}  |  Quick mode: {args.quick}")
    print(f"Platform : {sys.platform} / CPython {platform.python_version()}")
    print("-" * 96)

    t0 = time.perf_counter()
    reports = asyncio.run(run(
        iterations_override=args.iterations,
        n_runs=args.runs,
        quick=args.quick,
    ))
    elapsed = time.perf_counter() - t0
    report(reports)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"transport_latency_{timestamp}.json"
    payload = assemble_report(reports, n_runs=args.runs, elapsed_s=elapsed)
    with open(out_file, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"\nElapsed: {elapsed:.2f} s")
    print(f"Results saved to {out_file}")


if __name__ == "__main__":
    main()
