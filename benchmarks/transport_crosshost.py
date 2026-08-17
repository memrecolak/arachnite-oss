"""
benchmarks/transport_crosshost.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cross-host transport latency over a real network (revision experiment E4).

Motivation (referee point R1-5): every transport figure in the submitted
paper is single-host loopback. This benchmark measures publish-to-deliver
latency between two physical hosts on a LAN (wired / Wi-Fi / netem-shaped
arms), through a real broker, on MQTT, NATS, or Redis.

Clock strategy: round-trip halving. The two hosts' clocks are never
compared — the client publishes a ping, the reflector on the peer host
echoes a pong, and the client measures the full round trip on its own
``time.perf_counter()`` clock. One-way latency is reported as RTT/2 under
a symmetric-path assumption (both legs cross the same broker and NIC; the
assumption and its limits are stated in the paper's threats section).
Loss arms: a timed-out ping is counted as a miss (per-cell ``missed``
count and loss percentage), not an error — packet loss is the point of
the netem arms.

Paths measured:
  transport — raw ``BaseTransport`` publish/subscribe on both ends
              (transport-layer cost only; comparable to
              ``transport_latency.py`` loopback numbers).
  bus       — each end runs a ``SignalBus`` bridged to its transport
              (the 0.11.7 bridge: egress forwarding, ``wire_kinds``
              allowlist, ``__origin_agent__`` loop prevention). This is
              the true agent-to-agent signal path of a distributed
              deployment.

The reflector and client MUST be started with the same ``--path`` and
``--transport``.

Broker URLs come from the same env vars as ``transport_latency.py``
(``ARACHNITE_TEST_MQTT_URL`` / ``ARACHNITE_TEST_NATS_URL`` /
``ARACHNITE_TEST_REDIS_URL``); both hosts point them at the shared broker.

Run (reflector first, on the peer host):
    python benchmarks/transport_crosshost.py --role reflector --transport mqtt --path bus
    python benchmarks/transport_crosshost.py --role client --transport mqtt --path bus \\
        --arm wired --peer <peer-name> --runs 5 --iterations 200
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
from dataclasses import asdict
from pathlib import Path

from arachnite.bus import SignalBus
from arachnite.models import Signal
from arachnite.transport.base import BaseTransport
from benchmarks.stats import DescriptiveStats
from benchmarks.transport_latency import (
    _ENV_VAR_MQTT,
    _ENV_VAR_NATS,
    _ENV_VAR_REDIS,
    _construct_mqtt,
    _construct_nats,
    _construct_redis,
)

_KIND_PING = "xh_ping"
_KIND_PONG = "xh_pong"

_PAYLOAD_SIZES_B: tuple[int, ...] = (8, 1024, 65536)
_RUNS_DEFAULT = 5
_ITERATIONS_DEFAULT = 200
_WARMUP_ITERATIONS = 50
_SUBSCRIBE_SETTLE_S = 0.5
# Cross-host default is higher than loopback: netem delay arms add up to
# hundreds of ms per leg, and Wi-Fi retransmission bursts are legitimate
# samples, not failures.
_RECV_TIMEOUT_S = 10.0

_TRANSPORTS: dict[str, tuple[str, object]] = {
    "mqtt": (_ENV_VAR_MQTT, _construct_mqtt),
    "nats": (_ENV_VAR_NATS, _construct_nats),
    "redis": (_ENV_VAR_REDIS, _construct_redis),
}


def _resolve_transport(name: str, agent_node_id: str) -> tuple[BaseTransport, str]:
    env_var, construct = _TRANSPORTS[name]
    url = os.environ.get(env_var, "")
    if not url:
        raise SystemExit(f"{env_var} is not set — export it with the shared broker URL")
    transport = construct(url)  # type: ignore[operator]
    # Both ends must have distinct agent ids or origin-loop prevention on the
    # bus path would drop the peer's signals as self-echo.
    transport._agent_node_id = agent_node_id  # noqa: SLF001 — bench-only override
    return transport, url


def _make_signal(kind: str, payload: bytes, seq: int) -> Signal:
    return Signal(
        source="crosshost_bench",
        kind=kind,
        value=payload,
        confidence=1.0,
        timestamp=time.monotonic(),
        metadata={"seq": seq},
    )


# ── Reflector role ──────────────────────────────────────────────────────────


async def run_reflector(transport_name: str, path: str) -> None:
    """Echo every ping back as a pong, forever (Ctrl+C to stop)"""
    transport, url = _resolve_transport(transport_name, agent_node_id="xh-reflector")
    echoed = 0

    if path == "bus":
        bus = SignalBus(
            transport=transport,
            agent_node_id="xh-reflector",
            wire_kinds={_KIND_PONG},
        )

        async def _on_ping_bus(sig: Signal) -> None:
            nonlocal echoed
            await bus.publish(_make_signal(_KIND_PONG, sig.value, sig.metadata["seq"]))
            echoed += 1
            if echoed % 100 == 0:
                print(f"[reflector] echoed {echoed}")

        bus.subscribe(_KIND_PING, _on_ping_bus)
        await transport.connect()
        await bus.start_transport()
    else:

        async def _on_ping_raw(sig: Signal) -> None:
            nonlocal echoed
            await transport.publish(_make_signal(_KIND_PONG, sig.value, sig.metadata["seq"]))
            echoed += 1
            if echoed % 100 == 0:
                print(f"[reflector] echoed {echoed}")

        await transport.connect()
        await transport.subscribe(_KIND_PING, _on_ping_raw)

    print(f"[reflector] {transport_name} path={path} broker={url} — waiting for pings")
    try:
        while True:
            await asyncio.sleep(3600.0)
    finally:
        await transport.disconnect()


# ── Client role ─────────────────────────────────────────────────────────────


async def _measure_cell(
    publish,  # async (Signal) -> None
    payload: bytes,
    iterations: int,
    pong_event: asyncio.Event,
    pong_box: dict[str, object],
    timeout_s: float,
    seq_start: int,
) -> tuple[list[float], int]:
    """One (payload, run) cell: serialized ping→pong RTT samples in ms"""
    samples: list[float] = []
    missed = 0
    seq = seq_start
    for _ in range(iterations):
        seq += 1
        pong_event.clear()
        pong_box.clear()
        sig = _make_signal(_KIND_PING, payload, seq)
        t0 = time.perf_counter()
        await publish(sig)
        deadline = t0 + timeout_s
        # Wait until the matching seq arrives (stale pongs from lost/late
        # iterations are ignored, they just re-arm the wait).
        while True:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                missed += 1
                break
            try:
                await asyncio.wait_for(pong_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                missed += 1
                break
            if pong_box.get("seq") == seq:
                samples.append((pong_box["ts"] - t0) * 1_000.0)  # type: ignore[operator]
                break
            pong_event.clear()  # stale pong — keep waiting for ours
    return samples, missed


async def run_client(args: argparse.Namespace) -> None:
    transport, url = _resolve_transport(args.transport, agent_node_id="xh-client")
    pong_event = asyncio.Event()
    pong_box: dict[str, object] = {}

    async def _on_pong(sig: Signal) -> None:
        # First matching arrival wins; box is read before the next ping.
        if not pong_event.is_set():
            pong_box["seq"] = sig.metadata.get("seq")
            pong_box["ts"] = time.perf_counter()
            pong_event.set()

    if args.path == "bus":
        bus = SignalBus(
            transport=transport,
            agent_node_id="xh-client",
            wire_kinds={_KIND_PING},
        )
        bus.subscribe(_KIND_PONG, _on_pong)
        await transport.connect()
        await bus.start_transport()
        publish = bus.publish
    else:
        await transport.connect()
        await transport.subscribe(_KIND_PONG, _on_pong)
        publish = transport.publish

    await asyncio.sleep(_SUBSCRIBE_SETTLE_S)
    print(f"[client] {args.transport} path={args.path} broker={url} arm={args.arm}")

    # Warmup (smallest payload) — pays broker/session cold-start cost.
    warm_payload = b"x" * _PAYLOAD_SIZES_B[0]
    await _measure_cell(publish, warm_payload, _WARMUP_ITERATIONS,
                        pong_event, pong_box, args.timeout, seq_start=0)

    cells: list[dict[str, object]] = []
    seq_base = 1_000_000
    for size_b in _PAYLOAD_SIZES_B:
        payload = b"x" * size_b
        run_samples: list[list[float]] = []
        run_medians: list[float] = []
        missed_total = 0
        for run_idx in range(args.runs):
            samples, missed = await _measure_cell(
                publish, payload, args.iterations, pong_event, pong_box,
                args.timeout, seq_start=seq_base,
            )
            seq_base += args.iterations + 1
            missed_total += missed
            if samples:
                run_samples.append(samples)
                run_medians.append(statistics.median(samples))
            print(f"[client] size={size_b}B run {run_idx + 1}/{args.runs}: "
                  f"{len(samples)} samples, {missed} missed")
        pooled = [s for run in run_samples for s in run]
        rtt = DescriptiveStats.from_runs(run_medians, pooled,
                                         args.iterations, run_samples)
        one_way = DescriptiveStats.from_runs(
            [m / 2.0 for m in run_medians],
            [s / 2.0 for s in pooled],
            args.iterations,
            [[s / 2.0 for s in run] for run in run_samples],
        )
        sent = args.runs * args.iterations
        cells.append({
            "payload_size_b": size_b,
            "iterations_per_run": args.iterations,
            "n_runs": args.runs,
            "sent": sent,
            "missed": missed_total,
            "loss_pct": round(100.0 * missed_total / sent, 2) if sent else None,
            "rtt_ms": asdict(rtt),
            "one_way_ms_rtt_halved": asdict(one_way),
        })

    await transport.disconnect()

    out_dir = Path(__file__).resolve().parent / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"transport_crosshost_{args.transport}_{args.arm}_{stamp}.json"
    doc = {
        "benchmark": "transport_crosshost",
        "schema_version": 1,
        "unit": "ms",
        "platform": f"{sys.platform} / CPython {platform.python_version()}",
        "client_host": platform.node(),
        "peer_host": args.peer,
        "arm": args.arm,
        "note": args.note,
        "transport": args.transport,
        "path": args.path,
        "broker_url": url,
        "clock_strategy": "rtt_halving (client perf_counter only; symmetric-path assumption)",
        "recv_timeout_s": args.timeout,
        "warmup_iterations": _WARMUP_ITERATIONS,
        "cells": cells,
    }
    out_file.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(f"\nResults saved to {out_file}")


# ── Entry point ─────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--role", choices=("client", "reflector"), required=True)
    parser.add_argument("--transport", choices=tuple(_TRANSPORTS), required=True)
    parser.add_argument("--path", choices=("transport", "bus"), default="bus")
    parser.add_argument("--arm", default=None,
                        help="client only: network-condition label, e.g. wired / "
                             "wifi / netem_delay20ms_loss1pct (required)")
    parser.add_argument("--peer", default=None,
                        help="client only: name of the reflector host (required)")
    parser.add_argument("--note", default="",
                        help="free-form context, e.g. exact tc/netem command used")
    parser.add_argument("--runs", type=int, default=_RUNS_DEFAULT)
    parser.add_argument("--iterations", type=int, default=_ITERATIONS_DEFAULT)
    parser.add_argument("--timeout", type=float, default=_RECV_TIMEOUT_S,
                        help="per-ping receive timeout in seconds (raise for "
                             "high-delay netem arms)")
    args = parser.parse_args()

    if args.role == "client":
        if not args.arm or not args.peer:
            parser.error("--arm and --peer are required for --role client")
        asyncio.run(run_client(args))
    else:
        try:
            asyncio.run(run_reflector(args.transport, args.path))
        except KeyboardInterrupt:
            print("\n[reflector] stopped")


if __name__ == "__main__":
    main()
