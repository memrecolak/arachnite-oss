"""
arachnite.transport.nats
~~~~~~~~~~~~~~~~~~~~~~~~
NATSTransport: high-throughput, low-latency messaging for cloud and LAN.
Requires: nats-py >= 2.7 (pip install arachnite[nats])
Spec reference: Section 10.2.
"""

from __future__ import annotations

import contextlib
from collections import defaultdict
from collections.abc import Awaitable, Callable

from arachnite.codec import CodecRegistry
from arachnite.exceptions import TransportConnectionError
from arachnite.models import Signal
from arachnite.transport.base import BaseTransport

Callback = Callable[[Signal], Awaitable[None]]

try:
    import nats  # type: ignore[import-untyped,unused-ignore]
    import nats.aio.client as nats_client  # type: ignore[import-untyped,unused-ignore]
    _NATS_AVAILABLE = True
except ImportError:
    _NATS_AVAILABLE = False


class NATSTransport(BaseTransport):
    """
    NATS-backed transport using nats-py.

    Subject convention:
        {subject_prefix}.{signal.kind}
    e.g.  arachnite.thermal, arachnite.visual

    Supports optional JetStream persistence for replay and guaranteed delivery.
    Recommended for cloud nodes and laptop-class hardware.

    Spec reference: Section 10.2.
    """

    def __init__(
        self,
        servers:              str | list[str] = "nats://localhost:4222",
        agent_node_id:        str = "cloud",
        subject_prefix:       str = "arachnite",
        reconnect_interval_s: float = 2.0,
        max_reconnect_attempts: int = 10,
        codec_registry:       CodecRegistry | None = None,
    ) -> None:
        if not _NATS_AVAILABLE:
            raise ImportError(
                "NATSTransport requires 'nats-py'. "
                "Install with: pip install 'arachnite[nats]'"
            )
        super().__init__(agent_node_id=agent_node_id, codec_registry=codec_registry)
        self._servers       = [servers] if isinstance(servers, str) else servers
        self._subject_pfx   = subject_prefix
        self._reconnect_s   = reconnect_interval_s
        self._max_reconnect = max_reconnect_attempts

        self._nc:          nats_client.Client | None = None
        self._connected:   bool = False
        self._subscribers: dict[str, list[Callback]] = defaultdict(list)
        self._subs:        dict[str, object] = {}  # kind -> nats Subscription

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Connect to the NATS server."""
        self._codec.check_network_safety(type(self).__name__)
        try:
            self._nc = await nats.connect(
                servers             = self._servers,
                reconnect_time_wait = self._reconnect_s,
                max_reconnect_attempts = self._max_reconnect,
                error_cb            = self._on_error,
                disconnected_cb     = self._on_disconnected,
                reconnected_cb      = self._on_reconnected,
            )
            self._connected = True
            self._logger.info("Transport connected", transport=type(self).__name__)
            # Subscribe to any kinds registered before connect() was called
            for kind, callbacks in self._subscribers.items():
                if callbacks and kind not in self._subs:
                    subject = f"{self._subject_pfx}.{kind}"
                    with contextlib.suppress(Exception):
                        sub = await self._nc.subscribe(subject, cb=self._make_handler(kind))
                        self._subs[kind] = sub
        except Exception as exc:
            raise TransportConnectionError(
                "NATSTransport", str(exc)
            ) from exc

    async def disconnect(self) -> None:
        """Drain pending messages and close the connection."""
        self._logger.info("Transport disconnected", transport=type(self).__name__)
        self._connected = False
        if self._nc:
            with contextlib.suppress(Exception):
                await self._nc.drain()
            self._nc = None

    # ── Pub/Sub ───────────────────────────────────────────────────────────────

    async def publish(self, signal: Signal) -> None:
        if not self._connected or self._nc is None:
            raise TransportConnectionError("NATSTransport", "Not connected")
        subject = f"{self._subject_pfx}.{signal.kind}"
        payload = self._encode_signal(signal)
        await self._nc.publish(subject, payload)

    async def subscribe(self, kind: str, callback: Callback) -> None:
        self._subscribers[kind].append(callback)
        if kind not in self._subs and self._connected and self._nc:
            subject = f"{self._subject_pfx}.{kind}"
            sub = await self._nc.subscribe(
                subject,
                cb=self._make_handler(kind),
            )
            self._subs[kind] = sub

    async def unsubscribe(self, kind: str, callback: Callback) -> None:
        with contextlib.suppress(ValueError):
            self._subscribers[kind].remove(callback)
        if not self._subscribers[kind] and kind in self._subs:
            sub = self._subs.pop(kind)
            with contextlib.suppress(Exception):
                await sub.unsubscribe()  # type: ignore[attr-defined]

    def _make_handler(self, kind: str) -> Callable[[object], Awaitable[None]]:
        async def handler(msg: object) -> None:
            try:
                signal = self._decode_signal(msg.data)  # type: ignore[attr-defined]
            except Exception:
                return
            callbacks = (
                list(self._subscribers.get(signal.kind, []))
                + list(self._subscribers.get("*", []))
            )
            for cb in callbacks:
                with contextlib.suppress(Exception):
                    await cb(signal)
        return handler

    # ── NATS event callbacks ──────────────────────────────────────────────────

    async def _on_error(self, exc: Exception) -> None:
        pass  # logged by supervisor via SupervisorSignal

    async def _on_disconnected(self) -> None:
        self._connected = False

    async def _on_reconnected(self) -> None:
        self._connected = True
        # nats-py automatically re-sends SUBSCRIBE commands to the server for all
        # existing Subscription objects; calling nc.subscribe() again here would
        # create duplicate handlers and fire each callback twice.

    @property
    def connected(self) -> bool:
        return self._connected
