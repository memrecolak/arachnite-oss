"""
arachnite.transport.mqtt
~~~~~~~~~~~~~~~~~~~~~~~~
MQTTTransport: lightweight pub/sub over TCP for edge devices.
Requires: aiomqtt >= 2.0 (pip install arachnite[mqtt])
Spec reference: Section 10.2.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from collections.abc import Awaitable, Callable

from arachnite.codec import CodecRegistry
from arachnite.exceptions import TransportConnectionError
from arachnite.models import Signal
from arachnite.transport.base import BaseTransport

Callback = Callable[[Signal], Awaitable[None]]

try:
    import aiomqtt  # type: ignore[import-untyped,unused-ignore]
    _AIOMQTT_AVAILABLE = True
except ImportError:
    _AIOMQTT_AVAILABLE = False


class MQTTTransport(BaseTransport):
    """
    MQTT-backed transport using aiomqtt.

    Topic convention:
        {topic_prefix}{signal.kind}
    e.g.  arachnite/thermal, arachnite/visual

    The wildcard kind ``'*'`` subscribes the multi-level MQTT wildcard
    ``{topic_prefix}#`` (``#`` alone when the prefix does not end in ``/``,
    since MQTT wildcards must occupy a whole topic level).

    Each AgentNode subscribes to the kinds its nodes need and
    publishes to the kinds its SenseNodes emit.

    QoS levels:
        0 = at-most-once  (fast, no guarantee)
        1 = at-least-once (default, recommended for sensor data)
        2 = exactly-once  (slow, use for critical commands)

    Reconnect: a lost connection is re-established in the background (up to
    ``max_reconnect_attempts`` tries, ``reconnect_interval_s`` apart).
    ``disconnect()`` cancels a pending or running reconnect, so the transport
    stays down once its owner has shut it.

    Deployment note (latency): this transport disables Nagle's algorithm on
    its own sockets, but the broker's outbound sockets are outside its reach.
    A broker-mediated round trip has four TCP legs, and brokers commonly ship
    with Nagle enabled — mosquitto's ``set_tcp_nodelay`` defaults to false,
    which alone leaves a ~40-50 ms floor on sub-MSS publishes over a real
    network. For latency-sensitive deployments enable the broker's no-delay
    option (mosquitto: ``set_tcp_nodelay true``).

    Spec reference: Section 10.2.
    """

    def __init__(
        self,
        broker_host:          str,
        broker_port:          int = 1883,
        agent_node_id:        str = "edge",
        topic_prefix:         str = "arachnite/",
        qos:                  int = 1,
        reconnect_interval_s: float = 2.0,
        max_reconnect_attempts: int = 10,
        username:             str | None = None,
        password:             str | None = None,
        tls:                  bool = False,
        codec_registry:       CodecRegistry | None = None,
    ) -> None:
        if not _AIOMQTT_AVAILABLE:
            raise ImportError(
                "MQTTTransport requires 'aiomqtt'. "
                "Install with: pip install 'arachnite[mqtt]'"
            )
        super().__init__(agent_node_id=agent_node_id, codec_registry=codec_registry)
        self._host                 = broker_host
        self._port                 = broker_port
        self._topic_prefix         = topic_prefix
        self._qos                  = qos
        self._reconnect_interval   = reconnect_interval_s
        self._max_reconnect        = max_reconnect_attempts
        self._username             = username
        self._password             = password
        self._tls                  = tls

        self._client:       aiomqtt.Client | None = None
        self._connected:    bool                  = False
        self._stopped:      bool                  = True
        self._subscribers:  dict[str, list[Callback]] = defaultdict(list)
        self._listen_task:  asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None

    def _topic(self, kind: str) -> str:
        """The MQTT topic (or topic filter, for '*') of a signal kind."""
        if kind == "*":
            prefix = self._topic_prefix
            return f"{prefix}#" if prefix == "" or prefix.endswith("/") else "#"
        return f"{self._topic_prefix}{kind}"

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Connect to the MQTT broker and start the listener loop."""
        self._codec.check_network_safety(type(self).__name__)
        self._stopped = False
        await self._connect_loop()

    async def _connect_loop(self) -> None:
        """
        Open the connection, retrying up to ``max_reconnect_attempts`` times.

        Re-checks ``_stopped`` before every attempt, so a ``disconnect()`` that
        lands while a (re)connect is retrying ends it instead of being undone.
        """
        attempts = 0
        while not self._stopped:
            try:
                await self._open()
                return
            except Exception as exc:
                attempts += 1
                if attempts >= self._max_reconnect:
                    raise TransportConnectionError(
                        "MQTTTransport",
                        f"Failed after {attempts} attempts: {exc}",
                    ) from exc
                await asyncio.sleep(self._reconnect_interval)

    async def _open(self) -> None:
        """One connection attempt: connect, subscribe known kinds, start listening."""
        tls_context = None
        if self._tls:
            import ssl
            tls_context = ssl.create_default_context()
        client = aiomqtt.Client(
            hostname    = self._host,
            port        = self._port,
            username    = self._username,
            password    = self._password,
            tls_context = tls_context,
        )
        await client.__aenter__()
        try:
            if self._stopped:
                # disconnect() ran while this attempt was connecting.
                await client.__aexit__(None, None, None)
                return
            self._client = client
            self._enable_tcp_nodelay()
            # Subscribe to any kinds registered before connect() was called
            for kind, callbacks in list(self._subscribers.items()):
                if callbacks:
                    await client.subscribe(self._topic(kind), qos=self._qos)
        except BaseException:
            self._client = None
            with contextlib.suppress(Exception):
                await client.__aexit__(None, None, None)
            raise
        self._connected   = True
        self._listen_task = asyncio.create_task(self._listen_loop(client))
        self._logger.info("Transport connected", transport=type(self).__name__)

    def _enable_tcp_nodelay(self) -> None:
        """Disable Nagle's algorithm on the underlying paho socket.

        paho-mqtt (through at least 2.1) never sets TCP_NODELAY, so on a
        real network every sub-MSS publish can stall for a Nagle /
        delayed-ACK interaction (~40-45 ms per leg on Linux) before it
        leaves the host. Loopback benchmarks never show this; cross-host
        ones do (revision experiment E4 measured a ~90-100 ms RTT floor
        that disappears at 64 KiB payloads). Must be re-applied after
        every (re)connect — paho creates a fresh socket each time.
        """
        import socket
        try:
            paho_client = self._client._client  # type: ignore[union-attr]
            sock = paho_client.socket()
            if sock is not None and hasattr(sock, "setsockopt"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            # Non-fatal: websocket transports expose no raw socket, and a
            # failed setsockopt only restores the pre-fix latency profile.
            self._logger.warning(
                "Could not set TCP_NODELAY on MQTT socket",
                transport=type(self).__name__,
            )

    async def disconnect(self) -> None:
        """Close the MQTT connection cleanly and cancel any pending reconnect."""
        self._logger.info("Transport disconnected", transport=type(self).__name__)
        self._stopped = True
        self._connected = False
        current = asyncio.current_task()
        for task in (self._reconnect_task, self._listen_task):
            if task is not None and task is not current and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._reconnect_task = None
        self._listen_task = None
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.__aexit__(None, None, None)
            self._client = None

    # ── Pub/Sub ───────────────────────────────────────────────────────────────

    async def publish(self, signal: Signal) -> None:
        if not self._connected or self._client is None:
            raise TransportConnectionError("MQTTTransport", "Not connected")
        topic   = self._topic(signal.kind)
        payload = self._encode_signal(signal)
        await self._client.publish(topic, payload=payload, qos=self._qos)

    async def subscribe(self, kind: str, callback: Callback) -> None:
        """Subscribe to a signal kind. Subscribes to the MQTT topic on first callback."""
        already_subscribed = bool(self._subscribers[kind])
        if callback not in self._subscribers[kind]:
            self._subscribers[kind].append(callback)
        if not already_subscribed and self._connected and self._client:
            await self._client.subscribe(self._topic(kind), qos=self._qos)

    async def unsubscribe(self, kind: str, callback: Callback) -> None:
        with contextlib.suppress(ValueError):
            self._subscribers[kind].remove(callback)
        if not self._subscribers[kind] and self._connected and self._client:
            with contextlib.suppress(Exception):
                await self._client.unsubscribe(self._topic(kind))

    # ── Listener loop ─────────────────────────────────────────────────────────

    async def _listen_loop(self, client: aiomqtt.Client | None = None) -> None:
        """Receive messages from the broker and dispatch to local subscribers."""
        client = client if client is not None else self._client
        if client is None:
            return
        try:
            async for message in client.messages:
                await self._dispatch(message)
        except asyncio.CancelledError:
            return
        except Exception:
            # Connection lost — attempt reconnect (unless we are shutting down)
            self._connected = False
            if not self._stopped:
                self._reconnect_task = asyncio.create_task(self._reconnect())

    async def _dispatch(self, message: object) -> None:
        """Decode a raw MQTT message and call matching subscribers."""
        try:
            payload = bytes(message.payload)  # type: ignore[attr-defined]
            signal  = self._decode_signal(payload)
        except Exception as exc:
            self._logger.warning(
                "Dropped undecodable MQTT message",
                error=f"{type(exc).__name__}: {exc}",
            )
            return

        # Match on signal kind from decoded signal, not raw topic. One broker
        # delivery per message: a callback under both the kind and '*' is
        # called once.
        await self._deliver(signal, (
            list(self._subscribers.get(signal.kind, []))
            + list(self._subscribers.get("*", []))
        ))

    async def _reconnect(self) -> None:
        """Attempt to reconnect after a connection loss."""
        if self._stopped:
            return
        await asyncio.sleep(self._reconnect_interval)
        if self._stopped:
            return
        # Close the stale client before creating a new one.
        # The listen task has already exited (it raised and scheduled us).
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.__aexit__(None, None, None)
            self._client = None
        self._listen_task = None
        # _connect_loop() re-subscribes all active kinds and starts a fresh
        # listen task; it stops early if disconnect() is called meanwhile.
        try:
            await self._connect_loop()
        except TransportConnectionError as exc:
            self._logger.error("MQTT reconnect gave up", error=str(exc))

    @property
    def connected(self) -> bool:
        return self._connected
