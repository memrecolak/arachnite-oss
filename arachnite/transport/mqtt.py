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

    Each AgentNode subscribes to the kinds its nodes need and
    publishes to the kinds its SenseNodes emit.

    QoS levels:
        0 = at-most-once  (fast, no guarantee)
        1 = at-least-once (default, recommended for sensor data)
        2 = exactly-once  (slow, use for critical commands)

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

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Connect to the MQTT broker and start the listener loop."""
        self._codec.check_network_safety(type(self).__name__)
        self._stopped = False
        attempts = 0
        while attempts < self._max_reconnect:
            try:
                tls_context = None
                if self._tls:
                    import ssl
                    tls_context = ssl.create_default_context()
                self._client = aiomqtt.Client(
                    hostname    = self._host,
                    port        = self._port,
                    username    = self._username,
                    password    = self._password,
                    tls_context = tls_context,
                )
                await self._client.__aenter__()
                self._connected   = True
                self._listen_task = asyncio.create_task(self._listen_loop())
                self._logger.info("Transport connected", transport=type(self).__name__)
                # Subscribe to any kinds registered before connect() was called
                for kind, callbacks in self._subscribers.items():
                    if callbacks:
                        topic = f"{self._topic_prefix}{kind}"
                        with contextlib.suppress(Exception):
                            await self._client.subscribe(topic, qos=self._qos)
                return
            except Exception as exc:
                attempts += 1
                if attempts >= self._max_reconnect:
                    raise TransportConnectionError(
                        "MQTTTransport",
                        f"Failed after {attempts} attempts: {exc}",
                    ) from exc
                await asyncio.sleep(self._reconnect_interval)

    async def disconnect(self) -> None:
        """Close the MQTT connection cleanly."""
        self._logger.info("Transport disconnected", transport=type(self).__name__)
        self._stopped = True
        self._connected = False
        if self._listen_task:
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.__aexit__(None, None, None)
            self._client = None

    # ── Pub/Sub ───────────────────────────────────────────────────────────────

    async def publish(self, signal: Signal) -> None:
        if not self._connected or self._client is None:
            raise TransportConnectionError("MQTTTransport", "Not connected")
        topic   = f"{self._topic_prefix}{signal.kind}"
        payload = self._encode_signal(signal)
        await self._client.publish(topic, payload=payload, qos=self._qos)

    async def subscribe(self, kind: str, callback: Callback) -> None:
        """Subscribe to a signal kind. Subscribes to the MQTT topic on first callback."""
        already_subscribed = bool(self._subscribers[kind])
        self._subscribers[kind].append(callback)
        if not already_subscribed and self._connected and self._client:
            topic = f"{self._topic_prefix}{kind}"
            await self._client.subscribe(topic, qos=self._qos)

    async def unsubscribe(self, kind: str, callback: Callback) -> None:
        with contextlib.suppress(ValueError):
            self._subscribers[kind].remove(callback)
        if not self._subscribers[kind] and self._connected and self._client:
            topic = f"{self._topic_prefix}{kind}"
            with contextlib.suppress(Exception):
                await self._client.unsubscribe(topic)

    # ── Listener loop ─────────────────────────────────────────────────────────

    async def _listen_loop(self) -> None:
        """Receive messages from the broker and dispatch to local subscribers."""
        if self._client is None:
            return
        try:
            async for message in self._client.messages:
                await self._dispatch(message)
        except asyncio.CancelledError:
            return
        except Exception:
            # Connection lost — attempt reconnect
            self._connected = False
            asyncio.create_task(self._reconnect())

    async def _dispatch(self, message: object) -> None:
        """Decode a raw MQTT message and call matching subscribers."""
        try:
            payload = bytes(message.payload)  # type: ignore[attr-defined]
            signal  = self._decode_signal(payload)
        except Exception:
            return  # malformed message — drop silently

        # Match on signal kind from decoded signal, not raw topic
        callbacks = (
            list(self._subscribers.get(signal.kind, []))
            + list(self._subscribers.get("*", []))
        )
        for cb in callbacks:
            with contextlib.suppress(Exception):
                await cb(signal)

    async def _reconnect(self) -> None:
        """Attempt to reconnect after a connection loss."""
        if self._stopped:
            return
        await asyncio.sleep(self._reconnect_interval)
        if self._stopped:
            return
        # Close the stale client before creating a new one.
        # The listen task has already exited (it raised and called us via create_task).
        if self._client:
            with contextlib.suppress(Exception):
                await self._client.__aexit__(None, None, None)
            self._client = None
        self._listen_task = None
        # connect() re-subscribes all active kinds and starts a fresh listen task
        with contextlib.suppress(Exception):
            await self.connect()

    @property
    def connected(self) -> bool:
        return self._connected
