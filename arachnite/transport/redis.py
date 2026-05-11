"""
arachnite.transport.redis
~~~~~~~~~~~~~~~~~~~~~~~~~
RedisTransport: pub/sub backed by Redis Streams.
Requires: redis >= 5.0 (pip install arachnite[redis])
Spec reference: Section 10.2.

Migration note (0.8.0): switched from the legacy ``aioredis`` 2.x package to
``redis.asyncio`` (shipped by ``redis-py`` >= 4.2). ``aioredis`` was abandoned
in 2022 and breaks on Python >= 3.12 due to its ``distutils`` import.
``redis.asyncio`` is the same codebase, merged upstream by the original
``aioredis`` maintainer, with API parity for the surface we use
(``from_url``, ``pubsub``, ``subscribe`` / ``unsubscribe``, ``listen``,
``publish``, ``aclose``).

Known caveats after the swap:
  * Reconnect handling under abrupt network drops may differ subtly because
    ``redis.asyncio`` uses connection pools more aggressively than legacy
    ``aioredis``. The ``_reconnect()`` path below is the area to watch in
    production deployments.
  * ``client.close()`` is deprecated on ``redis-py`` 5.x in favour of
    ``aclose()``; we call ``aclose()`` directly.
  * Pinned ``redis < 7`` to avoid unknown future breaking changes; bump the
    ceiling deliberately after testing against new majors.
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
    from redis import asyncio as aioredis  # type: ignore[import-untyped,unused-ignore]
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False


class RedisTransport(BaseTransport):
    """
    Redis-backed pub/sub transport using ``redis.asyncio`` (from ``redis-py``).

    Channel convention:
        {channel_prefix}:{signal.kind}
    e.g.  arachnite:thermal, arachnite:visual

    Good when Redis is already part of the deployment stack.
    Uses Redis Pub/Sub for delivery and optionally Redis Streams
    for durable replay (future roadmap feature).

    Spec reference: Section 10.2.
    """

    def __init__(
        self,
        url:                  str = "redis://localhost:6379",
        agent_node_id:        str = "node",
        channel_prefix:       str = "arachnite",
        reconnect_interval_s: float = 2.0,
        max_reconnect_attempts: int = 10,
        db:                   int = 0,
        password:             str | None = None,
        codec_registry:       CodecRegistry | None = None,
    ) -> None:
        if not _REDIS_AVAILABLE:
            raise ImportError(
                "RedisTransport requires 'redis' (>= 5.0). "
                "Install with: pip install 'arachnite[redis]'"
            )
        super().__init__(agent_node_id=agent_node_id, codec_registry=codec_registry)
        self._url                = url
        self._channel_prefix     = channel_prefix
        self._reconnect_s        = reconnect_interval_s
        self._max_reconnect      = max_reconnect_attempts
        self._db                 = db
        self._password           = password

        self._pub_client:  aioredis.Redis | None = None
        self._sub_client:  aioredis.Redis | None = None
        self._pubsub:      object | None = None
        self._connected:   bool = False
        self._stopped:     bool = True
        self._subscribers: dict[str, list[Callback]] = defaultdict(list)
        self._listen_task: asyncio.Task[None] | None = None

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Connect publish and subscribe Redis clients."""
        self._codec.check_network_safety(type(self).__name__)
        self._stopped = False
        try:
            self._pub_client = await aioredis.from_url(  # type: ignore[no-untyped-call,unused-ignore]
                self._url, db=self._db, password=self._password,
                decode_responses=False,
            )
            self._sub_client = await aioredis.from_url(  # type: ignore[no-untyped-call,unused-ignore]
                self._url, db=self._db, password=self._password,
                decode_responses=False,
            )
            self._pubsub     = self._sub_client.pubsub()
            self._connected  = True
            self._logger.info("Transport connected", transport=type(self).__name__)
            # Subscribe to any channels registered before connect() was called
            for kind, callbacks in self._subscribers.items():
                if callbacks:
                    channel = f"{self._channel_prefix}:{kind}"
                    with contextlib.suppress(Exception):
                        await self._pubsub.subscribe(channel)  # type: ignore[union-attr,unused-ignore]
            self._listen_task = asyncio.create_task(self._listen_loop())
        except Exception as exc:
            raise TransportConnectionError("RedisTransport", str(exc)) from exc

    async def disconnect(self) -> None:
        """Close both Redis connections cleanly."""
        self._logger.info("Transport disconnected", transport=type(self).__name__)
        self._stopped = True
        self._connected = False
        if self._listen_task:
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
        for client in (self._pub_client, self._sub_client):
            if client:
                with contextlib.suppress(Exception):
                    await client.aclose()  # type: ignore[no-untyped-call,unused-ignore]
        self._pub_client = self._sub_client = self._pubsub = None

    # ── Pub/Sub ───────────────────────────────────────────────────────────────

    async def publish(self, signal: Signal) -> None:
        if not self._connected or self._pub_client is None:
            raise TransportConnectionError("RedisTransport", "Not connected")
        channel = f"{self._channel_prefix}:{signal.kind}"
        payload = self._encode_signal(signal)
        await self._pub_client.publish(channel, payload)

    async def subscribe(self, kind: str, callback: Callback) -> None:
        already = bool(self._subscribers[kind])
        self._subscribers[kind].append(callback)
        if not already and self._pubsub and self._connected:
            channel = f"{self._channel_prefix}:{kind}"
            await self._pubsub.subscribe(channel)  # type: ignore[attr-defined]
            # redis.asyncio PubSub.listen() exits when no channels are subscribed.
            # _listen_loop may have already exited before this first subscription
            # completed (the task runs during the above await). Restart it.
            if self._listen_task is None or self._listen_task.done():
                self._listen_task = asyncio.create_task(self._listen_loop())

    async def unsubscribe(self, kind: str, callback: Callback) -> None:
        with contextlib.suppress(ValueError):
            self._subscribers[kind].remove(callback)
        if not self._subscribers[kind] and self._pubsub and self._connected:
            channel = f"{self._channel_prefix}:{kind}"
            with contextlib.suppress(Exception):
                await self._pubsub.unsubscribe(channel)  # type: ignore[attr-defined]

    # ── Listener loop ─────────────────────────────────────────────────────────

    async def _listen_loop(self) -> None:
        """Receive messages from Redis and dispatch to local subscribers."""
        if self._pubsub is None:
            return
        try:
            async for message in self._pubsub.listen():  # type: ignore[attr-defined]
                if message["type"] != "message":
                    continue
                await self._dispatch(message["data"])
        except asyncio.CancelledError:
            return
        except Exception:
            self._connected = False
            asyncio.create_task(self._reconnect())

    async def _dispatch(self, data: bytes) -> None:
        try:
            signal = self._decode_signal(data)
        except Exception:
            return
        callbacks = (
            list(self._subscribers.get(signal.kind, []))
            + list(self._subscribers.get("*", []))
        )
        for cb in callbacks:
            with contextlib.suppress(Exception):
                await cb(signal)

    async def _reconnect(self) -> None:
        if self._stopped:
            return
        await asyncio.sleep(self._reconnect_s)
        if self._stopped:
            return
        # Close stale clients before creating new ones to avoid resource leaks.
        # The listen task has already exited (it raised and called us via create_task).
        for client in (self._pub_client, self._sub_client):
            if client:
                with contextlib.suppress(Exception):
                    await client.aclose()  # type: ignore[no-untyped-call,unused-ignore]
        self._pub_client = self._sub_client = self._pubsub = None
        self._listen_task = None
        # connect() re-subscribes all active channels and starts a fresh listen task
        with contextlib.suppress(Exception):
            await self.connect()

    @property
    def connected(self) -> bool:
        return self._connected
