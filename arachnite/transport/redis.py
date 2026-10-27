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
from typing import Any

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

    The wildcard kind ``'*'`` is a pattern subscription (``PSUBSCRIBE
    {channel_prefix}:*``); a plain SUBSCRIBE would only match a channel
    literally named ``…:*``.

    ``connect()`` verifies the server answers (``PING``) and retries up to
    ``max_reconnect_attempts`` times, ``reconnect_interval_s`` apart, before
    raising ``TransportConnectionError`` — ``from_url()`` alone is lazy and
    would report success against a server that is down. A connection lost
    later is re-established in the background; ``disconnect()`` cancels it.

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
        self._pubsub:      Any | None = None
        self._connected:   bool = False
        self._stopped:     bool = True
        self._subscribers: dict[str, list[Callback]] = defaultdict(list)
        self._listen_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None

    def _channel(self, kind: str) -> str:
        return f"{self._channel_prefix}:{kind}"

    async def _pubsub_subscribe(self, pubsub: Any, kind: str) -> None:
        if kind == "*":
            await pubsub.psubscribe(self._channel("*"))
        else:
            await pubsub.subscribe(self._channel(kind))

    async def _pubsub_unsubscribe(self, pubsub: Any, kind: str) -> None:
        if kind == "*":
            await pubsub.punsubscribe(self._channel("*"))
        else:
            await pubsub.unsubscribe(self._channel(kind))

    # ── Connection ────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Connect publish and subscribe Redis clients."""
        self._codec.check_network_safety(type(self).__name__)
        self._stopped = False
        await self._connect_loop()

    async def _connect_loop(self) -> None:
        """Open the connection, retrying; stops early once disconnect() was called."""
        attempts = 0
        while not self._stopped:
            try:
                await self._open()
                return
            except Exception as exc:
                attempts += 1
                if attempts >= self._max_reconnect:
                    raise TransportConnectionError(
                        "RedisTransport",
                        f"Failed after {attempts} attempts: {exc}",
                    ) from exc
                await asyncio.sleep(self._reconnect_s)

    async def _open(self) -> None:
        """One connection attempt: connect, PING, subscribe known kinds, listen."""
        pub = sub = None
        try:
            pub = await aioredis.from_url(  # type: ignore[no-untyped-call,unused-ignore]
                self._url, db=self._db, password=self._password,
                decode_responses=False,
            )
            sub = await aioredis.from_url(  # type: ignore[no-untyped-call,unused-ignore]
                self._url, db=self._db, password=self._password,
                decode_responses=False,
            )
            # from_url() is lazy: without a round trip a server that is down
            # would only show up as silently dropped subscriptions.
            await pub.ping()
            pubsub = sub.pubsub()
            for kind, callbacks in list(self._subscribers.items()):
                if callbacks:
                    await self._pubsub_subscribe(pubsub, kind)
        except BaseException:
            await self._close_clients(pub, sub)
            raise
        if self._stopped:
            # disconnect() ran while this attempt was connecting.
            await self._close_clients(pub, sub)
            return
        self._pub_client, self._sub_client, self._pubsub = pub, sub, pubsub
        self._connected  = True
        self._logger.info("Transport connected", transport=type(self).__name__)
        self._listen_task = asyncio.create_task(self._listen_loop(pubsub))

    async def disconnect(self) -> None:
        """Close both Redis connections cleanly and cancel any pending reconnect."""
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
        await self._close_clients(self._pub_client, self._sub_client)
        self._pub_client = self._sub_client = self._pubsub = None

    @staticmethod
    async def _close_clients(*clients: Any) -> None:
        for client in clients:
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.aclose()

    # ── Pub/Sub ───────────────────────────────────────────────────────────────

    async def publish(self, signal: Signal) -> None:
        if not self._connected or self._pub_client is None:
            raise TransportConnectionError("RedisTransport", "Not connected")
        channel = self._channel(signal.kind)
        payload = self._encode_signal(signal)
        await self._pub_client.publish(channel, payload)

    async def subscribe(self, kind: str, callback: Callback) -> None:
        already = bool(self._subscribers[kind])
        if callback not in self._subscribers[kind]:
            self._subscribers[kind].append(callback)
        if not already and self._pubsub and self._connected:
            await self._pubsub_subscribe(self._pubsub, kind)
            # redis.asyncio PubSub.listen() exits when no channels are subscribed.
            # _listen_loop may have already exited before this first subscription
            # completed (the task runs during the above await). Restart it.
            if self._listen_task is None or self._listen_task.done():
                self._listen_task = asyncio.create_task(self._listen_loop(self._pubsub))

    async def unsubscribe(self, kind: str, callback: Callback) -> None:
        with contextlib.suppress(ValueError):
            self._subscribers[kind].remove(callback)
        if not self._subscribers[kind] and self._pubsub and self._connected:
            with contextlib.suppress(Exception):
                await self._pubsub_unsubscribe(self._pubsub, kind)

    # ── Listener loop ─────────────────────────────────────────────────────────

    async def _listen_loop(self, pubsub: Any = None) -> None:
        """Receive messages from Redis and dispatch to local subscribers."""
        pubsub = pubsub if pubsub is not None else self._pubsub
        if pubsub is None:
            return
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    await self._dispatch(message["data"])
                elif message["type"] == "pmessage":
                    await self._dispatch(message["data"], via_pattern=True)
        except asyncio.CancelledError:
            return
        except Exception:
            self._connected = False
            if not self._stopped:
                self._reconnect_task = asyncio.create_task(self._reconnect())

    async def _dispatch(self, data: bytes, via_pattern: bool = False) -> None:
        """
        Decode one Redis message and call the matching subscribers.

        Redis delivers a publish once per matching subscription: once as a
        'message' for ``SUBSCRIBE <prefix>:<kind>`` and once as a 'pmessage'
        for ``PSUBSCRIBE <prefix>:*``. Each copy serves a disjoint set of
        callbacks, so a callback under both the kind and '*' is called once.
        """
        try:
            signal = self._decode_signal(data)
        except Exception as exc:
            self._logger.warning(
                "Dropped undecodable Redis message",
                error=f"{type(exc).__name__}: {exc}",
            )
            return
        kind_callbacks = list(self._subscribers.get(signal.kind, []))
        star_callbacks = list(self._subscribers.get("*", []))
        if via_pattern:
            callbacks = [cb for cb in star_callbacks if cb not in kind_callbacks]
        elif self._subscribers.get("*") and self._connected:
            callbacks = kind_callbacks          # the 'pmessage' copy serves '*'
        else:
            callbacks = kind_callbacks + star_callbacks
        await self._deliver(signal, callbacks)

    async def _reconnect(self) -> None:
        if self._stopped:
            return
        await asyncio.sleep(self._reconnect_s)
        if self._stopped:
            return
        # Close stale clients before creating new ones to avoid resource leaks.
        # The listen task has already exited (it raised and scheduled us).
        await self._close_clients(self._pub_client, self._sub_client)
        self._pub_client = self._sub_client = self._pubsub = None
        self._listen_task = None
        # _connect_loop() re-subscribes all active channels and starts a fresh
        # listen task; it stops early if disconnect() is called meanwhile.
        try:
            await self._connect_loop()
        except TransportConnectionError as exc:
            self._logger.error("Redis reconnect gave up", error=str(exc))

    @property
    def connected(self) -> bool:
        return self._connected
