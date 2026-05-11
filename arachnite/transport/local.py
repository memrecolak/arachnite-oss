"""
arachnite.transport.local
~~~~~~~~~~~~~~~~~~~~~~~~~
LocalTransport: in-memory asyncio queues. Default for single-process deployments.
Zero network overhead, no external dependencies.
Spec reference: Section 10.2.
"""

from __future__ import annotations

import contextlib
from collections import defaultdict
from collections.abc import Awaitable, Callable
from itertools import chain

from arachnite.models import Signal
from arachnite.transport.base import BaseTransport

Callback = Callable[[Signal], Awaitable[None]]


class LocalTransport(BaseTransport):
    """
    Default in-process transport using asyncio callbacks.

    Signals are delivered synchronously within the same event loop.
    No serialisation, no network, no external dependencies.
    Use this for single-device deployments.

    Spec reference: Section 10.2.
    """

    def __init__(self, agent_node_id: str = "local") -> None:
        super().__init__(agent_node_id=agent_node_id)
        self._subscribers: dict[str, list[Callback]] = defaultdict(list)
        self._connected = False

    async def connect(self) -> None:
        self._connected = True
        self._logger.info("Transport connected", transport=type(self).__name__)

    async def disconnect(self) -> None:
        self._logger.info("Transport disconnected", transport=type(self).__name__)
        self._connected = False

    async def publish(self, signal: Signal) -> None:
        """Deliver a signal to all matching local subscribers."""
        for cb in chain(
            self._subscribers.get(signal.kind, ()),
            self._subscribers.get("*", ()),
        ):
            await cb(signal)

    async def subscribe(self, kind: str, callback: Callback) -> None:
        if callback not in self._subscribers[kind]:
            self._subscribers[kind].append(callback)

    async def unsubscribe(self, kind: str, callback: Callback) -> None:
        with contextlib.suppress(ValueError):
            self._subscribers[kind].remove(callback)

    @property
    def connected(self) -> bool:
        return self._connected
