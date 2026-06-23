"""
arachnite.bus
~~~~~~~~~~~~~
SignalBus: the publish-subscribe channel connecting all nodes.
Spec reference: Section 4.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from collections.abc import Awaitable, Callable
from itertools import chain
from typing import TYPE_CHECKING

from arachnite.exceptions import SignalBusError
from arachnite.models import Signal

if TYPE_CHECKING:
    from arachnite.transport.base import BaseTransport

Callback = Callable[[Signal], Awaitable[None]]

# Metadata key stamped on a signal when it is forwarded to the transport, so
# that the publisher can recognise and drop its own message when a broker (or a
# shared LocalTransport) echoes it back. The networked transports also set this
# on decode (see BaseTransport._decode_signal); setdefault keeps whichever value
# was assigned first, so the originating agent id is preserved end to end.
ORIGIN_AGENT_KEY = "__origin_agent__"


class SignalBus:
    """
    The central nervous system of the framework.

    Nodes communicate exclusively through the bus — they never hold direct
    references to each other. Publishing a signal calls all registered
    subscribers for that kind concurrently.

    Design notes (spec Section 4.2):
    - All callbacks are async.
    - publish() dispatches concurrently via asyncio.gather().
    - The bus does not persist signals.
    - Wildcard kind '*' receives every signal regardless of type.
    - Exceptions in subscribers are caught and re-raised as SignalBusError
      after all other subscribers have been notified.

    Distributed delivery (spec §10):
    - Pass a ``transport`` to bridge this bus to other AgentNodes. publish()
      then also forwards each signal to the transport, and signals arriving
      from the transport are dispatched to local subscribers — so nodes keep
      calling publish()/subscribe() exactly as before. ``transport=None`` (the
      default) is a pure in-process bus with no behaviour change.
    - After the transport is connected, call ``await bus.start_transport()``
      once so the bus subscribes its inbound handler for every kind that has a
      local subscriber. ``AgentNode.start()`` does this automatically.
    """

    def __init__(
        self,
        transport: BaseTransport | None = None,
        agent_node_id: str = "local",
    ) -> None:
        # kind -> list of callbacks
        self._subscribers: dict[str, list[Callback]] = defaultdict(list)
        self._subscriber_set: dict[str, set[Callback]] = defaultdict(set)

        # Transport bridge (spec §10). When None this bus is purely in-process.
        self._transport = transport
        self._agent_node_id = agent_node_id
        self._transport_kinds: set[str] = set()    # kinds we want from the wire
        self._transport_active: set[str] = set()   # kinds subscribed on transport
        self._transport_started = False

    # ── Subscription management ───────────────────────────────────────────────

    def subscribe(self, kind: str, callback: Callback) -> None:
        """
        Register an async callback for a given signal kind.

        Use kind='*' to receive all signals regardless of type.
        """
        bucket = self._subscriber_set[kind]
        if callback not in bucket:
            bucket.add(callback)
            self._subscribers[kind].append(callback)

        # Mirror the interest onto the transport so remote signals of this kind
        # are delivered here. Registration is async; if the transport is already
        # started we schedule it, otherwise start_transport() flushes it later.
        if self._transport is not None and kind not in self._transport_kinds:
            self._transport_kinds.add(kind)
            if self._transport_started:
                with contextlib.suppress(RuntimeError):
                    asyncio.get_running_loop().create_task(
                        self._activate_transport_kind(kind)
                    )

    def unsubscribe(self, kind: str, callback: Callback) -> None:
        """Remove a previously registered callback."""
        with contextlib.suppress(ValueError):
            self._subscribers[kind].remove(callback)
        self._subscriber_set[kind].discard(callback)

    def clear(self) -> None:
        """Remove all subscribers. Useful between test cases."""
        self._subscribers.clear()
        self._subscriber_set.clear()

    # ── Publishing ────────────────────────────────────────────────────────────

    async def publish(self, signal: Signal) -> None:
        """
        Broadcast a signal to all subscribers of its kind and to '*' subscribers.

        All callbacks are invoked concurrently. If any callback raises,
        the exception is captured; remaining callbacks still run.

        When a transport is attached, the signal is also forwarded to the
        transport for delivery to other AgentNodes. After all local callbacks
        and the transport forward complete, a SignalBusError is raised if any
        of them failed, carrying the first exception as .cause.
        """
        errors = await self._dispatch_local(signal)

        if self._transport is not None:
            # Tag the originating agent so we can drop our own message if the
            # transport echoes it back to us (broker echo / shared transport).
            signal.metadata.setdefault(ORIGIN_AGENT_KEY, self._agent_node_id)
            try:
                await self._transport.publish(signal)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        if errors:
            raise SignalBusError(
                f"{len(errors)} subscriber(s) raised during publish "
                f"of signal kind='{signal.kind}'",
                cause=errors[0],
            )

    async def _dispatch_local(self, signal: Signal) -> list[BaseException]:
        """Invoke all local subscribers for this signal, collecting errors.

        Used by both publish() (the local leg) and the inbound transport
        handler. Returns the list of exceptions raised by callbacks so the
        caller can decide whether to surface them.
        """
        callbacks = list(chain(
            self._subscribers.get(signal.kind, ()),
            self._subscribers.get("*", ()),
        ))
        if not callbacks:
            return []

        errors: list[BaseException] = []

        async def _safe_call(cb: Callback) -> None:
            try:
                await cb(signal)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        await asyncio.gather(*(_safe_call(cb) for cb in callbacks))
        return errors

    # ── Transport bridge ──────────────────────────────────────────────────────

    async def start_transport(self) -> None:
        """
        Subscribe the bus's inbound handler on the transport for every kind that
        currently has a local subscriber. Call once after the transport is
        connected (AgentNode.start() does this). Idempotent and a no-op when no
        transport is attached.
        """
        if self._transport is None:
            return
        self._transport_started = True
        for kind in list(self._transport_kinds):
            await self._activate_transport_kind(kind)

    async def _activate_transport_kind(self, kind: str) -> None:
        """Subscribe the inbound handler for one kind on the transport (once)."""
        if self._transport is None or kind in self._transport_active:
            return
        self._transport_active.add(kind)
        await self._transport.subscribe(kind, self._on_transport_signal)

    async def _on_transport_signal(self, signal: Signal) -> None:
        """
        Dispatch a signal received from the transport to local subscribers.

        Drops signals that originated from this same agent (our own publish
        echoed back by the broker or a shared transport). Never forwards back
        to the transport, so there is no delivery loop.
        """
        if signal.metadata.get(ORIGIN_AGENT_KEY) == self._agent_node_id:
            return
        await self._dispatch_local(signal)

    async def publish_many(self, signals: list[Signal]) -> None:
        """
        Publish a batch of signals concurrently.

        Each signal is published independently; errors in one signal's
        subscribers do not prevent others from being delivered.
        """
        if not signals:
            return
        await asyncio.gather(*(self.publish(s) for s in signals))

    # ── Introspection ─────────────────────────────────────────────────────────

    def subscriber_count(self, kind: str) -> int:
        """Return the number of subscribers for a given kind."""
        return len(self._subscribers.get(kind, []))

    def subscribed_kinds(self) -> list[str]:
        """Return all kinds that have at least one subscriber."""
        return [k for k, v in self._subscribers.items() if v]

    def __repr__(self) -> str:
        total = sum(len(v) for v in self._subscribers.values())
        return (
            f"SignalBus("
            f"kinds={len(self._subscribers)}, "
            f"total_subscribers={total})"
        )
