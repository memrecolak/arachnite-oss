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

from arachnite.exceptions import SignalBusError
from arachnite.models import Signal

Callback = Callable[[Signal], Awaitable[None]]


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
    """

    def __init__(self) -> None:
        # kind -> list of callbacks
        self._subscribers: dict[str, list[Callback]] = defaultdict(list)
        self._subscriber_set: dict[str, set[Callback]] = defaultdict(set)

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
        After all callbacks complete, a SignalBusError is raised if any
        callback failed, carrying the first exception as .cause.
        """
        callbacks = list(chain(
            self._subscribers.get(signal.kind, ()),
            self._subscribers.get("*", ()),
        ))
        if not callbacks:
            return

        errors: list[BaseException] = []

        async def _safe_call(cb: Callback) -> None:
            try:
                await cb(signal)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        await asyncio.gather(*(_safe_call(cb) for cb in callbacks))

        if errors:
            raise SignalBusError(
                f"{len(errors)} subscriber(s) raised during publish "
                f"of signal kind='{signal.kind}'",
                cause=errors[0],
            )

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
