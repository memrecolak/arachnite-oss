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
from arachnite.logging import StructuredLogger
from arachnite.models import Signal

if TYPE_CHECKING:
    from arachnite.transport.base import BaseTransport

Callback = Callable[[Signal], Awaitable[None]]

#: Synchronous observer of signals delivered from the transport (remote ones).
RemoteListener = Callable[[Signal], None]

# Metadata key stamped on a signal when it is forwarded to the transport, so
# that the publisher can recognise and drop its own message when a broker (or a
# shared LocalTransport) echoes it back. The networked transports also set this
# on decode (see BaseTransport._decode_signal); setdefault keeps whichever value
# was assigned first, so the originating agent id is preserved end to end.
ORIGIN_AGENT_KEY = "__origin_agent__"

# First and maximum delay between retries of a failed transport subscription.
_TRANSPORT_RETRY_S = 1.0
_TRANSPORT_RETRY_MAX_S = 30.0


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
    - ``agent_node_id`` defaults to the transport's ``agent_node_id`` (or
      ``"local"`` without a transport). It is the echo-suppression key, so it
      must be unique per AgentNode across the mesh.
    - After the transport is connected, call ``await bus.start_transport()``
      once so the bus subscribes its inbound handler for every kind that has a
      local subscriber. ``AgentNode.start()`` does this automatically, and
      ``AgentNode.stop()`` calls ``stop_transport()``.
    - Transport interest follows local interest: a kind's inbound handler is
      subscribed on the transport while the kind has a local subscriber (or
      when a remote listener is attached, see ``add_remote_listener``), and
      withdrawn when the last one unsubscribes. When '*' is wanted, only '*'
      is subscribed on the transport, so each remote signal arrives once. A
      transport subscription that fails is logged and retried with backoff.

    Egress allowlist (``wire_kinds``):
    - By default a transport-bridged bus forwards *every* published kind to the
      transport, so anything published reaches the broker. Pass ``wire_kinds``
      to restrict egress to an explicit allowlist: only those kinds (plus '*'
      if listed) leave for the transport; all other kinds stay local even
      though a transport is attached. ``wire_kinds=None`` (the default) keeps
      the historical publish-everything behaviour; an empty set forwards
      nothing. This gate is egress-only — inbound delivery is unaffected, so a
      node can still receive kinds it chooses not to publish onto the wire.
    """

    def __init__(
        self,
        transport: BaseTransport | None = None,
        agent_node_id: str | None = None,
        wire_kinds: set[str] | None = None,
    ) -> None:
        # kind -> list of callbacks
        self._subscribers: dict[str, list[Callback]] = defaultdict(list)
        self._subscriber_set: dict[str, set[Callback]] = defaultdict(set)

        # Transport bridge (spec §10). When None this bus is purely in-process.
        self._transport = transport
        if agent_node_id is None:
            # Default to the transport's id: a shared literal default would make
            # every peer's signals look like this bus's own echo.
            agent_node_id = transport.agent_node_id if transport is not None else "local"
        self._agent_node_id = agent_node_id
        self._transport_kinds: set[str] = set()    # kinds we want from the wire
        self._transport_active: set[str] = set()   # kinds subscribed on transport
        self._transport_started = False
        self._remote_listeners: list[RemoteListener] = []
        self._transport_lock = asyncio.Lock()
        self._sync_task: asyncio.Task[None] | None = None
        self._sync_wakeup = asyncio.Event()
        self._logger = StructuredLogger(node_id="SignalBus", agent_node_id=agent_node_id)

        # Egress allowlist. None => forward every kind (back-compat default);
        # a set => only these kinds (or '*') may egress to the transport.
        self._wire_kinds: frozenset[str] | None = (
            None if wire_kinds is None else frozenset(wire_kinds)
        )

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
        if self._transport is not None:
            self._transport_kinds.add(kind)
            self._schedule_transport_sync()

    def unsubscribe(self, kind: str, callback: Callback) -> None:
        """Remove a previously registered callback."""
        with contextlib.suppress(ValueError):
            self._subscribers[kind].remove(callback)
        self._subscriber_set[kind].discard(callback)
        if self._transport is not None and not self._subscribers[kind]:
            # Last local subscriber gone: withdraw the transport interest too.
            self._transport_kinds.discard(kind)
            self._schedule_transport_sync()

    def clear(self) -> None:
        """Remove all subscribers. Useful between test cases."""
        self._subscribers.clear()
        self._subscriber_set.clear()
        if self._transport is not None:
            self._transport_kinds.clear()
            self._schedule_transport_sync()

    # ── Publishing ────────────────────────────────────────────────────────────

    async def publish(self, signal: Signal) -> None:
        """
        Broadcast a signal to all subscribers of its kind and to '*' subscribers.

        All callbacks are invoked concurrently. If any callback raises,
        the exception is captured; remaining callbacks still run.

        When a transport is attached, the signal is also forwarded to the
        transport for delivery to other AgentNodes — unless an egress allowlist
        (``wire_kinds``) is configured and this kind is not on it, in which case
        the signal is dispatched locally only. After all local callbacks and the
        transport forward complete, a SignalBusError is raised if any of them
        failed, carrying the first exception as .cause.
        """
        errors = await self._dispatch_local(signal)

        if self._transport is not None and self._may_egress(signal.kind):
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

        # return_exceptions: a callback that raises CancelledError itself
        # (it awaited something that was cancelled) is a subscriber error,
        # not a cancel of the publisher; a cancel of the publisher still
        # propagates out of gather().
        outcomes = await asyncio.gather(
            *(_safe_call(cb) for cb in callbacks), return_exceptions=True,
        )
        errors.extend(o for o in outcomes if isinstance(o, BaseException))
        return errors

    def _may_egress(self, kind: str) -> bool:
        """Return True if a signal of this kind may be forwarded to the transport.

        With no allowlist (``wire_kinds is None``) every kind egresses, matching
        the historical publish-everything behaviour. With an allowlist, only
        listed kinds — or '*' as a catch-all — egress; every other kind stays
        local. Egress-only: this does not affect inbound delivery.
        """
        if self._wire_kinds is None:
            return True
        return kind in self._wire_kinds or "*" in self._wire_kinds

    # ── Transport bridge ──────────────────────────────────────────────────────

    async def start_transport(self) -> None:
        """
        Subscribe the bus's inbound handler on the transport for every kind that
        currently has a local subscriber. Call once after the transport is
        connected (AgentNode.start() does this). Idempotent and a no-op when no
        transport is attached. A subscription the transport refuses is logged
        and retried in the background.
        """
        if self._transport is None:
            return
        self._transport_started = True
        if await self._sync_transport():
            self._schedule_transport_sync(force=True)

    async def stop_transport(self) -> None:
        """
        Withdraw every transport subscription of this bus and stop retrying.

        Call before disconnecting the transport (``AgentNode.stop()`` does), so
        a transport shared with other buses stops delivering to this one. A
        later ``start_transport()`` subscribes again. No-op without a transport.
        """
        if self._transport is None:
            return
        self._transport_started = False
        task, self._sync_task = self._sync_task, None
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        async with self._transport_lock:
            for kind in sorted(self._transport_active):
                with contextlib.suppress(Exception):
                    await self._transport.unsubscribe(kind, self._on_transport_signal)
            self._transport_active.clear()

    def add_remote_listener(self, listener: RemoteListener) -> None:
        """
        Observe every signal that arrives from another AgentNode.

        ``listener`` is called synchronously for each transport-delivered
        signal that passes echo suppression (never for a local publish), in
        addition to the normal dispatch to local subscribers. While a listener
        is attached the bus subscribes '*' on the transport, so every kind any
        peer publishes is received. ``AgentNode`` uses this to feed remote
        signals into its runtime's next tick and its mesh health view.
        """
        if listener not in self._remote_listeners:
            self._remote_listeners.append(listener)
        if self._transport is not None:
            self._schedule_transport_sync()

    def remove_remote_listener(self, listener: RemoteListener) -> None:
        """Detach a listener added with ``add_remote_listener``."""
        with contextlib.suppress(ValueError):
            self._remote_listeners.remove(listener)
        if self._transport is not None:
            self._schedule_transport_sync()

    def is_remote(self, signal: Signal) -> bool:
        """True if ``signal`` was published by another AgentNode's bus."""
        origin = signal.metadata.get(ORIGIN_AGENT_KEY)
        return origin is not None and origin != self._agent_node_id

    def _transport_interest(self) -> set[str]:
        interest = set(self._transport_kinds)
        if self._remote_listeners:
            interest.add("*")
        return interest

    def _transport_settled(self) -> bool:
        interest = self._transport_interest()
        if "*" in interest:
            return self._transport_active == {"*"}
        return self._transport_active == interest

    def _schedule_transport_sync(self, force: bool = False) -> None:
        """Reconcile transport subscriptions in the background, if needed."""
        if not self._transport_started or (not force and self._transport_settled()):
            return
        if self._sync_task is not None and not self._sync_task.done():
            self._sync_wakeup.set()   # cut a retry backoff short
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return   # no loop: start_transport() reconciles later
        self._sync_task = loop.create_task(self._sync_loop())

    async def _sync_loop(self) -> None:
        """Reconcile until settled; a failed subscription is retried with backoff."""
        delay = _TRANSPORT_RETRY_S
        while self._transport_started:
            if not await self._sync_transport():
                if self._transport_settled():
                    return
                continue          # interest changed while we were syncing
            self._sync_wakeup.clear()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._sync_wakeup.wait(), timeout=delay)
            delay = min(delay * 2, _TRANSPORT_RETRY_MAX_S)

    async def _sync_transport(self) -> bool:
        """
        Bring the transport subscriptions in line with the local interest.

        Returns True if a subscription failed (it stays pending for a retry).
        With '*' wanted only '*' is subscribed; kind subscriptions are dropped
        once '*' is active (kept, or added, while it is not, so a failing
        wildcard does not cut off the kinds that do work).
        """
        transport = self._transport
        if transport is None:
            return False
        async with self._transport_lock:
            failed = False
            interest = self._transport_interest()
            if "*" in interest and "*" not in self._transport_active:
                failed |= not await self._subscribe_kind(transport, "*")
            if "*" not in self._transport_active:
                for kind in sorted(interest - self._transport_active - {"*"}):
                    failed |= not await self._subscribe_kind(transport, kind)

            stale = {k for k in self._transport_active if k not in interest}
            if "*" in self._transport_active:
                stale |= self._transport_active - {"*"}
            for kind in sorted(stale):
                self._transport_active.discard(kind)
                with contextlib.suppress(Exception):
                    await transport.unsubscribe(kind, self._on_transport_signal)
            return failed

    async def _subscribe_kind(self, transport: BaseTransport, kind: str) -> bool:
        """Subscribe the inbound handler for one kind; False (logged) on failure."""
        try:
            await transport.subscribe(kind, self._on_transport_signal)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning(
                "Transport subscription failed; will retry",
                kind=kind,
                error=f"{type(exc).__name__}: {exc}",
            )
            return False
        self._transport_active.add(kind)
        return True

    async def _on_transport_signal(self, signal: Signal) -> None:
        """
        Dispatch a signal received from the transport to local subscribers.

        Drops signals that originated from this same agent (our own publish
        echoed back by the broker or a shared transport). Never forwards back
        to the transport, so there is no delivery loop. Remote listeners see
        the signal before the local subscribers do.
        """
        if signal.metadata.get(ORIGIN_AGENT_KEY) == self._agent_node_id:
            return
        for listener in list(self._remote_listeners):
            try:
                listener(signal)
            except Exception as exc:  # noqa: BLE001
                self._logger.warning(
                    "Remote listener raised; ignoring",
                    kind=signal.kind,
                    error=f"{type(exc).__name__}: {exc}",
                )
        await self._dispatch_local(signal)

    async def publish_many(self, signals: list[Signal]) -> None:
        """
        Publish a batch of signals concurrently.

        Each signal is published independently; errors in one signal's
        subscribers do not prevent others from being delivered. Every publish
        completes before the first SignalBusError (if any) is re-raised.
        """
        if not signals:
            return
        outcomes = await asyncio.gather(
            *(self.publish(s) for s in signals), return_exceptions=True
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome

    # ── Introspection ─────────────────────────────────────────────────────────

    @property
    def agent_node_id(self) -> str:
        """The AgentNode id this bus stamps on (and echo-filters by)."""
        return self._agent_node_id

    def subscriber_count(self, kind: str) -> int:
        """Return the number of subscribers for a given kind."""
        return len(self._subscribers.get(kind, []))

    def subscribed_kinds(self) -> list[str]:
        """Return all kinds that have at least one subscriber."""
        return [k for k, v in self._subscribers.items() if v]

    def wire_kinds(self) -> set[str] | None:
        """Return the egress allowlist, or None when every kind is forwarded."""
        return None if self._wire_kinds is None else set(self._wire_kinds)

    def __repr__(self) -> str:
        total = sum(len(v) for v in self._subscribers.values())
        return (
            f"SignalBus("
            f"kinds={len(self._subscribers)}, "
            f"total_subscribers={total})"
        )
