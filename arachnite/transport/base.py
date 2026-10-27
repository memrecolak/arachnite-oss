"""
arachnite.transport.base
~~~~~~~~~~~~~~~~~~~~~~~~
BaseTransport: the pluggable delivery backend beneath the SignalBus.
Spec reference: Section 10.2.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from arachnite.codec import CodecRegistry, default_registry
from arachnite.logging import StructuredLogger
from arachnite.models import (
    NodeFaultSignal,
    NodeState,
    Signal,
    StateUpdateSignal,
    SupervisorSignal,
)

Callback = Callable[[Signal], Awaitable[None]]

# Wire envelope version
_WIRE_VERSION: int = 1

# Signal subclasses whose extra fields travel in the envelope's optional
# 'type'/'ext' entries. Most specific first: the first isinstance match wins,
# so a user subclass travels as its nearest framework ancestor.
_WIRE_SUBCLASSES: tuple[type[Signal], ...] = (
    NodeFaultSignal, SupervisorSignal, StateUpdateSignal,
)
_WIRE_TYPES: dict[str, type[Signal]] = {c.__name__: c for c in _WIRE_SUBCLASSES}


class BaseTransport(ABC):
    """
    Pluggable delivery backend for the SignalBus.

    Handles serialisation, network delivery, and deserialisation.
    All methods are async. The transport sits beneath the SignalBus:
    nodes never interact with the transport directly.

    Wire envelope format (msgpack-encoded dict):
        {
            'v':   1,                   # protocol version
            'src': 'agent-node-id',     # originating AgentNode
            'sig': {                    # serialised Signal fields
                'source':     str,
                'kind':       str,
                'value':      bytes,    # codec-encoded
                'confidence': float,
                'timestamp':  float,
                'metadata':   dict,
                'type':       str,      # optional: Signal subclass name
                'ext':        dict,     # optional: that subclass's fields
            }
        }

    ``type``/``ext`` carry SupervisorSignal, NodeFaultSignal and
    StateUpdateSignal fields (node_id/states/error type and message;
    key/state_value/delete), so the receiver rebuilds the same subclass.
    Decoders that predate them ignore the two keys and build a plain Signal.

    Subscription semantics shared by every transport: kind ``'*'`` receives
    every signal, and a callback subscribed under both a kind and ``'*'`` is
    called once per signal, not twice. A raising callback is logged and does
    not stop delivery to the others.

    Spec reference: Section 10.2, 10.5.
    """

    def __init__(
        self,
        agent_node_id: str = "local",
        codec_registry: CodecRegistry | None = None,
    ) -> None:
        self._agent_node_id = agent_node_id
        self._codec         = codec_registry or default_registry()
        self._logger        = StructuredLogger(
            node_id=type(self).__name__,
            agent_node_id=agent_node_id,
        )

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the transport backend."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the connection cleanly."""

    @abstractmethod
    async def publish(self, signal: Signal) -> None:
        """Serialise and deliver a signal to all subscribers of signal.kind."""

    @abstractmethod
    async def subscribe(self, kind: str, callback: Callback) -> None:
        """Register an async callback for signals of the given kind."""

    @abstractmethod
    async def unsubscribe(self, kind: str, callback: Callback) -> None:
        """Remove a previously registered callback."""

    # ── Delivery helpers (shared by all transports) ──────────────────────────

    @staticmethod
    def _unique(callbacks: Iterable[Callback]) -> list[Callback]:
        """Return ``callbacks`` without repeats, first occurrence kept."""
        out: list[Callback] = []
        for cb in callbacks:
            if cb not in out:
                out.append(cb)
        return out

    async def _deliver(self, signal: Signal, callbacks: Iterable[Callback]) -> None:
        """Call each callback once; a raising one is logged, the rest still run."""
        for cb in self._unique(callbacks):
            try:
                await cb(signal)
            except Exception as exc:  # noqa: BLE001
                self._logger.warning(
                    "Transport subscriber raised; continuing delivery",
                    kind=signal.kind,
                    error=f"{type(exc).__name__}: {exc}",
                )

    # ── Wire encoding helpers (shared by all transports) ─────────────────────

    def _encode_signal(self, signal: Signal) -> bytes:
        """
        Serialise a Signal to a wire-format bytes envelope.
        Uses msgpack for the outer envelope and the CodecRegistry
        for Signal.value (which may be any type).
        """
        import msgpack  # type: ignore[import-untyped,unused-ignore]

        value_bytes = self._codec.encode(signal.kind, signal.value)
        sig: dict[str, Any] = {
            "source":     signal.source,
            "kind":       signal.kind,
            "value":      value_bytes,
            "confidence": signal.confidence,
            "timestamp":  signal.timestamp,
            "metadata":   signal.metadata,
        }
        for cls in _WIRE_SUBCLASSES:
            if isinstance(signal, cls):
                sig["type"] = cls.__name__
                sig["ext"] = self._encode_ext(signal)
                break
        envelope = {"v": _WIRE_VERSION, "src": self._agent_node_id, "sig": sig}
        return bytes(msgpack.packb(envelope, use_bin_type=True))

    def _encode_ext(self, signal: Signal) -> dict[str, Any]:
        """The subclass fields of ``signal`` in wire form."""
        if isinstance(signal, StateUpdateSignal):
            return {
                "key":         signal.key,
                "state_value": self._codec.encode(signal.kind, signal.state_value),
                "delete":      signal.delete,
            }
        if not isinstance(signal, SupervisorSignal):
            return {}
        ext: dict[str, Any] = {
            "node_id":        signal.node_id,
            "previous_state": signal.previous_state.value,
            "current_state":  signal.current_state.value,
            "restart_count":  signal.restart_count,
        }
        # fault_error (an exception object) does not cross the wire; its type
        # and message do, as NodeFaultSignal.error_type/error_message.
        if isinstance(signal, NodeFaultSignal):
            ext["error_type"] = signal.error_type
            ext["error_message"] = signal.error_message
        return ext

    def _decode_signal(self, data: bytes) -> Signal:
        """
        Deserialise a wire-format bytes envelope back to a Signal.
        """
        import msgpack  # type: ignore[import-untyped,unused-ignore]

        # strict_map_key=False: the sender's packb() accepts int/float dict
        # keys (in metadata), so the receiver must as well.
        envelope = msgpack.unpackb(data, raw=False, strict_map_key=False)
        if envelope.get("v") != _WIRE_VERSION:
            raise ValueError(
                f"Unknown wire protocol version: {envelope.get('v')}"
            )
        sig = envelope["sig"]
        value = self._codec.decode_network(sig["kind"], sig["value"])
        # Preserve the originating agent in metadata under a reserved key
        # so consumers (the dashboard, mesh-aware instincts) can attribute
        # the signal back to its source agent.  The local bus never sets
        # this key, so its presence means "delivered over the wire".
        metadata = dict(sig.get("metadata") or {})
        origin = envelope.get("src")
        if origin:
            metadata.setdefault("__origin_agent__", origin)
        fields: dict[str, Any] = {
            "source":     sig["source"],
            "kind":       sig["kind"],
            "value":      value,
            "confidence": sig["confidence"],
            "timestamp":  sig["timestamp"],
            "metadata":   metadata,
        }
        cls = _WIRE_TYPES.get(str(sig.get("type", "")))
        if cls is None:
            return Signal(**fields)
        ext = sig.get("ext") or {}
        if cls is StateUpdateSignal:
            state_value = (
                self._codec.decode_network(sig["kind"], ext["state_value"])
                if "state_value" in ext else None
            )
            return StateUpdateSignal(
                **fields,
                key=str(ext.get("key", "")),
                state_value=state_value,
                delete=bool(ext.get("delete", False)),
            )
        sup: dict[str, Any] = {
            "node_id":        str(ext.get("node_id", "")),
            "previous_state": NodeState(ext.get("previous_state", "starting")),
            "current_state":  NodeState(ext.get("current_state", "running")),
            "restart_count":  int(ext.get("restart_count", 0)),
        }
        if cls is NodeFaultSignal:
            return NodeFaultSignal(
                **fields, **sup,
                error_type=str(ext.get("error_type", "")),
                error_message=str(ext.get("error_message", "")),
            )
        return SupervisorSignal(**fields, **sup)

    @property
    def agent_node_id(self) -> str:
        return self._agent_node_id

    def __repr__(self) -> str:
        return f"{type(self).__name__}(agent_node_id={self._agent_node_id!r})"
