"""
arachnite.transport.base
~~~~~~~~~~~~~~~~~~~~~~~~
BaseTransport: the pluggable delivery backend beneath the SignalBus.
Spec reference: Section 10.2.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from arachnite.codec import CodecRegistry, default_registry
from arachnite.logging import StructuredLogger
from arachnite.models import Signal

Callback = Callable[[Signal], Awaitable[None]]

# Wire envelope version
_WIRE_VERSION: int = 1


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
            }
        }

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

    # ── Wire encoding helpers (shared by all transports) ─────────────────────

    def _encode_signal(self, signal: Signal) -> bytes:
        """
        Serialise a Signal to a wire-format bytes envelope.
        Uses msgpack for the outer envelope and the CodecRegistry
        for Signal.value (which may be any type).
        """
        import msgpack  # type: ignore[import-untyped,unused-ignore]

        value_bytes = self._codec.encode(signal.kind, signal.value)
        envelope = {
            "v":   _WIRE_VERSION,
            "src": self._agent_node_id,
            "sig": {
                "source":     signal.source,
                "kind":       signal.kind,
                "value":      value_bytes,
                "confidence": signal.confidence,
                "timestamp":  signal.timestamp,
                "metadata":   signal.metadata,
            },
        }
        return bytes(msgpack.packb(envelope, use_bin_type=True))

    def _decode_signal(self, data: bytes) -> Signal:
        """
        Deserialise a wire-format bytes envelope back to a Signal.
        """
        import msgpack  # type: ignore[import-untyped,unused-ignore]

        envelope = msgpack.unpackb(data, raw=False)
        if envelope.get("v") != _WIRE_VERSION:
            raise ValueError(
                f"Unknown wire protocol version: {envelope.get('v')}"
            )
        sig = envelope["sig"]
        value = self._codec.decode(sig["kind"], sig["value"])
        # Preserve the originating agent in metadata under a reserved key
        # so consumers (the dashboard, mesh-aware instincts) can attribute
        # the signal back to its source agent.  The local bus never sets
        # this key, so its presence means "delivered over the wire".
        metadata = dict(sig.get("metadata") or {})
        origin = envelope.get("src")
        if origin:
            metadata.setdefault("__origin_agent__", origin)
        return Signal(
            source     = sig["source"],
            kind       = sig["kind"],
            value      = value,
            confidence = sig["confidence"],
            timestamp  = sig["timestamp"],
            metadata   = metadata,
        )

    @property
    def agent_node_id(self) -> str:
        return self._agent_node_id

    def __repr__(self) -> str:
        return f"{type(self).__name__}(agent_node_id={self._agent_node_id!r})"
