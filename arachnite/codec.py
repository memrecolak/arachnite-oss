"""
arachnite.codec
~~~~~~~~~~~~~~~
SignalCodec, CodecRegistry, and built-in codec implementations.
Spec reference: Section 14.
"""

from __future__ import annotations

import json
import pickle
from abc import ABC, abstractmethod
from typing import Any

from arachnite.exceptions import UnsafeCodecError

try:
    import msgpack  # type: ignore[import-untyped,unused-ignore]
    _MSGPACK_AVAILABLE = True
except ImportError:
    _MSGPACK_AVAILABLE = False

try:
    import numpy as np  # type: ignore[import-untyped,unused-ignore]
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False


# ══════════════════════════════════════════════════════════════════════════════
# Abstract base
# ══════════════════════════════════════════════════════════════════════════════

class SignalCodec(ABC):
    """
    Handles serialisation and deserialisation of Signal.value for a specific
    signal kind when crossing a network transport boundary.
    Spec reference: Section 14.1.
    """

    network_safe: bool = True
    """Whether this codec is safe for use over network transports"""

    @abstractmethod
    def encode(self, value: Any) -> bytes:
        """Serialise Signal.value to bytes for wire transmission."""

    @abstractmethod
    def decode(self, data: bytes) -> Any:
        """Deserialise bytes back to the original value type."""


# ══════════════════════════════════════════════════════════════════════════════
# Built-in codecs
# ══════════════════════════════════════════════════════════════════════════════

class MsgpackCodec(SignalCodec):
    """
    Default codec for primitive values (int, float, str, list, dict).
    Compact binary encoding using the msgpack format.
    Requires: msgpack
    """

    def encode(self, value: Any) -> bytes:
        if not _MSGPACK_AVAILABLE:
            raise ImportError(
                "MsgpackCodec requires 'msgpack'. Install with: pip install msgpack"
            )
        return msgpack.packb(value, use_bin_type=True)  # type: ignore[no-any-return]

    def decode(self, data: bytes) -> Any:
        if not _MSGPACK_AVAILABLE:
            raise ImportError(
                "MsgpackCodec requires 'msgpack'. Install with: pip install msgpack"
            )
        return msgpack.unpackb(data, raw=False)


class JSONCodec(SignalCodec):
    """
    Human-readable JSON fallback codec.
    Slower than msgpack but useful for debugging and interoperability.
    """

    def encode(self, value: Any) -> bytes:
        return json.dumps(value, default=str).encode("utf-8")

    def decode(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


class PickleCodec(SignalCodec):
    """
    Arbitrary Python object codec using pickle.

    WARNING: Only use in trusted mesh environments. Pickle deserialisation
    of untrusted data is a security vulnerability.
    """

    network_safe: bool = False

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self._protocol = protocol

    def encode(self, value: Any) -> bytes:
        return pickle.dumps(value, protocol=self._protocol)

    def decode(self, data: bytes) -> Any:
        return pickle.loads(data)  # noqa: S301


class NumpyCodec(SignalCodec):
    """
    Codec for numpy ndarrays. Encodes shape, dtype, and raw bytes.
    Requires: numpy
    """

    def encode(self, value: Any) -> bytes:
        if not _NUMPY_AVAILABLE:
            raise ImportError(
                "NumpyCodec requires 'numpy'. Install with: pip install numpy"
            )
        arr = np.asarray(value)
        # Header: shape length (1 byte), shape ints (8 bytes each), dtype string
        shape_bytes = len(arr.shape).to_bytes(1, "little")
        dims_bytes  = b"".join(d.to_bytes(8, "little") for d in arr.shape)
        dtype_str   = arr.dtype.str.encode("ascii").ljust(16, b"\x00")[:16]
        result: bytes = shape_bytes + dims_bytes + dtype_str + bytes(arr.tobytes())
        return result

    def decode(self, data: bytes) -> Any:
        if not _NUMPY_AVAILABLE:
            raise ImportError(
                "NumpyCodec requires 'numpy'. Install with: pip install numpy"
            )
        ndim      = data[0]
        offset    = 1
        shape     = tuple(
            int.from_bytes(data[offset + i*8 : offset + (i+1)*8], "little")
            for i in range(ndim)
        )
        offset   += ndim * 8
        dtype_str = data[offset : offset + 16].rstrip(b"\x00").decode("ascii")
        offset   += 16
        return np.frombuffer(data[offset:], dtype=np.dtype(dtype_str)).reshape(shape)


# ══════════════════════════════════════════════════════════════════════════════
# CodecRegistry
# ══════════════════════════════════════════════════════════════════════════════

class CodecRegistry:
    """
    Maps signal kinds to SignalCodec instances.

    Registration order matters: the first matching kind wins.
    Wildcard '*' applies to all unregistered kinds.
    Spec reference: Section 14.2.

    Usage::

        registry = CodecRegistry()
        registry.register('visual', NumpyCodec())
        registry.register('thermal', MsgpackCodec())
        registry.register('*', MsgpackCodec())   # fallback
    """

    def __init__(self) -> None:
        self._codecs: dict[str, SignalCodec] = {}
        # Install a default msgpack fallback if available, else JSON
        if _MSGPACK_AVAILABLE:
            self._codecs["*"] = MsgpackCodec()
        else:
            self._codecs["*"] = JSONCodec()

    def register(self, kind: str, codec: SignalCodec) -> None:
        """
        Register a codec for a signal kind.

        Use kind='*' to set the fallback for all unregistered kinds.
        """
        self._codecs[kind] = codec

    def check_network_safety(self, transport_name: str) -> None:
        """
        Check all registered codecs for network safety.

        Raises UnsafeCodecError if any codec with network_safe=False is
        registered. Call this before establishing a network transport
        connection to prevent remote code execution via unsafe codecs
        such as PickleCodec.
        """
        for _kind, codec in self._codecs.items():
            if not codec.network_safe:
                raise UnsafeCodecError(
                    f"{type(codec).__name__} is not safe for network transport "
                    f"'{transport_name}'. PickleCodec can execute arbitrary code "
                    f"when deserialising untrusted data. Use MsgpackCodec or "
                    f"JSONCodec instead, or set PickleCodec.network_safe = True "
                    f"if you accept the risk."
                )

    def _get_codec(self, kind: str) -> SignalCodec:
        if kind in self._codecs:
            return self._codecs[kind]
        if "*" in self._codecs:
            return self._codecs["*"]
        raise KeyError(
            f"No codec registered for signal kind '{kind}' and no wildcard '*' fallback."
        )

    def encode(self, kind: str, value: Any) -> bytes:
        """Encode a Signal.value using the codec registered for *kind*."""
        return self._get_codec(kind).encode(value)

    def decode(self, kind: str, data: bytes) -> Any:
        """Decode bytes back to a Signal.value using the codec for *kind*."""
        return self._get_codec(kind).decode(data)

    def __repr__(self) -> str:
        kinds = list(self._codecs)
        return f"CodecRegistry(kinds={kinds})"


# ── Convenience singleton ─────────────────────────────────────────────────────

def default_registry() -> CodecRegistry:
    """
    Return a CodecRegistry pre-configured with sensible defaults:
    - NumpyCodec for 'visual' kind (if numpy is available)
    - MsgpackCodec (or JSONCodec fallback) for everything else
    """
    registry = CodecRegistry()
    if _NUMPY_AVAILABLE:
        registry.register("visual", NumpyCodec())
    return registry
