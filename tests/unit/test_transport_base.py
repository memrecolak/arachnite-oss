"""Unit tests for BaseTransport wire encoding helpers."""

from __future__ import annotations

import time

import pytest

from arachnite.codec import CodecRegistry, MsgpackCodec
from arachnite.models import Signal
from arachnite.transport.local import LocalTransport


def _sig(kind: str = "temperature", value: float = 42.0) -> Signal:
    return Signal(
        source="sensor", kind=kind, value=value,
        confidence=0.9, timestamp=time.monotonic(),
    )


def _transport(agent_id: str = "agent-1") -> LocalTransport:
    return LocalTransport(agent_node_id=agent_id)


class TestEncodeDecodeRoundtrip:
    def test_roundtrip_float_value(self) -> None:
        t = _transport()
        sig = _sig("temperature", 37.5)
        data = t._encode_signal(sig)
        restored = t._decode_signal(data)
        assert restored.source == sig.source
        assert restored.kind == sig.kind
        assert abs(restored.value - sig.value) < 1e-6
        assert abs(restored.confidence - sig.confidence) < 1e-6

    def test_roundtrip_string_value(self) -> None:
        t = _transport()
        sig = Signal(
            source="cam", kind="label", value="cat",
            confidence=1.0, timestamp=time.monotonic(),
        )
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.value == "cat"

    def test_roundtrip_preserves_metadata(self) -> None:
        t = _transport()
        sig = Signal(
            source="s", kind="x", value=1,
            confidence=1.0, timestamp=time.monotonic(),
            metadata={"sensor_id": "ABC"},
        )
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.metadata["sensor_id"] == "ABC"

    def test_roundtrip_none_value(self) -> None:
        t = _transport()
        sig = Signal(
            source="s", kind="x", value=None,
            confidence=0.0, timestamp=time.monotonic(),
        )
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.value is None

    def test_decode_wrong_version_raises(self) -> None:
        import msgpack  # type: ignore[import-untyped,unused-ignore]
        t = _transport()
        bad = msgpack.packb({"v": 99, "sig": {}}, use_bin_type=True)
        with pytest.raises(ValueError, match="wire protocol version"):
            t._decode_signal(bad)

    def test_custom_codec_registry_used(self) -> None:
        # BaseTransport accepts codec_registry; access via the base directly
        registry = CodecRegistry()
        registry.register("temperature", MsgpackCodec())
        registry.register("*", MsgpackCodec())

        # Instantiate via LocalTransport then override _codec to test the path
        t = LocalTransport(agent_node_id="a")
        t._codec = registry
        sig = _sig("temperature", 99.0)
        restored = t._decode_signal(t._encode_signal(sig))
        assert abs(restored.value - 99.0) < 1e-6


class TestTransportRepr:
    def test_repr_contains_agent_id(self) -> None:
        t = _transport("my-agent")
        assert "my-agent" in repr(t)
