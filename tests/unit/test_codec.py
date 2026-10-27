"""Unit tests for SignalCodec implementations and CodecRegistry."""

from __future__ import annotations

import os
import pickle
from typing import Any

import msgpack  # type: ignore[import-untyped,unused-ignore]
import pytest

import arachnite.codec as _codec_module
from arachnite.codec import (
    CodecRegistry,
    JSONCodec,
    MsgpackCodec,
    NumpyCodec,
    PickleCodec,
    default_registry,
)
from arachnite.exceptions import UnsafeCodecError
from arachnite.models import Signal


class TestMsgpackCodec:
    def test_roundtrip_int(self) -> None:
        codec = MsgpackCodec()
        assert codec.decode(codec.encode(42)) == 42

    def test_roundtrip_float(self) -> None:
        codec = MsgpackCodec()
        value = 3.14159
        assert abs(codec.decode(codec.encode(value)) - value) < 1e-6

    def test_roundtrip_string(self) -> None:
        codec = MsgpackCodec()
        assert codec.decode(codec.encode("hello")) == "hello"

    def test_roundtrip_dict(self) -> None:
        codec = MsgpackCodec()
        data  = {"temperature": 42.0, "unit": "celsius"}
        assert codec.decode(codec.encode(data)) == data

    def test_roundtrip_list(self) -> None:
        codec = MsgpackCodec()
        data  = [1, 2.5, "three"]
        assert codec.decode(codec.encode(data)) == data

    def test_encode_returns_bytes(self) -> None:
        codec = MsgpackCodec()
        assert isinstance(codec.encode(1), bytes)


class TestJSONCodec:
    def test_roundtrip_int(self) -> None:
        codec = JSONCodec()
        assert codec.decode(codec.encode(99)) == 99

    def test_roundtrip_dict(self) -> None:
        codec = JSONCodec()
        data  = {"key": "value", "n": 1}
        assert codec.decode(codec.encode(data)) == data

    def test_roundtrip_nested(self) -> None:
        codec = JSONCodec()
        data  = {"outer": {"inner": [1, 2, 3]}}
        assert codec.decode(codec.encode(data)) == data

    def test_encode_returns_bytes(self) -> None:
        codec = JSONCodec()
        assert isinstance(codec.encode("x"), bytes)

    def test_non_serializable_uses_str_fallback(self) -> None:
        codec  = JSONCodec()
        result = codec.decode(codec.encode({"obj": object()}))
        # default=str turns the object into its repr string — just mustn't raise
        assert "obj" in result


class TestPickleCodec:
    def test_roundtrip_int(self) -> None:
        codec = PickleCodec()
        assert codec.decode(codec.encode(7)) == 7

    def test_roundtrip_arbitrary_object(self) -> None:
        codec = PickleCodec()
        data  = {"a": [1, 2], "b": (3, 4)}
        assert codec.decode(codec.encode(data)) == data

    def test_roundtrip_none(self) -> None:
        codec = PickleCodec()
        assert codec.decode(codec.encode(None)) is None


class TestCodecRegistry:
    def test_default_registry_has_wildcard(self) -> None:
        reg = CodecRegistry()
        # Should not raise — wildcard is always installed
        encoded = reg.encode("unknown_kind", 42)
        assert isinstance(encoded, bytes)

    def test_explicit_registration_takes_priority(self) -> None:
        reg = CodecRegistry()
        reg.register("thermal", JSONCodec())
        encoded = reg.encode("thermal", 99)
        # JSONCodec produces valid UTF-8 JSON
        import json
        assert json.loads(encoded) == 99

    def test_wildcard_fallback_used_for_unknown_kind(self) -> None:
        reg = CodecRegistry()
        reg.register("*", JSONCodec())
        encoded = reg.encode("anything", "hello")
        assert reg.decode("anything", encoded) == "hello"

    def test_registry_roundtrip_via_kind(self) -> None:
        reg = CodecRegistry()
        reg.register("visual", PickleCodec())
        value   = {"pixels": [1, 2, 3]}
        encoded = reg.encode("visual", value)
        decoded = reg.decode("visual", encoded)
        assert decoded == value

    def test_no_wildcard_raises_key_error(self) -> None:
        reg = CodecRegistry()
        reg._codecs.clear()  # remove auto-installed wildcard
        with pytest.raises(KeyError):
            reg.encode("orphan_kind", 1)

    def test_multiple_kinds_independent(self) -> None:
        reg = CodecRegistry()
        reg.register("a", MsgpackCodec())
        reg.register("b", JSONCodec())
        assert reg.decode("a", reg.encode("a", 1)) == 1
        assert reg.decode("b", reg.encode("b", "hi")) == "hi"


class TestCodecRegistryRepr:
    def test_repr_contains_registry(self) -> None:
        reg = CodecRegistry()
        assert "CodecRegistry" in repr(reg)

    def test_repr_lists_registered_kinds(self) -> None:
        reg = CodecRegistry()
        reg.register("thermal", JSONCodec())
        assert "thermal" in repr(reg)


class TestDefaultRegistry:
    def test_default_registry_encodes_primitives(self) -> None:
        reg     = default_registry()
        encoded = reg.encode("thermal", 55.0)
        assert isinstance(encoded, bytes)
        decoded = reg.decode("thermal", encoded)
        assert abs(decoded - 55.0) < 1e-6

    def test_default_registry_encodes_dict(self) -> None:
        reg   = default_registry()
        data  = {"reading": 42, "unit": "C"}
        assert reg.decode("any_kind", reg.encode("any_kind", data)) == data

    def test_default_registry_visual_uses_numpy_when_available(self) -> None:
        if not _codec_module._NUMPY_AVAILABLE:
            pytest.skip("numpy not installed")
        reg = default_registry()
        assert "visual" in reg._codecs
        assert isinstance(reg._codecs["visual"], NumpyCodec)

    def test_default_registry_no_visual_without_numpy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_codec_module, "_NUMPY_AVAILABLE", False)
        reg = default_registry()
        assert "visual" not in reg._codecs


class TestNumpyCodec:
    def test_skip_if_no_numpy(self) -> None:
        if not _codec_module._NUMPY_AVAILABLE:
            pytest.skip("numpy not installed")

    def test_roundtrip_1d_array(self) -> None:
        if not _codec_module._NUMPY_AVAILABLE:
            pytest.skip("numpy not installed")
        import numpy as np
        codec = NumpyCodec()
        arr   = np.array([1.0, 2.0, 3.0], dtype=np.float64)
        result = codec.decode(codec.encode(arr))
        assert result.shape == arr.shape
        assert (result == arr).all()

    def test_roundtrip_2d_array(self) -> None:
        if not _codec_module._NUMPY_AVAILABLE:
            pytest.skip("numpy not installed")
        import numpy as np
        codec = NumpyCodec()
        arr   = np.array([[1, 2], [3, 4]], dtype=np.int32)
        result = codec.decode(codec.encode(arr))
        assert result.shape == (2, 2)
        assert (result == arr).all()

    def test_encode_raises_without_numpy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_codec_module, "_NUMPY_AVAILABLE", False)
        codec = NumpyCodec()
        with pytest.raises(ImportError, match="numpy"):
            codec.encode([1, 2, 3])

    def test_decode_raises_without_numpy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_codec_module, "_NUMPY_AVAILABLE", False)
        codec = NumpyCodec()
        with pytest.raises(ImportError, match="numpy"):
            codec.decode(b"\x00" * 20)


class TestMsgpackImportGuard:
    def test_encode_raises_without_msgpack(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_codec_module, "_MSGPACK_AVAILABLE", False)
        codec = MsgpackCodec()
        with pytest.raises(ImportError, match="msgpack"):
            codec.encode(42)

    def test_decode_raises_without_msgpack(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_codec_module, "_MSGPACK_AVAILABLE", False)
        codec = MsgpackCodec()
        with pytest.raises(ImportError, match="msgpack"):
            codec.decode(b"\x00")


class TestNetworkSafeAttribute:
    """Test that the network_safe class attribute is set correctly on all codecs."""

    def test_msgpack_codec_is_network_safe(self) -> None:
        assert MsgpackCodec.network_safe is True
        assert MsgpackCodec().network_safe is True

    def test_json_codec_is_network_safe(self) -> None:
        assert JSONCodec.network_safe is True
        assert JSONCodec().network_safe is True

    def test_numpy_codec_is_network_safe(self) -> None:
        assert NumpyCodec.network_safe is True
        assert NumpyCodec().network_safe is True

    def test_pickle_codec_is_not_network_safe(self) -> None:
        assert PickleCodec.network_safe is False
        assert PickleCodec().network_safe is False


class TestCheckNetworkSafety:
    """Test CodecRegistry.check_network_safety()."""

    def test_raises_for_pickle_codec(self) -> None:
        reg = CodecRegistry()
        reg.register("thermal", PickleCodec())
        with pytest.raises(UnsafeCodecError, match="PickleCodec"):
            reg.check_network_safety("MQTTTransport")

    def test_passes_for_msgpack_only(self) -> None:
        reg = CodecRegistry()
        reg.register("thermal", MsgpackCodec())
        # Should not raise
        reg.check_network_safety("MQTTTransport")

    def test_passes_for_json_only(self) -> None:
        reg = CodecRegistry()
        reg.register("*", JSONCodec())
        # Should not raise
        reg.check_network_safety("NATSTransport")

    def test_raises_for_pickle_wildcard_fallback(self) -> None:
        reg = CodecRegistry()
        reg.register("*", PickleCodec())
        with pytest.raises(UnsafeCodecError, match="PickleCodec"):
            reg.check_network_safety("RedisTransport")

    def test_raises_message_includes_transport_name(self) -> None:
        reg = CodecRegistry()
        reg.register("visual", PickleCodec())
        with pytest.raises(UnsafeCodecError, match="MQTTTransport"):
            reg.check_network_safety("MQTTTransport")

    def test_passes_for_mixed_safe_codecs(self) -> None:
        reg = CodecRegistry()
        reg.register("thermal", MsgpackCodec())
        reg.register("debug", JSONCodec())
        # Should not raise
        reg.check_network_safety("NATSTransport")

    def test_raises_when_one_of_many_is_unsafe(self) -> None:
        reg = CodecRegistry()
        reg.register("thermal", MsgpackCodec())
        reg.register("arbitrary", PickleCodec())
        with pytest.raises(UnsafeCodecError):
            reg.check_network_safety("RedisTransport")


class TestTransportNetworkSafetyCheck:
    """Test that network transports call check_network_safety on connect()."""

    async def test_mqtt_transport_rejects_pickle_codec(self) -> None:
        pytest.importorskip("aiomqtt")
        from arachnite.transport.mqtt import MQTTTransport

        reg = CodecRegistry()
        reg.register("thermal", PickleCodec())
        transport = MQTTTransport(
            broker_host="localhost",
            codec_registry=reg,
        )
        with pytest.raises(UnsafeCodecError, match="PickleCodec"):
            await transport.connect()

    async def test_nats_transport_rejects_pickle_codec(self) -> None:
        pytest.importorskip("nats")
        from arachnite.transport.nats import NATSTransport

        reg = CodecRegistry()
        reg.register("*", PickleCodec())
        transport = NATSTransport(codec_registry=reg)
        with pytest.raises(UnsafeCodecError, match="PickleCodec"):
            await transport.connect()

    async def test_redis_transport_rejects_pickle_codec(self) -> None:
        pytest.importorskip("redis.asyncio")
        from arachnite.transport.redis import RedisTransport

        reg = CodecRegistry()
        reg.register("data", PickleCodec())
        transport = RedisTransport(codec_registry=reg)
        with pytest.raises(UnsafeCodecError, match="PickleCodec"):
            await transport.connect()


# ── Review 6 (distributed): codec network safety, numpy fallback, map keys ───

class _R6Collector:
    def __init__(self) -> None:
        self.seen: list[Signal] = []

    async def __call__(self, signal: Signal) -> None:
        self.seen.append(signal)


class _R6PwnMarker:
    """Unpickling this runs exec() — a harmless stand-in for remote code execution."""

    def __reduce__(self) -> tuple[Any, ...]:
        return (exec, ("import os; os.environ['R6_PWNED'] = '1'",))


class TestR6CodecNetworkSafety:
    """R6-F11: an unsafe codec cannot be slipped in after a network connect()."""

    def test_register_unsafe_codec_after_network_check_raises(self) -> None:
        """R6-F11: register() re-checks network safety once the registry is bound"""
        reg = CodecRegistry()
        reg.check_network_safety("NATSTransport")
        with pytest.raises(UnsafeCodecError, match="NATSTransport"):
            reg.register("cmd", PickleCodec())
        assert not isinstance(reg._get_codec("cmd"), PickleCodec)

    async def test_pickle_registered_after_connect_never_executes(
        self, fake_nats_server: Any, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """R6-F11: connect(), then register PickleCodec -> refused; a forced one is not decoded"""
        from arachnite.transport.nats import NATSTransport
        monkeypatch.delenv("R6_PWNED", raising=False)
        reg = CodecRegistry()
        t = NATSTransport(agent_node_id="victim", codec_registry=reg)
        await t.connect()
        with pytest.raises(UnsafeCodecError):
            reg.register("cmd", PickleCodec())
        # Even a codec forced in behind register()'s back is refused on receive.
        reg._codecs["cmd"] = PickleCodec()
        got = _R6Collector()
        await t.subscribe("cmd", got)
        envelope = {"v": 1, "src": "attacker", "sig": {
            "source": "x", "kind": "cmd", "value": pickle.dumps(_R6PwnMarker()),
            "confidence": 1.0, "timestamp": 0.0, "metadata": {}}}
        await fake_nats_server.publish("arachnite.cmd", msgpack.packb(envelope, use_bin_type=True))
        await t.disconnect()
        assert os.environ.get("R6_PWNED") is None
        assert got.seen == []


class TestR6NumpyCodecFallback:
    """R6-F12-13: NumpyCodec (default for 'visual') must not corrupt non-array values."""

    @pytest.mark.parametrize("value", [
        "/var/arachnite_media/visual/cam_1_23.jpg",
        b"\x89PNG\x00\x00",
        {"label": "person", "bbox": [1, 2, 3, 4]},
        [1, 2, 3],
        7,
        None,
    ])
    def test_non_array_values_roundtrip_unchanged(self, value: Any) -> None:
        """R6-F12-13: str/bytes/dict/list/scalar/None survive NumpyCodec unchanged"""
        pytest.importorskip("numpy")
        reg = default_registry()
        out = reg.decode("visual", reg.encode("visual", value))
        assert type(out) is type(value)
        assert out == value

    def test_decoded_array_is_writeable(self) -> None:
        """R6-F12-13: a received frame can be edited in place"""
        np = pytest.importorskip("numpy")
        codec = NumpyCodec()
        arr = np.arange(12, dtype=np.uint16).reshape(3, 4)
        out = codec.decode(codec.encode(arr))
        assert out.flags.writeable
        out[0, 0] = 99      # must not raise
        assert np.array_equal(out[1:], arr[1:]) and out.dtype == arr.dtype

    async def test_media_path_on_visual_crosses_the_wire_as_str(
        self, fake_nats_server: Any,
    ) -> None:
        """R6-F12-13: a MediaStore path published on 'visual' arrives as a usable str"""
        pytest.importorskip("numpy")
        from arachnite.bus import SignalBus
        from arachnite.transport.nats import NATSTransport
        ta, tb = NATSTransport(agent_node_id="A"), NATSTransport(agent_node_id="B")
        await ta.connect()
        await tb.connect()
        bus_a = SignalBus(transport=ta, agent_node_id="A")
        bus_b = SignalBus(transport=tb, agent_node_id="B")
        got = _R6Collector()
        bus_b.subscribe("visual", got)
        await bus_a.start_transport()
        await bus_b.start_transport()
        path = "/var/arachnite_media/visual/cam_1_23.jpg"
        await bus_a.publish(Signal(source="Cam", kind="visual", value=path,
                                   confidence=1.0, timestamp=0.0))
        await ta.disconnect()
        await tb.disconnect()
        assert [s.value for s in got.seen] == [path]
        assert isinstance(got.seen[0].value, str)


class TestR6MsgpackNonStrKeys:
    """R6-F12-22: dicts with int/float keys encode, so they must decode too."""

    def test_int_and_float_keys_roundtrip(self) -> None:
        """R6-F12-22: MsgpackCodec round-trips {0: 1.2, 1.5: 'x'}"""
        codec = MsgpackCodec()
        value = {0: 1.2, 1.5: "x", "s": [1]}
        assert codec.decode(codec.encode(value)) == value
