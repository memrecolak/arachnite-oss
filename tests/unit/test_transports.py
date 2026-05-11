"""Unit tests for MQTT, NATS, and Redis transports (no live broker required).

Covers:
- Wire encoding round-trip via a concrete BaseTransport subclass
- Wire encoding with custom JSONCodec
- Wire version mismatch rejection
- Constructor defaults and configuration for each transport
- Missing-dependency error paths (monkeypatched)
- codec_registry acceptance for each transport
- Publish-when-not-connected error paths
- Mock-based publish/subscribe patterns
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import msgpack  # type: ignore[import-untyped,unused-ignore]
import pytest

from arachnite.codec import CodecRegistry, JSONCodec, MsgpackCodec
from arachnite.exceptions import TransportConnectionError, UnsafeCodecError
from arachnite.logging import BaseLogSink, LogEvent, LogLevel
from arachnite.models import Signal
from arachnite.transport.base import _WIRE_VERSION, BaseTransport

Callback = Callable[[Signal], Awaitable[None]]


# ── Concrete stub for testing BaseTransport helpers ──────────────────────────

class StubTransport(BaseTransport):
    """Minimal concrete BaseTransport for testing wire encoding helpers."""

    def __init__(
        self,
        agent_node_id: str = "stub",
        codec_registry: CodecRegistry | None = None,
    ) -> None:
        super().__init__(agent_node_id=agent_node_id, codec_registry=codec_registry)
        self._connected = False

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def publish(self, signal: Signal) -> None:
        pass

    async def subscribe(self, kind: str, callback: Callback) -> None:
        pass

    async def unsubscribe(self, kind: str, callback: Callback) -> None:
        pass


def _sig(
    kind: str = "temperature",
    value: Any = 42.0,
    metadata: dict[str, Any] | None = None,
) -> Signal:
    return Signal(
        source="sensor-1",
        kind=kind,
        value=value,
        confidence=0.95,
        timestamp=time.monotonic(),
        metadata=metadata or {},
    )


# ═════════════════════════════════════════════════════════════════════════════
# Wire encoding round-trip (via StubTransport)
# ═════════════════════════════════════════════════════════════════════════════

class TestWireEncodingRoundtrip:
    """Encode then decode through BaseTransport helpers — verify fidelity."""

    def test_roundtrip_float(self) -> None:
        t = StubTransport()
        sig = _sig("temperature", 37.5)
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.source == sig.source
        assert restored.kind == sig.kind
        assert abs(restored.value - sig.value) < 1e-6
        assert abs(restored.confidence - sig.confidence) < 1e-6

    def test_roundtrip_int(self) -> None:
        t = StubTransport()
        sig = _sig("count", 7)
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.value == 7

    def test_roundtrip_string(self) -> None:
        t = StubTransport()
        sig = _sig("label", "cat")
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.value == "cat"

    def test_roundtrip_dict(self) -> None:
        t = StubTransport()
        sig = _sig("composite", {"a": 1, "b": [2, 3]})
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.value == {"a": 1, "b": [2, 3]}

    def test_roundtrip_none(self) -> None:
        t = StubTransport()
        sig = _sig("heartbeat", None)
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.value is None

    def test_roundtrip_preserves_metadata(self) -> None:
        t = StubTransport()
        sig = _sig("temperature", 42.0, metadata={"unit": "celsius", "sensor_id": "T1"})
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.metadata == {"unit": "celsius", "sensor_id": "T1"}

    def test_roundtrip_preserves_timestamp(self) -> None:
        t = StubTransport()
        sig = _sig("temperature", 1.0)
        restored = t._decode_signal(t._encode_signal(sig))
        assert abs(restored.timestamp - sig.timestamp) < 1e-6

    def test_roundtrip_empty_metadata(self) -> None:
        t = StubTransport()
        sig = _sig("temperature", 1.0, metadata={})
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.metadata == {}

    def test_encode_returns_bytes(self) -> None:
        t = StubTransport()
        data = t._encode_signal(_sig())
        assert isinstance(data, bytes)

    def test_envelope_contains_agent_node_id(self) -> None:
        t = StubTransport(agent_node_id="edge-42")
        data = t._encode_signal(_sig())
        envelope = msgpack.unpackb(data, raw=False)
        assert envelope["src"] == "edge-42"

    def test_envelope_contains_wire_version(self) -> None:
        t = StubTransport()
        data = t._encode_signal(_sig())
        envelope = msgpack.unpackb(data, raw=False)
        assert envelope["v"] == _WIRE_VERSION


# ═════════════════════════════════════════════════════════════════════════════
# Wire encoding with custom codec
# ═════════════════════════════════════════════════════════════════════════════

class TestWireEncodingWithCustomCodec:
    """Verify custom CodecRegistry is used for encode/decode."""

    def test_json_codec_roundtrip(self) -> None:
        reg = CodecRegistry()
        reg.register("thermal", JSONCodec())
        reg.register("*", JSONCodec())
        t = StubTransport(codec_registry=reg)
        sig = _sig("thermal", {"reading": 55.5})
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.value == {"reading": 55.5}

    def test_mixed_codecs_per_kind(self) -> None:
        reg = CodecRegistry()
        reg.register("fast", MsgpackCodec())
        reg.register("readable", JSONCodec())
        reg.register("*", MsgpackCodec())
        t = StubTransport(codec_registry=reg)

        sig_fast = _sig("fast", 100)
        sig_read = _sig("readable", "hello")
        assert t._decode_signal(t._encode_signal(sig_fast)).value == 100
        assert t._decode_signal(t._encode_signal(sig_read)).value == "hello"

    def test_wildcard_fallback_codec(self) -> None:
        reg = CodecRegistry()
        reg.register("*", JSONCodec())
        t = StubTransport(codec_registry=reg)
        sig = _sig("unregistered_kind", [1, 2, 3])
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.value == [1, 2, 3]


# ═════════════════════════════════════════════════════════════════════════════
# Wire version mismatch
# ═════════════════════════════════════════════════════════════════════════════

class TestWireVersionMismatch:
    """Tampered wire bytes with wrong version number must raise ValueError."""

    def test_version_zero_rejected(self) -> None:
        t = StubTransport()
        bad = msgpack.packb(
            {"v": 0, "src": "x", "sig": {"source": "s", "kind": "k",
             "value": b"", "confidence": 1.0, "timestamp": 0.0}},
            use_bin_type=True,
        )
        with pytest.raises(ValueError, match="wire protocol version"):
            t._decode_signal(bad)

    def test_version_99_rejected(self) -> None:
        t = StubTransport()
        bad = msgpack.packb({"v": 99, "sig": {}}, use_bin_type=True)
        with pytest.raises(ValueError, match="wire protocol version"):
            t._decode_signal(bad)

    def test_missing_version_rejected(self) -> None:
        t = StubTransport()
        bad = msgpack.packb({"sig": {}}, use_bin_type=True)
        with pytest.raises(ValueError, match="wire protocol version"):
            t._decode_signal(bad)

    def test_tampered_real_payload(self) -> None:
        """Encode a valid signal, then flip the version and verify rejection."""
        t = StubTransport()
        data = t._encode_signal(_sig())
        envelope = msgpack.unpackb(data, raw=False)
        envelope["v"] = 255
        tampered = msgpack.packb(envelope, use_bin_type=True)
        with pytest.raises(ValueError, match="wire protocol version"):
            t._decode_signal(tampered)


# ═════════════════════════════════════════════════════════════════════════════
# Constructor defaults
# ═════════════════════════════════════════════════════════════════════════════

class TestConstructorDefaults:
    """Verify default agent_node_id and codec_registry for each transport."""

    def test_base_transport_defaults(self) -> None:
        t = StubTransport()
        assert t.agent_node_id == "stub"
        assert t._codec is not None

    def test_local_transport_defaults(self) -> None:
        from arachnite.transport.local import LocalTransport
        t = LocalTransport()
        assert t.agent_node_id == "local"
        assert t._codec is not None

    def test_mqtt_transport_defaults(self) -> None:
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        assert t.agent_node_id == "edge"
        assert t._host == "localhost"
        assert t._port == 1883
        assert t._topic_prefix == "arachnite/"
        assert t._qos == 1
        assert t._reconnect_interval == 2.0
        assert t._max_reconnect == 10
        assert t._username is None
        assert t._password is None
        assert t._tls is False
        assert t._codec is not None

    def test_nats_transport_defaults(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        assert t.agent_node_id == "cloud"
        assert t._servers == ["nats://localhost:4222"]
        assert t._subject_pfx == "arachnite"
        assert t._reconnect_s == 2.0
        assert t._max_reconnect == 10
        assert t._codec is not None

    def test_nats_transport_multiple_servers(self) -> None:
        from arachnite.transport.nats import NATSTransport
        servers = ["nats://a:4222", "nats://b:4222"]
        t = NATSTransport(servers=servers)
        assert t._servers == servers

    def test_redis_transport_defaults(self) -> None:
        pytest.importorskip("redis.asyncio")
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        assert t.agent_node_id == "node"
        assert t._url == "redis://localhost:6379"
        assert t._channel_prefix == "arachnite"
        assert t._reconnect_s == 2.0
        assert t._max_reconnect == 10
        assert t._db == 0
        assert t._password is None
        assert t._codec is not None


# ═════════════════════════════════════════════════════════════════════════════
# codec_registry acceptance
# ═════════════════════════════════════════════════════════════════════════════

class TestCodecRegistryAcceptance:
    """Verify each transport stores a custom CodecRegistry correctly."""

    def test_stub_transport_accepts_registry(self) -> None:
        reg = CodecRegistry()
        reg.register("thermal", JSONCodec())
        t = StubTransport(codec_registry=reg)
        assert t._codec is reg

    def test_local_transport_accepts_registry(self) -> None:
        from arachnite.transport.local import LocalTransport
        t = LocalTransport(agent_node_id="local")
        # LocalTransport doesn't pass codec_registry in __init__, so it uses default
        assert t._codec is not None

    def test_mqtt_transport_stores_codec_registry(self) -> None:
        from arachnite.transport.mqtt import MQTTTransport
        reg = CodecRegistry()
        reg.register("thermal", JSONCodec())
        t = MQTTTransport(broker_host="localhost", codec_registry=reg)
        assert t._codec is reg

    def test_nats_transport_stores_codec_registry(self) -> None:
        from arachnite.transport.nats import NATSTransport
        reg = CodecRegistry()
        reg.register("thermal", JSONCodec())
        t = NATSTransport(codec_registry=reg)
        assert t._codec is reg

    def test_redis_transport_stores_codec_registry(self) -> None:
        pytest.importorskip("redis.asyncio")
        from arachnite.transport.redis import RedisTransport
        reg = CodecRegistry()
        reg.register("thermal", JSONCodec())
        t = RedisTransport(codec_registry=reg)
        assert t._codec is reg


# ═════════════════════════════════════════════════════════════════════════════
# Missing dependency error paths
# ═════════════════════════════════════════════════════════════════════════════

class TestMissingDependencyErrors:
    """Constructing a transport without its optional library raises ImportError."""

    def test_mqtt_constructor_raises_without_aiomqtt(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import arachnite.transport.mqtt as mqtt_mod
        monkeypatch.setattr(mqtt_mod, "_AIOMQTT_AVAILABLE", False)
        with pytest.raises(ImportError, match="aiomqtt"):
            mqtt_mod.MQTTTransport(broker_host="localhost")

    def test_nats_constructor_raises_without_nats(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import arachnite.transport.nats as nats_mod
        monkeypatch.setattr(nats_mod, "_NATS_AVAILABLE", False)
        with pytest.raises(ImportError, match="nats-py"):
            nats_mod.NATSTransport()

    def test_redis_constructor_raises_without_redis(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import arachnite.transport.redis as redis_mod
        monkeypatch.setattr(redis_mod, "_REDIS_AVAILABLE", False)
        with pytest.raises(ImportError, match="redis"):
            redis_mod.RedisTransport()


# ═════════════════════════════════════════════════════════════════════════════
# Publish-when-not-connected error paths
# ═════════════════════════════════════════════════════════════════════════════

class TestPublishNotConnected:
    """Publishing on a disconnected transport must raise TransportConnectionError."""

    async def test_mqtt_publish_not_connected(self) -> None:
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        with pytest.raises(TransportConnectionError, match="Not connected"):
            await t.publish(_sig())

    async def test_nats_publish_not_connected(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        with pytest.raises(TransportConnectionError, match="Not connected"):
            await t.publish(_sig())

    async def test_redis_publish_not_connected(self) -> None:
        pytest.importorskip("redis.asyncio")
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        with pytest.raises(TransportConnectionError, match="Not connected"):
            await t.publish(_sig())


# ═════════════════════════════════════════════════════════════════════════════
# MQTT mock-based tests
# ═════════════════════════════════════════════════════════════════════════════

class TestMQTTMockBased:
    """Mock-based tests for MQTTTransport publish/subscribe (no broker)."""

    async def test_mqtt_connect_calls_safety_check(self) -> None:
        """connect() calls check_network_safety before broker connection."""
        from arachnite.transport.mqtt import MQTTTransport
        reg = MagicMock(spec=CodecRegistry)
        reg.check_network_safety = MagicMock(
            side_effect=UnsafeCodecError("test: unsafe")
        )
        t = MQTTTransport(broker_host="localhost", codec_registry=reg)
        with pytest.raises(UnsafeCodecError, match="test: unsafe"):
            await t.connect()

    async def test_mqtt_subscribe_before_connect(self) -> None:
        """Subscribing before connect() queues the callback."""
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        cb = AsyncMock()
        await t.subscribe("thermal", cb)
        assert cb in t._subscribers["thermal"]

    async def test_mqtt_unsubscribe_removes_callback(self) -> None:
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        cb = AsyncMock()
        await t.subscribe("thermal", cb)
        await t.unsubscribe("thermal", cb)
        assert cb not in t._subscribers["thermal"]

    async def test_mqtt_unsubscribe_nonexistent_no_error(self) -> None:
        """Unsubscribing a callback that was never registered does not raise."""
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        cb = AsyncMock()
        # Should not raise
        await t.unsubscribe("thermal", cb)

    async def test_mqtt_repr(self) -> None:
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost", agent_node_id="pi-01")
        assert "pi-01" in repr(t)
        assert "MQTTTransport" in repr(t)

    async def test_mqtt_connected_property_default(self) -> None:
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        assert t.connected is False

    async def test_mqtt_custom_params(self) -> None:
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(
            broker_host="mqtt.example.com",
            broker_port=8883,
            agent_node_id="edge-99",
            topic_prefix="myapp/",
            qos=2,
            reconnect_interval_s=5.0,
            max_reconnect_attempts=3,
            username="user",
            password="pass",
            tls=True,
        )
        assert t._host == "mqtt.example.com"
        assert t._port == 8883
        assert t.agent_node_id == "edge-99"
        assert t._topic_prefix == "myapp/"
        assert t._qos == 2
        assert t._reconnect_interval == 5.0
        assert t._max_reconnect == 3
        assert t._username == "user"
        assert t._password == "pass"
        assert t._tls is True

    async def test_mqtt_dispatch_decodes_and_calls_subscribers(self) -> None:
        """Verify _dispatch decodes wire bytes and calls registered callbacks."""
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        received: list[Signal] = []

        async def handler(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("thermal", handler)
        sig = _sig("thermal", 99.0)
        wire_data = t._encode_signal(sig)

        # Create a mock MQTT message with payload attribute
        mock_msg = MagicMock()
        mock_msg.payload = wire_data
        await t._dispatch(mock_msg)

        assert len(received) == 1
        assert abs(received[0].value - 99.0) < 1e-6

    async def test_mqtt_dispatch_wildcard_subscriber(self) -> None:
        """Wildcard '*' subscriber receives signals of any kind."""
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        received: list[Signal] = []

        async def handler(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("*", handler)
        sig = _sig("unknown_kind", "data")
        wire_data = t._encode_signal(sig)

        mock_msg = MagicMock()
        mock_msg.payload = wire_data
        await t._dispatch(mock_msg)

        assert len(received) == 1
        assert received[0].kind == "unknown_kind"

    async def test_mqtt_dispatch_malformed_payload_dropped(self) -> None:
        """Malformed payloads are dropped silently, no exception raised."""
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        cb = AsyncMock()
        await t.subscribe("thermal", cb)

        mock_msg = MagicMock()
        mock_msg.payload = b"not valid msgpack at all"
        # Must not raise
        await t._dispatch(mock_msg)
        cb.assert_not_awaited()

    async def test_mqtt_disconnect_without_connect(self) -> None:
        """Disconnecting a transport that was never connected must not raise."""
        from arachnite.transport.mqtt import MQTTTransport
        t = MQTTTransport(broker_host="localhost")
        await t.disconnect()
        assert t.connected is False


# ═════════════════════════════════════════════════════════════════════════════
# NATS mock-based tests
# ═════════════════════════════════════════════════════════════════════════════

class TestNATSMockBased:
    """Mock-based tests for NATSTransport publish/subscribe (no broker)."""

    async def test_nats_connect_calls_safety_check(self) -> None:
        from arachnite.transport.nats import NATSTransport
        reg = MagicMock(spec=CodecRegistry)
        reg.check_network_safety = MagicMock(
            side_effect=UnsafeCodecError("test: unsafe")
        )
        t = NATSTransport(codec_registry=reg)
        with pytest.raises(UnsafeCodecError, match="test: unsafe"):
            await t.connect()

    async def test_nats_subscribe_before_connect(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        cb = AsyncMock()
        await t.subscribe("thermal", cb)
        assert cb in t._subscribers["thermal"]

    async def test_nats_unsubscribe_removes_callback(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        cb = AsyncMock()
        await t.subscribe("thermal", cb)
        await t.unsubscribe("thermal", cb)
        assert cb not in t._subscribers["thermal"]

    async def test_nats_unsubscribe_nonexistent_no_error(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        cb = AsyncMock()
        await t.unsubscribe("thermal", cb)

    async def test_nats_repr(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport(agent_node_id="cloud-01")
        assert "cloud-01" in repr(t)
        assert "NATSTransport" in repr(t)

    async def test_nats_connected_property_default(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        assert t.connected is False

    async def test_nats_custom_params(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport(
            servers=["nats://a:4222", "nats://b:4222"],
            agent_node_id="gpu-node",
            subject_prefix="myapp",
            reconnect_interval_s=10.0,
            max_reconnect_attempts=5,
        )
        assert t._servers == ["nats://a:4222", "nats://b:4222"]
        assert t.agent_node_id == "gpu-node"
        assert t._subject_pfx == "myapp"
        assert t._reconnect_s == 10.0
        assert t._max_reconnect == 5

    async def test_nats_handler_decodes_and_dispatches(self) -> None:
        """Verify _make_handler decodes wire bytes and calls subscribers."""
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        received: list[Signal] = []

        async def cb(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("thermal", cb)
        handler = t._make_handler("thermal")
        sig = _sig("thermal", 55.0)
        wire_data = t._encode_signal(sig)

        mock_msg = MagicMock()
        mock_msg.data = wire_data
        await handler(mock_msg)

        assert len(received) == 1
        assert abs(received[0].value - 55.0) < 1e-6

    async def test_nats_handler_malformed_data_dropped(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        cb = AsyncMock()
        await t.subscribe("thermal", cb)
        handler = t._make_handler("thermal")

        mock_msg = MagicMock()
        mock_msg.data = b"garbage bytes"
        await handler(mock_msg)
        cb.assert_not_awaited()

    async def test_nats_handler_wildcard_subscriber(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        received: list[Signal] = []

        async def cb(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("*", cb)
        handler = t._make_handler("thermal")
        sig = _sig("thermal", 77.0)
        wire_data = t._encode_signal(sig)

        mock_msg = MagicMock()
        mock_msg.data = wire_data
        await handler(mock_msg)

        assert len(received) == 1

    async def test_nats_disconnect_without_connect(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        await t.disconnect()
        assert t.connected is False

    async def test_nats_on_disconnected_sets_flag(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        t._connected = True
        await t._on_disconnected()
        assert t.connected is False

    async def test_nats_on_reconnected_sets_flag(self) -> None:
        from arachnite.transport.nats import NATSTransport
        t = NATSTransport()
        t._connected = False
        await t._on_reconnected()
        assert t.connected is True


# ═════════════════════════════════════════════════════════════════════════════
# Redis mock-based tests
# ═════════════════════════════════════════════════════════════════════════════

class TestRedisMockBased:
    """Mock-based tests for RedisTransport (no broker)."""

    @pytest.fixture(autouse=True)
    def _skip_if_no_redis(self) -> None:
        pytest.importorskip("redis.asyncio")

    async def test_redis_connect_calls_safety_check(self) -> None:
        from arachnite.transport.redis import RedisTransport
        reg = MagicMock(spec=CodecRegistry)
        reg.check_network_safety = MagicMock(
            side_effect=UnsafeCodecError("test: unsafe")
        )
        t = RedisTransport(codec_registry=reg)
        with pytest.raises(UnsafeCodecError, match="test: unsafe"):
            await t.connect()

    async def test_redis_subscribe_before_connect(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        cb = AsyncMock()
        await t.subscribe("thermal", cb)
        assert cb in t._subscribers["thermal"]

    async def test_redis_unsubscribe_removes_callback(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        cb = AsyncMock()
        await t.subscribe("thermal", cb)
        await t.unsubscribe("thermal", cb)
        assert cb not in t._subscribers["thermal"]

    async def test_redis_unsubscribe_nonexistent_no_error(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        cb = AsyncMock()
        await t.unsubscribe("thermal", cb)

    async def test_redis_repr(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport(agent_node_id="cache-01")
        assert "cache-01" in repr(t)
        assert "RedisTransport" in repr(t)

    async def test_redis_connected_property_default(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        assert t.connected is False

    async def test_redis_custom_params(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport(
            url="redis://cache:6380",
            agent_node_id="worker-3",
            channel_prefix="myapp",
            reconnect_interval_s=5.0,
            max_reconnect_attempts=3,
            db=2,
            password="secret",
        )
        assert t._url == "redis://cache:6380"
        assert t.agent_node_id == "worker-3"
        assert t._channel_prefix == "myapp"
        assert t._reconnect_s == 5.0
        assert t._max_reconnect == 3
        assert t._db == 2
        assert t._password == "secret"

    async def test_redis_dispatch_decodes_and_calls_subscribers(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        received: list[Signal] = []

        async def handler(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("thermal", handler)
        sig = _sig("thermal", 88.0)
        wire_data = t._encode_signal(sig)
        await t._dispatch(wire_data)

        assert len(received) == 1
        assert abs(received[0].value - 88.0) < 1e-6

    async def test_redis_dispatch_wildcard_subscriber(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        received: list[Signal] = []

        async def handler(sig: Signal) -> None:
            received.append(sig)

        await t.subscribe("*", handler)
        sig = _sig("sensor_xyz", "reading")
        wire_data = t._encode_signal(sig)
        await t._dispatch(wire_data)

        assert len(received) == 1

    async def test_redis_dispatch_malformed_data_dropped(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        cb = AsyncMock()
        await t.subscribe("thermal", cb)
        await t._dispatch(b"not valid msgpack data")
        cb.assert_not_awaited()

    async def test_redis_disconnect_without_connect(self) -> None:
        from arachnite.transport.redis import RedisTransport
        t = RedisTransport()
        await t.disconnect()
        assert t.connected is False


# ═════════════════════════════════════════════════════════════════════════════
# Cross-transport wire interoperability
# ═════════════════════════════════════════════════════════════════════════════

class TestCrossTransportWireInterop:
    """Signal encoded by one transport type can be decoded by another."""

    def test_stub_to_local_roundtrip(self) -> None:
        from arachnite.transport.local import LocalTransport
        encoder = StubTransport(agent_node_id="edge")
        decoder = LocalTransport(agent_node_id="cloud")
        sig = _sig("thermal", 42.0)
        wire = encoder._encode_signal(sig)
        restored = decoder._decode_signal(wire)
        assert restored.kind == sig.kind
        assert abs(restored.value - sig.value) < 1e-6

    def test_mqtt_encodes_local_decodes(self) -> None:
        from arachnite.transport.local import LocalTransport
        from arachnite.transport.mqtt import MQTTTransport
        encoder = MQTTTransport(broker_host="x", agent_node_id="pi")
        decoder = LocalTransport(agent_node_id="laptop")
        sig = _sig("thermal", 55.0)
        wire = encoder._encode_signal(sig)
        restored = decoder._decode_signal(wire)
        assert restored.kind == sig.kind
        assert abs(restored.value - 55.0) < 1e-6

    def test_nats_encodes_mqtt_decodes(self) -> None:
        from arachnite.transport.mqtt import MQTTTransport
        from arachnite.transport.nats import NATSTransport
        encoder = NATSTransport(agent_node_id="cloud")
        decoder = MQTTTransport(broker_host="x", agent_node_id="edge")
        sig = _sig("visual", "frame_data")
        wire = encoder._encode_signal(sig)
        restored = decoder._decode_signal(wire)
        assert restored.value == "frame_data"

    def test_shared_codec_registry_interop(self) -> None:
        from arachnite.transport.nats import NATSTransport
        reg = CodecRegistry()
        reg.register("*", JSONCodec())
        encoder = StubTransport(agent_node_id="a", codec_registry=reg)
        decoder = NATSTransport(agent_node_id="b", codec_registry=reg)
        sig = _sig("debug", {"msg": "hello"})
        wire = encoder._encode_signal(sig)
        restored = decoder._decode_signal(wire)
        assert restored.value == {"msg": "hello"}


# ═════════════════════════════════════════════════════════════════════════════
# Transport connect/disconnect log events (Spec §13.3)
# ═════════════════════════════════════════════════════════════════════════════


class _CaptureSink(BaseLogSink):
    """Collects all log events for assertion."""

    def __init__(self) -> None:
        super().__init__(level=LogLevel.DEBUG)
        self.events: list[LogEvent] = []

    async def emit(self, event: LogEvent) -> None:
        self.events.append(event)


class TestTransportConnectDisconnectLogEvents:
    """Verify transport.connected/disconnected events are emitted (Spec §13.3)."""

    async def test_local_transport_emits_connect_disconnect(self) -> None:
        from arachnite.transport.local import LocalTransport
        sink = _CaptureSink()
        t = LocalTransport()
        t._logger._sinks = [sink]
        await t.connect()
        await t.disconnect()
        await asyncio.sleep(0)  # let fire-and-forget tasks flush
        messages = [ev.message for ev in sink.events]
        assert "Transport connected" in messages
        assert "Transport disconnected" in messages

    async def test_mqtt_transport_emits_disconnect(self) -> None:
        """MQTT disconnect emits log event (connect requires a broker)."""
        from arachnite.transport.mqtt import MQTTTransport
        sink = _CaptureSink()
        t = MQTTTransport(broker_host="localhost")
        t._logger._sinks = [sink]
        await t.disconnect()
        await asyncio.sleep(0)
        messages = [ev.message for ev in sink.events]
        assert "Transport disconnected" in messages

    async def test_nats_transport_emits_disconnect(self) -> None:
        from arachnite.transport.nats import NATSTransport
        sink = _CaptureSink()
        t = NATSTransport()
        t._logger._sinks = [sink]
        await t.disconnect()
        await asyncio.sleep(0)
        messages = [ev.message for ev in sink.events]
        assert "Transport disconnected" in messages

    async def test_redis_transport_emits_disconnect(self) -> None:
        pytest.importorskip("redis.asyncio")
        from arachnite.transport.redis import RedisTransport
        sink = _CaptureSink()
        t = RedisTransport()
        t._logger._sinks = [sink]
        await t.disconnect()
        await asyncio.sleep(0)
        messages = [ev.message for ev in sink.events]
        assert "Transport disconnected" in messages

    async def test_local_connect_event_has_transport_name(self) -> None:
        from arachnite.transport.local import LocalTransport
        sink = _CaptureSink()
        t = LocalTransport()
        t._logger._sinks = [sink]
        await t.connect()
        await asyncio.sleep(0)
        connected = [ev for ev in sink.events if ev.message == "Transport connected"]
        assert len(connected) == 1
        assert connected[0].data["transport"] == "LocalTransport"
