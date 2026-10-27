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
from arachnite.models import (
    NodeFaultSignal,
    NodeState,
    Signal,
    StateUpdateSignal,
    SupervisorSignal,
)
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
        # Decode preserves all user-supplied metadata keys verbatim.  It also
        # injects a reserved ``__origin_agent__`` marker carrying the
        # envelope's ``src`` so consumers can attribute the signal back to
        # its originating agent — the test below covers that explicitly.
        t = StubTransport()
        sig = _sig("temperature", 42.0, metadata={"unit": "celsius", "sensor_id": "T1"})
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.metadata["unit"] == "celsius"
        assert restored.metadata["sensor_id"] == "T1"

    def test_decode_stamps_origin_agent_in_metadata(self) -> None:
        t = StubTransport(agent_node_id="edge-7")
        sig = _sig("temperature", 42.0, metadata={"unit": "celsius"})
        restored = t._decode_signal(t._encode_signal(sig))
        assert restored.metadata["__origin_agent__"] == "edge-7"
        # Original user metadata is still intact.
        assert restored.metadata["unit"] == "celsius"

    def test_roundtrip_preserves_timestamp(self) -> None:
        t = StubTransport()
        sig = _sig("temperature", 1.0)
        restored = t._decode_signal(t._encode_signal(sig))
        assert abs(restored.timestamp - sig.timestamp) < 1e-6

    def test_roundtrip_empty_metadata(self) -> None:
        # Empty user-metadata still gets the origin marker.  The metadata
        # is otherwise empty — no spurious keys are added.
        t = StubTransport()
        sig = _sig("temperature", 1.0, metadata={})
        restored = t._decode_signal(t._encode_signal(sig))
        assert set(restored.metadata) == {"__origin_agent__"}

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


# ═════════════════════════════════════════════════════════════════════════════
# Review 6 (distributed): wire fidelity and broker semantics, via broker fakes
# ═════════════════════════════════════════════════════════════════════════════

class _R6Collector:
    def __init__(self) -> None:
        self.seen: list[Signal] = []

    async def __call__(self, signal: Signal) -> None:
        self.seen.append(signal)


class TestR6WireFidelity:
    """R6-F12-06/22: the wire envelope keeps Signal subclasses and non-str keys."""

    def test_node_fault_signal_survives_the_wire(self) -> None:
        """R6-F12-06: NodeFaultSignal keeps node_id, states and error type/message"""
        t = StubTransport(agent_node_id="A")
        sent = NodeFaultSignal(
            source="sv", kind="node_fault", value="faulted", confidence=1.0,
            timestamp=12.5, node_id="LidarSense", previous_state=NodeState.RUNNING,
            current_state=NodeState.FAULTED, restart_count=2,
            fault_error=RuntimeError("i2c timeout"),
        )
        got = t._decode_signal(t._encode_signal(sent))
        assert type(got) is NodeFaultSignal
        assert (got.node_id, got.previous_state, got.current_state, got.restart_count) == (
            "LidarSense", NodeState.RUNNING, NodeState.FAULTED, 2)
        assert (got.error_type, got.error_message) == ("RuntimeError", "i2c timeout")
        assert got.kind == "node_fault" and got.value == "faulted"
        assert got.metadata["__origin_agent__"] == "A"

    def test_supervisor_signal_survives_the_wire(self) -> None:
        """R6-F12-06: SupervisorSignal keeps node_id and states"""
        t = StubTransport()
        sent = SupervisorSignal(
            source="sv", kind="supervisor", value=None, confidence=1.0, timestamp=1.0,
            node_id="Arm", previous_state=NodeState.FAULTED,
            current_state=NodeState.RESTARTING, restart_count=1,
        )
        got = t._decode_signal(t._encode_signal(sent))
        assert type(got) is SupervisorSignal
        assert (got.node_id, got.previous_state, got.current_state, got.restart_count) == (
            "Arm", NodeState.FAULTED, NodeState.RESTARTING, 1)

    def test_state_update_signal_survives_the_wire(self) -> None:
        """R6-F12-06: StateUpdateSignal keeps key/state_value/delete"""
        t = StubTransport()
        sent = StateUpdateSignal(
            source="I", kind="state_update", value=None, confidence=1.0,
            timestamp=1.0, key="mode", state_value={"level": 3}, delete=False,
        )
        got = t._decode_signal(t._encode_signal(sent))
        assert type(got) is StateUpdateSignal
        assert (got.key, got.state_value, got.delete) == ("mode", {"level": 3}, False)
        gone = t._decode_signal(t._encode_signal(StateUpdateSignal(
            source="I", kind="state_update", value=None, confidence=1.0,
            timestamp=1.0, key="mode", delete=True)))
        assert isinstance(gone, StateUpdateSignal) and gone.delete is True

    def test_non_str_keys_in_value_and_metadata_survive(self) -> None:
        """R6-F12-22: int keys in value and metadata decode instead of being dropped"""
        t = StubTransport()
        got = t._decode_signal(t._encode_signal(
            _sig("adc", value={0: 1.2, 1: 3.4}, metadata={7: "ch7"})))
        assert got.value == {0: 1.2, 1: 3.4}
        assert got.metadata[7] == "ch7"

    async def test_non_str_keys_cross_nats(self, fake_nats_server: Any) -> None:
        """R6-F12-22: a {0: 1.2} reading published over NATS reaches the subscriber"""
        from arachnite.transport.nats import NATSTransport
        pub, sub = NATSTransport(agent_node_id="A"), NATSTransport(agent_node_id="B")
        await pub.connect()
        await sub.connect()
        got = _R6Collector()
        await sub.subscribe("adc", got)
        await pub.publish(_sig("adc", value={0: 1.2, 1: 3.4}))
        assert [s.value for s in got.seen] == [{0: 1.2, 1: 3.4}]


class TestR6NatsSemantics:
    """R6-F12-01/05: NATS delivers once per callback and survives reconnect cycles."""

    async def test_callback_under_kind_and_wildcard_called_once(
        self, fake_nats_server: Any,
    ) -> None:
        """R6-F12-01: a callback on both 'k' and '*' gets one call per message, not 4"""
        from arachnite.transport.nats import NATSTransport
        pub, sub = NATSTransport(agent_node_id="A"), NATSTransport(agent_node_id="B")
        await pub.connect()
        await sub.connect()
        both, star_only, kind_only = _R6Collector(), _R6Collector(), _R6Collector()
        await sub.subscribe("node_fault", both)
        await sub.subscribe("*", both)
        await sub.subscribe("*", star_only)
        await sub.subscribe("node_fault", kind_only)
        await pub.publish(_sig("node_fault", "faulted"))
        await pub.publish(_sig("other", 1))
        assert [s.kind for s in both.seen] == ["node_fault", "other"]
        assert [s.kind for s in star_only.seen] == ["node_fault", "other"]
        assert [s.kind for s in kind_only.seen] == ["node_fault"]

    async def test_wildcard_matches_kinds_with_dots(self, fake_nats_server: Any) -> None:
        """R6-F12-01: '*' maps to the NATS full wildcard, so 'a.b' kinds arrive too"""
        from arachnite.transport.nats import NATSTransport
        pub, sub = NATSTransport(agent_node_id="A"), NATSTransport(agent_node_id="B")
        await pub.connect()
        await sub.connect()
        star = _R6Collector()
        await sub.subscribe("*", star)
        await pub.publish(_sig("sensor.temp", 1))
        assert [s.kind for s in star.seen] == ["sensor.temp"]

    async def test_disconnect_connect_keeps_subscriptions(self, fake_nats_server: Any) -> None:
        """R6-F12-05: after disconnect()+connect() the kinds are subscribed again"""
        from arachnite.transport.nats import NATSTransport
        pub, sub = NATSTransport(agent_node_id="A"), NATSTransport(agent_node_id="B")
        await pub.connect()
        got = _R6Collector()
        await sub.subscribe("k", got)
        await sub.connect()
        await pub.publish(_sig("k", 1))
        await sub.disconnect()
        await sub.connect()
        await pub.publish(_sig("k", 2))
        assert [s.value for s in got.seen] == [1, 2]
        assert fake_nats_server.live_subjects(sub._nc) == ["arachnite.k"]


class TestR6MqttSemantics:
    """R6-F12-02/03: MQTT wildcard subscriptions and disconnect-during-reconnect."""

    async def test_remote_wildcard_subscriber_receives(self, fake_mqtt_broker: Any) -> None:
        """R6-F12-02: '*' subscribes the MQTT '#' filter, so it matches every kind"""
        from arachnite.transport.mqtt import MQTTTransport
        pub = MQTTTransport("fake", agent_node_id="A")
        sub = MQTTTransport("fake", agent_node_id="B")
        await pub.connect()
        await sub.connect()
        star, both = _R6Collector(), _R6Collector()
        await sub.subscribe("*", star)
        await sub.subscribe("thermal", both)
        await sub.subscribe("*", both)
        await pub.publish(_sig("thermal", 42.0))
        await asyncio.sleep(0.01)
        await pub.disconnect()
        await sub.disconnect()
        assert [s.kind for s in star.seen] == ["thermal"]
        assert len(both.seen) == 1

    async def test_disconnect_during_reconnect_stays_down(self, fake_mqtt_broker: Any) -> None:
        """R6-F12-03: disconnect() while a reconnect is retrying is not undone"""
        from arachnite.transport.mqtt import MQTTTransport
        broker = fake_mqtt_broker
        t = MQTTTransport("fake", agent_node_id="A", reconnect_interval_s=0.02,
                          max_reconnect_attempts=50)
        await t.subscribe("k", _R6Collector())
        await t.connect()
        broker.up = False
        broker.drop_all()                 # connection lost -> background reconnect
        await asyncio.sleep(0.07)         # reconnect is now retrying
        await t.disconnect()
        broker.up = True                  # broker comes back
        await asyncio.sleep(0.1)
        assert not t.connected
        assert broker.clients == []
        assert t._listen_task is None or t._listen_task.done()



class TestR6RedisSemantics:
    """R6-F12-02/04: Redis pattern subscription for '*' and a verified connect()."""

    async def test_remote_wildcard_subscriber_receives(self, fake_redis_server: Any) -> None:
        """R6-F12-02: '*' is a PSUBSCRIBE, so it matches every kind, once per callback"""
        from arachnite.transport.redis import RedisTransport
        pub, sub = RedisTransport(agent_node_id="A"), RedisTransport(agent_node_id="B")
        await pub.connect()
        await sub.connect()
        star, both = _R6Collector(), _R6Collector()
        await sub.subscribe("*", star)
        await sub.subscribe("thermal", both)
        await sub.subscribe("*", both)
        await pub.publish(_sig("thermal", 42.0))
        await asyncio.sleep(0.01)
        await pub.disconnect()
        await sub.disconnect()
        assert [s.kind for s in star.seen] == ["thermal"]
        assert len(both.seen) == 1

    async def test_connect_to_down_server_raises_after_retries(
        self, fake_redis_server: Any,
    ) -> None:
        """R6-F12-04: connect() verifies the server and raises when it is down"""
        from arachnite.transport.redis import RedisTransport
        fake_redis_server.up = False
        t = RedisTransport(agent_node_id="A", reconnect_interval_s=0.0,
                           max_reconnect_attempts=3)
        await t.subscribe("k", _R6Collector())
        with pytest.raises(TransportConnectionError, match="3 attempts"):
            await t.connect()
        assert not t.connected
        assert fake_redis_server.pings == 3


class TestR6TransportReconnectCoverage:
    """R6-F20: coverage of the reworked (re)connect paths through broker fakes.

    Guards, not regressions: both behaviours held (vacuously, for Redis) before
    the review-6 fixes and must keep holding after them.
    """

    async def test_reconnects_after_connection_loss(self, fake_mqtt_broker: Any) -> None:
        """R6-F12-03: a lost connection is re-established and re-subscribed"""
        from arachnite.transport.mqtt import MQTTTransport
        broker = fake_mqtt_broker
        pub = MQTTTransport("fake", agent_node_id="P")
        t = MQTTTransport("fake", agent_node_id="A", reconnect_interval_s=0.01)
        got = _R6Collector()
        await t.subscribe("k", got)
        await t.connect()
        broker.drop_all()
        for _ in range(100):
            await asyncio.sleep(0.01)
            if t.connected:
                break
        await pub.connect()
        await pub.publish(_sig("k", 5))
        await asyncio.sleep(0.01)
        await t.disconnect()
        await pub.disconnect()
        assert [s.value for s in got.seen] == [5]

    async def test_connect_retries_until_server_is_up(self, fake_redis_server: Any) -> None:
        """R6-F12-04: max_reconnect_attempts is honoured by connect()"""
        from arachnite.transport.redis import RedisTransport
        fake_redis_server.up = False
        t = RedisTransport(agent_node_id="A", reconnect_interval_s=0.01,
                           max_reconnect_attempts=50)
        task = asyncio.create_task(t.connect())
        await asyncio.sleep(0.05)
        fake_redis_server.up = True
        await asyncio.wait_for(task, timeout=2.0)
        assert t.connected
        await t.disconnect()
