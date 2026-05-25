"""Unit tests for arachnite.framework_config (FrameworkConfig, sub-settings, builders)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from arachnite.framework_config import (
    ContextSettings,
    FrameworkConfig,
    LoggingSettings,
    MQTTSettings,
    NATSSettings,
    RedisSettings,
    RuntimeSettings,
    SupervisorSettings,
    TransportSettings,
    _interpolate_env,
    _interpolate_env_in_values,
)

# ── _interpolate_env ──────────────────────────────────────────────────────────

class TestInterpolateEnv:
    def test_no_vars(self) -> None:
        assert _interpolate_env("hello world") == "hello world"

    def test_present_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_VAR", "42")
        assert _interpolate_env("value=${MY_VAR}") == "value=42"

    def test_default_used_when_var_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        assert _interpolate_env("x=${MISSING_VAR:-fallback}") == "x=fallback"

    def test_missing_var_no_default_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(KeyError, match="MISSING_VAR"):
            _interpolate_env("${MISSING_VAR}")

    def test_multiple_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("A", "1")
        monkeypatch.setenv("B", "2")
        assert _interpolate_env("${A}-${B}") == "1-2"


# ── _interpolate_env_in_values ────────────────────────────────────────────────

class TestInterpolateEnvInValues:
    def test_walks_nested_dicts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("X", "hello")
        data = {"a": {"b": "${X}"}}
        assert _interpolate_env_in_values(data) == {"a": {"b": "hello"}}

    def test_walks_lists(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("Y", "world")
        data = {"items": ["${Y}", "literal"]}
        assert _interpolate_env_in_values(data) == {"items": ["world", "literal"]}

    def test_non_string_leaves_untouched(self) -> None:
        data = {"count": 42, "flag": True, "ratio": 3.14}
        assert _interpolate_env_in_values(data) == data

    def test_missing_var_in_string_leaf_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GONE", raising=False)
        with pytest.raises(KeyError, match="GONE"):
            _interpolate_env_in_values({"key": "${GONE}"})


# ── FrameworkConfig defaults ──────────────────────────────────────────────────

class TestFrameworkConfigDefaults:
    def test_defaults_when_no_file(self) -> None:
        cfg = FrameworkConfig.from_toml("nonexistent_path_xyz.toml")
        assert cfg.runtime.tick_rate_hz == 10.0
        assert cfg.transport.backend == "local"
        assert cfg.logging.level == "WARNING"
        assert cfg.supervisor.max_restarts == 3
        assert cfg.context.history_length == 10

    def test_default_construction(self) -> None:
        cfg = FrameworkConfig()
        assert isinstance(cfg.runtime, RuntimeSettings)
        assert isinstance(cfg.transport, TransportSettings)
        assert isinstance(cfg.mqtt, MQTTSettings)
        assert isinstance(cfg.nats, NATSSettings)
        assert isinstance(cfg.redis, RedisSettings)
        assert isinstance(cfg.logging, LoggingSettings)
        assert isinstance(cfg.supervisor, SupervisorSettings)
        assert isinstance(cfg.context, ContextSettings)


# ── FrameworkConfig.from_toml ─────────────────────────────────────────────────

class TestFromToml:
    def _write_toml(self, content: str) -> Path:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(content)
            name = tmp.name
        return Path(name)

    def test_runtime_section(self) -> None:
        p = self._write_toml("""
[runtime]
tick_rate_hz = 20.0
overrun_warn_pct = 0.1
overrun_warn_consecutive = 5
teardown_timeout_s = 3.0
""")
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.runtime.tick_rate_hz == 20.0
            assert cfg.runtime.overrun_warn_pct == 0.1
            assert cfg.runtime.overrun_warn_consecutive == 5
            assert cfg.runtime.teardown_timeout_s == 3.0
        finally:
            p.unlink()

    def test_runtime_overrun_warn_consecutive_default_preserved(self) -> None:
        p = self._write_toml("""
[runtime]
tick_rate_hz = 20.0
""")
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.runtime.overrun_warn_consecutive == 3
        finally:
            p.unlink()

    def test_transport_section(self) -> None:
        p = self._write_toml('[transport]\nbackend = "mqtt"\n')
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.transport.backend == "mqtt"
        finally:
            p.unlink()

    def test_mqtt_subsection(self) -> None:
        p = self._write_toml("""
[transport]
backend = "mqtt"
[transport.mqtt]
broker = "192.168.1.10:1883"
""")
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.mqtt.broker == "192.168.1.10:1883"
        finally:
            p.unlink()

    def test_nats_subsection(self) -> None:
        p = self._write_toml("""
[transport.nats]
url = "nats://myserver:4222"
""")
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.nats.url == "nats://myserver:4222"
        finally:
            p.unlink()

    def test_redis_subsection(self) -> None:
        p = self._write_toml("""
[transport.redis]
url = "redis://myserver:6379"
""")
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.redis.url == "redis://myserver:6379"
        finally:
            p.unlink()

    def test_logging_section(self) -> None:
        p = self._write_toml('[logging]\nlevel = "DEBUG"\nfile = "/tmp/log.json"\n')
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.logging.level == "DEBUG"
            assert cfg.logging.file == "/tmp/log.json"
        finally:
            p.unlink()

    def test_supervisor_section(self) -> None:
        p = self._write_toml("""
[supervisor]
restart_policy = "always"
max_restarts = 5
restart_delay_s = 2.0
""")
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.supervisor.restart_policy == "always"
            assert cfg.supervisor.max_restarts == 5
            assert cfg.supervisor.restart_delay_s == 2.0
        finally:
            p.unlink()

    def test_context_section(self) -> None:
        p = self._write_toml("""
[context]
history_length = 25
flush_on_write = true
""")
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.context.history_length == 25
            assert cfg.context.flush_on_write is True
        finally:
            p.unlink()

    def test_env_interpolation_in_toml(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_BACKEND", "mqtt")
        p = self._write_toml('[transport]\nbackend = "${MY_BACKEND}"\n')
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.transport.backend == "mqtt"
        finally:
            p.unlink()

    def test_env_var_in_comment_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("UNSET_VAR", raising=False)
        p = self._write_toml(
            '# Example: ${UNSET_VAR} is the syntax\n'
            '[runtime]\n'
            'tick_rate_hz = 20.0\n'
        )
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.runtime.tick_rate_hz == 20.0
        finally:
            p.unlink()

    def test_env_var_in_inline_comment_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("WHATEVER", raising=False)
        p = self._write_toml(
            '[runtime]\n'
            'tick_rate_hz = 20.0  # see ${WHATEVER}\n'
        )
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.runtime.tick_rate_hz == 20.0
        finally:
            p.unlink()

    def test_unknown_keys_ignored(self) -> None:
        p = self._write_toml('[runtime]\ntick_rate_hz = 5.0\nunknown_key = "ignored"\n')
        try:
            cfg = FrameworkConfig.from_toml(p)
            assert cfg.runtime.tick_rate_hz == 5.0
        finally:
            p.unlink()


# ── build_transport ───────────────────────────────────────────────────────────

class TestBuildTransport:
    def test_local_transport(self) -> None:
        from arachnite.transport.local import LocalTransport
        cfg = FrameworkConfig()
        t = cfg.build_transport()
        assert isinstance(t, LocalTransport)

    def test_unknown_backend_raises(self) -> None:
        cfg = FrameworkConfig(transport=TransportSettings(backend="bogus"))
        with pytest.raises(ValueError, match="bogus"):
            cfg.build_transport()


# ── build_log_sinks ───────────────────────────────────────────────────────────

class TestBuildLogSinks:
    def test_stdout_sink_included(self) -> None:
        from arachnite.logging import StdoutLogSink
        cfg = FrameworkConfig()
        sinks = cfg.build_log_sinks()
        assert len(sinks) >= 1
        assert any(isinstance(s, StdoutLogSink) for s in sinks)

    def test_log_level_applied(self) -> None:
        from arachnite.logging import LogLevel, StdoutLogSink
        cfg = FrameworkConfig(logging=LoggingSettings(level="DEBUG"))
        sinks = cfg.build_log_sinks()
        stdout = next(s for s in sinks if isinstance(s, StdoutLogSink))
        assert stdout.level == LogLevel.DEBUG

    def test_unknown_level_falls_back_to_warning(self) -> None:
        from arachnite.logging import LogLevel, StdoutLogSink
        cfg = FrameworkConfig(logging=LoggingSettings(level="NOTLEVEL"))
        sinks = cfg.build_log_sinks()
        stdout = next(s for s in sinks if isinstance(s, StdoutLogSink))
        assert stdout.level == LogLevel.WARNING

    def test_no_file_sink_when_empty(self) -> None:
        cfg = FrameworkConfig(logging=LoggingSettings(file=""))
        sinks = cfg.build_log_sinks()
        assert len(sinks) == 1


# ── build_context ─────────────────────────────────────────────────────────────

class TestBuildContext:
    def test_returns_context_node(self) -> None:
        from arachnite.context import ContextNode
        cfg = FrameworkConfig()
        ctx = cfg.build_context()
        assert isinstance(ctx, ContextNode)

    def test_history_length_applied(self) -> None:
        from arachnite.context import ContextNode
        cfg = FrameworkConfig(context=ContextSettings(history_length=5))
        ctx = cfg.build_context()
        assert isinstance(ctx, ContextNode)

    def test_state_path_none_when_empty(self) -> None:
        from arachnite.context import ContextNode
        cfg = FrameworkConfig(context=ContextSettings(state_path=""))
        ctx = cfg.build_context()
        assert isinstance(ctx, ContextNode)


# ── build_supervisor_kwargs ───────────────────────────────────────────────────

class TestBuildSupervisorKwargs:
    def test_on_failure_policy(self) -> None:
        from arachnite.models import RestartPolicy
        cfg = FrameworkConfig(supervisor=SupervisorSettings(restart_policy="on_failure"))
        kwargs = cfg.build_supervisor_kwargs()
        assert kwargs["restart_policy"] == RestartPolicy.ON_FAILURE
        assert kwargs["max_restarts"] == 3
        assert kwargs["restart_delay_s"] == 1.0

    def test_never_policy(self) -> None:
        from arachnite.models import RestartPolicy
        cfg = FrameworkConfig(supervisor=SupervisorSettings(restart_policy="never"))
        kwargs = cfg.build_supervisor_kwargs()
        assert kwargs["restart_policy"] == RestartPolicy.NEVER

    def test_always_policy(self) -> None:
        from arachnite.models import RestartPolicy
        cfg = FrameworkConfig(supervisor=SupervisorSettings(restart_policy="always"))
        kwargs = cfg.build_supervisor_kwargs()
        assert kwargs["restart_policy"] == RestartPolicy.ALWAYS

    def test_unknown_policy_raises(self) -> None:
        cfg = FrameworkConfig(supervisor=SupervisorSettings(restart_policy="sometimes"))
        with pytest.raises(ValueError, match="sometimes"):
            cfg.build_supervisor_kwargs()

    def test_custom_values(self) -> None:
        cfg = FrameworkConfig(
            supervisor=SupervisorSettings(
                restart_policy="always", max_restarts=10, restart_delay_s=0.5
            )
        )
        kwargs = cfg.build_supervisor_kwargs()
        assert kwargs["max_restarts"] == 10
        assert kwargs["restart_delay_s"] == 0.5
