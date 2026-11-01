"""
arachnite.framework_config
~~~~~~~~~~~~~~~~~~~~~~~~~~
FrameworkConfig — typed configuration loader for the Arachnite runtime.

Reads arachnite.toml (TOML, stdlib tomllib, Python 3.11+).
Supports ${ENV_VAR} and ${ENV_VAR:-default} interpolation in string values.
Every value is checked against its setting's declared type; a string (the
only form ${...} interpolation can produce) is converted, so
``flush_on_write = "${FLUSH:-false}"`` is ``False`` and
``tick_rate_hz = "${HZ:-20}"`` is ``20.0``. A value that does not convert
raises ``ValueError`` naming the section and key.

This config covers the *framework plumbing*: how fast to tick, which transport
to use, logging verbosity, supervisor restart policies, and context history.
It covers framework plumbing only — agent-level concerns (LLM backend,
memory paths, hardware discovery) belong in the agent's own config.

Usage::

    from arachnite.framework_config import FrameworkConfig

    cfg = FrameworkConfig.from_toml()          # reads arachnite.toml
    transport = cfg.build_transport()
    log_sinks = cfg.build_log_sinks()
    context   = cfg.build_context()

    rt = ArachniteRuntime(
        ...,
        tick_rate_hz         = cfg.runtime.tick_rate_hz,
        overrun_warn_pct     = cfg.runtime.overrun_warn_pct,
        overrun_warn_consecutive = cfg.runtime.overrun_warn_consecutive,
        log_sinks            = log_sinks,
        shutdown_coordinator = cfg.build_shutdown_coordinator(),
    )
"""

from __future__ import annotations

import dataclasses
import os
import re
import warnings

try:
    import tomllib
except ImportError:  # Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef,unused-ignore]
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Top-level TOML sections FrameworkConfig reads; anything else is ignored with a warning.
_SECTIONS = frozenset({"runtime", "transport", "logging", "supervisor", "context"})

# ── Sub-configs ────────────────────────────────────────────────────────────────

@dataclass
class RuntimeSettings:
    """Controls the tick loop and graceful shutdown."""
    tick_rate_hz:        float = 10.0   # target ticks per second
    overrun_warn_pct:    float = 0.2    # log warning when tick exceeds interval by this fraction
    overrun_warn_consecutive: int = 3   # warn only after N consecutive overruns (1 = every)
    teardown_timeout_s:  float = 5.0    # grace period for node teardown during stop()


@dataclass
class TransportSettings:
    """Selects the message transport backend."""
    backend: str = "local"   # local | mqtt | nats | redis


@dataclass
class MQTTSettings:
    broker: str = "localhost:1883"   # host:port


@dataclass
class NATSSettings:
    url: str = "nats://localhost:4222"


@dataclass
class RedisSettings:
    url: str = "redis://localhost:6379"


@dataclass
class LoggingSettings:
    """Controls log output."""
    level: str = "WARNING"   # DEBUG | INFO | WARNING | ERROR | CRITICAL
    file:  str = ""          # path to a JSON log file; empty = stdout only


@dataclass
class SupervisorSettings:
    """
    Default restart behaviour applied to all NodeSupervisor instances.
    Can be overridden per AgentNode in a DeploymentManifest.
    """
    restart_policy:  str   = "on_failure"   # never | on_failure | always
    max_restarts:    int   = 3
    restart_delay_s: float = 1.0


@dataclass
class ContextSettings:
    """Controls signal history retention and optional state persistence."""
    history_length:  int   = 10
    state_path:      str   = ""      # empty = in-memory only; path → JSON file
    flush_on_write:  bool  = False   # flush state to disk on every set()/delete()
    max_state_keys:  int   = 0       # 0 = no limit; positive int = cap with LRU eviction


# ── Top-level config ───────────────────────────────────────────────────────────

@dataclass
class FrameworkConfig:
    """
    Typed representation of arachnite.toml.

    Load with::

        cfg = FrameworkConfig.from_toml()   # reads arachnite.toml

    Then build runtime objects::

        transport = cfg.build_transport()
        log_sinks = cfg.build_log_sinks()
        context   = cfg.build_context()
        supervisor_kwargs = cfg.build_supervisor_kwargs()
    """

    runtime:    RuntimeSettings    = field(default_factory=RuntimeSettings)
    transport:  TransportSettings  = field(default_factory=TransportSettings)
    mqtt:       MQTTSettings       = field(default_factory=MQTTSettings)
    nats:       NATSSettings       = field(default_factory=NATSSettings)
    redis:      RedisSettings      = field(default_factory=RedisSettings)
    logging:    LoggingSettings    = field(default_factory=LoggingSettings)
    supervisor: SupervisorSettings = field(default_factory=SupervisorSettings)
    context:    ContextSettings    = field(default_factory=ContextSettings)

    # ── Loader ─────────────────────────────────────────────────────────────────

    @classmethod
    def from_toml(cls, path: str | Path = "arachnite.toml") -> FrameworkConfig:
        """
        Load configuration from a TOML file.

        If the file does not exist at *path*, all settings fall back to their
        defaults — this allows code to call from_toml() unconditionally and
        work out-of-the-box without a config file present.

        String values support ${VAR} and ${VAR:-default} interpolation.
        """
        p = Path(path)
        if not p.exists():
            return cls()
        raw = p.read_text(encoding="utf-8")
        data = _interpolate_env_in_values(tomllib.loads(raw))
        return cls._from_dict(data)

    @classmethod
    def _from_dict(cls, d: dict[str, Any]) -> FrameworkConfig:
        def _pick(
            src: dict[str, Any], dc: type, section: str, subtables: tuple[str, ...] = (),
        ) -> dict[str, Any]:
            types = {f.name: f.type for f in dataclasses.fields(dc)}
            unknown = sorted(k for k in src if k not in types and k not in subtables)
            if unknown:
                # A misspelt key would otherwise leave its setting at the
                # default without a trace.
                warnings.warn(
                    f"[{section}]: unknown key(s) ignored: {', '.join(unknown)}",
                    UserWarning,
                    stacklevel=4,
                )
            return {
                k: _coerce(dc.__name__, k, str(types[k]), v)
                for k, v in src.items() if k in types
            }

        unknown_sections = sorted(set(d) - _SECTIONS)
        if unknown_sections:
            warnings.warn(
                f"unknown section(s) ignored: {', '.join(unknown_sections)}",
                UserWarning,
                stacklevel=3,
            )

        rt_d   = d.get("runtime", {})
        tr_d   = d.get("transport", {})
        log_d  = d.get("logging", {})
        sup_d  = d.get("supervisor", {})
        ctx_d  = d.get("context", {})

        return cls(
            runtime    = RuntimeSettings(**_pick(rt_d, RuntimeSettings, "runtime")),
            transport  = TransportSettings(
                **_pick(tr_d, TransportSettings, "transport", ("mqtt", "nats", "redis")),
            ),
            mqtt       = MQTTSettings(
                **_pick(tr_d.get("mqtt", {}), MQTTSettings, "transport.mqtt"),
            ),
            nats       = NATSSettings(
                **_pick(tr_d.get("nats", {}), NATSSettings, "transport.nats"),
            ),
            redis      = RedisSettings(
                **_pick(tr_d.get("redis", {}), RedisSettings, "transport.redis"),
            ),
            logging    = LoggingSettings(**_pick(log_d, LoggingSettings, "logging")),
            supervisor = SupervisorSettings(
                **_pick(sup_d, SupervisorSettings, "supervisor"),
            ),
            context    = ContextSettings(**_pick(ctx_d, ContextSettings, "context")),
        )

    # ── Builders ───────────────────────────────────────────────────────────────

    def build_transport(self) -> Any:
        """
        Instantiate the transport specified by transport.backend.

        LocalTransport is always available; others require optional extras.
        Raises ValueError for unknown backend names.
        Raises ImportError if the required backend package is not installed.
        """
        name = self.transport.backend
        if name == "local":
            from arachnite.transport.local import LocalTransport  # noqa: PLC0415
            return LocalTransport()
        if name == "mqtt":
            from arachnite.transport.mqtt import MQTTTransport  # noqa: PLC0415
            host, _, port_str = self.mqtt.broker.partition(":")
            port = int(port_str) if port_str else 1883
            return MQTTTransport(broker_host=host, broker_port=port)
        if name == "nats":
            from arachnite.transport.nats import NATSTransport  # noqa: PLC0415
            return NATSTransport(servers=self.nats.url)
        if name == "redis":
            from arachnite.transport.redis import RedisTransport  # noqa: PLC0415
            return RedisTransport(url=self.redis.url)
        raise ValueError(
            f"Unknown transport backend '{name}'. "
            "Valid values: 'local', 'mqtt', 'nats', 'redis'."
        )

    def build_log_sinks(self) -> list[Any]:
        """
        Build the list of log sinks from the [logging] section.

        Always includes a StdoutLogSink at the configured level.
        Adds a FileLogSink (JSON) when logging.file is non-empty.
        """
        from arachnite.logging import LogLevel, StdoutLogSink  # noqa: PLC0415

        level_map = {
            "DEBUG":    LogLevel.DEBUG,
            "INFO":     LogLevel.INFO,
            "WARNING":  LogLevel.WARNING,
            "ERROR":    LogLevel.ERROR,
            "CRITICAL": LogLevel.CRITICAL,
        }
        level = level_map.get(self.logging.level.upper(), LogLevel.WARNING)
        sinks: list[Any] = [StdoutLogSink(level=level)]

        if self.logging.file:
            from arachnite.web import FileLogSink  # noqa: PLC0415
            file_sink = FileLogSink(path=self.logging.file, level=level)
            file_sink.open()  # an unopened FileLogSink drops every event
            sinks.append(file_sink)

        return sinks

    def build_context(self) -> Any:
        """
        Build a ContextNode from the [context] section.

        Passes state_path and flush_on_write so state survives reboots when
        configured.
        """
        from arachnite.context import ContextNode  # noqa: PLC0415

        return ContextNode(
            history_length = self.context.history_length,
            state_path     = self.context.state_path or None,
            flush_on_write = self.context.flush_on_write,
            max_state_keys = self.context.max_state_keys or None,
        )

    def build_shutdown_coordinator(self) -> Any:
        """
        Build a ShutdownCoordinator applying ``runtime.teardown_timeout_s``.

        Usage::

            rt = ArachniteRuntime(..., shutdown_coordinator=cfg.build_shutdown_coordinator())
        """
        from arachnite.shutdown import ShutdownCoordinator  # noqa: PLC0415

        return ShutdownCoordinator(teardown_timeout_s=self.runtime.teardown_timeout_s)

    def build_supervisor_kwargs(self) -> dict[str, Any]:
        """
        Return keyword arguments to pass to each NodeSupervisor constructor.

        Usage::

            kwargs = cfg.build_supervisor_kwargs()
            sup = NodeSupervisor(bus, supervisor_id="sense", **kwargs)
        """
        from arachnite.models import RestartPolicy  # noqa: PLC0415

        policy_map = {
            "never":      RestartPolicy.NEVER,
            "on_failure": RestartPolicy.ON_FAILURE,
            "always":     RestartPolicy.ALWAYS,
        }
        policy_name = self.supervisor.restart_policy.lower()
        if policy_name not in policy_map:
            raise ValueError(
                f"Unknown restart_policy '{policy_name}'. "
                "Valid values: 'never', 'on_failure', 'always'."
            )
        return {
            "restart_policy":  policy_map[policy_name],
            "max_restarts":    self.supervisor.max_restarts,
            "restart_delay_s": self.supervisor.restart_delay_s,
        }


# ── Type coercion ──────────────────────────────────────────────────────────────

_TRUE_STRINGS = frozenset({"true", "1", "yes", "on"})
_FALSE_STRINGS = frozenset({"false", "0", "no", "off", ""})


def _coerce(section: str, key: str, type_name: str, value: Any) -> Any:
    """
    Check ``value`` against a setting's declared type (``bool``, ``int``,
    ``float`` or ``str``), converting strings — ``${VAR}`` interpolation only
    ever yields strings. Raises ValueError for anything that does not fit
    (e.g. ``"maybe"`` for a bool, ``True`` or ``"fast"`` for a float).
    """
    def _bad() -> ValueError:
        return ValueError(
            f"{section}.{key}: expected {type_name}, got {value!r}"
        )

    if type_name == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            text = value.strip().lower()
            if text in _TRUE_STRINGS:
                return True
            if text in _FALSE_STRINGS:
                return False
        raise _bad()
    if type_name in ("int", "float"):
        if isinstance(value, bool):
            raise _bad()
        try:
            if type_name == "int":
                if isinstance(value, int):
                    return value
                if isinstance(value, float) and value.is_integer():
                    return int(value)
                if isinstance(value, str):
                    return int(value.strip())
                raise _bad()
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                return float(value.strip())
        except ValueError:
            raise _bad() from None
        raise _bad()
    if type_name == "str":
        if isinstance(value, str):
            return value
        raise _bad()
    return value


# ── Env-var interpolation ──────────────────────────────────────────────────────

_ENV_PATTERN = re.compile(r"\$\{([^}:-]+)(?::-(.*?))?\}")


def _interpolate_env(text: str) -> str:
    """Replace ${VAR} and ${VAR:-default} with environment variable values."""
    def _replace(m: re.Match[str]) -> str:
        var, default = m.group(1), m.group(2)
        value = os.environ.get(var)
        if value is not None:
            return value
        if default is not None:
            return default
        raise KeyError(f"Environment variable '{var}' is not set and has no default")
    return _ENV_PATTERN.sub(_replace, text)


def _interpolate_env_in_values(data: Any) -> Any:
    """Walk a parsed TOML tree and interpolate env vars in string leaves only."""
    if isinstance(data, dict):
        return {k: _interpolate_env_in_values(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_interpolate_env_in_values(v) for v in data]
    if isinstance(data, str):
        return _interpolate_env(data)
    return data
