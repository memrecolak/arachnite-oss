"""
arachnite.distributed.manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DeploymentManifest: declarative YAML-based deployment configuration.
NodeAssignment: binding of a node class to an AgentNode with its config.
Spec reference: Section 11.
"""

from __future__ import annotations

import importlib
import os
import re
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from arachnite.distributed.colocation import validate_colocation
from arachnite.exceptions import ManifestValidationError
from arachnite.models import Permission
from arachnite.nodes.action import BaseActionNode
from arachnite.nodes.base import BaseNode
from arachnite.nodes.decision import BaseDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, BaseReflexInstinctNode
from arachnite.nodes.sense import BaseSenseNode

try:
    import yaml  # type: ignore[import-untyped,unused-ignore]
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

# Env-var interpolation pattern: ${VAR} or ${VAR:-default}
_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
# Patterns that look like plain secrets (warn but don't error)
_SECRET_PATTERNS = re.compile(
    r"(bearer_|sk-|password\s*=|api.?key\s*=|token\s*=)", re.IGNORECASE
)


# Manifest node sections and the base class every node listed there must extend.
_SECTION_BASES: dict[str, type[BaseNode]] = {
    "sense":    BaseSenseNode,
    "instinct": BaseInstinctNode,
    "decision": BaseDecisionNode,
    "action":   BaseActionNode,
}

# transport_config keys each network transport understands (spec §11.1).
# A key outside this set is a ManifestValidationError, not silently ignored.
_TRANSPORT_CONFIG_KEYS: dict[str, frozenset[str]] = {
    "mqtt": frozenset({
        "broker_host", "broker_port", "topic_prefix", "qos", "username",
        "password", "tls", "reconnect_interval_s", "max_reconnect_attempts",
    }),
    "nats": frozenset({
        "servers", "subject_prefix", "reconnect_interval_s",
        "max_reconnect_attempts",
    }),
    "redis": frozenset({
        "url", "channel_prefix", "db", "password", "reconnect_interval_s",
        "max_reconnect_attempts",
    }),
}
_TRANSPORT_KINDS = frozenset({"", "local", *_TRANSPORT_CONFIG_KEYS})


_SECRET_KEY_PATTERNS = re.compile(
    r"(password|api[_\-]?key|secret|token|bearer|private[_\-]?key)",
    re.IGNORECASE,
)


def _check_for_secrets(value: Any, path: str = "") -> None:
    """
    Recursively walk a manifest dict and warn if any string value looks like
    a hardcoded secret — either the value itself matches a secret pattern, or
    the dict key is a well-known secret field name with a non-empty value that
    is not an ${ENV_VAR} reference.

    Secrets should always be injected via ${ENV_VAR} interpolation, never
    written literally into the manifest file.
    """
    if isinstance(value, str):
        if _SECRET_PATTERNS.search(value):
            warnings.warn(
                f"Manifest path '{path}' appears to contain a hardcoded secret "
                f"(matched pattern: {_SECRET_PATTERNS.pattern!r}). "
                "Use ${VAR} environment variable interpolation instead.",
                stacklevel=4,
            )
    elif isinstance(value, dict):
        for k, v in value.items():
            child_path = f"{path}.{k}" if path else k
            # Warn when a secret-named key holds a literal (non-interpolated) string
            if (
                isinstance(v, str)
                and v
                and not _ENV_RE.fullmatch(v)
                and _SECRET_KEY_PATTERNS.search(k)
            ):
                warnings.warn(
                    f"Manifest key '{child_path}' looks like a secret field "
                    "but its value is not an environment variable reference. "
                    "Use ${VAR} environment variable interpolation instead.",
                    stacklevel=4,
                )
            else:
                _check_for_secrets(v, path=child_path)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            _check_for_secrets(item, path=f"{path}[{i}]")


def _resolve_env_in_value(value: Any) -> Any:
    """Recursively resolve ${VAR} and ${VAR:-default} in strings."""
    if isinstance(value, str):
        def replacer(m: re.Match[str]) -> str:
            var, default = m.group(1), m.group(2)
            env_val = os.environ.get(var)
            if env_val is not None:
                return env_val
            if default is not None:
                return default
            raise ManifestValidationError(
                [f"Environment variable '{var}' is not set and has no default."]
            )
        return _ENV_RE.sub(replacer, value)
    if isinstance(value, dict):
        return {k: _resolve_env_in_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_in_value(v) for v in value]
    return value


@dataclass
class NodeAssignment:
    """
    The internal binding of a node class, its configuration, and its
    owning AgentNode, derived from the manifest.
    Spec reference: Section 11.3.
    """
    node_class:        type[BaseNode]
    agent_node_id:     str
    node_section:      str          # 'sense' | 'instinct' | 'decision' | 'action'
    config:            dict[str, Any] = field(default_factory=dict)
    is_reflex:         bool = False
    reflex_action_id:  str | None = None
    allowed_permissions: set[Permission] | None = None  # None = skip validation

    @property
    def node_id(self) -> str:
        """
        The id the node registers under: its class's ``node_id`` attribute.

        This is the id a ``Proposal.action_id`` / reflex ``action_id`` must
        match, so co-location and duplicate checks use it, not the class name
        (``BaseNode`` defaults ``node_id`` to the class name when unset).
        """
        node_id = getattr(self.node_class, "node_id", None)
        return node_id if isinstance(node_id, str) and node_id else self.node_class.__name__


@dataclass
class AgentNodeConfig:
    """Raw configuration for one AgentNode, parsed from the manifest."""
    agent_id:      str
    description:   str
    transport:     str
    tick_rate_hz:  float
    tags:          list[str]
    node_sections: dict[str, list[dict[str, Any]]]  # section -> list of node defs
    transport_config: dict[str, Any] = field(default_factory=dict)
    on_transport_fault: dict[str, Any] = field(default_factory=dict)
    # Egress allowlist for this AgentNode's bus. None = forward every kind onto
    # the transport (default); a set restricts egress to those kinds. See
    # SignalBus(wire_kinds=…). An empty set forwards nothing (inbound only).
    wire_kinds: set[str] | None = None


class DeploymentManifest:
    """
    Declarative deployment manifest.

    Parses a YAML file, resolves env-var references, loads node classes,
    validates co-location constraints, and produces NodeAssignment objects
    that AgentNode and MeshRuntime use to build the runtime.

    Spec reference: Section 11.
    """

    def __init__(
        self,
        mesh_config:    dict[str, Any],
        agent_configs:  list[AgentNodeConfig],
        assignments:    list[NodeAssignment],
    ) -> None:
        self._mesh_config   = mesh_config
        self._agent_configs = {a.agent_id: a for a in agent_configs}
        self._assignments   = assignments

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_yaml(cls, path: str | Path) -> DeploymentManifest:
        """
        Load, parse and validate a manifest from a YAML file.
        Resolves environment variable references automatically and raises
        ManifestValidationError for a manifest that fails ``validate()``.
        """
        if not _YAML_AVAILABLE:
            raise ImportError(
                "DeploymentManifest.from_yaml() requires 'pyyaml'. "
                'Install with: pip install "arachnite[yaml]"'
            )
        with open(path) as f:
            raw = yaml.safe_load(f)
        return cls._parse(raw)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DeploymentManifest:
        """Load and validate a manifest from a Python dict (useful in tests)."""
        return cls._parse(data)

    @classmethod
    def _parse(cls, raw: dict[str, Any]) -> DeploymentManifest:
        _check_for_secrets(raw)           # warn before env substitution
        raw = _resolve_env_in_value(raw)
        mesh_config   = raw.get("mesh", {})
        agents_raw    = raw.get("agents", [])

        agent_configs: list[AgentNodeConfig] = []
        assignments:   list[NodeAssignment]  = []

        for agent_raw in agents_raw:
            ac = cls._parse_agent(agent_raw, mesh_config)
            agent_configs.append(ac)
            assignments.extend(cls._parse_assignments(ac))

        manifest = cls(mesh_config, agent_configs, assignments)
        # Enforced at load time (spec §10.4): a manifest that splits a reflex
        # from its action, or breaks its permission whitelist, never loads.
        manifest.validate()
        return manifest

    @staticmethod
    def _parse_agent(raw: dict[str, Any], mesh: dict[str, Any]) -> AgentNodeConfig:
        # Egress allowlist: absent (or null) => forward every kind; a list =>
        # restrict egress to those kinds; an empty list => forward nothing.
        wire_kinds_raw = raw.get("wire_kinds")
        wire_kinds: set[str] | None = None
        if wire_kinds_raw is not None:
            if not isinstance(wire_kinds_raw, list):
                raise ManifestValidationError(
                    [f"AgentNode '{raw.get('id')}': 'wire_kinds' must be a list "
                     "of signal kinds (or omitted to forward every kind)."]
                )
            wire_kinds = {str(k) for k in wire_kinds_raw}

        return AgentNodeConfig(
            agent_id      = str(raw["id"]),
            description   = str(raw.get("description", "")),
            transport     = str(raw.get("transport", mesh.get("transport_default", "local"))),
            tick_rate_hz  = float(raw.get("tick_rate_hz", 10.0)),
            tags          = list(raw.get("tags", [])),
            node_sections = raw.get("nodes", {}),
            transport_config = raw.get("transport_config", {}),
            on_transport_fault = raw.get("on_transport_fault", {}),
            wire_kinds    = wire_kinds,
        )

    @staticmethod
    def _parse_assignments(ac: AgentNodeConfig) -> list[NodeAssignment]:
        assignments: list[NodeAssignment] = []
        for section, node_defs in ac.node_sections.items():
            if not isinstance(node_defs, list):
                continue
            for node_def in node_defs:
                if isinstance(node_def, str):
                    node_def = {"kind": node_def}
                kind       = str(node_def["kind"])
                config     = dict(node_def.get("config", {}))
                is_reflex  = bool(node_def.get("reflex", False))
                action_id  = node_def.get("action_id")

                node_class = _import_node_class(kind)

                # Detect reflex from class inheritance if not explicit in manifest
                if not is_reflex and issubclass(node_class, BaseReflexInstinctNode):
                    is_reflex = True

                # Parse allowed_permissions whitelist
                raw_perms = node_def.get("permissions")
                allowed_permissions: set[Permission] | None = None
                if raw_perms is not None:
                    allowed_permissions = set()
                    for p in raw_perms:
                        try:
                            allowed_permissions.add(Permission(p))
                        except ValueError as err:
                            valid = ", ".join(e.value for e in Permission)
                            raise ManifestValidationError(
                                [f"Unknown permission '{p}' for node '{kind}'. "
                                 f"Valid: {valid}"]
                            ) from err

                assignments.append(NodeAssignment(
                    node_class          = node_class,
                    agent_node_id       = ac.agent_id,
                    node_section        = section,
                    config              = config,
                    is_reflex           = is_reflex,
                    reflex_action_id    = str(action_id) if action_id else None,
                    allowed_permissions = allowed_permissions,
                ))
        return assignments

    # ── Validation ────────────────────────────────────────────────────────────

    def validate(self) -> None:
        """
        Validate the manifest. Raises ManifestValidationError on failure.

        Runs automatically when a manifest is loaded (``from_dict`` /
        ``from_yaml``) and again in ``AgentNode.from_manifest`` and
        ``MeshRuntime.start``. Checks:
        - Node sections are sense/instinct/decision/action, and each listed
          class extends that section's base class.
        - No duplicate node_ids within an AgentNode.
        - Every node's declared permissions are within its manifest whitelist.
        - Transport kinds are known and transport_config holds only keys the
          transport understands; ``on_transport_fault`` (not implemented) is
          rejected rather than silently ignored.
        - Every reflex node is co-located with its target action node (by
          node_id, the id its proposals target).
        """
        errors: list[str] = []

        for agent_id, ac in self._agent_configs.items():
            errors.extend(_section_errors(agent_id, ac))
            errors.extend(_transport_errors(agent_id, ac, self._mesh_config))

        for a in self._assignments:
            base = _SECTION_BASES.get(a.node_section)
            if base is not None and not issubclass(a.node_class, base):
                errors.append(
                    f"AgentNode '{a.agent_node_id}': node '{a.node_id}' "
                    f"({a.node_class.__name__}) is listed under '{a.node_section}' "
                    f"but is not a {base.__name__}."
                )

        # Check for duplicate node_ids within an AgentNode
        for agent_id in self._agent_configs:
            seen: set[str] = set()
            for a in self.assignments_for(agent_id):
                if a.node_id in seen:
                    errors.append(
                        f"AgentNode '{agent_id}': duplicate node_id '{a.node_id}'."
                    )
                seen.add(a.node_id)

        # Permission whitelist check
        for a in self._assignments:
            if a.allowed_permissions is None:
                continue
            declared: frozenset[Permission] | set[Permission] = getattr(
                a.node_class, "permissions", frozenset(),
            )
            denied = declared - a.allowed_permissions
            if denied:
                denied_str = ", ".join(sorted(p.value for p in denied))
                allowed_str = ", ".join(sorted(p.value for p in a.allowed_permissions)) or "(none)"
                errors.append(
                    f"Node '{a.node_id}' on AgentNode '{a.agent_node_id}' declares "
                    f"permissions [{denied_str}] not in its allowed set [{allowed_str}]."
                )

        if errors:
            raise ManifestValidationError(errors)

        # Co-location check
        colocation_data: list[dict[str, object]] = [
            {
                "node_id":         a.node_id,
                "agent_node_id":   a.agent_node_id,
                "node_section":    a.node_section,
                "is_reflex":       a.is_reflex,
                "reflex_action_id": a.reflex_action_id,
            }
            for a in self._assignments
        ]
        validate_colocation(colocation_data)

    # ── Accessors ─────────────────────────────────────────────────────────────

    def assignments_for(self, agent_node_id: str) -> list[NodeAssignment]:
        """Return all NodeAssignments for a specific AgentNode."""
        return [a for a in self._assignments if a.agent_node_id == agent_node_id]

    def agent_ids(self) -> list[str]:
        """Return all AgentNode ids defined in the manifest."""
        return list(self._agent_configs)

    def agent_config(self, agent_id: str) -> AgentNodeConfig:
        if agent_id not in self._agent_configs:
            raise KeyError(f"AgentNode '{agent_id}' not found in manifest.")
        return self._agent_configs[agent_id]

    @property
    def mesh_config(self) -> dict[str, Any]:
        return dict(self._mesh_config)

    @property
    def assignments(self) -> list[NodeAssignment]:
        return list(self._assignments)

    def __repr__(self) -> str:
        return (
            f"DeploymentManifest("
            f"agents={list(self._agent_configs)}, "
            f"assignments={len(self._assignments)})"
        )


def _section_errors(agent_id: str, ac: AgentNodeConfig) -> list[str]:
    """Unknown node-section names (a typo would silently drop its nodes)."""
    if not isinstance(ac.node_sections, dict):
        return [f"AgentNode '{agent_id}': 'nodes' must be a mapping of sections."]
    valid = ", ".join(_SECTION_BASES)
    return [
        f"AgentNode '{agent_id}': unknown node section '{section}' (valid: {valid})."
        for section in ac.node_sections
        if section not in _SECTION_BASES
    ]


def _transport_errors(
    agent_id: str, ac: AgentNodeConfig, mesh: dict[str, Any],
) -> list[str]:
    """Transport kind, transport_config keys, and unsupported options."""
    errors: list[str] = []
    if ac.transport not in _TRANSPORT_KINDS:
        errors.append(
            f"AgentNode '{agent_id}': unknown transport '{ac.transport}' "
            "(valid: local, mqtt, nats, redis)."
        )
    if not isinstance(ac.transport_config, dict):
        errors.append(f"AgentNode '{agent_id}': 'transport_config' must be a mapping.")
    elif ac.transport in _TRANSPORT_CONFIG_KEYS:
        unknown = sorted(set(ac.transport_config) - _TRANSPORT_CONFIG_KEYS[ac.transport])
        if unknown:
            valid = ", ".join(sorted(_TRANSPORT_CONFIG_KEYS[ac.transport]))
            errors.append(
                f"AgentNode '{agent_id}': unknown {ac.transport} transport_config "
                f"key(s) {unknown} (valid: {valid})."
            )
    broker = mesh.get("mqtt_broker")
    if (
        ac.transport == "mqtt"
        and broker is not None
        and _parse_host_port(str(broker), 1883) is None
    ):
        errors.append(f"mesh.mqtt_broker '{broker}' is not 'host' or 'host:port'.")
    if ac.on_transport_fault:
        errors.append(
            f"AgentNode '{agent_id}': 'on_transport_fault' is not supported "
            "(there is no built-in TransportFaultReflex); remove it and handle "
            "transport loss with a node of your own."
        )
    return errors


def _parse_host_port(value: str, default_port: int) -> tuple[str, int] | None:
    """Split 'host' or 'host:port'; None when the port is not a number."""
    host, sep, port = value.rpartition(":")
    if not sep:
        return value, default_port
    if not host or not port.isdigit():
        return None
    return host, int(port)


def _import_node_class(kind: str) -> type[BaseNode]:
    """
    Import a node class by its fully-qualified name or short class name.

    Short names are looked up in the arachnite.nodes namespace first,
    then as a fully-qualified dotted path.
    """
    # Try arachnite.nodes first
    for module_path in [
        "arachnite.nodes.sense",
        "arachnite.nodes.instinct",
        "arachnite.nodes.decision",
        "arachnite.nodes.action",
    ]:
        try:
            mod = importlib.import_module(module_path)
            if hasattr(mod, kind):
                cls = getattr(mod, kind)
                if isinstance(cls, type) and issubclass(cls, BaseNode):
                    return cls
        except ImportError:
            pass

    # Try as a fully-qualified dotted import path
    if "." in kind:
        module_path, class_name = kind.rsplit(".", 1)
        try:
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            if isinstance(cls, type) and issubclass(cls, BaseNode):
                return cls
        except (ImportError, AttributeError) as exc:
            raise ManifestValidationError(
                [f"Cannot import node class '{kind}': {exc}"]
            ) from exc

    raise ManifestValidationError(
        [
            f"Node class '{kind}' not found. "
            "Use a fully-qualified dotted path (e.g. 'myapp.nodes.MySenseNode') "
            "or a built-in class name."
        ]
    )
