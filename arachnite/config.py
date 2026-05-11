"""
arachnite.config
~~~~~~~~~~~~~~~~
NodeConfig: typed, manifest-injected configuration for every node.
Spec reference: Section 12.
"""

from __future__ import annotations

import os
import re
from typing import Any, overload

from arachnite.exceptions import NodeConfigError
from arachnite.models import REQUIRED, _Required

# Pattern for env-var interpolation: ${VAR_NAME} or ${VAR_NAME:-default}
_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _resolve_env(value: str) -> str:
    """Replace ${VAR} and ${VAR:-default} references with environment values."""
    def replacer(match: re.Match[str]) -> str:
        var, default = match.group(1), match.group(2)
        env_val = os.environ.get(var)
        if env_val is not None:
            return env_val
        if default is not None:
            return default
        raise OSError(
            f"Environment variable '{var}' is not set and has no default. "
            f"Use ${{'{var}':-fallback}} to provide a fallback."
        )
    return _ENV_RE.sub(replacer, value)


def _resolve_value(value: Any) -> Any:
    """Recursively resolve env-var references in string values."""
    if isinstance(value, str):
        return _resolve_env(value)
    if isinstance(value, dict):
        return {k: _resolve_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_value(v) for v in value]
    return value


class NodeConfig:
    """
    Typed wrapper around a node's configuration dictionary.

    Provides typed accessors with defaults and raises NodeConfigError
    for missing required keys. Supports nested sections via .section().
    Env-var interpolation is applied automatically on string values.

    Spec reference: Section 12.1.
    """

    def __init__(self, data: dict[str, Any], node_id: str = "<unknown>") -> None:
        self._data    = data
        self._node_id = node_id

    # ── Generic getter ────────────────────────────────────────────────────────

    @overload
    def get(self, key: str) -> Any: ...
    @overload
    def get(self, key: str, default: Any) -> Any: ...

    def get(self, key: str, default: Any = REQUIRED) -> Any:
        """
        Retrieve a config value.

        If *default* is REQUIRED and the key is absent, raises NodeConfigError.
        String values have environment variable references resolved automatically.
        """
        if key not in self._data:
            if isinstance(default, _Required):
                raise NodeConfigError(
                    self._node_id, key,
                    "required key is missing from node config"
                )
            return default
        return _resolve_value(self._data[key])

    # ── Typed getters ─────────────────────────────────────────────────────────

    def get_str(self, key: str, default: str | _Required = REQUIRED) -> str:
        value = self.get(key, default)
        if not isinstance(value, str):
            raise NodeConfigError(
                self._node_id, key,
                f"expected str, got {type(value).__name__}"
            )
        return value

    def get_int(self, key: str, default: int | _Required = REQUIRED) -> int:
        value = self.get(key, default)
        if isinstance(value, bool):
            raise NodeConfigError(
                self._node_id, key, "expected int, got bool"
            )
        if not isinstance(value, int):
            raise NodeConfigError(
                self._node_id, key,
                f"expected int, got {type(value).__name__}"
            )
        return value

    def get_float(self, key: str, default: float | _Required = REQUIRED) -> float:
        value = self.get(key, default)
        if isinstance(value, bool):
            raise NodeConfigError(
                self._node_id, key, "expected float, got bool"
            )
        if not isinstance(value, (int, float)):
            raise NodeConfigError(
                self._node_id, key,
                f"expected float, got {type(value).__name__}"
            )
        return float(value)

    def get_bool(self, key: str, default: bool | _Required = REQUIRED) -> bool:
        value = self.get(key, default)
        if not isinstance(value, bool):
            raise NodeConfigError(
                self._node_id, key,
                f"expected bool, got {type(value).__name__}"
            )
        return value

    def get_list(self, key: str, default: list[Any] | _Required = REQUIRED) -> list[Any]:
        value = self.get(key, default)
        if not isinstance(value, list):
            raise NodeConfigError(
                self._node_id, key,
                f"expected list, got {type(value).__name__}"
            )
        return value

    def section(self, key: str) -> NodeConfig:
        """Return a nested NodeConfig for a sub-dict key."""
        value = self.get(key)
        if not isinstance(value, dict):
            raise NodeConfigError(
                self._node_id, key,
                f"expected dict for section, got {type(value).__name__}"
            )
        return NodeConfig(value, node_id=f"{self._node_id}.{key}")

    # ── Dunder helpers ────────────────────────────────────────────────────────

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __repr__(self) -> str:
        return f"NodeConfig(node_id={self._node_id!r}, keys={list(self._data)})"

    @classmethod
    def empty(cls, node_id: str = "<unknown>") -> NodeConfig:
        """Return an empty NodeConfig. All get() calls will use defaults or raise."""
        return cls({}, node_id=node_id)
