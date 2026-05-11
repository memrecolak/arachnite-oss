"""Unit tests for NodeConfig."""

from __future__ import annotations

import pytest

from arachnite.config import NodeConfig
from arachnite.exceptions import NodeConfigError


class TestNodeConfig:
    def test_get_required_key(self) -> None:
        cfg = NodeConfig({"port": 1883}, node_id="test")
        assert cfg.get("port") == 1883

    def test_get_missing_required_raises(self) -> None:
        cfg = NodeConfig({}, node_id="test")
        with pytest.raises(NodeConfigError, match="required key is missing"):
            cfg.get("port")

    def test_get_with_default(self) -> None:
        cfg = NodeConfig({}, node_id="test")
        assert cfg.get("port", 1883) == 1883

    def test_get_int(self) -> None:
        cfg = NodeConfig({"port": 1883}, node_id="test")
        assert cfg.get_int("port") == 1883

    def test_get_float(self) -> None:
        cfg = NodeConfig({"rate": 0.5}, node_id="test")
        assert cfg.get_float("rate") == 0.5

    def test_get_float_from_int(self) -> None:
        cfg = NodeConfig({"rate": 1}, node_id="test")
        assert cfg.get_float("rate") == 1.0

    def test_get_bool(self) -> None:
        cfg = NodeConfig({"enabled": True}, node_id="test")
        assert cfg.get_bool("enabled") is True

    def test_get_list(self) -> None:
        cfg = NodeConfig({"items": [1, 2, 3]}, node_id="test")
        assert cfg.get_list("items") == [1, 2, 3]

    def test_section(self) -> None:
        cfg = NodeConfig({"hw": {"pin": 4, "bus": 1}}, node_id="test")
        sub = cfg.section("hw")
        assert sub.get_int("pin") == 4

    def test_env_var_interpolation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_API_KEY", "secret123")
        cfg = NodeConfig({"key": "${TEST_API_KEY}"}, node_id="test")
        assert cfg.get_str("key") == "secret123"

    def test_env_var_default(self) -> None:
        cfg = NodeConfig({"key": "${UNSET_VAR_XYZ:-fallback}"}, node_id="test")
        assert cfg.get_str("key") == "fallback"

    def test_env_var_missing_raises(self) -> None:
        cfg = NodeConfig({"key": "${UNSET_VAR_XYZ_NO_DEFAULT}"}, node_id="test")
        with pytest.raises(EnvironmentError):
            cfg.get_str("key")

    def test_contains(self) -> None:
        cfg = NodeConfig({"x": 1}, node_id="test")
        assert "x" in cfg
        assert "y" not in cfg

    def test_empty(self) -> None:
        cfg = NodeConfig.empty("test")
        with pytest.raises(NodeConfigError):
            cfg.get("anything")

    def test_wrong_type_raises(self) -> None:
        cfg = NodeConfig({"port": "not_an_int"}, node_id="test")
        with pytest.raises(NodeConfigError, match="expected int"):
            cfg.get_int("port")

    def test_get_str_wrong_type_raises(self) -> None:
        cfg = NodeConfig({"name": 42}, node_id="test")
        with pytest.raises(NodeConfigError, match="expected str"):
            cfg.get_str("name")

    def test_get_int_bool_raises(self) -> None:
        cfg = NodeConfig({"flag": True}, node_id="test")
        with pytest.raises(NodeConfigError, match="expected int, got bool"):
            cfg.get_int("flag")

    def test_get_float_bool_raises(self) -> None:
        cfg = NodeConfig({"flag": True}, node_id="test")
        with pytest.raises(NodeConfigError, match="expected float, got bool"):
            cfg.get_float("flag")

    def test_get_float_wrong_type_raises(self) -> None:
        cfg = NodeConfig({"rate": "fast"}, node_id="test")
        with pytest.raises(NodeConfigError, match="expected float"):
            cfg.get_float("rate")

    def test_get_bool_wrong_type_raises(self) -> None:
        cfg = NodeConfig({"enabled": "yes"}, node_id="test")
        with pytest.raises(NodeConfigError, match="expected bool"):
            cfg.get_bool("enabled")

    def test_get_list_wrong_type_raises(self) -> None:
        cfg = NodeConfig({"items": "a,b,c"}, node_id="test")
        with pytest.raises(NodeConfigError, match="expected list"):
            cfg.get_list("items")

    def test_section_wrong_type_raises(self) -> None:
        cfg = NodeConfig({"hw": "not_a_dict"}, node_id="test")
        with pytest.raises(NodeConfigError, match="expected dict"):
            cfg.section("hw")

    def test_repr(self) -> None:
        cfg = NodeConfig({"port": 1883, "host": "localhost"}, node_id="mynode")
        r   = repr(cfg)
        assert "mynode" in r
        assert "NodeConfig" in r
