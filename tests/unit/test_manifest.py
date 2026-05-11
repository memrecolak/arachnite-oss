"""Unit tests for DeploymentManifest.validate() and co-location validation."""

from __future__ import annotations

import os
import warnings

import pytest

import arachnite.distributed.manifest as _manifest_module
from arachnite.distributed.colocation import validate_colocation
from arachnite.distributed.manifest import DeploymentManifest
from arachnite.exceptions import ManifestValidationError

# ── Helpers ───────────────────────────────────────────────────────────────────

def _assignment(
    node_id: str,
    agent_node_id: str,
    node_section: str = "action",
    is_reflex: bool = False,
    reflex_action_id: str | None = None,
) -> dict[str, object]:
    return {
        "node_id":         node_id,
        "agent_node_id":   agent_node_id,
        "node_section":    node_section,
        "is_reflex":       is_reflex,
        "reflex_action_id": reflex_action_id,
    }


# ── validate_colocation (low-level) ──────────────────────────────────────────

class TestValidateColocation:
    def test_valid_colocation_passes(self) -> None:
        assignments = [
            _assignment("EmergencyStop", "edge-01", "action"),
            _assignment("CriticalReflex", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id="EmergencyStop"),
        ]
        validate_colocation(assignments)  # must not raise

    def test_violation_raises(self) -> None:
        assignments = [
            _assignment("EmergencyStop", "edge-02", "action"),
            _assignment("CriticalReflex", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id="EmergencyStop"),
        ]
        with pytest.raises(ManifestValidationError) as exc_info:
            validate_colocation(assignments)
        assert "CriticalReflex" in str(exc_info.value)

    def test_reflex_without_action_id_raises(self) -> None:
        assignments = [
            _assignment("CriticalReflex", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id=None),
        ]
        with pytest.raises(ManifestValidationError) as exc_info:
            validate_colocation(assignments)
        # colocation validator converts None → 'None' string and reports it missing
        assert "CriticalReflex" in str(exc_info.value)

    def test_reflex_with_empty_string_action_id_raises(self) -> None:
        # Empty string (not None) hits the `if not action_id:` branch directly
        assignments = [
            _assignment("EmptyReflex", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id=""),
        ]
        with pytest.raises(ManifestValidationError) as exc_info:
            validate_colocation(assignments)
        assert "EmptyReflex" in str(exc_info.value)

    def test_reflex_target_not_registered_raises(self) -> None:
        assignments = [
            _assignment("CriticalReflex", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id="MissingAction"),
        ]
        with pytest.raises(ManifestValidationError) as exc_info:
            validate_colocation(assignments)
        assert "MissingAction" in str(exc_info.value)

    def test_normal_nodes_ignored(self) -> None:
        assignments = [
            _assignment("NormalInstinct", "edge-01", "instinct",
                        is_reflex=False),
            _assignment("SomeAction", "edge-02", "action"),
        ]
        validate_colocation(assignments)  # no reflex → no violation

    def test_multiple_violations_all_reported(self) -> None:
        assignments = [
            _assignment("ActionX", "agent-A", "action"),
            _assignment("ReflexX", "agent-B", "instinct",
                        is_reflex=True, reflex_action_id="ActionX"),
            _assignment("ReflexY", "agent-C", "instinct",
                        is_reflex=True, reflex_action_id="ActionX"),
        ]
        with pytest.raises(ManifestValidationError) as exc_info:
            validate_colocation(assignments)
        errors = exc_info.value.errors
        assert len(errors) == 2


# ── DeploymentManifest.from_dict + validate() ────────────────────────────────

# A minimal manifest dict — uses a fully-qualified class that actually exists
_VALID_MANIFEST = {
    "mesh": {"transport_default": "local"},
    "agents": [
        {
            "id":           "edge-01",
            "transport":    "local",
            "tick_rate_hz": 10.0,
            "nodes": {
                "sense": [
                    {"kind": "tests.conftest.ConstantSenseNode"},
                ],
                "action": [
                    {"kind": "tests.conftest.RecordingAction"},
                ],
            },
        }
    ],
}


class TestDeploymentManifestValidate:
    def test_valid_manifest_passes_validate(self) -> None:
        manifest = DeploymentManifest.from_dict(_VALID_MANIFEST)
        manifest.validate()   # must not raise

    def test_agent_ids_returns_all_agents(self) -> None:
        manifest = DeploymentManifest.from_dict(_VALID_MANIFEST)
        assert "edge-01" in manifest.agent_ids()

    def test_assignments_for_agent(self) -> None:
        manifest = DeploymentManifest.from_dict(_VALID_MANIFEST)
        assignments = manifest.assignments_for("edge-01")
        node_ids = [a.node_id for a in assignments]
        assert "ConstantSenseNode" in node_ids
        assert "RecordingAction"   in node_ids

    def test_colocation_violation_raises_on_validate(self) -> None:
        bad_manifest = {
            "mesh": {},
            "agents": [
                {
                    "id":           "agent-A",
                    "transport":    "local",
                    "tick_rate_hz": 10.0,
                    "nodes": {
                        "instinct": [
                            {
                                "kind":            "tests.conftest.EmergencyReflex",
                                "reflex":          True,
                                "action_id":       "EmergencyStop",
                            }
                        ],
                    },
                },
                {
                    "id":           "agent-B",
                    "transport":    "local",
                    "tick_rate_hz": 10.0,
                    "nodes": {
                        "action": [
                            {"kind": "tests.conftest.RecordingAction"},
                        ],
                    },
                },
            ],
        }
        # EmergencyReflex on agent-A targets "EmergencyStop" which is NOT registered
        # as an action on any agent → co-location violation
        manifest = DeploymentManifest.from_dict(bad_manifest)
        with pytest.raises(ManifestValidationError):
            manifest.validate()

    def test_unknown_agent_raises_key_error(self) -> None:
        manifest = DeploymentManifest.from_dict(_VALID_MANIFEST)
        with pytest.raises(KeyError):
            manifest.agent_config("nonexistent")


class TestSecretPatternWarning:
    def test_hardcoded_password_triggers_warning(self) -> None:
        manifest_with_secret = {
            "mesh": {},
            "agents": [
                {
                    "id":           "edge-01",
                    "transport":    "local",
                    "tick_rate_hz": 10.0,
                    "nodes": {},
                    "transport_config": {
                        "password": "supersecret123",
                    },
                }
            ],
        }
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            DeploymentManifest.from_dict(manifest_with_secret)
        assert any("secret" in str(w.message).lower() for w in caught)

    def test_hardcoded_api_key_triggers_warning(self) -> None:
        manifest_with_secret = {
            "mesh": {},
            "agents": [
                {
                    "id":           "cloud-01",
                    "transport":    "local",
                    "tick_rate_hz": 10.0,
                    "nodes": {},
                    "transport_config": {
                        "api_key": "sk-abc123xyz",
                    },
                }
            ],
        }
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            DeploymentManifest.from_dict(manifest_with_secret)
        assert any("secret" in str(w.message).lower() for w in caught)

    def test_env_var_reference_does_not_trigger_warning(self) -> None:
        import os
        os.environ["TEST_PASSWORD"] = "safe"
        manifest_safe = {
            "mesh": {},
            "agents": [
                {
                    "id":           "edge-01",
                    "transport":    "local",
                    "tick_rate_hz": 10.0,
                    "nodes": {},
                    "transport_config": {
                        "password": "${TEST_PASSWORD}",
                    },
                }
            ],
        }
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            DeploymentManifest.from_dict(manifest_safe)
        secret_warnings = [w for w in caught if "hardcoded secret" in str(w.message)]
        assert not secret_warnings


class TestSecretValuePattern:
    def test_value_matching_secret_pattern_warns(self) -> None:
        """A STRING VALUE that contains e.g. 'sk-...' triggers the value-level warn."""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
                "transport_config": {"auth_header": "bearer_token_abc123"},
            }],
        }
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            DeploymentManifest.from_dict(manifest_data)
        assert any("hardcoded secret" in str(w.message).lower() for w in caught)


class TestEnvVarInterpolation:
    def test_env_var_resolved_when_set(self) -> None:
        os.environ["ARACHNITE_TEST_HOST"] = "myhost"
        try:
            manifest_data = {
                "mesh": {},
                "agents": [{
                    "id": "a", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
                    "transport_config": {"host": "${ARACHNITE_TEST_HOST}"},
                }],
            }
            manifest = DeploymentManifest.from_dict(manifest_data)
            ac = manifest.agent_config("a")
            assert ac.transport_config["host"] == "myhost"
        finally:
            del os.environ["ARACHNITE_TEST_HOST"]

    def test_env_var_default_used_when_not_set(self) -> None:
        os.environ.pop("ARACHNITE_TEST_MISSING_9999", None)
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
                "transport_config": {"host": "${ARACHNITE_TEST_MISSING_9999:-fallback}"},
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        ac = manifest.agent_config("a")
        assert ac.transport_config["host"] == "fallback"

    def test_missing_env_var_no_default_raises(self) -> None:
        os.environ.pop("ARACHNITE_TEST_MISSING_9999", None)
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
                "transport_config": {"host": "${ARACHNITE_TEST_MISSING_9999}"},
            }],
        }
        with pytest.raises(ManifestValidationError, match="ARACHNITE_TEST_MISSING_9999"):
            DeploymentManifest.from_dict(manifest_data)


class TestFromYaml:
    def test_from_yaml_without_pyyaml_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_manifest_module, "_YAML_AVAILABLE", False)
        with pytest.raises(ImportError, match="pyyaml"):
            DeploymentManifest.from_yaml("any_path.yaml")


class TestNodeDefShorthand:
    def test_string_node_def_parsed_as_kind(self) -> None:
        """A bare string in the node list is treated as {kind: <string>}."""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {
                    "decision": ["GreedyDecisionNode"],
                },
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assignments = manifest.assignments_for("a")
        assert any(a.node_id == "GreedyDecisionNode" for a in assignments)

    def test_non_list_section_is_skipped(self) -> None:
        """A non-list node section (e.g. a dict) is skipped without error."""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {
                    "sense": {"not": "a list"},   # dict, not list → skipped
                    "action": [{"kind": "tests.conftest.RecordingAction"}],
                },
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assignments = manifest.assignments_for("a")
        # Only action node registered; sense section was skipped
        assert all(a.node_section == "action" for a in assignments)


class TestAutoDetectReflex:
    def test_reflex_class_detected_without_manifest_flag(self) -> None:
        """If node class inherits BaseReflexInstinctNode, is_reflex is set automatically."""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {
                    "instinct": [
                        # No "reflex": True — should be auto-detected
                        {"kind": "tests.conftest.EmergencyReflex", "action_id": "EmergencyStop"},
                    ],
                },
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assignments = manifest.assignments_for("a")
        reflex_assignments = [a for a in assignments if a.is_reflex]
        assert len(reflex_assignments) == 1
        assert reflex_assignments[0].node_id == "EmergencyReflex"


class TestDuplicateNodeId:
    def test_duplicate_node_id_in_same_agent_raises_on_validate(self) -> None:
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {
                    "action": [
                        {"kind": "tests.conftest.RecordingAction"},
                        {"kind": "tests.conftest.RecordingAction"},
                    ],
                },
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        with pytest.raises(ManifestValidationError, match="duplicate"):
            manifest.validate()


class TestManifestAccessors:
    def test_mesh_config_property(self) -> None:
        manifest_data = {
            "mesh": {"transport_default": "local", "log_level": "DEBUG"},
            "agents": [],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assert manifest.mesh_config["log_level"] == "DEBUG"

    def test_assignments_property(self) -> None:
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {"action": [{"kind": "tests.conftest.RecordingAction"}]},
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assert len(manifest.assignments) == 1

    def test_repr_contains_agent_ids(self) -> None:
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "edge-01", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assert "edge-01" in repr(manifest)


class TestImportNodeClass:
    def test_short_name_found_in_arachnite_modules(self) -> None:
        """Short names like 'GreedyDecisionNode' are looked up in arachnite.nodes.*"""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {"decision": [{"kind": "GreedyDecisionNode"}]},
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assignments = manifest.assignments_for("a")
        assert assignments[0].node_id == "GreedyDecisionNode"

    def test_fully_qualified_bad_attribute_raises(self) -> None:
        """A fully-qualified path where the class doesn't exist raises ManifestValidationError."""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {"action": [{"kind": "arachnite.nodes.sense.NonExistentClass"}]},
            }],
        }
        with pytest.raises(ManifestValidationError, match="Cannot import"):
            DeploymentManifest.from_dict(manifest_data)

    def test_short_name_not_found_raises(self) -> None:
        """A short name that doesn't exist raises ManifestValidationError."""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {"action": [{"kind": "TotallyMadeUpNode"}]},
            }],
        }
        with pytest.raises(ManifestValidationError, match="TotallyMadeUpNode"):
            DeploymentManifest.from_dict(manifest_data)
