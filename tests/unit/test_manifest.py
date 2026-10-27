"""Unit tests for DeploymentManifest.validate() and co-location validation."""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any

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
        # A missing (None) target is reported as missing — not as an
        # ActionNode literally named 'None'.
        assert "CriticalReflex" in str(exc_info.value)
        assert "has no reflex_action_id set" in str(exc_info.value)
        assert "'None'" not in str(exc_info.value)

    def test_robot_arm_example_manifest_validates(self) -> None:
        from pathlib import Path

        path = Path(__file__).resolve().parents[2] / "examples" / "robot_arm" / "manifest.yaml"
        DeploymentManifest.from_yaml(str(path)).validate()  # must not raise

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
        # as an action on any agent → co-location violation, now caught at load
        with pytest.raises(ManifestValidationError):
            DeploymentManifest.from_dict(bad_manifest)

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
                        {"kind": "tests.conftest.EmergencyReflex", "action_id": "RecordingAction"},
                    ],
                    # co-located target, so the manifest passes load-time validation
                    "action": [{"kind": "tests.conftest.RecordingAction"}],
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
        with pytest.raises(ManifestValidationError, match="duplicate"):
            DeploymentManifest.from_dict(manifest_data)


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


class TestWireKinds:
    def test_absent_wire_kinds_is_none(self) -> None:
        """No wire_kinds key => None (forward every kind, back-compat default)."""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assert manifest.agent_config("a").wire_kinds is None

    def test_list_wire_kinds_parsed_as_set(self) -> None:
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
                "wire_kinds": ["telemetry", "alert"],
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assert manifest.agent_config("a").wire_kinds == {"telemetry", "alert"}

    def test_empty_list_wire_kinds_is_empty_set(self) -> None:
        """An explicit empty list means "forward nothing", distinct from absent."""
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
                "wire_kinds": [],
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        assert manifest.agent_config("a").wire_kinds == set()

    def test_non_list_wire_kinds_raises(self) -> None:
        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0, "nodes": {},
                "wire_kinds": "telemetry",   # string, not a list
            }],
        }
        with pytest.raises(ManifestValidationError, match="wire_kinds"):
            DeploymentManifest.from_dict(manifest_data)

    def test_wire_kinds_threaded_into_bus(self) -> None:
        """AgentNode.from_manifest passes wire_kinds through to the SignalBus."""
        from arachnite.distributed.agent_node import AgentNode

        manifest_data = {
            "mesh": {},
            "agents": [{
                "id": "a", "transport": "local", "tick_rate_hz": 10.0,
                "nodes": {"action": [{"kind": "tests.conftest.RecordingAction"}]},
                "wire_kinds": ["alert"],
            }],
        }
        manifest = DeploymentManifest.from_dict(manifest_data)
        node = AgentNode.from_manifest(manifest, "a")
        assert node.runtime.bus.wire_kinds() == {"alert"}


# ── Mutation-evidence hardening (revision E7) ─────────────────────────────────

class TestValidateColocationErrorAccumulation:
    def test_errors_accumulate_across_both_early_continue_branches(self) -> None:
        # Three faulty reflexes: one missing its reflex_action_id, two whose
        # targets do not exist. Validation must report ALL of them, not stop
        # at the first (the `continue` statements must not behave like breaks).
        assignments = [
            _assignment("NoIdReflex", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id=""),
            _assignment("GhostReflexA", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id="GhostActionA"),
            _assignment("GhostReflexB", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id="GhostActionB"),
        ]
        with pytest.raises(ManifestValidationError) as exc_info:
            validate_colocation(assignments)
        msg = str(exc_info.value)
        assert "NoIdReflex" in msg
        assert "GhostActionA" in msg
        assert "GhostActionB" in msg

    def test_non_action_section_does_not_satisfy_reflex_target(self) -> None:
        # A non-action node carrying the target node_id must NOT count as the
        # reflex's target action, even on the same agent.
        assignments = [
            _assignment("EmergencyStop", "edge-01", "instinct"),
            _assignment("CriticalReflex", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id="EmergencyStop"),
        ]
        with pytest.raises(ManifestValidationError) as exc_info:
            validate_colocation(assignments)
        assert "no ActionNode" in str(exc_info.value)


# ── Mutation-evidence hardening (v0.13) ───────────────────────────────────────

class TestValidateColocationActionSection:
    @pytest.mark.parametrize("section", [None, "", "Action", "ability"])
    def test_target_outside_the_action_section_is_not_an_action(
        self, section: str | None,
    ) -> None:
        """Only an assignment in the 'action' section counts as a reflex target"""
        target = _assignment("EmergencyStop", "edge-01")
        if section is None:
            del target["node_section"]
        else:
            target["node_section"] = section
        assignments = [
            target,
            _assignment("CriticalReflex", "edge-01", "instinct",
                        is_reflex=True, reflex_action_id="EmergencyStop"),
        ]
        with pytest.raises(ManifestValidationError, match="no ActionNode"):
            validate_colocation(assignments)


# ── Review 6 (distributed): node_id resolution, load-time enforcement ────────

_R6 = "tests.conftest."


def _r6_colocated() -> dict[str, object]:
    """Reflex 'r6.hot_reflex' and its target node_id 'Brake' (class R6BrakeActuator)."""
    return {"agents": [{"id": "edge", "nodes": {
        "sense": [f"{_R6}R6TempSense"],
        "instinct": [{"kind": f"{_R6}R6HotReflex", "action_id": "Brake"}],
        "action": [f"{_R6}R6BrakeActuator"],
    }}]}


def _r6_split_name_clash() -> dict[str, object]:
    """Real 'Brake' action on cloud; edge only has a class NAMED Brake (node_id BrakeLog)."""
    return {"agents": [
        {"id": "edge", "nodes": {
            "sense": [f"{_R6}R6TempSense"],
            "instinct": [{"kind": f"{_R6}R6HotReflex", "action_id": "Brake"}],
            "action": [f"{_R6}Brake"],
        }},
        {"id": "cloud", "nodes": {"action": [f"{_R6}R6BrakeActuator"]}},
    ]}


def _r6_split() -> dict[str, object]:
    return {"agents": [
        {"id": "edge", "nodes": {
            "sense": [f"{_R6}R6TempSense"],
            "instinct": [{"kind": f"{_R6}R6HotReflex", "action_id": "Brake"}],
        }},
        {"id": "cloud", "nodes": {"action": [f"{_R6}R6BrakeActuator"]}},
    ]}


class TestR6ManifestNodeIdAndLoadValidation:
    """R6-F2: co-location uses node_ids, and validation is enforced at load/start."""

    def test_assignment_node_id_is_the_class_node_id(self) -> None:
        """R6-F2: NodeAssignment.node_id is the class's node_id, not its class name"""
        manifest = DeploymentManifest.from_dict(_r6_colocated())
        ids = {a.node_class.__name__: a.node_id for a in manifest.assignments_for("edge")}
        assert ids == {"R6TempSense": "r6.temp", "R6HotReflex": "r6.hot_reflex",
                       "R6BrakeActuator": "Brake"}

    def test_colocated_reflex_whose_action_class_name_differs_passes(self) -> None:
        """R6-F2: a correct manifest is not rejected because node_id != class name"""
        DeploymentManifest.from_dict(_r6_colocated()).validate()   # must not raise

    def test_split_reflex_hidden_by_class_name_clash_is_rejected(self) -> None:
        """R6-F2: an unrelated class named like the target does not mask a split reflex"""
        with pytest.raises(ManifestValidationError, match="r6.hot_reflex"):
            DeploymentManifest.from_dict(_r6_split_name_clash()).validate()

    def test_split_reflex_rejected_at_load(self) -> None:
        """R6-F2: from_dict() itself rejects a reflex split from its action"""
        with pytest.raises(ManifestValidationError, match="Brake"):
            DeploymentManifest.from_dict(_r6_split())

    def test_split_reflex_rejected_by_from_yaml(self, tmp_path: Path) -> None:
        """R6-F2: from_yaml() validates too"""
        yaml = pytest.importorskip("yaml")
        path = tmp_path / "m.yaml"
        path.write_text(yaml.safe_dump(_r6_split()), encoding="utf-8")
        with pytest.raises(ManifestValidationError):
            DeploymentManifest.from_yaml(path)

    def test_permission_violation_rejected_at_load(self) -> None:
        """R6-F2: the permission whitelist is enforced at load, not only by validate()"""
        with pytest.raises(ManifestValidationError, match="network"):
            DeploymentManifest.from_dict({"agents": [{"id": "edge", "nodes": {
                "sense": [{"kind": f"{_R6}R6NetSense", "permissions": []}]}}]})

    def test_duplicate_node_id_from_two_classes_rejected(self) -> None:
        """R6-F2: two classes sharing a node_id on one agent are duplicates"""
        with pytest.raises(ManifestValidationError, match="duplicate node_id 'Shared'"):
            DeploymentManifest.from_dict({"agents": [{"id": "a", "nodes": {
                "action": [f"{_R6}R6SharedIdA", f"{_R6}R6SharedIdB"]}}]})

    def test_from_manifest_revalidates(self) -> None:
        """R6-F2: AgentNode.from_manifest re-validates a manifest built directly"""
        from arachnite.distributed.agent_node import AgentNode
        ok = DeploymentManifest.from_dict(_r6_colocated())
        # Same nodes, but the action moved to another agent: bypasses the load check.
        moved = [
            _manifest_module.NodeAssignment(
                node_class=a.node_class,
                agent_node_id="cloud" if a.node_id == "Brake" else a.agent_node_id,
                node_section=a.node_section, config=a.config, is_reflex=a.is_reflex,
                reflex_action_id=a.reflex_action_id,
            )
            for a in ok.assignments
        ]
        configs = [ok.agent_config("edge"),
                   _manifest_module.AgentNodeConfig("cloud", "", "local", 10.0, [], {})]
        bad = DeploymentManifest(ok.mesh_config, configs, moved)
        with pytest.raises(ManifestValidationError):
            AgentNode.from_manifest(bad, "edge")

    async def test_mesh_start_revalidates(self) -> None:
        """R6-F2: MeshRuntime.start re-validates and starts nothing on failure"""
        from arachnite.distributed.mesh import MeshRuntime
        ok = DeploymentManifest.from_dict(_r6_colocated())
        no_action = [a for a in ok.assignments if a.node_id != "Brake"]
        bad = DeploymentManifest(ok.mesh_config, [ok.agent_config("edge")], no_action)
        mesh = MeshRuntime(bad)
        with pytest.raises(ManifestValidationError):
            await mesh.start()
        running = [a for a in mesh.agents.values() if a.is_running]
        await mesh.stop()
        assert not mesh.started
        assert running == []

    def test_unknown_section_and_wrong_section_class_rejected(self) -> None:
        """R6-F12-10: a section typo or an action class under 'instinct' is an error"""
        with pytest.raises(ManifestValidationError, match="unknown node section 'sensors'"):
            DeploymentManifest.from_dict({"agents": [{"id": "e", "nodes": {
                "sensors": [f"{_R6}R6TempSense"]}}]})
        with pytest.raises(ManifestValidationError, match="not a BaseInstinctNode"):
            DeploymentManifest.from_dict({"agents": [{"id": "e", "nodes": {
                "instinct": [f"{_R6}R6BrakeActuator"]}}]})


class TestR6ManifestTransportSettings:
    """R6-F12-19/20: manifest transport settings are honoured or rejected, never ignored."""

    def test_mesh_broker_and_mqtt_credentials_reach_the_transport(self) -> None:
        """R6-F12-19: mesh mqtt_broker and transport_config auth/TLS/reconnect are applied"""
        pytest.importorskip("aiomqtt")
        from arachnite.distributed.agent_node import AgentNode
        m = DeploymentManifest.from_dict({
            "mesh": {"mqtt_broker": "192.168.1.10:1884",
                     "nats_server": "nats://cloud.example.com:4222"},
            "agents": [
                {"id": "pi", "transport": "mqtt",
                 "transport_config": {"username": "robot", "password": "${R6_MQTT_PW:-pw}",
                                      "tls": True, "max_reconnect_attempts": 99,
                                      "reconnect_interval_s": 0.5},
                 "nodes": {"sense": [f"{_R6}R6TempSense"]}},
                {"id": "brain", "transport": "nats", "nodes": {}},
                {"id": "pi2", "transport": "mqtt",
                 "transport_config": {"broker_host": "10.0.0.2"}, "nodes": {}},
            ]})
        mq: Any = AgentNode.from_manifest(m, "pi").transport
        assert (mq._host, mq._port, mq._username, mq._password, mq._tls) == (
            "192.168.1.10", 1884, "robot", "pw", True)
        assert (mq._max_reconnect, mq._reconnect_interval) == (99, 0.5)
        # an agent's own broker_host wins over the mesh default (port still from it)
        mq2: Any = AgentNode.from_manifest(m, "pi2").transport
        assert (mq2._host, mq2._port) == ("10.0.0.2", 1884)
        pytest.importorskip("nats")
        na: Any = AgentNode.from_manifest(m, "brain").transport
        assert na._servers == ["nats://cloud.example.com:4222"]

    def test_unknown_transport_config_key_rejected(self) -> None:
        """R6-F12-19: a transport_config key the transport cannot use is an error"""
        with pytest.raises(ManifestValidationError, match="ca_file"):
            DeploymentManifest.from_dict({"agents": [{
                "id": "pi", "transport": "mqtt",
                "transport_config": {"ca_file": "/etc/ca.pem"}, "nodes": {}}]})

    def test_unknown_transport_kind_rejected(self) -> None:
        """R6-F12-20: validate() checks the transport kind (spec §11.2)"""
        with pytest.raises(ManifestValidationError, match="unknown transport 'zeromq'"):
            DeploymentManifest.from_dict({"agents": [{
                "id": "pi", "transport": "zeromq", "nodes": {}}]})

    def test_on_transport_fault_rejected_not_ignored(self) -> None:
        """R6-F12-20: the unimplemented on_transport_fault option is refused"""
        with pytest.raises(ManifestValidationError, match="on_transport_fault"):
            DeploymentManifest.from_dict({"agents": [{
                "id": "pi", "transport": "local", "nodes": {},
                "on_transport_fault": {"reflex": "TransportFaultReflex",
                                       "action": "SafeModeActionNode"}}]})


class TestR7ReflexByAttribute:
    """R7: the manifest treats a node as a reflex by the same rule the runtime routes by."""

    def test_attribute_reflex_split_from_its_action_is_rejected(self) -> None:
        """R7-C3: a reflex=True class outside the reflex base is co-location checked"""
        with pytest.raises(ManifestValidationError, match="Brake"):
            DeploymentManifest.from_dict({"agents": [
                {"id": "edge", "nodes": {
                    "instinct": [{"kind": f"{_R6}R7AttrReflex", "action_id": "Brake"}],
                }},
                {"id": "cloud", "nodes": {"action": [f"{_R6}R6BrakeActuator"]}},
            ]})

    def test_unknown_node_key_is_rejected(self) -> None:
        """R7-C4: a node key the loader does not read (priority) is an error, not a no-op"""
        with pytest.raises(ManifestValidationError, match="priority"):
            DeploymentManifest.from_dict({"agents": [{"id": "edge", "nodes": {
                "instinct": [{"kind": f"{_R6}R6HotReflex", "action_id": "Brake",
                              "priority": 200}],
                "action": [f"{_R6}R6BrakeActuator"],
            }}]})

