"""
Tests for the startup-only permission whitelist validation.

Covers:
- arachnite.distributed.permissions.validate_permissions()
- Permission field on BaseNode
- Manifest-level permission parsing and validation
- Runtime-level permission check in ArachniteRuntime.start()
"""

from __future__ import annotations

import pytest

from arachnite import ContextNode, Permission, SignalBus
from arachnite.distributed.manifest import DeploymentManifest
from arachnite.distributed.permissions import validate_permissions
from arachnite.exceptions import (
    ManifestValidationError,
    PermissionValidationError,
)
from arachnite.models import Proposal, Result, Signal
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.base import BaseNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime
from tests.conftest import make_signal

# ── Test node classes ────────────────────────────────────────────────────────

class _PlainSense(BaseSenseNode):
    node_id = "PlainSense"
    signal_kind = "test"

    async def read(self) -> Signal:
        return make_signal(kind=self.signal_kind)


class _NetworkSense(BaseSenseNode):
    node_id = "NetworkSense"
    signal_kind = "remote"
    permissions = frozenset({Permission.NETWORK})

    async def read(self) -> Signal:
        return make_signal(kind=self.signal_kind)


class _GpuAction(BaseActionNode):
    node_id = "GpuAction"
    permissions = frozenset({Permission.GPU, Permission.NETWORK})

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class _NopInstinct(BaseInstinctNode):
    node_id = "NopInstinct"
    priority = 50

    async def evaluate(self, ctx) -> Proposal | None:  # type: ignore[override]
        return None


class _NopAction(BaseActionNode):
    node_id = "NopAction"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


# ── validate_permissions() unit tests ────────────────────────────────────────

class TestValidatePermissions:

    def test_no_allowed_map_skips(self) -> None:
        """None map = skip entirely, even if nodes declare permissions."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_NetworkSense(bus=bus)]
        validate_permissions(nodes, None)  # should not raise

    def test_empty_allowed_map_skips(self) -> None:
        """Empty dict = skip entirely."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_NetworkSense(bus=bus)]
        validate_permissions(nodes, {})

    def test_node_not_in_map_skips(self) -> None:
        """A node not listed in the map is not validated."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_NetworkSense(bus=bus)]
        validate_permissions(nodes, {"OtherNode": set()})

    def test_declared_subset_of_allowed(self) -> None:
        """Node declares {NETWORK}, allowed is {NETWORK, GPU} — passes."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_NetworkSense(bus=bus)]
        validate_permissions(nodes, {"NetworkSense": {Permission.NETWORK, Permission.GPU}})

    def test_declared_equals_allowed(self) -> None:
        """Exact match passes."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_NetworkSense(bus=bus)]
        validate_permissions(nodes, {"NetworkSense": {Permission.NETWORK}})

    def test_declared_empty_always_passes(self) -> None:
        """Node with no permissions always passes, even against empty allowed."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_PlainSense(bus=bus)]
        validate_permissions(nodes, {"PlainSense": set()})

    def test_declared_exceeds_allowed_raises(self) -> None:
        """Node declares {GPU, NETWORK}, allowed is {NETWORK} — raises."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_GpuAction(bus=bus)]
        with pytest.raises(PermissionValidationError, match="gpu"):
            validate_permissions(nodes, {"GpuAction": {Permission.NETWORK}})

    def test_multiple_violations_single_error(self) -> None:
        """Multiple denied permissions are all listed."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_GpuAction(bus=bus)]
        with pytest.raises(PermissionValidationError) as exc_info:
            validate_permissions(nodes, {"GpuAction": set()})
        assert "gpu" in str(exc_info.value)
        assert "network" in str(exc_info.value)

    def test_multiple_nodes_multiple_violations(self) -> None:
        """Two nodes both violate — both appear in error."""
        bus = SignalBus()
        nodes: list[BaseNode] = [_NetworkSense(bus=bus), _GpuAction(bus=bus)]
        with pytest.raises(PermissionValidationError) as exc_info:
            validate_permissions(nodes, {
                "NetworkSense": set(),
                "GpuAction": set(),
            })
        assert "NetworkSense" in str(exc_info.value)
        assert "GpuAction" in str(exc_info.value)


# ── BaseNode.permissions default ─────────────────────────────────────────────

class TestPermissionDefault:

    def test_default_is_empty_set(self) -> None:
        """Nodes that don't declare permissions get an empty set."""
        bus = SignalBus()
        node = _PlainSense(bus=bus)
        assert node.permissions == set()

    def test_declared_permissions_available(self) -> None:
        """Nodes that declare permissions have them accessible."""
        bus = SignalBus()
        node = _NetworkSense(bus=bus)
        assert node.permissions == {Permission.NETWORK}


# ── Manifest-level permission tests ──────────────────────────────────────────

class TestManifestPermissions:

    def _make_manifest(self, perms: list[str] | None = None) -> DeploymentManifest:
        """Build a minimal manifest dict with optional permissions on a node."""
        node_def: dict = {"kind": "tests.unit.test_permissions._PlainSense"}
        if perms is not None:
            node_def["permissions"] = perms
        return DeploymentManifest.from_dict({
            "mesh": {"name": "test", "transport_default": "local"},
            "agents": [{
                "id": "agent-1",
                "nodes": {"sense": [node_def]},
            }],
        })

    def test_no_permissions_key_produces_none(self) -> None:
        m = self._make_manifest(perms=None)
        assert m.assignments[0].allowed_permissions is None

    def test_empty_permissions_produces_empty_set(self) -> None:
        m = self._make_manifest(perms=[])
        assert m.assignments[0].allowed_permissions == set()

    def test_valid_permission_parsed(self) -> None:
        m = self._make_manifest(perms=["network", "gpu"])
        assert m.assignments[0].allowed_permissions == {Permission.NETWORK, Permission.GPU}

    def test_invalid_permission_raises(self) -> None:
        with pytest.raises(ManifestValidationError, match="Unknown permission 'teleport'"):
            self._make_manifest(perms=["teleport"])

    def test_validate_catches_permission_violation(self) -> None:
        """Node declares {NETWORK} but manifest allows only []."""
        node_def = {
            "kind": "tests.unit.test_permissions._NetworkSense",
            "permissions": [],
        }
        m = DeploymentManifest.from_dict({
            "mesh": {"name": "test", "transport_default": "local"},
            "agents": [{
                "id": "agent-1",
                "nodes": {"sense": [node_def]},
            }],
        })
        with pytest.raises(ManifestValidationError, match="network"):
            m.validate()

    def test_validate_passes_when_allowed(self) -> None:
        """Node declares {NETWORK}, manifest allows [network] — passes."""
        node_def = {
            "kind": "tests.unit.test_permissions._NetworkSense",
            "permissions": ["network"],
        }
        m = DeploymentManifest.from_dict({
            "mesh": {"name": "test", "transport_default": "local"},
            "agents": [{
                "id": "agent-1",
                "nodes": {"sense": [node_def]},
            }],
        })
        m.validate()  # should not raise

    def test_validate_skips_when_no_permissions_configured(self) -> None:
        """No permissions keys anywhere — validate passes (backward compat)."""
        m = self._make_manifest(perms=None)
        m.validate()  # should not raise


# ── Runtime-level permission tests ───────────────────────────────────────────

class TestRuntimePermissions:

    @pytest.mark.asyncio
    async def test_start_no_allowed_permissions_backward_compat(self) -> None:
        """Runtime starts normally when allowed_permissions is not set."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        im = InstinctMasterNode(bus=bus)
        dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am = ActionMasterNode(bus=bus)
        sm.register(_NetworkSense(bus=bus))
        im.register(_NopInstinct(bus=bus))
        am.register(_NopAction(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=100.0,
        )
        await rt.start()
        assert rt.is_running
        await rt.stop()

    @pytest.mark.asyncio
    async def test_start_raises_on_unauthorized_permission(self) -> None:
        """Runtime raises before setup if node declares unauthorized permission."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        im = InstinctMasterNode(bus=bus)
        dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am = ActionMasterNode(bus=bus)
        sm.register(_NetworkSense(bus=bus))
        im.register(_NopInstinct(bus=bus))
        am.register(_NopAction(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=100.0,
            allowed_permissions={"NetworkSense": set()},
        )
        with pytest.raises(PermissionValidationError, match="network"):
            await rt.start()
        assert not rt.is_running

    @pytest.mark.asyncio
    async def test_start_passes_when_permissions_match(self) -> None:
        """Runtime starts when declared permissions are within allowed set."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        im = InstinctMasterNode(bus=bus)
        dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am = ActionMasterNode(bus=bus)
        sm.register(_NetworkSense(bus=bus))
        im.register(_NopInstinct(bus=bus))
        am.register(_NopAction(bus=bus))

        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus, tick_rate_hz=100.0,
            allowed_permissions={"NetworkSense": {Permission.NETWORK}},
        )
        await rt.start()
        assert rt.is_running
        await rt.stop()
