"""
Unit tests for the three P2 framework gaps:

  Gap 1 — Background task lifecycle (spawn_background_task / cancel_background_tasks)
  Gap 2 — Node dependency declaration (requires + startup validation)
  Gap 3 — Artifact directory convention (artifact_dir property)
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from arachnite import ContextNode, SignalBus
from arachnite.exceptions import DependencyValidationError
from arachnite.models import Permission, Proposal, Result, Signal
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.base import BaseNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, InstinctMasterNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime

# ══════════════════════════════════════════════════════════════════════════════
# Gap 1 — Background task lifecycle
# ══════════════════════════════════════════════════════════════════════════════

class BackgroundSense(BaseSenseNode):
    """Starts a background task in setup() that buffers data."""
    node_id = "BackgroundSense"
    signal_kind = "bg_data"

    def __init__(self, bus: SignalBus, **kw: object) -> None:
        super().__init__(bus, **kw)  # type: ignore[arg-type]
        self.buffer: asyncio.Queue[float] = asyncio.Queue()
        self.bg_running = False

    async def _listener(self) -> None:
        self.bg_running = True
        try:
            while True:
                await asyncio.sleep(0.01)
                await self.buffer.put(time.monotonic())
        except asyncio.CancelledError:
            self.bg_running = False

    async def setup(self) -> None:
        self.spawn_background_task(self._listener(), name="bg_listener")

    async def read(self) -> Signal | None:
        if not self.buffer.empty():
            val = self.buffer.get_nowait()
            return Signal(
                source=self.node_id, kind=self.signal_kind,
                value=val, confidence=1.0, timestamp=time.monotonic(),
            )
        return None


class TestBackgroundTaskLifecycle:
    @pytest.mark.asyncio
    async def test_spawn_creates_tracked_task(self) -> None:
        bus = SignalBus()
        node = BackgroundSense(bus=bus)
        await node.setup()
        assert len(node._background_tasks) == 1
        await node.cancel_background_tasks()

    @pytest.mark.asyncio
    async def test_cancel_stops_all_tasks(self) -> None:
        bus = SignalBus()
        node = BackgroundSense(bus=bus)
        await node.setup()
        await asyncio.sleep(0.05)  # let listener run
        assert node.bg_running is True
        await node.cancel_background_tasks()
        assert node.bg_running is False
        assert len(node._background_tasks) == 0

    @pytest.mark.asyncio
    async def test_done_task_removed_automatically(self) -> None:
        """Tasks that finish naturally are removed from the tracking set."""
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus)

        async def quick_job() -> None:
            pass

        task = node.spawn_background_task(quick_job())
        await task
        # Allow done_callback to fire
        await asyncio.sleep(0)
        assert task not in node._background_tasks

    @pytest.mark.asyncio
    async def test_cancel_on_empty_is_noop(self) -> None:
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus)
        await node.cancel_background_tasks()  # must not raise

    @pytest.mark.asyncio
    async def test_master_teardown_cancels_background_tasks(self) -> None:
        """SenseMasterNode.teardown() cancels child background tasks."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        node = BackgroundSense(bus=bus)
        sm.register(node)
        await sm.setup()
        await asyncio.sleep(0.05)
        assert node.bg_running is True
        await sm.teardown()
        assert node.bg_running is False

    @pytest.mark.asyncio
    async def test_instinct_master_teardown_cancels_background_tasks(self) -> None:
        """InstinctMasterNode.teardown() cancels child background tasks."""
        bus = SignalBus()
        im = InstinctMasterNode(bus=bus)
        node = BackgroundInstinct(bus=bus)
        im.register(node)
        await im.setup()
        await asyncio.sleep(0.05)
        assert node.bg_running is True
        await im.teardown()
        assert node.bg_running is False

    @pytest.mark.asyncio
    async def test_action_master_teardown_cancels_background_tasks(self) -> None:
        """ActionMasterNode.teardown() cancels child background tasks."""
        bus = SignalBus()
        am = ActionMasterNode(bus=bus)
        node = BackgroundAction(bus=bus)
        am.register(node)
        await am.setup()
        await asyncio.sleep(0.05)
        assert node.bg_running is True
        await am.teardown()
        assert node.bg_running is False

    @pytest.mark.asyncio
    async def test_multiple_background_tasks(self) -> None:
        """Multiple tasks can be spawned and all get cancelled."""
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus)
        started = asyncio.Event()
        cancelled: list[str] = []

        async def worker(name: str) -> None:
            try:
                started.set()
                await asyncio.sleep(999)
            except asyncio.CancelledError:
                cancelled.append(name)
                raise

        node.spawn_background_task(worker("a"))
        node.spawn_background_task(worker("b"))
        node.spawn_background_task(worker("c"))
        await asyncio.sleep(0.01)  # let workers start
        assert len(node._background_tasks) == 3
        await node.cancel_background_tasks()
        assert len(cancelled) == 3
        assert set(cancelled) == {"a", "b", "c"}


# ── Helper nodes for background task tests ─────────────────────────────────

class BaseSenseNodeStub(BaseSenseNode):
    node_id = "Stub"
    signal_kind = "stub"

    async def read(self) -> Signal | None:
        return None


class BackgroundInstinct(BaseInstinctNode):
    node_id = "BackgroundInstinct"
    priority = 50

    def __init__(self, bus: SignalBus, **kw: object) -> None:
        super().__init__(bus, **kw)  # type: ignore[arg-type]
        self.bg_running = False

    async def _bg_loop(self) -> None:
        self.bg_running = True
        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            self.bg_running = False

    async def setup(self) -> None:
        self.spawn_background_task(self._bg_loop())

    async def evaluate(self, ctx: object) -> Proposal | None:
        return None


class BackgroundAction(BaseActionNode):
    node_id = "BackgroundAction"

    def __init__(self, bus: SignalBus, **kw: object) -> None:
        super().__init__(bus, **kw)  # type: ignore[arg-type]
        self.bg_running = False

    async def _bg_loop(self) -> None:
        self.bg_running = True
        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            self.bg_running = False

    async def setup(self) -> None:
        self.spawn_background_task(self._bg_loop())

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


# ══════════════════════════════════════════════════════════════════════════════
# Gap 2 — Node dependency declaration
# ══════════════════════════════════════════════════════════════════════════════

class DependentInstinct(BaseInstinctNode):
    node_id = "DependentInstinct"
    priority = 50
    requires = ("TempSense",)

    async def evaluate(self, ctx: object) -> Proposal | None:
        return None


class TempSense(BaseSenseNode):
    node_id = "TempSense"
    signal_kind = "temperature"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=25.0, confidence=1.0, timestamp=time.monotonic(),
        )


class NoDepsAction(BaseActionNode):
    node_id = "NoDepsAction"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(action_id=self.node_id, success=True)


class TestDependencyValidation:
    @pytest.mark.asyncio
    async def test_satisfied_dependency_starts_ok(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        im = InstinctMasterNode(bus=bus)
        dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am = ActionMasterNode(bus=bus)
        sm.register(TempSense(bus=bus))
        im.register(DependentInstinct(bus=bus))
        am.register(NoDepsAction(bus=bus))
        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus,
        )
        await rt.start()
        assert rt.is_running
        await rt.stop()

    @pytest.mark.asyncio
    async def test_missing_dependency_raises(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        im = InstinctMasterNode(bus=bus)
        dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am = ActionMasterNode(bus=bus)
        # Do NOT register TempSense
        im.register(DependentInstinct(bus=bus))
        am.register(NoDepsAction(bus=bus))
        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus,
        )
        with pytest.raises(DependencyValidationError, match="TempSense"):
            await rt.start()

    @pytest.mark.asyncio
    async def test_multiple_missing_dependencies(self) -> None:
        class MultiDep(BaseInstinctNode):
            node_id = "MultiDep"
            priority = 50
            requires = ("SensorA", "SensorB")

            async def evaluate(self, ctx: object) -> Proposal | None:
                return None

        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        im = InstinctMasterNode(bus=bus)
        dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am = ActionMasterNode(bus=bus)
        im.register(MultiDep(bus=bus))
        am.register(NoDepsAction(bus=bus))
        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus,
        )
        with pytest.raises(DependencyValidationError) as exc_info:
            await rt.start()
        assert len(exc_info.value.errors) == 2

    def test_requires_defaults_empty(self) -> None:
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus)
        assert node.requires == []

    def test_requires_class_attribute(self) -> None:
        assert DependentInstinct.requires == ("TempSense",)

    @pytest.mark.asyncio
    async def test_no_requires_no_validation(self) -> None:
        """Nodes with no requires skip validation silently."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        im = InstinctMasterNode(bus=bus)
        dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
        am = ActionMasterNode(bus=bus)
        am.register(NoDepsAction(bus=bus))
        rt = ArachniteRuntime(
            sense_master=sm, context=ContextNode(),
            instinct_master=im, decision_master=dm,
            action_master=am, bus=bus,
        )
        await rt.start()
        assert rt.is_running
        await rt.stop()


# ══════════════════════════════════════════════════════════════════════════════
# Gap 3 — Artifact directory convention
# ══════════════════════════════════════════════════════════════════════════════

class TestArtifactDirectory:
    def test_default_artifact_dir(self, tmp_path: Path) -> None:
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus, artifact_root=tmp_path / "artifacts")
        path = node.artifact_dir
        assert path == tmp_path / "artifacts" / "local" / "Stub"
        assert path.is_dir()

    def test_custom_agent_node_id(self, tmp_path: Path) -> None:
        bus = SignalBus()
        node = BaseSenseNodeStub(
            bus=bus, agent_node_id="jetson01",
            artifact_root=tmp_path / "out",
        )
        path = node.artifact_dir
        assert path == tmp_path / "out" / "jetson01" / "Stub"
        assert path.is_dir()

    def test_directory_created_lazily(self, tmp_path: Path) -> None:
        bus = SignalBus()
        root = tmp_path / "lazy_test"
        node = BaseSenseNodeStub(bus=bus, artifact_root=root)
        # Directory should not exist yet
        assert not (root / "local" / "Stub").exists()
        # Access triggers creation
        _ = node.artifact_dir
        assert (root / "local" / "Stub").is_dir()

    def test_repeated_access_is_idempotent(self, tmp_path: Path) -> None:
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus, artifact_root=tmp_path)
        path1 = node.artifact_dir
        path2 = node.artifact_dir
        assert path1 == path2

    def test_default_root_is_artifacts(self) -> None:
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus)
        # Default root is "artifacts" (relative)
        assert node._artifact_root == Path("artifacts")

    def test_write_file_to_artifact_dir(self, tmp_path: Path) -> None:
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus, artifact_root=tmp_path)
        out = node.artifact_dir / "data.txt"
        out.write_text("hello")
        assert out.read_text() == "hello"


# ══════════════════════════════════════════════════════════════════════════════
# A-12 — Mutable class-level permissions / requires
# ══════════════════════════════════════════════════════════════════════════════

class TestImmutableClassDefaults:
    """Verify that class-level permissions/requires are immutable (A-12 fix)."""

    def test_class_level_permissions_is_frozenset(self) -> None:
        """BaseNode.permissions class default must be a frozenset"""
        assert isinstance(BaseNode.permissions, frozenset)

    def test_class_level_requires_is_tuple(self) -> None:
        """BaseNode.requires class default must be a tuple"""
        assert isinstance(BaseNode.requires, tuple)

    def test_subclass_inherits_immutable_permissions(self) -> None:
        """A subclass that does not override permissions inherits the frozenset"""
        assert isinstance(BaseSenseNodeStub.permissions, frozenset)
        assert BaseSenseNodeStub.permissions == frozenset()

    def test_subclass_inherits_immutable_requires(self) -> None:
        """A subclass that does not override requires inherits the tuple"""
        assert isinstance(BaseSenseNodeStub.requires, tuple)
        assert BaseSenseNodeStub.requires == ()

    def test_instance_permissions_is_mutable_set(self) -> None:
        """After __init__, instance.permissions is a mutable set (copied)"""
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus)
        assert isinstance(node.permissions, set)
        # Mutation should succeed on the instance copy
        node.permissions.add(Permission.NETWORK)
        assert Permission.NETWORK in node.permissions

    def test_instance_requires_is_mutable_list(self) -> None:
        """After __init__, instance.requires is a mutable list (copied)"""
        bus = SignalBus()
        node = BaseSenseNodeStub(bus=bus)
        assert isinstance(node.requires, list)
        # Mutation should succeed on the instance copy
        node.requires.append("SomeNode")
        assert "SomeNode" in node.requires

    def test_instance_mutation_does_not_affect_class(self) -> None:
        """Mutating one instance's permissions/requires must not affect the class"""
        bus = SignalBus()
        node_a = BaseSenseNodeStub(bus=bus)
        node_b = BaseSenseNodeStub(bus=bus)

        node_a.permissions.add(Permission.GPU)
        node_a.requires.append("FooNode")

        # node_b must remain unaffected
        assert Permission.GPU not in node_b.permissions
        assert "FooNode" not in node_b.requires

        # Class-level must remain unaffected
        assert Permission.GPU not in BaseSenseNodeStub.permissions
        assert "FooNode" not in BaseSenseNodeStub.requires

    def test_skipped_super_init_gets_immutable_attrs(self) -> None:
        """If a subclass skips super().__init__(), class-level attrs are immutable"""

        class BrokenNode(BaseSenseNode):
            node_id = "BrokenNode"
            signal_kind = "test"

            def __init__(self) -> None:
                # Deliberately skip super().__init__()
                pass

            async def read(self) -> Signal:
                return Signal(
                    source=self.node_id, kind=self.signal_kind,
                    value=0.0, confidence=1.0, timestamp=0.0,
                )

        node = BrokenNode()
        # Without super().__init__(), the node falls back to the class attr
        assert isinstance(node.permissions, frozenset)
        assert isinstance(node.requires, tuple)
        # Attempting to mutate the frozenset should raise AttributeError
        with pytest.raises(AttributeError):
            node.permissions.add(Permission.NETWORK)  # type: ignore[union-attr]
        # Attempting to mutate the tuple should raise AttributeError
        with pytest.raises(AttributeError):
            node.requires.append("Foo")  # type: ignore[union-attr]

    def test_subclass_with_declared_permissions_frozenset(self) -> None:
        """Subclass declaring permissions as frozenset works correctly"""
        from arachnite.models import Permission

        class PermNode(BaseSenseNode):
            node_id = "PermNode"
            signal_kind = "test"
            permissions = frozenset({Permission.NETWORK, Permission.GPU})

            async def read(self) -> Signal:
                return Signal(
                    source=self.node_id, kind=self.signal_kind,
                    value=0.0, confidence=1.0, timestamp=0.0,
                )

        # Class level is frozenset
        assert isinstance(PermNode.permissions, frozenset)
        assert PermNode.permissions == frozenset({Permission.NETWORK, Permission.GPU})

        # Instance level is mutable set (via __init__ copy)
        bus = SignalBus()
        node = PermNode(bus=bus)
        assert isinstance(node.permissions, set)
        assert node.permissions == {Permission.NETWORK, Permission.GPU}

    def test_subclass_with_declared_requires_tuple(self) -> None:
        """Subclass declaring requires as tuple works correctly"""

        class ReqNode(BaseSenseNode):
            node_id = "ReqNode"
            signal_kind = "test"
            requires = ("SensorA", "SensorB")

            async def read(self) -> Signal:
                return Signal(
                    source=self.node_id, kind=self.signal_kind,
                    value=0.0, confidence=1.0, timestamp=0.0,
                )

        # Class level is tuple
        assert isinstance(ReqNode.requires, tuple)
        assert ReqNode.requires == ("SensorA", "SensorB")

        # Instance level is mutable list (via __init__ copy)
        bus = SignalBus()
        node = ReqNode(bus=bus)
        assert isinstance(node.requires, list)
        assert node.requires == ["SensorA", "SensorB"]
