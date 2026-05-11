"""Unit tests for RuntimeBuilder."""

from __future__ import annotations

import pytest

from arachnite import RuntimeBuilder, SignalBus
from arachnite.models import Context, Proposal
from arachnite.nodes.decision import (
    GreedyDecisionNode,
    RandomDecisionNode,
)
from tests.conftest import (
    ConstantSenseNode,
    RecordingAction,
    ThresholdInstinct,
    make_proposal,
)


class AlwaysFireInstinct(ThresholdInstinct):
    """Fires every tick, targeting RecordingAction"""
    node_id = "AlwaysFireInstinct"
    priority = 80

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return make_proposal(
            action_id="RecordingAction",
            priority=self.priority,
            instinct_id=self.node_id,
        )


class TestRuntimeBuilderBasic:
    """Basic builder construction tests."""

    def test_build_minimal(self) -> None:
        rt = RuntimeBuilder().build()
        assert rt is not None
        assert rt.tick_count == 0

    def test_build_with_node_classes(self) -> None:
        rt = (
            RuntimeBuilder()
            .sense(ConstantSenseNode)
            .instinct(ThresholdInstinct)
            .action(RecordingAction)
            .build()
        )
        assert rt is not None

    def test_build_with_node_instances(self) -> None:
        builder = RuntimeBuilder()
        bus = builder.bus
        rt = (
            builder
            .sense(ConstantSenseNode(bus=bus, value=30.0))
            .instinct(ThresholdInstinct(bus=bus, threshold=50.0))
            .action(RecordingAction(bus=bus))
            .build()
        )
        assert rt is not None

    def test_bus_property(self) -> None:
        builder = RuntimeBuilder()
        assert isinstance(builder.bus, SignalBus)

    def test_default_strategy_is_greedy(self) -> None:
        builder = RuntimeBuilder()
        assert builder._strategy_cls is GreedyDecisionNode

    def test_custom_strategy_class(self) -> None:
        rt = (
            RuntimeBuilder()
            .strategy(RandomDecisionNode)
            .build()
        )
        assert rt is not None

    def test_custom_strategy_instance(self) -> None:
        builder = RuntimeBuilder()
        strategy = RandomDecisionNode(bus=builder.bus)
        rt = builder.strategy(strategy).build()
        assert rt is not None

    def test_tick_rate(self) -> None:
        rt = RuntimeBuilder().tick_rate(5.0).build()
        assert rt._tick_rate_hz == 5.0

    def test_reflex_conflict(self) -> None:
        rt = RuntimeBuilder().reflex_conflict("raise").build()
        assert rt._instinct_master.reflex_conflict == "raise"

    def test_overrun_warn(self) -> None:
        rt = RuntimeBuilder().overrun_warn(0.5).build()
        assert rt._overrun_warn == 0.5

    def test_overrun_warn_consecutive(self) -> None:
        rt = RuntimeBuilder().overrun_warn_consecutive(5).build()
        assert rt._overrun_warn_consecutive == 5

    def test_overrun_warn_consecutive_default(self) -> None:
        rt = RuntimeBuilder().build()
        assert rt._overrun_warn_consecutive == 3


class TestRuntimeBuilderIntegration:
    """Builder produces a working runtime."""

    @pytest.mark.asyncio
    async def test_built_runtime_runs_tick(self) -> None:
        rt = (
            RuntimeBuilder()
            .sense(ConstantSenseNode)
            .instinct(ThresholdInstinct)
            .action(RecordingAction)
            .tick_rate(100.0)
            .build()
        )
        await rt.start()
        await rt.tick()
        assert rt.tick_count >= 1
        await rt.stop()

    @pytest.mark.asyncio
    async def test_built_runtime_fires_instinct(self) -> None:
        builder = RuntimeBuilder()
        bus = builder.bus
        action = RecordingAction(bus=bus)
        rt = (
            builder
            .sense(ConstantSenseNode(bus=bus, value=90.0))
            .instinct(AlwaysFireInstinct(bus=bus))
            .action(action)
            .tick_rate(100.0)
            .build()
        )
        await rt.start()
        for _ in range(3):
            await rt.tick()
        await rt.stop()
        assert len(action.calls) > 0

    @pytest.mark.asyncio
    async def test_chaining_multiple_nodes(self) -> None:
        builder = RuntimeBuilder()
        bus = builder.bus
        action1 = RecordingAction(bus=bus)
        # Override node_id so we can register two actions
        action1.node_id = "RecordingAction1"  # type: ignore[assignment]

        action2 = RecordingAction(bus=bus)
        action2.node_id = "RecordingAction2"  # type: ignore[assignment]

        rt = (
            builder
            .sense(ConstantSenseNode(bus=bus))
            .action(action1)
            .action(action2)
            .build()
        )
        assert rt is not None
        await rt.start()
        await rt.stop()
