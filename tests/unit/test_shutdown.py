"""Unit tests for ShutdownCoordinator."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from arachnite import ShutdownCoordinator, SignalBus
from arachnite.context import ContextNode
from arachnite.models import (
    ActionStep,
    InterruptPolicy,
    Proposal,
    ShutdownPhase,
    StepResult,
)
from arachnite.nodes.action import ActionMasterNode, MultiStepActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import InstinctMasterNode
from arachnite.nodes.sense import SenseMasterNode
from arachnite.runtime import ArachniteRuntime

# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_runtime(tick_rate_hz: float = 100.0, **kwargs) -> ArachniteRuntime:
    bus = SignalBus()
    return ArachniteRuntime(
        sense_master    = SenseMasterNode(bus=bus),
        context         = ContextNode(),
        instinct_master = InstinctMasterNode(bus=bus),
        decision_master = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus)),
        action_master   = ActionMasterNode(bus=bus),
        bus             = bus,
        tick_rate_hz    = tick_rate_hz,
        **kwargs,
    )


# ── Constructor ───────────────────────────────────────────────────────────────

class TestShutdownCoordinatorInit:
    def test_default_phase_is_not_started(self) -> None:
        sc = ShutdownCoordinator()
        assert sc.phase == ShutdownPhase.NOT_STARTED

    def test_not_completed_before_execute(self) -> None:
        sc = ShutdownCoordinator()
        assert not sc.completed

    def test_custom_teardown_timeout(self) -> None:
        sc = ShutdownCoordinator(teardown_timeout_s=10.0)
        assert sc._teardown_timeout_s == 10.0

    def test_custom_multiplier(self) -> None:
        sc = ShutdownCoordinator(mandatory_block_timeout_multiplier=2.0)
        assert sc._multiplier == 2.0


# ── execute() ────────────────────────────────────────────────────────────────

class TestShutdownCoordinatorExecute:
    @pytest.mark.asyncio
    async def test_execute_reaches_complete(self) -> None:
        sc = ShutdownCoordinator()
        rt = _make_runtime(shutdown_coordinator=sc)
        await rt.start()
        await sc.execute(rt)
        assert sc.completed
        assert sc.phase == ShutdownPhase.COMPLETE

    @pytest.mark.asyncio
    async def test_execute_stops_runtime(self) -> None:
        sc = ShutdownCoordinator()
        rt = _make_runtime(shutdown_coordinator=sc)
        await rt.start()
        await sc.execute(rt)
        assert not rt.is_running

    @pytest.mark.asyncio
    async def test_on_shutdown_action_called(self) -> None:
        called: list[bool] = []

        async def pre_shutdown() -> None:
            called.append(True)

        sc = ShutdownCoordinator(on_shutdown_action=pre_shutdown)
        rt = _make_runtime(shutdown_coordinator=sc)
        await rt.start()
        await sc.execute(rt)
        assert called == [True]

    @pytest.mark.asyncio
    async def test_on_shutdown_action_exception_does_not_abort(self) -> None:
        async def bad_action() -> None:
            raise RuntimeError("pre-shutdown failure")

        sc = ShutdownCoordinator(on_shutdown_action=bad_action)
        rt = _make_runtime(shutdown_coordinator=sc)
        await rt.start()
        await sc.execute(rt)   # must not propagate the exception
        assert sc.completed

    @pytest.mark.asyncio
    async def test_runtime_stop_delegates_to_coordinator(self) -> None:
        sc = ShutdownCoordinator()
        rt = _make_runtime(shutdown_coordinator=sc)
        await rt.start()
        await rt.stop()
        assert sc.completed

    @pytest.mark.asyncio
    async def test_stop_idempotent_on_non_running_runtime(self) -> None:
        sc = ShutdownCoordinator()
        rt = _make_runtime(shutdown_coordinator=sc)
        # stop() before start() should be a no-op (runtime not running)
        await rt.stop()
        assert not sc.completed   # coordinator was never invoked

    @pytest.mark.asyncio
    async def test_default_coordinator_created_when_not_provided(self) -> None:
        rt = _make_runtime()
        await rt.start()
        await rt.stop()   # uses auto-created ShutdownCoordinator
        assert not rt.is_running


# ── Loop-task timeout path (phase 3 lines 94-97) ─────────────────────────────

class TestShutdownLoopTaskTimeout:
    @pytest.mark.asyncio
    async def test_hanging_loop_task_is_cancelled_after_timeout(self) -> None:
        sc = ShutdownCoordinator(teardown_timeout_s=0.02)
        rt = _make_runtime(shutdown_coordinator=sc)
        await rt.start()
        # Let the real loop task complete
        await asyncio.sleep(0.01)

        # Inject a task that ignores _running and hangs
        async def hang() -> None:
            await asyncio.sleep(1000)

        hanging: asyncio.Task[None] = asyncio.create_task(hang())
        rt._loop_task = hanging

        await sc.execute(rt)

        assert sc.completed
        assert hanging.cancelled()


# ── Teardown-timeout path (phase 6) ──────────────────────────────────────────

class TestShutdownTeardownTimeout:
    @pytest.mark.asyncio
    async def test_slow_teardown_does_not_escape_stop(self) -> None:
        # A node teardown that overruns the grace period makes asyncio.wait_for raise
        # asyncio.TimeoutError — a distinct class from the builtin TimeoutError on
        # Python 3.10. The Phase-6 suppress must catch the asyncio one, or execute()
        # (and stop()) would raise instead of completing.
        sc = ShutdownCoordinator(teardown_timeout_s=0.02)
        rt = _make_runtime(shutdown_coordinator=sc)
        await rt.start()
        await asyncio.sleep(0.01)   # let the real loop task settle

        async def slow_teardown() -> None:
            await asyncio.sleep(1000)

        rt._action_master.teardown = slow_teardown  # type: ignore[method-assign]

        await sc.execute(rt)   # must not raise
        assert sc.completed


# ── Interrupt-action phase (lines 104-119) ────────────────────────────────────

class _ShutdownProbe(MultiStepActionNode):
    """Five short steps: a two-step mandatory block, then interruptible ones."""
    node_id = "_ShutdownProbe"
    timeout_s = 10.0

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.executed: list[str] = []
        self.undone: list[str] = []
        self.safe_stops = 0

    def _undo(self, name: str):  # type: ignore[no-untyped-def]
        async def _rollback() -> None:
            self.undone.append(name)
        return _rollback

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("lower", interruptible=False, rollback=self._undo("lower")),
            ActionStep("close", interruptible=False, rollback=self._undo("close")),
            ActionStep("raise", interruptible=True),
            ActionStep("move",  interruptible=True),
            ActionStep("drop",  interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        self.executed.append(step.name)
        await asyncio.sleep(0.05)
        return StepResult(step_name=step.name, success=True)

    async def on_preempted(self, proposal: Proposal, preempted_by: Proposal) -> None:
        self.safe_stops += 1


def _launch_probe(rt: ArachniteRuntime, probe: MultiStepActionNode) -> None:
    rt._action_master.register(probe)
    rt._action_master.launch(Proposal(
        instinct_id="test", action_id=probe.node_id, priority=50, urgency=0.5,
    ))


class TestShutdownInterruptAction:
    @pytest.mark.asyncio
    async def test_running_rollback_action_completes_block_then_unwinds(self) -> None:
        """Phase 3 lets the mandatory block finish; phase 4's interrupt stops
        the action at the next boundary and its ROLLBACK actually runs."""
        sc = ShutdownCoordinator()
        rt = _make_runtime(shutdown_coordinator=sc)
        probe = _ShutdownProbe(bus=rt.bus)
        probe.interrupt_policy = InterruptPolicy.ROLLBACK
        _launch_probe(rt, probe)
        await asyncio.sleep(0.01)           # inside "lower" (mandatory)

        await sc.execute(rt)

        assert probe.executed == ["lower", "close"]   # block completed, no more
        assert probe.undone == ["close", "lower"]     # reverse-order rollback ran
        assert probe.safe_stops == 0
        assert rt._action_master.running_action_ids() == set()
        assert sc.completed

    @pytest.mark.asyncio
    async def test_action_ignoring_interrupts_is_cancelled_with_safe_stop(self) -> None:
        """A NEVER-policy action outliving the grace period is cancelled; its
        safe-stop hook runs and shutdown still completes."""
        sc = ShutdownCoordinator(teardown_timeout_s=0.1)
        rt = _make_runtime(shutdown_coordinator=sc)
        probe = _ShutdownProbe(bus=rt.bus)
        probe.interrupt_policy = InterruptPolicy.NEVER
        _launch_probe(rt, probe)
        await asyncio.sleep(0.01)

        await sc.execute(rt)

        assert probe.safe_stops == 1
        assert probe.undone == []
        assert rt._action_master.running_action_ids() == set()
        assert sc.completed


# ── _mandatory_timeout with execution_state (lines 149-152) ──────────────────

class TestMandatoryTimeout:
    def test_uses_remaining_s_when_positive(self) -> None:
        sc = ShutdownCoordinator(
            teardown_timeout_s=5.0,
            mandatory_block_timeout_multiplier=2.0,
        )
        rt = _make_runtime()

        mock_state = MagicMock()
        mock_state.mandatory_block_remaining_s = 3.0
        mock_action = MagicMock()
        mock_action.execution_state = MagicMock(return_value=mock_state)
        rt._action_master.current_actions = MagicMock(return_value={"Act": mock_action})  # type: ignore[method-assign]

        assert abs(sc._mandatory_timeout(rt) - 6.0) < 1e-9   # 3.0 * 2.0

    def test_falls_back_to_teardown_timeout_when_remaining_is_zero(self) -> None:
        sc = ShutdownCoordinator(
            teardown_timeout_s=4.0,
            mandatory_block_timeout_multiplier=2.0,
        )
        rt = _make_runtime()

        mock_state = MagicMock()
        mock_state.mandatory_block_remaining_s = 0.0
        mock_action = MagicMock()
        mock_action.execution_state = MagicMock(return_value=mock_state)
        rt._action_master.current_actions = MagicMock(return_value={"Act": mock_action})  # type: ignore[method-assign]

        assert sc._mandatory_timeout(rt) == 4.0

    def test_falls_back_when_no_execution_state(self) -> None:
        sc = ShutdownCoordinator(teardown_timeout_s=7.0)
        rt = _make_runtime()
        rt._action_master.current_actions = MagicMock(return_value={})  # type: ignore[method-assign]
        assert sc._mandatory_timeout(rt) == 7.0


# ── Public API export ─────────────────────────────────────────────────────────

class TestShutdownCoordinatorPublicAPI:
    def test_importable_from_root_package(self) -> None:
        from arachnite import ShutdownCoordinator as SC
        assert SC is ShutdownCoordinator

    def test_in_all(self) -> None:
        import arachnite
        assert "ShutdownCoordinator" in arachnite.__all__
