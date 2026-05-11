"""Unit tests for NodeSupervisor restart logic."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from arachnite import SignalBus
from arachnite.exceptions import SupervisorError
from arachnite.models import NodeFaultSignal, NodeState, RestartPolicy, SupervisorSignal
from arachnite.nodes.base import BaseNode
from arachnite.supervisor import NodeSupervisor

# ── Minimal concrete node ─────────────────────────────────────────────────────

class SimpleNode(BaseNode):
    node_id = "SimpleNode"

    def __init__(self, bus: SignalBus, *, fail_setup: bool = False) -> None:
        super().__init__(bus)
        self.setup_count    = 0
        self.teardown_count = 0
        self._fail_setup    = fail_setup

    async def setup(self) -> None:
        self.setup_count += 1
        if self._fail_setup:
            raise RuntimeError("setup failed")

    async def teardown(self) -> None:
        self.teardown_count += 1


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def bus() -> SignalBus:
    return SignalBus()


def make_supervisor(
    bus: SignalBus,
    *,
    policy: RestartPolicy = RestartPolicy.ON_FAILURE,
    max_restarts: int = 2,
    restart_delay_s: float = 0.0,
) -> NodeSupervisor:
    return NodeSupervisor(
        bus             = bus,
        restart_policy  = policy,
        max_restarts    = max_restarts,
        restart_delay_s = restart_delay_s,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestNodeSupervisorTracking:
    def test_track_sets_starting_state(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = SimpleNode(bus=bus)
        sv.track(node)
        assert sv.state_of("SimpleNode") == NodeState.STARTING

    def test_untrack_removes_node(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = SimpleNode(bus=bus)
        sv.track(node)
        sv.untrack("SimpleNode")
        with pytest.raises(KeyError):
            sv.state_of("SimpleNode")

    def test_is_healthy_with_no_faults(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = SimpleNode(bus=bus)
        sv.track(node)
        assert sv.is_healthy()

    @pytest.mark.asyncio
    async def test_mark_running_transitions_state(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.mark_running("SimpleNode")
        assert sv.state_of("SimpleNode") == NodeState.RUNNING

    @pytest.mark.asyncio
    async def test_is_healthy_false_when_faulted(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("boom"))
        assert not sv.is_healthy()


class TestRestartPolicyNever:
    @pytest.mark.asyncio
    async def test_never_goes_straight_to_dead(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("boom"))
        assert sv.state_of("SimpleNode") == NodeState.DEAD

    @pytest.mark.asyncio
    async def test_never_emits_supervisor_signal(self, bus: SignalBus) -> None:
        sv      = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node    = SimpleNode(bus=bus)
        signals: list[SupervisorSignal] = []
        bus.subscribe("supervisor", lambda s: signals.append(s))  # type: ignore[arg-type]
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("x"))
        await asyncio.sleep(0.01)  # let bus deliver
        assert any(isinstance(s, SupervisorSignal) for s in signals)
        # Final state should be DEAD
        dead = [s for s in signals if s.current_state == NodeState.DEAD]
        assert dead


class TestRestartPolicyOnFailure:
    @pytest.mark.asyncio
    async def test_restarts_on_fault(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.ON_FAILURE, max_restarts=1)
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("first"))
        await asyncio.sleep(0.05)  # wait for restart task
        assert node.setup_count >= 1
        assert sv.state_of("SimpleNode") == NodeState.RUNNING

    @pytest.mark.asyncio
    async def test_exhausted_max_restarts_goes_dead(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.ON_FAILURE, max_restarts=1)
        node = SimpleNode(bus=bus)
        sv.track(node)
        # First fault: uses the one allowed restart
        await sv.on_fault("SimpleNode", RuntimeError("first"))
        await asyncio.sleep(0.05)
        # Second fault: restart count (1) >= max_restarts (1), goes DEAD
        await sv.on_fault("SimpleNode", RuntimeError("second"))
        assert sv.state_of("SimpleNode") == NodeState.DEAD

    @pytest.mark.asyncio
    async def test_restart_count_increments(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.ON_FAILURE, max_restarts=3)
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("boom"))
        await asyncio.sleep(0.05)
        assert sv._restart_counts["SimpleNode"] == 1


class TestRestartPolicyAlways:
    @pytest.mark.asyncio
    async def test_always_restarts_on_fault(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.ALWAYS, max_restarts=2)
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("boom"))
        await asyncio.sleep(0.05)
        assert sv.state_of("SimpleNode") == NodeState.RUNNING

    @pytest.mark.asyncio
    async def test_always_exhausted_goes_dead(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.ALWAYS, max_restarts=1)
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("first"))
        await asyncio.sleep(0.05)
        await sv.on_fault("SimpleNode", RuntimeError("second"))
        assert sv.state_of("SimpleNode") == NodeState.DEAD


class TestManualRestart:
    @pytest.mark.asyncio
    async def test_restart_untracked_node_raises(self, bus: SignalBus) -> None:
        sv = make_supervisor(bus)
        with pytest.raises(KeyError):
            await sv.restart("NotTracked")

    @pytest.mark.asyncio
    async def test_restart_tracked_node_calls_restart(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, restart_delay_s=0.0)
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.restart("SimpleNode")
        assert node.setup_count >= 1
        assert sv.state_of("SimpleNode") == NodeState.RUNNING

    @pytest.mark.asyncio
    async def test_restart_after_untrack_is_noop(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus, restart_delay_s=0.0)
        node = SimpleNode(bus=bus)
        sv.track(node)
        sv.untrack("SimpleNode")
        # Node is gone — _restart must return early without raising
        await sv._restart("SimpleNode")   # must not raise


class TestRestartSetupFailure:
    @pytest.mark.asyncio
    async def test_setup_failure_exhausted_raises_supervisor_error(
        self, bus: SignalBus
    ) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.ON_FAILURE,
                               max_restarts=1, restart_delay_s=0.0)
        node = SimpleNode(bus=bus, fail_setup=True)
        sv.track(node)
        # Pre-fill restart count to max so the first failed restart exhausts budget
        sv._restart_counts["SimpleNode"] = 1
        with pytest.raises(SupervisorError):
            await sv._restart("SimpleNode")
        assert sv.state_of("SimpleNode") == NodeState.DEAD

    @pytest.mark.asyncio
    async def test_setup_failure_below_max_schedules_another_restart(
        self, bus: SignalBus
    ) -> None:
        sv   = make_supervisor(bus, policy=RestartPolicy.ON_FAILURE,
                               max_restarts=3, restart_delay_s=0.0)
        node = SimpleNode(bus=bus, fail_setup=True)
        sv.track(node)
        sv._restart_counts["SimpleNode"] = 0
        # First failed restart: count becomes 1, still below max → schedules next
        # We cancel pending tasks to avoid infinite loop in tests
        pending_before = sv.restart_task_count
        with contextlib.suppress(Exception):
            await sv._restart("SimpleNode")
        # Another restart must have been scheduled (pending supervisor task count grew)
        assert sv.restart_task_count > pending_before
        # Node is FAULTED (not DEAD) since we're below max_restarts
        assert sv.state_of("SimpleNode") == NodeState.FAULTED
        # Clean up to prevent leaked tasks
        await sv.cancel_restart_tasks()


class TestSupervisorRepr:
    def test_repr_contains_supervisor_id(self, bus: SignalBus) -> None:
        from arachnite.supervisor import NodeSupervisor
        sv = NodeSupervisor(bus=bus, supervisor_id="my-sv")
        assert "my-sv" in repr(sv)

    def test_repr_shows_node_count(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = SimpleNode(bus=bus)
        sv.track(node)
        assert "nodes=1" in repr(sv)

    def test_repr_shows_healthy_count(self, bus: SignalBus) -> None:
        sv   = make_supervisor(bus)
        node = SimpleNode(bus=bus)
        sv.track(node)
        assert "healthy=1" in repr(sv)


class TestSupervisorSignalEmission:
    @pytest.mark.asyncio
    async def test_signal_published_with_correct_node_id(self, bus: SignalBus) -> None:
        sv      = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node    = SimpleNode(bus=bus)
        signals: list[SupervisorSignal] = []
        bus.subscribe("supervisor", lambda s: signals.append(s))  # type: ignore[arg-type]
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("err"))
        await asyncio.sleep(0.01)
        assert any(s.node_id == "SimpleNode" for s in signals)

    @pytest.mark.asyncio
    async def test_signal_carries_fault_error(self, bus: SignalBus) -> None:
        sv      = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node    = SimpleNode(bus=bus)
        signals: list[SupervisorSignal] = []
        bus.subscribe("supervisor", lambda s: signals.append(s))  # type: ignore[arg-type]
        sv.track(node)
        err = RuntimeError("specific error")
        await sv.on_fault("SimpleNode", err)
        await asyncio.sleep(0.01)
        faulted = [s for s in signals if s.current_state == NodeState.FAULTED]
        assert faulted
        assert faulted[0].fault_error is err


class TestNodeFaultSignal:
    """Tests for the typed NodeFaultSignal emitted on fault transitions."""

    @pytest.mark.asyncio
    async def test_fault_signal_emitted_on_fault(self, bus: SignalBus) -> None:
        """on_fault() emits a NodeFaultSignal on the 'node_fault' kind."""
        sv = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node = SimpleNode(bus=bus)
        fault_signals: list[NodeFaultSignal] = []
        bus.subscribe("node_fault", lambda s: fault_signals.append(s))  # type: ignore[arg-type]
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("boom"))
        await asyncio.sleep(0.01)
        # FAULTED + DEAD = 2 fault signals (NEVER policy goes straight to DEAD)
        assert len(fault_signals) >= 1
        assert fault_signals[0].node_id == "SimpleNode"

    @pytest.mark.asyncio
    async def test_fault_signal_carries_error_type(self, bus: SignalBus) -> None:
        sv = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node = SimpleNode(bus=bus)
        fault_signals: list[NodeFaultSignal] = []
        bus.subscribe("node_fault", lambda s: fault_signals.append(s))  # type: ignore[arg-type]
        sv.track(node)
        await sv.on_fault("SimpleNode", ValueError("bad value"))
        await asyncio.sleep(0.01)
        assert fault_signals[0].error_type == "ValueError"
        assert fault_signals[0].error_message == "bad value"

    @pytest.mark.asyncio
    async def test_fault_signal_kind_is_node_fault(self, bus: SignalBus) -> None:
        sv = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node = SimpleNode(bus=bus)
        fault_signals: list[NodeFaultSignal] = []
        bus.subscribe("node_fault", lambda s: fault_signals.append(s))  # type: ignore[arg-type]
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("x"))
        await asyncio.sleep(0.01)
        assert all(s.kind == "node_fault" for s in fault_signals)

    @pytest.mark.asyncio
    async def test_no_fault_signal_on_normal_transition(self, bus: SignalBus) -> None:
        """Normal transitions (STARTING->RUNNING) do not emit NodeFaultSignal."""
        sv = make_supervisor(bus)
        node = SimpleNode(bus=bus)
        fault_signals: list[NodeFaultSignal] = []
        bus.subscribe("node_fault", lambda s: fault_signals.append(s))  # type: ignore[arg-type]
        sv.track(node)
        await sv.mark_running("SimpleNode")
        await asyncio.sleep(0.01)
        assert len(fault_signals) == 0

    @pytest.mark.asyncio
    async def test_fault_signal_also_emits_supervisor_signal(self, bus: SignalBus) -> None:
        """Both supervisor and node_fault signals are emitted — backward compatible."""
        sv = make_supervisor(bus, policy=RestartPolicy.NEVER)
        node = SimpleNode(bus=bus)
        sv_signals: list[SupervisorSignal] = []
        fault_signals: list[NodeFaultSignal] = []
        bus.subscribe("supervisor", lambda s: sv_signals.append(s))  # type: ignore[arg-type]
        bus.subscribe("node_fault", lambda s: fault_signals.append(s))  # type: ignore[arg-type]
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("x"))
        await asyncio.sleep(0.01)
        assert len(sv_signals) >= 1
        assert len(fault_signals) >= 1

    def test_node_fault_signal_model_defaults(self) -> None:
        """NodeFaultSignal auto-populates error_type and error_message from fault_error."""
        sig = NodeFaultSignal(
            source="sv", kind="node_fault", value="faulted",
            confidence=1.0, timestamp=0.0,
            node_id="TestNode",
            fault_error=TypeError("wrong type"),
        )
        assert sig.error_type == "TypeError"
        assert sig.error_message == "wrong type"
        assert sig.kind == "node_fault"

    def test_node_fault_signal_without_error(self) -> None:
        """NodeFaultSignal with no fault_error has empty error fields."""
        sig = NodeFaultSignal(
            source="sv", kind="node_fault", value="faulted",
            confidence=1.0, timestamp=0.0,
            node_id="TestNode",
        )
        assert sig.error_type == ""
        assert sig.error_message == ""


class TestRestartTaskTracking:
    """Tests for B-18: supervisor restart tasks are tracked and cancellable."""

    @pytest.mark.asyncio
    async def test_restart_tasks_are_tracked(self, bus: SignalBus) -> None:
        """on_fault() creates a tracked restart task visible via restart_task_count."""
        sv = make_supervisor(
            bus, policy=RestartPolicy.ON_FAILURE, max_restarts=2,
            restart_delay_s=5.0,  # long delay so task stays in-flight
        )
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("boom"))
        assert sv.restart_task_count > 0
        # Clean up to avoid warnings about pending tasks
        await sv.cancel_restart_tasks()

    @pytest.mark.asyncio
    async def test_restart_tasks_clean_up_on_completion(self, bus: SignalBus) -> None:
        """After a restart task completes, it is removed from the tracked set."""
        sv = make_supervisor(
            bus, policy=RestartPolicy.ON_FAILURE, max_restarts=2,
            restart_delay_s=0.0,
        )
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("boom"))
        await asyncio.sleep(0.05)  # let the restart complete
        assert sv.restart_task_count == 0

    @pytest.mark.asyncio
    async def test_cancel_restart_tasks_cancels_in_flight(self, bus: SignalBus) -> None:
        """cancel_restart_tasks() cancels pending restarts and zeroes the count."""
        sv = make_supervisor(
            bus, policy=RestartPolicy.ON_FAILURE, max_restarts=2,
            restart_delay_s=10.0,  # very long delay — task will be in-flight
        )
        node = SimpleNode(bus=bus)
        sv.track(node)
        await sv.on_fault("SimpleNode", RuntimeError("boom"))
        assert sv.restart_task_count > 0
        await sv.cancel_restart_tasks()
        assert sv.restart_task_count == 0
        # Node should NOT have been restarted (setup not called)
        assert node.setup_count == 0

    @pytest.mark.asyncio
    async def test_cancel_restart_tasks_is_safe_when_empty(self, bus: SignalBus) -> None:
        """cancel_restart_tasks() is a no-op when no restarts are pending."""
        sv = make_supervisor(bus)
        await sv.cancel_restart_tasks()  # must not raise
        assert sv.restart_task_count == 0
