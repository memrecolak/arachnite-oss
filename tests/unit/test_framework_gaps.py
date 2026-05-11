"""
Unit tests for the five framework gaps added to support Ariadne:

  Gap 1 — Live node registration  (ArachniteRuntime.register_*_live)
  Gap 2 — ContextNode state persistence  (state_path / flush_on_write)
  Gap 3 — StateUpdateSignal  (bus-based ContextNode state write)
  Gap 4 — LLMInstinctNode.setup() preloads provider
  Gap 5 — trigger_interval_s on BaseInstinctNode
  Gap 6 — Live node unregistration  (ArachniteRuntime.unregister_*_live)
  Gap 7 — Supervisor signals visible in Context
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from arachnite import ContextNode, SignalBus
from arachnite.models import (
    Context,
    NodeState,
    Proposal,
    Signal,
    StateUpdateSignal,
    SupervisorSignal,
)
from arachnite.nodes.action import ActionMasterNode, BaseActionNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import BaseInstinctNode, InstinctMasterNode
from arachnite.nodes.llm import LLMInstinctNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime

# ── Shared helpers ─────────────────────────────────────────────────────────────

def _bus() -> SignalBus:
    return SignalBus()


def _ctx() -> Context:
    return Context(
        tick=1, signals=[], history=deque(),
        state={}, last_result=None, timestamp=time.monotonic(),
    )


def _sig(kind: str = "temperature", value: float = 1.0) -> Signal:
    return Signal(source="test", kind=kind, value=value,
                  confidence=1.0, timestamp=time.monotonic())


def _build_rt(bus: SignalBus | None = None) -> ArachniteRuntime:
    bus = bus or _bus()
    sm  = SenseMasterNode(bus=bus)
    im  = InstinctMasterNode(bus=bus)
    dm  = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am  = ActionMasterNode(bus=bus)
    return ArachniteRuntime(
        sense_master=sm, context=ContextNode(),
        instinct_master=im, decision_master=dm,
        action_master=am, bus=bus, tick_rate_hz=1000.0,
    )


# ── Concrete node stubs ────────────────────────────────────────────────────────

class DummySense(BaseSenseNode):
    node_id = "DummySense"
    signal_kind = "dummy"
    setup_called = False
    teardown_called = False

    async def setup(self) -> None:
        DummySense.setup_called = True

    async def teardown(self) -> None:
        DummySense.teardown_called = True

    async def read(self) -> Signal:
        return _sig()


class DummyAction(BaseActionNode):
    node_id = "DummyAction"
    setup_called = False
    teardown_called = False

    async def setup(self) -> None:
        DummyAction.setup_called = True

    async def teardown(self) -> None:
        DummyAction.teardown_called = True

    async def execute(self, proposal: Proposal) -> Any:
        from arachnite.models import Result
        return Result(action_id=self.node_id, success=True)


class DummyInstinct(BaseInstinctNode):
    node_id  = "DummyInstinct"
    priority = 50
    setup_called = False
    teardown_called = False

    async def setup(self) -> None:
        DummyInstinct.setup_called = True

    async def teardown(self) -> None:
        DummyInstinct.teardown_called = True

    async def evaluate(self, ctx: Context) -> Proposal | None:
        return None


class SlowInstinct(BaseInstinctNode):
    """Instinct that should only fire every 60 s."""
    node_id            = "SlowInstinct"
    priority           = 50
    trigger_interval_s: float | None = 60.0
    call_count         = 0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        SlowInstinct.call_count += 1
        return None


class FastIntervalInstinct(BaseInstinctNode):
    """Short trigger_interval_s for tests that need it to expire."""
    node_id            = "FastIntervalInstinct"
    priority           = 50
    trigger_interval_s: float | None = 0.01
    call_count         = 0

    async def evaluate(self, ctx: Context) -> Proposal | None:
        FastIntervalInstinct.call_count += 1
        return None


class LLMTestInstinct(LLMInstinctNode):
    node_id  = "LLMTestInstinct"
    priority = 50

    def available_actions(self) -> dict[str, str]:
        return {"DoSomething": "Do something"}


# ══════════════════════════════════════════════════════════════════════════════
# Gap 1 — Live node registration
# ══════════════════════════════════════════════════════════════════════════════

class TestLiveNodeRegistration:
    @pytest.mark.asyncio
    async def test_register_sense_live_calls_setup_when_running(self) -> None:
        DummySense.setup_called = False
        rt = _build_rt()
        await rt.start()
        node = DummySense(bus=rt.bus)
        await rt.register_sense_live(node)
        assert DummySense.setup_called
        await rt.stop()

    @pytest.mark.asyncio
    async def test_register_sense_live_before_start_does_not_call_setup(self) -> None:
        DummySense.setup_called = False
        rt = _build_rt()
        node = DummySense(bus=rt.bus)
        await rt.register_sense_live(node)
        assert not DummySense.setup_called

    @pytest.mark.asyncio
    async def test_register_instinct_live_calls_setup_when_running(self) -> None:
        DummyInstinct.setup_called = False
        rt = _build_rt()
        await rt.start()
        node = DummyInstinct(bus=rt.bus)
        await rt.register_instinct_live(node)
        assert DummyInstinct.setup_called
        await rt.stop()

    @pytest.mark.asyncio
    async def test_register_action_live_calls_setup_when_running(self) -> None:
        DummyAction.setup_called = False
        rt = _build_rt()
        await rt.start()
        node = DummyAction(bus=rt.bus)
        await rt.register_action_live(node)
        assert DummyAction.setup_called
        await rt.stop()

    @pytest.mark.asyncio
    async def test_register_sense_live_tracked_by_supervisor(self) -> None:
        rt = _build_rt()
        await rt.start()
        node = DummySense(bus=rt.bus)
        await rt.register_sense_live(node)
        # Verify supervisor 0 (sense) is tracking the new node
        tracked = rt._supervisors[0].all_states()
        assert node.node_id in tracked
        await rt.stop()

    @pytest.mark.asyncio
    async def test_register_action_live_node_is_usable(self) -> None:
        """Node registered live can be dispatched to."""
        rt = _build_rt()
        await rt.start()
        node = DummyAction(bus=rt.bus)
        await rt.register_action_live(node)
        # Verify node is in action master
        assert rt._action_master._nodes.get("DummyAction") is node
        await rt.stop()


# ══════════════════════════════════════════════════════════════════════════════
# Gap 6 — Live node unregistration
# ══════════════════════════════════════════════════════════════════════════════

class TestLiveNodeUnregistration:
    @pytest.mark.asyncio
    async def test_unregister_sense_live_calls_teardown(self) -> None:
        DummySense.teardown_called = False
        rt = _build_rt()
        await rt.start()
        node = DummySense(bus=rt.bus)
        await rt.register_sense_live(node)
        await rt.unregister_sense_live("DummySense")
        assert DummySense.teardown_called
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_sense_live_removes_from_master(self) -> None:
        rt = _build_rt()
        await rt.start()
        node = DummySense(bus=rt.bus)
        await rt.register_sense_live(node)
        await rt.unregister_sense_live("DummySense")
        assert "DummySense" not in rt._sense_master._nodes
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_sense_live_untracks_from_supervisor(self) -> None:
        rt = _build_rt()
        await rt.start()
        node = DummySense(bus=rt.bus)
        await rt.register_sense_live(node)
        await rt.unregister_sense_live("DummySense")
        assert "DummySense" not in rt._supervisors[0].all_states()
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_instinct_live_calls_teardown(self) -> None:
        DummyInstinct.teardown_called = False
        rt = _build_rt()
        await rt.start()
        node = DummyInstinct(bus=rt.bus)
        await rt.register_instinct_live(node)
        await rt.unregister_instinct_live("DummyInstinct")
        assert DummyInstinct.teardown_called
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_instinct_live_removes_from_master(self) -> None:
        rt = _build_rt()
        await rt.start()
        node = DummyInstinct(bus=rt.bus)
        await rt.register_instinct_live(node)
        await rt.unregister_instinct_live("DummyInstinct")
        assert "DummyInstinct" not in rt._instinct_master._normal_nodes
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_instinct_live_untracks_from_supervisor(self) -> None:
        rt = _build_rt()
        await rt.start()
        node = DummyInstinct(bus=rt.bus)
        await rt.register_instinct_live(node)
        await rt.unregister_instinct_live("DummyInstinct")
        assert "DummyInstinct" not in rt._supervisors[1].all_states()
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_action_live_calls_teardown(self) -> None:
        DummyAction.teardown_called = False
        rt = _build_rt()
        await rt.start()
        node = DummyAction(bus=rt.bus)
        await rt.register_action_live(node)
        await rt.unregister_action_live("DummyAction")
        assert DummyAction.teardown_called
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_action_live_removes_from_master(self) -> None:
        rt = _build_rt()
        await rt.start()
        node = DummyAction(bus=rt.bus)
        await rt.register_action_live(node)
        await rt.unregister_action_live("DummyAction")
        assert "DummyAction" not in rt._action_master._nodes
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_action_live_untracks_from_supervisor(self) -> None:
        rt = _build_rt()
        await rt.start()
        node = DummyAction(bus=rt.bus)
        await rt.register_action_live(node)
        await rt.unregister_action_live("DummyAction")
        assert "DummyAction" not in rt._supervisors[3].all_states()
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_nonexistent_node_is_silent(self) -> None:
        """Unregistering a node that does not exist must not raise."""
        rt = _build_rt()
        await rt.start()
        await rt.unregister_sense_live("NoSuchNode")
        await rt.unregister_instinct_live("NoSuchNode")
        await rt.unregister_action_live("NoSuchNode")
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_then_reregister_sense(self) -> None:
        """Full hot-swap cycle: unregister old node, register replacement."""
        rt = _build_rt()
        await rt.start()
        node_v1 = DummySense(bus=rt.bus)
        await rt.register_sense_live(node_v1)
        await rt.unregister_sense_live("DummySense")
        # Re-register a fresh instance with the same node_id
        DummySense.setup_called = False
        node_v2 = DummySense(bus=rt.bus)
        await rt.register_sense_live(node_v2)
        assert DummySense.setup_called
        assert rt._sense_master._nodes["DummySense"] is node_v2
        assert "DummySense" in rt._supervisors[0].all_states()
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_then_reregister_action(self) -> None:
        """Full hot-swap cycle for action nodes."""
        rt = _build_rt()
        await rt.start()
        node_v1 = DummyAction(bus=rt.bus)
        await rt.register_action_live(node_v1)
        await rt.unregister_action_live("DummyAction")
        DummyAction.setup_called = False
        node_v2 = DummyAction(bus=rt.bus)
        await rt.register_action_live(node_v2)
        assert DummyAction.setup_called
        assert rt._action_master._nodes["DummyAction"] is node_v2
        assert "DummyAction" in rt._supervisors[3].all_states()
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_tolerates_teardown_exception(self) -> None:
        """If teardown() raises, the node is still removed cleanly."""
        class FailTeardownSense(BaseSenseNode):
            node_id = "FailTeardownSense"
            signal_kind = "fail"
            async def read(self) -> Signal:
                return _sig()
            async def teardown(self) -> None:
                raise RuntimeError("teardown exploded")

        rt = _build_rt()
        await rt.start()
        node = FailTeardownSense(bus=rt.bus)
        await rt.register_sense_live(node)
        # Must not raise despite teardown failure
        await rt.unregister_sense_live("FailTeardownSense")
        assert "FailTeardownSense" not in rt._sense_master._nodes
        assert "FailTeardownSense" not in rt._supervisors[0].all_states()
        await rt.stop()

    @pytest.mark.asyncio
    async def test_unregister_reflex_instinct_live(self) -> None:
        """Reflex instincts are unregistered through the same API."""
        from arachnite.nodes.instinct import BaseReflexInstinctNode

        class DummyReflex(BaseReflexInstinctNode):
            node_id  = "DummyReflex"
            priority = 200
            teardown_called = False
            async def teardown(self) -> None:
                DummyReflex.teardown_called = True
            async def evaluate(self, ctx: Context) -> Proposal | None:
                return None

        rt = _build_rt()
        await rt.start()
        node = DummyReflex(bus=rt.bus)
        await rt.register_instinct_live(node)
        assert "DummyReflex" in rt._instinct_master._reflex_nodes
        await rt.unregister_instinct_live("DummyReflex")
        assert DummyReflex.teardown_called
        assert "DummyReflex" not in rt._instinct_master._reflex_nodes
        assert "DummyReflex" not in rt._supervisors[1].all_states()
        await rt.stop()


# ══════════════════════════════════════════════════════════════════════════════
# Gap 2 — State persistence
# ══════════════════════════════════════════════════════════════════════════════

class TestStatePersistence:
    def test_flush_state_writes_json(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        ctx = ContextNode(state_path=p)
        ctx.set("key", "value")
        ctx.flush_state()
        assert p.exists()
        data = json.loads(p.read_text())
        assert data["key"] == "value"

    def test_flush_on_write_flushes_on_set(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        ctx = ContextNode(state_path=p, flush_on_write=True)
        ctx.set("x", 42)
        assert p.exists()
        assert json.loads(p.read_text())["x"] == 42

    def test_flush_on_write_flushes_on_delete(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        ctx = ContextNode(state_path=p, flush_on_write=True)
        ctx.set("x", 1)
        ctx.delete("x")
        assert "x" not in json.loads(p.read_text())

    def test_state_loaded_from_existing_file(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        p.write_text(json.dumps({"restored": True}), encoding="utf-8")
        ctx = ContextNode(state_path=p)
        assert ctx.get("restored") is True

    def test_corrupt_state_file_starts_empty(self, tmp_path: Path) -> None:
        p = tmp_path / "state.json"
        p.write_text("not valid json", encoding="utf-8")
        ctx = ContextNode(state_path=p)
        assert ctx.get("anything") is None

    def test_flush_state_no_path_is_noop(self) -> None:
        ctx = ContextNode()
        ctx.set("x", 1)
        ctx.flush_state()  # must not raise

    def test_flush_state_creates_parent_dirs(self, tmp_path: Path) -> None:
        p = tmp_path / "deep" / "nested" / "state.json"
        ctx = ContextNode(state_path=p)
        ctx.set("a", "b")
        ctx.flush_state()
        assert p.exists()


# ══════════════════════════════════════════════════════════════════════════════
# Gap 3 — StateUpdateSignal
# ══════════════════════════════════════════════════════════════════════════════

def _state_sig(key: str, state_value: Any = None, delete: bool = False) -> StateUpdateSignal:
    return StateUpdateSignal(
        source="test", kind="state_update", value=None,
        confidence=1.0, timestamp=time.monotonic(),
        key=key, state_value=state_value, delete=delete,
    )


class TestStateUpdateSignal:
    def test_state_update_signal_has_kind_state_update(self) -> None:
        sig = _state_sig("k", state_value="v")
        assert sig.kind == "state_update"
        assert sig.confidence == 1.0

    def test_context_applies_state_update_signal(self) -> None:
        ctx = ContextNode()
        snap = ctx.update([_state_sig("world", state_value={"temp": 42})])
        assert snap.state["world"] == {"temp": 42}
        assert ctx.get("world") == {"temp": 42}

    def test_context_applies_delete_signal(self) -> None:
        ctx = ContextNode()
        ctx.set("old_key", "some_value")
        ctx.update([_state_sig("old_key", delete=True)])
        assert ctx.get("old_key") is None

    def test_normal_signals_not_affected(self) -> None:
        ctx = ContextNode()
        snap = ctx.update([_sig("temperature", 99.0)])
        assert any(s.kind == "temperature" for s in snap.signals)

    def test_state_update_applied_before_snapshot_built(self) -> None:
        """Instincts should see the updated state in the same tick."""
        ctx = ContextNode()
        snap = ctx.update([_state_sig("mode", state_value="active")])
        assert snap.state.get("mode") == "active"


# ══════════════════════════════════════════════════════════════════════════════
# Gap 4 — LLMInstinctNode.setup() preloads provider
# ══════════════════════════════════════════════════════════════════════════════

class TestLLMInstinctNodeSetupPreload:
    @pytest.mark.asyncio
    async def test_setup_calls_preload_on_provider(self) -> None:
        mock_provider = MagicMock()
        mock_provider.preload = MagicMock()
        node = LLMTestInstinct(bus=_bus(), provider=mock_provider)
        await node.setup()
        mock_provider.preload.assert_called_once()

    @pytest.mark.asyncio
    async def test_setup_works_when_provider_has_no_preload(self) -> None:
        mock_provider = MagicMock(spec=[])   # spec=[] → no attributes
        mock_provider.complete = MagicMock(return_value=None)
        node = LLMTestInstinct(bus=_bus(), provider=mock_provider)
        await node.setup()   # must not raise

    @pytest.mark.asyncio
    async def test_setup_skips_preload_when_no_provider(self) -> None:
        node = LLMTestInstinct(bus=_bus())
        # No provider — default AnthropicProvider is built lazily in _call_llm_sync
        await node.setup()   # must not raise


# ══════════════════════════════════════════════════════════════════════════════
# Gap 5 — trigger_interval_s
# ══════════════════════════════════════════════════════════════════════════════

class TestTriggerIntervalS:
    @pytest.mark.asyncio
    async def test_evaluate_skipped_before_interval_elapses(self) -> None:
        SlowInstinct.call_count = 0
        im = InstinctMasterNode(bus=_bus())
        im.register(SlowInstinct(bus=_bus()))
        # First call should fire
        await im.evaluate_all(_ctx())
        assert SlowInstinct.call_count == 1
        # Immediate second call should be skipped (60 s interval hasn't elapsed)
        await im.evaluate_all(_ctx())
        assert SlowInstinct.call_count == 1

    @pytest.mark.asyncio
    async def test_evaluate_fires_again_after_interval(self) -> None:
        FastIntervalInstinct.call_count = 0
        im = InstinctMasterNode(bus=_bus())
        node = FastIntervalInstinct(bus=_bus())
        im.register(node)
        await im.evaluate_all(_ctx())
        assert FastIntervalInstinct.call_count == 1
        await asyncio.sleep(0.02)    # let interval expire
        await im.evaluate_all(_ctx())
        assert FastIntervalInstinct.call_count == 2

    @pytest.mark.asyncio
    async def test_none_interval_fires_every_tick(self) -> None:
        """Default trigger_interval_s=None → node fires on every evaluate_all."""
        call_count = 0

        class EveryTickInstinct(BaseInstinctNode):
            node_id  = "EveryTickInstinct"
            priority = 50

            async def evaluate(self, ctx: Context) -> Proposal | None:
                nonlocal call_count
                call_count += 1
                return None

        im = InstinctMasterNode(bus=_bus())
        im.register(EveryTickInstinct(bus=_bus()))
        for _ in range(5):
            await im.evaluate_all(_ctx())
        assert call_count == 5

    @pytest.mark.asyncio
    async def test_trigger_interval_does_not_affect_reflexes(self) -> None:
        """trigger_interval_s is not applied to reflex nodes."""
        from arachnite.nodes.instinct import BaseReflexInstinctNode

        call_count = 0

        class SlowReflex(BaseReflexInstinctNode):
            node_id            = "SlowReflex"
            priority           = 200
            trigger_interval_s: float | None = 60.0

            async def evaluate(self, ctx: Context) -> Proposal | None:
                nonlocal call_count
                call_count += 1
                return None

        im = InstinctMasterNode(bus=_bus())
        im.register(SlowReflex(bus=_bus()))
        # Reflexes go through evaluate_reflexes(), not evaluate_all()
        await im.evaluate_reflexes(_ctx())
        await im.evaluate_reflexes(_ctx())
        assert call_count == 2   # both calls fired — no throttling on reflex path


# ══════════════════════════════════════════════════════════════════════════════
# Gap 7 — Supervisor signals visible in Context
# ══════════════════════════════════════════════════════════════════════════════

class TestSupervisorSignalsInContext:
    @pytest.mark.asyncio
    async def test_supervisor_signal_appears_in_context(self) -> None:
        """When a supervisor emits a signal, it should appear in ctx.signals."""
        rt = _build_rt()
        node = DummySense(bus=rt.bus)
        await rt.register_sense_live(node)
        await rt.start()

        # Simulate a supervisor fault signal being published on the bus
        sv_sig = SupervisorSignal(
            source="supervisor_0", kind="supervisor", value="faulted",
            confidence=1.0, timestamp=time.monotonic(),
            node_id="DummySense", previous_state=NodeState.RUNNING,
            current_state=NodeState.FAULTED, restart_count=0,
        )
        await rt.bus.publish(sv_sig)

        # Capture the context from the next tick
        captured_ctx: list[Context] = []
        original_update = rt._context.update

        def capturing_update(signals, **kwargs):  # type: ignore[no-untyped-def]
            result = original_update(signals, **kwargs)
            captured_ctx.append(result)
            return result

        rt._context.update = capturing_update  # type: ignore[assignment]
        await rt.tick()

        assert len(captured_ctx) >= 1
        supervisor_signals = [
            s for s in captured_ctx[0].signals if s.kind == "supervisor"
        ]
        assert len(supervisor_signals) == 1
        assert supervisor_signals[0].node_id == "DummySense"  # type: ignore[attr-defined]
        await rt.stop()

    @pytest.mark.asyncio
    async def test_supervisor_buffer_drained_after_tick(self) -> None:
        """The buffer should be empty after tick() consumes its signals."""
        rt = _build_rt()
        await rt.start()

        sv_sig = SupervisorSignal(
            source="supervisor_0", kind="supervisor", value="faulted",
            confidence=1.0, timestamp=time.monotonic(),
            node_id="test_node", previous_state=NodeState.RUNNING,
            current_state=NodeState.FAULTED, restart_count=0,
        )
        await rt.bus.publish(sv_sig)
        assert len(rt._supervisor_signal_buffer) == 1

        await rt.tick()
        assert len(rt._supervisor_signal_buffer) == 0
        await rt.stop()

    @pytest.mark.asyncio
    async def test_no_supervisor_signals_when_buffer_empty(self) -> None:
        """When no supervisor signals were emitted, tick works normally."""
        rt = _build_rt()
        node = DummySense(bus=rt.bus)
        await rt.register_sense_live(node)
        await rt.start()
        # tick with no supervisor signals should not inject anything extra
        assert len(rt._supervisor_signal_buffer) == 0
        await rt.tick()
        assert len(rt._supervisor_signal_buffer) == 0
        await rt.stop()

    @pytest.mark.asyncio
    async def test_multiple_supervisor_signals_all_injected(self) -> None:
        """Multiple supervisor signals between ticks all appear in context."""
        rt = _build_rt()
        await rt.start()
        await rt.pause()  # prevent background loop from draining the buffer

        for i in range(3):
            sv_sig = SupervisorSignal(
                source="supervisor_0", kind="supervisor", value="faulted",
                confidence=1.0, timestamp=time.monotonic(),
                node_id=f"node_{i}", previous_state=NodeState.RUNNING,
                current_state=NodeState.FAULTED, restart_count=0,
            )
            await rt.bus.publish(sv_sig)

        assert len(rt._supervisor_signal_buffer) == 3
        await rt.resume()
        await rt.tick()
        assert len(rt._supervisor_signal_buffer) == 0
        await rt.stop()

    @pytest.mark.asyncio
    async def test_instinct_can_react_to_supervisor_signal(self) -> None:
        """An instinct node sees supervisor signals and can propose based on them."""
        seen_supervisor: list[bool] = []

        class FaultWatcherInstinct(BaseInstinctNode):
            node_id  = "FaultWatcher"
            priority = 50

            async def evaluate(self, ctx: Context) -> Proposal | None:
                has_fault = any(
                    s.kind == "supervisor" for s in ctx.signals
                )
                seen_supervisor.append(has_fault)
                return None

        bus = _bus()
        rt = _build_rt(bus)
        watcher = FaultWatcherInstinct(bus=bus)
        await rt.register_instinct_live(watcher)
        await rt.start()
        # Pause the background loop so it doesn't race the manual tick() calls
        # below — without this, on Python 3.10 the background tick can drain
        # the supervisor buffer before the manual tick observes the signal
        # (Bug B pattern from audit 2026-04-16).
        await rt.pause()

        # First tick — no supervisor signal
        await rt.tick()
        assert seen_supervisor[-1] is False

        # Publish a supervisor signal, then tick again
        sv_sig = SupervisorSignal(
            source="supervisor_0", kind="supervisor", value="dead",
            confidence=1.0, timestamp=time.monotonic(),
            node_id="some_node", previous_state=NodeState.FAULTED,
            current_state=NodeState.DEAD, restart_count=3,
        )
        await bus.publish(sv_sig)
        await rt.tick()
        assert seen_supervisor[-1] is True
        await rt.stop()

    @pytest.mark.asyncio
    async def test_reflex_can_react_to_supervisor_signal(self) -> None:
        """A reflex instinct sees supervisor signals in the same tick."""
        from arachnite.nodes.instinct import BaseReflexInstinctNode

        reflex_saw_fault: list[bool] = []

        class NodeFaultReflex(BaseReflexInstinctNode):
            node_id  = "NodeFaultReflex"
            priority = 200

            async def evaluate(self, ctx: Context) -> Proposal | None:
                has_fault = any(
                    s.kind == "supervisor" for s in ctx.signals
                )
                reflex_saw_fault.append(has_fault)
                return None

        bus = _bus()
        rt = _build_rt(bus)
        reflex = NodeFaultReflex(bus=bus)
        await rt.register_instinct_live(reflex)
        await rt.start()
        await rt.pause()  # prevent background loop from consuming the signal

        sv_sig = SupervisorSignal(
            source="supervisor_1", kind="supervisor", value="dead",
            confidence=1.0, timestamp=time.monotonic(),
            node_id="broken_node", previous_state=NodeState.FAULTED,
            current_state=NodeState.DEAD, restart_count=3,
        )
        await bus.publish(sv_sig)
        # Do NOT resume() before the manual tick — on Python 3.10 the
        # background loop can race the manual tick and drain the supervisor
        # buffer first (Bug B pattern). Manual tick() works while paused.
        await rt.tick()
        assert reflex_saw_fault[-1] is True
        await rt.stop()
