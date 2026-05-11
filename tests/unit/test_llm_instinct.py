"""Unit tests for LLMInstinctNode."""

from __future__ import annotations

import asyncio
import sys
import time
from collections import deque
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from arachnite import SignalBus
from arachnite.models import Context, Proposal, Signal
from arachnite.nodes.llm import LLMInstinctNode

# ── Inject a fake 'anthropic' module so tests run without the real package ─────

def _make_mock_anthropic(response: MagicMock) -> ModuleType:
    """Return a fake anthropic module whose Anthropic().messages.create() returns response."""
    mod = ModuleType("anthropic")
    client = MagicMock()
    client.messages.create.return_value = response
    mod.Anthropic = MagicMock(return_value=client)  # type: ignore[attr-defined]
    return mod


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ctx(signals: list[Signal] | None = None, tick: int = 1) -> Context:
    return Context(
        tick=tick,
        signals=signals or [],
        history=deque(),
        state={},
        last_result=None,
        timestamp=time.monotonic(),
    )


def _bus() -> SignalBus:
    return SignalBus()


def _tool_use_block(name: str, inp: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(type="tool_use", name=name, input=inp)


def _text_block(text: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=text)


def _mock_anthropic_response(*blocks: SimpleNamespace) -> MagicMock:
    msg = MagicMock()
    msg.content = list(blocks)
    return msg


# ── Concrete test node ─────────────────────────────────────────────────────────

class ThermalLLMInstinct(LLMInstinctNode):
    node_id          = "ThermalLLMInstinct"
    priority         = 70
    min_interval_s   = 0.0   # no cooldown in tests

    def available_actions(self) -> dict[str, str]:
        return {
            "CoolDownAction": "Activate cooling system",
            "ShutdownAction": "Safely shut down the device",
        }


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestLLMInstinctNodeProposal:
    @pytest.mark.asyncio
    async def test_returns_none_before_first_llm_call_completes(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        # Inject a slow LLM call that won't finish before we check
        async def _slow(_ctx: Context) -> None:
            await asyncio.sleep(10)

        with patch.object(node, "_call_llm", _slow):
            result = await node.evaluate(_ctx())
        assert result is None

    @pytest.mark.asyncio
    async def test_cached_proposal_returned_after_llm_call(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        fake_response = _mock_anthropic_response(
            _tool_use_block("propose_action", {
                "action_id": "CoolDownAction",
                "urgency": 0.8,
                "rationale": "Temperature is high",
            })
        )
        with patch.dict(sys.modules, {"anthropic": _make_mock_anthropic(fake_response)}):
            await node.evaluate(_ctx())
            await asyncio.sleep(0.05)
            result = await node.evaluate(_ctx())

        assert result is not None
        assert result.action_id == "CoolDownAction"
        assert result.urgency == 0.8
        assert result.rationale == "Temperature is high"
        assert result.priority == node.priority
        assert result.instinct_id == node.node_id

    @pytest.mark.asyncio
    async def test_no_action_tool_returns_none(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        fake_response = _mock_anthropic_response(
            _tool_use_block("no_action", {"reason": "Temperature is normal"})
        )
        with patch.dict(sys.modules, {"anthropic": _make_mock_anthropic(fake_response)}):
            await node.evaluate(_ctx())
            await asyncio.sleep(0.05)
            result = await node.evaluate(_ctx())

        assert result is None

    @pytest.mark.asyncio
    async def test_unknown_action_id_ignored(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        fake_response = _mock_anthropic_response(
            _tool_use_block("propose_action", {
                "action_id": "NonExistentAction",
                "urgency": 0.5,
                "rationale": "test",
            })
        )
        with patch.dict(sys.modules, {"anthropic": _make_mock_anthropic(fake_response)}):
            await node.evaluate(_ctx())
            await asyncio.sleep(0.05)
            result = await node.evaluate(_ctx())

        assert result is None

    @pytest.mark.asyncio
    async def test_text_only_response_returns_none(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        fake_response = _mock_anthropic_response(
            _text_block("I think no action is needed.")
        )
        with patch.dict(sys.modules, {"anthropic": _make_mock_anthropic(fake_response)}):
            await node.evaluate(_ctx())
            await asyncio.sleep(0.05)
            result = await node.evaluate(_ctx())

        assert result is None

    @pytest.mark.asyncio
    async def test_llm_exception_does_not_propagate(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        broken = MagicMock()
        broken.messages.create.side_effect = RuntimeError("API error")
        mock_mod = ModuleType("anthropic")
        mock_mod.Anthropic = MagicMock(return_value=broken)  # type: ignore[attr-defined]
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            result = await node.evaluate(_ctx())
            await asyncio.sleep(0.05)

        assert result is None   # no exception raised, cached stays None


class TestLLMInstinctNodeCooldown:
    @pytest.mark.asyncio
    async def test_no_second_call_during_cooldown(self) -> None:
        class CooldownNode(ThermalLLMInstinct):
            min_interval_s = 60.0   # very long cooldown

        node = CooldownNode(bus=_bus())
        call_count = 0

        async def _counting_call(ctx: Context) -> None:
            nonlocal call_count
            call_count += 1

        with patch.object(node, "_call_llm", _counting_call):
            await node.evaluate(_ctx())
            await node.evaluate(_ctx())   # should be skipped — cooldown active
            await asyncio.sleep(0.05)

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_new_call_fires_after_cooldown(self) -> None:
        class FastCooldownNode(ThermalLLMInstinct):
            min_interval_s = 0.01

        node = FastCooldownNode(bus=_bus())
        call_count = 0

        async def _counting_call(ctx: Context) -> None:
            nonlocal call_count
            call_count += 1

        with patch.object(node, "_call_llm", _counting_call):
            await node.evaluate(_ctx())
            await asyncio.sleep(0.02)    # let cooldown expire
            await node.evaluate(_ctx())
            await asyncio.sleep(0.02)

        assert call_count == 2


class TestLLMInstinctNodeMissingDep:
    @pytest.mark.asyncio
    async def test_missing_anthropic_raises_import_error(self) -> None:
        # No provider injected → falls back to AnthropicProvider which lazy-imports anthropic
        node = ThermalLLMInstinct(bus=_bus())
        with (
            patch.dict("sys.modules", {"anthropic": None}),
            pytest.raises(ImportError, match="anthropic"),
        ):
            node._call_llm_sync(_ctx())

    @pytest.mark.asyncio
    async def test_injected_provider_used_instead_of_default(self) -> None:
        from arachnite.llm_provider import OllamaProvider
        mock_provider = MagicMock(spec=OllamaProvider)
        mock_provider.complete.return_value = (
            "propose_action",
            {"action_id": "CoolDownAction", "urgency": 0.7, "rationale": "test"},
        )
        node = ThermalLLMInstinct(bus=_bus(), provider=mock_provider)
        result = node._call_llm_sync(_ctx())
        assert result is not None
        assert result.action_id == "CoolDownAction"
        mock_provider.complete.assert_called_once()


class TestContextToText:
    def test_no_signals(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        text = node.context_to_text(_ctx())
        assert "No signals" in text

    def test_signals_included(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        signals = [Signal(
            source="TempSense", kind="temperature",
            value=72.0, confidence=0.99, timestamp=time.monotonic(),
        )]
        text = node.context_to_text(_ctx(signals=signals))
        assert "temperature" in text
        assert "72.0" in text

    def test_state_included(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        ctx = _ctx()
        ctx.state["world_model"] = {"temp": 42, "faces": 1}
        ctx.state["mode"] = "active"
        text = node.context_to_text(ctx)
        assert "Agent state:" in text
        assert "world_model" in text
        assert "mode" in text
        assert "active" in text

    def test_empty_state_omitted(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        text = node.context_to_text(_ctx())
        assert "Agent state" not in text


class TestLLMInstinctNodeLock:
    """Tests for the asyncio.Lock guarding _cached_proposal (TOCTOU fix)."""

    def test_lock_exists_and_is_asyncio_lock(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        assert hasattr(node, "_lock")
        assert isinstance(node._lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_concurrent_evaluate_and_call_llm_is_safe(self) -> None:
        """Simulate a background LLM call in flight while evaluate() runs repeatedly.

        Before the background call completes, evaluate() must consistently
        return the old cached value (None). After it completes, evaluate()
        must return the new proposal.
        """
        node = ThermalLLMInstinct(bus=_bus())
        new_proposal = Proposal(
            instinct_id=node.node_id,
            action_id="CoolDownAction",
            priority=node.priority,
            urgency=0.9,
        )

        call_started = asyncio.Event()
        release_call = asyncio.Event()

        async def _delayed_call_llm_sync(ctx: Context) -> Proposal | None:
            call_started.set()
            await release_call.wait()
            return new_proposal

        # Patch _call_llm_sync (the sync part run in to_thread) with an async
        # version injected via patching _call_llm itself to skip to_thread.
        async def _patched_call_llm(ctx: Context) -> None:
            try:
                proposal = await _delayed_call_llm_sync(ctx)
                async with node._lock:
                    node._cached_proposal = proposal
            except Exception:
                pass

        with patch.object(node, "_call_llm", _patched_call_llm):
            # First evaluate fires the background call
            result1 = await node.evaluate(_ctx())
            assert result1 is None  # no cached proposal yet

            # Wait for the background call to start
            await asyncio.sleep(0.01)
            await call_started.wait()

            # While background call is in-flight, evaluate returns old value
            for _ in range(5):
                mid_result = await node.evaluate(_ctx())
                assert mid_result is None

            # Release the background call
            release_call.set()
            await asyncio.sleep(0.05)  # let the background task complete

        # After background task completes, the new proposal is visible
        final_result = await node.evaluate(_ctx())
        assert final_result is not None
        assert final_result.action_id == "CoolDownAction"
        assert final_result.urgency == 0.9

    @pytest.mark.asyncio
    async def test_call_llm_acquires_lock_for_write(self) -> None:
        """Verify that _call_llm writes _cached_proposal under the lock."""
        node = ThermalLLMInstinct(bus=_bus())
        new_proposal = Proposal(
            instinct_id=node.node_id,
            action_id="ShutdownAction",
            priority=node.priority,
            urgency=0.7,
        )

        lock_was_held = False

        original_lock_acquire = node._lock.acquire

        async def _spy_acquire() -> bool:
            nonlocal lock_was_held
            result = await original_lock_acquire()
            lock_was_held = True
            return result

        # Mock _call_llm_sync to return a known proposal
        with (
            patch.object(node, "_call_llm_sync", return_value=new_proposal),
            patch.object(node._lock, "acquire", _spy_acquire),
        ):
            await node._call_llm(_ctx())

        assert lock_was_held, "_call_llm must acquire the lock when writing _cached_proposal"
        assert node._cached_proposal is new_proposal
