"""Unit tests for LLMInstinctNode."""

from __future__ import annotations

import asyncio
import math
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import arachnite
from arachnite import SignalBus
from arachnite.llm_provider import LLMProvider
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


async def _settle(node: LLMInstinctNode) -> None:
    """
    Wait for the node's in-flight background LLM call. Leaving a
    ``patch.dict(sys.modules, ...)`` block while that thread still runs lets
    it import the real ``anthropic`` package later, during another test.
    """
    if node._pending is not None:
        await node._pending


def _stale_proposal(node: LLMInstinctNode) -> Proposal:
    """A proposal standing in for an earlier, now outdated LLM decision."""
    return Proposal(
        instinct_id=node.node_id, action_id="ShutdownAction",
        priority=node.priority, urgency=0.9, rationale="stale",
    )


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
            await _settle(node)  # no fixed sleep: slow hosts take longer
            result = await node.evaluate(_ctx())
            await _settle(node)

        assert result is not None
        assert result.action_id == "CoolDownAction"
        assert result.urgency == 0.8
        assert result.rationale == "Temperature is high"
        assert result.priority == node.priority
        assert result.instinct_id == node.node_id

    @pytest.mark.asyncio
    async def test_no_action_tool_returns_none(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        node._cached_proposal = _stale_proposal(node)  # must be replaced, not kept
        fake_response = _mock_anthropic_response(
            _tool_use_block("no_action", {"reason": "Temperature is normal"})
        )
        with patch.dict(sys.modules, {"anthropic": _make_mock_anthropic(fake_response)}):
            await node.evaluate(_ctx())
            await _settle(node)
            result = await node.evaluate(_ctx())
            await _settle(node)
            # The parse path itself answers None (it does not raise).
            direct = node._call_llm_sync(_ctx())

        assert result is None
        assert direct is None

    @pytest.mark.asyncio
    async def test_unknown_action_id_ignored(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        node._cached_proposal = _stale_proposal(node)  # must be replaced, not kept
        fake_response = _mock_anthropic_response(
            _tool_use_block("propose_action", {
                "action_id": "NonExistentAction",
                "urgency": 0.5,
                "rationale": "test",
            })
        )
        with patch.dict(sys.modules, {"anthropic": _make_mock_anthropic(fake_response)}):
            await node.evaluate(_ctx())
            await _settle(node)
            result = await node.evaluate(_ctx())
            await _settle(node)
            # The parse path itself rejects the action (it does not raise).
            direct = node._call_llm_sync(_ctx())

        assert result is None
        assert direct is None

    @pytest.mark.asyncio
    async def test_text_only_response_returns_none(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        node._cached_proposal = _stale_proposal(node)  # must be replaced, not kept
        fake_response = _mock_anthropic_response(
            _text_block("I think no action is needed.")
        )
        with patch.dict(sys.modules, {"anthropic": _make_mock_anthropic(fake_response)}):
            await node.evaluate(_ctx())
            await _settle(node)
            result = await node.evaluate(_ctx())
            await _settle(node)
            direct = node._call_llm_sync(_ctx())

        assert result is None
        assert direct is None

    @pytest.mark.asyncio
    async def test_llm_exception_does_not_propagate(self) -> None:
        node = ThermalLLMInstinct(bus=_bus())
        broken = MagicMock()
        broken.messages.create.side_effect = RuntimeError("API error")
        mock_mod = ModuleType("anthropic")
        mock_mod.Anthropic = MagicMock(return_value=broken)  # type: ignore[attr-defined]
        node._cached_proposal = _stale_proposal(node)
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            first = await node.evaluate(_ctx())
            await _settle(node)  # the failing call must not raise out of the task
            result = await node.evaluate(_ctx())
            await _settle(node)

        assert first is not None  # the cache is served until the call completes
        assert result is None     # a failed call clears it (never re-proposed)


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
            await _settle(node)  # let the background task complete

            # After background task completes, the new proposal is visible.
            # Still patched: this evaluate() starts another call, which must
            # not reach the real provider (its thread would outlive the test).
            final_result = await node.evaluate(_ctx())
            await _settle(node)
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


class _ScriptedProvider(LLMProvider):
    """Answers each call with the next scripted step (the last one repeats)."""

    def __init__(self, script: list[Any]) -> None:
        self.script = script
        self.calls = 0

    def complete(self, system: str, user: str, tools: Any) -> Any:
        self.calls += 1
        step = self.script[min(self.calls - 1, len(self.script) - 1)]
        if isinstance(step, BaseException):
            raise step
        return step


class _BlockingProvider(LLMProvider):
    """A hung HTTP call: blocks until released (or 10 s)."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.calls = 0

    def complete(self, system: str, user: str, tools: Any) -> Any:
        self.calls += 1
        self.release.wait(10.0)
        return None


_HUNG_EXIT_SCRIPT = """
import asyncio, time
from collections import deque
import arachnite
from arachnite import SignalBus
from arachnite.llm_provider import LLMProvider
from arachnite.models import Context
from arachnite.nodes.llm import LLMInstinctNode

class Hung(LLMProvider):
    def complete(self, system, user, tools):
        time.sleep(30.0)
        return None

class Brain(LLMInstinctNode):
    node_id = "Brain"
    min_interval_s = 0.0
    def available_actions(self):
        return {"Noop": "do nothing"}

async def main():
    node = Brain(bus=SignalBus(), provider=Hung())
    ctx = Context(tick=1, signals=[], history=deque(), state={},
                  last_result=None, timestamp=0.0)
    await node.evaluate(ctx)
    await asyncio.sleep(0.2)
    await node.cancel_background_tasks()  # what master teardown does

asyncio.run(main())
print(arachnite.__file__)
"""


_GOOD_CALL = (
    "propose_action",
    {"action_id": "CoolDownAction", "urgency": 0.9, "rationale": "hot"},
)


class TestReview6LLMInstinct:
    """R6-F16: failed, invalid or hung LLM calls never leave a stale proposal."""

    @pytest.mark.parametrize("failure", [
        ConnectionError("network down"),
        ("propose_action", {"action_id": "CoolDownAction", "urgency": 1.5, "rationale": "x"}),
        ("propose_action", {"action_id": "CoolDownAction", "urgency": "high", "rationale": "x"}),
        ("propose_action", {"action_id": "CoolDownAction", "urgency": math.nan}),
        ("propose_action", ["CoolDownAction"]),
        ("propose_action", {"action_id": "CoolDownAction", "urgency": 0.5, "parameters": [1]}),
    ], ids=["raises", "urgency>1", "urgency-str", "urgency-nan", "args-list", "params-list"])
    @pytest.mark.asyncio
    async def test_failed_call_clears_cached_proposal(self, failure: Any) -> None:
        """R6-F16: after a failing/invalid call the last proposal is not re-proposed"""
        provider = _ScriptedProvider([_GOOD_CALL, failure])
        node = ThermalLLMInstinct(bus=_bus(), provider=provider)
        seen = []
        for _ in range(4):
            proposal = await node.evaluate(_ctx())
            seen.append(None if proposal is None else proposal.action_id)
            await _settle(node)
        assert provider.calls == 4
        assert seen == [None, "CoolDownAction", None, None]

    @pytest.mark.asyncio
    async def test_hung_call_times_out_and_clears_cache(self) -> None:
        """R6-F16: a call exceeding call_timeout_s is abandoned and clears the cache"""

        class Timed(ThermalLLMInstinct):
            call_timeout_s = 0.05

        provider = _BlockingProvider()
        node = Timed(bus=_bus(), provider=provider)
        node._cached_proposal = _stale_proposal(node)
        try:
            await node.evaluate(_ctx())
            await asyncio.wait_for(_settle(node), 5.0)
            assert await node.evaluate(_ctx()) is None
            # The abandoned call still runs: no second call meanwhile, and its
            # thread is a daemon, so it never holds up interpreter exit.
            assert provider.calls == 1
            assert node._call_thread is not None and node._call_thread.daemon
        finally:
            provider.release.set()

    def test_hung_call_does_not_delay_process_exit(self) -> None:
        """R6-F16: asyncio.run() returns promptly after a node with a hung call shuts down"""
        script = _HUNG_EXIT_SCRIPT
        root = Path(arachnite.__file__).resolve().parents[1]  # the package under test
        started = time.monotonic()
        proc = subprocess.run(
            [sys.executable, "-c", script], cwd=root, capture_output=True,
            text=True, timeout=120,
        )
        elapsed = time.monotonic() - started
        assert proc.returncode == 0, proc.stderr
        assert proc.stdout.strip() == str(root / "arachnite" / "__init__.py")
        # The provider hangs for 30 s; interpreter start-up is a few seconds
        # even on a Raspberry Pi.
        assert elapsed < 20.0
