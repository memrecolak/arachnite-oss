"""Unit tests for LLMProvider implementations."""

from __future__ import annotations

import json
import sys
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from arachnite.llm_provider import (
    AnthropicProvider,
    LLMProvider,
    LocalProvider,
    OllamaProvider,
    SharedModelRegistry,
    ThreadSafeProvider,
    ToolList,
    ToolResult,
    _openai_tool_to_anthropic,
)

# ── Shared helpers ─────────────────────────────────────────────────────────────

SAMPLE_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "propose_action",
            "description": "Propose an action",
            "parameters": {
                "type": "object",
                "properties": {
                    "action_id": {"type": "string", "enum": ["CoolDown"]},
                    "urgency":   {"type": "number"},
                    "rationale": {"type": "string"},
                },
                "required": ["action_id", "urgency", "rationale"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "no_action",
            "description": "No action needed",
            "parameters": {
                "type": "object",
                "properties": {"reason": {"type": "string"}},
                "required": ["reason"],
            },
        },
    },
]


# ── _openai_tool_to_anthropic ──────────────────────────────────────────────────

class TestOpenAIToAnthropicConversion:
    def test_converts_name_and_description(self) -> None:
        result = _openai_tool_to_anthropic(SAMPLE_TOOLS[0])
        assert result["name"] == "propose_action"
        assert result["description"] == "Propose an action"

    def test_parameters_become_input_schema(self) -> None:
        result = _openai_tool_to_anthropic(SAMPLE_TOOLS[0])
        assert "input_schema" in result
        assert result["input_schema"]["type"] == "object"

    def test_no_type_wrapper_in_output(self) -> None:
        result = _openai_tool_to_anthropic(SAMPLE_TOOLS[0])
        assert "type" not in result


# ── AnthropicProvider ──────────────────────────────────────────────────────────

def _make_anthropic_mod(tool_name: str, tool_input: dict[str, Any]) -> ModuleType:
    block = MagicMock()
    block.type  = "tool_use"
    block.name  = tool_name
    block.input = tool_input
    msg = MagicMock()
    msg.content = [block]
    client = MagicMock()
    client.messages.create.return_value = msg
    mod = ModuleType("anthropic")
    mod.Anthropic = MagicMock(return_value=client)  # type: ignore[attr-defined]
    return mod


class TestAnthropicProvider:
    def test_returns_tool_name_and_args(self) -> None:
        provider = AnthropicProvider(model="claude-haiku-4-5-20251001")
        expected_args = {"action_id": "CoolDown", "urgency": 0.8, "rationale": "hot"}
        mod = _make_anthropic_mod("propose_action", expected_args)
        with patch.dict(sys.modules, {"anthropic": mod}):
            result = provider.complete("system", "user", SAMPLE_TOOLS)
        assert result == ("propose_action", expected_args)

    def test_no_tool_call_returns_none(self) -> None:
        provider = AnthropicProvider()
        text_block = MagicMock()
        text_block.type = "text"
        msg = MagicMock()
        msg.content = [text_block]
        client = MagicMock()
        client.messages.create.return_value = msg
        mod = ModuleType("anthropic")
        mod.Anthropic = MagicMock(return_value=client)  # type: ignore[attr-defined]
        with patch.dict(sys.modules, {"anthropic": mod}):
            result = provider.complete("system", "user", SAMPLE_TOOLS)
        assert result is None

    def test_missing_anthropic_raises_import_error(self) -> None:
        provider = AnthropicProvider()
        with (
            patch.dict(sys.modules, {"anthropic": None}),
            pytest.raises(ImportError, match="anthropic"),
        ):
            provider.complete("s", "u", SAMPLE_TOOLS)


# ── OllamaProvider ─────────────────────────────────────────────────────────────

def _make_openai_mod(fn_name: str, fn_args: dict[str, Any]) -> ModuleType:
    tool_call = MagicMock()
    tool_call.function.name      = fn_name
    tool_call.function.arguments = json.dumps(fn_args)
    message = MagicMock()
    message.tool_calls = [tool_call]
    choice = MagicMock()
    choice.message = message
    response = MagicMock()
    response.choices = [choice]
    client = MagicMock()
    client.chat.completions.create.return_value = response
    mod = ModuleType("openai")
    mod.OpenAI = MagicMock(return_value=client)  # type: ignore[attr-defined]
    return mod


class TestOllamaProvider:
    def test_returns_tool_name_and_args(self) -> None:
        provider = OllamaProvider(model="llama3.1")
        expected_args = {"action_id": "CoolDown", "urgency": 0.9, "rationale": "temp"}
        mod = _make_openai_mod("propose_action", expected_args)
        with patch.dict(sys.modules, {"openai": mod}):
            result = provider.complete("system", "user", SAMPLE_TOOLS)
        assert result is not None
        assert result[0] == "propose_action"
        assert result[1]["action_id"] == "CoolDown"

    def test_no_tool_calls_returns_none(self) -> None:
        provider = OllamaProvider()
        message = MagicMock()
        message.tool_calls = []
        choice = MagicMock()
        choice.message = message
        response = MagicMock()
        response.choices = [choice]
        client = MagicMock()
        client.chat.completions.create.return_value = response
        mod = ModuleType("openai")
        mod.OpenAI = MagicMock(return_value=client)  # type: ignore[attr-defined]
        with patch.dict(sys.modules, {"openai": mod}):
            result = provider.complete("system", "user", SAMPLE_TOOLS)
        assert result is None

    def test_missing_openai_raises_import_error(self) -> None:
        provider = OllamaProvider()
        with (
            patch.dict(sys.modules, {"openai": None}),
            pytest.raises(ImportError, match="openai"),
        ):
            provider.complete("s", "u", SAMPLE_TOOLS)


# ── LocalProvider ──────────────────────────────────────────────────────────────

class TestLocalProvider:
    def test_preload_calls_load_model(self) -> None:
        provider = LocalProvider(model_path="/fake/model.gguf")
        mock_llm = MagicMock()
        with patch.object(provider, "_load_model", return_value=mock_llm):
            provider.preload()
        assert provider._llm is mock_llm

    def test_preload_is_idempotent(self) -> None:
        provider = LocalProvider(model_path="/fake/model.gguf")
        mock_llm = MagicMock()
        with patch.object(provider, "_load_model", return_value=mock_llm) as mock_load:
            provider.preload()
            provider.preload()
        mock_load.assert_called_once()

    def test_complete_returns_tool_result(self) -> None:
        provider = LocalProvider(model_path="/fake/model.gguf")
        provider._llm = MagicMock()
        provider._llm.create_chat_completion.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "no_action",
                            "arguments": json.dumps({"reason": "all good"}),
                        }
                    }]
                }
            }]
        }
        result = provider.complete("system", "user", SAMPLE_TOOLS)
        assert result == ("no_action", {"reason": "all good"})

    def test_no_tool_calls_returns_none(self) -> None:
        provider = LocalProvider(model_path="/fake/model.gguf")
        provider._llm = MagicMock()
        provider._llm.create_chat_completion.return_value = {
            "choices": [{"message": {"tool_calls": None}}]
        }
        result = provider.complete("system", "user", SAMPLE_TOOLS)
        assert result is None

    def test_missing_llama_cpp_raises_import_error(self) -> None:
        provider = LocalProvider(model_path="/fake/model.gguf")
        with (
            patch.dict(sys.modules, {"llama_cpp": None}),
            pytest.raises(ImportError, match="llama-cpp-python"),
        ):
            provider._load_model()


# ── ThreadSafeProvider ────────────────────────────────────────────────────────

class TestThreadSafeProvider:
    def test_delegates_complete_to_inner(self) -> None:
        inner = MagicMock(spec=["complete"])
        inner.complete.return_value = ("no_action", {"reason": "ok"})
        tsp = ThreadSafeProvider(inner)
        result = tsp.complete("sys", "usr", SAMPLE_TOOLS)
        inner.complete.assert_called_once_with("sys", "usr", SAMPLE_TOOLS)
        assert result == ("no_action", {"reason": "ok"})

    def test_returns_none_when_inner_returns_none(self) -> None:
        inner = MagicMock(spec=["complete"])
        inner.complete.return_value = None
        tsp = ThreadSafeProvider(inner)
        assert tsp.complete("s", "u", []) is None

    def test_forwards_preload(self) -> None:
        inner = MagicMock()
        inner.preload = MagicMock()
        tsp = ThreadSafeProvider(inner)
        tsp.preload()
        inner.preload.assert_called_once()

    def test_preload_safe_when_inner_has_no_preload(self) -> None:
        inner = MagicMock(spec=["complete"])  # no preload attribute
        tsp = ThreadSafeProvider(inner)
        tsp.preload()  # must not raise

    def test_inner_property(self) -> None:
        inner = MagicMock(spec=["complete"])
        tsp = ThreadSafeProvider(inner)
        assert tsp.inner is inner

    def test_serialises_concurrent_calls(self) -> None:
        """Verify that the lock prevents truly concurrent access."""
        import threading
        import time

        call_log: list[tuple[str, float]] = []
        lock_held = threading.Event()

        class SlowProvider:
            def complete(self, system: str, user: str, tools: list) -> None:  # type: ignore[type-arg]
                call_log.append(("enter", time.monotonic()))
                lock_held.set()
                time.sleep(0.05)
                call_log.append(("exit", time.monotonic()))
                return None

        tsp = ThreadSafeProvider(SlowProvider())  # type: ignore[arg-type]

        t1 = threading.Thread(target=tsp.complete, args=("s", "u", []))
        t2 = threading.Thread(target=tsp.complete, args=("s", "u", []))
        t1.start()
        lock_held.wait()  # ensure t1 is inside complete()
        t2.start()
        t1.join()
        t2.join()

        # t2 should not enter until t1 exits
        # call_log: [enter_t1, exit_t1, enter_t2, exit_t2]
        assert len(call_log) == 4
        assert call_log[0][0] == "enter"
        assert call_log[1][0] == "exit"
        assert call_log[2][0] == "enter"
        # t2 entered after t1 exited
        assert call_log[2][1] >= call_log[1][1]


# ── SharedModelRegistry ──────────────────────────────────────────────────────

class TestSharedModelRegistry:
    def test_get_or_create_returns_thread_safe_provider(self) -> None:
        registry = SharedModelRegistry()
        inner = MagicMock(spec=["complete"])
        provider = registry.get_or_create("model-a", lambda: inner)
        assert isinstance(provider, ThreadSafeProvider)
        assert provider.inner is inner

    def test_get_or_create_returns_same_instance(self) -> None:
        registry = SharedModelRegistry()
        inner = MagicMock(spec=["complete"])
        p1 = registry.get_or_create("model-a", lambda: inner)
        p2 = registry.get_or_create("model-a", lambda: MagicMock())
        assert p1 is p2

    def test_factory_called_only_once(self) -> None:
        registry = SharedModelRegistry()
        factory = MagicMock(return_value=MagicMock(spec=["complete"]))
        registry.get_or_create("model-a", factory)
        registry.get_or_create("model-a", factory)
        factory.assert_called_once()

    def test_different_keys_get_different_providers(self) -> None:
        registry = SharedModelRegistry()
        p1 = registry.get_or_create("a", lambda: MagicMock(spec=["complete"]))
        p2 = registry.get_or_create("b", lambda: MagicMock(spec=["complete"]))
        assert p1 is not p2

    def test_get_returns_none_for_unknown_key(self) -> None:
        registry = SharedModelRegistry()
        assert registry.get("nope") is None

    def test_get_returns_existing_provider(self) -> None:
        registry = SharedModelRegistry()
        p = registry.get_or_create("x", lambda: MagicMock(spec=["complete"]))
        assert registry.get("x") is p

    def test_keys_lists_registered_models(self) -> None:
        registry = SharedModelRegistry()
        registry.get_or_create("alpha", lambda: MagicMock(spec=["complete"]))
        registry.get_or_create("beta", lambda: MagicMock(spec=["complete"]))
        assert sorted(registry.keys()) == ["alpha", "beta"]

    def test_preload_all_calls_preload_on_each(self) -> None:
        registry = SharedModelRegistry()
        inner_a = MagicMock()
        inner_b = MagicMock()
        registry.get_or_create("a", lambda: inner_a)
        registry.get_or_create("b", lambda: inner_b)
        registry.preload_all()
        inner_a.preload.assert_called_once()
        inner_b.preload.assert_called_once()


# ── complete_text (base + providers) ───────────────────────────────────────────

def _make_anthropic_text_mod(text: str) -> ModuleType:
    block = MagicMock()
    block.type = "text"
    block.text = text
    msg = MagicMock()
    msg.content = [block]
    client = MagicMock()
    client.messages.create.return_value = msg
    mod = ModuleType("anthropic")
    mod.Anthropic = MagicMock(return_value=client)  # type: ignore[attr-defined]
    return mod


def _make_openai_text_mod(text: str | None) -> ModuleType:
    message = MagicMock()
    message.content = text
    message.tool_calls = None
    choice = MagicMock()
    choice.message = message
    response = MagicMock()
    response.choices = [choice]
    client = MagicMock()
    client.chat.completions.create.return_value = response
    mod = ModuleType("openai")
    mod.OpenAI = MagicMock(return_value=client)  # type: ignore[attr-defined]
    return mod


class TestCompleteTextBase:
    async def test_base_complete_text_raises_not_implemented(self) -> None:
        class _NoTextProvider(LLMProvider):
            def complete(self, system: str, user: str, tools: ToolList) -> ToolResult | None:
                return None

        provider = _NoTextProvider()
        with pytest.raises(NotImplementedError, match="_NoTextProvider"):
            await provider.complete_text("hello")


class TestAnthropicCompleteText:
    async def test_returns_concatenated_text_blocks(self) -> None:
        provider = AnthropicProvider(model="claude-haiku-4-5-20251001")
        mod = _make_anthropic_text_mod("Hello there.")
        with patch.dict(sys.modules, {"anthropic": mod}):
            result = await provider.complete_text("say hi")
        assert result == "Hello there."

    async def test_concatenates_multiple_text_blocks(self) -> None:
        provider = AnthropicProvider()
        b1, b2 = MagicMock(), MagicMock()
        b1.type, b1.text = "text", "Hello "
        b2.type, b2.text = "text", "world."
        msg = MagicMock()
        msg.content = [b1, b2]
        client = MagicMock()
        client.messages.create.return_value = msg
        mod = ModuleType("anthropic")
        mod.Anthropic = MagicMock(return_value=client)  # type: ignore[attr-defined]
        with patch.dict(sys.modules, {"anthropic": mod}):
            result = await provider.complete_text("x")
        assert result == "Hello world."

    async def test_skips_non_text_blocks(self) -> None:
        provider = AnthropicProvider()
        text_block, tool_block = MagicMock(), MagicMock()
        text_block.type, text_block.text = "text", "visible"
        tool_block.type = "tool_use"
        msg = MagicMock()
        msg.content = [tool_block, text_block]
        client = MagicMock()
        client.messages.create.return_value = msg
        mod = ModuleType("anthropic")
        mod.Anthropic = MagicMock(return_value=client)  # type: ignore[attr-defined]
        with patch.dict(sys.modules, {"anthropic": mod}):
            result = await provider.complete_text("x")
        assert result == "visible"

    async def test_no_text_returns_empty_string(self) -> None:
        provider = AnthropicProvider()
        msg = MagicMock()
        msg.content = []
        client = MagicMock()
        client.messages.create.return_value = msg
        mod = ModuleType("anthropic")
        mod.Anthropic = MagicMock(return_value=client)  # type: ignore[attr-defined]
        with patch.dict(sys.modules, {"anthropic": mod}):
            result = await provider.complete_text("x")
        assert result == ""

    async def test_max_tokens_override_passed_through(self) -> None:
        provider = AnthropicProvider(max_tokens=256)
        mod = _make_anthropic_text_mod("ok")
        with patch.dict(sys.modules, {"anthropic": mod}):
            await provider.complete_text("x", max_tokens=42)
        client = mod.Anthropic.return_value  # type: ignore[attr-defined]
        kwargs = client.messages.create.call_args.kwargs
        assert kwargs["max_tokens"] == 42

    async def test_max_tokens_default_uses_instance(self) -> None:
        provider = AnthropicProvider(max_tokens=256)
        mod = _make_anthropic_text_mod("ok")
        with patch.dict(sys.modules, {"anthropic": mod}):
            await provider.complete_text("x")
        client = mod.Anthropic.return_value  # type: ignore[attr-defined]
        assert client.messages.create.call_args.kwargs["max_tokens"] == 256

    async def test_system_prompt_passed_through(self) -> None:
        provider = AnthropicProvider()
        mod = _make_anthropic_text_mod("ok")
        with patch.dict(sys.modules, {"anthropic": mod}):
            await provider.complete_text("q", system="you are terse")
        client = mod.Anthropic.return_value  # type: ignore[attr-defined]
        assert client.messages.create.call_args.kwargs["system"] == "you are terse"

    async def test_missing_anthropic_raises_import_error(self) -> None:
        provider = AnthropicProvider()
        with (
            patch.dict(sys.modules, {"anthropic": None}),
            pytest.raises(ImportError, match="anthropic"),
        ):
            await provider.complete_text("x")


class TestOllamaCompleteText:
    async def test_returns_message_content(self) -> None:
        provider = OllamaProvider(model="llama3.1")
        mod = _make_openai_text_mod("hi from ollama")
        with patch.dict(sys.modules, {"openai": mod}):
            result = await provider.complete_text("prompt")
        assert result == "hi from ollama"

    async def test_none_content_returns_empty_string(self) -> None:
        provider = OllamaProvider()
        mod = _make_openai_text_mod(None)
        with patch.dict(sys.modules, {"openai": mod}):
            result = await provider.complete_text("prompt")
        assert result == ""

    async def test_system_prompt_prepended_only_when_set(self) -> None:
        provider = OllamaProvider()
        mod = _make_openai_text_mod("ok")
        with patch.dict(sys.modules, {"openai": mod}):
            await provider.complete_text("q", system="be brief")
        client = mod.OpenAI.return_value  # type: ignore[attr-defined]
        msgs = client.chat.completions.create.call_args.kwargs["messages"]
        assert msgs[0] == {"role": "system", "content": "be brief"}
        assert msgs[1] == {"role": "user", "content": "q"}

    async def test_no_system_means_user_only(self) -> None:
        provider = OllamaProvider()
        mod = _make_openai_text_mod("ok")
        with patch.dict(sys.modules, {"openai": mod}):
            await provider.complete_text("q")
        client = mod.OpenAI.return_value  # type: ignore[attr-defined]
        msgs = client.chat.completions.create.call_args.kwargs["messages"]
        assert len(msgs) == 1
        assert msgs[0] == {"role": "user", "content": "q"}

    async def test_max_tokens_override(self) -> None:
        provider = OllamaProvider(max_tokens=128)
        mod = _make_openai_text_mod("ok")
        with patch.dict(sys.modules, {"openai": mod}):
            await provider.complete_text("q", max_tokens=7)
        client = mod.OpenAI.return_value  # type: ignore[attr-defined]
        assert client.chat.completions.create.call_args.kwargs["max_tokens"] == 7

    async def test_missing_openai_raises_import_error(self) -> None:
        provider = OllamaProvider()
        with (
            patch.dict(sys.modules, {"openai": None}),
            pytest.raises(ImportError, match="openai"),
        ):
            await provider.complete_text("x")


class TestLocalCompleteText:
    async def test_returns_message_content(self) -> None:
        provider = LocalProvider(model_path="/fake/m.gguf")
        provider._llm = MagicMock()
        provider._llm.create_chat_completion.return_value = {
            "choices": [{"message": {"content": "local hello"}}]
        }
        result = await provider.complete_text("prompt")
        assert result == "local hello"

    async def test_missing_content_returns_empty_string(self) -> None:
        provider = LocalProvider(model_path="/fake/m.gguf")
        provider._llm = MagicMock()
        provider._llm.create_chat_completion.return_value = {
            "choices": [{"message": {}}]
        }
        result = await provider.complete_text("p")
        assert result == ""

    async def test_none_content_returns_empty_string(self) -> None:
        provider = LocalProvider(model_path="/fake/m.gguf")
        provider._llm = MagicMock()
        provider._llm.create_chat_completion.return_value = {
            "choices": [{"message": {"content": None}}]
        }
        result = await provider.complete_text("p")
        assert result == ""

    async def test_system_prompt_prepended(self) -> None:
        provider = LocalProvider(model_path="/fake/m.gguf")
        provider._llm = MagicMock()
        provider._llm.create_chat_completion.return_value = {
            "choices": [{"message": {"content": "ok"}}]
        }
        await provider.complete_text("q", system="sys")
        msgs = provider._llm.create_chat_completion.call_args.kwargs["messages"]
        assert msgs == [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "q"},
        ]

    async def test_lazy_load_happens_on_first_text_call(self) -> None:
        provider = LocalProvider(model_path="/fake/m.gguf")
        mock_llm = MagicMock()
        mock_llm.create_chat_completion.return_value = {
            "choices": [{"message": {"content": "x"}}]
        }
        with patch.object(provider, "_load_model", return_value=mock_llm) as load:
            await provider.complete_text("p")
            load.assert_called_once()
        assert provider._llm is mock_llm

    async def test_max_tokens_override(self) -> None:
        provider = LocalProvider(model_path="/fake/m.gguf", max_tokens=64)
        provider._llm = MagicMock()
        provider._llm.create_chat_completion.return_value = {
            "choices": [{"message": {"content": "x"}}]
        }
        await provider.complete_text("p", max_tokens=11)
        assert provider._llm.create_chat_completion.call_args.kwargs["max_tokens"] == 11


class TestThreadSafeCompleteText:
    async def test_delegates_to_inner_sync_helper(self) -> None:
        inner = MagicMock(spec=["complete", "_complete_text_sync"])
        inner._complete_text_sync.return_value = "inner result"
        tsp = ThreadSafeProvider(inner)  # type: ignore[arg-type]
        result = await tsp.complete_text("p", system="s", max_tokens=9)
        inner._complete_text_sync.assert_called_once_with("p", "s", 9)
        assert result == "inner result"

    def test_serialises_concurrent_text_and_tool_calls(self) -> None:
        """Both complete() and complete_text() share the same lock.

        Drives both surfaces via their sync entry points (``complete`` and
        ``_complete_text_sync``) — that is what the lock actually guards, and
        it keeps this test free of asyncio so it cannot leak event loops or
        self-pipe sockets into the pytest warning filter.
        """
        import threading
        import time

        call_log: list[tuple[str, float]] = []
        entered = threading.Event()

        class MixedSlowProvider:
            def complete(self, system: str, user: str, tools: ToolList) -> ToolResult | None:
                call_log.append(("tool-enter", time.monotonic()))
                entered.set()
                time.sleep(0.05)
                call_log.append(("tool-exit", time.monotonic()))
                return None

            def _complete_text_sync(
                self, prompt: str, system: str, max_tokens: int | None
            ) -> str:
                call_log.append(("text-enter", time.monotonic()))
                time.sleep(0.05)
                call_log.append(("text-exit", time.monotonic()))
                return "ok"

        tsp = ThreadSafeProvider(MixedSlowProvider())  # type: ignore[arg-type]

        t_tool = threading.Thread(target=tsp.complete, args=("s", "u", []))
        t_text = threading.Thread(
            target=tsp._complete_text_sync, args=("p", "", None)
        )
        t_tool.start()
        entered.wait()  # ensure tool-call is inside the lock
        t_text.start()
        t_tool.join()
        t_text.join()

        assert len(call_log) == 4
        # Text call must not enter until tool call exits.
        names = [name for name, _ in call_log]
        assert names == ["tool-enter", "tool-exit", "text-enter", "text-exit"]
        assert call_log[2][1] >= call_log[1][1]

