"""
arachnite.llm_provider
~~~~~~~~~~~~~~~~~~~~~~
Abstract LLMProvider and three concrete implementations:

  AnthropicProvider — Anthropic cloud API (requires: arachnite[llm])
  OllamaProvider    — local Ollama server, OpenAI-compat (requires: arachnite[ollama])
  LocalProvider     — embedded llama-cpp-python (requires: arachnite[local-llm])

Providers expose two completion surfaces:

  complete()       — synchronous, tool-calling; returns (tool_name, tool_args)
                     or None. Called via asyncio.to_thread() by LLMInstinctNode.
  complete_text()  — async, plain-text; returns the model's string response for
                     a single prompt. Each provider overrides with a direct
                     text path (the tool-calling `complete()` discards text
                     blocks, so it cannot be used as a fallback adapter).
"""

from __future__ import annotations

import asyncio
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

# ── Tool definition format (OpenAI-compatible, canonical) ─────────────────────
#
# Tools are passed to providers in OpenAI function-calling format:
#
#   {"type": "function", "function": {"name": ..., "description": ...,
#                                      "parameters": <JSON Schema>}}
#
# AnthropicProvider converts these to Anthropic tool format internally.
# OllamaProvider and LocalProvider use them as-is.

ToolList = list[dict[str, Any]]
ToolResult = tuple[str, dict[str, Any]]   # (function_name, function_args)


# ── Abstract base ──────────────────────────────────────────────────────────────

class LLMProvider(ABC):
    """
    Common interface for all LLM backends.

    Implementations must be safe to call from a background thread
    (asyncio.to_thread). They must not hold asyncio state.
    """

    @abstractmethod
    def complete(
        self,
        system: str,
        user: str,
        tools: ToolList,
    ) -> ToolResult | None:
        """
        Call the language model and return the first tool call it made,
        or None if the model chose no_action or produced no tool call.

        Runs synchronously — called via asyncio.to_thread() by LLMInstinctNode.

        Parameters
        ----------
        system:
            System prompt describing the agent's role and available actions.
        user:
            Serialised context snapshot (tick, signals, world state, etc.).
        tools:
            Tool definitions in OpenAI function-calling format.

        Returns
        -------
        (tool_name, tool_args) if the model called a tool, else None.
        """

    async def complete_text(
        self,
        prompt: str,
        *,
        system: str = "",
        max_tokens: int | None = None,
    ) -> str:
        """
        Async: return the model's plain-text response to *prompt*.

        Concrete providers override ``_complete_text_sync()`` — this method
        runs the sync helper on a worker thread via ``asyncio.to_thread``,
        preserving the "no asyncio state in providers" invariant.

        The tool-calling ``complete()`` path cannot be reused as a fallback:
        it returns only ``tool_use`` / ``tool_calls`` blocks and discards the
        assistant's text content. Each provider reads the text directly
        (Anthropic: ``TextBlock.text``; Ollama/Local: ``message.content``).

        Parameters
        ----------
        prompt:
            The user prompt. Sent as a single user-role message.
        system:
            Optional system prompt. Empty string → no system role.
        max_tokens:
            Override the provider instance's ``max_tokens`` for this call only.
            None → use the instance default.

        Returns
        -------
        The assistant's text response. Empty string if the model produced no
        text content.
        """
        return await asyncio.to_thread(
            self._complete_text_sync, prompt, system, max_tokens
        )

    def _complete_text_sync(
        self, prompt: str, system: str, max_tokens: int | None
    ) -> str:
        """
        Synchronous text-completion implementation. Providers MUST override.

        Called from ``complete_text()`` via ``asyncio.to_thread``; runs on a
        worker thread, so implementations may make blocking network or
        subprocess calls and must not hold asyncio state.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement _complete_text_sync(). "
            "Subclasses of LLMProvider must override _complete_text_sync() — "
            "the tool-calling complete() path discards assistant text and "
            "cannot be used as a fallback."
        )


# ── Anthropic ──────────────────────────────────────────────────────────────────

class AnthropicProvider(LLMProvider):
    """
    Cloud LLM via the Anthropic Messages API.

    Requires: pip install arachnite[llm]

    Parameters
    ----------
    model:
        Anthropic model ID, e.g. 'claude-haiku-4-5-20251001'.
    max_tokens:
        Maximum tokens in the model response.
    api_key:
        Anthropic API key. None → use ANTHROPIC_API_KEY env var.
    """

    def __init__(
        self,
        model: str = "claude-haiku-4-5-20251001",
        max_tokens: int = 256,
        api_key: str | None = None,
    ) -> None:
        self.model = model
        self.max_tokens = max_tokens
        self.api_key = api_key
        self._client: Any | None = None

    def complete(self, system: str, user: str, tools: ToolList) -> ToolResult | None:
        try:
            import anthropic  # lazy — optional dep
        except ImportError as exc:
            raise ImportError(
                "anthropic package required. Install with: pip install arachnite[llm]"
            ) from exc

        if self._client is None:
            self._client = anthropic.Anthropic(api_key=self.api_key)
        client = self._client
        anthropic_tools = [_openai_tool_to_anthropic(t) for t in tools]

        message = client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system,
            tools=anthropic_tools,  # type: ignore[arg-type,unused-ignore]
            messages=[{"role": "user", "content": user}],
        )

        for block in message.content:
            if getattr(block, "type", None) == "tool_use":
                # Newer Anthropic SDKs widen ``message.content`` to a union
                # of ~13 block types; only ``ToolUseBlock`` carries ``.name``
                # and ``.input``. We narrow at runtime via ``getattr`` rather
                # than ``isinstance`` so the code keeps working against older
                # SDKs that don't expose every block class.
                return block.name, block.input  # type: ignore[return-value,union-attr,unused-ignore]

        return None

    def _complete_text_sync(
        self, prompt: str, system: str, max_tokens: int | None
    ) -> str:
        try:
            import anthropic  # lazy — optional dep
        except ImportError as exc:
            raise ImportError(
                "anthropic package required. Install with: pip install arachnite[llm]"
            ) from exc

        if self._client is None:
            self._client = anthropic.Anthropic(api_key=self.api_key)
        client = self._client

        message = client.messages.create(
            model=self.model,
            max_tokens=max_tokens if max_tokens is not None else self.max_tokens,
            system=system,
            messages=[{"role": "user", "content": prompt}],
        )

        # Concatenate every text block; non-text blocks (e.g. tool_use) are
        # skipped. Runtime attribute check keeps this working against older
        # SDKs that expose fewer block types (see complete() above).
        parts: list[str] = []
        for block in message.content:
            if getattr(block, "type", None) == "text":
                parts.append(block.text)  # type: ignore[union-attr,unused-ignore]
        return "".join(parts)


# ── Ollama ─────────────────────────────────────────────────────────────────────

class OllamaProvider(LLMProvider):
    """
    Local Ollama server via its OpenAI-compatible REST API.

    Requires: pip install arachnite[ollama]  (installs openai client)
    Requires: Ollama running at base_url with the chosen model pulled.

    Parameters
    ----------
    model:
        Model tag as shown in `ollama list`, e.g. 'llama3.1', 'mistral'.
    base_url:
        Ollama server URL (default: http://localhost:11434/v1).
    max_tokens:
        Maximum tokens in the model response.
    """

    def __init__(
        self,
        model: str = "llama3.1",
        base_url: str = "http://localhost:11434/v1",
        max_tokens: int = 256,
    ) -> None:
        self.model = model
        self.base_url = base_url
        self.max_tokens = max_tokens

    def complete(self, system: str, user: str, tools: ToolList) -> ToolResult | None:
        try:
            import openai  # lazy — optional dep
        except ImportError as exc:
            raise ImportError(
                "openai package required for OllamaProvider. "
                "Install with: pip install arachnite[ollama]"
            ) from exc

        client = openai.OpenAI(base_url=self.base_url, api_key="ollama")
        # Newer ``openai`` releases ship strictly-typed message overloads (each
        # role has its own TypedDict). We pass plain dicts intentionally so the
        # call works against the wider Ollama-compat surface; silence the
        # corresponding ``call-overload`` error.
        response = client.chat.completions.create(  # type: ignore[call-overload,unused-ignore]
            model=self.model,
            max_tokens=self.max_tokens,
            tools=tools,  # type: ignore[arg-type,unused-ignore]
            tool_choice="auto",
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
        )

        message = response.choices[0].message
        if message.tool_calls:
            call = message.tool_calls[0]
            import json
            args = json.loads(call.function.arguments)
            return call.function.name, args

        return None

    def _complete_text_sync(
        self, prompt: str, system: str, max_tokens: int | None
    ) -> str:
        try:
            import openai  # lazy — optional dep
        except ImportError as exc:
            raise ImportError(
                "openai package required for OllamaProvider. "
                "Install with: pip install arachnite[ollama]"
            ) from exc

        client = openai.OpenAI(base_url=self.base_url, api_key="ollama")
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        response = client.chat.completions.create(  # type: ignore[call-overload,unused-ignore]
            model=self.model,
            max_tokens=max_tokens if max_tokens is not None else self.max_tokens,
            messages=messages,  # type: ignore[arg-type,unused-ignore]
        )
        content = response.choices[0].message.content
        return content or ""


# ── Local (llama-cpp-python) ───────────────────────────────────────────────────

class LocalProvider(LLMProvider):
    """
    Embedded inference via llama-cpp-python. No external server required.

    The model is loaded lazily on the first complete() call and cached
    for the lifetime of the provider. Call preload() from a node's
    setup() method to load eagerly and avoid first-call latency.

    Requires: pip install arachnite[local-llm]

    Parameters
    ----------
    model_path:
        Path to a GGUF model file.
    n_ctx:
        Context window size in tokens (default: 2048).
    n_gpu_layers:
        Number of model layers to offload to GPU. 0 = CPU-only.
        Set to -1 to offload all layers (requires CUDA/Metal build).
    max_tokens:
        Maximum tokens to generate per call.
    """

    def __init__(
        self,
        model_path: str,
        n_ctx: int = 2048,
        n_gpu_layers: int = 0,
        max_tokens: int = 256,
    ) -> None:
        self.model_path = model_path
        self.n_ctx = n_ctx
        self.n_gpu_layers = n_gpu_layers
        self.max_tokens = max_tokens
        self._llm: Any = None   # loaded lazily

    def preload(self) -> None:
        """
        Load the model into memory immediately.
        Call this from a node's setup() to avoid first-call latency.
        Safe to call multiple times — no-op after first load.
        """
        if self._llm is None:
            self._llm = self._load_model()

    def _load_model(self) -> Any:
        try:
            from llama_cpp import Llama  # lazy — optional dep  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "llama-cpp-python required for LocalProvider. "
                "Install with: pip install arachnite[local-llm]"
            ) from exc
        return Llama(
            model_path=self.model_path,
            n_ctx=self.n_ctx,
            n_gpu_layers=self.n_gpu_layers,
            verbose=False,
        )

    def complete(self, system: str, user: str, tools: ToolList) -> ToolResult | None:
        if self._llm is None:
            self._llm = self._load_model()

        import json  # noqa: PLC0415
        response = self._llm.create_chat_completion(
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            tools=tools,
            tool_choice="auto",
            max_tokens=self.max_tokens,
        )

        message = response["choices"][0]["message"]
        tool_calls = message.get("tool_calls") or []
        if tool_calls:
            call = tool_calls[0]
            args = json.loads(call["function"]["arguments"])
            return call["function"]["name"], args

        return None

    def _complete_text_sync(
        self, prompt: str, system: str, max_tokens: int | None
    ) -> str:
        if self._llm is None:
            self._llm = self._load_model()

        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        response = self._llm.create_chat_completion(
            messages=messages,
            max_tokens=max_tokens if max_tokens is not None else self.max_tokens,
        )
        content = response["choices"][0]["message"].get("content")
        return content or ""


# ── Thread-safe provider wrapper ──────────────────────────────────────────────

class ThreadSafeProvider(LLMProvider):
    """
    Wraps any LLMProvider with a threading.Lock to serialise complete() calls.

    llama-cpp-python (and many other local inference libraries) is not
    thread-safe.  When multiple LLMInstinctNode instances share the same
    LocalProvider, their concurrent asyncio.to_thread() calls would
    invoke complete() in parallel threads, corrupting model state.

    ThreadSafeProvider ensures only one thread enters complete() at a time.
    It also forwards preload() if the inner provider supports it.
    """

    def __init__(self, provider: LLMProvider) -> None:
        self._provider = provider
        self._lock = threading.Lock()

    def complete(
        self,
        system: str,
        user: str,
        tools: ToolList,
    ) -> ToolResult | None:
        with self._lock:
            return self._provider.complete(system, user, tools)

    def _complete_text_sync(
        self, prompt: str, system: str, max_tokens: int | None
    ) -> str:
        # Hold the same lock as complete() so concurrent text and tool-calling
        # paths are mutually serialised — critical for llama-cpp-python, which
        # is not thread-safe across either surface.
        with self._lock:
            return self._provider._complete_text_sync(prompt, system, max_tokens)

    def preload(self) -> None:
        """Forward preload() to the inner provider if supported."""
        if hasattr(self._provider, "preload"):
            self._provider.preload()  # type: ignore[attr-defined,unused-ignore]

    @property
    def inner(self) -> LLMProvider:
        """Access the wrapped provider for introspection."""
        return self._provider


# ── Shared model registry ────────────────────────────────────────────────────

class SharedModelRegistry:
    """
    Registry for sharing LLMProvider instances across multiple nodes.

    On memory-constrained devices (Jetson Nano, 4 GB RAM) loading the same
    GGUF model once per instinct node is not feasible.  SharedModelRegistry
    loads each model once, wraps it in a ThreadSafeProvider, and hands out
    the same instance to every node that requests it.

    Usage::

        registry = SharedModelRegistry()

        # All four instinct nodes share one model, one lock:
        provider = registry.get_or_create(
            "llama-8b",
            lambda: LocalProvider(model_path="/models/llama-8b.gguf"),
        )
        curiosity  = CuriosityInstinct(bus=bus, provider=provider)
        social     = SocialInstinct(bus=bus, provider=provider)
        reflection = ReflectionInstinct(bus=bus, provider=provider)
        goal       = GoalInstinct(bus=bus, provider=provider)

    The registry is not a singleton — create one per runtime and pass it
    around explicitly.
    """

    def __init__(self) -> None:
        self._providers: dict[str, ThreadSafeProvider] = {}
        self._lock = threading.Lock()

    def get_or_create(
        self,
        key: str,
        factory: Callable[[], LLMProvider],
    ) -> ThreadSafeProvider:
        """
        Return the shared provider for *key*, creating it on first access.

        The factory callable is invoked only once per key.  The resulting
        provider is wrapped in ThreadSafeProvider automatically.
        Thread-safe: concurrent calls for the same key will only invoke
        the factory once.
        """
        with self._lock:
            if key not in self._providers:
                self._providers[key] = ThreadSafeProvider(factory())
            return self._providers[key]

    def get(self, key: str) -> ThreadSafeProvider | None:
        """Return the provider for *key*, or None if not yet created."""
        return self._providers.get(key)

    def keys(self) -> list[str]:
        """Return all registered model keys."""
        return list(self._providers.keys())

    def preload_all(self) -> None:
        """Call preload() on every registered provider."""
        for provider in self._providers.values():
            provider.preload()


# ── Internal helpers ───────────────────────────────────────────────────────────

def _openai_tool_to_anthropic(tool: dict[str, Any]) -> dict[str, Any]:
    """Convert an OpenAI function-calling tool dict to Anthropic tool format."""
    fn = tool["function"]
    return {
        "name":         fn["name"],
        "description":  fn.get("description", ""),
        "input_schema": fn.get("parameters", {"type": "object", "properties": {}}),
    }
