"""
arachnite.nodes.llm
~~~~~~~~~~~~~~~~~~~
LLMInstinctNode — a BaseInstinctNode backed by a language model.

The LLM is called in the background so it never blocks the tick loop.
The most recently completed Proposal is cached and returned on each tick
until a newer one arrives.

Provider selection
------------------
Pass a provider instance to the constructor for full control::

    from arachnite.llm_provider import OllamaProvider
    node = MyInstinct(bus=bus, provider=OllamaProvider(model="llama3.1"))

If no provider is given the node falls back to AnthropicProvider using
the class-level ``model`` and ``api_key`` attributes (original behaviour).

Spec reference: Section 5.4 (BaseInstinctNode extension pattern).
"""

from __future__ import annotations

import asyncio
import sys
import time
from abc import abstractmethod
from typing import Any

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from arachnite.llm_provider import AnthropicProvider, LLMProvider
from arachnite.models import Context, Proposal
from arachnite.nodes.instinct import BaseInstinctNode


class LLMInstinctNode(BaseInstinctNode):
    """
    An InstinctNode that delegates evaluation to a language model.

    The LLM is invoked in a background thread via asyncio.to_thread() so
    the tick loop is never blocked. The last completed Proposal is cached
    and returned on each subsequent tick until a new LLM call finishes.

    Subclass contract
    -----------------
    - Set ``node_id`` and ``priority`` as usual.
    - Implement ``available_actions()`` — return {action_id: description}
      for every action this instinct may propose.
    - Optionally override ``system_prompt()`` to describe the agent's
      purpose and environment.
    - Optionally override ``context_to_text()`` to customise how the
      current Context is presented to the LLM.

    Class-level configuration (used when no provider is injected)
    -------------------------------------------------------------
    model           : Anthropic model ID (default: claude-haiku-4-5-20251001)
    max_tokens      : max tokens in the LLM response (default: 256)
    min_interval_s  : minimum seconds between consecutive LLM calls (default: 10.0)
    api_key         : Anthropic API key; None → use ANTHROPIC_API_KEY env var

    Not suitable as a reflex
    ------------------------
    Do **not** subclass this as a ``BaseReflexInstinctNode``. LLM inference
    latency (typically 100ms to seconds) is unbounded and dwarfs the reflex
    latency budget. Reflex nodes must be classical, deterministic logic with
    statically bounded execution time; the bounded reflex-latency guarantee
    relies on this. Use an LLM as a normal deliberative instinct and route
    safety-critical responses through a dedicated classical reflex.
    """

    #: Fallback Anthropic model when no provider is injected.
    model: str = "claude-haiku-4-5-20251001"

    #: Fallback max tokens when no provider is injected.
    max_tokens: int = 256

    #: Minimum seconds between consecutive LLM calls.
    min_interval_s: float = 10.0

    #: Fallback Anthropic API key when no provider is injected.
    api_key: str | None = None

    def __init__(
        self,
        provider: LLMProvider | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._provider = provider
        self._fallback_provider: AnthropicProvider | None = None
        self._cached_proposal: Proposal | None = None
        self._lock = asyncio.Lock()
        self._pending: asyncio.Task[None] | None = None
        self._last_call_time: float = 0.0

    @override
    async def setup(self) -> None:
        """
        Call preload() on the injected provider if it supports eager loading.
        This avoids first-call model-load latency for LocalProvider.
        """
        await super().setup()
        if self._provider is not None and hasattr(self._provider, "preload"):
            await asyncio.to_thread(self._provider.preload)

    # ── Abstract interface ─────────────────────────────────────────────────────

    @abstractmethod
    def available_actions(self) -> dict[str, str]:
        """
        Return {action_id: human-readable description} for every action
        this instinct may propose.

        Example::

            return {
                "CoolDownAction": "Activate cooling when temperature is high",
                "ShutdownAction": "Safely shut down the device",
            }
        """

    # ── Overridable hooks ──────────────────────────────────────────────────────

    def system_prompt(self) -> str:
        """
        System prompt sent to the LLM on every call.
        Override to add agent-specific goals, constraints, or domain knowledge.
        """
        actions = "\n".join(
            f"  - {aid}: {desc}"
            for aid, desc in self.available_actions().items()
        )
        return (
            "You are an instinct node in a reactive agent framework. "
            "Each tick you receive the agent's current sensor readings and must "
            "decide whether to propose an action.\n\n"
            f"Available actions:\n{actions}\n\n"
            "Use the propose_action tool if an action is warranted, "
            "or the no_action tool if no intervention is needed."
        )

    def context_to_text(self, ctx: Context) -> str:
        """
        Serialise the Context to plain text for the LLM user message.
        Override for domain-specific formatting.
        """
        lines: list[str] = [f"Tick: {ctx.tick}"]

        if ctx.signals:
            lines.append("Current signals:")
            for s in ctx.signals:
                lines.append(
                    f"  {s.kind} from {s.source}: "
                    f"value={s.value}, confidence={s.confidence:.2f}"
                )
        else:
            lines.append("No signals this tick.")

        if ctx.state:
            lines.append("Agent state:")
            for key, val in ctx.state.items():
                lines.append(f"  {key}: {val}")

        # Show all running actions (concurrent dispatch)
        if ctx.action_states:
            lines.append("Currently executing:")
            for astate in ctx.action_states:
                if astate.action_id:
                    lines.append(
                        f"  {astate.action_id} "
                        f"(step: {astate.current_step}, "
                        f"interruptible: {astate.interruptible})"
                    )
        elif ctx.action_state and ctx.action_state.action_id:
            lines.append(
                f"Currently executing: {ctx.action_state.action_id} "
                f"(step: {ctx.action_state.current_step}, "
                f"interruptible: {ctx.action_state.interruptible})"
            )

        # Show all recent results (concurrent dispatch)
        if ctx.last_results:
            lines.append("Recent results:")
            for r in ctx.last_results:
                outcome = "success" if r.success else "failed"
                lines.append(f"  {r.action_id} ({outcome})")
        elif ctx.last_result:
            outcome = "success" if ctx.last_result.success else "failed"
            lines.append(f"Last action: {ctx.last_result.action_id} ({outcome})")

        return "\n".join(lines)

    # ── Core evaluate ──────────────────────────────────────────────────────────

    @override
    async def evaluate(self, ctx: Context) -> Proposal | None:
        """
        Return the cached Proposal from the last completed LLM call.
        Fires a new background LLM call when the cooldown has elapsed
        and no call is currently in flight.
        """
        now = time.monotonic()
        cooldown_elapsed = (now - self._last_call_time) >= self.min_interval_s
        no_pending = self._pending is None or self._pending.done()

        if cooldown_elapsed and no_pending:
            self._last_call_time = now
            self._pending = self.spawn_background_task(
                self._call_llm(ctx),
                name=f"{self.node_id}_llm_call",
            )

        async with self._lock:
            return self._cached_proposal

    # ── Internal LLM plumbing ──────────────────────────────────────────────────

    async def _call_llm(self, ctx: Context) -> None:
        """Dispatch the LLM call in a thread; update the cached proposal."""
        try:
            proposal = await asyncio.to_thread(self._call_llm_sync, ctx)
            async with self._lock:
                self._cached_proposal = proposal
            if proposal:
                self.logger.info(
                    "LLM proposed action",
                    action_id=proposal.action_id,
                    priority=proposal.priority,
                    rationale=proposal.rationale,
                )
            else:
                self.logger.debug("LLM returned no action")
        except Exception as exc:  # noqa: BLE001
            self.logger.error("LLM call failed", error=str(exc))

    def _call_llm_sync(self, ctx: Context) -> Proposal | None:
        """
        Synchronous LLM call — runs inside asyncio.to_thread().
        Delegates to the injected provider (or a default AnthropicProvider).
        """
        if self._provider is not None:
            provider = self._provider
        else:
            if self._fallback_provider is None:
                self._fallback_provider = AnthropicProvider(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    api_key=self.api_key,
                )
            provider = self._fallback_provider
        tools = self._build_tools()
        result = provider.complete(self.system_prompt(), self.context_to_text(ctx), tools)

        if result is None:
            return None

        tool_name, tool_args = result
        return self._parse_tool_result(tool_name, tool_args)

    def _build_tools(self) -> list[dict[str, Any]]:
        """Build the tool list in OpenAI function-calling format."""
        return [
            {
                "type": "function",
                "function": {
                    "name": "propose_action",
                    "description": "Propose an action for the agent to execute this tick",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "action_id": {
                                "type": "string",
                                "description": "ID of the action to execute",
                                "enum": list(self.available_actions().keys()),
                            },
                            "urgency": {
                                "type": "number",
                                "description": "Urgency score from 0.0 (low) to 1.0 (critical)",
                            },
                            "parameters": {
                                "type": "object",
                                "description": "Optional parameters forwarded to the action node",
                            },
                            "rationale": {
                                "type": "string",
                                "description": (
                                    "One-sentence explanation of why this action was chosen"
                                ),
                            },
                        },
                        "required": ["action_id", "urgency", "rationale"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "no_action",
                    "description": "Signal that no action is needed this tick",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "reason": {"type": "string", "description": "Brief explanation"},
                        },
                        "required": ["reason"],
                    },
                },
            },
        ]

    def _parse_tool_result(
        self, tool_name: str, tool_args: dict[str, Any]
    ) -> Proposal | None:
        """Convert a (tool_name, tool_args) pair into a Proposal or None."""
        if tool_name == "no_action":
            return None

        if tool_name == "propose_action":
            action_id = tool_args.get("action_id", "")
            if action_id not in self.available_actions():
                self.logger.warning(
                    "LLM proposed unknown action — ignoring",
                    action_id=action_id,
                )
                return None
            return Proposal(
                instinct_id=self.node_id,
                action_id=action_id,
                priority=self.priority,
                urgency=float(tool_args.get("urgency", 0.5)),
                parameters=tool_args.get("parameters") or {},
                rationale=tool_args.get("rationale", ""),
            )

        self.logger.warning("LLM called unknown tool — ignoring", tool_name=tool_name)
        return None
