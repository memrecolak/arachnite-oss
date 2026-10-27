"""
arachnite.nodes.llm
~~~~~~~~~~~~~~~~~~~
LLMInstinctNode — a BaseInstinctNode backed by a language model.

The LLM is called in the background so it never blocks the tick loop.
The most recently completed Proposal is cached and returned on each tick
until a newer call completes. A call that fails, times out
(``call_timeout_s``) or returns an invalid answer clears the cache, so a
stale decision is never re-proposed.

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
import contextlib
import math
import sys
import threading
import time
from abc import abstractmethod
from collections.abc import Callable
from typing import Any, TypeVar

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from arachnite.llm_provider import AnthropicProvider, LLMProvider
from arachnite.models import Context, Proposal
from arachnite.nodes.instinct import BaseInstinctNode

_R = TypeVar("_R")


def _run_in_daemon_thread(
    fn: Callable[[], _R], name: str,
) -> tuple[asyncio.Future[_R], threading.Thread]:
    """
    Run ``fn`` in a new daemon thread; return a future for its result and the thread.

    Unlike ``asyncio.to_thread`` (the loop's default executor), an abandoned
    call — timed out or cancelled at shutdown — does not hold up
    ``asyncio.run()`` or interpreter exit: the daemon thread is simply left to
    finish, and its result is dropped.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future[_R] = loop.create_future()

    def _settle(result: Any, error: BaseException | None) -> None:
        if future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(result)

    def _runner() -> None:
        try:
            result: Any = fn()
            error: BaseException | None = None
        except BaseException as exc:  # noqa: BLE001 — handed to the awaiting task
            result, error = None, exc
        # A closed loop raises RuntimeError: nobody is waiting any more.
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(_settle, result, error)

    thread = threading.Thread(target=_runner, name=name, daemon=True)
    thread.start()
    return future, thread


class LLMInstinctNode(BaseInstinctNode):
    """
    An InstinctNode that delegates evaluation to a language model.

    The LLM is invoked in a background daemon thread so the tick loop is
    never blocked. The last completed Proposal is cached and returned on
    each subsequent tick until a new LLM call finishes. A call that raises,
    exceeds ``call_timeout_s`` or answers with an invalid proposal clears
    the cache (the node proposes nothing until a call succeeds again). At
    most one provider call is in flight per node: a timed-out call's thread
    is abandoned (it never delays shutdown), and no new call starts until it
    has returned, so a provider is never called concurrently by one node.

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
    call_timeout_s  : seconds to wait for one LLM call; None = no bound (default: 30.0)
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

    #: Seconds to wait for one LLM call before abandoning it and clearing
    #: the cached proposal. ``None`` waits indefinitely (not recommended:
    #: provider SDKs default to a 600 s read timeout).
    call_timeout_s: float | None = 30.0

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
        # Thread of the latest call, and of a call abandoned on timeout or
        # cancel (still running in the provider) — no new call starts
        # until the abandoned one has returned.
        self._call_thread: threading.Thread | None = None
        self._abandoned_call: threading.Thread | None = None
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
        and no call is currently in flight (including an abandoned,
        timed-out call whose thread has not returned yet).
        """
        now = time.monotonic()
        cooldown_elapsed = (now - self._last_call_time) >= self.min_interval_s
        abandoned = self._abandoned_call
        no_pending = (self._pending is None or self._pending.done()) and (
            abandoned is None or not abandoned.is_alive()
        )

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
        """
        Dispatch the LLM call in a daemon thread; update the cached proposal.

        On failure or timeout the cached proposal is cleared: a decision made
        on older observations is never re-proposed while the LLM is failing.
        """
        call = self._call_llm_sync
        future, thread = _run_in_daemon_thread(
            lambda: call(ctx), name=f"{self.node_id}_llm_call",
        )
        self._call_thread = thread
        try:
            proposal = await asyncio.wait_for(future, self.call_timeout_s)
        except asyncio.CancelledError:
            self._abandoned_call = thread  # teardown: leave the thread behind
            raise
        except asyncio.TimeoutError:
            # asyncio.TimeoutError is the builtin TimeoutError only on 3.11+.
            self._abandoned_call = thread
            await self._clear_cached()
            self.logger.error("LLM call timed out", timeout_s=self.call_timeout_s)
            return
        except Exception as exc:  # noqa: BLE001
            await self._clear_cached()
            self.logger.error("LLM call failed", error=str(exc), error_type=type(exc).__name__)
            return
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

    async def _clear_cached(self) -> None:
        async with self._lock:
            self._cached_proposal = None

    def _call_llm_sync(self, ctx: Context) -> Proposal | None:
        """
        Synchronous LLM call — runs in the node's call thread.
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
        """
        Convert a (tool_name, tool_args) pair into a Proposal or None.

        A malformed ``propose_action`` (arguments not an object, unknown
        action, urgency missing a finite number in [0, 1], parameters not an
        object) is rejected with a warning and yields None — never a raise,
        never a clamped guess.
        """
        if tool_name == "no_action":
            return None

        if tool_name == "propose_action":
            if not isinstance(tool_args, dict):
                self.logger.warning(
                    "LLM tool arguments are not an object — ignoring",
                    args_type=type(tool_args).__name__,
                )
                return None
            action_id = tool_args.get("action_id", "")
            if not isinstance(action_id, str) or action_id not in self.available_actions():
                self.logger.warning(
                    "LLM proposed unknown action — ignoring",
                    action_id=action_id,
                )
                return None
            raw_urgency = tool_args.get("urgency", 0.5)
            try:
                urgency = math.nan if isinstance(raw_urgency, bool) else float(raw_urgency)
            except (TypeError, ValueError):
                urgency = math.nan
            if not 0.0 <= urgency <= 1.0:  # also rejects NaN
                self.logger.warning(
                    "LLM proposed invalid urgency — ignoring",
                    action_id=action_id,
                    urgency=raw_urgency,
                )
                return None
            parameters = tool_args.get("parameters") or {}
            if not isinstance(parameters, dict):
                self.logger.warning(
                    "LLM proposed non-object parameters — ignoring",
                    action_id=action_id,
                )
                return None
            rationale = tool_args.get("rationale", "")
            return Proposal(
                instinct_id=self.node_id,
                action_id=action_id,
                priority=self.priority,
                urgency=urgency,
                parameters=parameters,
                rationale=rationale if isinstance(rationale, str) else str(rationale),
            )

        self.logger.warning("LLM called unknown tool — ignoring", tool_name=tool_name)
        return None
