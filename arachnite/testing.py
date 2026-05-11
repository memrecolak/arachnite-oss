"""
arachnite.testing
~~~~~~~~~~~~~~~~~
Lightweight helpers for unit-testing nodes in isolation.

Provides factory functions for creating Signal, Proposal, Result, and Context
objects with sensible defaults, plus a MockBus that records all published
signals for assertions.

Spec reference: Section 5 (pipeline models).
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any

from arachnite.bus import SignalBus
from arachnite.models import (
    ActionExecutionState,
    Context,
    Proposal,
    Result,
    Signal,
)

# ── Factory functions ────────────────────────────────────────────────────────


def make_signal(
    kind: str = "test",
    value: Any = 0.0,
    source: str = "TestSense",
    confidence: float = 1.0,
    timestamp: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> Signal:
    """Create a Signal with sensible defaults"""
    return Signal(
        source=source,
        kind=kind,
        value=value,
        confidence=confidence,
        timestamp=timestamp if timestamp is not None else time.monotonic(),
        metadata=metadata if metadata is not None else {},
    )


def make_proposal(
    action_id: str = "TestAction",
    instinct_id: str = "TestInstinct",
    priority: int = 50,
    urgency: float = 0.5,
    parameters: dict[str, Any] | None = None,
    rationale: str = "",
    persist: bool = False,
) -> Proposal:
    """Create a Proposal with sensible defaults"""
    return Proposal(
        instinct_id=instinct_id,
        action_id=action_id,
        priority=priority,
        urgency=urgency,
        parameters=parameters if parameters is not None else {},
        rationale=rationale,
        persist=persist,
    )


def make_result(
    action_id: str = "TestAction",
    success: bool = True,
    error: BaseException | None = None,
    output: Any = None,
) -> Result:
    """Create a Result with sensible defaults"""
    return Result(
        action_id=action_id,
        success=success,
        error=error,
        output=output,
    )


def make_context(
    tick: int = 1,
    signals: list[Signal] | None = None,
    state: dict[str, Any] | None = None,
    last_result: Result | None = None,
    last_results: list[Result] | None = None,
    action_state: ActionExecutionState | None = None,
    action_states: list[ActionExecutionState] | None = None,
    history: deque[list[Signal]] | None = None,
) -> Context:
    """Create a Context snapshot with sensible defaults"""
    return Context(
        tick=tick,
        signals=signals if signals is not None else [],
        history=history if history is not None else deque(),
        state=state if state is not None else {},
        last_result=last_result,
        timestamp=time.monotonic(),
        action_state=action_state,
        last_results=last_results if last_results is not None else [],
        action_states=action_states if action_states is not None else [],
    )


# ── MockBus ──────────────────────────────────────────────────────────────────


class MockBus(SignalBus):
    """A SignalBus that records all published signals for assertions"""

    def __init__(self) -> None:
        super().__init__()
        self._published: list[Signal] = []

    async def publish(self, signal: Signal) -> None:
        """Record the signal, then delegate to the real bus."""
        self._published.append(signal)
        await super().publish(signal)

    @property
    def published(self) -> list[Signal]:
        """All signals published to this bus"""
        return list(self._published)

    def published_of_kind(self, kind: str) -> list[Signal]:
        """Filter published signals by kind"""
        return [s for s in self._published if s.kind == kind]

    def clear(self) -> None:
        """Reset recorded signals and subscribers"""
        self._published.clear()
        super().clear()
