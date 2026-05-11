"""
arachnite.context
~~~~~~~~~~~~~~~~~
ContextNode: the working memory of the agent.
Spec reference: Section 5.3.
"""

from __future__ import annotations

import json
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

from arachnite.exceptions import ContextError
from arachnite.models import (
    ActionExecutionState,
    Context,
    HistoryConfig,
    Result,
    Signal,
    StateUpdateSignal,
)


class ContextNode:
    """
    Assembles the Context snapshot each tick and maintains working state.

    Receives all signals from the current tick, merges them with rolling
    history, and exposes the last action result for instinct feedback.

    Spec reference: Section 5.3.
    """

    def __init__(
        self,
        history_length: int = 10,
        history_config: dict[str, HistoryConfig] | None = None,
        state_path: str | Path | None = None,
        flush_on_write: bool = False,
        max_state_keys: int | None = None,
    ) -> None:
        if max_state_keys is not None and max_state_keys < 1:
            raise ValueError("max_state_keys must be >= 1 or None")

        self.history_length = history_length
        self._history_config: dict[str, HistoryConfig] = history_config or {}
        self._state_path: Path | None = Path(state_path) if state_path else None
        self._flush_on_write = flush_on_write
        self._max_state_keys = max_state_keys

        self._tick:    int                     = 0
        self._history: deque[list[Signal]]     = deque(maxlen=history_length)
        self._state:   dict[str, Any]          = {}
        self._last_result: Result | None       = None
        self._action_state: ActionExecutionState | None = None
        self._last_results: list[Result]       = []
        self._action_states: list[ActionExecutionState] = []
        self._initialized: bool                = False

        # Load persisted state from disk if available
        if self._state_path and self._state_path.exists():
            try:
                self._state = json.loads(
                    self._state_path.read_text(encoding="utf-8")
                )
            except Exception:  # noqa: BLE001
                self._state = {}

        self._enforce_state_limit()

    def _enforce_state_limit(self) -> None:
        """Evict the oldest keys when state exceeds max_state_keys"""
        if self._max_state_keys is None:
            return
        while len(self._state) > self._max_state_keys:
            oldest_key = next(iter(self._state))
            del self._state[oldest_key]

    # ── Core update ───────────────────────────────────────────────────────────

    def update(
        self,
        signals: list[Signal],
        result:  Result | None = None,
        action_state: ActionExecutionState | None = None,
        results: list[Result] | None = None,
        action_states: list[ActionExecutionState] | None = None,
    ) -> Context:
        """
        Merge this tick's signals and last result into a new Context snapshot.
        Called once per tick by ArachniteRuntime before instinct evaluation.

        The plural parameters ``results`` and ``action_states`` support
        concurrent action execution.  The singular ``result`` and
        ``action_state`` remain for backward compatibility; when plural
        parameters are given, the singular fields are populated from the
        first (highest-priority) element.

        StateUpdateSignals are intercepted here and applied to _state before
        the snapshot is built, so instincts see the updated state immediately.
        """
        self._tick        += 1
        self._last_result  = result
        self._action_state = action_state
        self._last_results = results or []
        self._action_states = action_states or []

        # Populate singular from plural if singular not explicitly given
        if result is None and self._last_results:
            self._last_result = self._last_results[0]
        if action_state is None and self._action_states:
            self._action_state = self._action_states[0]

        # Apply any state-update signals before building the snapshot
        for sig in signals:
            if isinstance(sig, StateUpdateSignal):
                if sig.delete:
                    self._state.pop(sig.key, None)
                else:
                    self._state[sig.key] = sig.state_value
                    self._enforce_state_limit()

        # Append current tick signals to history
        self._history.append(signals)
        self._apply_history_config()
        self._initialized = True

        return self._build_context(signals)

    def snapshot(self) -> Context:
        """
        Return the most recently assembled Context without updating.
        Raises ContextError if called before the first update().
        """
        if not self._initialized:
            raise ContextError()
        return self._build_context(
            self._history[-1] if self._history else []
        )

    def _build_context(self, signals: list[Signal]) -> Context:
        # Snapshot the history so that later mutations (e.g. eviction in
        # _apply_history_config) do not affect concurrent consumers.
        history_snapshot: deque[list[Signal]] = deque(
            (list(tick_sigs) for tick_sigs in self._history),
            maxlen=self._history.maxlen,
        )
        return Context(
            tick          = self._tick,
            signals       = signals,
            history       = history_snapshot,
            state         = dict(self._state),
            last_result   = self._last_result,
            timestamp     = time.monotonic(),
            action_state  = self._action_state,
            last_results  = self._last_results,
            action_states = self._action_states,
        )

    def _apply_history_config(self) -> None:
        """Evict entries from history according to per-kind HistoryConfig rules."""
        if not self._history_config:
            return
        if not self._history:
            return
        now = time.monotonic()
        for kind, cfg in self._history_config.items():
            if (
                cfg.value_ttl_s is None
                and cfg.max_ticks is None
                and cfg.max_bytes is None
            ):
                continue
            self._apply_kind_config(kind, cfg, now)

    def _apply_kind_config(
        self, kind: str, cfg: HistoryConfig, now: float
    ) -> None:
        # Pass 1: TTL — walk once, mutate in place.
        # ``cfg.value_ttl_s`` truthiness guard preserves the legacy behavior that
        # ``value_ttl_s=0`` is treated as disabled (not "evict everything").
        if cfg.value_ttl_s:
            ttl = cfg.value_ttl_s
            for tick_signals in self._history:
                for i, sig in enumerate(tick_signals):
                    if sig.kind != kind:
                        continue
                    if (now - sig.timestamp) > ttl:
                        tick_signals[i] = Signal(
                            source=sig.source,
                            kind=sig.kind,
                            value=None,
                            confidence=0.0,
                            timestamp=sig.timestamp,
                            metadata={"evicted": True},
                        )

        # Passes 2 and 3 share an index of *live* (non-evicted) entries for this kind.
        if cfg.max_ticks is None and cfg.max_bytes is None:
            return

        index: list[tuple[int, int, Signal]] = [
            (t_idx, s_idx, sig)
            for t_idx, tick_signals in enumerate(self._history)
            for s_idx, sig in enumerate(tick_signals)
            if sig.kind == kind and sig.value is not None
        ]

        # Pass 2: max_ticks
        if cfg.max_ticks is not None and cfg.max_ticks >= 0:
            excess = len(index) - cfg.max_ticks
            if excess > 0:
                for t_idx, s_idx, old in index[:excess]:
                    self._history[t_idx][s_idx] = Signal(
                        source=old.source,
                        kind=old.kind,
                        value=None,
                        confidence=0.0,
                        timestamp=old.timestamp,
                        metadata={"evicted": True, "reason": "max_ticks"},
                    )
                index = index[excess:]

        # Pass 3: max_bytes
        if cfg.max_bytes is not None and index:
            total = sum(sys.getsizeof(sig.value) for _, _, sig in index)
            evicted = 0
            while total > cfg.max_bytes and evicted < len(index):
                t_idx, s_idx, sig = index[evicted]
                total -= sys.getsizeof(sig.value)
                self._history[t_idx][s_idx] = Signal(
                    source=sig.source,
                    kind=sig.kind,
                    value=None,
                    confidence=0.0,
                    timestamp=sig.timestamp,
                    metadata={"evicted": True, "reason": "max_bytes"},
                )
                evicted += 1

    # ── State access ──────────────────────────────────────────────────────────

    def get(self, key: str, default: Any = None) -> Any:
        """Read a named value from the persistent state dict."""
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Write a named value to the persistent state dict."""
        self._state[key] = value
        self._enforce_state_limit()
        if self._flush_on_write:
            self.flush_state()

    def delete(self, key: str) -> None:
        """Remove a key from the persistent state dict."""
        self._state.pop(key, None)
        if self._flush_on_write:
            self.flush_state()

    def flush_state(self) -> None:
        """
        Write _state to the configured state_path as JSON.
        No-op if state_path was not set at construction time.
        Values that are not JSON-serialisable are converted to strings.
        """
        if self._state_path is None:
            return
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            self._state_path.write_text(
                json.dumps(self._state, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception:  # noqa: BLE001
            pass  # best-effort; do not crash the agent on I/O errors

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def tick(self) -> int:
        return self._tick

    @property
    def last_result(self) -> Result | None:
        return self._last_result

    def __repr__(self) -> str:
        parts = [
            f"tick={self._tick}",
            f"history_length={self.history_length}",
            f"state_keys={list(self._state)}",
        ]
        if self._max_state_keys is not None:
            parts.append(f"max_state_keys={self._max_state_keys}")
        return f"ContextNode({', '.join(parts)})"
