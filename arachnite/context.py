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

_MISSING = object()


def _differs(old: Any, new: Any) -> bool:
    """True unless ``old`` and ``new`` compare equal (an uncomparable pair differs)."""
    if old is new:
        return False
    try:
        return not bool(old == new)
    except Exception:  # noqa: BLE001 — e.g. numpy arrays: ambiguous truth value
        return True


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
        # The signals list of the latest update(), exactly as handed out in
        # that tick's ctx.signals (history eviction never touches it).
        self._last_signals: list[Signal]       = []

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
        first element (the runtime passes results in completion order, so
        ``last_result`` is the first-completed result, spec §7.2.1).

        StateUpdateSignals are intercepted here and applied to _state before
        the snapshot is built, so instincts see the updated state immediately.
        With ``flush_on_write`` a StateUpdateSignal that changed the state is
        persisted here, as ``set()`` / ``delete()`` would.

        ``ctx.signals`` is ``signals`` itself, never modified: HistoryConfig
        eviction applies to history only and copies a tick's list before
        replacing an entry in it, so neither the caller's list nor a Context
        handed out earlier is rewritten by a later tick.
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
        state_changed = False
        for sig in signals:
            if isinstance(sig, StateUpdateSignal):
                if sig.delete:
                    if sig.key in self._state:
                        del self._state[sig.key]
                        state_changed = True
                else:
                    if self._flush_on_write and not state_changed:
                        state_changed = _differs(
                            self._state.get(sig.key, _MISSING), sig.state_value,
                        )
                    self._state[sig.key] = sig.state_value
                    self._enforce_state_limit()
        if state_changed and self._flush_on_write:
            self.flush_state()

        # Append current tick signals to history. The list is shared with
        # ctx.signals; eviction copies it before writing (copy on evict).
        self._history.append(signals)
        self._last_signals = signals
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
        return self._build_context(self._last_signals)

    def reflex_view(self, signals: list[Signal]) -> Context:
        """
        A lightweight Context for the event-driven reflex lane (ADR 0005):
        the given ``signals``, the current state and action/result fields,
        and an **empty** history — no per-event history copy. Never raises
        (usable before the first update()) and does not mutate the node.
        """
        return Context(
            tick          = self._tick,
            signals       = signals,
            history       = deque(maxlen=0),
            state         = dict(self._state),
            last_result   = self._last_result,
            timestamp     = time.monotonic(),
            action_state  = self._action_state,
            last_results  = self._last_results,
            action_states = self._action_states,
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
        # Indices of history entries already copied during this call: each
        # tick's list is copied at most once, on its first eviction.
        copied: set[int] = set()
        for kind, cfg in self._history_config.items():
            if (
                cfg.value_ttl_s is None
                and cfg.max_ticks is None
                and cfg.max_bytes is None
            ):
                continue
            self._apply_kind_config(kind, cfg, now, copied)

    def _evict(self, t_idx: int, s_idx: int, replacement: Signal, copied: set[int]) -> None:
        """
        Replace one history entry, copying that tick's list first (copy on
        evict). The list stored by update() is the one handed out as that
        tick's ctx.signals (and the caller's own list), so it is never
        written to in place.
        """
        if t_idx not in copied:
            self._history[t_idx] = list(self._history[t_idx])
            copied.add(t_idx)
        self._history[t_idx][s_idx] = replacement

    def _apply_kind_config(
        self, kind: str, cfg: HistoryConfig, now: float, copied: set[int],
    ) -> None:
        # Pass 1: TTL — walk once, evict expired entries (copy on evict).
        # ``cfg.value_ttl_s`` truthiness guard preserves the legacy behavior that
        # ``value_ttl_s=0`` is treated as disabled (not "evict everything").
        if cfg.value_ttl_s:
            ttl = cfg.value_ttl_s
            for t_idx, tick_signals in enumerate(list(self._history)):
                for i, sig in enumerate(tick_signals):
                    if sig.kind != kind:
                        continue
                    if sig.value is None and (sig.metadata or {}).get("evicted"):
                        continue  # already evicted: keep it (and its reason) as is
                    if (now - sig.timestamp) > ttl:
                        self._evict(t_idx, i, Signal(
                            source=sig.source,
                            kind=sig.kind,
                            value=None,
                            confidence=0.0,
                            timestamp=sig.timestamp,
                            metadata={"evicted": True},
                        ), copied)

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
                    self._evict(t_idx, s_idx, Signal(
                        source=old.source,
                        kind=old.kind,
                        value=None,
                        confidence=0.0,
                        timestamp=old.timestamp,
                        metadata={"evicted": True, "reason": "max_ticks"},
                    ), copied)
                index = index[excess:]

        # Pass 3: max_bytes
        if cfg.max_bytes is not None and index:
            total = sum(sys.getsizeof(sig.value) for _, _, sig in index)
            evicted = 0
            while total > cfg.max_bytes and evicted < len(index):
                t_idx, s_idx, sig = index[evicted]
                total -= sys.getsizeof(sig.value)
                self._evict(t_idx, s_idx, Signal(
                    source=sig.source,
                    kind=sig.kind,
                    value=None,
                    confidence=0.0,
                    timestamp=sig.timestamp,
                    metadata={"evicted": True, "reason": "max_bytes"},
                ), copied)
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
