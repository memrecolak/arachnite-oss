"""Unit tests for ContextNode."""

from __future__ import annotations

import time

import pytest

from arachnite.context import ContextNode
from arachnite.exceptions import ContextError
from arachnite.models import (
    ActionExecutionState,
    HistoryConfig,
    Result,
    Signal,
    StateUpdateSignal,
)


def _sig(kind: str = "temperature", value: float = 42.0) -> Signal:
    return Signal(
        source="test", kind=kind, value=value,
        confidence=1.0, timestamp=time.monotonic(),
    )


def _result(success: bool = True) -> Result:
    return Result(action_id="Act", success=success)


class TestContextNodeUpdate:
    def test_update_increments_tick(self) -> None:
        ctx = ContextNode()
        ctx.update([_sig()])
        ctx.update([_sig()])
        assert ctx.tick == 2

    def test_update_returns_context_with_signals(self) -> None:
        ctx = ContextNode()
        snap = ctx.update([_sig(kind="thermal", value=55.0)])
        assert any(s.kind == "thermal" for s in snap.signals)

    def test_update_stores_last_result(self) -> None:
        ctx = ContextNode()
        r = _result()
        snap = ctx.update([_sig()], result=r)
        assert snap.last_result is r
        assert ctx.last_result is r

    def test_update_history_grows(self) -> None:
        ctx = ContextNode(history_length=5)
        for _ in range(3):
            ctx.update([_sig()])
        assert len(ctx._history) == 3

    def test_history_capped_at_length(self) -> None:
        ctx = ContextNode(history_length=3)
        for _ in range(10):
            ctx.update([_sig()])
        assert len(ctx._history) == 3


class TestContextNodeSnapshot:
    def test_snapshot_before_update_raises(self) -> None:
        ctx = ContextNode()
        with pytest.raises(ContextError):
            ctx.snapshot()

    def test_snapshot_after_update_returns_context(self) -> None:
        ctx = ContextNode()
        ctx.update([_sig(kind="x")])
        snap = ctx.snapshot()
        assert snap.tick == 1

    def test_snapshot_reflects_latest_tick(self) -> None:
        ctx = ContextNode()
        ctx.update([_sig()])
        ctx.update([_sig()])
        snap = ctx.snapshot()
        assert snap.tick == 2


class TestContextNodeState:
    def test_set_and_get(self) -> None:
        ctx = ContextNode()
        ctx.set("mode", "active")
        assert ctx.get("mode") == "active"

    def test_get_missing_returns_default(self) -> None:
        ctx = ContextNode()
        assert ctx.get("missing") is None
        assert ctx.get("missing", "fallback") == "fallback"

    def test_delete_removes_key(self) -> None:
        ctx = ContextNode()
        ctx.set("x", 1)
        ctx.delete("x")
        assert ctx.get("x") is None

    def test_delete_nonexistent_is_silent(self) -> None:
        ctx = ContextNode()
        ctx.delete("nonexistent")   # must not raise

    def test_state_visible_in_context_snapshot(self) -> None:
        ctx = ContextNode()
        ctx.set("goal", "cool")
        snap = ctx.update([])
        assert snap.state["goal"] == "cool"


class TestContextNodeHistoryConfig:
    def test_history_config_with_no_constraints_is_skipped(self) -> None:
        # HistoryConfig with neither value_ttl_s nor max_bytes → continue (line 99)
        cfg = {"temperature": HistoryConfig()}
        ctx = ContextNode(history_length=5, history_config=cfg)
        ctx.update([_sig(kind="temperature")])
        ctx.update([_sig(kind="other")])  # triggers _apply_history_config
        # No eviction should occur
        found = [
            s for tick in ctx._history for s in tick
            if s.kind == "temperature"
        ]
        assert all(s.value is not None for s in found)

    def test_ttl_evicts_old_signals(self) -> None:
        # Use ttl=1.0s and a signal timestamped at epoch 0 — guaranteed stale
        cfg = {"temperature": HistoryConfig(value_ttl_s=1.0)}
        ctx = ContextNode(history_length=5, history_config=cfg)
        old_sig = Signal(
            source="s", kind="temperature", value=99.0,
            confidence=1.0, timestamp=0.0,  # ancient timestamp
        )
        ctx.update([old_sig])
        ctx.update([_sig(kind="other")])   # triggers _apply_history_config
        # The old signal should be evicted (value set to None)
        for tick_sigs in ctx._history:
            for s in tick_sigs:
                if s.kind == "temperature":
                    assert s.value is None or s.metadata.get("evicted")

    def test_apply_noop_with_empty_history_config(self) -> None:
        """No history_config → _apply_history_config must be a safe no-op."""
        ctx = ContextNode(history_length=5, history_config={})
        for _ in range(3):
            ctx.update([_sig(kind="temperature")])
        all_sigs = [s for tick in ctx._history for s in tick]
        assert len(all_sigs) == 3
        assert all(s.value is not None for s in all_sigs)
        assert all(not s.metadata.get("evicted") for s in all_sigs)

    def test_ttl_zero_treated_as_disabled(self) -> None:
        """value_ttl_s=0 is the legacy "disabled" sentinel, not "evict all"."""
        cfg = {"temperature": HistoryConfig(value_ttl_s=0)}
        ctx = ContextNode(history_length=5, history_config=cfg)
        sig = _sig(kind="temperature", value=42.0)
        ctx.update([sig])
        time.sleep(0.01)
        ctx.update([_sig(kind="other")])  # triggers _apply_history_config
        temps = [
            s for tick in ctx._history for s in tick
            if s.kind == "temperature"
        ]
        assert len(temps) == 1
        assert temps[0].value == 42.0
        assert not temps[0].metadata.get("evicted")

    def test_max_ticks_and_max_bytes_together(self) -> None:
        """Shared index: max_bytes must exclude entries max_ticks just evicted.

        All five signals are delivered in a single tick so that
        ``_apply_history_config`` runs exactly once against the full set — this
        way ``max_ticks`` evicts first (oldest 2), then ``max_bytes`` trims
        further from the surviving 3.
        """
        import sys

        payload = "x"
        unit = sys.getsizeof(payload)
        byte_budget = unit * 2  # only ~2 payloads fit under the byte cap
        cfg = {
            "data": HistoryConfig(max_ticks=3, max_bytes=byte_budget),
        }
        ctx = ContextNode(history_length=10, history_config=cfg)

        batch = [_sig(kind="data", value=payload) for _ in range(5)]
        ctx.update(batch)

        data_sigs = [s for tick in ctx._history for s in tick if s.kind == "data"]
        live = [s for s in data_sigs if s.value is not None]
        max_ticks_ev = [
            s for s in data_sigs if s.metadata.get("reason") == "max_ticks"
        ]
        max_bytes_ev = [
            s for s in data_sigs if s.metadata.get("reason") == "max_bytes"
        ]

        # (a) at most max_ticks live signals remain
        assert len(live) <= 3
        # (b) live bytes under budget
        live_bytes = sum(sys.getsizeof(s.value) for s in live)
        assert live_bytes <= byte_budget
        # (c) the two oldest (of 5) were evicted by max_ticks; then max_bytes
        # trimmed additional ones from the remaining 3 to fit the budget.
        assert len(max_ticks_ev) == 2
        assert len(max_bytes_ev) >= 1
        # No signal should carry both reasons — max_bytes pass must skip
        # entries already evicted by max_ticks.
        for s in data_sigs:
            if s.value is None:
                reason = s.metadata.get("reason")
                assert reason in (None, "max_ticks", "max_bytes")

    def test_triple_rule_active_no_double_evict(self) -> None:
        """All three rules active: each evicted slot carries exactly one reason."""
        import sys

        payload = b"\x00" * 100
        # max_bytes budget: one payload's size — forces byte eviction on any
        # live entry that survives max_ticks.
        cfg = {
            "data": HistoryConfig(
                value_ttl_s=1.0,
                max_ticks=3,
                max_bytes=sys.getsizeof(payload),
            ),
        }
        ctx = ContextNode(history_length=10, history_config=cfg)

        # Two ancient signals (will be TTL-evicted).
        for _ in range(2):
            ctx.update([Signal(
                source="s", kind="data", value=payload,
                confidence=1.0, timestamp=0.0,
            )])
        # Five fresh signals — max_ticks=3 will trim the oldest 2, then
        # max_bytes (budget = 1 payload) forces more eviction.
        for _ in range(5):
            ctx.update([_sig(kind="data", value=payload)])

        data_sigs = [s for tick in ctx._history for s in tick if s.kind == "data"]

        # No non-evicted "data" signal should have eviction metadata.
        for s in data_sigs:
            if s.value is not None:
                assert not s.metadata.get("evicted")

        # Every evicted slot carries exactly one reason tag (TTL writes no
        # reason; max_ticks and max_bytes each write their own).
        for s in data_sigs:
            if s.value is None:
                reason = s.metadata.get("reason")
                # TTL-evicted signals have no reason; rule-evicted have exactly one.
                assert reason in (None, "max_ticks", "max_bytes")
                assert s.metadata.get("evicted") is True

        # Sanity: at least one TTL eviction occurred (old signals were stale).
        ttl_only = [
            s for s in data_sigs
            if s.value is None and s.metadata.get("reason") is None
        ]
        assert len(ttl_only) >= 1


class TestContextNodePluralFields:
    def test_plural_results_populates_singular(self) -> None:
        """When results is given but result is not, singular is set from first."""
        ctx = ContextNode()
        r1 = Result(action_id="A", success=True)
        r2 = Result(action_id="B", success=False)
        snap = ctx.update([], results=[r1, r2])
        assert snap.last_results == [r1, r2]
        assert snap.last_result is r1

    def test_plural_action_states_populates_singular(self) -> None:
        ctx = ContextNode()
        s1 = ActionExecutionState(
            action_id="A", current_step="s1",
            completed_steps=[], interruptible=True,
            mandatory_block_remaining_s=0.0,
        )
        snap = ctx.update([], action_states=[s1])
        assert snap.action_states == [s1]
        assert snap.action_state is s1

    def test_singular_result_takes_precedence(self) -> None:
        """When both singular and plural are given, singular wins."""
        ctx = ContextNode()
        r_singular = Result(action_id="Singular", success=True)
        r_plural   = Result(action_id="Plural", success=False)
        snap = ctx.update([], result=r_singular, results=[r_plural])
        assert snap.last_result is r_singular
        assert snap.last_results == [r_plural]

    def test_empty_plural_leaves_singular_none(self) -> None:
        ctx = ContextNode()
        snap = ctx.update([])
        assert snap.last_result is None
        assert snap.last_results == []
        assert snap.action_state is None
        assert snap.action_states == []


class TestContextNodeStateIsolation:
    def test_context_state_is_shallow_copy(self) -> None:
        """Mutating ctx.state must not affect ContextNode._state (#15)"""
        node = ContextNode()
        node.set("key", "original")
        ctx = node.update([])
        # Mutate the snapshot's state
        ctx.state["key"] = "tampered"
        ctx.state["injected"] = True
        # ContextNode's internal state must be untouched
        assert node.get("key") == "original"
        assert node.get("injected") is None


class TestContextNodeHistoryIsolation:
    def test_snapshot_history_not_mutated_by_later_update(self) -> None:
        """Context snapshot history must be independent of the internal deque.

        _apply_history_config() mutates self._history in-place (evicting stale
        signals).  A snapshot taken on tick N must not be affected by
        _apply_history_config() running on tick N+1.

        Strategy: use a TTL of 2 seconds.  Tick 1 inserts a signal with
        timestamp=now (fresh — not evicted within that same tick).  Then we
        wait conceptually by inserting a second signal with a timestamp that
        makes the first one stale.  The second update() triggers eviction of
        the first signal *in the internal deque*, but the snapshot from
        tick 1 must still show the original value.
        """
        cfg = {"temperature": HistoryConfig(value_ttl_s=2.0)}
        node = ContextNode(history_length=5, history_config=cfg)

        # Tick 1: insert a signal that is fresh (within TTL)
        now = time.monotonic()
        fresh_sig = Signal(
            source="s", kind="temperature", value=99.0,
            confidence=1.0, timestamp=now,
        )
        snap1 = node.update([fresh_sig])

        # Pre-condition: the signal was NOT evicted in this tick's snapshot
        snap1_values = [
            s.value
            for tick_sigs in snap1.history
            for s in tick_sigs
            if s.kind == "temperature"
        ]
        assert snap1_values == [99.0], "Pre-condition: value present before eviction"

        # Tick 2: insert another signal, but with a timestamp far in the
        # future so that _apply_history_config sees fresh_sig as stale
        future_sig = Signal(
            source="s", kind="other", value=1.0,
            confidence=1.0, timestamp=now + 10.0,
        )
        # Monkey-patch time.monotonic so _apply_history_config thinks
        # enough time has elapsed for the TTL to expire
        original_monotonic = time.monotonic
        time.monotonic = lambda: now + 10.0  # type: ignore[assignment]
        try:
            node.update([future_sig])
        finally:
            time.monotonic = original_monotonic  # type: ignore[assignment]

        # The FIRST snapshot's history must still contain the original value
        post_values = [
            s.value
            for tick_sigs in snap1.history
            for s in tick_sigs
            if s.kind == "temperature"
        ]
        assert post_values == [99.0], (
            "Snapshot history was mutated by a later update — "
            "history must be an independent copy"
        )


class TestContextNodeRepr:
    def test_repr_contains_tick(self) -> None:
        ctx = ContextNode()
        ctx.update([])
        assert "tick=1" in repr(ctx)

    def test_repr_includes_max_state_keys_when_set(self) -> None:
        ctx = ContextNode(max_state_keys=5)
        assert "max_state_keys=5" in repr(ctx)

    def test_repr_omits_max_state_keys_when_none(self) -> None:
        ctx = ContextNode()
        assert "max_state_keys" not in repr(ctx)


class TestContextNodeMaxStateKeys:
    def test_default_no_limit(self) -> None:
        ctx = ContextNode()
        for i in range(100):
            ctx.set(f"k{i}", i)
        assert len(ctx._state) == 100

    def test_evicts_oldest_key(self) -> None:
        ctx = ContextNode(max_state_keys=3)
        ctx.set("a", 1)
        ctx.set("b", 2)
        ctx.set("c", 3)
        ctx.set("d", 4)
        assert ctx.get("a") is None
        assert ctx.get("b") == 2
        assert ctx.get("c") == 3
        assert ctx.get("d") == 4

    def test_overwrite_no_eviction(self) -> None:
        ctx = ContextNode(max_state_keys=2)
        ctx.set("a", 1)
        ctx.set("b", 2)
        ctx.set("a", 99)
        assert ctx.get("a") == 99
        assert ctx.get("b") == 2
        assert len(ctx._state) == 2

    def test_state_update_signal_triggers_eviction(self) -> None:
        ctx = ContextNode(max_state_keys=2)
        signals = [
            StateUpdateSignal(
                source="test", kind="state_update", value=None,
                confidence=1.0, timestamp=time.monotonic(),
                key="x", state_value=10,
            ),
            StateUpdateSignal(
                source="test", kind="state_update", value=None,
                confidence=1.0, timestamp=time.monotonic(),
                key="y", state_value=20,
            ),
            StateUpdateSignal(
                source="test", kind="state_update", value=None,
                confidence=1.0, timestamp=time.monotonic(),
                key="z", state_value=30,
            ),
        ]
        ctx.update(signals)
        assert ctx.get("x") is None
        assert ctx.get("y") == 20
        assert ctx.get("z") == 30

    def test_delete_does_not_evict(self) -> None:
        ctx = ContextNode(max_state_keys=3)
        ctx.set("a", 1)
        ctx.set("b", 2)
        ctx.set("c", 3)
        ctx.delete("b")
        # After delete, count is 2 — no spurious eviction
        assert ctx.get("a") == 1
        assert ctx.get("c") == 3
        assert len(ctx._state) == 2

    def test_persisted_state_trimmed_on_load(self, tmp_path: object) -> None:
        import json
        from pathlib import Path

        p = Path(str(tmp_path)) / "state.json"
        p.write_text(json.dumps({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}))
        ctx = ContextNode(state_path=p, max_state_keys=3)
        assert len(ctx._state) == 3
        # The oldest keys (a, b) should have been evicted
        assert ctx.get("a") is None
        assert ctx.get("b") is None

    def test_validation_rejects_zero(self) -> None:
        with pytest.raises(ValueError, match="max_state_keys must be >= 1"):
            ContextNode(max_state_keys=0)

    def test_validation_rejects_negative(self) -> None:
        with pytest.raises(ValueError, match="max_state_keys must be >= 1"):
            ContextNode(max_state_keys=-1)

    def test_context_snapshot_reflects_eviction(self) -> None:
        ctx = ContextNode(max_state_keys=2)
        ctx.set("a", 1)
        ctx.set("b", 2)
        ctx.set("c", 3)
        snap = ctx.update([])
        assert "a" not in snap.state
        assert snap.state["b"] == 2
        assert snap.state["c"] == 3


class TestContextNodeMaxBytes:
    """Tests for HistoryConfig.max_bytes enforcement in ContextNode."""

    @staticmethod
    def _big_sig(kind: str = "image", payload_size: int = 200) -> Signal:
        """Create a signal whose value occupies a known large size."""
        return Signal(
            source="cam",
            kind=kind,
            value=b"\x00" * payload_size,
            confidence=1.0,
            timestamp=time.monotonic(),
        )

    def test_max_bytes_evicts_oldest_entries(self) -> None:
        """Oldest entries are evicted when total bytes exceed max_bytes."""
        import sys

        cfg = {"image": HistoryConfig(max_bytes=100)}
        ctx = ContextNode(history_length=10, history_config=cfg)

        # Push two signals whose combined value size exceeds 100 bytes
        sig1 = self._big_sig(payload_size=200)
        sig2 = self._big_sig(payload_size=200)
        ctx.update([sig1])
        ctx.update([sig2])

        # Collect all image signals from history
        image_sigs = [
            s for tick in ctx._history for s in tick if s.kind == "image"
        ]

        # At least the oldest should be evicted
        evicted = [s for s in image_sigs if s.value is None]
        assert len(evicted) >= 1
        for s in evicted:
            assert s.confidence == 0.0
            assert s.metadata.get("evicted") is True
            assert s.metadata.get("reason") == "max_bytes"

        # Remaining non-evicted values must fit under budget
        remaining_total = sum(
            sys.getsizeof(s.value)
            for s in image_sigs
            if s.value is not None
        )
        assert remaining_total <= 100

    def test_max_bytes_leaves_signals_under_budget(self) -> None:
        """Signals totaling less than max_bytes are not evicted."""
        import sys

        small_value = b"\x01\x02"
        budget = sys.getsizeof(small_value) * 10  # generous budget
        cfg = {"sensor": HistoryConfig(max_bytes=budget)}
        ctx = ContextNode(history_length=10, history_config=cfg)

        sig = Signal(
            source="s", kind="sensor", value=small_value,
            confidence=1.0, timestamp=time.monotonic(),
        )
        ctx.update([sig])
        ctx.update([_sig(kind="other")])  # triggers _apply_history_config again

        sensor_sigs = [
            s for tick in ctx._history for s in tick if s.kind == "sensor"
        ]
        assert all(s.value is not None for s in sensor_sigs)

    def test_max_bytes_only_affects_configured_kind(self) -> None:
        """Only the kind with max_bytes configured is evicted."""
        cfg = {"image": HistoryConfig(max_bytes=10)}  # tiny budget
        ctx = ContextNode(history_length=10, history_config=cfg)

        img_sig = self._big_sig(kind="image", payload_size=200)
        temp_sig = Signal(
            source="t", kind="temperature", value=42.0,
            confidence=1.0, timestamp=time.monotonic(),
        )
        ctx.update([img_sig, temp_sig])
        ctx.update([_sig(kind="other")])  # second tick triggers eviction

        # Image should be evicted
        img_sigs = [
            s for tick in ctx._history for s in tick if s.kind == "image"
        ]
        assert any(s.value is None for s in img_sigs)

        # Temperature must remain untouched
        temp_sigs = [
            s for tick in ctx._history for s in tick if s.kind == "temperature"
        ]
        assert all(s.value is not None for s in temp_sigs)

    def test_max_bytes_works_alongside_value_ttl_s(self) -> None:
        """Both TTL and max_bytes eviction rules apply on the same kind."""
        cfg = {"image": HistoryConfig(value_ttl_s=1.0, max_bytes=10)}
        ctx = ContextNode(history_length=10, history_config=cfg)

        # Insert an old signal (stale by TTL) and a fresh big one
        old_sig = Signal(
            source="cam", kind="image", value=b"\x00" * 200,
            confidence=1.0, timestamp=0.0,  # ancient
        )
        fresh_sig = self._big_sig(kind="image", payload_size=200)

        ctx.update([old_sig])
        ctx.update([fresh_sig])

        img_sigs = [
            s for tick in ctx._history for s in tick if s.kind == "image"
        ]
        # Both should be evicted: old one by TTL, fresh one by max_bytes
        assert all(s.value is None for s in img_sigs)

    def test_max_bytes_skips_already_evicted_signals(self) -> None:
        """Signals with value=None do not contribute to byte count."""
        import sys

        # Budget just barely fits one signal's value
        payload = b"\xAB" * 50
        budget = sys.getsizeof(payload) + 1
        cfg = {"data": HistoryConfig(max_bytes=budget)}
        ctx = ContextNode(history_length=10, history_config=cfg)

        # First signal: manually pre-evicted (value=None)
        evicted_sig = Signal(
            source="s", kind="data", value=None,
            confidence=0.0, timestamp=time.monotonic(),
            metadata={"evicted": True},
        )
        # Second signal: real payload within budget
        real_sig = Signal(
            source="s", kind="data", value=payload,
            confidence=1.0, timestamp=time.monotonic(),
        )
        ctx.update([evicted_sig, real_sig])
        ctx.update([_sig(kind="other")])  # trigger _apply_history_config

        data_sigs = [
            s for tick in ctx._history for s in tick if s.kind == "data"
        ]
        # The pre-evicted signal stays as-is (already None)
        pre_evicted = [
            s for s in data_sigs
            if s.metadata.get("evicted") and s.metadata.get("reason") is None
        ]
        assert len(pre_evicted) == 1

        # The real signal should NOT be evicted (fits in budget)
        real = [s for s in data_sigs if s.value is not None]
        assert len(real) == 1
        assert real[0].value == payload


# ── #22: HistoryConfig.max_ticks enforcement ────────────────────────────────


class TestContextNodeMaxTicks:
    """Tests for HistoryConfig.max_ticks enforcement in ContextNode.

    `max_ticks=N` keeps only the most recent N tick occurrences of a kind.
    Older entries are evicted (value=None, metadata reason="max_ticks") so
    other kinds in the same tick slot remain readable.
    """

    def test_keeps_only_n_most_recent_kind_occurrences(self) -> None:
        cfg = {"image": HistoryConfig(max_ticks=2)}
        ctx = ContextNode(history_length=10, history_config=cfg)

        # Push 5 ticks each containing one "image" signal.
        for i in range(5):
            ctx.update([_sig(kind="image", value=float(i))])

        image_sigs = [s for tick in ctx._history for s in tick if s.kind == "image"]
        live = [s for s in image_sigs if s.value is not None]
        evicted = [s for s in image_sigs if s.value is None]

        # Only the 2 most recent (values 3.0, 4.0) should remain live.
        assert len(live) == 2
        assert {s.value for s in live} == {3.0, 4.0}
        # The 3 oldest should be evicted with the right marker.
        assert len(evicted) == 3
        for s in evicted:
            assert s.metadata.get("evicted") is True
            assert s.metadata.get("reason") == "max_ticks"

    def test_does_not_affect_other_kinds(self) -> None:
        cfg = {"image": HistoryConfig(max_ticks=1)}
        ctx = ContextNode(history_length=10, history_config=cfg)

        for i in range(3):
            ctx.update([
                _sig(kind="image", value=float(i)),
                _sig(kind="temperature", value=20.0 + i),
            ])

        live_images = [
            s for tick in ctx._history for s in tick
            if s.kind == "image" and s.value is not None
        ]
        live_temps = [
            s for tick in ctx._history for s in tick
            if s.kind == "temperature" and s.value is not None
        ]
        # Image: only most recent (2.0) survives.
        assert len(live_images) == 1
        assert live_images[0].value == 2.0
        # Temperature: untouched.
        assert len(live_temps) == 3

    def test_none_means_no_per_kind_eviction(self) -> None:
        """max_ticks=None falls back to global ContextNode.history_length."""
        cfg = {"image": HistoryConfig(max_ticks=None)}
        ctx = ContextNode(history_length=10, history_config=cfg)

        for i in range(5):
            ctx.update([_sig(kind="image", value=float(i))])

        live = [
            s for tick in ctx._history for s in tick
            if s.kind == "image" and s.value is not None
        ]
        # All 5 survive — global history_length=10 is the only cap.
        assert len(live) == 5

    def test_zero_evicts_all(self) -> None:
        cfg = {"image": HistoryConfig(max_ticks=0)}
        ctx = ContextNode(history_length=10, history_config=cfg)

        for i in range(3):
            ctx.update([_sig(kind="image", value=float(i))])

        live = [
            s for tick in ctx._history for s in tick
            if s.kind == "image" and s.value is not None
        ]
        assert live == []
