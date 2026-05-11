"""Unit tests for the TickInstrumenter protocol (ADR 0002).

Covers:
  - The default ``tick_instrumenter=None`` path remains a straight-line
    tick with no side effects (regression guard for Bench-3).
  - A custom instrumenter receives one ``on_stage`` call per stage per
    tick, in the order defined by ``TICK_STAGE_NAMES``, plus one
    ``on_tick_complete`` per tick.
  - Stage names are drawn verbatim from ``TICK_STAGE_NAMES`` — no typos
    between the runtime's ``tick()`` method and the exported vocabulary.
  - Exceptions raised by the instrumenter do **not** crash the tick loop
    (per the "never raise in node interfaces" rule extended to
    instrumenters — ADR 0002).
  - ``set_tick_instrumenter`` can attach / detach an instrumenter on an
    already-running (or not-yet-started) runtime.
"""

from __future__ import annotations

import pytest

from arachnite import (
    TICK_STAGE_NAMES,
    ArachniteRuntime,
    ContextNode,
    SignalBus,
    TickInstrumenter,
)
from arachnite.nodes.action import ActionMasterNode
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import InstinctMasterNode
from arachnite.nodes.sense import SenseMasterNode
from tests.conftest import ConstantSenseNode, RecordingAction

# ── Helpers ────────────────────────────────────────────────────────────────────


def _build_rt(
    instrumenter: TickInstrumenter | None = None,
) -> tuple[ArachniteRuntime, RecordingAction]:
    bus = SignalBus()
    sm = SenseMasterNode(bus=bus)
    im = InstinctMasterNode(bus=bus)
    dm = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    am = ActionMasterNode(bus=bus)

    sm.register(ConstantSenseNode(bus=bus))
    action = RecordingAction(bus=bus)
    am.register(action)

    rt = ArachniteRuntime(
        sense_master=sm,
        context=ContextNode(),
        instinct_master=im,
        decision_master=dm,
        action_master=am,
        bus=bus,
        tick_rate_hz=1000.0,
        tick_instrumenter=instrumenter,
    )
    return rt, action


class _RecordingInstrumenter:
    """Straightforward ``TickInstrumenter`` that captures every call."""

    def __init__(self) -> None:
        self.stage_calls: list[tuple[str, float]] = []
        self.tick_calls: list[tuple[int, float]] = []

    def on_stage(self, stage: str, duration_s: float) -> None:
        self.stage_calls.append((stage, duration_s))

    def on_tick_complete(self, tick_index: int, total_s: float) -> None:
        self.tick_calls.append((tick_index, total_s))


class _BrokenInstrumenter:
    """Always raises — used to verify the runtime isolates errors."""

    def __init__(self) -> None:
        self.stage_calls = 0
        self.tick_calls = 0

    def on_stage(self, stage: str, duration_s: float) -> None:
        self.stage_calls += 1
        raise RuntimeError(f"boom in {stage}")

    def on_tick_complete(self, tick_index: int, total_s: float) -> None:
        self.tick_calls += 1
        raise RuntimeError("boom in on_tick_complete")


# ── Default path: no instrumenter ──────────────────────────────────────────────


class TestDefaultPath:
    @pytest.mark.asyncio
    async def test_default_is_none(self) -> None:
        """No instrumenter kwarg → ``_tick_instrumenter`` is None."""
        rt, _ = _build_rt()
        assert rt._tick_instrumenter is None

    @pytest.mark.asyncio
    async def test_tick_runs_without_instrumenter(self) -> None:
        """tick() is a no-op regression: count increments, no side effects."""
        rt, _ = _build_rt()
        # Drive manually (no background loop) so tick_count is deterministic.
        for master in (
            rt._sense_master, rt._instinct_master,
            rt._decision_master, rt._action_master,
        ):
            await master.setup()
        initial = rt.tick_count
        await rt.tick()
        assert rt.tick_count == initial + 1
        for master in (
            rt._action_master, rt._decision_master,
            rt._instinct_master, rt._sense_master,
        ):
            await master.teardown()


# ── Protocol conformance ──────────────────────────────────────────────────────


class TestProtocolConformance:
    def test_tick_stage_names_are_six(self) -> None:
        """The published vocabulary is exactly the six ADR 0002 stages."""
        assert TICK_STAGE_NAMES == (
            "sense", "context", "reflex", "instinct", "decide", "act",
        )

    def test_recording_instrumenter_is_protocol_conformant(self) -> None:
        """Structural check — no explicit inheritance required."""
        inst = _RecordingInstrumenter()
        assert isinstance(inst, TickInstrumenter)


# ── Hook invocation order / arity ─────────────────────────────────────────────


class TestHookInvocation:
    @pytest.mark.asyncio
    async def test_every_stage_called_once_per_tick_in_order(self) -> None:
        inst = _RecordingInstrumenter()
        rt, _ = _build_rt(instrumenter=inst)
        await rt.start()
        await rt.tick()
        await rt.stop()

        # Filter out any stage calls emitted by implicit background tick
        # before our manual tick() (there may be a couple under normal
        # scheduling). We assert on at least one complete sequence.
        stage_names_seen = [name for name, _ in inst.stage_calls]
        # The six stages must appear as a contiguous in-order sub-sequence.
        target = list(TICK_STAGE_NAMES)
        found = False
        for i in range(len(stage_names_seen) - len(target) + 1):
            if stage_names_seen[i : i + len(target)] == target:
                found = True
                break
        assert found, f"expected {target!r} in order; got {stage_names_seen!r}"

    @pytest.mark.asyncio
    async def test_stage_durations_are_non_negative(self) -> None:
        inst = _RecordingInstrumenter()
        rt, _ = _build_rt(instrumenter=inst)
        await rt.start()
        await rt.tick()
        await rt.stop()
        for name, duration in inst.stage_calls:
            assert name in TICK_STAGE_NAMES
            assert duration >= 0.0

    @pytest.mark.asyncio
    async def test_tick_complete_fires_once_per_tick(self) -> None:
        inst = _RecordingInstrumenter()
        rt, _ = _build_rt(instrumenter=inst)
        # Drive ticks manually without the background loop so the counts
        # are deterministic.
        for master in (
            rt._sense_master, rt._instinct_master,
            rt._decision_master, rt._action_master,
        ):
            await master.setup()
        n_ticks = 5
        before = len(inst.tick_calls)
        for _ in range(n_ticks):
            await rt.tick()
        after = len(inst.tick_calls)
        assert after - before == n_ticks

        # Stage calls: each manual tick contributes exactly 6 calls.
        # Filter by the most recent block (last n_ticks * 6 entries).
        recent_stage_calls = inst.stage_calls[-n_ticks * 6:]
        recent_names = [name for name, _ in recent_stage_calls]
        expected = list(TICK_STAGE_NAMES) * n_ticks
        assert recent_names == expected

        for master in (
            rt._action_master, rt._decision_master,
            rt._instinct_master, rt._sense_master,
        ):
            await master.teardown()

    @pytest.mark.asyncio
    async def test_tick_index_matches_tick_count(self) -> None:
        inst = _RecordingInstrumenter()
        rt, _ = _build_rt(instrumenter=inst)
        for master in (
            rt._sense_master, rt._instinct_master,
            rt._decision_master, rt._action_master,
        ):
            await master.setup()
        for _ in range(3):
            await rt.tick()
        for master in (
            rt._action_master, rt._decision_master,
            rt._instinct_master, rt._sense_master,
        ):
            await master.teardown()

        # The tick index reported to ``on_tick_complete`` must match the
        # runtime's ``tick_count`` at the moment the call was made.
        indices = [idx for idx, _ in inst.tick_calls]
        assert indices == [1, 2, 3]


# ── Setter: set_tick_instrumenter ────────────────────────────────────────────


class TestSetter:
    @pytest.mark.asyncio
    async def test_attach_post_construction(self) -> None:
        rt, _ = _build_rt()
        assert rt._tick_instrumenter is None
        inst = _RecordingInstrumenter()
        rt.set_tick_instrumenter(inst)
        assert rt._tick_instrumenter is inst

    @pytest.mark.asyncio
    async def test_detach_restores_default_path(self) -> None:
        inst = _RecordingInstrumenter()
        rt, _ = _build_rt(instrumenter=inst)
        rt.set_tick_instrumenter(None)
        assert rt._tick_instrumenter is None

        for master in (
            rt._sense_master, rt._instinct_master,
            rt._decision_master, rt._action_master,
        ):
            await master.setup()
        await rt.tick()
        for master in (
            rt._action_master, rt._decision_master,
            rt._instinct_master, rt._sense_master,
        ):
            await master.teardown()

        # No stage calls should have been recorded after detachment
        # (the one tick we drove here contributed 0 entries).
        # Exact emptiness depends on prior state, but no *new* entries
        # appended since the detach is the key assertion — we check the
        # counts post-tick match pre-tick.
        # (The fixture creates no runtime-driven ticks because we don't
        # start the background loop, so the list remains whatever size
        # it was pre-detach — exactly zero.)
        assert inst.stage_calls == []
        assert inst.tick_calls == []


# ── Error isolation ──────────────────────────────────────────────────────────


class TestErrorIsolation:
    @pytest.mark.asyncio
    async def test_broken_instrumenter_does_not_crash_tick(self) -> None:
        """Per ADR 0002: instrumenter exceptions are logged, not raised."""
        broken = _BrokenInstrumenter()
        rt, _ = _build_rt(instrumenter=broken)
        for master in (
            rt._sense_master, rt._instinct_master,
            rt._decision_master, rt._action_master,
        ):
            await master.setup()
        # Should not raise
        await rt.tick()
        await rt.tick()
        for master in (
            rt._action_master, rt._decision_master,
            rt._instinct_master, rt._sense_master,
        ):
            await master.teardown()

        # Hooks were still invoked (the runtime caught the raise each time).
        assert broken.stage_calls == 2 * len(TICK_STAGE_NAMES)
        assert broken.tick_calls == 2
        assert rt.tick_count == 2

    @pytest.mark.asyncio
    async def test_partial_failure_still_records_all_stages(self) -> None:
        """One-shot error in a single stage does not prevent later hooks."""

        class _FlakyInstrumenter:
            def __init__(self) -> None:
                self.stage_calls: list[str] = []
                self.tick_calls = 0

            def on_stage(self, stage: str, duration_s: float) -> None:
                self.stage_calls.append(stage)
                if stage == "reflex":
                    raise ValueError("only reflex fails")

            def on_tick_complete(self, tick_index: int, total_s: float) -> None:
                self.tick_calls += 1

        flaky = _FlakyInstrumenter()
        rt, _ = _build_rt(instrumenter=flaky)
        for master in (
            rt._sense_master, rt._instinct_master,
            rt._decision_master, rt._action_master,
        ):
            await master.setup()
        await rt.tick()
        for master in (
            rt._action_master, rt._decision_master,
            rt._instinct_master, rt._sense_master,
        ):
            await master.teardown()

        # All six stage names were visited even though ``reflex`` raised.
        assert flaky.stage_calls == list(TICK_STAGE_NAMES)
        assert flaky.tick_calls == 1
