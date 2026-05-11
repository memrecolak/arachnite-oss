"""
Tests for arachnite.testing — factory functions and MockBus.
"""

from __future__ import annotations

import time
from collections import deque

import pytest

from arachnite.models import (
    ActionExecutionState,
    Context,
    Proposal,
    Result,
    Signal,
)
from arachnite.testing import (
    MockBus,
    make_context,
    make_proposal,
    make_result,
    make_signal,
)

# ═══════════════════════════════════════════════════════════════════════════════
# make_signal
# ═══════════════════════════════════════════════════════════════════════════════


class TestMakeSignal:
    """Tests for the make_signal factory"""

    def test_returns_signal_type(self) -> None:
        sig = make_signal()
        assert isinstance(sig, Signal)

    def test_defaults(self) -> None:
        before = time.monotonic()
        sig = make_signal()
        after = time.monotonic()

        assert sig.kind == "test"
        assert sig.value == 0.0
        assert sig.source == "TestSense"
        assert sig.confidence == 1.0
        assert before <= sig.timestamp <= after
        assert sig.metadata == {}

    def test_overrides(self) -> None:
        meta = {"unit": "celsius"}
        sig = make_signal(
            kind="thermal",
            value=42.5,
            source="MySensor",
            confidence=0.9,
            timestamp=100.0,
            metadata=meta,
        )
        assert sig.kind == "thermal"
        assert sig.value == 42.5
        assert sig.source == "MySensor"
        assert sig.confidence == 0.9
        assert sig.timestamp == 100.0
        assert sig.metadata == meta

    def test_metadata_default_is_independent(self) -> None:
        s1 = make_signal()
        s2 = make_signal()
        s1.metadata["key"] = "val"
        assert "key" not in s2.metadata


# ═══════════════════════════════════════════════════════════════════════════════
# make_proposal
# ═══════════════════════════════════════════════════════════════════════════════


class TestMakeProposal:
    """Tests for the make_proposal factory"""

    def test_returns_proposal_type(self) -> None:
        p = make_proposal()
        assert isinstance(p, Proposal)

    def test_defaults(self) -> None:
        p = make_proposal()
        assert p.action_id == "TestAction"
        assert p.instinct_id == "TestInstinct"
        assert p.priority == 50
        assert p.urgency == 0.5
        assert p.parameters == {}
        assert p.rationale == ""
        assert p.persist is False

    def test_overrides(self) -> None:
        params = {"target": 35}
        p = make_proposal(
            action_id="CoolDown",
            instinct_id="HotInstinct",
            priority=100,
            urgency=0.9,
            parameters=params,
            rationale="too hot",
            persist=True,
        )
        assert p.action_id == "CoolDown"
        assert p.instinct_id == "HotInstinct"
        assert p.priority == 100
        assert p.urgency == 0.9
        assert p.parameters == params
        assert p.rationale == "too hot"
        assert p.persist is True

    def test_parameters_default_is_independent(self) -> None:
        p1 = make_proposal()
        p2 = make_proposal()
        p1.parameters["x"] = 1
        assert "x" not in p2.parameters


# ═══════════════════════════════════════════════════════════════════════════════
# make_result
# ═══════════════════════════════════════════════════════════════════════════════


class TestMakeResult:
    """Tests for the make_result factory"""

    def test_returns_result_type(self) -> None:
        r = make_result()
        assert isinstance(r, Result)

    def test_defaults(self) -> None:
        r = make_result()
        assert r.action_id == "TestAction"
        assert r.success is True
        assert r.error is None
        assert r.output is None

    def test_overrides(self) -> None:
        err = RuntimeError("boom")
        r = make_result(
            action_id="CoolDown",
            success=False,
            error=err,
            output={"status": "failed"},
        )
        assert r.action_id == "CoolDown"
        assert r.success is False
        assert r.error is err
        assert r.output == {"status": "failed"}

    def test_failure_result(self) -> None:
        r = make_result(success=False, error=ValueError("bad"))
        assert r.success is False
        assert isinstance(r.error, ValueError)


# ═══════════════════════════════════════════════════════════════════════════════
# make_context
# ═══════════════════════════════════════════════════════════════════════════════


class TestMakeContext:
    """Tests for the make_context factory"""

    def test_returns_context_type(self) -> None:
        ctx = make_context()
        assert isinstance(ctx, Context)

    def test_defaults(self) -> None:
        before = time.monotonic()
        ctx = make_context()
        after = time.monotonic()

        assert ctx.tick == 1
        assert ctx.signals == []
        assert ctx.state == {}
        assert ctx.last_result is None
        assert ctx.last_results == []
        assert ctx.action_state is None
        assert ctx.action_states == []
        assert isinstance(ctx.history, deque)
        assert len(ctx.history) == 0
        assert before <= ctx.timestamp <= after

    def test_override_tick_and_signals(self) -> None:
        sigs = [make_signal(kind="a"), make_signal(kind="b")]
        ctx = make_context(tick=42, signals=sigs)
        assert ctx.tick == 42
        assert len(ctx.signals) == 2
        assert ctx.signals[0].kind == "a"

    def test_override_state(self) -> None:
        ctx = make_context(state={"temp": 72, "mode": "cool"})
        assert ctx.state == {"temp": 72, "mode": "cool"}

    def test_override_last_result(self) -> None:
        r = make_result(action_id="CoolDown", success=True)
        ctx = make_context(last_result=r)
        assert ctx.last_result is r

    def test_override_last_results(self) -> None:
        r1 = make_result(action_id="A")
        r2 = make_result(action_id="B")
        ctx = make_context(last_results=[r1, r2])
        assert len(ctx.last_results) == 2
        assert ctx.last_results[0].action_id == "A"
        assert ctx.last_results[1].action_id == "B"

    def test_override_action_state(self) -> None:
        aes = ActionExecutionState(
            action_id="CoolDown",
            current_step="step1",
            completed_steps=[],
            interruptible=True,
            mandatory_block_remaining_s=0.0,
        )
        ctx = make_context(action_state=aes)
        assert ctx.action_state is aes

    def test_override_action_states(self) -> None:
        aes = ActionExecutionState(
            action_id="CoolDown",
            current_step="step1",
            completed_steps=[],
            interruptible=True,
            mandatory_block_remaining_s=0.0,
        )
        ctx = make_context(action_states=[aes])
        assert len(ctx.action_states) == 1
        assert ctx.action_states[0].action_id == "CoolDown"

    def test_override_history(self) -> None:
        sig = make_signal(kind="thermal", value=25.0)
        hist: deque[list[Signal]] = deque([[sig]], maxlen=5)
        ctx = make_context(history=hist)
        assert len(ctx.history) == 1
        assert ctx.history[0][0].kind == "thermal"

    def test_signals_default_is_independent(self) -> None:
        ctx1 = make_context()
        ctx2 = make_context()
        ctx1.signals.append(make_signal())
        assert len(ctx2.signals) == 0

    def test_state_default_is_independent(self) -> None:
        ctx1 = make_context()
        ctx2 = make_context()
        ctx1.state["key"] = "val"
        assert "key" not in ctx2.state


# ═══════════════════════════════════════════════════════════════════════════════
# MockBus
# ═══════════════════════════════════════════════════════════════════════════════


class TestMockBus:
    """Tests for the MockBus test helper"""

    @pytest.fixture
    def mock_bus(self) -> MockBus:
        return MockBus()

    @pytest.mark.asyncio
    async def test_records_published_signal(self, mock_bus: MockBus) -> None:
        sig = make_signal(kind="thermal", value=42.0)
        await mock_bus.publish(sig)
        assert len(mock_bus.published) == 1
        assert mock_bus.published[0] is sig

    @pytest.mark.asyncio
    async def test_records_multiple_signals(self, mock_bus: MockBus) -> None:
        s1 = make_signal(kind="thermal")
        s2 = make_signal(kind="audio")
        s3 = make_signal(kind="thermal")
        await mock_bus.publish(s1)
        await mock_bus.publish(s2)
        await mock_bus.publish(s3)
        assert len(mock_bus.published) == 3

    @pytest.mark.asyncio
    async def test_published_of_kind(self, mock_bus: MockBus) -> None:
        await mock_bus.publish(make_signal(kind="thermal", value=1.0))
        await mock_bus.publish(make_signal(kind="audio", value=2.0))
        await mock_bus.publish(make_signal(kind="thermal", value=3.0))

        thermal = mock_bus.published_of_kind("thermal")
        assert len(thermal) == 2
        assert all(s.kind == "thermal" for s in thermal)

        audio = mock_bus.published_of_kind("audio")
        assert len(audio) == 1
        assert audio[0].value == 2.0

    @pytest.mark.asyncio
    async def test_published_of_kind_returns_empty_for_unknown(
        self, mock_bus: MockBus
    ) -> None:
        await mock_bus.publish(make_signal(kind="thermal"))
        assert mock_bus.published_of_kind("unknown") == []

    @pytest.mark.asyncio
    async def test_clear_resets_published(self, mock_bus: MockBus) -> None:
        await mock_bus.publish(make_signal())
        await mock_bus.publish(make_signal())
        assert len(mock_bus.published) == 2

        mock_bus.clear()
        assert mock_bus.published == []

    @pytest.mark.asyncio
    async def test_clear_also_clears_subscribers(self, mock_bus: MockBus) -> None:
        received: list[Signal] = []

        async def handler(sig: Signal) -> None:
            received.append(sig)

        mock_bus.subscribe("thermal", handler)
        mock_bus.clear()

        await mock_bus.publish(make_signal(kind="thermal"))
        # Signal is recorded but the handler was cleared
        assert len(mock_bus.published) == 1
        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_subscribers_still_called(self, mock_bus: MockBus) -> None:
        received: list[Signal] = []

        async def handler(sig: Signal) -> None:
            received.append(sig)

        mock_bus.subscribe("thermal", handler)
        sig = make_signal(kind="thermal", value=99.0)
        await mock_bus.publish(sig)

        assert len(received) == 1
        assert received[0] is sig

    @pytest.mark.asyncio
    async def test_published_returns_copy(self, mock_bus: MockBus) -> None:
        await mock_bus.publish(make_signal())
        published = mock_bus.published
        published.clear()
        # Clearing the returned list does not affect the bus
        assert len(mock_bus.published) == 1

    def test_empty_bus_has_no_published(self, mock_bus: MockBus) -> None:
        assert mock_bus.published == []
        assert mock_bus.published_of_kind("anything") == []
