"""Unit tests for arachnite.models."""

from __future__ import annotations

import pytest

from arachnite.models import (
    REQUIRED,
    ActionExecutionState,
    ActionStep,
    InterruptPolicy,
    LogLevel,
    NodeState,
    Proposal,
    Result,
    Signal,
    StepResult,
)
from tests.conftest import make_context, make_proposal, make_signal


class TestSignal:
    def test_valid_signal(self) -> None:
        s = make_signal()
        assert s.kind == "thermal"
        assert s.confidence == 1.0

    def test_confidence_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="confidence"):
            Signal(source="x", kind="t", value=0, confidence=1.5, timestamp=0.0)

    def test_confidence_negative_raises(self) -> None:
        with pytest.raises(ValueError):
            Signal(source="x", kind="t", value=0, confidence=-0.1, timestamp=0.0)

    def test_confidence_nan_raises(self) -> None:
        with pytest.raises(ValueError, match="finite number"):
            Signal(source="x", kind="t", value=0, confidence=float("nan"), timestamp=0.0)

    def test_confidence_inf_raises(self) -> None:
        with pytest.raises(ValueError, match="finite number"):
            Signal(source="x", kind="t", value=0, confidence=float("inf"), timestamp=0.0)

    def test_confidence_negative_inf_raises(self) -> None:
        with pytest.raises(ValueError, match="finite number"):
            Signal(source="x", kind="t", value=0, confidence=float("-inf"), timestamp=0.0)

    def test_confidence_valid_boundaries(self) -> None:
        s0 = Signal(source="x", kind="t", value=0, confidence=0.0, timestamp=0.0)
        s1 = Signal(source="x", kind="t", value=0, confidence=0.5, timestamp=0.0)
        s2 = Signal(source="x", kind="t", value=0, confidence=1.0, timestamp=0.0)
        assert s0.confidence == 0.0
        assert s1.confidence == 0.5
        assert s2.confidence == 1.0

    def test_metadata_defaults_empty(self) -> None:
        s = make_signal()
        assert s.metadata == {}


class TestProposal:
    def test_valid_proposal(self) -> None:
        p = make_proposal()
        assert p.priority == 50
        assert p.urgency == 0.5

    def test_urgency_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="urgency"):
            Proposal(
                instinct_id="x",
                action_id="y",
                priority=50,
                urgency=1.5,
            )

    def test_evidence_defaults_empty(self) -> None:
        p = make_proposal()
        assert p.evidence == {}

    def test_evidence_carries_paths(self) -> None:
        p = Proposal(
            instinct_id="vision",
            action_id="identify",
            priority=80,
            urgency=0.9,
            evidence={
                "camera_path": "/tmp/media/cam_tick1.jpg",
                "camera_summary": "Person at entrance",
            },
        )
        assert p.evidence["camera_path"] == "/tmp/media/cam_tick1.jpg"
        assert "Person" in p.evidence["camera_summary"]

    def test_persist_defaults_false(self) -> None:
        p = make_proposal()
        assert p.persist is False

    def test_persist_true(self) -> None:
        p = Proposal(
            instinct_id="x", action_id="y",
            priority=50, urgency=0.5, persist=True,
        )
        assert p.persist is True


class TestResult:
    def test_default_fields(self) -> None:
        r = Result(action_id="act", success=True)
        assert r.interrupted is False
        assert r.stopped_at_step is None
        assert r.step_results == []
        assert r.rolled_back is False

    def test_multistep_fields(self) -> None:
        sr = StepResult(step_name="s1", success=True)
        r  = Result(
            action_id       = "act",
            success         = False,
            interrupted     = True,
            stopped_at_step = "s1",
            step_results    = [sr],
            rolled_back     = True,
        )
        assert r.interrupted is True
        assert r.stopped_at_step == "s1"
        assert len(r.step_results) == 1
        assert r.rolled_back is True


class TestActionStep:
    def test_defaults(self) -> None:
        step = ActionStep(name="open_valve")
        assert step.interruptible is True
        assert step.rollback is None
        assert step.timeout_s is None
        assert step.checkpoint is False

    def test_non_interruptible(self) -> None:
        step = ActionStep(name="close_valve", interruptible=False)
        assert step.interruptible is False


class TestNodeState:
    def test_all_states_exist(self) -> None:
        states = {s.value for s in NodeState}
        assert "running" in states
        assert "faulted" in states
        assert "dead" in states


class TestInterruptPolicy:
    def test_all_policies(self) -> None:
        assert InterruptPolicy.ALWAYS.value    == "always"
        assert InterruptPolicy.NEVER.value     == "never"
        assert InterruptPolicy.CHECKPOINT.value == "checkpoint"
        assert InterruptPolicy.ROLLBACK.value  == "rollback"


class TestRequired:
    def test_repr_returns_required_string(self) -> None:
        assert repr(REQUIRED) == "REQUIRED"

    def test_singleton(self) -> None:
        from arachnite.models import _Required
        assert _Required() is REQUIRED


class TestLogLevel:
    def test_lt_comparison(self) -> None:
        assert LogLevel.DEBUG < LogLevel.INFO
        assert LogLevel.INFO < LogLevel.WARNING
        assert not (LogLevel.ERROR < LogLevel.WARNING)

    def test_le_comparison(self) -> None:
        assert LogLevel.DEBUG <= LogLevel.DEBUG
        assert LogLevel.DEBUG <= LogLevel.INFO


class TestContextPluralFields:
    def test_plural_fields_default_empty(self) -> None:
        ctx = make_context()
        assert ctx.last_results == []
        assert ctx.action_states == []

    def test_plural_fields_populated(self) -> None:
        r = Result(action_id="Act", success=True)
        s = ActionExecutionState(
            action_id="Act", current_step=None,
            completed_steps=[], interruptible=True,
            mandatory_block_remaining_s=0.0,
        )
        ctx = make_context()
        ctx.last_results = [r]
        ctx.action_states = [s]
        assert len(ctx.last_results) == 1
        assert len(ctx.action_states) == 1
