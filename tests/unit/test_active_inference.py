"""Tests for ActiveInferenceDecisionNode."""

from __future__ import annotations

import pytest

from arachnite import ActiveInferenceDecisionNode, Proposal, SignalBus


@pytest.fixture
def bus() -> SignalBus:
    return SignalBus()


def _proposal(
    priority: int = 50,
    urgency: float = 0.5,
    evidence: dict | None = None,
    action_id: str = "action",
    instinct_id: str = "instinct",
) -> Proposal:
    return Proposal(
        instinct_id=instinct_id,
        action_id=action_id,
        priority=priority,
        urgency=urgency,
        evidence=evidence or {},
    )


# ── Basic selection ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_empty_proposals_returns_none(bus: SignalBus) -> None:
    node = ActiveInferenceDecisionNode(bus=bus)
    assert await node.decide([]) is None


@pytest.mark.asyncio
async def test_single_proposal_returned(bus: SignalBus) -> None:
    node = ActiveInferenceDecisionNode(bus=bus)
    p = _proposal(priority=80, urgency=0.9)
    assert await node.decide([p]) is p


@pytest.mark.asyncio
async def test_higher_pragmatic_value_wins(bus: SignalBus) -> None:
    """With beta=0 (no epistemic), behaves like WeightedDecisionNode."""
    node = ActiveInferenceDecisionNode(beta=0.0, bus=bus)
    low = _proposal(priority=30, urgency=0.5, action_id="low", instinct_id="i1")
    high = _proposal(priority=90, urgency=0.9, action_id="high", instinct_id="i2")
    chosen = await node.decide([low, high])
    assert chosen is high


@pytest.mark.asyncio
async def test_beta_zero_equivalent_to_weighted(bus: SignalBus) -> None:
    """beta=0 should select same as priority*urgency."""
    node = ActiveInferenceDecisionNode(beta=0.0, bus=bus)
    # priority*urgency: a=40, b=45, c=35
    a = _proposal(priority=80, urgency=0.5, action_id="a", instinct_id="ia")
    b = _proposal(priority=50, urgency=0.9, action_id="b", instinct_id="ib")
    c = _proposal(priority=70, urgency=0.5, action_id="c", instinct_id="ic")
    chosen = await node.decide([a, b, c])
    assert chosen is b  # 50*0.9 = 45 > 80*0.5 = 40 > 70*0.5 = 35


# ── Epistemic value ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_epistemic_value_penalises_uncertainty(bus: SignalBus) -> None:
    """With high beta, uncertain contexts increase EFE (worse).

    Active inference agents prefer actions in contexts where they are
    confident — the epistemic term penalises acting under uncertainty,
    driving the agent to first gather information (via other actions)
    before committing to uncertain actions.
    """
    node = ActiveInferenceDecisionNode(beta=100.0, bus=bus)
    # Same pragmatic value, different epistemic value
    certain = _proposal(
        priority=50, urgency=0.5,
        evidence={"sensor_confidence": 0.99},
        action_id="certain", instinct_id="i1",
    )
    uncertain = _proposal(
        priority=50, urgency=0.5,
        evidence={"sensor_confidence": 0.1},
        action_id="uncertain", instinct_id="i2",
    )
    chosen = await node.decide([certain, uncertain])
    # Certain action has lower EFE (less uncertainty penalty)
    assert chosen is certain


@pytest.mark.asyncio
async def test_no_evidence_uses_prior(bus: SignalBus) -> None:
    """Proposals without evidence use prior_confidence for epistemic calc."""
    node = ActiveInferenceDecisionNode(beta=0.0, prior_confidence=0.5, bus=bus)
    p = _proposal(priority=50, urgency=0.5)
    efe = node._expected_free_energy(p)
    # -pragmatic + beta*epistemic = -(50*0.5) + 0*(1-0.5) = -25
    assert efe == pytest.approx(-25.0)


@pytest.mark.asyncio
async def test_multiple_confidence_keys_averaged(bus: SignalBus) -> None:
    """Multiple *_confidence keys in evidence are averaged."""
    node = ActiveInferenceDecisionNode(beta=10.0, bus=bus)
    p = _proposal(
        priority=50, urgency=0.5,
        evidence={
            "camera_confidence": 0.9,
            "audio_confidence": 0.5,
        },
    )
    epistemic = node._epistemic_value(p)
    # avg confidence = 0.7, epistemic = 1 - 0.7 = 0.3
    assert epistemic == pytest.approx(0.3)


# ── EFE computation ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_efe_computation(bus: SignalBus) -> None:
    node = ActiveInferenceDecisionNode(beta=2.0, prior_confidence=0.5, bus=bus)
    p = _proposal(priority=80, urgency=0.9)
    efe = node._expected_free_energy(p)
    # -pragmatic + beta*epistemic = -(80*0.9) + 2*(1-0.5) = -72 + 1 = -71
    assert efe == pytest.approx(-71.0)


@pytest.mark.asyncio
async def test_efe_with_evidence(bus: SignalBus) -> None:
    node = ActiveInferenceDecisionNode(beta=5.0, bus=bus)
    p = _proposal(
        priority=60, urgency=0.8,
        evidence={"lidar_confidence": 0.95},
    )
    efe = node._expected_free_energy(p)
    # -(60*0.8) + 5*(1-0.95) = -48 + 0.25 = -47.75
    assert efe == pytest.approx(-47.75)


# ── Probabilistic selection ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_temperature_zero_is_deterministic(bus: SignalBus) -> None:
    """temperature=0 should always return the same proposal."""
    node = ActiveInferenceDecisionNode(beta=0.0, temperature=0.0, bus=bus)
    a = _proposal(priority=90, urgency=0.9, action_id="a", instinct_id="ia")
    b = _proposal(priority=30, urgency=0.3, action_id="b", instinct_id="ib")
    results = set()
    for _ in range(20):
        chosen = await node.decide([a, b])
        results.add(chosen.action_id)
    assert results == {"a"}


@pytest.mark.asyncio
async def test_temperature_positive_is_probabilistic(bus: SignalBus) -> None:
    """With temperature > 0, both proposals should be selected sometimes."""
    node = ActiveInferenceDecisionNode(beta=0.0, temperature=50.0, bus=bus)
    a = _proposal(priority=60, urgency=0.5, action_id="a", instinct_id="ia")
    b = _proposal(priority=50, urgency=0.5, action_id="b", instinct_id="ib")
    results = set()
    for _ in range(100):
        chosen = await node.decide([a, b])
        results.add(chosen.action_id)
    # With high temperature and close values, both should appear
    assert len(results) == 2


# ── Edge cases ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_all_zero_urgency(bus: SignalBus) -> None:
    """Proposals with zero urgency should still be selectable."""
    node = ActiveInferenceDecisionNode(beta=0.0, bus=bus)
    a = _proposal(priority=50, urgency=0.0, action_id="a", instinct_id="ia")
    b = _proposal(priority=80, urgency=0.0, action_id="b", instinct_id="ib")
    # Both have pragmatic value 0; should still return one
    chosen = await node.decide([a, b])
    assert chosen is not None


@pytest.mark.asyncio
async def test_non_confidence_evidence_ignored(bus: SignalBus) -> None:
    """Evidence keys not ending in _confidence are ignored."""
    node = ActiveInferenceDecisionNode(beta=1.0, prior_confidence=0.5, bus=bus)
    p = _proposal(
        priority=50, urgency=0.5,
        evidence={"camera_path": "/tmp/img.jpg", "summary": "hello"},
    )
    # No _confidence keys -> uses prior
    epistemic = node._epistemic_value(p)
    assert epistemic == pytest.approx(0.5)
