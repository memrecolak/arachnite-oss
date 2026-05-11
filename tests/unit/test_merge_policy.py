"""Unit tests for signal merge policies on SenseMasterNode."""

from __future__ import annotations

import time

import pytest

from arachnite import SignalBus
from arachnite.models import MergePolicy, Signal
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode

# ── Concrete test nodes ────────────────────────────────────────────────────────

class TempSenseA(BaseSenseNode):
    node_id = "TempSenseA"
    signal_kind = "temperature"

    def __init__(
        self,
        bus: SignalBus,
        value: float = 30.0,
        confidence: float = 0.8,
        **kw: object,
    ) -> None:
        super().__init__(bus, **kw)  # type: ignore[arg-type]
        self._value = value
        self._confidence = confidence

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=self._value, confidence=self._confidence,
            timestamp=time.monotonic(),
        )


class TempSenseB(BaseSenseNode):
    node_id = "TempSenseB"
    signal_kind = "temperature"

    def __init__(
        self,
        bus: SignalBus,
        value: float = 35.0,
        confidence: float = 0.9,
        **kw: object,
    ) -> None:
        super().__init__(bus, **kw)  # type: ignore[arg-type]
        self._value = value
        self._confidence = confidence

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=self._value, confidence=self._confidence,
            timestamp=time.monotonic(),
        )


class ProximitySense(BaseSenseNode):
    node_id = "ProximitySense"
    signal_kind = "proximity"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value=1.5, confidence=1.0, timestamp=time.monotonic(),
        )


class TextSenseA(BaseSenseNode):
    node_id = "TextSenseA"
    signal_kind = "text"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value="hello", confidence=0.7, timestamp=time.monotonic(),
        )


class TextSenseB(BaseSenseNode):
    node_id = "TextSenseB"
    signal_kind = "text"

    async def read(self) -> Signal:
        return Signal(
            source=self.node_id, kind=self.signal_kind,
            value="world", confidence=0.9, timestamp=time.monotonic(),
        )


# ── Default behavior (ALL) ──────────────────────────────────────────────────

class TestMergePolicyAll:
    @pytest.mark.asyncio
    async def test_no_policy_keeps_all_signals(self) -> None:
        """Default: no merge_policies means all signals pass through."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        sm.register(TempSenseA(bus=bus))
        sm.register(TempSenseB(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 2

    @pytest.mark.asyncio
    async def test_explicit_all_keeps_all_signals(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.ALL})
        sm.register(TempSenseA(bus=bus))
        sm.register(TempSenseB(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 2


# ── LATEST ──────────────────────────────────────────────────────────────────

class TestMergePolicyLatest:
    @pytest.mark.asyncio
    async def test_latest_keeps_newest_timestamp(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.LATEST})
        sm.register(TempSenseA(bus=bus))
        sm.register(TempSenseB(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert signals[0].metadata["merge_policy"] == "latest"
        assert len(signals[0].metadata["merged_from"]) == 2

    @pytest.mark.asyncio
    async def test_latest_single_signal_unchanged(self) -> None:
        """Single signal of a merged kind should pass through unchanged."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.LATEST})
        sm.register(TempSenseA(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert "merge_policy" not in signals[0].metadata

    @pytest.mark.asyncio
    async def test_latest_unconfigured_kind_unaffected(self) -> None:
        """Kinds without a merge policy are not merged."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.LATEST})
        sm.register(TempSenseA(bus=bus))
        sm.register(TempSenseB(bus=bus))
        sm.register(ProximitySense(bus=bus))
        signals = await sm.read_all()
        # 1 merged temperature + 1 proximity
        assert len(signals) == 2
        kinds = {s.kind for s in signals}
        assert kinds == {"temperature", "proximity"}


# ── HIGHEST_CONFIDENCE ──────────────────────────────────────────────────────

class TestMergePolicyHighestConfidence:
    @pytest.mark.asyncio
    async def test_picks_highest_confidence(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(
            bus=bus,
            merge_policies={"temperature": MergePolicy.HIGHEST_CONFIDENCE},
        )
        sm.register(TempSenseA(bus=bus, value=30.0, confidence=0.5))
        sm.register(TempSenseB(bus=bus, value=35.0, confidence=0.95))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert signals[0].value == 35.0
        assert signals[0].confidence == 0.95
        assert signals[0].source == "TempSenseB"
        assert signals[0].metadata["merge_policy"] == "highest_confidence"

    @pytest.mark.asyncio
    async def test_highest_confidence_metadata_has_sources(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(
            bus=bus,
            merge_policies={"temperature": MergePolicy.HIGHEST_CONFIDENCE},
        )
        sm.register(TempSenseA(bus=bus))
        sm.register(TempSenseB(bus=bus))
        signals = await sm.read_all()
        sources = signals[0].metadata["merged_from"]
        assert set(sources) == {"TempSenseA", "TempSenseB"}


# ── MEAN ────────────────────────────────────────────────────────────────────

class TestMergePolicyMean:
    @pytest.mark.asyncio
    async def test_mean_averages_numeric_values(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.MEAN})
        sm.register(TempSenseA(bus=bus, value=30.0, confidence=0.8))
        sm.register(TempSenseB(bus=bus, value=40.0, confidence=1.0))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert signals[0].value == pytest.approx(35.0)
        assert signals[0].confidence == pytest.approx(0.9)
        assert signals[0].metadata["merge_policy"] == "mean"
        assert signals[0].metadata["sample_count"] == 2

    @pytest.mark.asyncio
    async def test_mean_non_numeric_falls_back_to_highest_confidence(self) -> None:
        """Non-numeric values cannot be averaged; fall back to highest confidence."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"text": MergePolicy.MEAN})
        sm.register(TextSenseA(bus=bus))
        sm.register(TextSenseB(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 1
        # Falls back to highest confidence (TextSenseB at 0.9)
        assert signals[0].value == "world"
        assert signals[0].confidence == 0.9
        assert signals[0].metadata["merge_fallback"] is True
        assert signals[0].metadata["merge_policy"] == "highest_confidence"

    @pytest.mark.asyncio
    async def test_mean_metadata_has_sources_and_count(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.MEAN})
        sm.register(TempSenseA(bus=bus))
        sm.register(TempSenseB(bus=bus))
        signals = await sm.read_all()
        sources = signals[0].metadata["merged_from"]
        assert set(sources) == {"TempSenseA", "TempSenseB"}
        assert signals[0].metadata["sample_count"] == 2


# ── Mixed kinds ─────────────────────────────────────────────────────────────

class TestMergePolicyMixed:
    @pytest.mark.asyncio
    async def test_different_policies_per_kind(self) -> None:
        """Each kind can have its own merge policy."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={
            "temperature": MergePolicy.MEAN,
            "text": MergePolicy.HIGHEST_CONFIDENCE,
        })
        sm.register(TempSenseA(bus=bus, value=20.0, confidence=0.8))
        sm.register(TempSenseB(bus=bus, value=40.0, confidence=1.0))
        sm.register(TextSenseA(bus=bus))
        sm.register(TextSenseB(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 2
        by_kind = {s.kind: s for s in signals}
        assert by_kind["temperature"].value == pytest.approx(30.0)
        assert by_kind["text"].value == "world"

    @pytest.mark.asyncio
    async def test_merge_does_not_affect_bus_publish(self) -> None:
        """Merged signals (not originals) should be published to the bus."""
        bus = SignalBus()
        received: list[Signal] = []

        async def _capture(sig: Signal) -> None:
            received.append(sig)

        bus.subscribe("temperature", _capture)
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.LATEST})
        sm.register(TempSenseA(bus=bus))
        sm.register(TempSenseB(bus=bus))
        await sm.read_all()
        # Bus should receive the single merged signal
        assert len(received) == 1
        assert received[0].metadata["merge_policy"] == "latest"


# ── Backward compatibility ──────────────────────────────────────────────────

class TestMergePolicyBackwardCompat:
    @pytest.mark.asyncio
    async def test_empty_merge_policies_no_change(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={})
        sm.register(TempSenseA(bus=bus))
        sm.register(TempSenseB(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 2

    @pytest.mark.asyncio
    async def test_no_merge_policies_kwarg_backward_compat(self) -> None:
        """Existing code that doesn't pass merge_policies still works."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus)
        sm.register(TempSenseA(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert "merge_policy" not in signals[0].metadata


# ── MergePolicy enum ────────────────────────────────────────────────────────

class TestMergePolicyEnum:
    def test_all_values(self) -> None:
        assert MergePolicy.ALL.value == "all"
        assert MergePolicy.LATEST.value == "latest"
        assert MergePolicy.HIGHEST_CONFIDENCE.value == "highest_confidence"
        assert MergePolicy.MEAN.value == "mean"
        assert MergePolicy.BAYESIAN.value == "bayesian"
        assert MergePolicy.ENSEMBLE.value == "ensemble"


# ── BAYESIAN ───────────────────────────────────────────────────────────────

class TestMergePolicyBayesian:
    @pytest.mark.asyncio
    async def test_bayesian_weights_by_confidence(self) -> None:
        """Higher-confidence sensor should dominate the fused value."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.BAYESIAN})
        # Low confidence (0.3) sensor says 30, high confidence (0.95) says 40
        sm.register(TempSenseA(bus=bus, value=30.0, confidence=0.3))
        sm.register(TempSenseB(bus=bus, value=40.0, confidence=0.95))
        signals = await sm.read_all()
        assert len(signals) == 1
        # Fused value should be closer to 40 (high-confidence reading)
        assert signals[0].value > 37.0
        assert signals[0].metadata["merge_policy"] == "bayesian"

    @pytest.mark.asyncio
    async def test_bayesian_fused_confidence_higher_than_inputs(self) -> None:
        """Combined precision should yield higher confidence than any single sensor."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.BAYESIAN})
        sm.register(TempSenseA(bus=bus, value=35.0, confidence=0.7))
        sm.register(TempSenseB(bus=bus, value=36.0, confidence=0.7))
        signals = await sm.read_all()
        assert signals[0].confidence > 0.7  # Combined > individual

    @pytest.mark.asyncio
    async def test_bayesian_metadata_has_variance(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.BAYESIAN})
        sm.register(TempSenseA(bus=bus, value=30.0, confidence=0.8))
        sm.register(TempSenseB(bus=bus, value=35.0, confidence=0.9))
        signals = await sm.read_all()
        assert "fused_variance" in signals[0].metadata
        assert "per_sensor_precisions" in signals[0].metadata
        assert signals[0].metadata["sample_count"] == 2

    @pytest.mark.asyncio
    async def test_bayesian_equal_confidence_yields_mean(self) -> None:
        """Equal confidence sensors should fuse to the arithmetic mean."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.BAYESIAN})
        sm.register(TempSenseA(bus=bus, value=30.0, confidence=0.8))
        sm.register(TempSenseB(bus=bus, value=40.0, confidence=0.8))
        signals = await sm.read_all()
        assert signals[0].value == pytest.approx(35.0)

    @pytest.mark.asyncio
    async def test_bayesian_non_numeric_falls_back(self) -> None:
        """Non-numeric values fall back to HIGHEST_CONFIDENCE."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"text": MergePolicy.BAYESIAN})
        sm.register(TextSenseA(bus=bus))
        sm.register(TextSenseB(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert signals[0].value == "world"  # highest confidence
        assert signals[0].metadata["merge_fallback"] is True

    @pytest.mark.asyncio
    async def test_bayesian_single_signal_unchanged(self) -> None:
        """Single signal passes through without merging."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.BAYESIAN})
        sm.register(TempSenseA(bus=bus, value=30.0, confidence=0.8))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert "merge_policy" not in signals[0].metadata


# ── ENSEMBLE ───────────────────────────────────────────────────────────────

class TestMergePolicyEnsemble:
    @pytest.mark.asyncio
    async def test_ensemble_weights_by_confidence(self) -> None:
        """Higher-confidence sensor should have more influence."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.ENSEMBLE})
        sm.register(TempSenseA(bus=bus, value=30.0, confidence=0.2))
        sm.register(TempSenseB(bus=bus, value=40.0, confidence=0.8))
        signals = await sm.read_all()
        assert len(signals) == 1
        # Fused value should be closer to 40
        assert signals[0].value > 35.0
        assert signals[0].metadata["merge_policy"] == "ensemble"

    @pytest.mark.asyncio
    async def test_ensemble_metadata_has_uncertainty(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.ENSEMBLE})
        sm.register(TempSenseA(bus=bus, value=30.0, confidence=0.8))
        sm.register(TempSenseB(bus=bus, value=35.0, confidence=0.9))
        signals = await sm.read_all()
        meta = signals[0].metadata
        assert "epistemic_variance" in meta
        assert "aleatoric_variance" in meta
        assert "total_uncertainty" in meta
        assert meta["sample_count"] == 2

    @pytest.mark.asyncio
    async def test_ensemble_agreeing_sensors_low_epistemic(self) -> None:
        """Sensors that agree should have low epistemic variance."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.ENSEMBLE})
        sm.register(TempSenseA(bus=bus, value=35.0, confidence=0.9))
        sm.register(TempSenseB(bus=bus, value=35.0, confidence=0.9))
        signals = await sm.read_all()
        assert signals[0].metadata["epistemic_variance"] == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_ensemble_disagreeing_sensors_high_epistemic(self) -> None:
        """Sensors that disagree should have high epistemic variance."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.ENSEMBLE})
        sm.register(TempSenseA(bus=bus, value=10.0, confidence=0.9))
        sm.register(TempSenseB(bus=bus, value=50.0, confidence=0.9))
        signals = await sm.read_all()
        assert signals[0].metadata["epistemic_variance"] > 100.0

    @pytest.mark.asyncio
    async def test_ensemble_non_numeric_falls_back(self) -> None:
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"text": MergePolicy.ENSEMBLE})
        sm.register(TextSenseA(bus=bus))
        sm.register(TextSenseB(bus=bus))
        signals = await sm.read_all()
        assert len(signals) == 1
        assert signals[0].value == "world"
        assert signals[0].metadata["merge_fallback"] is True

    @pytest.mark.asyncio
    async def test_ensemble_confidence_reflects_agreement(self) -> None:
        """Agreeing, confident sensors should yield high fused confidence."""
        bus = SignalBus()
        sm = SenseMasterNode(bus=bus, merge_policies={"temperature": MergePolicy.ENSEMBLE})
        sm.register(TempSenseA(bus=bus, value=35.0, confidence=0.95))
        sm.register(TempSenseB(bus=bus, value=35.0, confidence=0.95))
        signals = await sm.read_all()
        # Low uncertainty -> high confidence
        assert signals[0].confidence > 0.8
