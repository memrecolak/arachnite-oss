"""
arachnite.nodes.sense
~~~~~~~~~~~~~~~~~~~~~
BaseSenseNode and SenseMasterNode.
Spec reference: Section 5.2.
"""

from __future__ import annotations

import asyncio
import time
from abc import abstractmethod
from collections.abc import Sequence

from arachnite.bus import SignalBus
from arachnite.config import NodeConfig
from arachnite.exceptions import NodeRegistrationError
from arachnite.logging import BaseLogSink
from arachnite.models import MergePolicy, Signal
from arachnite.nodes.base import BaseNode


class BaseSenseNode(BaseNode):
    """
    Reads from exactly one sensor or data source and emits a typed Signal.

    Developer contract:
    - Extend this class and implement read().
    - Set signal_kind to the kind string this node emits.
    - Use poll_interval_s to throttle expensive sensors.
    - Wrap blocking I/O in asyncio.to_thread().
    - Never cache state in read() — each call should be a fresh observation.

    Spec reference: Section 5.2.
    """

    #: The kind string this node emits, e.g. 'thermal', 'visual'.
    signal_kind: str = "unknown"

    #: How often to poll the sensor, in seconds.
    poll_interval_s: float = 0.1

    def __init__(
        self,
        bus: SignalBus,
        config: NodeConfig | None = None,
        log_sinks: list[BaseLogSink] | None = None,
        agent_node_id: str = "local",
        **kwargs: object,
    ) -> None:
        super().__init__(bus, config, log_sinks, agent_node_id, **kwargs)  # type: ignore[arg-type]
        self._last_read_time: float = 0.0

    @abstractmethod
    async def read(self) -> Signal | None:
        """
        Read from the sensor and return a Signal, or None if there is
        nothing to report this tick.

        Returning None is the correct way to indicate "no data" — e.g.
        a BootstrapSenseNode that fires once then becomes idle.  The
        SenseMasterNode filters out None returns automatically.

        Must be non-blocking. For blocking hardware calls, wrap in
        asyncio.to_thread(). Must not raise — return a low-confidence
        Signal on error, or delegate to on_error().
        """

    async def on_error(self, exc: Exception) -> Signal | None:
        """
        Called when read() raises an unexpected exception.

        Default: log the error and return None (signal is dropped).
        Override to produce a fallback Signal or trigger supervisor events.
        """
        self.logger.error(
            "SenseNode read error",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return None


class SenseMasterNode(BaseNode):
    """
    Owns a collection of BaseSenseNode instances.

    On each tick, calls read() on all of them concurrently, publishes the
    resulting signals to the bus, and returns the list for ContextNode.

    Spec reference: Section 5.2.
    """

    node_id = "SenseMasterNode"

    def __init__(
        self,
        bus: SignalBus,
        config: NodeConfig | None = None,
        log_sinks: list[BaseLogSink] | None = None,
        agent_node_id: str = "local",
        merge_policies: dict[str, MergePolicy] | None = None,
    ) -> None:
        super().__init__(bus, config, log_sinks, agent_node_id)
        self._nodes: dict[str, BaseSenseNode] = {}
        self._merge_policies: dict[str, MergePolicy] = merge_policies or {}

    def register(self, node: BaseSenseNode) -> None:
        """Add a SenseNode to this master. Raises NodeRegistrationError on duplicate."""
        if node.node_id in self._nodes:
            raise NodeRegistrationError(node.node_id, self.node_id)
        self._nodes[node.node_id] = node
        self.logger.debug("Registered sense node", sense_node_id=node.node_id)

    def get_node(self, node_id: str) -> BaseSenseNode | None:
        """Return a registered sense node by ID, or None"""
        return self._nodes.get(node_id)

    def unregister(self, node_id: str) -> None:
        """Remove a SenseNode by id. Silent if not found."""
        self._nodes.pop(node_id, None)

    @property
    def nodes(self) -> Sequence[BaseSenseNode]:
        return list(self._nodes.values())

    async def setup(self) -> None:
        await asyncio.gather(*(n.setup() for n in self._nodes.values()))

    async def teardown(self) -> None:
        await asyncio.gather(*(n.cancel_background_tasks() for n in self._nodes.values()))
        await asyncio.gather(*(n.teardown() for n in self._nodes.values()))

    async def on_pause(self) -> None:
        await asyncio.gather(*(n.on_pause() for n in self._nodes.values()))

    async def on_resume(self) -> None:
        await asyncio.gather(*(n.on_resume() for n in self._nodes.values()))

    async def notify_tick_start(self, tick: int) -> None:
        await asyncio.gather(*(n.on_tick_start(tick) for n in self._nodes.values()))

    async def notify_tick_end(self, tick: int, duration_s: float) -> None:
        await asyncio.gather(*(n.on_tick_end(tick, duration_s) for n in self._nodes.values()))

    async def read_all(self) -> list[Signal]:
        """
        Call read() on all SenseNodes concurrently.

        After collecting all signals, applies per-kind merge policies
        (if configured) to resolve conflicts when multiple nodes emit
        the same signal kind.  Publishes the (possibly merged) signals
        to the bus and returns the list for ContextNode.
        """
        if not self._nodes:
            return []

        start = time.monotonic()

        async def _read_one(node: BaseSenseNode) -> Signal | None:
            if (time.monotonic() - node._last_read_time) < node.poll_interval_s:
                return None
            try:
                signal = await node.read()
                node._last_read_time = time.monotonic()
                return signal
            except Exception as exc:  # noqa: BLE001
                node._last_read_time = time.monotonic()
                return await node.on_error(exc)

        results = await asyncio.gather(*(_read_one(n) for n in self._nodes.values()))
        signals: list[Signal] = [s for s in results if s is not None]

        # Apply merge policies before publishing
        if self._merge_policies and signals:
            signals = self._apply_merge(signals)

        # Publish all signals to the bus concurrently
        if signals:
            await self.bus.publish_many(signals)

        self.logger.debug(
            "read_all complete",
            signals_produced=len(signals),
            duration_ms=round((time.monotonic() - start) * 1000, 2),
        )
        return signals

    def _apply_merge(self, signals: list[Signal]) -> list[Signal]:
        """Apply configured merge policies to resolve same-kind conflicts."""
        # Group signals by kind
        by_kind: dict[str, list[Signal]] = {}
        for sig in signals:
            by_kind.setdefault(sig.kind, []).append(sig)

        merged: list[Signal] = []
        for kind, group in by_kind.items():
            policy = self._merge_policies.get(kind)
            if policy is None or policy == MergePolicy.ALL or len(group) <= 1:
                merged.extend(group)
                continue

            if policy == MergePolicy.LATEST:
                winner = max(group, key=lambda s: s.timestamp)
                merged.append(Signal(
                    source=winner.source, kind=kind,
                    value=winner.value, confidence=winner.confidence,
                    timestamp=winner.timestamp,
                    metadata={
                        **winner.metadata,
                        "merge_policy": "latest",
                        "merged_from": [s.source for s in group],
                    },
                ))

            elif policy == MergePolicy.HIGHEST_CONFIDENCE:
                winner = max(group, key=lambda s: s.confidence)
                merged.append(Signal(
                    source=winner.source, kind=kind,
                    value=winner.value, confidence=winner.confidence,
                    timestamp=winner.timestamp,
                    metadata={
                        **winner.metadata,
                        "merge_policy": "highest_confidence",
                        "merged_from": [s.source for s in group],
                    },
                ))

            elif policy == MergePolicy.MEAN:
                # Requires numeric values; fall back to HIGHEST_CONFIDENCE
                # if any value is non-numeric
                try:
                    values = [float(s.value) for s in group]
                except (TypeError, ValueError):
                    self.logger.warning(
                        "MEAN merge requires numeric values, falling back "
                        "to HIGHEST_CONFIDENCE",
                        kind=kind,
                    )
                    winner = max(group, key=lambda s: s.confidence)
                    merged.append(Signal(
                        source=winner.source, kind=kind,
                        value=winner.value, confidence=winner.confidence,
                        timestamp=winner.timestamp,
                        metadata={
                            **winner.metadata,
                            "merge_policy": "highest_confidence",
                            "merge_fallback": True,
                            "merged_from": [s.source for s in group],
                        },
                    ))
                    continue

                mean_val = sum(values) / len(values)
                mean_conf = sum(s.confidence for s in group) / len(group)
                latest = max(group, key=lambda s: s.timestamp)
                merged.append(Signal(
                    source=latest.source, kind=kind,
                    value=mean_val, confidence=mean_conf,
                    timestamp=latest.timestamp,
                    metadata={
                        "merge_policy": "mean",
                        "sample_count": len(group),
                        "merged_from": [s.source for s in group],
                    },
                ))

            elif policy == MergePolicy.BAYESIAN:
                merged.extend(self._merge_bayesian(kind, group))

            elif policy == MergePolicy.ENSEMBLE:
                merged.extend(self._merge_ensemble(kind, group))

        return merged

    def _merge_bayesian(self, kind: str, group: list[Signal]) -> list[Signal]:
        """Inverse-variance weighted fusion (Bayesian sensor fusion).

        Treats each sensor's confidence as a precision (inverse variance).
        The fused value is the precision-weighted mean, and the fused
        confidence reflects the combined precision.

        Reference: Gal & Ghahramani, "Dropout as a Bayesian Approximation,"
        ICML 2016. General Bayesian fusion: p(x|z1,z2) ∝ p(z1|x)p(z2|x)p(x).

        Requires numeric values; falls back to HIGHEST_CONFIDENCE on error.
        """
        try:
            values = [float(s.value) for s in group]
        except (TypeError, ValueError):
            self.logger.warning(
                "BAYESIAN merge requires numeric values, falling back "
                "to HIGHEST_CONFIDENCE",
                kind=kind,
            )
            winner = max(group, key=lambda s: s.confidence)
            return [Signal(
                source=winner.source, kind=kind,
                value=winner.value, confidence=winner.confidence,
                timestamp=winner.timestamp,
                metadata={
                    **winner.metadata,
                    "merge_policy": "highest_confidence",
                    "merge_fallback": True,
                    "merged_from": [s.source for s in group],
                },
            )]

        # Precision = confidence / (1 - confidence), clamped to avoid division by zero
        precisions: list[float] = []
        for s in group:
            conf = max(0.001, min(0.999, s.confidence))
            precisions.append(conf / (1.0 - conf))

        total_precision = sum(precisions)
        if total_precision == 0:
            total_precision = 1.0

        # Precision-weighted mean
        fused_value = sum(v * p for v, p in zip(values, precisions, strict=True)) / total_precision

        # Fused confidence from combined precision
        fused_confidence = total_precision / (1.0 + total_precision)

        # Uncertainty (variance) of the fused estimate
        fused_variance = 1.0 / total_precision if total_precision > 0 else float("inf")

        latest = max(group, key=lambda s: s.timestamp)
        return [Signal(
            source=latest.source, kind=kind,
            value=round(fused_value, 6),
            confidence=round(fused_confidence, 6),
            timestamp=latest.timestamp,
            metadata={
                "merge_policy": "bayesian",
                "sample_count": len(group),
                "fused_variance": round(fused_variance, 6),
                "per_sensor_precisions": [round(p, 4) for p in precisions],
                "merged_from": [s.source for s in group],
            },
        )]

    def _merge_ensemble(self, kind: str, group: list[Signal]) -> list[Signal]:
        """Confidence-weighted mean with uncertainty propagation (ensemble fusion).

        Inspired by deep ensemble uncertainty estimation (Lakshminarayanan et al.,
        "Simple and Scalable Predictive Uncertainty Estimation using Deep Ensembles,"
        NeurIPS 2017). Computes a confidence-weighted mean and estimates epistemic
        uncertainty from the spread of sensor readings.

        Requires numeric values; falls back to HIGHEST_CONFIDENCE on error.
        """
        try:
            values = [float(s.value) for s in group]
        except (TypeError, ValueError):
            self.logger.warning(
                "ENSEMBLE merge requires numeric values, falling back "
                "to HIGHEST_CONFIDENCE",
                kind=kind,
            )
            winner = max(group, key=lambda s: s.confidence)
            return [Signal(
                source=winner.source, kind=kind,
                value=winner.value, confidence=winner.confidence,
                timestamp=winner.timestamp,
                metadata={
                    **winner.metadata,
                    "merge_policy": "highest_confidence",
                    "merge_fallback": True,
                    "merged_from": [s.source for s in group],
                },
            )]

        weights = [s.confidence for s in group]
        total_weight = sum(weights)
        if total_weight == 0:
            total_weight = len(group)
            weights = [1.0] * len(group)

        # Confidence-weighted mean (aleatoric estimate)
        fused_value = sum(v * w for v, w in zip(values, weights, strict=True)) / total_weight

        # Epistemic uncertainty: variance of the ensemble predictions
        mean_val = sum(values) / len(values)
        epistemic_var = sum((v - mean_val) ** 2 for v in values) / len(values)

        # Aleatoric uncertainty: mean of individual uncertainties
        aleatoric_var = sum((1.0 - c) for c in [s.confidence for s in group]) / len(group)

        # Total uncertainty
        total_uncertainty = epistemic_var + aleatoric_var

        # Fused confidence: inverse of total uncertainty, clamped to [0, 1]
        fused_confidence = max(0.0, min(1.0, 1.0 / (1.0 + total_uncertainty)))

        latest = max(group, key=lambda s: s.timestamp)
        return [Signal(
            source=latest.source, kind=kind,
            value=round(fused_value, 6),
            confidence=round(fused_confidence, 6),
            timestamp=latest.timestamp,
            metadata={
                "merge_policy": "ensemble",
                "sample_count": len(group),
                "epistemic_variance": round(epistemic_var, 6),
                "aleatoric_variance": round(aleatoric_var, 6),
                "total_uncertainty": round(total_uncertainty, 6),
                "per_sensor_values": [round(v, 4) for v in values],
                "per_sensor_confidences": [round(s.confidence, 4) for s in group],
                "merged_from": [s.source for s in group],
            },
        )]
