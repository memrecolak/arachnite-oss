"""
arachnite.nodes.active_inference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Active inference decision strategy for Arachnite.

Implements a simplified active inference agent that selects proposals by
minimising *expected free energy* (EFE) — a quantity that trades off
goal-directed behaviour (pragmatic value) against information-seeking
behaviour (epistemic value).

Theoretical basis:
  - Friston, "The free-energy principle: A unified brain theory?"
    Nature Reviews Neuroscience, 11(2), 127-138, 2010.
  - Friston et al., "Active inference: A process theory,"
    Neural Computation, 29(1), 1-49, 2017.
  - Pezzato et al., "Active inference and behavior trees for reactive
    action planning and execution in robotics," IEEE T-RO, 39(2), 2023.

The key insight from active inference is that an agent should prefer
actions that both achieve goals AND reduce uncertainty about the world.
This maps naturally to Arachnite's Proposal model:
  - priority    -> pragmatic value (goal relevance)
  - urgency     -> temporal discount (how soon must this be done)
  - confidence  -> inverse epistemic value (high confidence = less to learn)
  - evidence    -> observations supporting the proposal

The EFE for a proposal is:
  G(p) = -pragmatic(p) + epistemic(p)
       = -(priority * urgency) + beta * (1 - avg_confidence)

The agent selects the proposal with the LOWEST EFE (most negative = best).

When beta=0, this reduces to the WeightedDecisionNode.
Because the uncertainty term ``beta * (1 - avg_confidence)`` *adds* to G,
a larger beta penalises low-confidence proposals: between proposals of
similar pragmatic value the agent prefers the better-evidenced one. The
term is bounded by ``[0, beta]`` while ``priority * urgency`` spans
``0..priority``, so at ``beta = 1`` it only separates proposals whose
pragmatic values differ by less than 1 (in practice: a tie-breaker).

Evidence confidences are validated: a non-finite value (NaN, inf) or a
non-number is ignored, and a finite one is clamped to [0, 1], so a
malformed evidence entry can neither win the argmin by list position nor
turn the uncertainty penalty into a bonus.
"""

from __future__ import annotations

import math
import random
import sys

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from arachnite.models import Proposal
from arachnite.nodes.decision import BaseDecisionNode


class ActiveInferenceDecisionNode(BaseDecisionNode):
    """
    Selects proposals by minimising expected free energy (EFE).

    EFE(p) = -pragmatic_value(p) + beta * epistemic_value(p)

    Where:
      pragmatic_value = priority * urgency  (goal-directed)
      epistemic_value = 1 - avg_confidence  (information gain)

    Lower EFE is better (selected).

    Args:
        beta: Weight of the uncertainty term ``1 - avg_confidence``.
              0.0 = pragmatic value only (equivalent to WeightedDecisionNode).
              >0.0 = low-confidence proposals are penalised by up to beta.
              Must be finite and >= 0.
        temperature: Softmax temperature for probabilistic selection.
              0.0 = deterministic (argmin EFE).
              >0.0 = probabilistic (softmax over -EFE). Must be finite and >= 0.
        prior_confidence: Default confidence when proposal has no (valid)
              evidence. Must be in [0, 1].
        seed: Seed for the softmax sampling (temperature > 0), giving a
              reproducible sequence of choices; None uses the module-level
              ``random`` generator.
    """

    node_id = "ActiveInferenceDecisionNode"

    def __init__(
        self,
        beta: float = 1.0,
        temperature: float = 0.0,
        prior_confidence: float = 0.5,
        seed: int | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        if not (math.isfinite(beta) and beta >= 0.0):
            raise ValueError(f"beta must be finite and >= 0, got {beta}")
        if not (math.isfinite(temperature) and temperature >= 0.0):
            raise ValueError(f"temperature must be finite and >= 0, got {temperature}")
        if not 0.0 <= prior_confidence <= 1.0:  # also rejects NaN
            raise ValueError(f"prior_confidence must be in [0, 1], got {prior_confidence}")
        self.beta = beta
        self.temperature = temperature
        self.prior_confidence = prior_confidence
        self._rng: random.Random | None = random.Random(seed) if seed is not None else None

    def _pragmatic_value(self, p: Proposal) -> float:
        """Goal-directed value: how well does this proposal serve objectives."""
        return p.priority * p.urgency

    def _epistemic_value(self, p: Proposal) -> float:
        """Information gain: how much uncertainty does this action context have.

        High epistemic value = high uncertainty. Computed from the
        ``*_confidence`` entries of the proposal's evidence: non-numbers
        (including bools) and non-finite values are ignored, finite values
        are clamped to [0, 1]; the result is always in [0, 1].
        """
        confidences: list[float] = []

        # Extract confidence from evidence if available
        if p.evidence:
            for key, val in p.evidence.items():
                if (
                    key.endswith("_confidence")
                    and isinstance(val, (int, float))
                    and not isinstance(val, bool)
                    and math.isfinite(val)
                ):
                    confidences.append(min(1.0, max(0.0, float(val))))

        if not confidences:
            # Fall back to prior: moderate uncertainty
            return 1.0 - self.prior_confidence

        avg_conf = sum(confidences) / len(confidences)
        return 1.0 - avg_conf

    def _expected_free_energy(self, p: Proposal) -> float:
        """Compute EFE for a proposal. Lower is better."""
        pragmatic = self._pragmatic_value(p)
        epistemic = self._epistemic_value(p)
        return -pragmatic + self.beta * epistemic

    @override
    async def decide(self, proposals: list[Proposal]) -> Proposal | None:
        if not proposals:
            return None

        efes = [(p, self._expected_free_energy(p)) for p in proposals]

        if self.temperature <= 0.0:
            # Deterministic: select minimum EFE
            return min(efes, key=lambda x: x[1])[0]

        # Probabilistic: softmax over negative EFE (higher = more likely)
        neg_efes = [-efe for _, efe in efes]
        max_neg = max(neg_efes)
        # Numerically stable softmax
        exp_vals = [math.exp((v - max_neg) / self.temperature) for v in neg_efes]
        total = sum(exp_vals)
        probs = [e / total for e in exp_vals]

        rng = self._rng
        if rng is None:
            return random.choices([p for p, _ in efes], weights=probs, k=1)[0]
        return rng.choices([p for p, _ in efes], weights=probs, k=1)[0]
