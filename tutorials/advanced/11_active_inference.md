# Lesson 11 — Active Inference: Smarter Decision-Making

The built-in decision strategies (Greedy, Weighted, Random) pick actions based
on priority and urgency. But what if your agent should also consider *how
uncertain* it is about the world? That's where active inference comes in.

## The Idea

Active inference is a theory from neuroscience (Karl Friston, 2010) that says
biological agents do two things at once:

1. **Exploit** — do things that achieve goals (high priority, high urgency)
2. **Explore** — do things that reduce uncertainty about the world

The `ActiveInferenceDecisionNode` combines both into a single score called
**Expected Free Energy (EFE)**:

```
EFE(proposal) = -pragmatic_value + beta * epistemic_value
```

- **Pragmatic value** = priority * urgency (how useful is this action?)
- **Epistemic value** = 1 - average_confidence (how uncertain is the context?)
- **beta** = how much to weight exploration vs exploitation

Lower EFE is better. The agent picks the proposal with the lowest EFE.

## Basic Usage

```python
from arachnite import (
    DecisionMasterNode, ActiveInferenceDecisionNode, SignalBus
)

bus = SignalBus()
strategy = ActiveInferenceDecisionNode(
    bus=bus,
    beta=1.0,          # balanced exploration/exploitation
    temperature=0.0,   # deterministic (pick the best)
)
dm = DecisionMasterNode(bus=bus, strategy=strategy)
```

## The beta Parameter

`beta` controls the exploration-exploitation trade-off:

| beta | Behaviour |
|------|-----------|
| 0.0 | Pure exploitation — identical to `WeightedDecisionNode` |
| 1.0 | Balanced — considers both goal value and uncertainty |
| 5.0+ | Exploration-biased — prefers acting in uncertain contexts |

## How Confidence Flows Through

The agent extracts confidence from the `evidence` field in proposals.
Any key ending in `_confidence` is treated as a sensor confidence:

```python
class PatrolInstinct(BaseInstinctNode):
    async def evaluate(self, ctx) -> Proposal | None:
        camera = [s for s in ctx.signals if s.kind == "camera"]
        if camera:
            return Proposal(
                instinct_id=self.node_id,
                action_id="PatrolAction",
                priority=50, urgency=0.5,
                evidence={
                    "camera_confidence": camera[0].confidence,
                    "lidar_confidence": 0.3,  # low confidence = uncertain
                },
            )
        return None
```

With `beta=0`, the agent ignores the confidence values.
With `beta=5`, the agent notices the low lidar confidence and may prefer
actions that would improve lidar readings first.

## Probabilistic Selection

Set `temperature > 0` to make selection probabilistic (softmax):

```python
strategy = ActiveInferenceDecisionNode(
    bus=bus,
    beta=1.0,
    temperature=10.0,  # higher = more random
)
```

This is useful when you want the agent to occasionally try lower-ranked
actions, avoiding getting stuck in local optima.

## Combining with Ensemble Fusion

This strategy pairs naturally with `MergePolicy.ENSEMBLE` from Lesson 10.
Ensemble fusion produces `epistemic_variance` and `aleatoric_variance` in
signal metadata. An instinct can pass these through as evidence:

```python
evidence={
    "sensor_confidence": signal.confidence,
    # Low confidence when sensors disagree
}
```

The active inference agent will then prefer actions in contexts where
sensors agree (low epistemic variance) over contexts where sensors
disagree (high epistemic variance).

## When to Use

| Scenario | Strategy |
|----------|----------|
| Simple priority-based agent | `GreedyDecisionNode` |
| Priority * urgency weighting | `WeightedDecisionNode` |
| Exploratory/creative agent | `RandomDecisionNode` |
| Agent that balances goals and uncertainty | `ActiveInferenceDecisionNode` |

## Next Steps

- **Lesson 12** covers runtime safety monitors — how to verify your agent
  is behaving correctly while it runs.
- **Lesson 13** covers benchmarking — how to measure your agent's performance.
