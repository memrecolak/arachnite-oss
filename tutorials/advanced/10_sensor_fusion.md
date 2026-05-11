# Lesson 10 — Sensor Fusion: Combining Multiple Readings

When your agent has two temperature sensors, three cameras, or a mix of
overlapping sensors, you need a way to combine their readings into a single,
reliable signal. Arachnite calls this **merge policies**.

## The Problem

Imagine two temperature sensors on a robot arm:

```python
class TempSensorA(BaseSenseNode):
    node_id = "TempA"
    signal_kind = "temperature"
    async def read(self) -> Signal:
        return Signal(source=self.node_id, kind="temperature",
                      value=36.5, confidence=0.8, timestamp=time.monotonic())

class TempSensorB(BaseSenseNode):
    node_id = "TempB"
    signal_kind = "temperature"
    async def read(self) -> Signal:
        return Signal(source=self.node_id, kind="temperature",
                      value=37.1, confidence=0.95, timestamp=time.monotonic())
```

Without a merge policy, both signals reach every instinct. The instinct has to
figure out which one to trust. With a merge policy, `SenseMasterNode` resolves
the conflict *before* signals reach the bus.

## Basic Policies

```python
from arachnite import SenseMasterNode, MergePolicy

sm = SenseMasterNode(bus=bus, merge_policies={
    "temperature": MergePolicy.MEAN,
})
```

| Policy | What it does | When to use |
|--------|-------------|-------------|
| `ALL` (default) | Keeps every signal | When instincts need to see each sensor individually |
| `LATEST` | Keeps the newest timestamp | Fast-changing readings where only the latest matters |
| `HIGHEST_CONFIDENCE` | Keeps the most confident | One sensor is clearly more reliable |
| `MEAN` | Averages values and confidences | Simple redundancy |

## Bayesian Fusion

When sensors have very different reliability, a simple average is misleading.
`BAYESIAN` fusion weights each sensor by its **precision** (how confident it is):

```python
sm = SenseMasterNode(bus=bus, merge_policies={
    "temperature": MergePolicy.BAYESIAN,
})
```

How it works:
- A sensor with confidence 0.95 has precision 19.0 (= 0.95 / 0.05)
- A sensor with confidence 0.5 has precision 1.0 (= 0.5 / 0.5)
- The fused value is the precision-weighted mean
- The fused confidence is *higher* than any individual sensor (combined knowledge)

The merged signal's metadata tells you what happened:

```python
signal.metadata = {
    "merge_policy": "bayesian",
    "sample_count": 2,
    "fused_variance": 0.049,
    "per_sensor_precisions": [4.0, 19.0],
    "merged_from": ["TempA", "TempB"],
}
```

**When to use:** When you trust confident sensors more than uncertain ones, and
you want the fused result to be *more* reliable than any single sensor.

## Ensemble Fusion

`ENSEMBLE` fusion goes further — it separates two kinds of uncertainty:

- **Epistemic** (disagreement): sensors give different readings
- **Aleatoric** (individual): each sensor has its own noise

```python
sm = SenseMasterNode(bus=bus, merge_policies={
    "temperature": MergePolicy.ENSEMBLE,
})
```

The metadata breaks down the uncertainty:

```python
signal.metadata = {
    "merge_policy": "ensemble",
    "epistemic_variance": 0.18,     # sensors disagree
    "aleatoric_variance": 0.15,     # individual uncertainty
    "total_uncertainty": 0.33,
    "per_sensor_values": [36.5, 37.1],
    "per_sensor_confidences": [0.8, 0.95],
}
```

**When to use:** When you want instincts (especially `ActiveInferenceDecisionNode`)
to know *why* a reading is uncertain — is it because sensors disagree (maybe
one is broken) or because all sensors are inherently noisy?

## Choosing a Policy

| Scenario | Recommended policy |
|----------|-------------------|
| Redundant sensors, equal quality | `MEAN` |
| Sensors with very different reliability | `BAYESIAN` |
| Need to detect sensor disagreement | `ENSEMBLE` |
| Only care about the latest reading | `LATEST` |
| Instincts need raw per-sensor data | `ALL` |

## Non-Numeric Values

`BAYESIAN` and `ENSEMBLE` require numeric `Signal.value`. If you configure them
for a non-numeric kind (like text), they automatically fall back to
`HIGHEST_CONFIDENCE` and set `metadata["merge_fallback"] = True`.

## Next Steps

- **Lesson 4** covers decision strategies — `ActiveInferenceDecisionNode` can
  use the epistemic/aleatoric metadata from `ENSEMBLE` fusion to make smarter
  exploration-vs-exploitation decisions.
- **Lesson 11** covers the active inference strategy in detail.
