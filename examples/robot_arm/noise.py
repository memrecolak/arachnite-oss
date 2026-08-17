"""
examples/robot_arm/noise.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Noise-mode support for the robot-arm simulation (revision experiment E5).

Mirrors ``baselines.shared_sim``: additive Gaussian noise on every sensed
proximity reading, plus a seeded uniform actuation delay at the two
completion points the baselines also perturb (pick release and emergency
retract). Composes with ``nodes.BENCHMARK_MODE``: the cross-framework
harness (``baselines.compare --noise``) enables both for the noisy arm.

Lives in its own module so the task-encoding LoC count (``nodes.py`` +
``simulate.py`` + ``manifest.yaml``, see ``baselines.compare.loc_analysis``)
is not inflated by simulation infrastructure.
"""

from __future__ import annotations

import asyncio
import random

NOISE_MODE: bool = False
NOISE_SENSOR_SIGMA_M: float = 0.02
NOISE_ACTUATION_DELAY_MS: tuple[float, float] = (1.0, 5.0)
NOISE_SEED: int = 20260812
_noise_rng: random.Random | None = None


def _rng() -> random.Random:
    global _noise_rng
    if _noise_rng is None:
        _noise_rng = random.Random(NOISE_SEED)
    return _noise_rng


def reset_noise_rng(offset: int = 0) -> None:
    """Re-seed the noise stream to ``NOISE_SEED + offset``.

    Call once per independent run with the run index as offset, so every
    run draws a distinct but reproducible noise sequence (matching the
    per-JVM seeding of the Jason arm).
    """
    global _noise_rng
    _noise_rng = random.Random(NOISE_SEED + offset)


def apply_sensor_noise(dist: float) -> float:
    if NOISE_MODE:
        return max(0.0, dist + _rng().gauss(0.0, NOISE_SENSOR_SIGMA_M))
    return dist


async def actuation_delay() -> None:
    """Noise-mode actuation latency; async so the tick loop stays live."""
    if NOISE_MODE:
        lo, hi = NOISE_ACTUATION_DELAY_MS
        await asyncio.sleep(_rng().uniform(lo, hi) / 1_000.0)
