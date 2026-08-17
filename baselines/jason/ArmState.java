/**
 * baselines/jason/ArmState.java
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * Java port of baselines/shared_sim.py:ArmState. Identical conveyor-drift
 * model and identical state transitions, so the Jason baseline measures
 * the same scenario as Arachnite and py_trees.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ArmState {

    // Noise mode (revision experiment E5) — mirrors baselines/shared_sim.py:
    // Gaussian sensor noise on every sensed distance plus a seeded uniform
    // actuation delay at the two shared completion points. Enabled with
    // -Darachnite.noise=true; constants must match the Python side.
    static final boolean NOISE = Boolean.getBoolean("arachnite.noise");
    static final double NOISE_SENSOR_SIGMA_M = 0.02;
    static final double NOISE_DELAY_MIN_MS = 1.0;
    static final double NOISE_DELAY_MAX_MS = 5.0;
    // Per-run seed passed by run_jason.py as NOISE_SEED + run index, so each
    // independent run draws a distinct but reproducible noise sequence.
    static final long NOISE_SEED = Long.getLong("arachnite.noise.seed", 20260812L);
    private final Random noiseRng = new Random(NOISE_SEED);

    public double[] joints = new double[]{0, 0, 0, 0, 0, 0};
    public double gripper = 0.0;
    public double objectDistance = 1.5;
    public boolean holding = false;
    public boolean collisionImminent = false;
    public long startNanos = System.nanoTime();
    public int pickCount = 0;
    public int emergencyCount = 0;
    // Per-pick wall-clock instrumentation. Mirrors
    // baselines/shared_sim.py:ArmState — pickStartNanos is set when the
    // object first enters the grasp window with holding=false and no pick
    // in flight; pickComplete() records elapsed ms in pickDurationsMs and
    // resets pickStartNanos to -1. Gives a per-framework distribution of
    // "object-detected -> object-released" wall times that is directly
    // comparable across all four baselines (unlike per-cycle latency,
    // which counts a different unit of work per framework).
    public long pickStartNanos = -1L;
    public final List<Double> pickDurationsMs = new ArrayList<>();

    /**
     * Benchmark-mode distance model: object is always within pick range (0.20 m)
     * when not held and no collision is imminent. The real-time conveyor drift
     * (0.067 m/s) requires ~17 s per pick cycle, making 10k-action Jason runs
     * infeasible. This model gives continuous pick-and-place execution so the
     * benchmark measures pure BDI reasoning overhead without idle-wait inflation.
     *
     * Side effect: starts the per-pick stopwatch the first time the object
     * becomes visibly graspable after the previous release.
     */
    public double updateDistance() {
        if (collisionImminent) {
            objectDistance = 0.02;
        } else if (holding) {
            objectDistance = 1.5;
        } else {
            objectDistance = 0.20;
        }
        if (NOISE) {
            // Sensor noise applies to every reading, including the collision
            // distance — the noisy-threshold robustness case of E5.
            objectDistance = Math.max(
                0.0, objectDistance + noiseRng.nextGaussian() * NOISE_SENSOR_SIGMA_M);
        }
        if (pickStartNanos < 0
                && !holding
                && objectDistance >= 0.10
                && objectDistance <= 0.35) {
            pickStartNanos = System.nanoTime();
        }
        return objectDistance;
    }

    /** Noise-mode actuation latency at the shared completion points. */
    private void actuationDelay() {
        if (!NOISE) {
            return;
        }
        double ms = NOISE_DELAY_MIN_MS
                + noiseRng.nextDouble() * (NOISE_DELAY_MAX_MS - NOISE_DELAY_MIN_MS);
        try {
            Thread.sleep((long) ms, (int) ((ms % 1.0) * 1_000_000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void emergencyRetract() {
        actuationDelay();
        joints = new double[]{0, 0, 0, 0, 0, 0};
        gripper = 0.0;
        holding = false;
        collisionImminent = false;
        startNanos = System.nanoTime();
        emergencyCount++;
        // Discard the in-flight pick stopwatch — the pick was aborted.
        pickStartNanos = -1L;
    }

    public void pickComplete() {
        actuationDelay();
        gripper = 0.0;
        holding = false;
        pickCount++;
        startNanos = System.nanoTime();
        if (pickStartNanos >= 0) {
            pickDurationsMs.add((System.nanoTime() - pickStartNanos) / 1e6);
            pickStartNanos = -1L;
        }
    }
}
