/**
 * baselines/jason/ArmState.java
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * Java port of baselines/shared_sim.py:ArmState. Identical conveyor-drift
 * model and identical state transitions, so the Jason baseline measures
 * the same scenario as Arachnite and py_trees.
 */

import java.util.ArrayList;
import java.util.List;

public class ArmState {

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
        if (pickStartNanos < 0
                && !holding
                && objectDistance >= 0.10
                && objectDistance <= 0.35) {
            pickStartNanos = System.nanoTime();
        }
        return objectDistance;
    }

    public void emergencyRetract() {
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
