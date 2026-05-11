/**
 * baselines/jason/RobotArmEnv.java
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * Jason environment for the pick-and-place simulation.
 *
 * Two roles:
 *   1. Maps AgentSpeak internal actions to the shared ArmState physics stub
 *      so the Jason baseline runs the same scenario as Arachnite and py_trees.
 *   2. Times the BDI cycle by recording wall-clock between successive
 *      executeAction() calls. In Jason's basic execution mode each
 *      reasoning cycle produces at most one external action, so per-action
 *      latency is the natural per-cycle latency analogue.
 *
 * The env writes JSONL per-cycle latencies to a file (path passed via the
 * "out" init arg) so the parent Python process can parse them without
 * fighting Jason's own stdout chatter. After `cycles + warmup` actions the
 * env calls System.exit(0) to terminate the JVM; the parent process then
 * reads the JSONL file.
 */

import jason.asSyntax.*;
import jason.environment.Environment;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;

public class RobotArmEnv extends Environment {

    private final ArmState sim = new ArmState();

    private long lastActionNanos = -1L;
    private int actionsSeen = 0;
    private int warmup = 1_000;
    private int cycles = 10_000;
    private int injectCollisionAt = -1;
    private BufferedWriter out;

    @Override
    public void init(String[] args) {
        // Args are quoted strings in the .mas2j ("out=<path>", "cycles=<n>", …)
        // Jason strips the surrounding quotes before passing to init().
        // Strip any residual quotes defensively.
        for (String raw : args) {
            String a = raw.replaceAll("^\"|\"$", "");
            String[] kv = a.split("=", 2);
            if (kv.length != 2) continue;
            switch (kv[0]) {
                case "out":     openOut(kv[1]); break;
                case "cycles":  cycles = Integer.parseInt(kv[1]); break;
                case "warmup":  warmup = Integer.parseInt(kv[1]); break;
                case "inject":  injectCollisionAt = Integer.parseInt(kv[1]); break;
                default: getLogger().warning("Unknown init arg: " + a);
            }
        }
        if (out == null) {
            getLogger().log(Level.SEVERE, "RobotArmEnv requires out=<path> init arg");
            System.exit(2);
        }
        updatePercepts();
    }

    private void openOut(String path) {
        try {
            out = new BufferedWriter(new FileWriter(path, false));
        } catch (IOException e) {
            getLogger().log(Level.SEVERE, "Cannot open " + path, e);
            System.exit(3);
        }
    }

    @Override
    public boolean executeAction(String agName, Structure action) {
        long now = System.nanoTime();

        // Apply the action to the physics stub.
        String functor = action.getFunctor();
        switch (functor) {
            case "home_joints":   sim.emergencyRetract(); break;
            case "open_gripper":
                if (sim.holding) sim.pickComplete();
                else { sim.gripper = 0.0; sim.holding = false; }
                break;
            case "close_gripper": sim.gripper = 1.0; sim.holding = true; break;
            case "move_to_object": sim.joints = new double[]{10, -20, 30, -10, 5, 0}; break;
            case "lower_gripper":  sim.joints[1] -= 15.0; break;
            case "raise_gripper":  sim.joints[1] += 15.0; break;
            case "move_to_target": sim.joints = new double[]{0, 0, 0, 0, 0, 0}; break;
            default:
                getLogger().warning("Unknown action: " + functor);
                return false;
        }

        // Record per-cycle latency once the warm-up window has passed.
        if (lastActionNanos >= 0 && actionsSeen >= warmup) {
            double latencyMs = (now - lastActionNanos) / 1e6;
            int idx = actionsSeen - warmup;
            try {
                out.write(String.format(Locale.US,
                    "{\"i\": %d, \"latency_ms\": %.6f, \"action\": \"%s\"}%n",
                    idx, latencyMs, functor));
            } catch (IOException e) {
                getLogger().log(Level.SEVERE, "Write failed", e);
            }
        }
        lastActionNanos = now;
        actionsSeen++;

        // Optional collision injection at a fixed cycle index (post-warmup).
        if (injectCollisionAt >= 0 && actionsSeen - warmup == injectCollisionAt) {
            sim.collisionImminent = true;
        }

        updatePercepts();

        // Stop after the requested number of measured actions.
        if (actionsSeen >= warmup + cycles) {
            try {
                // Final summary record so the Python harness can read pickCount
                // and emergencyCount back without an out-of-band channel —
                // the JVM is about to call System.exit(0) and the parent
                // can't query the live ArmState. Distinguished from per-cycle
                // latency records by the "summary":true marker; old harnesses
                // that don't recognise the marker will skip it cleanly because
                // the line has no "latency_ms" field.
                //
                // pick_durations_ms is the cross-framework comparable column:
                // each entry is one full pick cycle (object-detected ->
                // object-released) in wall-clock ms. Same trigger condition
                // as ArmState in shared_sim.py and examples/robot_arm/nodes.py.
                StringBuilder picks = new StringBuilder();
                picks.append('[');
                for (int i = 0; i < sim.pickDurationsMs.size(); i++) {
                    if (i > 0) picks.append(',');
                    picks.append(String.format(Locale.US, "%.6f", sim.pickDurationsMs.get(i)));
                }
                picks.append(']');
                out.write(String.format(
                    "{\"summary\": true, \"pick_count\": %d, \"emergency_count\": %d, \"pick_durations_ms\": %s}%n",
                    sim.pickCount, sim.emergencyCount, picks.toString()));
                out.flush();
                out.close();
            } catch (IOException ignored) { }
            // Hard exit: Jason's centralised runner has no clean public stop API.
            System.exit(0);
        }
        return true;
    }

    // Called every reasoning cycle so the agent always sees live simulation state.
    @Override
    public Collection<Literal> getPercepts(String agName) {
        sim.updateDistance();
        return Arrays.asList(
            Literal.parseLiteral("distance(" + sim.objectDistance + ")"),
            Literal.parseLiteral("collision(" + sim.collisionImminent + ")"),
            Literal.parseLiteral("gripper(" + (sim.gripper > 0.5 ? "closed" : "open") + ")"),
            Literal.parseLiteral("holding(" + (sim.holding ? "object" : "nothing") + ")")
        );
    }

    private void updatePercepts() {
        clearPercepts();
        sim.updateDistance();
        addPercept(Literal.parseLiteral("distance(" + sim.objectDistance + ")"));
        addPercept(Literal.parseLiteral("collision(" + sim.collisionImminent + ")"));
        addPercept(Literal.parseLiteral("gripper(" + (sim.gripper > 0.5 ? "closed" : "open") + ")"));
        addPercept(Literal.parseLiteral("holding(" + (sim.holding ? "object" : "nothing") + ")"));
    }

    public int getPickCount()      { return sim.pickCount; }
    public int getEmergencyCount() { return sim.emergencyCount; }
}
