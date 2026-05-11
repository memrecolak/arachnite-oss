/**
 * baselines/jason/robot_arm.asl
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * Equivalent pick-and-place robot arm controller in Jason AgentSpeak.
 *
 * This implements the same scenario as examples/robot_arm/ using the BDI
 * model. The agent has beliefs about sensor state, desires to pick objects,
 * and intentions that form plans.
 *
 * Key differences from Arachnite:
 *   - No reflex arc: collision handling is a high-priority plan, but it
 *     goes through the full BDI reasoning cycle (belief revision -> option
 *     generation -> deliberation -> means-end reasoning).
 *   - No mandatory completion blocks: plans can be dropped between steps,
 *     but there is no structured rollback mechanism.
 *   - No tick-based execution: Jason uses an event-driven reasoning cycle.
 *   - JVM required: cannot run on constrained edge devices (4 GB Jetson Nano).
 *
 * Requirements:
 *   - Jason 3.x (https://jason-lang.github.io/)
 *   - Java 11+
 *
 * Run:
 *   jason baselines/jason/robot_arm.mas2j
 *
 * Lines of code: ~90 (AgentSpeak) + ~120 (Java environment) = ~210 total
 */

// ── Initial beliefs ─────────────────────────────────────────────────────────

distance(1.5).
gripper(open).
holding(nothing).
collision(false).
joints([0.0, 0.0, 0.0, 0.0, 0.0, 0.0]).

// ── Perception rules ────────────────────────────────────────────────────────

// Collision detection: highest priority
+collision(true)
  : true
  <- !emergency_retract.

// Object in reach: initiate pick-and-place
+distance(D)
  : D >= 0.10 & D <= 0.35 & holding(nothing) & not collision(true)
  <- !pick_and_place.

// ── Plans ───────────────────────────────────────────────────────────────────

// Emergency retract plan — goes through full BDI cycle (no bypass)
+!emergency_retract
  : true
  <- .print("EMERGENCY: collision imminent — retracting");
     home_joints;
     open_gripper;
     -collision(true);
     +collision(false);
     .print("Emergency retract complete").

// Pick-and-place plan — no mandatory block, no rollback
+!pick_and_place
  : holding(nothing)
  <- .print("Starting pick-and-place");
     move_to_object;
     // NOTE: In Arachnite, the next 3 steps form a mandatory block
     // that cannot be interrupted. In Jason, any belief change between
     // steps can trigger plan reconsideration, potentially dropping
     // this intention mid-grip.
     lower_gripper;
     close_gripper;
     +holding(object);
     -holding(nothing);
     raise_gripper;
     move_to_target;
     open_gripper;
     -holding(object);
     +holding(nothing);
     .print("Pick-and-place complete").

// Failure handling — no structured rollback
-!pick_and_place
  : true
  <- .print("Pick-and-place FAILED — no rollback available");
     home_joints;
     open_gripper.

-!emergency_retract
  : true
  <- .print("Emergency retract FAILED").
