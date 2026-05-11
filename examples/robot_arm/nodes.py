"""
examples/robot_arm/nodes.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Simulated pick-and-place robot arm nodes for the Arachnite case study.

All sensor values are produced by a deterministic physics stub (ArmState).
No real hardware is required.  Timing measurements therefore reflect
pure framework overhead, not hardware I/O latency.

Architecture
------------
Two logical AgentNodes (see manifest.yaml):

  vision-node   ProximitySenseNode, ObjectDetectionSenseNode
  control-node  JointPositionSenseNode,
                CollisionReflex  (priority 250, reflex)  ← co-located with
                EmergencyRetractAction                       EmergencyRetract
                GraspInstinct    (priority 80,  normal)
                PickAndPlaceAction (MultiStepActionNode, ROLLBACK policy)

Mandatory completion block in PickAndPlaceAction:
  lower_gripper (1.0 s) + close_gripper (0.5 s) + raise_gripper (1.0 s)
  Worst-case interrupt latency for this block = 2.5 s (sum of timeout_s).
"""

from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass, field

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from arachnite import (
    ActionStep,
    BaseActionNode,
    BaseInstinctNode,
    BaseReflexInstinctNode,
    BaseSenseNode,
    Context,
    InterruptPolicy,
    Proposal,
    Result,
    Signal,
    StepResult,
)
from arachnite.nodes.action import MultiStepActionNode

# ── Physics stub ──────────────────────────────────────────────────────────────

@dataclass
class ArmState:
    """
    Shared simulation state.

    Sensors read from this object; actions write to it.
    In a real deployment each field maps to a hardware register or ROS topic.
    """
    joints:              list[float] = field(default_factory=lambda: [0.0] * 6)
    gripper:             float       = 0.0    # 0.0 = open, 1.0 = closed
    object_distance:     float       = 1.5    # metres to nearest object
    object_x:            float       = 0.50
    object_y:            float       = 0.00
    object_z:            float       = 0.30
    holding:             bool        = False
    collision_imminent:  bool        = False  # set externally to trigger reflex
    start_time:          float       = field(default_factory=time.monotonic)
    pick_count:          int         = 0
    emergency_count:     int         = 0


# Module-level singleton — shared by all nodes in a single-process simulation.
SIM = ArmState()


# Benchmark harnesses (e.g. benchmarks.active_inference_comparison) set this
# to True before running so the simulated-hardware ``asyncio.sleep`` calls
# inside PickAndPlaceAction and EmergencyRetractAction are skipped.
# Standalone runs of the example (examples/robot_arm/simulate.py) keep
# BENCHMARK_MODE == False so the demo still reflects realistic robot timing.
BENCHMARK_MODE: bool = False


async def _sim_sleep(seconds: float) -> None:
    """Simulated-hardware delay; bypassed when ``BENCHMARK_MODE`` is True."""
    if not BENCHMARK_MODE:
        await asyncio.sleep(seconds)


# ── Sense nodes ───────────────────────────────────────────────────────────────

class ProximitySenseNode(BaseSenseNode):
    """
    Reports distance (metres) to the nearest object in the workspace.

    The object drifts from 1.5 m to ~0.15 m over ~20 s (conveyor belt),
    then resets after each successful pick.
    """
    node_id     = "ProximitySenseNode"
    signal_kind = "proximity"

    @override
    async def read(self) -> Signal:
        if SIM.collision_imminent:
            dist = 0.02
        elif SIM.holding:
            dist = 1.5   # object is in gripper, no obstacle
        elif BENCHMARK_MODE:
            # Demo conveyor pacing (~20 s wall time per cycle) is too slow for
            # tick-bound benchmarks; with BENCHMARK_MODE the workload is
            # sampled at tick rate, so picks must fire every cycle.
            dist = 0.20
        else:
            elapsed = time.monotonic() - SIM.start_time
            dist = max(0.10, 1.5 - elapsed * 0.067)
        SIM.object_distance = round(dist, 4)
        return Signal(
            source    = self.node_id,
            kind      = self.signal_kind,
            value     = SIM.object_distance,
            confidence= 0.99,
            timestamp = time.monotonic(),
        )


class ObjectDetectionSenseNode(BaseSenseNode):
    """
    Reports whether a graspable object is within reach (< 0.35 m).

    value=1.0 means object detected; 0.0 means workspace clear.
    metadata carries the estimated Cartesian position.
    """
    node_id     = "ObjectDetectionSenseNode"
    signal_kind = "object_detected"

    @override
    async def read(self) -> Signal:
        in_range = (0.10 <= SIM.object_distance <= 0.35) and not SIM.holding
        return Signal(
            source    = self.node_id,
            kind      = self.signal_kind,
            value     = 1.0 if in_range else 0.0,
            confidence= 0.95,
            timestamp = time.monotonic(),
            metadata  = {
                "x": SIM.object_x,
                "y": SIM.object_y,
                "z": SIM.object_z,
                "distance": SIM.object_distance,
            },
        )


class JointPositionSenseNode(BaseSenseNode):
    """
    Reports the scalar total joint displacement (sum of |angle| for all joints).

    In a real system this would return a 6-element vector; here we use a
    scalar to keep Signal.value a float.  Full joint state lives in metadata.
    """
    node_id     = "JointPositionSenseNode"
    signal_kind = "joint_position"

    @override
    async def read(self) -> Signal:
        displacement = sum(abs(j) for j in SIM.joints)
        return Signal(
            source    = self.node_id,
            kind      = self.signal_kind,
            value     = round(displacement, 3),
            confidence= 1.0,
            timestamp = time.monotonic(),
            metadata  = {
                "joints":  list(SIM.joints),
                "gripper": SIM.gripper,
                "holding": SIM.holding,
            },
        )


# ── Instinct nodes ────────────────────────────────────────────────────────────

class CollisionReflex(BaseReflexInstinctNode):
    """
    Reflex: fires when proximity < 0.05 m (collision imminent).

    Bypasses DecisionMasterNode entirely.
    MUST be co-located with EmergencyRetractAction (same AgentNode).
    priority = 250 (≥ 200 required for reflexes).
    """
    node_id  = "CollisionReflex"
    priority = 250

    @override
    async def evaluate(self, ctx: Context) -> Proposal | None:
        prox = [s for s in ctx.signals if s.kind == "proximity"]
        if prox and prox[-1].value < 0.05:
            return Proposal(
                instinct_id = self.node_id,
                action_id   = "EmergencyRetractAction",
                priority    = self.priority,
                urgency     = 1.0,
                parameters  = {"distance": prox[-1].value},
                rationale   = f"Collision imminent at {prox[-1].value:.3f} m",
            )
        return None


class GraspInstinct(BaseInstinctNode):
    """
    Normal instinct: proposes a pick-and-place cycle when an object
    enters the reachable zone.

    priority = 80 (goal-directed range 50–99).
    """
    node_id  = "GraspInstinct"
    priority = 80

    @override
    async def evaluate(self, ctx: Context) -> Proposal | None:
        detections = [s for s in ctx.signals if s.kind == "object_detected"]
        if not detections or detections[-1].value != 1.0:
            return None
        if SIM.holding:
            return None
        meta = detections[-1].metadata or {}
        return Proposal(
            instinct_id = self.node_id,
            action_id   = "PickAndPlaceAction",
            priority    = self.priority,
            urgency     = 0.7,
            parameters  = {
                "target_x": 0.0,
                "target_y": 0.6,
                "target_z": 0.1,
                "object_x": meta.get("x", SIM.object_x),
                "object_y": meta.get("y", SIM.object_y),
                "object_z": meta.get("z", SIM.object_z),
            },
            rationale   = f"Object at {meta.get('distance', 0):.2f} m — initiating grasp",
        )


# ── Action nodes ──────────────────────────────────────────────────────────────

class PickAndPlaceAction(MultiStepActionNode):
    """
    Six-step pick-and-place sequence.

    Mandatory completion block: lower_gripper + close_gripper + raise_gripper
    These three steps must run to completion once started — interrupting mid-grip
    would leave the arm in an unsafe configuration.

    Worst-case interrupt latency = sum(timeout_s of mandatory steps)
                                 = 1.0 + 0.5 + 1.0 = 2.5 seconds.

    Uses ROLLBACK policy: if interrupted after the mandatory block,
    the rollback callables restore hardware to a safe state.
    """
    node_id          = "PickAndPlaceAction"
    interrupt_policy = InterruptPolicy.ROLLBACK
    timeout_s        = 10.0

    @override
    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("move_to_object",
                       interruptible=True,
                       timeout_s=2.0),
            ActionStep("lower_gripper",
                       interruptible=False,
                       rollback=self._raise_gripper,
                       timeout_s=1.0),
            ActionStep("close_gripper",
                       interruptible=False,
                       rollback=self._open_gripper,
                       timeout_s=0.5),
            ActionStep("raise_gripper",
                       interruptible=False,
                       rollback=self._lower_gripper,
                       timeout_s=1.0),
            ActionStep("move_to_target",
                       interruptible=True,
                       timeout_s=2.0),
            ActionStep("release_gripper",
                       interruptible=True,
                       timeout_s=0.5),
        ]

    @override
    async def execute_step(
        self,
        step:      ActionStep,
        proposal:  Proposal,
        completed: list[StepResult],
    ) -> StepResult:
        match step.name:
            case "move_to_object":
                self.logger.info("Moving to object", distance=SIM.object_distance)
                await _sim_sleep(0.05)         # sim: 50 ms travel
                SIM.joints = [10.0, -20.0, 30.0, -10.0, 5.0, 0.0]
                return StepResult(step_name="move_to_object", success=True,
                                  output={"joints": list(SIM.joints)})

            case "lower_gripper":
                self.logger.info("Lowering gripper (mandatory)")
                await _sim_sleep(0.02)         # sim: 20 ms
                SIM.joints[1] -= 15.0
                return StepResult(step_name="lower_gripper", success=True)

            case "close_gripper":
                self.logger.info("Closing gripper (mandatory)")
                await _sim_sleep(0.01)         # sim: 10 ms
                SIM.gripper  = 1.0
                SIM.holding  = True
                return StepResult(step_name="close_gripper", success=True,
                                  output={"gripper": 1.0})

            case "raise_gripper":
                self.logger.info("Raising gripper (mandatory)")
                await _sim_sleep(0.02)         # sim: 20 ms
                SIM.joints[1] += 15.0
                return StepResult(step_name="raise_gripper", success=True)

            case "move_to_target":
                tx = proposal.parameters.get("target_x", 0.0)
                ty = proposal.parameters.get("target_y", 0.6)
                self.logger.info("Moving to target", target_x=tx, target_y=ty)
                await _sim_sleep(0.05)         # sim: 50 ms travel
                SIM.joints = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
                return StepResult(step_name="move_to_target", success=True)

            case "release_gripper":
                self.logger.info("Releasing object at target")
                await _sim_sleep(0.01)
                SIM.gripper  = 0.0
                SIM.holding  = False
                SIM.pick_count += 1
                SIM.start_time = time.monotonic()  # reset conveyor cycle
                return StepResult(step_name="release_gripper", success=True,
                                  output={"pick_count": SIM.pick_count})

            case _:
                return StepResult(step_name=step.name, success=False,
                                  error=ValueError(f"Unknown step: {step.name}"))

    # ── Rollback callables ────────────────────────────────────────────────────

    async def _raise_gripper(self) -> None:
        """Undo lower_gripper: restore joint angle."""
        self.logger.warning("Rollback: raising gripper")
        await _sim_sleep(0.02)
        SIM.joints[1] += 15.0

    async def _open_gripper(self) -> None:
        """Undo close_gripper: release whatever was grabbed."""
        self.logger.warning("Rollback: opening gripper")
        await _sim_sleep(0.01)
        SIM.gripper = 0.0
        SIM.holding = False

    async def _lower_gripper(self) -> None:
        """Undo raise_gripper: lower back to safe position."""
        self.logger.warning("Rollback: lowering gripper")
        await _sim_sleep(0.02)
        SIM.joints[1] -= 15.0


class EmergencyRetractAction(BaseActionNode):
    """
    Reflex target: immediately resets all joints to the home position
    and opens the gripper.

    Co-location rule: must be on the same AgentNode as CollisionReflex.
    timeout_s is short — this must complete before the next tick.
    """
    node_id   = "EmergencyRetractAction"
    timeout_s = 1.0

    @override
    async def execute(self, proposal: Proposal) -> Result:
        dist = proposal.parameters.get("distance", "?")
        self.logger.error(
            "EMERGENCY RETRACT -- collision imminent",
            distance=dist,
        )
        # Immediate home: zero all joints, open gripper
        SIM.joints   = [0.0] * 6
        SIM.gripper  = 0.0
        SIM.holding  = False
        SIM.collision_imminent = False
        SIM.start_time = time.monotonic()
        SIM.emergency_count += 1
        await _sim_sleep(0.005)      # sim: 5 ms retract
        return Result(
            action_id = self.node_id,
            success   = True,
            output    = {
                "distance":        dist,
                "emergency_count": SIM.emergency_count,
            },
        )
