"""
examples/robot_arm/simulate.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Single-process simulation of the pick-and-place robot arm case study.

Runs all nodes in one ArachniteRuntime using the default LocalTransport.
Actions run as tasks while the tick loop keeps sensing (ADR 0005), so the
collision reflex is evaluated even while a pick is in progress.

Two collision scenarios:

  --collide-at SECONDS   inject a collision at a fixed wall-clock time
                         (default 15 s; the arm is usually idle then)
  --collide-in-block     inject the collision the moment PickAndPlaceAction
                         starts raise_gripper — inside its mandatory block,
                         with the object already gripped

In-block: the PREEMPT reflex cancels raise_gripper mid-step within ~1 tick,
on_preempted() freezes the arm and keeps the grip (no rollback), and
EmergencyRetractAction retracts the arm with the object still held. Once the
hazard has cleared, PlaceHeldObjectInstinct places the object. Nothing is
dropped.

Run:
    python -m examples.robot_arm.simulate
    python -m examples.robot_arm.simulate --collide-in-block --reflex-mode event
"""

from __future__ import annotations

import argparse
import asyncio
import time

from arachnite import (
    ActionMasterNode,
    ArachniteRuntime,
    BaseLogSink,
    ContextNode,
    DecisionMasterNode,
    InstinctMasterNode,
    LogLevel,
    SenseMasterNode,
    SignalBus,
    StdoutLogSink,
    WeightedDecisionNode,
)
from examples.robot_arm.nodes import (
    SIM,
    CollisionReflex,
    EmergencyRetractAction,
    GraspInstinct,
    JointPositionSenseNode,
    ObjectDetectionSenseNode,
    PickAndPlaceAction,
    PlaceHeldObjectAction,
    PlaceHeldObjectInstinct,
    ProximitySenseNode,
)

_RUN_SECONDS = 35.0
# After _RUN_SECONDS the session drains: it keeps ticking until no action is running and
# the gripper is empty, so the shutdown never cuts a pick in half (an interrupted pick is
# rolled back, which opens the gripper and counts as a dropped object). Bounded so a
# stuck action cannot hold the demo open.
_DRAIN_MAX_SECONDS = 15.0


async def _collide_at(seconds: float) -> None:
    """Background task: sets the collision flag at a fixed time."""
    await asyncio.sleep(seconds)
    print("\n" + "=" * 60)
    print(f"  [t={seconds:.0f}s] *** COLLISION INJECTED -- proximity -> 0.02 m")
    print("=" * 60 + "\n")
    SIM.collision_imminent = True   # EmergencyRetractAction clears it


async def _collide_in_block(pick: PickAndPlaceAction) -> None:
    """Background task: sets the collision flag as raise_gripper starts."""
    while not (pick._current_step and pick._current_step.name == "raise_gripper"):
        await asyncio.sleep(0.001)
    print("\n" + "=" * 60)
    print("  *** COLLISION INJECTED inside the mandatory block (raise_gripper)")
    print("=" * 60 + "\n")
    SIM.collision_imminent = True


async def _status_printer() -> None:
    """Prints arm state every second."""
    t0 = time.monotonic()
    while True:
        await asyncio.sleep(1.0)
        elapsed = time.monotonic() - t0
        gripper_str = "CLOSED" if SIM.gripper > 0.5 else "open  "
        holding_str = "holding" if SIM.holding else "       "
        print(
            f"  t={elapsed:5.1f}s  "
            f"dist={SIM.object_distance:.3f}m  "
            f"gripper={gripper_str}  {holding_str}  "
            f"picks={SIM.pick_count}  "
            f"emergencies={SIM.emergency_count}"
        )


async def main(collide_at: float, collide_in_block: bool, reflex_mode: str) -> None:
    log_sinks: list[BaseLogSink] = [StdoutLogSink(level=LogLevel.WARNING)]  # only warnings + errors
    bus = SignalBus()

    sense_master    = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(
        bus=bus, strategy=WeightedDecisionNode(bus=bus)
    )
    action_master   = ActionMasterNode(bus=bus)

    sense_master.register(ProximitySenseNode(bus=bus, log_sinks=log_sinks))
    sense_master.register(ObjectDetectionSenseNode(bus=bus, log_sinks=log_sinks))
    sense_master.register(JointPositionSenseNode(bus=bus, log_sinks=log_sinks))

    instinct_master.register(CollisionReflex(bus=bus, log_sinks=log_sinks))
    instinct_master.register(GraspInstinct(bus=bus, log_sinks=log_sinks))
    instinct_master.register(PlaceHeldObjectInstinct(bus=bus, log_sinks=log_sinks))

    pick = PickAndPlaceAction(bus=bus, log_sinks=log_sinks)
    action_master.register(pick)
    action_master.register(EmergencyRetractAction(bus=bus, log_sinks=log_sinks))
    action_master.register(PlaceHeldObjectAction(bus=bus, log_sinks=log_sinks))

    rt = ArachniteRuntime(
        sense_master    = sense_master,
        context         = ContextNode(),
        instinct_master = instinct_master,
        decision_master = decision_master,
        action_master   = action_master,
        bus             = bus,
        tick_rate_hz    = 10.0,
        log_sinks       = log_sinks,
        reflex_mode     = reflex_mode,
    )

    scenario = (
        "inside the mandatory block" if collide_in_block else f"at t={collide_at:.0f} s"
    )
    print("Robot arm simulation -- pick-and-place + collision reflex")
    print(f"Running for {_RUN_SECONDS:.0f} s at 10 Hz (then until the arm is idle), "
          f"reflex_mode={reflex_mode}. Collision {scenario}.")
    print("-" * 60)

    await rt.start()
    injector = (
        _collide_in_block(pick) if collide_in_block else _collide_at(collide_at)
    )
    tasks = [asyncio.create_task(injector), asyncio.create_task(_status_printer())]

    try:
        await asyncio.sleep(_RUN_SECONDS)
        loop = asyncio.get_running_loop()
        drain_until = loop.time() + _DRAIN_MAX_SECONDS
        while (action_master.running_action_ids() or SIM.holding) and loop.time() < drain_until:
            await asyncio.sleep(0.05)
    finally:
        await rt.stop()
        for task in tasks:
            task.cancel()

    print("-" * 60)
    print("Simulation complete.")
    print(f"  Ticks executed : {rt.tick_count}")
    print(f"  Picks completed: {SIM.pick_count}")
    print(f"  Emergencies    : {SIM.emergency_count}")
    print(f"  Preemptions    : {SIM.preempt_count}")
    print(f"  Objects dropped: {SIM.object_drops}")
    print(f"  Final state    : gripper={'closed' if SIM.gripper > 0.5 else 'open'}, "
          f"holding={SIM.holding}")


def _parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Robot-arm pick-and-place simulation")
    parser.add_argument("--collide-at", type=float, default=15.0,
                        help="inject the collision at this time (s)")
    parser.add_argument("--collide-in-block", action="store_true",
                        help="inject the collision as raise_gripper starts")
    parser.add_argument("--reflex-mode", choices=("tick", "event"), default="tick")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse()
    asyncio.run(main(args.collide_at, args.collide_in_block, args.reflex_mode))
