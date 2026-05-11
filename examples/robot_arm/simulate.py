"""
examples/robot_arm/simulate.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Single-process simulation of the pick-and-place robot arm case study.

Runs all nodes in one ArachniteRuntime using the default LocalTransport.
At t ≈ 15 s a collision event is injected to demonstrate the reflex arc
firing mid-action, deferring its interrupt through the mandatory block,
and then triggering EmergencyRetractAction.

Run:
    python examples/robot_arm/simulate.py

Expected output:
  - Several successful pick-and-place cycles (distance drifts into range)
  - At t≈15 s: collision injected → reflex fires → mandatory block completes
    → EmergencyRetractAction executes → state resets → cycles resume
"""

from __future__ import annotations

import asyncio
import time

from arachnite import (
    ArachniteRuntime,
    ContextNode,
    DecisionMasterNode,
    ActionMasterNode,
    InstinctMasterNode,
    SenseMasterNode,
    SignalBus,
    StdoutLogSink,
    LogLevel,
    WeightedDecisionNode,
)

from examples.robot_arm.nodes import (
    CollisionReflex,
    EmergencyRetractAction,
    GraspInstinct,
    JointPositionSenseNode,
    ObjectDetectionSenseNode,
    PickAndPlaceAction,
    ProximitySenseNode,
    SIM,
)

_RUN_SECONDS       = 35.0
_COLLISION_AT      = 15.0   # seconds after start — inject collision event
_COLLISION_CLEARED = False


async def _inject_collision(rt: ArachniteRuntime) -> None:
    """Background task: sets collision flag at t=COLLISION_AT."""
    global _COLLISION_CLEARED
    await asyncio.sleep(_COLLISION_AT)
    print("\n" + "=" * 60)
    print(f"  [t={_COLLISION_AT:.0f}s] *** COLLISION INJECTED -- proximity -> 0.02 m")
    print("=" * 60 + "\n")
    SIM.collision_imminent = True
    # The reflex will clear it in EmergencyRetractAction.execute()
    await asyncio.sleep(_RUN_SECONDS - _COLLISION_AT)
    await rt.stop()


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


async def main() -> None:
    log_sinks = [StdoutLogSink(level=LogLevel.WARNING)]  # only warnings + errors
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

    action_master.register(PickAndPlaceAction(bus=bus, log_sinks=log_sinks))
    action_master.register(EmergencyRetractAction(bus=bus, log_sinks=log_sinks))

    rt = ArachniteRuntime(
        sense_master    = sense_master,
        context         = ContextNode(),
        instinct_master = instinct_master,
        decision_master = decision_master,
        action_master   = action_master,
        bus             = bus,
        tick_rate_hz    = 10.0,
        log_sinks       = log_sinks,
    )

    print("Robot arm simulation -- pick-and-place + collision reflex")
    print(f"Running for {_RUN_SECONDS:.0f} s at 10 Hz.  "
          f"Collision injected at t={_COLLISION_AT:.0f} s.")
    print("-" * 60)
    print(f"  {'t':>6}  {'dist':>8}  {'gripper':>8}  {'holding':>8}  "
          f"{'picks':>6}  {'emergencies':>12}")
    print("-" * 60)

    await rt.start()
    asyncio.create_task(_inject_collision(rt))
    asyncio.create_task(_status_printer())

    try:
        await rt.wait()
    except KeyboardInterrupt:
        await rt.stop()

    print("-" * 60)
    print(f"Simulation complete.")
    print(f"  Ticks executed : {rt.tick_count}")
    print(f"  Picks completed: {SIM.pick_count}")
    print(f"  Emergencies    : {SIM.emergency_count}")
    print(f"  Final state    : gripper={'closed' if SIM.gripper > 0.5 else 'open'}, "
          f"holding={SIM.holding}")


if __name__ == "__main__":
    asyncio.run(main())
