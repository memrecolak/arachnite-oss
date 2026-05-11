"""
examples/minimal_agent.py
~~~~~~~~~~~~~~~~~~~~~~~~~
The simplest complete Arachnite agent.

Pipeline:
    TempSense (always reads 42 °C)
    → HotInstinct (fires when temp > 40 °C)
    → CoolDown (prints a message)

Run:
    python examples/minimal_agent.py
"""

from __future__ import annotations

import asyncio
import time

from arachnite import (
    ArachniteRuntime,
    SignalBus,
    ContextNode,
    Signal,
    Proposal,
    Result,
    BaseSenseNode,
    SenseMasterNode,
    BaseInstinctNode,
    InstinctMasterNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    BaseActionNode,
    ActionMasterNode,
)


# ── 1. Sense node ─────────────────────────────────────────────────────────────
# Extend BaseSenseNode and implement read().
# Set signal_kind to the string other nodes will filter on.

class TempSense(BaseSenseNode):
    node_id     = "TempSense"
    signal_kind = "temperature"

    async def read(self) -> Signal:
        # In a real agent, read from hardware here (must be async).
        return Signal(
            source     = self.node_id,
            kind       = self.signal_kind,
            value      = 42.0,
            confidence = 1.0,
            timestamp  = time.monotonic(),
        )


# ── 2. Instinct node ──────────────────────────────────────────────────────────
# Extend BaseInstinctNode and implement evaluate().
# Return a Proposal when this instinct wants to act; return None otherwise.

class HotInstinct(BaseInstinctNode):
    node_id  = "HotInstinct"
    priority = 80  # 50-99 = goal-directed

    async def evaluate(self, ctx) -> Proposal | None:  # type: ignore[override]
        hot = [s for s in ctx.signals if s.kind == "temperature" and s.value > 40.0]
        if hot:
            return Proposal(
                instinct_id = self.node_id,
                action_id   = "CoolDown",   # must match CoolDown.node_id
                priority    = self.priority,
                urgency     = 0.8,
                parameters  = {"reading": hot[-1].value},
            )
        return None  # explicit None when not applicable


# ── 3. Action node ────────────────────────────────────────────────────────────
# Extend BaseActionNode and implement execute().
# Always return a Result — never raise.

class CoolDown(BaseActionNode):
    node_id   = "CoolDown"
    timeout_s = 5.0

    async def execute(self, proposal) -> Result:  # type: ignore[override]
        reading = proposal.parameters.get("reading", "?")
        print(f"[CoolDown] Cooling down — last reading: {reading} °C")
        return Result(action_id=self.node_id, success=True)


# ── 4. Wire up and run ────────────────────────────────────────────────────────

async def main() -> None:
    bus             = SignalBus()
    sense_master    = SenseMasterNode(bus=bus)
    instinct_master = InstinctMasterNode(bus=bus)
    decision_master = DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))
    action_master   = ActionMasterNode(bus=bus)

    sense_master.register(TempSense(bus=bus))
    instinct_master.register(HotInstinct(bus=bus))
    action_master.register(CoolDown(bus=bus))

    rt = ArachniteRuntime(
        sense_master    = sense_master,
        context         = ContextNode(),
        instinct_master = instinct_master,
        decision_master = decision_master,
        action_master   = action_master,
        bus             = bus,
        tick_rate_hz    = 2.0,   # 2 ticks per second
    )

    print("Starting agent. Press Ctrl+C to stop.")
    await rt.start()
    try:
        await asyncio.sleep(5.0)
    except asyncio.CancelledError:
        pass
    finally:
        await rt.stop()
    print(f"Done. Ran {rt.tick_count} ticks.")


if __name__ == "__main__":
    asyncio.run(main())
