"""
Shared pytest fixtures for Arachnite tests.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque

import pytest
import pytest_asyncio

from arachnite import ContextNode, SignalBus
from arachnite.models import (
    ActionStep,
    Context,
    InterruptPolicy,
    Proposal,
    Result,
    Signal,
    StepResult,
)
from arachnite.nodes.action import (
    ActionMasterNode,
    BaseActionNode,
    MultiStepActionNode,
)
from arachnite.nodes.decision import DecisionMasterNode, GreedyDecisionNode
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import ArachniteRuntime

# ── Shared helpers ────────────────────────────────────────────────────────────

def make_signal(
    kind: str = "thermal",
    value: float = 20.0,
    confidence: float = 1.0,
    source: str = "test",
) -> Signal:
    return Signal(
        source=source,
        kind=kind,
        value=value,
        confidence=confidence,
        timestamp=time.monotonic(),
    )


def make_context(
    signals: list[Signal] | None = None,
    tick: int = 1,
) -> Context:
    return Context(
        tick        = tick,
        signals     = signals or [],
        history     = deque(),
        state       = {},
        last_result = None,
        timestamp   = time.monotonic(),
    )


def make_proposal(
    action_id: str = "TestAction",
    priority: int = 50,
    urgency: float = 0.5,
    instinct_id: str = "test_instinct",
) -> Proposal:
    return Proposal(
        instinct_id = instinct_id,
        action_id   = action_id,
        priority    = priority,
        urgency     = urgency,
    )


# ── Concrete test nodes ───────────────────────────────────────────────────────

class ConstantSenseNode(BaseSenseNode):
    """Emits a constant value each tick."""
    node_id     = "ConstantSenseNode"
    signal_kind = "thermal"

    def __init__(self, bus: SignalBus, value: float = 25.0, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self._value = value

    async def read(self) -> Signal:
        return make_signal(kind=self.signal_kind, value=self._value)


class SlowSenseNode(BaseSenseNode):
    """Sensor that sleeps during read() to simulate slow hardware"""
    node_id     = "SlowSenseNode"
    signal_kind = "thermal"
    poll_interval_s = 0.0

    def __init__(
        self, bus: SignalBus, delay: float = 0.05, **kwargs: object
    ) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self._delay = delay

    async def read(self) -> Signal:
        await asyncio.sleep(self._delay)
        return make_signal(kind=self.signal_kind, value=99.0)


class ThresholdInstinct(BaseInstinctNode):
    """Fires when thermal signal exceeds threshold."""
    node_id  = "ThresholdInstinct"
    priority = 100

    def __init__(self, bus: SignalBus, threshold: float = 80.0, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.threshold = threshold

    async def evaluate(self, ctx: Context) -> Proposal | None:
        thermal = [s for s in ctx.signals if s.kind == "thermal"]
        if thermal and thermal[-1].value > self.threshold:
            return make_proposal(
                action_id   = "CoolDownAction",
                priority    = self.priority,
                instinct_id = self.node_id,
            )
        return None


class EmergencyReflex(BaseReflexInstinctNode):
    """Reflex: fires when thermal > critical_threshold."""
    node_id  = "EmergencyReflex"
    priority = 200

    def __init__(
        self, bus: SignalBus, critical_threshold: float = 95.0, **kwargs: object
    ) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.critical = critical_threshold

    async def evaluate(self, ctx: Context) -> Proposal | None:
        thermal = [s for s in ctx.signals if s.kind == "thermal"]
        if thermal and thermal[-1].value > self.critical:
            return make_proposal(
                action_id   = "EmergencyStop",
                priority    = self.priority,
                urgency     = 1.0,
                instinct_id = self.node_id,
            )
        return None


class RecordingAction(BaseActionNode):
    """Records all proposals it receives."""
    node_id = "RecordingAction"

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.calls: list[Proposal] = []

    async def execute(self, proposal: Proposal) -> Result:
        self.calls.append(proposal)
        return Result(action_id=self.node_id, success=True, output=proposal.parameters)


class FailingAction(BaseActionNode):
    """Always returns a failed Result."""
    node_id = "FailingAction"

    async def execute(self, proposal: Proposal) -> Result:
        return Result(
            action_id = self.node_id,
            success   = False,
            error     = RuntimeError("Intentional failure"),
        )


class TwoStepAction(MultiStepActionNode):
    """Simple two-step action: step1 (interruptible) → step2 (interruptible)."""
    node_id          = "TwoStepAction"
    interrupt_policy = InterruptPolicy.ALWAYS

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step1", interruptible=True),
            ActionStep("step2", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        await asyncio.sleep(0.02)  # simulate async work so interrupt tasks can fire
        return StepResult(step_name=step.name, success=True, output=step.name)


class MandatoryBlockAction(MultiStepActionNode):
    """Three-step action with a mandatory middle block."""
    node_id          = "MandatoryBlockAction"
    interrupt_policy = InterruptPolicy.ROLLBACK

    def __init__(self, bus: SignalBus, **kwargs: object) -> None:
        super().__init__(bus, **kwargs)  # type: ignore[arg-type]
        self.rolled_back: list[str] = []

    async def _undo_step2(self) -> None:
        self.rolled_back.append("step2")

    def steps(self) -> list[ActionStep]:
        return [
            ActionStep("step1", interruptible=True),
            ActionStep("step2", interruptible=False, rollback=self._undo_step2),
            ActionStep("step3", interruptible=True),
        ]

    async def execute_step(
        self, step: ActionStep, proposal: Proposal, completed: list[StepResult]
    ) -> StepResult:
        return StepResult(step_name=step.name, success=True, output=step.name)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def bus() -> SignalBus:
    return SignalBus()


@pytest.fixture
def context() -> ContextNode:
    return ContextNode(history_length=5)


@pytest.fixture
def sense_master(bus: SignalBus) -> SenseMasterNode:
    return SenseMasterNode(bus=bus)


@pytest.fixture
def instinct_master(bus: SignalBus) -> InstinctMasterNode:
    return InstinctMasterNode(bus=bus)


@pytest.fixture
def decision_master(bus: SignalBus) -> DecisionMasterNode:
    return DecisionMasterNode(bus=bus, strategy=GreedyDecisionNode(bus=bus))


@pytest.fixture
def action_master(bus: SignalBus) -> ActionMasterNode:
    return ActionMasterNode(bus=bus)


@pytest.fixture
def recording_action(bus: SignalBus) -> RecordingAction:
    return RecordingAction(bus=bus)


@pytest_asyncio.fixture
async def runtime(
    bus:             SignalBus,
    context:         ContextNode,
    sense_master:    SenseMasterNode,
    instinct_master: InstinctMasterNode,
    decision_master: DecisionMasterNode,
    action_master:   ActionMasterNode,
    recording_action: RecordingAction,
) -> ArachniteRuntime:
    sense_master.register(ConstantSenseNode(bus=bus, value=25.0))
    instinct_master.register(ThresholdInstinct(bus=bus, threshold=80.0))
    action_master.register(recording_action)

    rt = ArachniteRuntime(
        sense_master    = sense_master,
        context         = context,
        instinct_master = instinct_master,
        decision_master = decision_master,
        action_master   = action_master,
        bus             = bus,
        tick_rate_hz    = 100.0,
    )
    await rt.start()
    yield rt
    await rt.stop()
