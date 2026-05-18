"""
arachnite.models
~~~~~~~~~~~~~~~~
All data models that flow between nodes: Signal, Context, Proposal, Result,
ActionStep, StepResult, InterruptPolicy, InterruptRequest, ActionExecutionState.
Spec reference: Sections 3, 17.
"""

from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# ── Sentinel for required config values ───────────────────────────────────────

class _Required:
    """Sentinel used by NodeConfig to mark required keys with no default."""
    _instance: _Required | None = None

    def __new__(cls) -> _Required:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "REQUIRED"


REQUIRED = _Required()


# ══════════════════════════════════════════════════════════════════════════════
# Core pipeline models
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(slots=True)
class Signal:
    """
    A typed, timestamped data packet emitted by a SenseNode onto the SignalBus.
    Spec reference: Section 3.1.
    """
    source:     str          # node_id of the SenseNode that produced this
    kind:       str          # e.g. 'thermal', 'visual', 'audio', 'supervisor'
    value:      Any          # raw reading — type defined by the SenseNode
    confidence: float        # 0.0 to 1.0
    timestamp:  float        # time.monotonic() at read time
    metadata:   dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if math.isnan(self.confidence) or math.isinf(self.confidence):
            raise ValueError(
                f"Signal.confidence must be a finite number in [0.0, 1.0], "
                f"got {self.confidence}"
            )
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(
                f"Signal.confidence must be in [0.0, 1.0], got {self.confidence}"
            )


@dataclass(slots=True)
class Context:
    """
    A snapshot assembled by ContextNode each tick.
    Passed to every InstinctNode. Spec reference: Section 3.2.

    The plural fields ``last_results`` and ``action_states`` support
    concurrent action execution (multiple ActionNodes running in
    parallel).  The singular ``last_result`` and ``action_state``
    fields remain for backward compatibility and reflect the
    highest-priority item from the plural lists.
    """
    tick:         int
    signals:      list[Signal]
    history:      deque[list[Signal]]
    state:        dict[str, Any]
    last_result:  Result | None
    timestamp:    float
    action_state: ActionExecutionState | None = None
    last_results:  list[Result] = field(default_factory=list)
    action_states: list[ActionExecutionState] = field(default_factory=list)


@dataclass(slots=True)
class Proposal:
    """
    A recommendation from an InstinctNode.
    Spec reference: Section 3.3.

    The ``evidence`` field lets instincts attach supporting data that
    flows through the decision and action layers.  Typical entries::

        evidence={
            "camera_path": "/tmp/arachnite/media/cam_tick42.jpg",
            "camera_summary": "Person detected at entrance (0.95)",
            "audio_path": "/tmp/arachnite/media/mic_tick42.wav",
            "audio_summary": "Doorbell sound detected",
        }

    Decision strategies can inspect ``evidence`` to make context-aware
    choices beyond raw priority/urgency numbers.  Action nodes receive
    the evidence via the proposal and can load referenced files.

    The ``persist`` flag controls whether the proposal survives across
    ticks.  When ``persist=True`` and the proposal is not selected by
    the DecisionNode, it is carried forward to the next tick's decision
    pool.  A persistent proposal is superseded when the same instinct
    produces a new proposal, cleared when the instinct is re-evaluated
    and returns None, or dropped after ``max_pending_ticks`` (configured
    on DecisionMasterNode).  Default is ``False`` (current behavior —
    lost if not selected).
    """
    instinct_id: str    # which InstinctNode generated this
    action_id:   str    # target ActionNode.node_id
    priority:    int    # higher = more urgent; 200+ reflex, 100-199 safety, etc.
    urgency:     float  # normalised 0.0-1.0, used by weighted strategies
    parameters:  dict[str, Any] = field(default_factory=dict)
    rationale:   str = ""
    evidence:    dict[str, Any] = field(default_factory=dict)
    persist:     bool = False

    def __post_init__(self) -> None:
        if not 0.0 <= self.urgency <= 1.0:
            raise ValueError(
                f"Proposal.urgency must be in [0.0, 1.0], got {self.urgency}"
            )


@dataclass(slots=True)
class Result:
    """
    The outcome of an executed action.
    Fed back into the next tick's Context. Spec reference: Section 3.4.
    """
    action_id:    str
    success:      bool
    output:       Any = None
    error:        BaseException | None = None
    duration_s:   float = 0.0
    # Multi-step fields (None / empty for single-step BaseActionNode)
    interrupted:      bool = False
    stopped_at_step:  str | None = None
    step_results:     list[StepResult] = field(default_factory=list)
    rolled_back:      bool = False


# ══════════════════════════════════════════════════════════════════════════════
# Multi-step action models
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(slots=True)
class StepResult:
    """
    The outcome of a single ActionStep execution.
    Spec reference: Section 17.2.
    """
    step_name:      str
    success:        bool
    output:         Any = None
    error:          BaseException | None = None
    duration_s:     float = 0.0
    abort_sequence: bool = False   # step requests immediate sequence abort


class InterruptPolicy(Enum):
    """
    Governs how a MultiStepActionNode responds to an interrupt request.
    Spec reference: Section 17.3.
    """
    ALWAYS     = "always"      # stop at next interruptible step boundary
    NEVER      = "never"       # run to completion (fixed action pattern)
    CHECKPOINT = "checkpoint"  # stop only at explicit checkpoint steps
    ROLLBACK   = "rollback"    # stop + undo completed non-interruptible steps


@dataclass(slots=True)
class ActionStep:
    """
    The atomic unit of a MultiStepActionNode.
    Spec reference: Section 17.1.
    """
    name:          str
    interruptible: bool = True
    rollback:      Callable[[], Awaitable[None]] | None = None
    timeout_s:     float | None = None   # None = inherit node default
    checkpoint:    bool = False          # used by CHECKPOINT policy
    metadata:      dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InterruptRequest:
    """
    Issued by DecisionMasterNode to ActionMasterNode when a higher-priority
    proposal arrives while an action is executing.
    Spec reference: Section 17.7.
    """
    new_proposal:           Proposal
    requesting_instinct_id: str
    reason:                 str = ""
    timestamp:              float = field(default_factory=time.monotonic)


@dataclass(slots=True)
class ActionExecutionState:
    """
    Snapshot of the currently running action, injected into Context.
    Allows InstinctNodes to reason about what the agent is currently doing.
    Spec reference: Section 17.9.
    """
    action_id:                   str | None  # None if no action is running
    current_step:                str | None
    completed_steps:             list[str]
    interruptible:               bool
    mandatory_block_remaining_s: float       # worst-case time to next safe stop


# ══════════════════════════════════════════════════════════════════════════════
# Supervisor models
# ══════════════════════════════════════════════════════════════════════════════

class NodeState(Enum):
    """
    Lifecycle state of a supervised node.
    Spec reference: Section 6.1.
    """
    STARTING   = "starting"
    RUNNING    = "running"
    FAULTED    = "faulted"
    RESTARTING = "restarting"
    STOPPED    = "stopped"
    DEAD       = "dead"


class RestartPolicy(Enum):
    """
    Governs whether and how a NodeSupervisor restarts a faulted node.
    Spec reference: Section 6.3.
    """
    NEVER      = "never"       # go straight to DEAD
    ON_FAILURE = "on_failure"  # restart only on unhandled exception
    ALWAYS     = "always"      # restart on any non-STOPPED exit


class MergePolicy(Enum):
    """
    Controls how multiple signals of the same kind are merged within one tick.

    Configured per signal-kind on SenseMasterNode. Kinds without a policy
    default to ALL (every signal passes through unmerged).
    Spec reference: Section 5.2.
    """
    ALL                 = "all"                  # keep all signals (default)
    LATEST              = "latest"               # keep signal with latest timestamp
    HIGHEST_CONFIDENCE  = "highest_confidence"   # keep signal with highest confidence
    MEAN                = "mean"                  # average numeric values and confidences
    BAYESIAN            = "bayesian"             # inverse-variance weighted fusion
    ENSEMBLE            = "ensemble"             # confidence-weighted mean with uncertainty


class Permission(Enum):
    """
    Capabilities a node may declare it requires.

    Validated at startup against a deployment whitelist — zero runtime cost.
    If no whitelist is configured, validation is skipped entirely (opt-in).
    """
    NETWORK          = "network"
    FILESYSTEM_READ  = "filesystem_read"
    FILESYSTEM_WRITE = "filesystem_write"
    SUBPROCESS       = "subprocess"
    GPU              = "gpu"


@dataclass(slots=True)
class SupervisorSignal(Signal):
    """
    A Signal of kind='supervisor' emitted by NodeSupervisor on state changes.
    Spec reference: Section 6.4.
    """
    node_id:        str = ""
    previous_state: NodeState = NodeState.STARTING
    current_state:  NodeState = NodeState.RUNNING
    restart_count:  int = 0
    fault_error:    BaseException | None = None

    def __post_init__(self) -> None:
        # Override kind to always be 'supervisor'
        object.__setattr__(self, "kind", "supervisor")


@dataclass(slots=True)
class NodeFaultSignal(SupervisorSignal):
    """
    Typed signal emitted when a node enters FAULTED or DEAD state.

    Subscribers can listen for kind='node_fault' on the bus to react
    specifically to faults without filtering all supervisor signals.
    Carries the error type and message for pattern-matching in instincts.
    """
    error_type: str = ""
    error_message: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", "node_fault")
        if self.fault_error is not None:
            if not self.error_type:
                object.__setattr__(self, "error_type", type(self.fault_error).__name__)
            if not self.error_message:
                object.__setattr__(self, "error_message", str(self.fault_error))
        # confidence is always 1.0 for supervisor signals
        object.__setattr__(self, "confidence", 1.0)


@dataclass(slots=True)
class StateUpdateSignal(Signal):
    """
    A Signal that carries a key/value write to ContextNode.state.

    Any node can publish a StateUpdateSignal onto the bus to update the
    shared world model or self-model without holding a direct reference to
    ContextNode.  The runtime processes these during ContextNode.update().

    Set delete=True to remove a key rather than set it.

    Example::

        bus.publish(StateUpdateSignal(
            source=self.node_id, kind="state_update",
            value=None, confidence=1.0, timestamp=time.monotonic(),
            key="world", state_value={"temp": 72},
        ))

    Spec reference: Section 5.3.
    """
    key:         str  = ""
    state_value: Any  = None   # value to write; ignored when delete=True
    delete:      bool = False  # True → remove key from state

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", "state_update")
        object.__setattr__(self, "confidence", 1.0)


@dataclass(slots=True)
class RemoteNodeState:
    """
    State record for a node running on a different AgentNode,
    received via SupervisorSignal over the transport.
    Spec reference: Section 10.6.
    """
    agent_node_id: str
    node_id:       str
    state:         NodeState
    timestamp:     float


# ══════════════════════════════════════════════════════════════════════════════
# Logging models
# ══════════════════════════════════════════════════════════════════════════════

class LogLevel(Enum):
    """Log severity levels, ordered from least to most severe."""
    DEBUG    = 10
    INFO     = 20
    WARNING  = 30
    ERROR    = 40
    CRITICAL = 50

    def __lt__(self, other: LogLevel) -> bool:
        return self.value < other.value

    def __le__(self, other: LogLevel) -> bool:
        return self.value <= other.value


@dataclass(slots=True)
class DecisionEvent:
    """
    Per-tick snapshot of the decision layer's activity.

    Emitted to observers registered on :class:`ArachniteRuntime` via
    ``decision_observers``.  Captures every proposal the decision strategy
    considered, which ones were dispatched, any interrupt requests issued,
    and the name of the active strategy — enough to reconstruct *why* the
    agent took (or did not take) an action on a given tick.
    """
    tick:       int
    timestamp:  float
    strategy:   str                       # type(strategy).__name__
    considered: list[Proposal]
    dispatched: list[Proposal]
    interrupts: list[InterruptRequest]


@dataclass(slots=True)
class LogEvent:
    """
    A structured log record emitted by StructuredLogger.
    Spec reference: Section 13.1.
    """
    level:         LogLevel
    node_id:       str
    agent_node_id: str
    tick:          int
    message:       str
    data:          dict[str, Any]
    timestamp:     float = field(default_factory=time.monotonic)


# ══════════════════════════════════════════════════════════════════════════════
# Shutdown models
# ══════════════════════════════════════════════════════════════════════════════

class ShutdownPhase(Enum):
    """
    Ordered phases of the graceful shutdown sequence.
    Spec reference: Section 15.1.
    """
    NOT_STARTED          = 0
    STOP_SENSING         = 1
    DRAIN_REFLEXES       = 2
    COMPLETE_MANDATORY   = 3
    INTERRUPT_ACTION     = 4
    STOP_SUPERVISORS     = 5
    TEARDOWN_NODES       = 6
    DISCONNECT_TRANSPORT = 7
    COMPLETE             = 8


# ══════════════════════════════════════════════════════════════════════════════
# History config
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(slots=True)
class HistoryConfig:
    """
    Per-signal-kind configuration for ContextNode history retention.
    Spec reference: Section 14.4.
    """
    max_ticks:   int | None   = None   # None = use ContextNode.history_length
    max_bytes:   int | None   = None   # evict oldest entries when total exceeds this
    value_ttl_s: float | None = None   # evict entries older than this
