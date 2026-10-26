"""
Arachnite
~~~~~~~~~
A biologically-inspired reactive agent framework for Python.

Quick start::

    from arachnite import ArachniteRuntime, SignalBus, ContextNode
    from arachnite.nodes import (
        SenseMasterNode, BaseSenseNode,
        InstinctMasterNode, BaseInstinctNode,
        DecisionMasterNode, GreedyDecisionNode,
        ActionMasterNode, BaseActionNode,
    )
    from arachnite.models import Signal, Proposal, Result, Context

Spec reference: https://github.com/memrecolak/arachnite-oss
"""

from arachnite.builder import RuntimeBuilder
from arachnite.bus import SignalBus
from arachnite.codec import CodecRegistry, SignalCodec, default_registry
from arachnite.config import NodeConfig
from arachnite.context import ContextNode
from arachnite.exceptions import (
    ActionVetoed,
    ArachniteError,
    DependencyValidationError,
    PathTraversalError,
    PermissionValidationError,
    ResourceConflictError,
    UnsafeCodecError,
)
from arachnite.framework_config import (
    ContextSettings,
    FrameworkConfig,
    LoggingSettings,
    MQTTSettings,
    NATSSettings,
    RedisSettings,
    RuntimeSettings,
    SupervisorSettings,
    TransportSettings,
)
from arachnite.health import HealthMonitor
from arachnite.llm_provider import (
    AnthropicProvider,
    LLMProvider,
    LocalProvider,
    OllamaProvider,
    SharedModelRegistry,
    ThreadSafeProvider,
)
from arachnite.logging import BaseLogSink, JSONLogSink, StdoutLogSink, StructuredLogger
from arachnite.media import MediaStore
from arachnite.models import (
    ActionExecutionState,
    ActionStep,
    Context,
    DecisionEvent,
    HistoryConfig,
    InterruptPolicy,
    InterruptRequest,
    LogEvent,
    LogLevel,
    MergePolicy,
    NodeFaultSignal,
    NodeState,
    Permission,
    Proposal,
    ReflexPreemption,
    RemoteNodeState,
    RestartPolicy,
    Result,
    ShutdownPhase,
    Signal,
    StateUpdateSignal,
    StepResult,
    SupervisorSignal,
)
from arachnite.nodes.action import (
    ActionGuard,
    ActionMasterNode,
    BaseActionNode,
    GuardVerdict,
    LaunchOutcome,
    LaunchStatus,
    MultiStepActionNode,
)
from arachnite.nodes.active_inference import ActiveInferenceDecisionNode
from arachnite.nodes.base import BaseNode
from arachnite.nodes.decision import (
    BaseDecisionNode,
    DecisionMasterNode,
    GreedyDecisionNode,
    RandomDecisionNode,
    WeightedDecisionNode,
)
from arachnite.nodes.instinct import (
    BaseInstinctNode,
    BaseReflexInstinctNode,
    InstinctMasterNode,
)
from arachnite.nodes.llm import LLMInstinctNode
from arachnite.nodes.sense import BaseSenseNode, SenseMasterNode
from arachnite.runtime import TICK_STAGE_NAMES, ArachniteRuntime, TickInstrumenter
from arachnite.safety_monitor import (
    ActuatorExclusivityMonitor,
    BaseSafetyMonitor,
    MandatoryBlockMonitor,
    MonitorState,
    ReflexAvailabilityMonitor,
    ReflexBypassMonitor,
    ReflexDispatchMonitor,
    SafetyMonitorRegistry,
    SafetySeverity,
    SafetyViolationSignal,
    TickBudgetMonitor,
)
from arachnite.shutdown import ShutdownCoordinator
from arachnite.supervisor import NodeSupervisor
from arachnite.testing import MockBus, make_context, make_proposal, make_result, make_signal
from arachnite.toolcodec import ParsedToolCall, parse_tool_calls, render_tool_schemas
from arachnite.web import FileLogSink, SignalDashboard

__version__ = "0.12.3"
__all__ = [
    # Core
    "ArachniteRuntime",
    "TickInstrumenter",
    "TICK_STAGE_NAMES",
    "RuntimeBuilder",
    "ShutdownCoordinator",
    "SignalBus",
    "ContextNode",
    "NodeSupervisor",
    "HealthMonitor",
    # Models
    "Signal",
    "StateUpdateSignal",
    "Context",
    "DecisionEvent",
    "Proposal",
    "Result",
    "ActionStep",
    "StepResult",
    "InterruptPolicy",
    "InterruptRequest",
    "ReflexPreemption",
    "ActionExecutionState",
    "MergePolicy",
    "NodeFaultSignal",
    "NodeState",
    "Permission",
    "RestartPolicy",
    "SupervisorSignal",
    "RemoteNodeState",
    "LogEvent",
    "LogLevel",
    "ShutdownPhase",
    "HistoryConfig",
    # Media
    "MediaStore",
    # Config
    "NodeConfig",
    # Logging
    "StructuredLogger",
    "BaseLogSink",
    "StdoutLogSink",
    "JSONLogSink",
    # Codec
    "SignalCodec",
    "CodecRegistry",
    "default_registry",
    # Errors
    "ArachniteError",
    "ActionVetoed",
    "DependencyValidationError",
    "PathTraversalError",
    "PermissionValidationError",
    "ResourceConflictError",
    "UnsafeCodecError",
    # Web dashboard
    "SignalDashboard",
    "FileLogSink",
    # Node base classes — extend these to build your agent
    "BaseNode",
    "BaseSenseNode",
    "SenseMasterNode",
    "BaseInstinctNode",
    "BaseReflexInstinctNode",
    "InstinctMasterNode",
    "LLMInstinctNode",
    # LLM providers
    "LLMProvider",
    "AnthropicProvider",
    "OllamaProvider",
    "LocalProvider",
    "ThreadSafeProvider",
    "SharedModelRegistry",
    # Framework config
    "FrameworkConfig",
    "RuntimeSettings",
    "TransportSettings",
    "MQTTSettings",
    "NATSSettings",
    "RedisSettings",
    "LoggingSettings",
    "SupervisorSettings",
    "ContextSettings",
    "BaseDecisionNode",
    "DecisionMasterNode",
    "GreedyDecisionNode",
    "WeightedDecisionNode",
    "RandomDecisionNode",
    "ActiveInferenceDecisionNode",
    # Safety monitors
    "BaseSafetyMonitor",
    "SafetyMonitorRegistry",
    "SafetyViolationSignal",
    "SafetySeverity",
    "MonitorState",
    "ReflexBypassMonitor",
    "MandatoryBlockMonitor",
    "ReflexDispatchMonitor",
    "ReflexAvailabilityMonitor",
    "TickBudgetMonitor",
    "ActuatorExclusivityMonitor",
    "BaseActionNode",
    "MultiStepActionNode",
    "ActionMasterNode",
    "ActionGuard",
    "GuardVerdict",
    "LaunchOutcome",
    "LaunchStatus",
    # Testing helpers
    "make_signal",
    "make_proposal",
    "make_result",
    "make_context",
    "MockBus",
    # Tool codec
    "ParsedToolCall",
    "parse_tool_calls",
    "render_tool_schemas",
]
