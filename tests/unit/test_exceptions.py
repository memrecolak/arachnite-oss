"""Unit tests for the exceptions module."""

from __future__ import annotations

from arachnite.exceptions import (
    ActionNotFoundError,
    ActionTimeoutError,
    ArachniteError,
    CoLocationError,
    ContextError,
    InterruptError,
    MandatoryBlockViolation,
    ManifestValidationError,
    NodeConfigError,
    NodeRegistrationError,
    ReflexConflictError,
    RollbackError,
    SignalBusError,
    StepAbortError,
    SupervisorError,
    TransportConnectionError,
    TransportError,
)


class TestExceptionHierarchy:
    def test_all_inherit_from_arachnite_error(self) -> None:
        for cls in [
            SignalBusError, NodeRegistrationError, NodeConfigError,
            ActionTimeoutError, ActionNotFoundError, InterruptError,
            RollbackError, MandatoryBlockViolation, StepAbortError,
            ContextError, SupervisorError, ReflexConflictError,
            TransportError, TransportConnectionError,
            CoLocationError, ManifestValidationError,
        ]:
            assert issubclass(cls, ArachniteError)

    def test_transport_connection_inherits_transport_error(self) -> None:
        assert issubclass(TransportConnectionError, TransportError)


class TestSignalBusError:
    def test_message_and_cause(self) -> None:
        cause = ValueError("boom")
        err = SignalBusError("bus exploded", cause=cause)
        assert "bus exploded" in str(err)
        assert err.cause is cause

    def test_cause_defaults_to_none(self) -> None:
        err = SignalBusError("oops")
        assert err.cause is None


class TestNodeRegistrationError:
    def test_message_contains_node_id(self) -> None:
        err = NodeRegistrationError("MyNode", "SenseMasterNode")
        assert "MyNode" in str(err)
        assert err.node_id == "MyNode"
        assert err.master == "SenseMasterNode"


class TestNodeConfigError:
    def test_message_with_detail(self) -> None:
        err = NodeConfigError("TempSensor", "threshold", "must be float")
        assert "threshold" in str(err)
        assert "must be float" in str(err)
        assert err.key == "threshold"

    def test_message_without_detail(self) -> None:
        err = NodeConfigError("N", "key")
        assert "key" in str(err)


class TestActionTimeoutError:
    def test_with_step_name(self) -> None:
        err = ActionTimeoutError("CoolDown", "ramp_up", 5.0)
        assert "CoolDown" in str(err)
        assert "ramp_up" in str(err)
        assert err.step_name == "ramp_up"
        assert err.timeout_s == 5.0

    def test_without_step_name(self) -> None:
        err = ActionTimeoutError("CoolDown", None, 2.5)
        assert "CoolDown" in str(err)
        assert err.step_name is None


class TestActionNotFoundError:
    def test_message_contains_action_id(self) -> None:
        err = ActionNotFoundError("EmergencyStop")
        assert "EmergencyStop" in str(err)
        assert err.action_id == "EmergencyStop"


class TestInterruptError:
    def test_fields(self) -> None:
        err = InterruptError("CoolDown", "mandatory block active")
        assert "CoolDown" in str(err)
        assert err.action_id == "CoolDown"
        assert err.reason == "mandatory block active"


class TestRollbackError:
    def test_fields(self) -> None:
        cause = RuntimeError("hw failure")
        err = RollbackError("CoolSeq", "sustain", cause)
        assert "CoolSeq" in str(err)
        assert err.step_name == "sustain"
        assert err.cause is cause


class TestMandatoryBlockViolation:
    def test_message_mentions_emergency_stop(self) -> None:
        err = MandatoryBlockViolation("CoolSeq", "sustain")
        assert "emergency_stop" in str(err)
        assert err.action_id == "CoolSeq"
        assert err.step_name == "sustain"


class TestStepAbortError:
    def test_fields(self) -> None:
        err = StepAbortError("Seq", "step2")
        assert "step2" in str(err)
        assert err.action_id == "Seq"
        assert err.step_name == "step2"


class TestContextError:
    def test_message_mentions_snapshot(self) -> None:
        err = ContextError()
        assert "snapshot" in str(err).lower() or "updated" in str(err).lower()


class TestSupervisorError:
    def test_fields(self) -> None:
        cause = RuntimeError("crash")
        err = SupervisorError("TempSense", cause)
        assert "TempSense" in str(err)
        assert err.cause is cause


class TestReflexConflictError:
    def test_fields(self) -> None:
        err = ReflexConflictError(250, ["ReflexA", "ReflexB"])
        assert "250" in str(err)
        assert err.priority == 250
        assert "ReflexA" in err.node_ids


class TestTransportConnectionError:
    def test_fields(self) -> None:
        err = TransportConnectionError("MQTTTransport", "broker unreachable")
        assert "MQTTTransport" in str(err)
        assert err.transport_name == "MQTTTransport"
        assert err.reason == "broker unreachable"


class TestCoLocationError:
    def test_fields(self) -> None:
        err = CoLocationError("ReflexA", "StopAction", "agent-1", "agent-2")
        assert "ReflexA" in str(err)
        assert "agent-1" in str(err)
        assert err.reflex_id == "ReflexA"
        assert err.action_id == "StopAction"
        assert err.reflex_agent == "agent-1"
        assert err.action_agent == "agent-2"


class TestManifestValidationError:
    def test_single_error(self) -> None:
        err = ManifestValidationError(["missing field X"])
        assert "missing field X" in str(err)
        assert err.errors == ["missing field X"]

    def test_multiple_errors(self) -> None:
        errs = ["error A", "error B", "error C"]
        err = ManifestValidationError(errs)
        assert len(err.errors) == 3
        assert all(e in str(err) for e in errs)
