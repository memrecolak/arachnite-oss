"""
arachnite.exceptions
~~~~~~~~~~~~~~~~~~~~
All framework-level exceptions, organised by subsystem.
Spec reference: Section 18.
"""

from __future__ import annotations

# ── Base ──────────────────────────────────────────────────────────────────────

class ArachniteError(Exception):
    """Base class for all Arachnite framework exceptions."""


# ── Signal Bus ────────────────────────────────────────────────────────────────

class SignalBusError(ArachniteError):
    """A subscriber raised during SignalBus.publish()."""

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.cause = cause


# ── Nodes ─────────────────────────────────────────────────────────────────────

class NodeRegistrationError(ArachniteError):
    """Invalid node registration to a master node (duplicate id or rule violation)."""

    def __init__(self, node_id: str, master: str, reason: str | None = None) -> None:
        if reason is None:
            reason = (
                f"Node '{node_id}' is already registered to {master}. "
                "Each node_id must be unique within a master node."
            )
        super().__init__(reason)
        self.node_id = node_id
        self.master  = master


class NodeConfigError(ArachniteError):
    """A required config key is missing or has the wrong type."""

    def __init__(self, node_id: str, key: str, message: str = "") -> None:
        detail = f": {message}" if message else ""
        super().__init__(f"Node '{node_id}' config error for key '{key}'{detail}")
        self.node_id = node_id
        self.key     = key


# ── Actions ───────────────────────────────────────────────────────────────────

class ActionTimeoutError(ArachniteError):
    """ActionNode.execute() or an individual ActionStep exceeded its timeout."""

    def __init__(self, action_id: str, step_name: str | None, timeout_s: float) -> None:
        location = f" at step '{step_name}'" if step_name else ""
        super().__init__(
            f"Action '{action_id}'{location} timed out after {timeout_s:.1f}s."
        )
        self.action_id  = action_id
        self.step_name  = step_name
        self.timeout_s  = timeout_s


class ActionNotFoundError(ArachniteError):
    """Proposal.action_id has no matching ActionNode registered."""

    def __init__(self, action_id: str) -> None:
        super().__init__(
            f"No ActionNode registered with node_id='{action_id}'. "
            "Register it with ActionMasterNode.register() before dispatching proposals."
        )
        self.action_id = action_id


# ── Multi-step / Interruption ─────────────────────────────────────────────────

class InterruptError(ArachniteError):
    """An interrupt request could not be fulfilled."""

    def __init__(self, action_id: str, reason: str) -> None:
        super().__init__(f"Could not interrupt action '{action_id}': {reason}")
        self.action_id = action_id
        self.reason    = reason


class RollbackError(ArachniteError):
    """A rollback callable raised during MultiStepActionNode.on_interrupted()."""

    def __init__(
        self,
        action_id: str,
        step_name: str,
        cause: BaseException,
    ) -> None:
        super().__init__(
            f"Rollback failed for action '{action_id}' at step '{step_name}': {cause}"
        )
        self.action_id = action_id
        self.step_name = step_name
        self.cause     = cause


class MandatoryBlockViolation(ArachniteError):
    """
    Raised if something attempts to force-stop an action inside a mandatory
    completion block outside of an emergency shutdown sequence.
    """

    def __init__(self, action_id: str, step_name: str) -> None:
        super().__init__(
            f"Cannot interrupt action '{action_id}' at step '{step_name}': "
            "step is inside a mandatory completion block. "
            "Use runtime.emergency_stop() to bypass mandatory blocks."
        )
        self.action_id = action_id
        self.step_name = step_name


class StepAbortError(ArachniteError):
    """A step returned abort_sequence=True. Carries the StepResult for diagnosis."""

    def __init__(self, action_id: str, step_name: str) -> None:
        super().__init__(
            f"Action '{action_id}' step '{step_name}' requested sequence abort."
        )
        self.action_id = action_id
        self.step_name = step_name


# ── Context ───────────────────────────────────────────────────────────────────

class ContextError(ArachniteError):
    """ContextNode accessed before first update()."""

    def __init__(self) -> None:
        super().__init__(
            "ContextNode has not been updated yet. "
            "Ensure at least one tick has completed before calling snapshot()."
        )


# ── Supervisor ────────────────────────────────────────────────────────────────

class SupervisorError(ArachniteError):
    """NodeSupervisor encountered an unrecoverable error during a restart."""

    def __init__(self, node_id: str, cause: BaseException) -> None:
        super().__init__(
            f"Supervisor failed to restart node '{node_id}': {cause}"
        )
        self.node_id = node_id
        self.cause   = cause


class ReflexConflictError(ArachniteError):
    """Two reflex nodes with equal priority both fired in the same tick."""

    def __init__(self, priority: int, node_ids: list[str]) -> None:
        super().__init__(
            f"Priority conflict: reflex nodes {node_ids} all fired at priority={priority}. "
            "Assign unique priorities or set InstinctMasterNode.reflex_conflict='dispatch_all'."
        )
        self.priority = priority
        self.node_ids = node_ids


# ── Codec ────────────────────────────────────────────────────────────────────

class UnsafeCodecError(ArachniteError):
    """A codec that is unsafe for network transport was registered with a network transport."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


# ── Transport ─────────────────────────────────────────────────────────────────

class TransportError(ArachniteError):
    """Base class for transport-layer failures."""


class TransportConnectionError(TransportError):
    """Transport failed to connect or lost connection after exhausting retries."""

    def __init__(self, transport_name: str, reason: str) -> None:
        super().__init__(f"Transport '{transport_name}' connection failed: {reason}")
        self.transport_name = transport_name
        self.reason         = reason


# ── Distributed / Manifest ────────────────────────────────────────────────────

class CoLocationError(ArachniteError):
    """
    A ReflexInstinctNode's target action_id is assigned to a different
    AgentNode than the reflex itself.
    """

    def __init__(
        self,
        reflex_id: str,
        action_id: str,
        reflex_agent: str,
        action_agent: str,
    ) -> None:
        super().__init__(
            f"Co-location violation: ReflexInstinctNode '{reflex_id}' is on "
            f"AgentNode '{reflex_agent}' but its target ActionNode '{action_id}' "
            f"is on AgentNode '{action_agent}'. Reflex nodes and their target "
            "actions must be on the same AgentNode."
        )
        self.reflex_id    = reflex_id
        self.action_id    = action_id
        self.reflex_agent = reflex_agent
        self.action_agent = action_agent


class ManifestValidationError(ArachniteError):
    """DeploymentManifest.validate() found one or more inconsistencies."""

    def __init__(self, errors: list[str]) -> None:
        bullet_list = "\n  - ".join(errors)
        super().__init__(
            f"Manifest validation failed with {len(errors)} error(s):\n  - {bullet_list}"
        )
        self.errors = errors


class PermissionValidationError(ArachniteError):
    """A node declared permissions not in its allowed set."""

    def __init__(self, errors: list[str]) -> None:
        bullet_list = "\n  - ".join(errors)
        super().__init__(
            f"Permission validation failed with {len(errors)} error(s):\n  - {bullet_list}"
        )
        self.errors = errors


class DependencyValidationError(ArachniteError):
    """A node's required dependencies are not registered."""

    def __init__(self, errors: list[str]) -> None:
        bullet_list = "\n  - ".join(errors)
        super().__init__(
            f"Dependency validation failed with {len(errors)} error(s):\n  - {bullet_list}"
        )
        self.errors = errors


# ── Media ────────────────────────────────────────────────────────────────────

class PathTraversalError(ArachniteError):
    """A media store path component would escape the base directory."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
