"""
arachnite.distributed.colocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Co-location validator: ensures every ReflexInstinctNode and its target
ActionNode are assigned to the same AgentNode.
Spec reference: Section 10.4, 11.2.
"""

from __future__ import annotations

from arachnite.exceptions import CoLocationError, ManifestValidationError


def validate_colocation(
    assignments: list[dict[str, object]],
) -> None:
    """
    Validate co-location constraints across all NodeAssignments.

    For every ReflexInstinctNode R on AgentNode A:
        The ActionNode whose node_id == R.action_id
        MUST also be assigned to AgentNode A.

    Args:
        assignments: list of dicts with keys:
            - node_class_name: str
            - agent_node_id:   str
            - is_reflex:       bool
            - reflex_action_id: str | None  (action_id for reflex nodes)
            - node_id:         str

    Raises:
        ManifestValidationError: if any co-location constraints are violated.
    """
    # Build a map: node_id -> set of AgentNode ids it's assigned to
    action_agents: dict[str, set[str]] = {}
    for a in assignments:
        node_id      = str(a["node_id"])
        agent_id     = str(a["agent_node_id"])
        node_section = str(a.get("node_section", ""))
        if node_section == "action":
            if node_id not in action_agents:
                action_agents[node_id] = set()
            action_agents[node_id].add(agent_id)

    errors: list[str] = []
    for a in assignments:
        if not a.get("is_reflex"):
            continue
        reflex_id   = str(a["node_id"])
        action_id   = str(a.get("reflex_action_id", ""))
        reflex_agent = str(a["agent_node_id"])

        if not action_id:
            errors.append(
                f"ReflexInstinctNode '{reflex_id}' on AgentNode '{reflex_agent}' "
                "has no reflex_action_id set. Reflex nodes must declare their "
                "target action_id."
            )
            continue

        target_agents = action_agents.get(action_id, set())
        if not target_agents:
            errors.append(
                f"ReflexInstinctNode '{reflex_id}' targets ActionNode '{action_id}' "
                f"but no ActionNode with that id is registered in any AgentNode."
            )
            continue

        if reflex_agent not in target_agents:
            action_agent = next(iter(target_agents))
            errors.append(
                CoLocationError(
                    reflex_id    = reflex_id,
                    action_id    = action_id,
                    reflex_agent = reflex_agent,
                    action_agent = action_agent,
                ).args[0]
            )

    if errors:
        raise ManifestValidationError(errors)
