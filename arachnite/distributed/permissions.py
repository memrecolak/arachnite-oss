"""
arachnite.distributed.permissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Permission whitelist validator: ensures every node's declared permissions
are within its allowed set. Startup-only, zero runtime cost.
"""

from __future__ import annotations

from arachnite.exceptions import PermissionValidationError
from arachnite.models import Permission
from arachnite.nodes.base import BaseNode


def validate_permissions(
    nodes: list[BaseNode],
    allowed_map: dict[str, set[Permission]] | None = None,
) -> None:
    """
    Validate that every node's declared permissions are within its allowed set.

    Args:
        nodes: Instantiated BaseNode subclass instances.
        allowed_map: Mapping of node_id -> allowed Permission values.
                     If None or empty, validation is skipped (backward compatible).

    Raises:
        PermissionValidationError: if any node declares a permission not allowed.
    """
    if not allowed_map:
        return

    errors: list[str] = []
    for node in nodes:
        nid = node.node_id
        if nid not in allowed_map:
            continue
        allowed = allowed_map[nid]
        declared: frozenset[Permission] | set[Permission] = getattr(
            node, "permissions", frozenset(),
        )
        denied = declared - allowed
        if denied:
            denied_str = ", ".join(sorted(p.value for p in denied))
            allowed_str = ", ".join(sorted(p.value for p in allowed)) or "(none)"
            errors.append(
                f"Node '{nid}' declares permissions [{denied_str}] "
                f"not in its allowed set [{allowed_str}]."
            )

    if errors:
        raise PermissionValidationError(errors)
