"""Shared surface-facing status-copy helpers."""

from __future__ import annotations

WORKSPACE_UNAVAILABLE_TEXT = "Workspace root is no longer available."


def surface_control_status_text(control_status_text: str | None, *, empty_flow_message: str = "") -> str:
    """Return the shared control/status line text shown by operator surfaces."""
    if not control_status_text:
        return empty_flow_message
    return control_status_text.replace("Â·", "-")


__all__ = ["WORKSPACE_UNAVAILABLE_TEXT", "surface_control_status_text"]
