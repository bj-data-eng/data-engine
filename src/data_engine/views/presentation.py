"""Shared presentation helpers across GUI and TUI surfaces."""

from __future__ import annotations

from dataclasses import dataclass
import math

from data_engine.domain.catalog import FlowCatalogLike


@dataclass(frozen=True)
class FlowGroupBucket:
    """One grouped bucket of flow cards in shared surface order."""

    group_name: str
    entries: tuple[FlowCatalogLike, ...]

    @property
    def title(self) -> str:
        """Return the user-facing label for this grouped flow section."""
        return group_label(self.group_name)


def flow_group_name(card: FlowCatalogLike) -> str:
    """Return the display/runtime group bucket for one flow card."""
    return card.group or card.mode


def group_label(group_name: str) -> str:
    """Return the user-facing label for one grouped flow section."""
    if group_name in {"poll", "schedule", "manual"}:
        return group_name.title()
    return group_name


def group_cards(cards: tuple[FlowCatalogLike, ...] | list[FlowCatalogLike]) -> tuple[FlowGroupBucket, ...]:
    """Group cards by display bucket in the shared surface order."""
    grouped: dict[str, list[FlowCatalogLike]] = {}
    for card in cards:
        grouped.setdefault(flow_group_name(card), []).append(card)
    priority = {"manual": 0, "poll": 1, "schedule": 2}
    return tuple(
        FlowGroupBucket(group_name=group_name, entries=tuple(entries))
        for group_name, entries in sorted(grouped.items(), key=lambda item: (priority.get(item[0], 10), item[0].lower()))
    )


def flow_secondary_text(mode: str, state: str) -> str:
    """Return the secondary status line for one flow card."""
    if mode == "poll":
        return "Polling" if state in {"poll ready", "polling"} else f"Polling  {state}"
    if mode == "schedule":
        return "Scheduled" if state in {"schedule ready", "scheduled"} else f"Scheduled  {state}"
    return "Manual" if state == "manual" else f"Manual  {state}"


def group_secondary_text(entries: list[FlowCatalogLike], flow_states: dict[str, str]) -> str:
    """Return one compact group summary line for sidebar/list displays."""
    total = len(entries)
    active = sum(1 for card in entries if flow_states.get(card.name, card.state) in {"running", "polling", "scheduled"})
    failed = sum(1 for card in entries if flow_states.get(card.name, card.state) == "failed")
    if failed:
        return f"{total} flow(s)  Error: {failed}"
    if active:
        return f"{total} flow(s)  Running: {active}"
    return f"{total} flow(s)"


def status_color_name(state: str) -> str:
    """Return the named status color token for one flow state."""
    if state == "failed":
        return "error"
    if state == "started":
        return "started"
    if state in {"running", "polling", "scheduled", "success", "finished"}:
        return "success"
    if state in {"stopping flow", "stopping runtime"}:
        return "warning"
    return "idle"


def state_dot(state: str) -> str:
    """Return one small textual state marker for compact terminal displays."""
    if state == "failed":
        return "!"
    if state in {"running", "polling", "scheduled", "success", "finished"}:
        return "*"
    if state in {"stopping flow", "stopping runtime"}:
        return "~"
    return "·"


def operation_marker(status: str) -> str:
    """Return one small textual marker for operation-level progress."""
    if status == "running":
        return ">"
    if status == "success":
        return "+"
    if status == "failed":
        return "!"
    return "·"


def format_seconds(seconds: float) -> str:
    """Render elapsed seconds into the compact duration text used across surfaces."""

    def truncate(value: float, decimals: int = 1) -> float:
        factor = 10**decimals
        return math.trunc(value * factor) / factor

    if seconds < 0.001:
        return "<1ms"
    if seconds < 1:
        return f"{math.trunc(seconds * 1000)}ms"
    if seconds < 60:
        return f"{truncate(seconds):.1f}s"
    if seconds < 3600:
        return f"{truncate(seconds / 60):.1f}m"
    return f"{truncate(seconds / 3600):.1f}h"


__all__ = [
    "FlowGroupBucket",
    "flow_secondary_text",
    "flow_group_name",
    "format_seconds",
    "group_cards",
    "group_label",
    "group_secondary_text",
    "operation_marker",
    "state_dot",
    "status_color_name",
]
