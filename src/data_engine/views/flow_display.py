"""Shared flow/group row display models across GUI and TUI."""

from __future__ import annotations

from dataclasses import dataclass

from data_engine.views.models import QtFlowCard
from data_engine.views.presentation import FlowGroupBucket, flow_secondary_text, group_label, group_secondary_text, state_dot, status_color_name


@dataclass(frozen=True)
class FlowRowDisplay:
    """Display metadata for one flow row in a list/tree surface."""

    primary: str
    secondary: str
    state_color: str
    dot: str
    tooltip: str

    @classmethod
    def from_card(cls, card: QtFlowCard, state: str, *, primary: str = "title") -> "FlowRowDisplay":
        """Return display metadata for one flow row."""
        primary_text = card.title if primary == "title" else card.name
        resolved_state = state if card.valid else "failed"
        tooltip = f"{card.name} | {card.title} | {state}"
        if card.group:
            tooltip = f"{tooltip} | group={card.group}"
        return cls(
            primary=primary_text,
            secondary=flow_secondary_text(card.mode, state),
            state_color=status_color_name(state),
            dot=state_dot(resolved_state),
            tooltip=tooltip,
        )


@dataclass(frozen=True)
class GroupRowDisplay:
    """Display metadata for one grouped flow header."""

    title: str
    secondary: str
    uppercase_title: str
    @classmethod
    def from_group(
        cls,
        group_name: str,
        entries: list[QtFlowCard] | tuple[QtFlowCard, ...],
        flow_states: dict[str, str],
    ) -> "GroupRowDisplay":
        """Return display metadata for one flow group header."""
        title = group_label(group_name)
        return cls(
            title=title,
            secondary=group_secondary_text(list(entries), flow_states),
            uppercase_title=title.upper(),
        )

    @classmethod
    def from_bucket(cls, bucket: FlowGroupBucket, flow_states: dict[str, str]) -> "GroupRowDisplay":
        """Return display metadata for one grouped flow bucket."""
        return cls.from_group(bucket.group_name, bucket.entries, flow_states)


__all__ = [
    "FlowRowDisplay",
    "GroupRowDisplay",
]
