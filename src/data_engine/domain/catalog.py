"""Domain models for discovered flow catalog state."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Protocol


@dataclass(frozen=True)
class FlowCatalogEntry:
    """Service/domain representation of one discovered flow."""

    name: str
    group: str | None
    title: str
    description: str
    source_root: str
    target_root: str
    mode: str
    interval: str
    operations: str
    operation_items: tuple[str, ...]
    state: str
    valid: bool
    category: str
    error: str = ""
    parallelism: str = "1"


class FlowCatalogLike(Protocol):
    """Structural flow metadata contract shared by domain and presentation layers."""

    name: str
    group: str | None
    title: str
    description: str
    source_root: str
    target_root: str
    mode: str
    interval: str
    operations: str
    operation_items: tuple[str, ...]
    state: str
    valid: bool
    category: str
    error: str
    parallelism: str


def flow_category(mode: str) -> str:
    """Return the top-level category for one flow mode."""
    return "automated" if mode in {"poll", "schedule"} else "manual"


def default_flow_state(mode: str | None) -> str:
    """Return the default idle state label for one flow mode."""
    if mode == "poll":
        return "poll ready"
    if mode == "schedule":
        return "schedule ready"
    return "manual"


@dataclass(frozen=True)
class FlowCatalogState:
    """Surface-agnostic state for discovered flows and current selection."""

    entries: tuple[FlowCatalogEntry, ...] = ()
    flow_states: dict[str, str] | None = None
    selected_flow_name: str | None = None
    empty_message: str = ""

    @classmethod
    def empty(cls, *, empty_message: str = "") -> "FlowCatalogState":
        """Return the empty flow-catalog state."""
        return cls(entries=(), flow_states={}, selected_flow_name=None, empty_message=empty_message)

    @property
    def entries_by_name(self) -> dict[str, FlowCatalogEntry]:
        """Return discovered entries keyed by internal flow name."""
        return {entry.name: entry for entry in self.entries}

    @property
    def valid_entries(self) -> tuple[FlowCatalogEntry, ...]:
        """Return only valid discovered flow entries."""
        return tuple(entry for entry in self.entries if entry.valid)

    @property
    def has_automated_flows(self) -> bool:
        """Return whether the catalog contains any valid automated flows."""
        return any(entry.valid and entry.mode in {"poll", "schedule"} for entry in self.entries)

    @property
    def selected_entry(self) -> FlowCatalogEntry | None:
        """Return the currently selected entry, if it still exists."""
        if self.selected_flow_name is None:
            return None
        return self.entries_by_name.get(self.selected_flow_name)

    def with_entries(self, entries: Iterable[FlowCatalogEntry]) -> "FlowCatalogState":
        """Return a copy with entries replaced and selection normalized."""
        entry_tuple = tuple(entries)
        entry_names = {entry.name for entry in entry_tuple}
        selected = self.selected_flow_name if self.selected_flow_name in entry_names else (entry_tuple[0].name if entry_tuple else None)
        flow_states = {
            entry.name: (self.flow_states or {}).get(entry.name, entry.state if entry.valid else "invalid")
            for entry in entry_tuple
        }
        return replace(self, entries=entry_tuple, flow_states=flow_states, selected_flow_name=selected)

    def with_selected_flow_name(self, flow_name: str | None) -> "FlowCatalogState":
        """Return a copy with the selected flow name replaced."""
        return replace(self, selected_flow_name=flow_name)

    def with_flow_states(self, flow_states: dict[str, str]) -> "FlowCatalogState":
        """Return a copy with flow states replaced."""
        return replace(self, flow_states=dict(flow_states))

    def with_empty_message(self, message: str) -> "FlowCatalogState":
        """Return a copy with the empty/error message replaced."""
        return replace(self, empty_message=message)


__all__ = [
    "FlowCatalogEntry",
    "FlowCatalogLike",
    "FlowCatalogState",
    "default_flow_state",
    "flow_category",
]
