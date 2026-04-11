"""Shared log storage helpers for Data Engine operator surfaces."""

from __future__ import annotations

from data_engine.domain import FlowLogEntry, FlowRunState, LogKind, RuntimeStepEvent

CollapsedLogKey = tuple[str, str, str]


class FlowLogStore:
    """Keep operator log history and expose per-flow filtered views."""

    def __init__(self, entries: tuple[FlowLogEntry, ...] = ()) -> None:
        self._entries: list[FlowLogEntry] = list(entries)

    def append_entry(self, entry: FlowLogEntry) -> None:
        self._entries.append(entry)

    def append_line(self, line: str, *, kind: LogKind, flow_name: str | None = None) -> FlowLogEntry:
        entry = FlowLogEntry(line=line, kind=kind, flow_name=flow_name)
        self.append_entry(entry)
        return entry

    def clear(self) -> None:
        self._entries.clear()

    def replace(self, entries: tuple[FlowLogEntry, ...]) -> None:
        """Replace the full visible log history."""
        self._entries = list(entries)

    def entries(self) -> tuple[FlowLogEntry, ...]:
        """Return every visible log entry."""
        return tuple(self._entries)

    def clear_flow(self, flow_name: str | None) -> None:
        if flow_name is None:
            return
        self._entries = [
            entry
            for entry in self._entries
            if not (entry.kind == "flow" and entry.flow_name == flow_name)
        ]

    def entries_for_flow(self, flow_name: str | None) -> tuple[FlowLogEntry, ...]:
        if flow_name is None:
            return ()
        return tuple(entry for entry in self._entries if entry.kind == "flow" and entry.flow_name == flow_name)

    def runs_for_flow(self, flow_name: str | None) -> tuple[FlowRunState, ...]:
        entries = self.entries_for_flow(flow_name)
        if not entries:
            return ()
        return FlowRunState.group_entries(entries)

__all__ = [
    "CollapsedLogKey",
    "FlowLogStore",
    "FlowLogEntry",
    "FlowRunState",
    "LogKind",
    "RuntimeStepEvent",
]
