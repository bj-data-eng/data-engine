"""Shared log storage helpers for Data Engine operator surfaces."""

from __future__ import annotations

from data_engine.domain import FlowLogEntry, FlowRunState, LogKind, RuntimeStepEvent

CollapsedLogKey = tuple[str, str, str]


class FlowLogStore:
    """Keep operator log history and expose per-flow filtered views."""

    def __init__(self, entries: tuple[FlowLogEntry, ...] = ()) -> None:
        self._entries: list[FlowLogEntry] = list(entries)
        self._runs_cache: dict[str, tuple[FlowRunState, ...]] = {}
        self._entry_fingerprints: set[tuple[object, ...]] = {entry.fingerprint() for entry in self._entries}
        persisted_ids = [entry.persisted_id for entry in self._entries if entry.persisted_id is not None]
        self._last_persisted_log_id = max(persisted_ids, default=None)

    def append_entry(self, entry: FlowLogEntry) -> None:
        fingerprint = entry.fingerprint()
        if fingerprint in self._entry_fingerprints:
            if entry.persisted_id is not None:
                current = self._last_persisted_log_id
                self._last_persisted_log_id = entry.persisted_id if current is None else max(current, entry.persisted_id)
            return
        self._entries.append(entry)
        self._entry_fingerprints.add(fingerprint)
        if entry.kind == "flow" and entry.flow_name is not None:
            self._runs_cache.pop(entry.flow_name, None)
        if entry.persisted_id is not None:
            current = self._last_persisted_log_id
            self._last_persisted_log_id = entry.persisted_id if current is None else max(current, entry.persisted_id)

    def append_line(self, line: str, *, kind: LogKind, flow_name: str | None = None) -> FlowLogEntry:
        entry = FlowLogEntry(line=line, kind=kind, flow_name=flow_name)
        self.append_entry(entry)
        return entry

    def clear(self) -> None:
        self._entries.clear()
        self._runs_cache.clear()
        self._entry_fingerprints.clear()
        self._last_persisted_log_id = None

    def replace(self, entries: tuple[FlowLogEntry, ...]) -> None:
        """Replace the full visible log history."""
        self._entries = list(entries)
        self._runs_cache.clear()
        self._entry_fingerprints = {entry.fingerprint() for entry in self._entries}
        persisted_ids = [entry.persisted_id for entry in self._entries if entry.persisted_id is not None]
        self._last_persisted_log_id = max(persisted_ids, default=None)

    def append_entries(self, entries: tuple[FlowLogEntry, ...]) -> None:
        """Append multiple visible log entries while preserving per-flow caches."""
        for entry in entries:
            self.append_entry(entry)

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
        self._entry_fingerprints = {entry.fingerprint() for entry in self._entries}
        persisted_ids = [entry.persisted_id for entry in self._entries if entry.persisted_id is not None]
        self._last_persisted_log_id = max(persisted_ids, default=None)
        self._runs_cache.pop(flow_name, None)

    def entries_for_flow(self, flow_name: str | None) -> tuple[FlowLogEntry, ...]:
        if flow_name is None:
            return ()
        return tuple(entry for entry in self._entries if entry.kind == "flow" and entry.flow_name == flow_name)

    def runs_for_flow(self, flow_name: str | None) -> tuple[FlowRunState, ...]:
        entries = self.entries_for_flow(flow_name)
        if not entries:
            return ()
        assert flow_name is not None
        cached = self._runs_cache.get(flow_name)
        if cached is not None:
            return cached
        grouped = FlowRunState.group_entries(entries)
        self._runs_cache[flow_name] = grouped
        return grouped

    @property
    def last_persisted_log_id(self) -> int | None:
        """Return the newest persisted log row already merged into the store."""
        return self._last_persisted_log_id

__all__ = [
    "CollapsedLogKey",
    "FlowLogStore",
    "FlowLogEntry",
    "FlowRunState",
    "LogKind",
    "RuntimeStepEvent",
]
