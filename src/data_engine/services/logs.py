"""Operator log history services."""

from __future__ import annotations

from datetime import datetime

from data_engine.domain import FlowLogEntry, FlowRunState
from data_engine.domain.logs import parse_runtime_message
from data_engine.runtime.runtime_db import RuntimeCacheLedger
from data_engine.views.logs import FlowLogStore


class LogService:
    """Own operator log-store construction and log history queries."""

    def create_store(self, runtime_cache_ledger: RuntimeCacheLedger | None = None) -> FlowLogStore:
        """Create one log store backed by the given runtime cache ledger."""
        store = FlowLogStore(self._hydrate_entries(runtime_cache_ledger))
        store._runtime_cache_ledger = runtime_cache_ledger
        return store

    def reload(self, store: FlowLogStore) -> None:
        """Reload one log store from its attached runtime ledger."""
        entries = self._hydrate_entries(getattr(store, "_runtime_cache_ledger", None))
        store.clear()
        for entry in entries:
            store.append_entry(entry)

    def append_entry(self, store: FlowLogStore, entry: FlowLogEntry) -> None:
        """Append one log entry to the current store."""
        store.append_entry(entry)

    def clear_flow(self, store: FlowLogStore, flow_name: str | None) -> None:
        """Clear one flow's visible log history from the current store."""
        store.clear_flow(flow_name)

    def all_entries(self, store: FlowLogStore) -> tuple[FlowLogEntry, ...]:
        """Return every entry currently held in the store."""
        return tuple(store._entries)

    def entries_for_flow(self, store: FlowLogStore, flow_name: str | None) -> tuple[FlowLogEntry, ...]:
        """Return flow-scoped entries for one selected flow."""
        return store.entries_for_flow(flow_name)

    def runs_for_flow(self, store: FlowLogStore, flow_name: str | None) -> tuple[FlowRunState, ...]:
        """Return grouped run history for one selected flow."""
        return store.runs_for_flow(flow_name)

    def _hydrate_entries(self, runtime_cache_ledger: RuntimeCacheLedger | None) -> tuple[FlowLogEntry, ...]:
        """Build in-memory flow log entries from one runtime cache ledger."""
        if runtime_cache_ledger is None:
            return ()
        return tuple(
            FlowLogEntry(
                line=FlowLogEntry.format_runtime_message(entry.message),
                kind="flow" if entry.flow_name is not None else "system",
                event=parse_runtime_message(entry.message),
                flow_name=entry.flow_name,
                created_at_utc=datetime.fromisoformat(entry.created_at_utc),
            )
            for entry in runtime_cache_ledger.logs.list()
        )


__all__ = ["LogService"]
