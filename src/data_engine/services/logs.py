"""Operator log history services."""

from __future__ import annotations

from datetime import datetime

from data_engine.domain import FlowLogEntry, FlowRunState
from data_engine.domain.logs import parse_runtime_message
from data_engine.services.runtime_ports import RuntimeCacheStore
from data_engine.views.logs import FlowLogStore


class LogService:
    """Own operator log-store construction and log history queries."""

    DEFAULT_VISIBLE_LOG_LIMIT = 20_000

    def create_store(self, runtime_cache_ledger: RuntimeCacheStore | None = None) -> FlowLogStore:
        """Create one log store hydrated from the given runtime cache store."""
        limit = self.DEFAULT_VISIBLE_LOG_LIMIT
        return FlowLogStore(
            self._hydrate_entries(runtime_cache_ledger, limit=limit),
            max_entries=limit,
        )

    def reload(self, store: FlowLogStore, runtime_cache_ledger: RuntimeCacheStore | None) -> None:
        """Reload one log store from an explicit runtime cache store."""
        if runtime_cache_ledger is None:
            store.replace(())
            return
        transient_entries = tuple(entry for entry in store.entries() if entry.persisted_id is None)
        after_id = store.last_persisted_log_id
        if after_id is None:
            persisted_entries = self._hydrate_entries(runtime_cache_ledger, limit=store.max_entries)
            if not persisted_entries and transient_entries:
                store.replace(transient_entries)
                return
            store.replace(persisted_entries)
            if transient_entries:
                store.append_entries(transient_entries)
            return
        store.append_entries(self._hydrate_entries(runtime_cache_ledger, after_id=after_id))

    def append_entry(self, store: FlowLogStore, entry: FlowLogEntry) -> None:
        """Append one log entry to the current store."""
        store.append_entry(entry)

    def clear_flow(self, store: FlowLogStore, flow_name: str | None) -> None:
        """Clear one flow's visible log history from the current store."""
        store.clear_flow(flow_name)

    def all_entries(self, store: FlowLogStore) -> tuple[FlowLogEntry, ...]:
        """Return every entry currently held in the store."""
        return store.entries()

    def transient_entries(self, store: FlowLogStore) -> tuple[FlowLogEntry, ...]:
        """Return only non-persisted live entries still held in the store."""
        return tuple(entry for entry in store.entries() if entry.persisted_id is None)

    def entries_for_flow(self, store: FlowLogStore, flow_name: str | None) -> tuple[FlowLogEntry, ...]:
        """Return flow-scoped entries for one selected flow."""
        return store.entries_for_flow(flow_name)

    def runs_for_flow(self, store: FlowLogStore, flow_name: str | None) -> tuple[FlowRunState, ...]:
        """Return grouped run history for one selected flow."""
        return store.runs_for_flow(flow_name)

    def entries_from_ledger(
        self,
        runtime_cache_ledger: RuntimeCacheStore | None,
        *,
        flow_name: str | None = None,
        run_id: str | None = None,
        after_id: int | None = None,
        limit: int | None = None,
    ) -> tuple[FlowLogEntry, ...]:
        """Return visible log entries loaded directly from one runtime cache ledger."""
        return self._hydrate_entries(
            runtime_cache_ledger,
            flow_name=flow_name,
            run_id=run_id,
            after_id=after_id,
            limit=limit,
        )

    def _hydrate_entries(
        self,
        runtime_cache_ledger: RuntimeCacheStore | None,
        *,
        flow_name: str | None = None,
        run_id: str | None = None,
        after_id: int | None = None,
        limit: int | None = None,
    ) -> tuple[FlowLogEntry, ...]:
        """Build in-memory flow log entries from one runtime cache ledger."""
        if runtime_cache_ledger is None:
            return ()
        try:
            persisted_entries = runtime_cache_ledger.logs.list(
                flow_name=flow_name,
                run_id=run_id,
                after_id=after_id,
                limit=limit,
            )
        except TypeError:
            try:
                persisted_entries = runtime_cache_ledger.logs.list(after_id=after_id, limit=limit)
            except TypeError:
                try:
                    persisted_entries = runtime_cache_ledger.logs.list(after_id=after_id)
                except TypeError:
                    persisted_entries = runtime_cache_ledger.logs.list()
        hydrated_entries = tuple(
            FlowLogEntry(
                line=FlowLogEntry.format_runtime_message(entry.message),
                kind="flow" if entry.flow_name is not None else "system",
                event=parse_runtime_message(entry.message),
                flow_name=entry.flow_name,
                created_at_utc=datetime.fromisoformat(entry.created_at_utc),
                persisted_id=entry.id,
            )
            for entry in persisted_entries
        )
        if flow_name is None and run_id is None:
            return hydrated_entries
        return tuple(
            entry
            for entry in hydrated_entries
            if (flow_name is None or entry.flow_name == flow_name)
            and (run_id is None or (entry.event is not None and entry.event.run_id == run_id))
        )


__all__ = ["LogService"]
