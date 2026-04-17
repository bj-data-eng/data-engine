"""Catalog and history query ports for operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from data_engine.core.model import FlowValidationError
from data_engine.domain import FlowCatalogLike, FlowCatalogState, FlowRunState, FlowSummaryRow
from data_engine.services.flow_catalog import FlowCatalogService
from data_engine.services.logs import LogService
from data_engine.services.runtime_ports import RuntimeCacheStore
from data_engine.views.logs import FlowLogStore
from data_engine.views.presentation import group_cards


@dataclass(frozen=True)
class FlowCatalogItem:
    """Lightweight catalog row for one discovered flow."""

    flow_name: str
    group_name: str
    title: str
    runtime_kind: Literal["manual", "poll", "schedule"]
    settle: int | None
    max_parallel: int


@dataclass(frozen=True)
class FlowConfigPreview:
    """Config-preview rows for one selected flow."""

    flow_name: str
    rows: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class WorkspaceCatalogLoadResult:
    """Normalized workspace-catalog load result for operator surfaces."""

    catalog_state: FlowCatalogState
    loaded: bool
    error_text: str | None = None


@dataclass(frozen=True)
class WorkspaceCatalogPresentation:
    """Grouped workspace-catalog presentation shared by operator surfaces."""

    entries: tuple[FlowCatalogLike, ...]
    grouped_entries: tuple[tuple[str, tuple[FlowCatalogLike, ...]], ...]
    selected_flow_name: str | None

    @property
    def entries_by_name(self) -> dict[str, FlowCatalogLike]:
        """Return entries keyed by internal flow name."""
        return {entry.name: entry for entry in self.entries}

    @property
    def selected_entry(self) -> FlowCatalogLike | None:
        """Return the normalized selected entry, if any."""
        if self.selected_flow_name is None:
            return None
        return self.entries_by_name.get(self.selected_flow_name)

    @property
    def cards(self) -> tuple[FlowCatalogLike, ...]:
        """Return catalog entries under the shared flow metadata protocol."""
        return self.entries

    @property
    def grouped_cards(self) -> tuple[tuple[str, tuple[FlowCatalogLike, ...]], ...]:
        """Return grouped entries under the shared flow metadata protocol."""
        return self.grouped_entries

    @property
    def selected_card(self) -> FlowCatalogLike | None:
        """Return the selected flow metadata under the shared flow protocol."""
        return self.selected_entry

    @property
    def selected_list_index(self) -> int | None:
        """Return the list index for the selected flow in a grouped header+item list."""
        if self.selected_flow_name is None:
            return None
        index = 0
        for _group_name, entries in self.grouped_entries:
            index += 1
            for entry in entries:
                if entry.name == self.selected_flow_name:
                    return index
                index += 1
        return None


@dataclass(frozen=True)
class RunGroupSummary:
    """Persisted grouped-run summary for one flow run."""

    flow_name: str
    run_id: str
    source_label: str | None
    state: Literal["success", "failed", "stopped", "running"]
    started_at_utc: str | None
    finished_at_utc: str | None
    elapsed_seconds: float | None
    error_text: str | None


@dataclass(frozen=True)
class RunStepDetail:
    """Persisted step detail for one run."""

    run_id: str
    step_name: str
    state: Literal["started", "success", "failed", "stopped"]
    elapsed_seconds: float | None
    output_path: str | None
    error_text: str | None


@dataclass(frozen=True)
class RunLogEntry:
    """Persisted log entry detail for one run."""

    run_id: str
    flow_name: str
    level: str
    created_at_utc: str
    text: str


class CatalogPort(Protocol):
    """Catalog query boundary for operator surfaces."""

    def list_flows(self, *, workspace_root: Path | None) -> tuple[FlowCatalogItem, ...]: ...

    def load_workspace_catalog(
        self,
        *,
        workspace_root: Path | None,
        current_state: FlowCatalogState | None = None,
        missing_message: str = "No flow modules discovered.",
    ) -> WorkspaceCatalogLoadResult: ...

    def select_flow(
        self,
        *,
        catalog_state: FlowCatalogState,
        flow_name: str | None,
    ) -> FlowCatalogState: ...

    def build_catalog_presentation(
        self,
        *,
        catalog_state: FlowCatalogState,
    ) -> WorkspaceCatalogPresentation: ...

    def get_flow_preview(
        self,
        *,
        card: FlowCatalogLike | None,
        flow_states: dict[str, str],
    ) -> FlowConfigPreview: ...


class HistoryPort(Protocol):
    """History query boundary for operator surfaces."""

    def list_flow_runs(self, store: FlowLogStore, *, flow_name: str | None) -> tuple[FlowRunState, ...]: ...

    def list_run_groups(self, store: FlowLogStore, *, flow_name: str | None, limit: int = 50) -> tuple[RunGroupSummary, ...]: ...

    def get_run_steps(self, ledger: RuntimeCacheStore, *, run_id: str) -> tuple[RunStepDetail, ...]: ...

    def get_run_logs(
        self,
        store: FlowLogStore,
        *,
        run_id: str,
        flow_name: str | None = None,
        limit: int = 500,
    ) -> tuple[RunLogEntry, ...]: ...


class CatalogQueryService:
    """Own explicit catalog query shapes for operator surfaces."""

    def __init__(self, *, flow_catalog_service: FlowCatalogService) -> None:
        self.flow_catalog_service = flow_catalog_service

    def list_flows(self, *, workspace_root: Path | None) -> tuple[FlowCatalogItem, ...]:
        """Return lightweight catalog items for one workspace root."""
        entries = self.flow_catalog_service.load_entries(workspace_root=workspace_root)
        return tuple(
            FlowCatalogItem(
                flow_name=entry.name,
                group_name=entry.group or "",
                title=entry.title,
                runtime_kind=entry.mode if entry.mode in {"manual", "poll", "schedule"} else "manual",
                settle=(int(entry.settle) if entry.settle != "-" else None),
                max_parallel=max(int(entry.parallelism), 1),
            )
            for entry in entries
        )

    def load_workspace_catalog(
        self,
        *,
        workspace_root: Path | None,
        current_state: FlowCatalogState | None = None,
        missing_message: str = "No flow modules discovered.",
    ) -> WorkspaceCatalogLoadResult:
        """Return one normalized catalog load result for a resolved workspace root."""
        if workspace_root is None or not (workspace_root / "flow_modules").is_dir():
            return WorkspaceCatalogLoadResult(
                catalog_state=self.empty_catalog_state(message=missing_message, current_state=current_state),
                loaded=False,
            )
        try:
            catalog_state = self.load_catalog_state(
                workspace_root=workspace_root,
                current_state=current_state,
            )
        except FlowValidationError as exc:
            message = str(exc)
            return WorkspaceCatalogLoadResult(
                catalog_state=self.empty_catalog_state(message=message, current_state=current_state),
                loaded=False,
                error_text=message,
            )
        return WorkspaceCatalogLoadResult(catalog_state=catalog_state, loaded=True)

    def load_catalog_state(
        self,
        *,
        workspace_root: Path,
        current_state: FlowCatalogState | None = None,
    ) -> FlowCatalogState:
        """Load discovered entries and merge them into one catalog state."""
        base = current_state or FlowCatalogState.empty()
        entries = self.flow_catalog_service.load_entries(workspace_root=workspace_root)
        state = base.with_entries(entries).with_empty_message("")
        if base.selected_flow_name is not None and state.selected_flow_name == base.selected_flow_name:
            return state
        return state.with_selected_flow_name(_first_grouped_entry_name(state.entries))

    @staticmethod
    def empty_catalog_state(
        *,
        message: str = "",
        current_state: FlowCatalogState | None = None,
    ) -> FlowCatalogState:
        """Return an empty catalog state with one host-provided message."""
        base = current_state or FlowCatalogState.empty()
        return FlowCatalogState.empty(empty_message=message).with_selected_flow_name(base.selected_flow_name)

    @staticmethod
    def select_flow(
        *,
        catalog_state: FlowCatalogState,
        flow_name: str | None,
    ) -> FlowCatalogState:
        """Return catalog state with one normalized selected flow."""
        return catalog_state.with_selected_flow_name(flow_name)

    @staticmethod
    def build_catalog_presentation(
        *,
        catalog_state: FlowCatalogState,
    ) -> WorkspaceCatalogPresentation:
        """Return grouped UI-friendly catalog presentation from one catalog state."""
        grouped = tuple(
            (bucket.group_name, bucket.entries)
            for bucket in group_cards(catalog_state.entries)
        )
        return WorkspaceCatalogPresentation(
            entries=catalog_state.entries,
            grouped_entries=grouped,
            selected_flow_name=catalog_state.selected_flow_name,
        )

    def get_flow_preview(
        self,
        *,
        card: FlowCatalogLike | None,
        flow_states: dict[str, str],
    ) -> FlowConfigPreview:
        """Return config-preview rows for one selected flow."""
        flow_name = card.name if card is not None else ""
        return FlowConfigPreview(
            flow_name=flow_name,
            rows=FlowSummaryRow.pairs_for_flow(card, flow_states),
        )


class HistoryQueryService:
    """Own explicit persisted-history query shapes for operator surfaces."""

    def __init__(self, *, log_service: LogService) -> None:
        self.log_service = log_service

    @staticmethod
    def _run_group_summary(run_group: FlowRunState) -> RunGroupSummary:
        started_at_utc = run_group.entries[0].created_at_utc.isoformat() if run_group.entries else None
        finished_at_utc = (
            run_group.summary_entry.created_at_utc.isoformat()
            if run_group.summary_entry is not None and run_group.status in {"success", "failed", "stopped"}
            else None
        )
        error_text = run_group.summary_entry.line if run_group.status == "failed" and run_group.summary_entry is not None else None
        state = run_group.status if run_group.status in {"success", "failed", "stopped"} else "running"
        return RunGroupSummary(
            flow_name=run_group.key[0],
            run_id=run_group.key[1],
            source_label=run_group.source_label if run_group.source_label not in {"", "-"} else None,
            state=state,
            started_at_utc=started_at_utc,
            finished_at_utc=finished_at_utc,
            elapsed_seconds=run_group.elapsed_seconds,
            error_text=error_text,
        )

    def list_flow_runs(self, store: FlowLogStore, *, flow_name: str | None) -> tuple[FlowRunState, ...]:
        """Return grouped run states for one flow."""
        return self.log_service.runs_for_flow(store, flow_name)

    def list_run_groups(self, store: FlowLogStore, *, flow_name: str | None, limit: int = 50) -> tuple[RunGroupSummary, ...]:
        """Return grouped run summaries for one flow."""
        run_groups = self.log_service.runs_for_flow(store, flow_name)
        if limit >= 0:
            run_groups = run_groups[-limit:]
        return tuple(self._run_group_summary(run_group) for run_group in run_groups)

    def get_run_steps(self, ledger: RuntimeCacheStore, *, run_id: str) -> tuple[RunStepDetail, ...]:
        """Return persisted step details for one run id."""
        step_runs = ledger.step_outputs.list_for_run(run_id)
        return tuple(
            RunStepDetail(
                run_id=run_id,
                step_name=step_run.step_label,
                state=step_run.status if step_run.status in {"started", "success", "failed", "stopped"} else "started",
                elapsed_seconds=step_run.elapsed_seconds,
                output_path=str(step_run.output_path) if step_run.output_path else None,
                error_text=step_run.error_text,
            )
            for step_run in step_runs
        )

    def get_run_logs(
        self,
        store: FlowLogStore,
        *,
        run_id: str,
        flow_name: str | None = None,
        limit: int = 500,
    ) -> tuple[RunLogEntry, ...]:
        """Return persisted log entries for one run id."""
        entries = self.log_service.entries_for_flow(store, flow_name)
        filtered: list[RunLogEntry] = []
        for entry in entries:
            event = entry.event
            if event is None or event.run_id != run_id:
                continue
            filtered.append(
                RunLogEntry(
                    run_id=run_id,
                    flow_name=event.flow_name,
                    level="error" if event.status == "failed" else "info",
                    created_at_utc=entry.created_at_utc.isoformat(),
                    text=entry.line,
                )
            )
        if limit >= 0:
            filtered = filtered[-limit:]
        return tuple(filtered)


__all__ = [
    "CatalogPort",
    "CatalogQueryService",
    "FlowCatalogItem",
    "FlowConfigPreview",
    "HistoryPort",
    "HistoryQueryService",
    "RunGroupSummary",
    "RunLogEntry",
    "RunStepDetail",
    "WorkspaceCatalogLoadResult",
    "WorkspaceCatalogPresentation",
]


def _first_grouped_entry_name(entries: tuple[FlowCatalogLike, ...]) -> str | None:
    """Return the first entry name in the same grouped order used by operator surfaces."""
    for bucket in group_cards(entries):
        if bucket.entries:
            return bucket.entries[0].name
    return None
