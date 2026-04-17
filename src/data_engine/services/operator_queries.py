"""Catalog and history query ports for operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from data_engine.domain import FlowCatalogLike, FlowRunState, FlowSummaryRow
from data_engine.services.flow_catalog import FlowCatalogService
from data_engine.services.logs import LogService
from data_engine.services.runtime_ports import RuntimeCacheStore
from data_engine.views.logs import FlowLogStore


@dataclass(frozen=True)
class FlowCatalogItem:
    """Lightweight catalog row for one discovered flow."""

    flow_name: str
    group_name: str
    title: str
    runtime_kind: Literal["manual", "poll", "schedule"]
    max_parallel: int


@dataclass(frozen=True)
class FlowConfigPreview:
    """Config-preview rows for one selected flow."""

    flow_name: str
    rows: tuple[tuple[str, str], ...]


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

    def get_flow_preview(
        self,
        *,
        card: FlowCatalogLike | None,
        flow_states: dict[str, str],
    ) -> FlowConfigPreview: ...


class HistoryPort(Protocol):
    """History query boundary for operator surfaces."""

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
                max_parallel=max(int(entry.parallelism), 1),
            )
            for entry in entries
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
]
