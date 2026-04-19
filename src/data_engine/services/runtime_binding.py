"""Workspace runtime binding services for operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from os import getpid
from typing import TYPE_CHECKING

from data_engine.domain import FlowLogEntry, FlowRunState, StepOutputIndex
from data_engine.domain.catalog import FlowCatalogLike
from data_engine.domain.time import parse_utc_text
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.services.daemon_state import DaemonStateService
from data_engine.services.ledger import RuntimeControlLedgerService
from data_engine.services.logs import LogService
from data_engine.services.runtime_history import RuntimeHistoryService
from data_engine.services.runtime_io import RuntimeIoLayer, default_runtime_io_layer
from data_engine.services.runtime_ports import RuntimeCacheStore, RuntimeControlStore
from data_engine.views.logs import FlowLogStore

if TYPE_CHECKING:
    from data_engine.application.runtime import RuntimeApplication


class _NullRuntimeCacheLedger:
    """In-memory no-op runtime cache ledger for an unconfigured workspace selection."""

    def __init__(self) -> None:
        self.runs = _NullRuntimeRunRepository()
        self.step_outputs = _NullRuntimeStepOutputRepository()
        self.logs = _NullRuntimeLogRepository()
        self.source_signatures = _NullRuntimeSourceSignatureRepository()
        self.execution_state = _NullRuntimeExecutionStateRepository()

    def close(self) -> None:
        return


class _NullRuntimeRunRepository:
    """No-op runtime run repository for an unconfigured workspace selection."""

    def list(self, *, flow_name: str | None = None) -> tuple[object, ...]:
        """Return no runs for an unconfigured workspace selection."""
        del flow_name
        return ()


class _NullRuntimeLogRepository:
    """No-op runtime log repository for an unconfigured workspace selection."""

    def list(
        self,
        *,
        flow_name: str | None = None,
        run_id: str | None = None,
        after_id: int | None = None,
    ) -> tuple[object, ...]:
        """Return no logs for an unconfigured workspace selection."""
        del flow_name, run_id, after_id
        return ()


class _NullRuntimeStepOutputRepository:
    """No-op step-output repository for an unconfigured workspace selection."""

    def list_for_run(self, run_id: str) -> tuple[object, ...]:
        """Return no step outputs for an unconfigured workspace selection."""
        del run_id
        return ()

    def list(self, *, flow_name: str | None = None, after_id: int | None = None) -> tuple[object, ...]:
        """Return no step outputs for an unconfigured workspace selection."""
        del flow_name, after_id
        return ()


class _NullRuntimeSourceSignatureRepository:
    """No-op source-signature repository for an unconfigured workspace selection."""

    def list_file_states(self, *, flow_name: str | None = None) -> tuple[object, ...]:
        """Return no file states for an unconfigured workspace selection."""
        del flow_name
        return ()


class _NullRuntimeExecutionStateRepository:
    """No-op execution-state writer for an unconfigured workspace selection."""

    def record_run_started(self, **kwargs: object) -> None:
        """Ignore execution-state writes for an unconfigured workspace selection."""
        del kwargs

    def record_run_finished(self, **kwargs: object) -> None:
        """Ignore execution-state writes for an unconfigured workspace selection."""
        del kwargs

    def record_step_started(self, **kwargs: object) -> int:
        """Ignore execution-state writes for an unconfigured workspace selection."""
        del kwargs
        return 0

    def record_step_finished(self, **kwargs: object) -> None:
        """Ignore execution-state writes for an unconfigured workspace selection."""
        del kwargs

    def upsert_file_state(self, **kwargs: object) -> None:
        """Ignore source-state writes for an unconfigured workspace selection."""
        del kwargs


class _NullClientSessionRepository:
    """No-op client-session repository for an unconfigured workspace selection."""

    def upsert(self, **kwargs: object) -> None:
        """Ignore client-session registration for an unconfigured workspace selection."""
        del kwargs

    def remove(self, client_id: str) -> None:
        """Ignore client-session removal for an unconfigured workspace selection."""
        del client_id

    def remove_for_process(self, *, workspace_id: str, client_kind: str, pid: int) -> None:
        """Ignore process-session removal for an unconfigured workspace selection."""
        del workspace_id, client_kind, pid

    def count_live(self, workspace_id: str, *, exclude_client_id: str | None = None) -> int:
        """Return zero live sessions for an unconfigured workspace selection."""
        del workspace_id, exclude_client_id
        return 0


class _NullRuntimeControlLedger:
    """No-op runtime control ledger for an unconfigured workspace selection."""

    def __init__(self) -> None:
        self.client_sessions = _NullClientSessionRepository()

    def close(self) -> None:
        return


@dataclass(frozen=True)
class WorkspaceRuntimeBinding:
    """Concrete runtime resources bound to one selected workspace."""

    workspace_paths: WorkspacePaths
    runtime_cache_ledger: RuntimeCacheStore
    runtime_control_ledger: RuntimeControlStore
    log_store: FlowLogStore
    daemon_manager: WorkspaceDaemonManager


class WorkspaceRuntimeBindingService:
    """Own concrete runtime binding lifecycle for GUI/TUI surfaces."""

    def __init__(
        self,
        *,
        ledger_service: RuntimeControlLedgerService,
        log_service: LogService,
        daemon_state_service: DaemonStateService,
        runtime_history_service: RuntimeHistoryService,
        runtime_io_layer: RuntimeIoLayer | None = None,
    ) -> None:
        self.ledger_service = ledger_service
        self.log_service = log_service
        self.daemon_state_service = daemon_state_service
        self.runtime_history_service = runtime_history_service
        self.runtime_io_layer = runtime_io_layer or default_runtime_io_layer()
        self._step_output_cache: dict[int, tuple[tuple[object, ...], int | None, StepOutputIndex]] = {}

    def open_binding(self, workspace_paths: WorkspacePaths) -> WorkspaceRuntimeBinding:
        """Open one concrete runtime binding for a workspace selection."""
        if workspace_paths.workspace_configured:
            runtime_cache_ledger = self.runtime_io_layer.open_cache_store(workspace_paths.runtime_cache_db_path)
            runtime_control_ledger = self.ledger_service.open_for_workspace(workspace_paths.workspace_root)
        else:
            runtime_cache_ledger = _NullRuntimeCacheLedger()
            runtime_control_ledger = _NullRuntimeControlLedger()
        return WorkspaceRuntimeBinding(
            workspace_paths=workspace_paths,
            runtime_cache_ledger=runtime_cache_ledger,
            runtime_control_ledger=runtime_control_ledger,
            log_store=self.log_service.create_store(runtime_cache_ledger),
            daemon_manager=self.daemon_state_service.create_manager(workspace_paths),
        )

    def close_binding(self, binding: WorkspaceRuntimeBinding) -> None:
        """Close one concrete runtime binding."""
        self._step_output_cache.pop(id(binding), None)
        binding.runtime_cache_ledger.close()
        self.ledger_service.close(binding.runtime_control_ledger)

    def register_client_session(
        self,
        binding: WorkspaceRuntimeBinding,
        *,
        client_id: str,
        client_kind: str,
        pid: int | None = None,
    ) -> None:
        """Register or refresh one local client session for the binding workspace."""
        self.ledger_service.register_client_session(
            binding.runtime_control_ledger,
            client_id=client_id,
            workspace_id=binding.workspace_paths.workspace_id,
            client_kind=client_kind,
            pid=getpid() if pid is None else pid,
        )

    def remove_client_session(self, binding: WorkspaceRuntimeBinding, client_id: str) -> None:
        """Remove one active local client session row."""
        self.ledger_service.remove_client_session(binding.runtime_control_ledger, client_id)

    def purge_process_client_sessions(
        self,
        binding: WorkspaceRuntimeBinding,
        *,
        client_kind: str,
        pid: int | None = None,
    ) -> None:
        """Remove all client sessions for this workspace/client-kind/process tuple."""
        self.ledger_service.purge_process_client_sessions(
            binding.runtime_control_ledger,
            workspace_id=binding.workspace_paths.workspace_id,
            client_kind=client_kind,
            pid=getpid() if pid is None else pid,
        )

    def count_live_client_sessions(
        self,
        binding: WorkspaceRuntimeBinding,
        *,
        exclude_client_id: str | None = None,
    ) -> int:
        """Return the number of live local client sessions for the binding workspace."""
        return self.ledger_service.count_live_client_sessions(
            binding.runtime_control_ledger,
            binding.workspace_paths.workspace_id,
            exclude_client_id=exclude_client_id,
        )

    def sync_runtime_state(
        self,
        binding: WorkspaceRuntimeBinding,
        *,
        runtime_application: "RuntimeApplication",
        flow_cards,
        daemon_startup_in_progress: bool = False,
    ) -> object:
        """Return daemon/runtime sync state for one bound workspace."""
        return runtime_application.sync_state(
            paths=binding.workspace_paths,
            daemon_manager=binding.daemon_manager,
            flow_cards=flow_cards,
            runtime_ledger=binding.runtime_cache_ledger,
            daemon_startup_in_progress=daemon_startup_in_progress,
        )

    def reload_logs(self, binding: WorkspaceRuntimeBinding) -> None:
        """Reload the binding log store from its runtime cache store."""
        self.log_service.reload(binding.log_store, binding.runtime_cache_ledger)

    def invalidate_flow_history(self, binding: WorkspaceRuntimeBinding, *, flow_name: str) -> None:
        """Drop one flow's cached logs and derived step-output state after destructive resets."""
        self.log_service.clear_flow(binding.log_store, flow_name)
        self._step_output_cache.pop(id(binding), None)

    def rebuild_step_outputs(
        self,
        binding: WorkspaceRuntimeBinding,
        flow_cards: dict[str, FlowCatalogLike],
    ) -> StepOutputIndex:
        """Rebuild latest successful per-step output paths for visible flows."""
        cache_key = id(binding)
        flow_signature = tuple(
            sorted((flow_name, tuple(card.operation_items)) for flow_name, card in flow_cards.items())
        )
        cached = self._step_output_cache.get(cache_key)
        if cached is None or cached[0] != flow_signature:
            refreshed = self.runtime_history_service.rebuild_step_outputs(
                binding.runtime_cache_ledger,
                flow_cards,
            )
            last_step_run_id = refreshed.last_step_run_id
            index = refreshed.index
        else:
            last_seen_id = cached[1]
            current_index = cached[2]
            refreshed = self.runtime_history_service.refresh_step_outputs(
                binding.runtime_cache_ledger,
                flow_cards,
                current_index=current_index,
                last_seen_step_run_id=last_seen_id,
            )
            last_step_run_id = refreshed.last_step_run_id
            index = refreshed.index
        self._step_output_cache[cache_key] = (flow_signature, last_step_run_id, index)
        return index

    def error_text_for_entry(
        self,
        binding: WorkspaceRuntimeBinding,
        run_group: FlowRunState,
        entry: FlowLogEntry,
    ) -> tuple[str, str | None]:
        """Return one user-facing error title and persisted error text."""
        return self.runtime_history_service.error_text_for_entry(binding.runtime_cache_ledger, run_group, entry)

    def recent_run_count(self, binding: WorkspaceRuntimeBinding, *, days: int) -> int:
        """Return the number of persisted runs started in the recent window."""
        cutoff = datetime.now(UTC) - timedelta(days=days)
        count = 0
        try:
            runs = binding.runtime_cache_ledger.runs.list()
        except Exception:
            return 0
        for run in runs:
            started_at = parse_utc_text(run.started_at_utc)
            if started_at is not None and started_at >= cutoff:
                count += 1
        return count


__all__ = ["WorkspaceRuntimeBinding", "WorkspaceRuntimeBindingService"]
