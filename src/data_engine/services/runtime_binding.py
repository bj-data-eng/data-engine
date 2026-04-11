"""Workspace runtime binding services for operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from os import getpid

from data_engine.hosts.daemon.manager import WorkspaceDaemonManager
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.runtime_db import RuntimeCacheLedger
from data_engine.services.daemon_state import DaemonStateService
from data_engine.services.ledger import RuntimeControlLedgerService
from data_engine.services.logs import LogService
from data_engine.services.runtime_ports import RuntimeCacheStore, RuntimeControlStore
from data_engine.views.logs import FlowLogStore


class _NullRuntimeCacheLedger:
    """In-memory no-op runtime cache ledger for an unconfigured workspace selection."""

    def __init__(self) -> None:
        self.runs = _NullRuntimeRunRepository()
        self.logs = _NullRuntimeLogRepository()

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

    def list(self, *, flow_name: str | None = None, run_id: str | None = None) -> tuple[object, ...]:
        """Return no logs for an unconfigured workspace selection."""
        del flow_name, run_id
        return ()


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
    ) -> None:
        self.ledger_service = ledger_service
        self.log_service = log_service
        self.daemon_state_service = daemon_state_service

    def open_binding(self, workspace_paths: WorkspacePaths) -> WorkspaceRuntimeBinding:
        """Open one concrete runtime binding for a workspace selection."""
        if workspace_paths.workspace_configured:
            runtime_cache_ledger = RuntimeCacheLedger(workspace_paths.runtime_cache_db_path)
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


__all__ = ["WorkspaceRuntimeBinding", "WorkspaceRuntimeBindingService"]
