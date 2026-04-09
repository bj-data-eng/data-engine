"""Workspace runtime binding services for operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from os import getpid

from data_engine.hosts.daemon.manager import WorkspaceDaemonManager
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.runtime_db import RuntimeLedger
from data_engine.services.daemon_state import DaemonStateService
from data_engine.services.ledger import LedgerService
from data_engine.services.logs import LogService
from data_engine.views.logs import FlowLogStore


@dataclass(frozen=True)
class WorkspaceRuntimeBinding:
    """Concrete runtime resources bound to one selected workspace."""

    workspace_paths: WorkspacePaths
    runtime_ledger: RuntimeLedger
    log_store: FlowLogStore
    daemon_manager: WorkspaceDaemonManager


class WorkspaceRuntimeBindingService:
    """Own concrete runtime binding lifecycle for GUI/TUI surfaces."""

    def __init__(
        self,
        *,
        ledger_service: LedgerService,
        log_service: LogService,
        daemon_state_service: DaemonStateService,
    ) -> None:
        self.ledger_service = ledger_service
        self.log_service = log_service
        self.daemon_state_service = daemon_state_service

    def open_binding(self, workspace_paths: WorkspacePaths) -> WorkspaceRuntimeBinding:
        """Open one concrete runtime binding for a workspace selection."""
        runtime_ledger = self.ledger_service.open_for_workspace(workspace_paths.workspace_root)
        return WorkspaceRuntimeBinding(
            workspace_paths=workspace_paths,
            runtime_ledger=runtime_ledger,
            log_store=self.log_service.create_store(runtime_ledger),
            daemon_manager=self.daemon_state_service.create_manager(workspace_paths),
        )

    def close_binding(self, binding: WorkspaceRuntimeBinding) -> None:
        """Close one concrete runtime binding."""
        self.ledger_service.close(binding.runtime_ledger)

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
            binding.runtime_ledger,
            client_id=client_id,
            workspace_id=binding.workspace_paths.workspace_id,
            client_kind=client_kind,
            pid=getpid() if pid is None else pid,
        )

    def remove_client_session(self, binding: WorkspaceRuntimeBinding, client_id: str) -> None:
        """Remove one active local client session row."""
        self.ledger_service.remove_client_session(binding.runtime_ledger, client_id)

    def purge_process_client_sessions(
        self,
        binding: WorkspaceRuntimeBinding,
        *,
        client_kind: str,
        pid: int | None = None,
    ) -> None:
        """Remove all client sessions for this workspace/client-kind/process tuple."""
        self.ledger_service.purge_process_client_sessions(
            binding.runtime_ledger,
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
            binding.runtime_ledger,
            binding.workspace_paths.workspace_id,
            exclude_client_id=exclude_client_id,
        )


__all__ = ["WorkspaceRuntimeBinding", "WorkspaceRuntimeBindingService"]
