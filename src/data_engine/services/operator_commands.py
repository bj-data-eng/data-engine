"""Operator command port for GUI/TUI surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from data_engine.application import FlowRefreshResult, OperatorActionResult, OperatorControlApplication
from data_engine.application.runtime import RuntimeApplication
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.runtime_db import RuntimeCacheLedger, RuntimeControlLedger
from data_engine.services.reset import ResetService
from data_engine.services.workspace_provisioning import WorkspaceProvisioningService


@dataclass(frozen=True)
class OperatorCommandResult:
    """Normalized command result for one operator action."""

    requested: bool
    sync_after: bool = False
    ensure_daemon_started: bool = False
    status_text: str | None = None
    error_text: str | None = None


@dataclass(frozen=True)
class RefreshFlowsCommandResult:
    """Normalized command result for one flow refresh request."""

    reload_catalog: bool
    sync_after: bool = False
    status_text: str | None = None
    warning_text: str | None = None
    error_text: str | None = None


@dataclass(frozen=True)
class ProvisionWorkspaceCommandResult:
    """Normalized command result for workspace provisioning."""

    workspace_id: str
    workspace_name: str | None = None
    created_names: str | None = None
    error_text: str | None = None


@dataclass(frozen=True)
class ResetWorkspaceCommandResult:
    """Normalized command result for workspace reset."""

    workspace_id: str
    error_text: str | None = None


@dataclass(frozen=True)
class ResetFlowCommandResult:
    """Normalized command result for flow reset."""

    flow_name: str
    error_text: str | None = None


@dataclass(frozen=True)
class ForceShutdownCommandResult:
    """Normalized command result for force-stopping the daemon."""

    error_text: str | None = None


class CommandPort(Protocol):
    """Command boundary for operator surfaces."""

    def run_selected_flow(self, **kwargs) -> OperatorCommandResult: ...
    def start_engine(self, **kwargs) -> OperatorCommandResult: ...
    def stop_pipeline(self, **kwargs) -> OperatorCommandResult: ...
    def request_control(self, daemon_manager: WorkspaceDaemonManager) -> OperatorCommandResult: ...
    def refresh_flows(self, **kwargs) -> RefreshFlowsCommandResult: ...
    def provision_workspace(self, paths: WorkspacePaths, *, interpreter_path: Path | None = None) -> ProvisionWorkspaceCommandResult: ...
    def force_shutdown_daemon(self, paths: WorkspacePaths, *, timeout: float = 0.5) -> ForceShutdownCommandResult: ...
    def reset_workspace(
        self,
        *,
        paths: WorkspacePaths,
        runtime_cache_ledger: RuntimeCacheLedger,
        runtime_control_ledger: RuntimeControlLedger,
    ) -> ResetWorkspaceCommandResult: ...
    def reset_flow(
        self,
        *,
        paths: WorkspacePaths,
        runtime_cache_ledger: RuntimeCacheLedger,
        flow_name: str,
    ) -> ResetFlowCommandResult: ...


class OperatorCommandService:
    """Own explicit operator command orchestration for GUI/TUI surfaces."""

    def __init__(
        self,
        *,
        control_application: OperatorControlApplication,
        runtime_application: RuntimeApplication,
        reset_service: ResetService,
        workspace_provisioning_service: WorkspaceProvisioningService | None,
    ) -> None:
        self.control_application = control_application
        self.runtime_application = runtime_application
        self.reset_service = reset_service
        self.workspace_provisioning_service = workspace_provisioning_service

    @staticmethod
    def _command_result(result: OperatorActionResult) -> OperatorCommandResult:
        return OperatorCommandResult(
            requested=getattr(result, "requested", False),
            sync_after=getattr(result, "sync_after", False),
            ensure_daemon_started=getattr(result, "ensure_daemon_started", False),
            status_text=getattr(result, "status_text", None),
            error_text=getattr(result, "error_text", None),
        )

    @staticmethod
    def _refresh_result(result: FlowRefreshResult) -> RefreshFlowsCommandResult:
        return RefreshFlowsCommandResult(
            reload_catalog=result.reload_catalog,
            sync_after=result.sync_after,
            status_text=result.status_text,
            warning_text=result.warning_text,
            error_text=result.error_text,
        )

    def run_selected_flow(self, **kwargs) -> OperatorCommandResult:
        """Validate and request one manual flow run."""
        return self._command_result(self.control_application.run_selected_flow(**kwargs))

    def start_engine(self, **kwargs) -> OperatorCommandResult:
        """Validate and request engine start."""
        return self._command_result(self.control_application.start_engine(**kwargs))

    def stop_pipeline(self, **kwargs) -> OperatorCommandResult:
        """Validate and request engine/manual stop."""
        return self._command_result(self.control_application.stop_pipeline(**kwargs))

    def request_control(self, daemon_manager: WorkspaceDaemonManager) -> OperatorCommandResult:
        """Request workspace control."""
        return self._command_result(self.control_application.request_control(daemon_manager))

    def refresh_flows(self, **kwargs) -> RefreshFlowsCommandResult:
        """Reload local flow definitions and refresh daemon catalog state."""
        return self._refresh_result(self.control_application.refresh_flows(**kwargs))

    def provision_workspace(
        self,
        paths: WorkspacePaths,
        *,
        interpreter_path: Path | None = None,
    ) -> ProvisionWorkspaceCommandResult:
        """Provision one selected workspace."""
        if self.workspace_provisioning_service is None:
            return ProvisionWorkspaceCommandResult(
                workspace_id=paths.workspace_id,
                error_text="Workspace provisioning is not available for this surface.",
            )
        try:
            result = self.workspace_provisioning_service.provision_workspace(
                paths,
                interpreter_path=interpreter_path,
            )
        except Exception as exc:
            return ProvisionWorkspaceCommandResult(
                workspace_id=paths.workspace_id,
                error_text=str(exc),
            )
        created_names = ", ".join(path.name for path in result.created_paths) if result.created_paths else "nothing new"
        return ProvisionWorkspaceCommandResult(
            workspace_id=paths.workspace_id,
            workspace_name=result.workspace_root.name,
            created_names=created_names,
            error_text=None,
        )

    def force_shutdown_daemon(self, paths: WorkspacePaths, *, timeout: float = 0.5) -> ForceShutdownCommandResult:
        """Force-stop one local workspace daemon."""
        result = self.runtime_application.force_shutdown_daemon(paths, timeout=timeout)
        return ForceShutdownCommandResult(error_text=None if result.ok else result.error)

    def reset_workspace(
        self,
        *,
        paths: WorkspacePaths,
        runtime_cache_ledger: RuntimeCacheLedger,
        runtime_control_ledger: RuntimeControlLedger,
    ) -> ResetWorkspaceCommandResult:
        """Delete local and shared runtime state for one workspace."""
        try:
            self.reset_service.reset_workspace(
                paths=paths,
                runtime_cache_ledger=runtime_cache_ledger,
                runtime_control_ledger=runtime_control_ledger,
            )
        except Exception as exc:
            return ResetWorkspaceCommandResult(workspace_id=paths.workspace_id, error_text=str(exc))
        return ResetWorkspaceCommandResult(workspace_id=paths.workspace_id, error_text=None)

    def reset_flow(
        self,
        *,
        paths: WorkspacePaths,
        runtime_cache_ledger: RuntimeCacheLedger,
        flow_name: str,
    ) -> ResetFlowCommandResult:
        """Delete persisted history for one flow."""
        try:
            self.reset_service.reset_flow(
                paths=paths,
                runtime_cache_ledger=runtime_cache_ledger,
                flow_name=flow_name,
            )
        except Exception as exc:
            return ResetFlowCommandResult(flow_name=flow_name, error_text=str(exc))
        return ResetFlowCommandResult(flow_name=flow_name, error_text=None)


__all__ = [
    "CommandPort",
    "ForceShutdownCommandResult",
    "OperatorCommandResult",
    "OperatorCommandService",
    "ProvisionWorkspaceCommandResult",
    "RefreshFlowsCommandResult",
    "ResetFlowCommandResult",
    "ResetWorkspaceCommandResult",
]
