"""Host-agnostic operator control and action use cases."""

from __future__ import annotations

from dataclasses import dataclass

from data_engine.domain import RuntimeSessionState
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager
from data_engine.platform.workspace_models import WorkspacePaths, authored_workspace_is_available
from data_engine.services import DaemonStateService

from data_engine.application.runtime import RuntimeApplication


@dataclass(frozen=True)
class OperatorActionResult:
    """Normalized result for one operator control action."""

    requested: bool
    sync_after: bool = False
    ensure_daemon_started: bool = False
    status_text: str | None = None
    error_text: str | None = None


@dataclass(frozen=True)
class FlowRefreshResult:
    """Normalized result for one flow-refresh request."""

    reload_catalog: bool
    sync_after: bool = False
    status_text: str | None = None
    warning_text: str | None = None
    error_text: str | None = None


class OperatorControlApplication:
    """Own host-neutral action orchestration for operator surfaces."""

    def __init__(
        self,
        *,
        runtime_application: RuntimeApplication,
        daemon_state_service: DaemonStateService,
    ) -> None:
        self.runtime_application = runtime_application
        self.daemon_state_service = daemon_state_service

    def run_selected_flow(
        self,
        *,
        paths: WorkspacePaths,
        runtime_session: RuntimeSessionState,
        selected_flow_name: str | None,
        selected_flow_valid: bool,
        selected_flow_group: str | None,
        selected_flow_group_active: bool,
        blocked_status_text: str,
        timeout: float = 2.0,
    ) -> OperatorActionResult:
        """Validate and request one manual run for the selected flow."""
        if not authored_workspace_is_available(paths):
            return OperatorActionResult(requested=False, error_text="Workspace root is no longer available.")
        if selected_flow_name is None:
            return OperatorActionResult(requested=False, status_text="Select one flow first.")
        if not selected_flow_valid:
            return OperatorActionResult(
                requested=False,
                status_text=f"{selected_flow_name} is invalid and cannot run.",
            )
        if selected_flow_group_active or runtime_session.manual_run_active:
            return OperatorActionResult(requested=False)
        if not runtime_session.control_available:
            return OperatorActionResult(requested=False, status_text=blocked_status_text)
        result = self.runtime_application.run_flow(
            paths,
            name=selected_flow_name,
            wait=False,
            timeout=timeout,
        )
        if not result.ok:
            return OperatorActionResult(
                requested=False,
                error_text=_verbose_action_error(
                    f"run {selected_flow_name}",
                    result.error,
                ),
            )
        return OperatorActionResult(
            requested=True,
            sync_after=True,
            status_text=f"Running {selected_flow_name}...",
        )

    def start_engine(
        self,
        *,
        paths: WorkspacePaths,
        runtime_session: RuntimeSessionState,
        has_automated_flows: bool,
        blocked_status_text: str,
        timeout: float = 2.0,
    ) -> OperatorActionResult:
        """Validate and request automated engine start."""
        if not authored_workspace_is_available(paths):
            return OperatorActionResult(requested=False, error_text="Workspace root is no longer available.")
        if runtime_session.runtime_active or runtime_session.runtime_stopping or runtime_session.manual_run_active:
            return OperatorActionResult(requested=False)
        if not runtime_session.control_available:
            return OperatorActionResult(requested=False, status_text=blocked_status_text)
        if not has_automated_flows:
            return OperatorActionResult(requested=False, status_text="No automated flows are available.")
        result = self.runtime_application.start_engine(paths, timeout=timeout)
        if not result.ok:
            return OperatorActionResult(
                requested=False,
                error_text=_verbose_action_error("start the automated engine", result.error),
            )
        return OperatorActionResult(requested=True, sync_after=True, status_text="Starting automated engine...")

    def stop_pipeline(
        self,
        *,
        paths: WorkspacePaths,
        runtime_session: RuntimeSessionState,
        selected_flow_group: str | None,
        blocked_status_text: str,
        timeout: float = 2.0,
    ) -> OperatorActionResult:
        """Validate and request stop for the engine or selected manual flow."""
        if runtime_session.runtime_active:
            result = self.runtime_application.stop_engine(paths, timeout=timeout)
            if not result.ok:
                return OperatorActionResult(
                    requested=False,
                    error_text=_verbose_action_error("stop the engine", result.error),
                )
            return OperatorActionResult(requested=True, sync_after=True, status_text="Stopping engine...")
        if runtime_session.manual_run_active:
            if not runtime_session.control_available:
                return OperatorActionResult(requested=False, status_text=blocked_status_text)
            flow_name = runtime_session.active_manual_runs.get(selected_flow_group)
            if flow_name is None:
                return OperatorActionResult(requested=False)
            result = self.runtime_application.stop_flow(paths, name=flow_name, timeout=timeout)
            if not result.ok:
                return OperatorActionResult(
                    requested=False,
                    error_text=_verbose_action_error(f"stop {flow_name}", result.error),
                )
            return OperatorActionResult(requested=True, sync_after=True, status_text="Stopping selected flow...")
        return OperatorActionResult(requested=False)

    def request_control(self, daemon_manager: WorkspaceDaemonManager) -> OperatorActionResult:
        """Request workspace control through the daemon-state manager."""
        try:
            message = self.daemon_state_service.request_control(daemon_manager)
        except Exception as exc:
            return OperatorActionResult(
                requested=False,
                error_text=_verbose_action_error("request workspace control", exc),
            )
        return OperatorActionResult(
            requested=True,
            sync_after=True,
            ensure_daemon_started=True,
            status_text=message,
        )

    def refresh_flows(
        self,
        *,
        paths: WorkspacePaths,
        runtime_session: RuntimeSessionState,
        has_authored_workspace: bool,
        timeout: float = 5.0,
    ) -> FlowRefreshResult:
        """Validate and request one flow refresh while preserving local reload behavior."""
        if runtime_session.runtime_active or runtime_session.active_manual_runs:
            return FlowRefreshResult(
                reload_catalog=False,
                error_text="Stop active engine or manual runs before refreshing flows.",
            )
        if not has_authored_workspace:
            return FlowRefreshResult(
                reload_catalog=True,
                sync_after=True,
                status_text="No flow modules discovered.",
            )
        result = self.runtime_application.refresh_flows(paths, timeout=timeout)
        if not result.ok:
            return FlowRefreshResult(
                reload_catalog=True,
                sync_after=True,
                status_text="Reloaded flow definitions.",
                warning_text=_verbose_action_error("refresh flows", result.error),
            )
        return FlowRefreshResult(
            reload_catalog=True,
            sync_after=True,
            status_text="Reloaded flow definitions.",
        )


def _verbose_action_error(action: str, detail: object | None) -> str:
    """Return a non-terse user-facing failure string for operator control paths."""
    text = str(detail).strip() if detail is not None else ""
    if text:
        return text
    return f"Failed to {action}. The daemon returned no additional detail."


__all__ = ["FlowRefreshResult", "OperatorActionResult", "OperatorControlApplication"]
