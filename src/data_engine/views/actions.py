"""Shared operator action-state view models across GUI and TUI."""

from __future__ import annotations

from dataclasses import dataclass

from data_engine.domain import OperatorActionContext, PendingWorkspaceActionOverlay, SelectedFlowState


def build_operator_action_context(
    *,
    card,
    flow_states: dict[str, str],
    runtime_session,
    flow_groups_by_name: dict[str, str | None],
    active_flow_states,
    engine_state: str = "idle",
    engine_truth_known: bool = False,
    live_runs: dict[str, object] | None = None,
    engine_active_flow_names: tuple[str, ...] = (),
    has_logs: bool,
    has_automated_flows: bool,
    workspace_available: bool = True,
    selected_run_group_present: bool = False,
    local_request_pending: bool = False,
    overlay: PendingWorkspaceActionOverlay | None = None,
) -> OperatorActionContext:
    """Return one operator action context from current runtime and selection state."""
    selected_flow = SelectedFlowState.from_runtime(
        card=card,
        flow_states=flow_states,
        runtime_session=runtime_session,
        flow_groups_by_name=flow_groups_by_name,
        active_flow_states=active_flow_states,
        has_logs=has_logs,
        live_runs=live_runs,
        engine_active_flow_names=engine_active_flow_names,
    )
    live_manual_run_active = False
    if live_runs is not None:
        engine_flow_names = set(engine_active_flow_names)
        for run in live_runs.values():
            state = str(getattr(run, "state", "") or "").strip().lower()
            flow_name = str(getattr(run, "flow_name", "") or "").strip()
            if state not in {"starting", "running", "stopping"}:
                continue
            if flow_name in engine_flow_names:
                continue
            live_manual_run_active = True
            break
    return OperatorActionContext(
        runtime_session=runtime_session,
        selected_flow=selected_flow,
        has_automated_flows=has_automated_flows,
        engine_state=engine_state,
        engine_truth_known=engine_truth_known,
        live_truth_known=live_runs is not None,
        live_manual_run_active=live_manual_run_active,
        workspace_available=workspace_available,
        selected_run_group_present=selected_run_group_present,
        local_request_pending=local_request_pending,
        overlay=PendingWorkspaceActionOverlay() if overlay is None else overlay,
    )


@dataclass(frozen=True)
class GuiActionState:
    """Button and control state for the desktop GUI surface."""

    flow_run_label: str
    flow_run_state: str
    flow_run_enabled: bool
    flow_config_enabled: bool
    engine_enabled: bool
    engine_label: str
    engine_state: str
    refresh_enabled: bool
    clear_flow_log_label: str
    clear_flow_log_enabled: bool
    request_control_label: str
    request_control_visible: bool
    request_control_enabled: bool

    @classmethod
    def from_context(cls, context: OperatorActionContext) -> "GuiActionState":
        """Return the GUI action state derived from one operator action context."""
        selected = context.selected_flow
        overlay = context.overlay
        selected_manual_running = context.selected_manual_running
        engine_state_name = context.normalized_engine_state
        engine_running = context.engine_running
        engine_busy = context.engine_busy
        selected_live_stopping = selected.stopping
        selected_group = None if selected.card is None else selected.card.group
        selected_manual_stopping = overlay.manual_group_stopping(
            selected_group,
            selected_manual_running=selected_manual_running,
        )
        selected_manual_starting = overlay.manual_group_starting(
            selected_group,
            selected_manual_running=selected_manual_running,
            selected_manual_stopping=selected_live_stopping or selected_manual_stopping,
        )
        return cls(
            flow_run_label=(
                "Stopping..."
                if selected_live_stopping or selected_manual_stopping or (overlay.stop_pipeline_pending and selected_manual_running)
                else "Stop Flow"
                if selected_manual_running
                else "Starting..."
                if overlay.run_selected_flow_pending or selected_manual_starting
                else "Running..."
                if selected.running
                else "Run Once"
            ),
            flow_run_state="stop" if selected_manual_running or selected_live_stopping else "run",
            flow_run_enabled=(
                (
                    (
                        selected_manual_running
                        and not selected_live_stopping
                        and not selected_manual_stopping
                        and not overlay.stop_pipeline_pending
                    )
                    or (
                        selected.valid
                        and not selected.group_active
                        and not (selected.automated and (context.engine_busy or overlay.engine_transition_pending))
                        and not overlay.run_selected_flow_pending
                        and not selected_manual_starting
                    )
                ) and context.control_available and context.workspace_available
            ),
            flow_config_enabled=selected.present,
            engine_enabled=(
                (engine_running and context.control_available and context.workspace_available)
                or (
                    engine_state_name == "idle"
                    and context.has_automated_flows
                    and context.control_available
                    and context.workspace_available
                    and not context.manual_run_active
                    and not selected_manual_starting
                )
            ) and engine_state_name not in {"starting", "stopping"} and not overlay.engine_transition_pending,
            engine_label=(
                "Stopping..."
                if overlay.stop_engine_pending or overlay.stop_pipeline_pending or engine_state_name == "stopping"
                else "Starting..."
                if overlay.start_engine_pending or engine_state_name == "starting"
                else "Stop Engine"
                if engine_running
                else "Start Engine"
            ),
            engine_state="running" if engine_running else "stopped",
            refresh_enabled=not engine_busy and not context.manual_run_active and not overlay.engine_transition_pending and not selected_manual_starting and not overlay.refresh_flows_pending,
            clear_flow_log_label="Resetting..." if overlay.reset_flow_pending else "Reset Flow",
            clear_flow_log_enabled=(
                selected.present
                and not context.manual_run_active
                and not engine_busy
                and context.control_available
                and context.workspace_available
                and not selected_manual_starting
                and not overlay.engine_transition_pending
                and not overlay.reset_flow_pending
            ),
            request_control_label="Requesting..." if (context.local_request_pending or overlay.request_control_pending) else "Request Control",
            request_control_visible=True,
            request_control_enabled=context.request_control_enabled,
        )


@dataclass(frozen=True)
class TuiActionState:
    """Button and control state for the terminal UI surface."""

    refresh_disabled: bool
    run_once_disabled: bool
    start_engine_disabled: bool
    stop_engine_disabled: bool
    view_config_disabled: bool
    view_log_disabled: bool
    clear_flow_log_disabled: bool
    @classmethod
    def from_context(cls, context: OperatorActionContext) -> "TuiActionState":
        """Return the TUI action state derived from one operator action context."""
        engine_starting = context.engine_starting
        engine_busy = context.engine_running
        overlay = context.overlay
        busy = context.engine_busy or context.manual_run_active or overlay.engine_transition_pending
        return cls(
            refresh_disabled=busy or overlay.refresh_flows_pending,
            run_once_disabled=(
                engine_starting
                or overlay.run_selected_flow_pending
                or (context.selected_flow.automated and context.engine_busy)
                or
                context.selected_flow.group_active
                or not context.control_available
                or not context.workspace_available
            ),
            start_engine_disabled=busy or not context.control_available or not context.workspace_available,
            stop_engine_disabled=(
                not (engine_busy or context.manual_run_active)
                or not context.control_available
                or not context.workspace_available
            ),
            view_config_disabled=not context.selected_flow.present,
            view_log_disabled=not context.selected_run_group_present,
            clear_flow_log_disabled=(
                not context.selected_flow.present
                or busy
                or context.selected_flow.group_active
                or not context.control_available
                or not context.workspace_available
            ),
        )


__all__ = [
    "build_operator_action_context",
    "GuiActionState",
    "TuiActionState",
]
