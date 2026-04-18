"""Shared operator action-state view models across GUI and TUI."""

from __future__ import annotations

from dataclasses import dataclass

from data_engine.domain import OperatorActionContext, SelectedFlowState


def build_operator_action_context(
    *,
    card,
    flow_states: dict[str, str],
    runtime_session,
    flow_groups_by_name: dict[str, str | None],
    active_flow_states,
    engine_state: str = "idle",
    live_runs: dict[str, object] | None = None,
    has_logs: bool,
    has_automated_flows: bool,
    workspace_available: bool = True,
    selected_run_group_present: bool = False,
    local_request_pending: bool = False,
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
    )
    return OperatorActionContext(
        runtime_session=runtime_session,
        selected_flow=selected_flow,
        has_automated_flows=has_automated_flows,
        engine_state=engine_state,
        workspace_available=workspace_available,
        selected_run_group_present=selected_run_group_present,
        local_request_pending=local_request_pending,
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
        session = context.runtime_session
        selected = context.selected_flow
        selected_group = selected.card.group if selected.card is not None else None
        selected_manual_running = bool(
            selected.card is not None
            and selected.card.name == session.manual_flow_name_for_group(selected_group)
        )
        engine_state_name = context.engine_state if context.engine_state in {"idle", "starting", "running", "stopping"} else "idle"
        if engine_state_name == "idle":
            engine_state_name = "stopping" if session.runtime_stopping else "running" if session.runtime_active else "idle"
        engine_running = engine_state_name in {"running", "stopping"}
        engine_busy = engine_state_name in {"starting", "running", "stopping"}
        selected_live_stopping = selected.stopping
        return cls(
            flow_run_label=(
                "Stopping..."
                if selected_live_stopping
                else "Stop Flow"
                if selected_manual_running
                else "Running..."
                if selected.running
                else "Run Once"
            ),
            flow_run_state="stop" if selected_manual_running or selected_live_stopping else "run",
            flow_run_enabled=(
                (
                    (selected_manual_running and not selected_live_stopping)
                    or (selected.valid and not selected.group_active)
                )
                and session.control_available
                and context.workspace_available
            ),
            flow_config_enabled=selected.present,
            engine_enabled=(
                engine_running
                or (
                    engine_state_name == "idle"
                    and context.has_automated_flows
                    and session.control_available
                    and context.workspace_available
                    and not session.manual_run_active
                )
            ) and engine_state_name not in {"starting", "stopping"},
            engine_label=(
                "Stopping..."
                if engine_state_name == "stopping"
                else "Starting..."
                if engine_state_name == "starting"
                else "Stop Engine"
                if engine_running
                else "Start Engine"
            ),
            engine_state="running" if engine_running else "stopped",
            refresh_enabled=not engine_busy and not session.manual_run_active,
            clear_flow_log_label="Reset Flow",
            clear_flow_log_enabled=(
                selected.present
                and not session.manual_run_active
                and not engine_busy
                and session.control_available
                and context.workspace_available
            ),
            request_control_label="Requesting..." if context.local_request_pending else "Request Control",
            request_control_visible=True,
            request_control_enabled=(
                not session.workspace_owned
                and not context.local_request_pending
                and context.workspace_available
            ),
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
        session = context.runtime_session
        engine_state_name = context.engine_state if context.engine_state in {"idle", "starting", "running", "stopping"} else "idle"
        if engine_state_name == "idle":
            engine_state_name = "stopping" if session.runtime_stopping else "running" if session.runtime_active else "idle"
        engine_starting = engine_state_name == "starting"
        engine_busy = engine_state_name in {"running", "stopping"}
        busy = engine_starting or engine_busy or session.manual_run_active
        return cls(
            refresh_disabled=busy,
            run_once_disabled=(
                engine_starting
                or
                context.selected_flow.group_active
                or not session.control_available
                or not context.workspace_available
            ),
            start_engine_disabled=busy or not session.control_available or not context.workspace_available,
            stop_engine_disabled=not (engine_busy or session.manual_run_active),
            view_config_disabled=not context.selected_flow.present,
            view_log_disabled=not context.selected_run_group_present,
            clear_flow_log_disabled=(
                not context.selected_flow.present
                or busy
                or context.selected_flow.group_active
                or not session.control_available
                or not context.workspace_available
            ),
        )


__all__ = [
    "build_operator_action_context",
    "GuiActionState",
    "TuiActionState",
]
