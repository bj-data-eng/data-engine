"""Shared operator action-state view models across GUI and TUI."""

from __future__ import annotations

from dataclasses import dataclass

from data_engine.domain import OperatorActionContext


@dataclass(frozen=True)
class GuiActionState:
    """Button and control state for the desktop GUI surface."""

    flow_run_label: str
    flow_run_enabled: bool
    flow_config_enabled: bool
    engine_enabled: bool
    engine_label: str
    engine_state: str
    refresh_enabled: bool
    clear_flow_log_enabled: bool
    request_control_visible: bool
    request_control_enabled: bool

    @classmethod
    def from_context(cls, context: OperatorActionContext) -> "GuiActionState":
        """Return the GUI action state derived from one operator action context."""
        session = context.runtime_session
        selected = context.selected_flow
        active = session.runtime_active or session.runtime_stopping
        return cls(
            flow_run_label="Running..." if selected.running else "Run Once",
            flow_run_enabled=selected.valid and not selected.group_active and session.control_available and context.workspace_available,
            flow_config_enabled=selected.present,
            engine_enabled=(
                session.runtime_active
                or (context.has_automated_flows and session.control_available and context.workspace_available)
            ) and not session.runtime_stopping,
            engine_label="Stopping..." if session.runtime_stopping else "Stop Engine" if active else "Start Engine",
            engine_state="running" if active else "stopped",
            refresh_enabled=not session.runtime_active and not session.manual_run_active,
            clear_flow_log_enabled=selected.present and selected.has_logs,
            request_control_visible=True,
            request_control_enabled=not session.workspace_owned,
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
    workspace_select_disabled: bool
    @classmethod
    def from_context(cls, context: OperatorActionContext) -> "TuiActionState":
        """Return the TUI action state derived from one operator action context."""
        session = context.runtime_session
        busy = session.runtime_active or session.manual_run_active or session.runtime_stopping
        return cls(
            refresh_disabled=busy,
            run_once_disabled=busy or not session.control_available or not context.workspace_available,
            start_engine_disabled=busy or not session.control_available or not context.workspace_available,
            stop_engine_disabled=not busy,
            view_config_disabled=not context.selected_flow.present,
            view_log_disabled=not context.selected_run_group_present,
            clear_flow_log_disabled=not context.selected_flow.present,
            workspace_select_disabled=False,
        )


__all__ = [
    "GuiActionState",
    "TuiActionState",
]
