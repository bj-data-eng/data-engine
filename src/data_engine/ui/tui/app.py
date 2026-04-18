"""Textual-based terminal UI for Data Engine."""

from __future__ import annotations

import logging
from queue import Empty
import threading

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Grid, Horizontal, Vertical
from textual.widgets import Button, Footer, ListView, Select, Static

from data_engine.domain import (
    FlowRunState,
    RunDetailState,
)
from data_engine.core.model import FlowValidationError
from data_engine.views.text import render_run_group_lines
from data_engine.ui.tui.bootstrap import TuiServices
from data_engine.ui.tui.app_binding import bootstrap_tui_app
from data_engine.ui.tui.theme import DEFAULT_THEME, stylesheet as tui_stylesheet
from data_engine.ui.tui.state_support import TuiStateMixin
from data_engine.ui.tui.support import TuiWindowSupportMixin
from data_engine.ui.tui.runtime import QueueLogHandler
from data_engine.ui.tui.widgets import FlowListItem, GroupHeaderListItem, InfoModal, RunGroupListItem


class DataEngineTui(TuiWindowSupportMixin, TuiStateMixin, App[None]):
    """Full-screen terminal UI for headless Data Engine operation."""

    CSS = tui_stylesheet(DEFAULT_THEME)
    _ACTIVE_FLOW_STATES = {"running", "polling", "scheduled", "stopping flow", "stopping runtime"}

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh_flows", "Refresh"),
        Binding("enter", "run_selected", "Run"),
        Binding("e", "start_engine", "Start Engine"),
        Binding("s", "stop_engine", "Stop"),
        Binding("v", "view_log", "View Log"),
    ]

    def __init__(self, *, theme_name: str = DEFAULT_THEME, services: TuiServices | None = None) -> None:
        super().__init__()
        bootstrap_tui_app(self, theme_name=theme_name, services=services)

    def compose(self) -> ComposeResult:
        with Horizontal(id="header"):
            with Vertical(id="header-copy"):
                yield Static("Flow Control", id="screen-title")
                yield Static("Monitor and operate one workspace daemon from the terminal.", id="screen-subtitle")
                yield Static("Workspace runtime is idle.", id="status-line")
                yield Static("", id="control-status")
            with Horizontal(id="header-actions"):
                with Horizontal(id="header-controls"):
                    yield Button("Start Engine", id="start-engine")
                    yield Button("Stop", id="stop-engine")
                    yield Button("Refresh", id="refresh")
                    yield Select([], prompt="Workspace", allow_blank=True, id="workspace-select")
        with Grid(id="body"):
            with Container(id="flow-list-pane"):
                yield Static("CONFIGURED FLOWS", classes="pane-title")
                yield ListView(id="flow-list")
            with Container(id="detail-pane"):
                yield Static("STEPS", classes="pane-title")
                with Horizontal(classes="pane-toolbar"):
                    yield Button("Run Once", id="run-once")
                    yield Button("View Config", id="view-config")
                yield Static("", id="detail-view")
            with Container(id="log-pane"):
                yield Static("LOGS", classes="pane-title")
                with Horizontal(classes="pane-toolbar"):
                    yield Button("View Log", id="view-log")
                    yield Button("Reset Flow", id="clear-flow-log")
                yield ListView(id="log-run-list")
        yield Footer()

    def on_mount(self) -> None:
        logger = logging.getLogger("data_engine")
        logger.setLevel(logging.INFO)
        logger.addHandler(self.log_handler)
        self._register_client_session()
        self._reload_workspace_options()
        self._load_flows()
        if self._has_authored_workspace():
            self._ensure_daemon_started()
        self._sync_daemon_state()
        if not getattr(self, "_daemon_wait_started", False):
            self._daemon_wait_started = True
            threading.Thread(target=self._daemon_wait_worker, daemon=True).start()
        self.set_interval(0.15, self._poll_ui)
        self._refresh_buttons()

    def on_unmount(self) -> None:
        logging.getLogger("data_engine").removeHandler(self.log_handler)
        daemon_wait_stop_event = getattr(self, "_daemon_wait_stop_event", None)
        if daemon_wait_stop_event is not None:
            daemon_wait_stop_event.set()
        if self._unregister_client_session_and_check_for_shutdown():
            self._shutdown_daemon_on_close()
        self.runtime_binding_service.close_binding(self.runtime_binding)

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, FlowListItem):
            self.selected_flow_name = event.item.card.name
            self._render_selected_flow()
        elif isinstance(event.item, RunGroupListItem):
            self.selected_run_key = event.item.run_group.key
            self._show_run_group_modal(event.item.run_group)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == "refresh":
            self.action_refresh_flows()
        elif button_id == "run-once":
            self.action_run_selected()
        elif button_id == "start-engine":
            self.action_start_engine()
        elif button_id == "stop-engine":
            self.action_stop_engine()
        elif button_id == "view-config":
            self.action_view_config()
        elif button_id == "view-log":
            self.action_view_log()
        elif button_id == "clear-flow-log":
            self.action_clear_flow_log()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id != "workspace-select":
            return
        if self._workspace_switch_suppressed or not self.is_mounted:
            return
        if event.value in {Select.NULL, Select.BLANK}:
            return
        workspace_id = str(event.value or "").strip()
        if not workspace_id or workspace_id == self.workspace_paths.workspace_id:
            return
        self._switch_workspace(workspace_id)

    def action_refresh_flows(self) -> None:
        self.flow_controller.action_refresh_flows(self)

    def action_run_selected(self) -> None:
        self.flow_controller.action_run_selected(self)

    def action_start_engine(self) -> None:
        self.flow_controller.action_start_engine(self)

    def action_stop_engine(self) -> None:
        self.flow_controller.action_stop_engine(self)

    def action_view_config(self) -> None:
        self.flow_controller.action_view_config(self)

    def action_clear_flow_log(self) -> None:
        self.flow_controller.action_clear_flow_log(self)

    def action_view_log(self) -> None:
        self.flow_controller.action_view_log(self)

    def _load_flows(self) -> None:
        self.flow_controller.load_flows(self)

    def _reload_workspace_options(self) -> None:
        self.flow_controller.reload_workspace_options(self)

    def _switch_workspace(self, workspace_id: str) -> None:
        self.flow_controller.switch_workspace(self, workspace_id)

    def _render_selected_flow(self) -> None:
        self.flow_controller.render_selected_flow(self)

    def _selected_run_group(self) -> FlowRunState | None:
        return self.flow_controller.selected_run_group(self)

    def _show_run_group_modal(self, run_group: FlowRunState) -> None:
        detail = RunDetailState.from_run(run_group)
        lines = render_run_group_lines(run_group)
        self.push_screen(InfoModal(title=f"Run Details · {detail.source_label}", body="\n".join(lines)))

    def _poll_ui(self) -> None:
        while True:
            try:
                entry = self.log_queue.get_nowait()
            except Empty:
                break
            del entry
        self._sync_daemon_state()

    def _refresh_flow_list_items(self) -> None:
        self.runtime_controller.refresh_flow_list_items(self)

    def _refresh_buttons(self) -> None:
        self.runtime_controller.refresh_buttons(self)

    def _set_status(self, message: str) -> None:
        self.query_one("#status-line", Static).update(message)

    def _sync_daemon_state(self) -> None:
        self.runtime_controller.sync_daemon_state(self)

    def _ensure_daemon_started(self) -> bool:
        return self.runtime_controller.ensure_daemon_started(self)

    def _start_daemon_worker(self) -> None:
        self.runtime_controller.start_daemon_worker(self)

    def _daemon_wait_worker(self) -> None:
        self.runtime_controller.daemon_wait_worker(self)

    def _finish_daemon_startup(self, success: bool, error_text: str) -> None:
        self.runtime_controller.finish_daemon_startup(self, success, error_text)

    def _rebuild_runtime_snapshot(self) -> None:
        self.runtime_controller.rebuild_runtime_snapshot(self)


def main() -> None:
    """Launch the Textual terminal UI."""
    try:
        app = DataEngineTui()
        app.run()
    except ModuleNotFoundError as exc:  # pragma: no cover - import-time dependency guard
        raise SystemExit(
            "The terminal UI requires the 'textual' package. Reinstall Data Engine after updating dependencies."
        ) from exc
    except FlowValidationError as exc:
        raise SystemExit(str(exc)) from exc


__all__ = ["DataEngineTui", "main"]
__all__ += ["FlowListItem", "GroupHeaderListItem", "RunGroupListItem", "InfoModal", "QueueLogHandler"]
