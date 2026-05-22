"""Textual-based terminal UI for Data Engine."""

from __future__ import annotations

import logging
from pathlib import Path
from queue import Empty
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Grid, Horizontal, Vertical
from textual.widgets import Button, DataTable, Footer, Input, ListView, Select, Static

from data_engine.domain import (
    FlowRunState,
    RunDetailState,
)
from data_engine.core.model import FlowValidationError
from data_engine.services.debug_artifacts import clear_debug_artifacts, list_debug_artifacts
from data_engine.views.text import render_run_group_lines
from data_engine.ui.tui.bootstrap import TuiServices
from data_engine.ui.tui.app_binding import bootstrap_tui_app
from data_engine.ui.tui.theme import DEFAULT_THEME, stylesheet as tui_stylesheet
from data_engine.ui.tui.state_support import TuiStateMixin
from data_engine.ui.tui.support import TuiWindowSupportMixin
from data_engine.ui.tui.runtime import QueueLogHandler
from data_engine.ui.tui.widgets import FlowListItem, GroupHeaderListItem, InfoModal, RunGroupListItem, TextListItem


class DataEngineTui(TuiWindowSupportMixin, TuiStateMixin, App[None]):
    """Full-screen terminal UI for headless Data Engine operation."""

    CSS = tui_stylesheet(DEFAULT_THEME)
    _ACTIVE_FLOW_STATES = {"running", "polling", "scheduled", "stopping flow", "stopping runtime"}
    _UI_POLL_INTERVAL_SECONDS = 0.15
    _DAEMON_HEARTBEAT_INTERVAL_SECONDS = 2.0

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh_flows", "Refresh"),
        Binding("enter", "run_selected", "Run"),
        Binding("e", "start_engine", "Start Engine"),
        Binding("s", "stop_engine", "Stop"),
        Binding("v", "view_log", "View Log"),
        Binding("1", "show_home", "Home"),
        Binding("2", "show_dataframes", "Dataframes"),
        Binding("3", "show_debug", "Debug"),
        Binding("4", "show_docs", "Docs"),
        Binding("5", "show_settings", "Settings"),
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
        with Horizontal(id="view-nav"):
            yield Button("Home", id="nav-home", classes="nav-button")
            yield Button("Dataframes", id="nav-dataframes", classes="nav-button")
            yield Button("Debug", id="nav-debug", classes="nav-button")
            yield Button("Docs", id="nav-docs", classes="nav-button")
            yield Button("Settings", id="nav-settings", classes="nav-button")
        with Container(id="views"):
            with Grid(id="body", classes="surface-view"):
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
            with Container(id="dataframes-view", classes="surface-view"):
                yield Static("DATAFRAMES", classes="pane-title")
                with Horizontal(classes="pane-toolbar"):
                    yield Input(placeholder="Parquet file or folder path", id="dataframe-path-input")
                    yield Input(value="200", placeholder="Top N", id="dataframe-limit-input")
                    yield Button("Connect", id="dataframe-connect")
                    yield Button("Clear", id="dataframe-clear")
                yield Static("Choose a parquet file or folder.", id="dataframe-status")
                yield DataTable(id="dataframe-table")
            with Grid(id="debug-view", classes="surface-view two-pane-view"):
                with Container(id="debug-list-pane"):
                    yield Static("SAVED ARTIFACTS", classes="pane-title")
                    with Horizontal(classes="pane-toolbar"):
                        yield Button("Refresh", id="debug-refresh")
                        yield Button("Clear", id="debug-clear")
                    yield ListView(id="debug-artifact-list")
                with Container(id="debug-preview-pane"):
                    yield Static("PREVIEW", classes="pane-title")
                    yield Static("Choose a saved artifact.", id="debug-status")
                    yield DataTable(id="debug-table")
            with Grid(id="docs-view", classes="surface-view two-pane-view"):
                with Container(id="docs-list-pane"):
                    yield Static("DOCUMENTATION", classes="pane-title")
                    yield ListView(id="docs-page-list")
                with Container(id="docs-preview-pane"):
                    yield Static("PREVIEW", classes="pane-title")
                    yield Static("Choose a document.", id="docs-status")
                    yield Static("", id="docs-preview")
            with Container(id="settings-view", classes="surface-view"):
                yield Static("SETTINGS", classes="pane-title")
                yield Static("", id="settings-summary")
                with Horizontal(classes="pane-toolbar"):
                    yield Button("Force Stop Daemon", id="settings-force-stop")
                    yield Button("Reset Workspace", id="settings-reset-workspace")
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
        self._ensure_daemon_wait_worker()
        self._show_view("home")
        self._refresh_docs_view()
        self._refresh_debug_view()
        self._refresh_settings_view()
        self.set_interval(self._UI_POLL_INTERVAL_SECONDS, self._poll_ui)
        self.set_interval(self._DAEMON_HEARTBEAT_INTERVAL_SECONDS, self._heartbeat_daemon_state)
        self._refresh_buttons()

    def on_unmount(self) -> None:
        logging.getLogger("data_engine").removeHandler(self.log_handler)
        daemon_subscription = getattr(self, "daemon_subscription", None)
        if daemon_subscription is not None:
            daemon_subscription.stop()
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
        elif isinstance(event.item, TextListItem):
            if event.item.value is not None and str(event.item.value).endswith((".md", ".rst")):
                self._show_docs_page(Path(str(event.item.value)))
            else:
                self._show_debug_artifact(event.item.value)

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
        elif button_id in {"nav-home", "nav-dataframes", "nav-debug", "nav-docs", "nav-settings"}:
            self._show_view(button_id.removeprefix("nav-"))
        elif button_id == "dataframe-connect":
            self._connect_dataframe_source()
        elif button_id == "dataframe-clear":
            self._clear_table("#dataframe-table")
            self.query_one("#dataframe-status", Static).update("Choose a parquet file or folder.")
        elif button_id == "debug-refresh":
            self._refresh_debug_view()
        elif button_id == "debug-clear":
            self._clear_debug_artifacts()
        elif button_id == "settings-force-stop":
            self._force_stop_daemon_from_settings()
        elif button_id == "settings-reset-workspace":
            self._reset_workspace_from_settings()

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

    def action_show_home(self) -> None:
        self._show_view("home")

    def action_show_dataframes(self) -> None:
        self._show_view("dataframes")

    def action_show_debug(self) -> None:
        self._show_view("debug")

    def action_show_docs(self) -> None:
        self._show_view("docs")

    def action_show_settings(self) -> None:
        self._show_view("settings")

    def _load_flows(self) -> None:
        self.flow_controller.load_flows(self)

    def _reload_workspace_options(self) -> None:
        self.flow_controller.reload_workspace_options(self)

    def _switch_workspace(self, workspace_id: str) -> None:
        self.flow_controller.switch_workspace(self, workspace_id)
        self._refresh_debug_view()
        self._refresh_settings_view()

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

    def _heartbeat_daemon_state(self) -> None:
        self._ensure_daemon_wait_worker()
        if not self._should_run_daemon_heartbeat():
            return
        self._sync_daemon_state()

    def _refresh_flow_list_items(self) -> None:
        self.runtime_controller.refresh_flow_list_items(self)

    def _refresh_buttons(self) -> None:
        self.runtime_controller.refresh_buttons(self)

    def _set_status(self, message: str) -> None:
        self.query_one("#status-line", Static).update(message)
        try:
            self._refresh_settings_view()
        except Exception:
            pass

    def _sync_daemon_state(self) -> None:
        self.runtime_controller.sync_daemon_state(self)

    def _apply_daemon_update_batch(self) -> None:
        self.runtime_controller.apply_daemon_update_batch(self)

    def _ensure_daemon_started(self) -> bool:
        return self.runtime_controller.ensure_daemon_started(self)

    def _start_daemon_worker(self) -> None:
        self.runtime_controller.start_daemon_worker(self)

    def _finish_daemon_startup(self, success: bool, error_text: str) -> None:
        self.runtime_controller.finish_daemon_startup(self, success, error_text)

    def _rebuild_runtime_snapshot(self) -> None:
        self.runtime_controller.rebuild_runtime_snapshot(self)

    def _show_view(self, name: str) -> None:
        ids = {
            "home": "body",
            "dataframes": "dataframes-view",
            "debug": "debug-view",
            "docs": "docs-view",
            "settings": "settings-view",
        }
        if name not in ids:
            name = "home"
        for view_id in ids.values():
            self.query_one(f"#{view_id}").display = view_id == ids[name]
        self.query_one("#screen-title", Static).update(
            {
                "home": "Flow Control",
                "dataframes": "Dataframes",
                "debug": "Debug",
                "docs": "Documentation",
                "settings": "Settings",
            }[name]
        )
        self.query_one("#screen-subtitle", Static).update(
            {
                "home": "Monitor and operate one workspace daemon from the terminal.",
                "dataframes": "Connect to a parquet file or folder and inspect the first rows.",
                "debug": "Inspect saved debug artifacts for the selected workspace.",
                "docs": "Read packaged guide pages without leaving the terminal.",
                "settings": "Review workspace state and emergency runtime actions.",
            }[name]
        )
        if name == "debug":
            self._refresh_debug_view()
        elif name == "docs":
            self._refresh_docs_view()
        elif name == "settings":
            self._refresh_settings_view()

    def _connect_dataframe_source(self) -> None:
        raw_path = self.query_one("#dataframe-path-input", Input).value.strip()
        if not raw_path:
            self.query_one("#dataframe-status", Static).update("Enter a parquet file or folder path.")
            return
        limit = self._preview_limit("#dataframe-limit-input")
        path = Path(raw_path).expanduser()
        if path.is_dir():
            parquet_files = tuple(sorted(path.rglob("*.parquet"), key=lambda item: str(item).lower()))
            if not parquet_files:
                self.query_one("#dataframe-status", Static).update("No parquet files found.")
                self._clear_table("#dataframe-table")
                return
            source = str(path / "**" / "*.parquet")
            label = f"{len(parquet_files)} parquet files"
        elif path.is_file() and path.suffix.lower() == ".parquet":
            source = str(path)
            label = path.name
        else:
            self.query_one("#dataframe-status", Static).update("Choose a .parquet file or folder.")
            self._clear_table("#dataframe-table")
            return
        self._preview_table_source(source, table_id="#dataframe-table", status_id="#dataframe-status", limit=limit, label=label)

    def _refresh_debug_view(self) -> None:
        try:
            records = list_debug_artifacts(self.workspace_paths.runtime_state_dir)
        except Exception as exc:
            self.query_one("#debug-status", Static).update(f"Could not read debug artifacts: {exc}")
            return
        list_view = self.query_one("#debug-artifact-list", ListView)
        list_view.clear()
        for record in records:
            title = record.flow_name or record.display_name or record.stem
            subtitle_parts = [part for part in (record.step_name, record.kind, record.created_at_utc) if part]
            list_view.append(TextListItem(title, " - ".join(subtitle_parts), value=record))
        if not records:
            self.query_one("#debug-status", Static).update("No saved debug artifacts yet.")
            self._clear_table("#debug-table")

    def _clear_debug_artifacts(self) -> None:
        deleted = clear_debug_artifacts(self.workspace_paths.runtime_state_dir)
        self._refresh_debug_view()
        self.query_one("#debug-status", Static).update(f"Cleared {deleted} debug artifact file(s).")

    def _show_debug_artifact(self, record: object) -> None:
        artifact_path = getattr(record, "artifact_path", None)
        if not isinstance(artifact_path, Path):
            return
        kind = str(getattr(record, "kind", "") or artifact_path.suffix.lstrip("."))
        if artifact_path.suffix.lower() == ".parquet":
            self._preview_table_source(
                str(artifact_path),
                table_id="#debug-table",
                status_id="#debug-status",
                limit=200,
                label=f"{artifact_path.name} - {kind}",
            )
            return
        self._clear_table("#debug-table")
        try:
            text = artifact_path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            text = f"Artifact is not previewable in the TUI yet: {exc}"
        self.query_one("#debug-status", Static).update(text[:4000])

    def _refresh_docs_view(self) -> None:
        list_view = self.query_one("#docs-page-list", ListView)
        list_view.clear()
        docs_root = Path(__file__).parents[2] / "docs" / "sphinx_source"
        paths = tuple(sorted((*docs_root.glob("*.rst"), *docs_root.glob("guides/*.md")), key=lambda item: str(item).lower()))
        for path in paths:
            title = path.stem.replace("-", " ").replace("_", " ").title()
            subtitle = str(path.relative_to(docs_root))
            list_view.append(TextListItem(title, subtitle, value=path))
        self.query_one("#docs-status", Static).update(f"{len(paths)} packaged document(s).")

    def _show_docs_page(self, path: Path) -> None:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            text = f"Could not read {path.name}: {exc}"
        self.query_one("#docs-status", Static).update(path.name)
        self.query_one("#docs-preview", Static).update(text[:6000])

    def _refresh_settings_view(self) -> None:
        workspace_snapshot = getattr(self, "workspace_snapshot", None)
        engine_state = "unknown" if workspace_snapshot is None else workspace_snapshot.engine.state
        control_text = (
            "workspace unavailable"
            if workspace_snapshot is None
            else workspace_snapshot.control.control_status_text
        )
        module_count = self._workspace_module_count()
        summary = "\n".join(
            (
                f"Workspace: {self.workspace_paths.workspace_id}",
                f"Workspace root: {self.workspace_paths.workspace_root}",
                f"Collection root: {self.workspace_paths.workspace_collection_root}",
                f"Flow modules: {module_count}",
                f"Discovered workspaces: {len(self.workspace_session_state.discovered_workspace_ids)}",
                f"Engine: {engine_state}",
                f"Control: {control_text}",
                f"Runtime state: {self.workspace_paths.runtime_state_dir}",
            )
        )
        self.query_one("#settings-summary", Static).update(summary)

    def _force_stop_daemon_from_settings(self) -> None:
        result = self.command_service.force_shutdown_daemon(self.workspace_paths, timeout=0.5)
        if result.error_text:
            self._set_status(f"Force stop failed: {result.error_text}")
            return
        self._set_status("Local daemon force-stopped.")
        self._sync_daemon_state()

    def _reset_workspace_from_settings(self) -> None:
        result = self.command_service.reset_workspace(
            paths=self.workspace_paths,
            runtime_cache_ledger=self.runtime_binding.runtime_cache_ledger,
            runtime_control_ledger=self.runtime_binding.runtime_control_ledger,
        )
        if result.error_text:
            self._set_status(f"Workspace reset failed: {result.error_text}")
            return
        self._set_status("Workspace runtime state reset.")
        self._sync_daemon_state()

    def _workspace_module_count(self) -> int:
        flow_modules_dir = self.workspace_paths.flow_modules_dir
        if not flow_modules_dir.is_dir():
            return 0
        return sum(
            1
            for path in flow_modules_dir.iterdir()
            if path.is_file() and path.suffix in {".py", ".ipynb"} and path.name != "__init__.py"
        )

    def _preview_limit(self, input_id: str) -> int:
        raw_value = self.query_one(input_id, Input).value.strip()
        try:
            return max(1, min(int(raw_value), 500_000))
        except ValueError:
            return 200

    def _preview_table_source(self, source: str, *, table_id: str, status_id: str, limit: int, label: str) -> None:
        try:
            import polars as pl

            frame = pl.scan_parquet(source).head(limit).collect()
        except Exception as exc:
            self._clear_table(table_id)
            self.query_one(status_id, Static).update(f"Preview failed: {exc}")
            return
        table = self.query_one(table_id, DataTable)
        table.clear(columns=True)
        columns = tuple(str(column) for column in frame.columns)
        if columns:
            table.add_columns(*columns)
            table.add_rows([tuple("" if value is None else str(value) for value in row) for row in frame.rows()])
        self.query_one(status_id, Static).update(
            f"{label} - {frame.height} rows - {frame.width} columns - showing first {limit} rows"
        )

    def _clear_table(self, table_id: str) -> None:
        self.query_one(table_id, DataTable).clear(columns=True)


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
