from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
import logging
import os
from pathlib import Path
import threading

import pytest
import polars as pl
from PySide6.QtCore import Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QLabel, QPushButton, QTableWidget, QTextEdit
from shiboken6 import delete as shiboken_delete
from shiboken6 import isValid as shiboken_is_valid

from data_engine.core.model import FlowValidationError
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, WorkspaceDaemonSnapshot
from data_engine.domain import FlowCatalogEntry, FlowRunState, RuntimeSessionState, WorkspaceControlState
from data_engine.platform.identity import APP_DISPLAY_NAME
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.workspace_models import DiscoveredWorkspace, machine_id_text
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.runtime_db import RuntimeLedger, utcnow_text
from data_engine.services import DaemonService
from data_engine.services.workspace_provisioning import WorkspaceProvisioningResult
from data_engine.ui.gui.bootstrap import build_gui_services
from data_engine.views import flow_category
from data_engine.ui.gui.icons import ICON_ASSETS, load_svg_icon_text
from data_engine.ui.gui.app import DataEngineWindow
from data_engine.ui.gui.rendering import classify_artifact_preview, theme_svg_paths
from data_engine.domain import FlowLogEntry
from data_engine.domain import RuntimeStepEvent, parse_runtime_event
from data_engine.views.models import QtFlowCard
from data_engine.views.logs import FlowLogStore
from data_engine.ui.gui.presenters.logs import next_log_scroll_value
from data_engine.ui.gui.theme import stylesheet, theme_button_text, toggle_theme_name


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


@pytest.fixture
def qapp():
    existing = QApplication.instance()
    app = existing if existing is not None and shiboken_is_valid(existing) else QApplication([])
    yield app
    for widget in list(app.topLevelWidgets()):
        if not shiboken_is_valid(widget):
            continue
        if hasattr(widget, "ui_closing"):
            widget.ui_closing = True
        if hasattr(widget, "_auto_daemon_enabled"):
            widget._auto_daemon_enabled = False
        if hasattr(widget, "_shutdown_daemon_on_close"):
            widget._shutdown_daemon_on_close = lambda: None
        if hasattr(widget, "_unregister_client_session_and_check_for_shutdown"):
            widget._unregister_client_session_and_check_for_shutdown = lambda **kwargs: False
        if hasattr(widget, "_wait_for_worker_threads"):
            widget._wait_for_worker_threads = lambda *, timeout_seconds: None
        for timer_name in ("log_timer", "ui_refresh_timer", "operation_timer", "daemon_timer"):
            timer = getattr(widget, timer_name, None)
            if timer is not None:
                timer.stop()
        runtime_stop_event = getattr(widget, "engine_runtime_stop_event", None)
        if runtime_stop_event is not None:
            runtime_stop_event.set()
        flow_stop_event = getattr(widget, "engine_flow_stop_event", None)
        if flow_stop_event is not None:
            flow_stop_event.set()
        if shiboken_is_valid(widget):
            shiboken_delete(widget)
    app.processEvents()


@pytest.fixture(autouse=True)
def stub_workspace_root(monkeypatch, tmp_path):
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ROOT", raising=False)
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", raising=False)
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ID", raising=False)
    app_root = tmp_path / "data_engine"
    workspace_collection_root = tmp_path / "workspaces"
    workspace_root = workspace_collection_root / "example_workspace" / "flow_modules"
    workspace_root.mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_workspace_collection_root(workspace_collection_root)


def _sample_qt_flow_cards() -> tuple[QtFlowCard, ...]:
    return (
        QtFlowCard(
            name="poller",
            group="Imports",
            title="Claims Poller",
            description="Polls for new claim workbooks.",
            source_root="/tmp/input",
            target_root="/tmp/output",
            mode="poll",
            interval="30s",
            operations="Read Excel -> Write Parquet",
            operation_items=("Read Excel", "Write Parquet"),
            state="poll ready",
            valid=True,
            category="automated",
        ),
        QtFlowCard(
            name="manual_review",
            group="Manual",
            title="Manual Review",
            description="Runs a one-off validation pass.",
            source_root="(not set)",
            target_root="/tmp/manual-output",
            mode="manual",
            interval="-",
            operations="Build Report",
            operation_items=("Build Report",),
            state="manual",
            valid=True,
            category="manual",
        ),
    )


def _sample_multi_active_qt_flow_cards() -> tuple[QtFlowCard, ...]:
    return (
        QtFlowCard(
            name="poller_a",
            group="Imports",
            title="Claims Poller A",
            description="Polls for new claim workbooks.",
            source_root="/tmp/input-a",
            target_root="/tmp/output-a",
            mode="poll",
            interval="30s",
            operations="Read Excel -> Write Parquet",
            operation_items=("Read Excel", "Write Parquet"),
            state="poll ready",
            valid=True,
            category="automated",
        ),
        QtFlowCard(
            name="poller_b",
            group="Imports",
            title="Claims Poller B",
            description="Polls for new claim workbooks.",
            source_root="/tmp/input-b",
            target_root="/tmp/output-b",
            mode="schedule",
            interval="30s",
            operations="Read Excel -> Write Parquet",
            operation_items=("Read Excel", "Write Parquet"),
            state="schedule ready",
            valid=True,
            category="automated",
        ),
    )


def _append_persisted_run_log(workspace_root, *, run_id: str, flow_name: str, source_path: str, status: str, elapsed: float | None = None) -> None:
    ledger = RuntimeLedger.open_default(data_root=workspace_root)
    try:
        message = f"run={run_id} flow={flow_name} source={source_path} status={status}"
        if elapsed is not None:
            message += f" elapsed={elapsed}"
        ledger.append_log(
            level="INFO",
            message=message,
            created_at_utc=utcnow_text(),
            run_id=run_id,
            flow_name=flow_name,
        )
    finally:
        ledger.close()


class _FakeDaemonManager:
    def __init__(self, snapshot: WorkspaceDaemonSnapshot | None = None) -> None:
        self.snapshot = snapshot or WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="none",
        )
        self._last_snapshot: WorkspaceDaemonSnapshot | None = None
        self._sync_misses = 0
        self._daemon_live = False
        self.request_control_message = "Control request sent."

    def sync(self) -> WorkspaceDaemonSnapshot:
        self._daemon_live = self.snapshot.live
        if not self.snapshot.live:
            self._sync_misses += 1
            if self._sync_misses < 3 and self._last_snapshot is not None:
                return WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=self._last_snapshot.workspace_owned,
                    leased_by_machine_id=self._last_snapshot.leased_by_machine_id,
                    runtime_active=self._last_snapshot.runtime_active,
                    runtime_stopping=self._last_snapshot.runtime_stopping,
                    manual_runs=self._last_snapshot.manual_runs,
                    last_checkpoint_at_utc=self._last_snapshot.last_checkpoint_at_utc,
                    source="cached",
                )
        else:
            self._sync_misses = 0
        self._last_snapshot = self.snapshot
        return self.snapshot

    def request_control(self) -> str:
        return self.request_control_message


class _FakeDaemonStateService:
    def __init__(self, manager: _FakeDaemonManager | None = None) -> None:
        self.manager = manager or _FakeDaemonManager()

    def create_manager(self, paths):
        del paths
        return self.manager

    def sync(self, manager):
        return manager.sync()

    def control_state(self, manager, snapshot, *, daemon_startup_in_progress: bool = False):
        del manager
        return WorkspaceControlState.from_snapshot(
            snapshot,
            daemon_live=snapshot.live,
            local_machine_id="test-host",
            control_request=None,
            daemon_startup_in_progress=daemon_startup_in_progress,
        )

    def request_control(self, manager):
        return manager.request_control()


class _FakeLedgerService:
    def __init__(self, *, remaining_counts: list[int] | None = None) -> None:
        self.remaining_counts = list(remaining_counts or [])
        self.removed_client_ids: list[str] = []
        self.purged_sessions: list[dict[str, object]] = []
        self.closed_ledgers: list[object] = []

    def open_for_workspace(self, workspace_root):
        return RuntimeLedger.open_default(data_root=workspace_root)

    def register_client_session(self, ledger, *, client_id: str, workspace_id: str, client_kind: str, pid: int) -> None:
        del ledger, client_id, workspace_id, client_kind, pid

    def remove_client_session(self, ledger, client_id: str) -> None:
        del ledger
        self.removed_client_ids.append(client_id)

    def purge_process_client_sessions(self, ledger, *, workspace_id: str, client_kind: str, pid: int) -> None:
        del ledger
        self.purged_sessions.append({"workspace_id": workspace_id, "client_kind": client_kind, "pid": pid})

    def count_live_client_sessions(self, ledger, workspace_id: str, *, exclude_client_id: str | None = None) -> int:
        del ledger, exclude_client_id
        if self.remaining_counts:
            return self.remaining_counts.pop(0)
        del workspace_id
        return 0

    def close(self, ledger) -> None:
        self.closed_ledgers.append(ledger)


class _FakeFlowCatalogService:
    def __init__(self, cards: tuple[QtFlowCard, ...] | None = None) -> None:
        self.cards = cards or _sample_qt_flow_cards()

    def load_entries(self, *, workspace_root=None):
        del workspace_root
        return tuple(FlowCatalogEntry(**card.__dict__) for card in self.cards)


class _RaisingFlowCatalogService:
    def __init__(self, message: str) -> None:
        self.message = message

    def load_entries(self, *, workspace_root=None):
        del workspace_root
        raise FlowValidationError(self.message)


class _MessageCapture:
    def __init__(self) -> None:
        self.shown_messages: list[tuple[str, str, str]] = []
        self.shown_later_messages: list[tuple[str, str, str]] = []

    def show_now(self, *, title: str, text: str, tone: str) -> None:
        self.shown_messages.append((title, text, tone))

    def show_later(self, *, title: str, text: str, tone: str) -> None:
        self.shown_later_messages.append((title, text, tone))


def _attach_message_capture(window: DataEngineWindow) -> _MessageCapture:
    capture = _MessageCapture()
    window._show_message_box = capture.show_now
    window._show_message_box_later = capture.show_later
    return capture


def _attach_call_recorder(window: DataEngineWindow, attr_name: str, *, side_effect=None) -> list[tuple[tuple[object, ...], dict[str, object]]]:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _record(*args, **kwargs):
        calls.append((args, kwargs))
        if side_effect is not None:
            return side_effect(*args, **kwargs)
        return None

    setattr(window, attr_name, _record)
    return calls


class _FakeLogService:
    def __init__(self, store=None, *, stores: tuple[object, ...] = ()) -> None:
        from data_engine.views.logs import FlowLogStore

        self.store = store or FlowLogStore()
        self._stores = list(stores)
        self.created_stores: list[object] = []

    def create_store(self, runtime_ledger=None):
        del runtime_ledger
        if self._stores:
            self.store = self._stores.pop(0)
        self.created_stores.append(self.store)
        return self.store

    def reload(self, store) -> None:
        del store

    def append_entry(self, store, entry) -> None:
        store.append_entry(entry)

    def clear_flow(self, store, flow_name):
        store.clear_flow(flow_name)

    def all_entries(self, store):
        return tuple(store._entries)

    def entries_for_flow(self, store, flow_name):
        return store.entries_for_flow(flow_name)

    def runs_for_flow(self, store, flow_name):
        return store.runs_for_flow(flow_name)


class _FakeControlApplication:
    def __init__(self, *, request_control_result=None) -> None:
        self.request_control_result = request_control_result
        self.request_control_calls: list[object] = []
        self.refresh_flows_result = None
        self.refresh_flows_calls: list[dict[str, object]] = []
        self.start_engine_result = None
        self.start_engine_calls: list[dict[str, object]] = []

    def request_control(self, manager):
        self.request_control_calls.append(manager)
        if self.request_control_result is None:
            raise AssertionError("request_control_result was not configured for this fake control application.")
        return self.request_control_result

    def refresh_flows(self, **kwargs):
        self.refresh_flows_calls.append(kwargs)
        if self.refresh_flows_result is None:
            raise AssertionError("refresh_flows_result was not configured for this fake control application.")
        return self.refresh_flows_result

    def start_engine(self, **kwargs):
        self.start_engine_calls.append(kwargs)
        if self.start_engine_result is None:
            raise AssertionError("start_engine_result was not configured for this fake control application.")
        return self.start_engine_result


class _FakeSharedStateService:
    def __init__(self) -> None:
        self.hydrated: list[tuple[object, object]] = []

    def hydrate_local_runtime(self, paths, ledger) -> None:
        self.hydrated.append((paths, ledger))


def _make_window(
    *,
    cards: tuple[QtFlowCard, ...] | None = None,
    snapshot: WorkspaceDaemonSnapshot | None = None,
    request_func=None,
    is_live_func=None,
    force_shutdown_func=None,
    discover_workspaces_func=None,
    resolve_workspace_paths_func=None,
    settings_store: LocalSettingsStore | None = None,
    ledger_service=None,
    log_service=None,
    control_application=None,
    shared_state_service=None,
    workspace_provisioning_service=None,
) -> DataEngineWindow:
    manager = _FakeDaemonManager(snapshot=snapshot)
    log_service = log_service or _FakeLogService()
    services = build_gui_services(
        settings_store=settings_store,
        flow_catalog_service=_FakeFlowCatalogService(cards),
        daemon_service=DaemonService(
            spawn_process_func=lambda paths: 0,
            request_func=request_func or (lambda paths, payload, timeout=0.0: {"ok": True}),
            is_live_func=is_live_func or (lambda paths: False),
            force_shutdown_func=force_shutdown_func or (lambda paths, timeout=0.5: None),
            client_error_type=Exception,
        ),
        daemon_state_service=_FakeDaemonStateService(manager),
        ledger_service=ledger_service,
        log_service=log_service,
        control_application=control_application,
        shared_state_service=shared_state_service,
        workspace_provisioning_service=workspace_provisioning_service,
        discover_workspaces_func=discover_workspaces_func or (lambda **kwargs: ()),
        resolve_workspace_paths_func=resolve_workspace_paths_func or resolve_workspace_paths,
    )
    return DataEngineWindow(services=services)


def _click_flow_row(window: DataEngineWindow, flow_name: str) -> None:
    widget = window.sidebar_flow_widgets[flow_name]
    QTest.mouseClick(widget, Qt.MouseButton.LeftButton)


def _dispose_window(qapp, window: DataEngineWindow) -> None:
    if shiboken_is_valid(window):
        window.close()
    qapp.processEvents()


def _visible_log_run_primary_labels(window: DataEngineWindow) -> list[str]:
    labels: list[str] = []
    for index in range(window.log_view.count()):
        item = window.log_view.item(index)
        widget = window.log_view.itemWidget(item)
        if widget is None:
            continue
        source_label = widget.property("sourceLabel")
        if source_label:
            labels.append(str(source_label))
    return labels


def test_flow_category_matches_mode():
    assert flow_category("manual") == "manual"
    assert flow_category("poll") == "automated"
    assert flow_category("schedule") == "automated"


def test_theme_helpers_cover_light_and_dark():
    assert toggle_theme_name("dark") == "light"
    assert toggle_theme_name("light") == "dark"
    assert theme_button_text("dark") == "Switch to Light"
    assert theme_button_text("light") == "Switch to Dark"

    dark_css = stylesheet("dark")
    light_css = stylesheet("light")

    assert "#0d1117" in dark_css
    assert "#ffffff" in light_css


def test_parse_runtime_event_extracts_step_elapsed():
    record = logging.makeLogRecord(
        {
            "msg": "run=run-123 flow=claims_poll step=Write Parquet source=/tmp/input.xlsx status=success elapsed=0.532100"
        }
    )

    event = parse_runtime_event(record)

    assert event is not None
    assert event.run_id == "run-123"
    assert event.flow_name == "claims_poll"
    assert event.step_name == "Write Parquet"
    assert event.source_label == "input.xlsx"
    assert event.status == "success"
    assert event.elapsed_seconds == 0.5321


def test_theme_svg_paths_applies_fill_to_every_path():
    svg = '<svg><path d="a"/><path fill="#000000" d="b"/></svg>'

    themed = theme_svg_paths(svg, "#ffffff")

    assert themed.count('fill="#ffffff"') == 2
    assert '#000000' not in themed


def test_settings_visibility_panel_reports_workspace_stats(qapp):
    del qapp
    window = _make_window(cards=_sample_multi_active_qt_flow_cards())
    recent_started = datetime.now(UTC).isoformat()
    old_started = (datetime.now(UTC) - timedelta(days=45)).isoformat()

    window.runtime_binding.runtime_ledger.record_run_started(
        run_id="run-recent",
        flow_name="poller_a",
        group_name="Imports",
        source_path="/tmp/input-a.xlsx",
        started_at_utc=recent_started,
    )
    window.runtime_binding.runtime_ledger.record_run_started(
        run_id="run-old",
        flow_name="poller_b",
        group_name="Imports",
        source_path="/tmp/input-b.xlsx",
        started_at_utc=old_started,
    )
    window._refresh_workspace_visibility_panel()

    assert window.workspace_counts_footer_label.text() == "0 modules - 1 groups - 2 flows - 1 runs last 30 days"
    assert window.visibility_interpreter_mode_value.text() == "Virtual Environment"


def test_provision_workspace_button_creates_missing_workspace_assets(qapp, tmp_path, monkeypatch):
    del qapp
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(tmp_path / "data_engine"))

    class _RecordingProvisioningService:
        def __init__(self) -> None:
            self.requested_paths = None

        def provision_workspace(self, workspace_paths, *, interpreter_path=None):
            del interpreter_path
            self.requested_paths = workspace_paths
            (workspace_paths.workspace_root / "flow_modules").mkdir(parents=True, exist_ok=True)
            return WorkspaceProvisioningResult(
                workspace_root=workspace_paths.workspace_root,
                created_paths=(workspace_paths.workspace_root, workspace_paths.flow_modules_dir),
                preserved_paths=(),
            )

    provisioning_service = _RecordingProvisioningService()
    window = _make_window(
        workspace_provisioning_service=provisioning_service,
    )
    selected_paths = window.workspace_paths

    window._provision_selected_workspace()

    assert provisioning_service.requested_paths is not None
    assert provisioning_service.requested_paths.workspace_root == selected_paths.workspace_root
    assert (selected_paths.workspace_root / "flow_modules").is_dir()
    assert selected_paths.workspace_id in window.workspace_target_label.text()
    assert f"Provisioned {selected_paths.workspace_root.name}" in window.workspace_provision_status_label.text()


def test_icon_registry_loads_current_file_backed_svg():
    assert ICON_ASSETS["dark_light"].file_name == "dark_light.svg"

    svg_text = load_svg_icon_text("dark_light")

    assert "<svg" in svg_text
    assert "viewBox=" in svg_text


def test_artifact_preview_classification_is_explicit(tmp_path):
    assert classify_artifact_preview(tmp_path / "output.parquet").kind == "parquet"
    assert classify_artifact_preview(tmp_path / "workbook.xlsx").kind == "excel"
    assert classify_artifact_preview(tmp_path / "notes.txt").kind == "text"
    assert classify_artifact_preview(tmp_path / "packet.pdf").kind == "pdf"
    assert classify_artifact_preview(tmp_path / "blob.bin").kind == "unsupported"


def test_structured_error_content_parses_step_failure(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        parsed = window._structured_error_content(
            'Flow "claims_summary" failed in step "Combine Claims" (function combine_claims) '
            'for source "/tmp/input.xlsx": ValueError: boom'
        )

        assert parsed is not None
        assert parsed.title == "Flow Failed"
        assert tuple((field.label, field.value) for field in parsed.fields) == (
            ("Flow", "claims_summary"),
            ("Phase", "step"),
            ("Step", "Combine Claims"),
            ("Function", "combine_claims"),
            ("Source", "/tmp/input.xlsx"),
        )
        assert parsed.detail == "ValueError: boom"
    finally:
        _dispose_window(qapp, window)


def test_structured_error_content_parses_build_failure(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        parsed = window._structured_error_content(
            'Flow module "claims_summary" failed during build() in build: RuntimeError: build boom'
        )

        assert parsed is not None
        assert parsed.title == "Flow Module Failed"
        assert tuple((field.label, field.value) for field in parsed.fields) == (
            ("Flow Module", "claims_summary"),
            ("Phase", "build"),
            ("Function", "build"),
        )
        assert parsed.detail == "RuntimeError: build boom"
    finally:
        _dispose_window(qapp, window)


def test_structured_error_content_parses_missing_flow_module_error(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        parsed = window._structured_error_content(
            "Flow module 'broken_step' is not available in /tmp/workspace/flow_modules. "
            "Available flow modules: claims_demo, manual_claims_demo."
        )

        assert parsed is not None
        assert parsed.title == "Flow Module Not Found"
        assert tuple((field.label, field.value) for field in parsed.fields) == (
            ("Flow Module", "broken_step"),
            ("Workspace", "/tmp/workspace/flow_modules"),
            ("Available", "claims_demo, manual_claims_demo"),
        )
        assert "broken_step" in parsed.detail
    finally:
        _dispose_window(qapp, window)


def test_rehydrate_step_outputs_from_ledger_enables_inspect_button(qapp, monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (tmp_path / "workspaces" / "example_workspace" / "flow_modules").mkdir(parents=True, exist_ok=True)
    output_path = tmp_path / "output.parquet"
    output_path.write_text("ok", encoding="utf-8")

    window = _make_window()
    try:
        started_at = utcnow_text()
        window.runtime_ledger.record_run_started(
            run_id="run-1",
            flow_name="poller",
            group_name="Imports",
            source_path="/tmp/input.xlsx",
            started_at_utc=started_at,
        )
        step_id = window.runtime_ledger.record_step_started(
            run_id="run-1",
            flow_name="poller",
            step_label="Write Parquet",
            started_at_utc=started_at,
        )
        window.runtime_ledger.record_step_finished(
            step_run_id=step_id,
            status="success",
            finished_at_utc=started_at,
            elapsed_ms=5,
            output_path=str(output_path),
        )
        window.runtime_ledger.record_run_finished(run_id="run-1", status="success", finished_at_utc=started_at)

        window._rebuild_runtime_snapshot()
        window._select_flow("poller")

        inspect_buttons = [button for button in window.findChildren(QPushButton) if button.objectName() == "inspectOutputButton"]
        assert any(button.isEnabled() for button in inspect_buttons)
    finally:
        _dispose_window(qapp, window)


def test_show_output_preview_renders_excel_as_table(qapp, monkeypatch, tmp_path):
    workbook_path = tmp_path / "preview.xlsx"
    workbook_path.write_bytes(b"placeholder")
    monkeypatch.setattr(
        "data_engine.ui.gui.app.pl.read_excel",
        lambda path, sheet_id=1, engine="calamine": pl.DataFrame({"member_id": ["A1", "A2"], "amount": [10, 20]}),
    )

    window = _make_window()
    try:
        window._show_output_preview("Write Workbook", workbook_path)

        assert window.output_preview_dialog is not None
        meta_label = next(
            label
            for label in window.output_preview_dialog.findChildren(QLabel)
            if label.objectName() == "sectionMeta"
        )
        table = window.output_preview_dialog.findChild(QTableWidget, "outputPreviewTable")

        assert "Excel table preview" in meta_label.text()
        assert table is not None
        assert table.rowCount() == 2
        assert table.columnCount() == 2
    finally:
        if window.output_preview_dialog is not None:
            window.output_preview_dialog.close()
        _dispose_window(qapp, window)


def test_show_output_preview_pdf_uses_placeholder_message(qapp, monkeypatch, tmp_path):
    pdf_path = tmp_path / "preview.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")

    window = _make_window()
    try:
        window._show_output_preview("Download PDF", pdf_path)

        assert window.output_preview_dialog is not None
        meta_label = next(
            label
            for label in window.output_preview_dialog.findChildren(QLabel)
            if label.objectName() == "sectionMeta"
        )
        body = window.output_preview_dialog.findChild(QTextEdit, "outputPreviewText")

        assert "PDF inspection" in meta_label.text()
        assert body is not None
        assert "not available yet" in body.toPlainText()
    finally:
        if window.output_preview_dialog is not None:
            window.output_preview_dialog.close()
        _dispose_window(qapp, window)


def test_format_seconds_truncates_and_changes_units(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        assert window._format_seconds(0.0005) == "<1ms"
        assert window._format_seconds(0.04899) == "48ms"
        assert window._format_seconds(1.239) == "1.2s"
        assert window._format_seconds(61.239) == "1.0m"
        assert window._format_seconds(3665.9) == "1.0h"
    finally:
        _dispose_window(qapp, window)


def test_rebuild_runtime_snapshot_preserves_running_step_elapsed_time(qapp, monkeypatch):
    window = _make_window()
    try:
        monkeypatch.setattr(window, "_monotonic", lambda: 100.0)
        window.log_store._entries = [
            FlowLogEntry(
                line="run=run-123 flow=poller step=Read Excel source=/tmp/input.xlsx status=started",
                kind="flow",
                flow_name="poller",
                created_at_utc=datetime.now(UTC) - timedelta(seconds=1.23),
                event=RuntimeStepEvent(
                    run_id="run-123",
                    flow_name="poller",
                    step_name="Read Excel",
                    source_label="input.xlsx",
                    status="started",
                    elapsed_seconds=None,
                ),
            )
        ]

        window._rebuild_runtime_snapshot()

        assert window._duration_text("poller", "Read Excel") == "1.2s"
    finally:
        _dispose_window(qapp, window)


def test_data_engine_window_instantiates_and_loads_flow_cards(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        assert window.view_stack.count() == 3
        assert window.selected_flow_name == "poller"
        assert set(window.sidebar_flow_widgets) == {"poller", "manual_review"}
        poller_widget = window.sidebar_flow_widgets["poller"]
        primary_label = next(label for label in poller_widget.findChildren(QLabel) if label.objectName() == "sidebarFlowCode")
        assert primary_label.text() == "poller"

        _click_flow_row(window, "manual_review")
        assert window.selected_flow_name == "manual_review"
    finally:
        _dispose_window(qapp, window)


def test_data_engine_window_nav_buttons_switch_views(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        assert window.view_stack.currentIndex() == 0

        window.docs_button.click()
        assert window.view_stack.currentIndex() == 1

        window.settings_button.click()
        assert window.view_stack.currentIndex() == 2

        window.home_button.click()
        assert window.view_stack.currentIndex() == 0
    finally:
        _dispose_window(qapp, window)


def test_refresh_button_reloads_flows(qapp, monkeypatch, tmp_path):
    workspace_root = tmp_path / "workspaces" / "claims"
    (workspace_root / "flow_modules").mkdir(parents=True)
    del monkeypatch
    control_application = _FakeControlApplication()
    control_application.refresh_flows_result = type(
        "Result",
        (),
        {
            "reload_catalog": True,
            "sync_after": True,
            "status_text": "Reloaded flow definitions.",
            "warning_text": None,
            "error_text": None,
        },
    )()
    window = _make_window(
        control_application=control_application,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: resolve_workspace_paths(
            workspace_root=workspace_root,
            workspace_id=workspace_id or "claims",
        ),
    )
    sync_calls = _attach_call_recorder(window, "_sync_from_daemon")
    try:
        window._load_flows()
        assert "poller" in window.flow_cards

        window._refresh_flows_requested()
        window._flush_deferred_ui_updates()

        assert len(control_application.refresh_flows_calls) == 1
        assert len(sync_calls) == 1
        assert any(entry.line == "Reloaded flow definitions." for entry in window.log_store._entries)
    finally:
        _dispose_window(qapp, window)


def test_refresh_button_still_reloads_locally_when_daemon_refresh_fails(qapp, monkeypatch):
    window = _make_window(
        request_func=lambda paths, payload, timeout=5.0: (_ for _ in ()).throw(Exception("unreachable")),
    )
    capture = _attach_message_capture(window)
    sync_calls = _attach_call_recorder(window, "_sync_from_daemon")
    try:
        window.refresh_button.click()
        window._flush_deferred_ui_updates()

        assert len(sync_calls) == 1
        assert any(entry.line == "Reloaded flow definitions." for entry in window.log_store._entries)
        assert len(capture.shown_later_messages) == 1
        assert "Refreshed local flow definitions" in capture.shown_later_messages[0][1]
    finally:
        _dispose_window(qapp, window)


def test_refresh_button_clears_flows_without_spawning_daemon_when_workspace_has_no_flow_modules(qapp, monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    workspace_collection_root = tmp_path / "ghost_workspaces"
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_workspace_collection_root(workspace_collection_root)
    store.set_default_workspace_id("example_workspace")
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(workspace_collection_root))
    workspace_root = workspace_collection_root / "example_workspace"
    workspace_root.mkdir(parents=True, exist_ok=True)

    spawn_calls: list[bool] = []
    daemon_calls: list[dict[str, object]] = []
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    window = _make_window(
        request_func=lambda paths, payload, timeout=5.0: daemon_calls.append(payload) or {"ok": True},
    )
    sync_calls = _attach_call_recorder(window, "_sync_from_daemon")
    try:
        assert window.flow_cards == {}

        window.refresh_button.click()
        window._flush_deferred_ui_updates()

        assert window.flow_cards == {}
        assert spawn_calls == []
        assert daemon_calls == []
        assert len(sync_calls) == 1
        assert "No discoverable flows were found yet" in window.empty_flow_message
    finally:
        _dispose_window(qapp, window)


def test_workspace_switch_remains_available_while_current_workspace_runtime_is_active(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "claims_workspaces"
    claims_root = workspace_collection_root / "claims"
    claims2_root = workspace_collection_root / "claims2"
    (claims_root / "flow_modules").mkdir(parents=True)
    (claims2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="claims", workspace_root=claims_root),
        DiscoveredWorkspace(workspace_id="claims2", workspace_root=claims2_root),
    )
    def _resolve(workspace_id=None):
        target = claims_root if workspace_id in (None, "claims") else claims2_root
        target_id = "claims" if workspace_id in (None, "claims") else "claims2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
        log_service=_FakeLogService(),
    )
    try:
        assert window.workspace_paths.workspace_id == "claims"
        assert window.workspace_selector.count() == 2
        assert window.workspace_settings_selector.count() == 2

        window.runtime_session = replace(window.runtime_session, runtime_active=True)
        window._refresh_action_buttons()
        target_index = window.workspace_selector.findData("claims2")
        assert target_index >= 0

        assert window.workspace_selector.isEnabled() is True
        window.workspace_selector.setCurrentIndex(target_index)
        window._flush_deferred_ui_updates()

        assert window.workspace_paths.workspace_id == "claims2"
        assert window.workspace_selector.currentData() == "claims2"
        assert window.workspace_settings_selector.currentData() == "claims2"
    finally:
        _dispose_window(qapp, window)


def test_switching_workspace_immediately_syncs_daemon_state_for_selected_workspace(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "claims_workspaces"
    claims_root = workspace_collection_root / "claims"
    claims2_root = workspace_collection_root / "claims2"
    (claims_root / "flow_modules").mkdir(parents=True)
    (claims2_root / "flow_modules").mkdir(parents=True)
    del monkeypatch

    discovered = (
        DiscoveredWorkspace(workspace_id="claims", workspace_root=claims_root),
        DiscoveredWorkspace(workspace_id="claims2", workspace_root=claims2_root),
    )

    def _resolve(workspace_id=None):
        target = claims_root if workspace_id in (None, "claims") else claims2_root
        target_id = "claims" if workspace_id in (None, "claims") else "claims2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    sync_calls = _attach_call_recorder(window, "_sync_from_daemon")
    try:
        window._auto_daemon_enabled = True
        target_index = window.workspace_selector.findData("claims2")
        assert target_index >= 0

        window.workspace_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        assert window.workspace_paths.workspace_id == "claims2"
        assert len(sync_calls) == 1
    finally:
        _dispose_window(qapp, window)


def test_switching_workspaces_hides_selector_popup_before_rebind(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "claims_workspaces"
    claims_root = workspace_collection_root / "claims"
    claims2_root = workspace_collection_root / "claims2"
    (claims_root / "flow_modules").mkdir(parents=True)
    (claims2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="claims", workspace_root=claims_root),
        DiscoveredWorkspace(workspace_id="claims2", workspace_root=claims2_root),
    )

    def _resolve(workspace_id=None):
        target = claims_root if workspace_id in (None, "claims") else claims2_root
        target_id = "claims" if workspace_id in (None, "claims") else "claims2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    popup_hide_calls: list[str] = []
    monkeypatch.setattr(window.workspace_selector, "hidePopup", lambda: popup_hide_calls.append("selector"))
    monkeypatch.setattr(window.workspace_settings_selector, "hidePopup", lambda: popup_hide_calls.append("settings"))
    try:
        target_index = window.workspace_selector.findData("claims2")
        assert target_index >= 0

        window.workspace_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        assert popup_hide_calls == ["selector", "settings"]
    finally:
        _dispose_window(qapp, window)


def test_sync_from_daemon_immediately_clears_ui_when_current_workspace_disappears(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "claims_workspaces"
    claims_root = workspace_collection_root / "claims"
    (claims_root / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(workspace_collection_root))

    def _discover(app_root=None, workspace_collection_root=None):
        del app_root
        root = Path(workspace_collection_root) if workspace_collection_root is not None else claims_root.parent
        discovered: list[DiscoveredWorkspace] = []
        if root.exists():
            for candidate in sorted(path for path in root.iterdir() if path.is_dir()):
                if (candidate / "flow_modules").is_dir():
                    discovered.append(DiscoveredWorkspace(workspace_id=candidate.name, workspace_root=candidate.resolve()))
        return tuple(discovered)

    window = _make_window(
        discover_workspaces_func=_discover,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: resolve_workspace_paths(
            workspace_root=claims_root if workspace_id in (None, "claims") else claims_root.parent / str(workspace_id),
            workspace_id="claims" if workspace_id in (None, "claims") else str(workspace_id),
        ),
    )
    try:
        assert window.workspace_paths.workspace_id == "claims"
        assert window.selected_flow_name == "poller"

        relocated_root = tmp_path / "elsewhere" / "claims"
        relocated_root.parent.mkdir(parents=True)
        claims_root.rename(relocated_root)

        window._sync_from_daemon()
        window._flush_deferred_ui_updates()

        assert window.runtime_session == RuntimeSessionState.empty()
        assert window.selected_flow_name is None
        assert window.workspace_selector.currentData() == ""
        assert window.workspace_selector.isEnabled() is False
        assert window.flow_catalog_state.empty_message == "No discoverable flows were found yet in this workspace folder."
    finally:
        _dispose_window(qapp, window)


def test_switching_workspaces_reloads_visible_log_runs_from_new_workspace(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "workspaces"
    claims_root = workspace_collection_root / "claims"
    claims2_root = workspace_collection_root / "claims2"
    (claims_root / "flow_modules").mkdir(parents=True)
    (claims2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="claims", workspace_root=claims_root),
        DiscoveredWorkspace(workspace_id="claims2", workspace_root=claims2_root),
    )
    def _resolve(workspace_id=None):
        target = claims_root if workspace_id in (None, "claims") else claims2_root
        target_id = "claims" if workspace_id in (None, "claims") else "claims2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    initial_store = FlowLogStore()
    replacement_store = FlowLogStore()
    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
        log_service=_FakeLogService(stores=(initial_store, replacement_store)),
    )
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        initial_store.append_entry(
            FlowLogEntry(
                line="run-claims",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-claims",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="claims.xlsx",
                    status="success",
                    elapsed_seconds=0.3,
                ),
            )
        )
        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.log_view.count() == 1
        assert [group.source_label for group in window.log_store.runs_for_flow(flow_name)] == ["claims.xlsx"]

        replacement_store.append_entry(
            FlowLogEntry(
                line="run-claims2-a",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-claims2-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="claims2_a.xlsx",
                    status="success",
                    elapsed_seconds=0.4,
                ),
            )
        )
        replacement_store.append_entry(
            FlowLogEntry(
                line="run-claims2-b",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-claims2-b",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="claims2_b.xlsx",
                    status="failed",
                    elapsed_seconds=0.6,
                ),
            )
        )

        target_index = window.workspace_selector.findData("claims2")
        assert target_index >= 0
        window.workspace_selector.setCurrentIndex(target_index)
        qapp.processEvents()
        window.selected_flow_name = flow_name
        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.workspace_paths.workspace_id == "claims2"
        assert window.log_store is replacement_store
        assert [group.source_label for group in window.log_store.runs_for_flow(flow_name)] == ["claims2_a.xlsx", "claims2_b.xlsx"]
        assert _visible_log_run_primary_labels(window) == ["claims2_a.xlsx", "claims2_b.xlsx"]
    finally:
        _dispose_window(qapp, window)


def test_switching_workspaces_closes_preview_dialogs(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "workspaces"
    claims_root = workspace_collection_root / "claims"
    claims2_root = workspace_collection_root / "claims2"
    (claims_root / "flow_modules").mkdir(parents=True)
    (claims2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="claims", workspace_root=claims_root),
        DiscoveredWorkspace(workspace_id="claims2", workspace_root=claims2_root),
    )

    def _resolve(workspace_id=None):
        target = claims_root if workspace_id in (None, "claims") else claims2_root
        target_id = "claims" if workspace_id in (None, "claims") else "claims2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )

    class _FakeDialog:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    output_dialog = _FakeDialog()
    config_dialog = _FakeDialog()
    run_log_dialog = _FakeDialog()
    window.output_preview_dialog = output_dialog
    window.config_preview_dialog = config_dialog
    window.run_log_preview_dialog = run_log_dialog
    del monkeypatch
    try:
        target_index = window.workspace_selector.findData("claims2")
        assert target_index >= 0
        window.workspace_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        assert output_dialog.closed is True
        assert config_dialog.closed is True
        assert run_log_dialog.closed is True
        assert window.output_preview_dialog is None
        assert window.config_preview_dialog is None
        assert window.run_log_preview_dialog is None
    finally:
        _dispose_window(qapp, window)


def test_settings_can_save_local_workspace_collection_root_override(qapp, monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    workspace_collection_root = tmp_path / "shared_workspaces"
    (workspace_collection_root / "example_workspace" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(tmp_path / "startup_workspaces"))
    monkeypatch.setattr(
        "data_engine.ui.gui.app.QFileDialog.getExistingDirectory",
        lambda *args, **kwargs: str(workspace_collection_root),
    )

    services = build_gui_services(flow_catalog_service=_FakeFlowCatalogService())
    window = DataEngineWindow(services=services)
    try:
        window.browse_workspace_root_button.click()

        assert window.workspace_collection_root_override == workspace_collection_root.resolve()
        assert window.workspace_paths.workspace_collection_root == workspace_collection_root.resolve()
        assert window.workspace_root_status_label.text().startswith("Workspace folder:")
        assert window.workspace_selector.isEnabled() is True
        assert window.workspace_selector.currentData() == "example_workspace"
    finally:
        _dispose_window(qapp, window)


def test_settings_can_rebind_workspace_root_while_runtime_is_active(qapp, monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    startup_root = tmp_path / "startup_workspaces"
    override_root = tmp_path / "shared_workspaces"
    (startup_root / "example_workspace" / "flow_modules").mkdir(parents=True)
    (override_root / "example_workspace" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(startup_root))
    monkeypatch.setattr(
        "data_engine.ui.gui.app.QFileDialog.getExistingDirectory",
        lambda *args, **kwargs: str(override_root),
    )

    window = _make_window()
    try:
        window.runtime_session = replace(window.runtime_session, runtime_active=True)

        window.browse_workspace_root_button.click()

        assert window.workspace_paths.workspace_collection_root == override_root.resolve()
        assert window.workspace_collection_root_override == override_root.resolve()
    finally:
        _dispose_window(qapp, window)


def test_saving_workspace_collection_root_override_reloads_visible_log_runs(qapp, monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    startup_root = tmp_path / "startup_workspaces"
    override_root = tmp_path / "shared_workspaces"
    (startup_root / "example_workspace" / "flow_modules").mkdir(parents=True)
    (override_root / "example_workspace" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(startup_root))
    monkeypatch.setattr(
        "data_engine.ui.gui.app.QFileDialog.getExistingDirectory",
        lambda *args, **kwargs: str(override_root),
    )

    initial_store = FlowLogStore()
    replacement_store = FlowLogStore()
    window = _make_window(log_service=_FakeLogService(stores=(initial_store, replacement_store)))
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        initial_store.append_entry(
            FlowLogEntry(
                line="run-startup",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-startup",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="startup.xlsx",
                    status="success",
                    elapsed_seconds=0.3,
                ),
            )
        )
        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.log_view.count() == 1
        assert [group.source_label for group in window.log_store.runs_for_flow(flow_name)] == ["startup.xlsx"]

        replacement_store.append_entry(
            FlowLogEntry(
                line="run-override-a",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-override-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="override_a.xlsx",
                    status="success",
                    elapsed_seconds=0.4,
                ),
            )
        )
        replacement_store.append_entry(
            FlowLogEntry(
                line="run-override-b",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-override-b",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="override_b.xlsx",
                    status="failed",
                    elapsed_seconds=0.6,
                ),
            )
        )

        window.browse_workspace_root_button.click()
        window._flush_deferred_ui_updates()
        window.selected_flow_name = flow_name
        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.workspace_paths.workspace_collection_root == override_root.resolve()
        assert window.log_store is replacement_store
        assert [group.source_label for group in window.log_store.runs_for_flow(flow_name)] == ["override_a.xlsx", "override_b.xlsx"]
        assert _visible_log_run_primary_labels(window) == ["override_a.xlsx", "override_b.xlsx"]
    finally:
        _dispose_window(qapp, window)


def test_settings_browse_workspace_collection_root_updates_binding_immediately(qapp, monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    selected_root = tmp_path / "picked_workspaces"
    (selected_root / "example_workspace" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))

    monkeypatch.setattr(
        "data_engine.ui.gui.app.QFileDialog.getExistingDirectory",
        lambda *args, **kwargs: str(selected_root),
    )

    window = _make_window()
    try:
        window.browse_workspace_root_button.click()

        assert window.workspace_root_input.text() == str(selected_root.resolve())
        assert window.workspace_collection_root_override == selected_root.resolve()
        assert window.workspace_paths.workspace_collection_root == selected_root.resolve()
    finally:
        _dispose_window(qapp, window)


def test_lease_status_shows_countdown_and_disables_run_controls(qapp, monkeypatch):
    del monkeypatch
    window = _make_window(
        snapshot=WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=False,
            leased_by_machine_id="other-host",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="lease",
        ),
        is_live_func=lambda paths: False,
    )
    try:
        window.runtime_session = replace(window.runtime_session, workspace_owned=False, leased_by_machine_id="other-host")
        window._refresh_lease_status()
        window._refresh_summary()
        window._refresh_action_buttons()

        assert window.lease_status_label.isHidden() is False
        assert "other-host has control" in window.lease_status_label.text()
        assert "takeover available in" in window.lease_status_label.text()
        assert window.flow_run_button.isEnabled() is False
        assert window.engine_button.isEnabled() is False
        assert window.request_control_button.isHidden() is False
    finally:
        _dispose_window(qapp, window)


def test_lease_status_shows_overdue_for_local_checkpoint_when_daemon_is_down(qapp, monkeypatch):
    window = _make_window()
    try:
        del monkeypatch
        window._daemon_startup_in_progress = False
        window._daemon_manager.snapshot = WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=True,
            leased_by_machine_id="example-machine",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=(datetime.now(UTC) - timedelta(seconds=45)).isoformat(),
            source="lease",
        )

        window._refresh_lease_status()

        assert window.lease_status_label.text() == "Local engine is not responding"
    finally:
        _dispose_window(qapp, window)


def test_lease_status_shows_refresh_due_instead_of_zero_seconds(qapp, monkeypatch):
    window = _make_window()
    try:
        del monkeypatch
        window._daemon_startup_in_progress = False
        window._daemon_manager.snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id="example-machine",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=(datetime.now(UTC) - timedelta(seconds=45)).isoformat(),
            source="daemon",
        )

        window._refresh_lease_status()

        assert window.lease_status_label.text() == "This Workstation has control"
    finally:
        _dispose_window(qapp, window)


def test_gui_tolerates_brief_daemon_sync_miss_without_flipping_to_lease_view(qapp, monkeypatch):
    window = _make_window(is_live_func=lambda paths: False)
    try:
        del monkeypatch
        window.runtime_session = replace(window.runtime_session, workspace_owned=True, leased_by_machine_id=None)
        window._daemon_manager._last_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="daemon",
        )
        window._daemon_manager._sync_misses = 0
        window._daemon_manager.snapshot = WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=False,
            leased_by_machine_id="other-host",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="lease",
        )

        window._sync_from_daemon()

        assert window.runtime_session.workspace_owned is True
        assert window._daemon_manager._sync_misses >= 1
    finally:
        _dispose_window(qapp, window)


def test_gui_sync_from_daemon_skips_liveness_when_workspace_root_is_missing(qapp, monkeypatch, tmp_path):
    live_calls: list[object] = []
    window = _make_window(is_live_func=lambda paths: live_calls.append(paths) or False)
    missing_root = tmp_path / "missing_workspace"
    window.workspace_paths = resolve_workspace_paths(workspace_root=missing_root)
    window.runtime_session = replace(window.runtime_session, runtime_active=True, workspace_owned=False)
    window.workspace_control_state = replace(window.workspace_control_state, blocked_status_text="stale")
    del monkeypatch

    try:
        window._sync_from_daemon()

        assert live_calls == []
        assert window.runtime_session == RuntimeSessionState.empty()
        assert window.workspace_control_state == WorkspaceControlState.empty()
    finally:
        _dispose_window(qapp, window)


def test_gui_hydrates_shared_runtime_logs_when_observing_lease(qapp, monkeypatch):
    shared_state_service = _FakeSharedStateService()
    window = _make_window(
        shared_state_service=shared_state_service,
        snapshot=WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=False,
            leased_by_machine_id="other-host",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="lease",
        ),
    )
    try:
        del monkeypatch

        window._sync_from_daemon()

        assert len(shared_state_service.hydrated) == 1
        assert shared_state_service.hydrated[0][0] == window.workspace_paths
        assert shared_state_service.hydrated[0][1] is window.runtime_ledger
    finally:
        _dispose_window(qapp, window)


def test_request_control_button_records_request_and_logs_result(qapp, monkeypatch):
    control_application = _FakeControlApplication(
        request_control_result=type(
            "Result",
            (),
            {
                "requested": True,
                "sync_after": True,
                "ensure_daemon_started": True,
                "status_text": "Control request sent.",
                "error_text": None,
            },
        )(),
    )
    window = _make_window(control_application=control_application)
    del monkeypatch
    daemon_bootstrap_requests = _attach_call_recorder(window, "_ensure_daemon_started")
    sync_calls = _attach_call_recorder(window, "_sync_from_daemon")
    try:
        window.runtime_session = replace(window.runtime_session, workspace_owned=False, leased_by_machine_id="other-host")

        window._refresh_action_buttons()
        assert window.request_control_button.isHidden() is False

        QTest.mouseClick(window.request_control_button, Qt.MouseButton.LeftButton)

        assert control_application.request_control_calls == [window._daemon_manager]
        assert any(entry.line == "Control request sent." for entry in window.log_store._entries)
        assert len(daemon_bootstrap_requests) == 1
        assert len(sync_calls) == 1
    finally:
        _dispose_window(qapp, window)


def test_request_control_button_preserves_verbose_error_text(qapp, monkeypatch):
    control_application = _FakeControlApplication(
        request_control_result=type(
            "Result",
            (),
            {
                "requested": False,
                "sync_after": False,
                "ensure_daemon_started": False,
                "status_text": None,
                "error_text": "Failed to request workspace control. The daemon returned no additional detail.",
            },
        )(),
    )
    window = _make_window(control_application=control_application)
    capture = _attach_message_capture(window)
    try:
        window.runtime_session = replace(window.runtime_session, workspace_owned=False, leased_by_machine_id="other-host")

        window._request_control()

        assert control_application.request_control_calls == [window._daemon_manager]
        assert any(
            entry.line == "Failed to request workspace control. The daemon returned no additional detail."
            for entry in window.log_store._entries
        )
        assert capture.shown_messages == [
            (
                APP_DISPLAY_NAME,
                "Failed to request workspace control. The daemon returned no additional detail.",
                "error",
            )
        ]
    finally:
        _dispose_window(qapp, window)


def test_request_control_does_not_recover_live_local_owner(monkeypatch):
    manager = WorkspaceDaemonManager(resolve_workspace_paths())
    monkeypatch.setattr(
        "data_engine.hosts.daemon.manager.read_lease_metadata",
        lambda paths: {
            "machine_id": machine_id_text(),
            "pid": os.getpid(),
            "last_checkpoint_at_utc": datetime.now(UTC).isoformat(),
        },
    )
    recovered_calls: list[bool] = []
    written_requests: list[str] = []
    monkeypatch.setattr("data_engine.hosts.daemon.manager.recover_stale_workspace", lambda *args, **kwargs: recovered_calls.append(True) or False)
    monkeypatch.setattr("data_engine.hosts.daemon.manager.write_control_request", lambda paths, **kwargs: written_requests.append("written"))

    message = manager.request_control()

    assert message == "Control request sent."
    assert recovered_calls == []
    assert written_requests == ["written"]


def test_close_event_requests_stop_waits_for_workers_and_closes_ledger(qapp, monkeypatch):
    window = _make_window()
    closed = False
    join_calls: list[float | None] = []

    def mark_closed():
        nonlocal closed
        closed = True

    monkeypatch.setattr(window.runtime_ledger, "close", mark_closed)

    class _FakeWorker:
        def __init__(self) -> None:
            self._alive = True

        def is_alive(self) -> bool:
            return self._alive

        def join(self, timeout: float | None = None) -> None:
            join_calls.append(timeout)
            # The close path should set the stop event before waiting on workers.
            assert window.engine_runtime_stop_event.is_set() is True
            self._alive = False

    thread = _FakeWorker()
    window._register_worker_thread(thread)

    try:
        _dispose_window(qapp, window)
        qapp.processEvents()

        assert closed is True
        assert join_calls
        assert thread.is_alive() is False
    finally:
        if thread.is_alive():
            window.engine_runtime_stop_event.set()
            thread.join(timeout=1.0)


def test_workspace_selector_shows_placeholder_when_no_workspaces_are_discovered(qapp, monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    placeholder_root = app_root / ".workspace_unconfigured"

    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ROOT", raising=False)
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ID", raising=False)
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", raising=False)
    LocalSettingsStore.open_default(app_root=app_root).set_workspace_collection_root(None)

    services = build_gui_services(
        flow_catalog_service=_RaisingFlowCatalogService("No flow modules discovered."),
    )
    window = DataEngineWindow(services=services)
    try:
        assert window.workspace_paths.workspace_configured is False
        assert window.workspace_selector.count() == 1
        assert window.workspace_selector.currentText() == "(no workspace)"
        assert window.workspace_selector.isEnabled() is False
        assert window.workspace_settings_selector.count() == 1
        assert window.workspace_settings_selector.currentText() == "(no workspace)"
        assert window.workspace_settings_selector.isEnabled() is False
        assert placeholder_root.exists() is False
        assert window.workspace_paths.runtime_cache_db_path.exists() is False
        assert window.workspace_paths.runtime_control_db_path.exists() is False
    finally:
        _dispose_window(qapp, window)


def test_settings_workspace_selector_can_switch_the_provisioning_target(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "claims_workspaces"
    claims_root = workspace_collection_root / "claims"
    claims2_root = workspace_collection_root / "claims2"
    (claims_root / "flow_modules").mkdir(parents=True)
    (claims2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="claims", workspace_root=claims_root),
        DiscoveredWorkspace(workspace_id="claims2", workspace_root=claims2_root),
    )

    def _resolve(workspace_id=None):
        target = claims_root if workspace_id in (None, "claims") else claims2_root
        target_id = "claims" if workspace_id in (None, "claims") else "claims2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    try:
        target_index = window.workspace_settings_selector.findData("claims2")
        assert target_index >= 0

        window.workspace_settings_selector.setCurrentIndex(target_index)
        window._flush_deferred_ui_updates()

        assert window.workspace_paths.workspace_id == "claims2"
        assert window.workspace_selector.currentData() == "claims2"
        assert "claims2" in window.workspace_target_label.text()
    finally:
        _dispose_window(qapp, window)


def test_load_flows_clears_visible_log_runs_when_reload_fails(qapp, monkeypatch):
    window = _make_window()
    capture = _attach_message_capture(window)
    try:
        window.log_store.append_entry(
            FlowLogEntry(
                line="poller started",
                kind="flow",
                flow_name="poller",
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name="poller",
                    step_name=None,
                    source_label="claims.xlsx",
                    status="started",
                ),
            )
        )
        window.log_store.append_entry(
            FlowLogEntry(
                line="poller success",
                kind="flow",
                flow_name="poller",
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name="poller",
                    step_name=None,
                    source_label="claims.xlsx",
                    status="success",
                ),
            )
        )
        window._select_flow("poller")
        qapp.processEvents()
        assert window.log_view.count() == 1

        window.flow_catalog_application.flow_catalog_service = _RaisingFlowCatalogService("boom")

        window._load_flows()
        qapp.processEvents()

        assert window.log_view.count() == 0
        assert capture.shown_messages == [(APP_DISPLAY_NAME, "Failed to load flows.\n\nboom", "error")]
    finally:
        _dispose_window(qapp, window)


def test_load_flows_clears_visible_log_runs_when_workspace_has_no_flows(qapp, monkeypatch, tmp_path):
    window = _make_window()
    empty_root = tmp_path / "empty_workspace"
    empty_root.mkdir(parents=True)
    window.workspace_paths = resolve_workspace_paths(workspace_root=empty_root)
    try:
        window.log_store.append_entry(
            FlowLogEntry(
                line="poller started",
                kind="flow",
                flow_name="poller",
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name="poller",
                    step_name=None,
                    source_label="claims.xlsx",
                    status="started",
                ),
            )
        )
        window.log_store.append_entry(
            FlowLogEntry(
                line="poller success",
                kind="flow",
                flow_name="poller",
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name="poller",
                    step_name=None,
                    source_label="claims.xlsx",
                    status="success",
                ),
            )
        )
        window._select_flow("poller")
        qapp.processEvents()
        assert window.log_view.count() == 1

        window._load_flows()
        qapp.processEvents()

        assert window.log_view.count() == 0
        assert "No discoverable flows were found yet" in window.empty_flow_message
    finally:
        _dispose_window(qapp, window)


def test_start_runtime_reuses_loaded_flow_cards(qapp, monkeypatch, tmp_path):
    workspace_root = tmp_path / "workspaces" / "claims"
    (workspace_root / "flow_modules").mkdir(parents=True)
    control_application = _FakeControlApplication()
    control_application.start_engine_result = type(
        "Result",
        (),
        {
            "requested": True,
            "sync_after": True,
            "status_text": "Starting automated engine...",
            "error_text": None,
        },
    )()

    window = _make_window(
        snapshot=WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="daemon",
        ),
        control_application=control_application,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: resolve_workspace_paths(
            workspace_root=workspace_root,
            workspace_id=workspace_id or "claims",
        ),
    )
    del monkeypatch
    try:
        window._load_flows()
        assert "poller" in window.flow_cards

        window._start_runtime()

        assert window.runtime_session.runtime_active is True
        assert window.runtime_session.active_runtime_flow_names == ("poller",)
        assert len(control_application.start_engine_calls) == 1
    finally:
        _dispose_window(qapp, window)


def test_finish_runtime_treats_stop_requested_error_as_normal_stop(qapp, monkeypatch):
    window = _make_window()
    capture = _attach_message_capture(window)
    try:
        window.runtime_session = replace(window.runtime_session, runtime_active=True).with_active_runtime_flow_names(("poller",))
        window.engine_runtime_stop_event.set()
        window._set_flow_state("poller", "stopping runtime")

        window._finish_runtime(("poller",), None, RuntimeError("background unwind"))

        assert window.runtime_session.runtime_active is False
        assert window.flow_states["poller"] == "poll ready"
        assert capture.shown_later_messages == []
    finally:
        _dispose_window(qapp, window)


def test_stop_runtime_enters_stopping_transition_and_disables_engine_button(qapp, monkeypatch):
    daemon_commands: list[str] = []

    window = _make_window(
        snapshot=WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=True,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="daemon",
        ),
        request_func=lambda paths, payload, timeout=0.0: daemon_commands.append(str(payload["command"])) or {"ok": True},
    )
    del monkeypatch
    try:
        window.runtime_session = replace(window.runtime_session, runtime_active=True).with_active_runtime_flow_names(("poller",))

        window._stop_runtime()

        assert daemon_commands == ["stop_engine"]
        assert window.runtime_session.runtime_stopping is True
        assert window.flow_states["poller"] == "stopping runtime"
        assert window.engine_button.text() == "Stopping..."
        assert window.engine_button.isEnabled() is False
    finally:
        _dispose_window(qapp, window)


def test_start_runtime_is_blocked_while_stop_transition_is_in_progress(qapp, monkeypatch):
    daemon_commands: list[str] = []

    window = _make_window(
        request_func=lambda paths, payload, timeout=0.0: daemon_commands.append(str(payload["command"])) or {"ok": True},
    )
    try:
        window.runtime_session = replace(window.runtime_session, runtime_stopping=True)

        window._start_runtime()

        assert daemon_commands == []
        assert window.runtime_session.runtime_active is False
    finally:
        _dispose_window(qapp, window)


def test_apply_runtime_event_marks_failed_automated_flow_in_summary_state(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window.runtime_session = replace(window.runtime_session, runtime_active=True).with_active_runtime_flow_names(("poller",))
        window._set_flow_state("poller", "polling")

        window._apply_runtime_event(
            RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name=None,
                source_label="input.xlsx",
                status="failed",
            )
        )

        assert window.flow_states["poller"] == "failed"

        window._apply_runtime_event(
            RuntimeStepEvent(
                run_id="run-2",
                flow_name="poller",
                step_name=None,
                source_label="input.xlsx",
                status="success",
            )
        )

        assert window.flow_states["poller"] == "polling"
    finally:
        _dispose_window(qapp, window)


def test_close_event_requests_daemon_shutdown(qapp, monkeypatch):
    del monkeypatch

    shutdown_calls: list[dict[str, object]] = []
    live_checks = iter([True, False])
    ledger_service = _FakeLedgerService(remaining_counts=[0])

    window = _make_window(
        is_live_func=lambda paths: next(live_checks, False),
        request_func=lambda paths, payload, timeout=0: shutdown_calls.append({"payload": payload, "timeout": timeout}) or {"ok": True},
        ledger_service=ledger_service,
    )
    try:
        _dispose_window(qapp, window)

        assert shutdown_calls == [{"payload": {"command": "shutdown_daemon"}, "timeout": 1.5}]
    finally:
        _dispose_window(qapp, window)


def test_close_event_does_not_request_daemon_shutdown_when_other_local_client_exists(qapp, monkeypatch):
    del monkeypatch

    shutdown_calls: list[dict[str, object]] = []
    ledger_service = _FakeLedgerService(remaining_counts=[1])

    window = _make_window(
        is_live_func=lambda paths: True,
        request_func=lambda paths, payload, timeout=0: shutdown_calls.append({"payload": payload, "timeout": timeout}) or {"ok": True},
        ledger_service=ledger_service,
    )
    try:
        _dispose_window(qapp, window)

        assert shutdown_calls == []
    finally:
        _dispose_window(qapp, window)


def test_last_window_close_purges_same_process_ui_sessions_before_shutdown_check(qapp, monkeypatch):
    shutdown_calls: list[dict[str, object]] = []
    first_ledger_service = _FakeLedgerService(remaining_counts=[1])
    second_ledger_service = _FakeLedgerService(remaining_counts=[0])
    live_checks = iter([True, False])
    first = _make_window(
        is_live_func=lambda paths: next(live_checks, False),
        request_func=lambda paths, payload, timeout=0: shutdown_calls.append({"payload": payload, "timeout": timeout}) or {"ok": True},
        ledger_service=first_ledger_service,
    )
    second = _make_window(
        is_live_func=lambda paths: next(live_checks, False),
        request_func=lambda paths, payload, timeout=0: shutdown_calls.append({"payload": payload, "timeout": timeout}) or {"ok": True},
        ledger_service=second_ledger_service,
    )
    try:
        monkeypatch.setattr(
            "data_engine.ui.gui.app.helper_is_last_process_ui_window",
            lambda window: window is second,
        )
        first.close()

        assert first_ledger_service.purged_sessions == []
        assert shutdown_calls == []

        second.close()

        assert second_ledger_service.removed_client_ids
        assert second_ledger_service.purged_sessions == [
            {"workspace_id": second.workspace_paths.workspace_id, "client_kind": "ui", "pid": os.getpid()}
        ]
        assert shutdown_calls == [{"payload": {"command": "shutdown_daemon"}, "timeout": 1.5}]
    finally:
        _dispose_window(qapp, first)
        _dispose_window(qapp, second)


def test_worker_thread_snapshot_tolerates_partially_bootstrapped_window(qapp):
    window = DataEngineWindow.__new__(DataEngineWindow)

    assert window._worker_threads_snapshot() == ()


def test_force_shutdown_daemon_button_calls_force_stop_path(qapp, monkeypatch):
    del monkeypatch

    force_shutdown_calls: list[dict[str, object]] = []
    window = _make_window(
        force_shutdown_func=lambda paths, timeout=0.5: force_shutdown_calls.append(
            {"workspace": paths.workspace_root, "timeout": timeout}
        ),
    )
    try:
        window._force_shutdown_daemon()

        assert force_shutdown_calls == [{"workspace": window.workspace_paths.workspace_root, "timeout": 0.5}]
        assert "force-stopped" in window.force_shutdown_daemon_status_label.text().lower()
    finally:
        _dispose_window(qapp, window)


def test_finish_run_does_not_show_modal_for_automated_flow_failure(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    capture = _attach_message_capture(window)
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Imports": "poller"})
        window.manual_flow_stop_events["Imports"] = threading.Event()

        window._finish_run("poller", None, RuntimeError("boom"))

        assert window.flow_states["poller"] == "failed"
        assert capture.shown_later_messages == []
    finally:
        _dispose_window(qapp, window)


def test_show_run_error_details_uses_verbose_fallback_when_persisted_error_is_blank(qapp, monkeypatch):
    window = _make_window()
    capture = _attach_message_capture(window)
    monkeypatch.setattr(
        window.runtime_history_service,
        "error_text_for_entry",
        lambda ledger, run_group, entry: ("Run Error", ""),
    )
    try:
        entry = FlowLogEntry(
            line="example_completed failed -",
            kind="flow",
            flow_name="example_completed",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="example_completed",
                step_name=None,
                source_label="-",
                status="failed",
            ),
        )
        run_group = FlowRunState(
            key=("example_completed", "run-1"),
            display_label="2026-04-05 06:36:14 PM",
            source_label="-",
            status="failed",
            elapsed_seconds=0.0,
            summary_entry=entry,
            steps=(),
            entries=(entry,),
        )

        window._show_run_error_details(run_group, entry)

        assert capture.shown_messages == [
            (
                "Run Error",
                'No persisted error detail was available for failed run "example_completed".',
                "error",
            )
        ]
    finally:
        _dispose_window(qapp, window)


def test_show_message_box_later_coalesces_same_tick_modal_requests(qapp, monkeypatch):
    window = _make_window()
    capture = _MessageCapture()
    window._show_message_box = capture.show_now
    scheduled_callbacks: list[object] = []

    monkeypatch.setattr(
        "data_engine.ui.gui.surface.QTimer.singleShot",
        lambda _delay, callback: scheduled_callbacks.append(callback),
    )
    try:
        window._show_message_box_later(title="Data Engine", text="first", tone="error")
        window._show_message_box_later(title="Data Engine", text="second", tone="error")

        assert len(scheduled_callbacks) == 1

        scheduled_callbacks.pop()()

        assert capture.shown_messages == [("Data Engine", "second", "error")]
    finally:
        _dispose_window(qapp, window)


def test_switching_workspaces_invalidates_stale_deferred_modal_callbacks(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "claims_workspaces"
    claims_root = workspace_collection_root / "claims"
    claims2_root = workspace_collection_root / "claims2"
    (claims_root / "flow_modules").mkdir(parents=True)
    (claims2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="claims", workspace_root=claims_root),
        DiscoveredWorkspace(workspace_id="claims2", workspace_root=claims2_root),
    )

    def _resolve(workspace_id=None):
        target = claims_root if workspace_id in (None, "claims") else claims2_root
        target_id = "claims" if workspace_id in (None, "claims") else "claims2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    capture = _MessageCapture()
    window._show_message_box = capture.show_now
    scheduled_callbacks: list[object] = []
    monotonic_now = 100.0
    monkeypatch.setattr(window, "_monotonic", lambda: monotonic_now)

    monkeypatch.setattr(
        "data_engine.ui.gui.surface.QTimer.singleShot",
        lambda _delay, callback: scheduled_callbacks.append(callback),
    )
    try:
        window._show_message_box_later(title="Data Engine", text="old workspace", tone="error")
        old_callback = scheduled_callbacks.pop()

        target_index = window.workspace_selector.findData("claims2")
        assert target_index >= 0
        window.workspace_selector.setCurrentIndex(target_index)
        switch_callback = scheduled_callbacks.pop()
        switch_callback()

        window._show_message_box_later(title="Data Engine", text="new workspace", tone="error")
        new_callback = scheduled_callbacks.pop()

        old_callback()
        assert capture.shown_messages == []

        new_callback()
        assert capture.shown_messages == [("Data Engine", "new workspace", "error")]
    finally:
        _dispose_window(qapp, window)


def test_show_run_error_details_uses_persisted_run_error_for_flow_level_failure(qapp, monkeypatch):
    window = _make_window()
    capture = _attach_message_capture(window)
    monkeypatch.setattr(
        window.runtime_history_service,
        "error_text_for_entry",
        lambda ledger, run_group, entry: ("Run Error", "Source path not found: /tmp/input"),
    )
    try:
        entry = FlowLogEntry(
            line="run=run-1 flow=example_completed source=None status=failed elapsed=0.000612",
            kind="flow",
            flow_name="example_completed",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="example_completed",
                step_name=None,
                source_label="-",
                status="failed",
            ),
        )
        run_group = FlowRunState(
            key=("example_completed", "run-1"),
            display_label="2026-04-05 06:46:04 PM",
            source_label="-",
            status="failed",
            elapsed_seconds=0.0,
            summary_entry=entry,
            steps=(),
            entries=(entry,),
        )

        window._show_run_error_details(run_group, entry)

        assert capture.shown_messages == [("Run Error", "Source path not found: /tmp/input", "error")]
    finally:
        _dispose_window(qapp, window)


def test_live_log_refresh_preserves_scroll_position_when_not_at_bottom(qapp, monkeypatch):
    del qapp, monkeypatch

    assert next_log_scroll_value(
        previous_value=40,
        previous_maximum=100,
        current_maximum=120,
    ) == 40


def test_live_log_refresh_does_not_snap_when_near_bottom_but_not_at_end(qapp, monkeypatch):
    del qapp, monkeypatch

    assert next_log_scroll_value(
        previous_value=98,
        previous_maximum=100,
        current_maximum=120,
    ) == 98


def test_set_flow_states_skips_sidebar_rebuild_when_state_is_unchanged(qapp, monkeypatch):
    window = _make_window()
    rebuild_calls = 0
    original = window._populate_flow_tree

    def counting_populate():
        nonlocal rebuild_calls
        rebuild_calls += 1
        return original()

    monkeypatch.setattr(window, "_populate_flow_tree", counting_populate)
    try:
        window.flow_states["poller"] = "polling"
        window._set_flow_state("poller", "polling")

        assert rebuild_calls == 0
    finally:
        _dispose_window(qapp, window)


def test_stop_runtime_updates_sidebar_in_place_without_rebuild(qapp, monkeypatch):
    window = _make_window(
        request_func=lambda paths, payload, timeout=0.0: {"ok": True},
    )
    rebuild_calls = 0
    original = window._populate_flow_tree

    def counting_populate():
        nonlocal rebuild_calls
        rebuild_calls += 1
        return original()

    monkeypatch.setattr(window, "_populate_flow_tree", counting_populate)
    monkeypatch.setattr(
        window,
        "_sync_from_daemon",
        lambda: setattr(window, "runtime_session", replace(window.runtime_session, runtime_stopping=True)) or window._set_flow_state("poller", "stopping runtime"),
    )
    try:
        window.runtime_session = replace(window.runtime_session, runtime_active=True).with_active_runtime_flow_names(("poller",))
        rebuild_calls = 0

        window._stop_runtime()

        assert rebuild_calls == 0
        assert window.flow_states["poller"] == "stopping runtime"
    finally:
        _dispose_window(qapp, window)


def test_poll_log_queue_batches_selected_flow_refresh(qapp, monkeypatch):
    window = _make_window()
    refresh_calls = 0
    original = window._refresh_log_view

    def counting_refresh(*, force_scroll_to_bottom: bool = False):
        nonlocal refresh_calls
        refresh_calls += 1
        return original(force_scroll_to_bottom=force_scroll_to_bottom)

    monkeypatch.setattr(window, "_refresh_log_view", counting_refresh)
    try:
        window._select_flow("poller")
        refresh_calls = 0

        for index in range(5):
            window.log_queue.put(
                FlowLogEntry(
                    line=f"poller event {index}",
                    kind="flow",
                    flow_name="poller",
                    event=RuntimeStepEvent(
                        run_id=f"run-{index}",
                        flow_name="poller",
                        step_name=None,
                        source_label=f"input-{index}.xlsx",
                        status="started",
                    ),
                )
            )

        window._poll_log_queue()
        qapp.processEvents()

        assert refresh_calls == 1
    finally:
        _dispose_window(qapp, window)


def test_run_log_preview_omits_placeholder_source_separator(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        entry = FlowLogEntry(
            line="run=abc flow=claims_summary step=Collect Claim Files source=None status=started",
            kind="runtime",
            flow_name="claims_summary",
            event=RuntimeStepEvent(
                run_id="abc",
                flow_name="claims_summary",
                step_name="Collect Claim Files",
                source_label="-",
                status="started",
            ),
        )

        rendered = window._format_raw_log_message(entry)

        assert "claims_summary &gt; &gt;" not in rendered
        assert rendered == "claims_summary &gt; <b>Collect Claim Files</b> - <i>started</i>"
    finally:
        _dispose_window(qapp, window)


def test_poll_log_queue_yields_when_backlog_exceeds_tick_limit(qapp, monkeypatch):
    window = _make_window()
    monkeypatch.setattr(window, "_MAX_LOG_EVENTS_PER_TICK", 2)
    try:
        window._select_flow("poller")

        for index in range(5):
            window.log_queue.put(
                FlowLogEntry(
                    line=f"poller event {index}",
                    kind="flow",
                    flow_name="poller",
                    event=RuntimeStepEvent(
                        run_id=f"run-{index}",
                        flow_name="poller",
                        step_name=None,
                        source_label=f"input-{index}.xlsx",
                        status="started",
                    ),
                )
            )

        window._poll_log_queue()

        assert window.log_queue.qsize() == 3

        qapp.processEvents()
        qapp.processEvents()

        assert window.log_queue.empty()
    finally:
        _dispose_window(qapp, window)


def test_log_view_limits_visible_run_history_for_busy_flow(qapp, monkeypatch):
    window = _make_window()
    try:
        for index in range(window._MAX_VISIBLE_LOG_RUNS + 25):
            run_id = f"run-{index}"
            window.log_store.append_entry(
                FlowLogEntry(
                    line=f"poller started {index}",
                    kind="flow",
                    flow_name="poller",
                    event=RuntimeStepEvent(
                        run_id=run_id,
                        flow_name="poller",
                        step_name=None,
                        source_label=f"input-{index}.xlsx",
                        status="started",
                    ),
                )
            )
            window.log_store.append_entry(
                FlowLogEntry(
                    line=f"poller success {index}",
                    kind="flow",
                    flow_name="poller",
                    event=RuntimeStepEvent(
                        run_id=run_id,
                        flow_name="poller",
                        step_name=None,
                        source_label=f"input-{index}.xlsx",
                        status="success",
                    ),
                )
            )

        window._select_flow("poller")
        qapp.processEvents()

        assert window.log_view.count() == window._MAX_VISIBLE_LOG_RUNS
    finally:
        _dispose_window(qapp, window)

