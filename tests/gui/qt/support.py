from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
import logging
import os
from pathlib import Path
from queue import Queue
import threading

import pytest
import polars as pl
from PySide6.QtCore import QPoint, Qt
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QLabel, QLineEdit, QListWidget, QPushButton, QSpinBox, QTableWidget, QTextEdit, QWidget
from shiboken6 import delete as shiboken_delete
from shiboken6 import isValid as shiboken_is_valid

from data_engine.core.model import FlowValidationError
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, WorkspaceDaemonSnapshot
from data_engine.domain import (
    ActiveRunState,
    DaemonStatusState,
    FlowActivityState,
    FlowCatalogEntry,
    FlowRunState,
    OperationSessionState,
    RuntimeSessionState,
    WorkspaceControlState,
)
from data_engine.platform.identity import APP_DISPLAY_NAME, APP_VERSION
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.workspace_models import DiscoveredWorkspace, machine_id_text
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.runtime.execution.logging import RuntimeLogEmitter
from data_engine.services.runtime_state import ControlSnapshot, EngineSnapshot, FlowLiveSummary, RunLiveSnapshot, WorkspaceRuntimeProjection, WorkspaceSnapshot
from data_engine.domain import StepOutputIndex
from data_engine.services import DaemonService, DaemonStateService
from data_engine.services.daemon_state import DaemonLaneUpdate, DaemonUpdateBatch
from data_engine.services.operator_commands import OperatorCommandService
from data_engine.services.workspace_provisioning import WorkspaceProvisioningResult
from data_engine.ui.gui.bootstrap import build_gui_services
from data_engine.views import flow_category
from data_engine.ui.gui.icons import ICON_ASSETS, load_svg_icon_text
from data_engine.ui.gui.app import DataEngineWindow
from data_engine.ui.gui.rendering import classify_artifact_preview, theme_svg_paths
from data_engine.ui.gui.rendering.artifacts import (
    _build_distinct_value_filter_expression,
    _export_frame_to_excel,
    _ParquetPreviewLoader,
)
from data_engine.ui.gui.runtime import QueueLogHandler
from data_engine.domain import FlowLogEntry
from data_engine.domain import RuntimeStepEvent, parse_runtime_event
from data_engine.views.models import QtFlowCard
from data_engine.views.logs import FlowLogStore
from data_engine.ui.gui.presenters.logs import next_log_scroll_value
from data_engine.ui.gui.theme import stylesheet, theme_button_text, toggle_theme_name
from data_engine.ui.gui.widgets.logs import build_log_run_widget
from data_engine.ui.gui.widgets.sidebar import build_flow_row_widget, build_group_row_widget


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def _workspace_snapshot_for_test(
    workspace_id: str,
    *,
    control: ControlSnapshot | None = None,
) -> WorkspaceSnapshot:
    return WorkspaceSnapshot(
        workspace_id=workspace_id,
        version=0,
        control=control or ControlSnapshot(state="available"),
        engine=EngineSnapshot(state="idle"),
        flows={},
        active_runs={},
    )


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
            title="Docs Poller",
            description="Polls for new claim workbooks.",
            source_root="/tmp/input",
            target_root="/tmp/output",
            mode="poll",
            interval="30s",
            settle="1",
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
            settle="-",
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
            title="Docs Poller A",
            description="Polls for new claim workbooks.",
            source_root="/tmp/input-a",
            target_root="/tmp/output-a",
            mode="poll",
            interval="30s",
            settle="1",
            operations="Read Excel -> Write Parquet",
            operation_items=("Read Excel", "Write Parquet"),
            state="poll ready",
            valid=True,
            category="automated",
        ),
        QtFlowCard(
            name="poller_b",
            group="Imports",
            title="Docs Poller B",
            description="Polls for new claim workbooks.",
            source_root="/tmp/input-b",
            target_root="/tmp/output-b",
            mode="schedule",
            interval="30s",
            settle="-",
            operations="Read Excel -> Write Parquet",
            operation_items=("Read Excel", "Write Parquet"),
            state="schedule ready",
            valid=True,
            category="automated",
        ),
    )


def _append_persisted_run_log(workspace_root, *, run_id: str, flow_name: str, source_path: str, status: str, elapsed: float | None = None) -> None:
    ledger = RuntimeCacheLedger.open_default(data_root=workspace_root)
    try:
        message = f"run={run_id} flow={flow_name} source={source_path} status={status}"
        if elapsed is not None:
            message += f" elapsed={elapsed}"
        ledger.logs.append(
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

    def wait_for_update(self, *, timeout_seconds: float = 5.0) -> WorkspaceDaemonSnapshot:
        del timeout_seconds
        if self._last_snapshot is not None:
            return self._last_snapshot
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

    def wait_for_update(self, manager, *, timeout_seconds: float = 5.0):
        return manager.wait_for_update(timeout_seconds=timeout_seconds)

    def run_subscription_loop(
        self,
        manager,
        *,
        stop_event,
        workspace_available,
        on_update,
        timeout_seconds: float = 1.5,
    ):
        while not stop_event.is_set():
            if not workspace_available():
                if stop_event.wait(timeout_seconds):
                    return
                continue
            previous_snapshot = getattr(manager, "_last_snapshot", None)
            snapshot = self.wait_for_update(manager, timeout_seconds=timeout_seconds)
            if stop_event.is_set():
                return
            if previous_snapshot is not None and snapshot == previous_snapshot:
                continue
            on_update(DaemonStateService.diff_update_batch(previous_snapshot, snapshot))

    @staticmethod
    def should_run_heartbeat(
        *,
        daemon_live: bool,
        transport_mode: str,
        wait_worker_alive: bool,
        now_monotonic: float,
        last_sync_monotonic: float,
        last_subscription_monotonic: float,
        stale_after_seconds: float = 15.0,
    ) -> bool:
        if not daemon_live:
            return True
        del transport_mode, wait_worker_alive
        freshest = max(float(last_sync_monotonic or 0.0), float(last_subscription_monotonic or 0.0))
        return (float(now_monotonic) - freshest) >= max(float(stale_after_seconds), 0.0)

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
        return RuntimeCacheLedger.open_default(data_root=workspace_root)

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

    def reload(self, store, runtime_ledger=None) -> None:
        del store, runtime_ledger

    def append_entry(self, store, entry) -> None:
        store.append_entry(entry)

    def clear_flow(self, store, flow_name):
        store.clear_flow(flow_name)

    def all_entries(self, store):
        return store.entries()

    def entries_for_flow(self, store, flow_name):
        return store.entries_for_flow(flow_name)

    def runs_for_flow(self, store, flow_name):
        return store.runs_for_flow(flow_name)


class _FakeControlApplication:
    def __init__(self, *, request_control_result=None) -> None:
        self.request_control_result = request_control_result
        self.request_control_calls: list[object] = []
        self.run_selected_flow_result = None
        self.run_selected_flow_calls: list[dict[str, object]] = []
        self.refresh_flows_result = None
        self.refresh_flows_calls: list[dict[str, object]] = []
        self.start_engine_result = None
        self.start_engine_calls: list[dict[str, object]] = []
        self.stop_pipeline_result = None
        self.stop_pipeline_calls: list[dict[str, object]] = []

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

    def run_selected_flow(self, **kwargs):
        self.run_selected_flow_calls.append(kwargs)
        if self.run_selected_flow_result is None:
            raise AssertionError("run_selected_flow_result was not configured for this fake control application.")
        return self.run_selected_flow_result

    def start_engine(self, **kwargs):
        self.start_engine_calls.append(kwargs)
        if self.start_engine_result is None:
            raise AssertionError("start_engine_result was not configured for this fake control application.")
        return self.start_engine_result

    def stop_pipeline(self, **kwargs):
        self.stop_pipeline_calls.append(kwargs)
        if self.stop_pipeline_result is None:
            raise AssertionError("stop_pipeline_result was not configured for this fake control application.")
        return self.stop_pipeline_result


class _FakeSharedStateService:
    def __init__(self) -> None:
        self.hydrated: list[tuple[object, object]] = []

    def hydrate_local_runtime(self, paths, ledger) -> None:
        self.hydrated.append((paths, ledger))


class _FakeResetService:
    def __init__(self) -> None:
        self.flow_resets: list[tuple[object, str]] = []
        self.workspace_resets: list[object] = []

    def reset_flow(self, *, paths, runtime_cache_ledger, flow_name: str) -> None:
        del runtime_cache_ledger
        self.flow_resets.append((paths, flow_name))

    def reset_workspace(self, *, paths, runtime_cache_ledger, runtime_control_ledger) -> None:
        del runtime_cache_ledger, runtime_control_ledger
        self.workspace_resets.append(paths)


def _command_service_for_test(
    *,
    control_application=None,
    reset_service=None,
    workspace_provisioning_service=None,
    force_shutdown_func=None,
):
    class _RuntimeApplicationForCommands:
        def force_shutdown_daemon(self, paths, timeout=0.5):
            try:
                if force_shutdown_func is not None:
                    force_shutdown_func(paths, timeout=timeout)
                return type("_Result", (), {"ok": True, "error": None})()
            except Exception as exc:  # pragma: no cover - defensive test hook
                return type("_Result", (), {"ok": False, "error": str(exc)})()

    return OperatorCommandService(
        control_application=control_application or _FakeControlApplication(),
        runtime_application=_RuntimeApplicationForCommands(),
        reset_service=reset_service or _FakeResetService(),
        workspace_provisioning_service=workspace_provisioning_service,
    )


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
    command_service=None,
    shared_state_service=None,
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
        command_service=command_service,
        shared_state_service=shared_state_service,
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


def _process_ui_until(qapp, predicate, *, timeout_ms: int = 1000) -> None:
    deadline = datetime.now(UTC) + timedelta(milliseconds=timeout_ms)
    while datetime.now(UTC) < deadline:
        qapp.processEvents()
        if predicate():
            return
        QTest.qWait(10)
    qapp.processEvents()


def _visible_log_run_primary_labels(window: DataEngineWindow) -> list[str]:
    labels: list[str] = []
    for index in range(window.log_view.count()):
        item = window.log_view.item(index)
        source_label = window.log_view.source_label(item)
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
            "msg": "run=run-123 flow=docs_poll step=Write Parquet source=/tmp/input.xlsx status=success elapsed=0.532100"
        }
    )

    event = parse_runtime_event(record)

    assert event is not None
    assert event.run_id == "run-123"
    assert event.flow_name == "docs_poll"
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

    window.runtime_binding.runtime_cache_ledger.runs.record_started(
        run_id="run-recent",
        flow_name="poller_a",
        group_name="Imports",
        source_path="/tmp/input-a.xlsx",
        started_at_utc=recent_started,
    )
    window.runtime_binding.runtime_cache_ledger.runs.record_started(
        run_id="run-old",
        flow_name="poller_b",
        group_name="Imports",
        source_path="/tmp/input-b.xlsx",
        started_at_utc=old_started,
    )
    window._workspace_counts_footer_cache.clear()
    window._refresh_workspace_visibility_panel()

    assert window.workspace_counts_footer_label.text() == "0 modules - 1 groups - 2 flows - 1 runs last 7 days"
    assert window.app_version_footer_label.text() == f"v{APP_VERSION}"
    assert not window.app_version_footer_label.isHidden()
    assert window.visibility_interpreter_mode_value.text() == "Virtual Environment"


def test_provision_workspace_button_creates_missing_workspace_assets(qapp, tmp_path, monkeypatch):
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

    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    provisioning_service = _RecordingProvisioningService()
    window = _make_window(
        command_service=_command_service_for_test(workspace_provisioning_service=provisioning_service),
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    selected_paths = resolve_workspace_paths(workspace_root=docs2_root, workspace_id="docs2")

    try:
        target_index = window.workspace_settings_selector.findData("docs2")
        assert target_index >= 0
        window.workspace_settings_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        window._provision_selected_workspace()
        _process_ui_until(
            qapp,
            lambda: provisioning_service.requested_paths is not None
            and "provision_workspace" not in window._pending_control_actions,
        )

        assert provisioning_service.requested_paths is not None
        assert provisioning_service.requested_paths.workspace_root == selected_paths.workspace_root
        assert window.workspace_paths.workspace_id == "docs"
        assert (selected_paths.workspace_root / "flow_modules").is_dir()
        assert selected_paths.workspace_id in window.workspace_target_label.text()
        assert f"Provisioned {selected_paths.workspace_root.name}" in window.workspace_provision_status_label.text()
    finally:
        _dispose_window(qapp, window)


def test_icon_registry_loads_current_file_backed_svg():
    assert ICON_ASSETS["dark_light"].file_name == "dark_light.svg"
    assert ICON_ASSETS["dataframe"].file_name == "dataframe.svg"

    svg_text = load_svg_icon_text("dataframe")

    assert "<svg" in svg_text
    assert "viewBox=" in svg_text


def test_artifact_preview_classification_is_explicit(tmp_path):
    assert classify_artifact_preview(tmp_path / "output.parquet").kind == "parquet"
    assert classify_artifact_preview(tmp_path / "workbook.xlsx").kind == "excel"
    assert classify_artifact_preview(tmp_path / "debug.json").kind == "json"
    assert classify_artifact_preview(tmp_path / "notes.txt").kind == "text"
    assert classify_artifact_preview(tmp_path / "packet.pdf").kind == "pdf"
    assert classify_artifact_preview(tmp_path / "blob.bin").kind == "unsupported"


def test_structured_error_content_parses_step_failure(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        parsed = window._structured_error_content(
            'Flow "docs_summary" failed in step "Combine Docs" (function combine_docs) '
            'for source "/tmp/input.xlsx": ValueError: boom'
        )

        assert parsed is not None
        assert parsed.title == "Flow Failed"
        assert tuple((field.label, field.value) for field in parsed.fields) == (
            ("Flow", "docs_summary"),
            ("Phase", "step"),
            ("Step", "Combine Docs"),
            ("Function", "combine_docs"),
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
            'Flow module "docs_summary" failed during build() in build: RuntimeError: build boom'
        )

        assert parsed is not None
        assert parsed.title == "Flow Module Failed"
        assert tuple((field.label, field.value) for field in parsed.fields) == (
            ("Flow Module", "docs_summary"),
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
            "Available flow modules: docs_demo, manual_docs_demo."
        )

        assert parsed is not None
        assert parsed.title == "Flow Module Not Found"
        assert tuple((field.label, field.value) for field in parsed.fields) == (
            ("Flow Module", "broken_step"),
            ("Workspace", "/tmp/workspace/flow_modules"),
            ("Available", "docs_demo, manual_docs_demo"),
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
        window.runtime_binding.runtime_cache_ledger.runs.record_started(
            run_id="run-1",
            flow_name="poller",
            group_name="Imports",
            source_path="/tmp/input.xlsx",
            started_at_utc=started_at,
        )
        step_id = window.runtime_binding.runtime_cache_ledger.step_outputs.record_started(
            run_id="run-1",
            flow_name="poller",
            step_label="Write Parquet",
            started_at_utc=started_at,
        )
        window.runtime_binding.runtime_cache_ledger.step_outputs.record_finished(
            step_run_id=step_id,
            status="success",
            finished_at_utc=started_at,
            elapsed_ms=5,
            output_path=str(output_path),
        )
        window.runtime_binding.runtime_cache_ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started_at)

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
        export_button = window.output_preview_dialog.findChild(QPushButton, "outputPreviewExportExcelButton")

        assert "2 rows" in meta_label.text()
        assert "2 columns" in meta_label.text()
        assert table is not None
        assert table.rowCount() == 2
        assert table.columnCount() == 2
        assert export_button is not None
        assert export_button.isEnabled()
    finally:
        if window.output_preview_dialog is not None:
            window.output_preview_dialog.close()
        _dispose_window(qapp, window)


def test_show_output_preview_parquet_summary_uses_footer(qapp, tmp_path):
    output_path = tmp_path / "preview.parquet"
    pl.DataFrame({"claim_id": [1001, 1002], "status": ["OPEN", "CLOSED"]}).write_parquet(output_path)

    window = _make_window()
    try:
        window._show_output_preview("Write Parquet", output_path)

        assert window.output_preview_dialog is not None
        table = window.output_preview_dialog.findChild(QTableWidget, "outputPreviewTable")
        summary_label = window.output_preview_dialog.findChild(QLabel, "workspaceCountsFooter")
        assert table is not None
        assert summary_label is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 2)
        assert summary_label.text() == "2 rows - 2 columns - showing top 200 rows"
    finally:
        if window.output_preview_dialog is not None:
            window.output_preview_dialog.close()
        _dispose_window(qapp, window)


def test_output_preview_export_writes_excel_workbook(qapp, monkeypatch, tmp_path):
    export_path = tmp_path / "preview_export"
    messages: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "data_engine.ui.gui.rendering.artifacts.QFileDialog.getSaveFileName",
        lambda *args, **kwargs: (str(export_path), "Excel Workbook (*.xlsx)"),
    )
    monkeypatch.setattr(
        "data_engine.ui.gui.rendering.artifacts.QMessageBox.information",
        lambda _parent, title, message: messages.append((title, message)),
    )
    monkeypatch.setattr(
        "data_engine.ui.gui.rendering.artifacts.QMessageBox.critical",
        lambda _parent, title, message: messages.append((title, message)),
    )

    written_path = _export_frame_to_excel(
        pl.DataFrame({"member_id": ["A1", "A2"], "amount": [10, 20]}),
        source_path=tmp_path / "source.parquet",
        parent=QWidget(),
    )

    assert written_path == export_path.with_suffix(".xlsx")
    assert written_path.exists()
    assert pl.read_excel(written_path, sheet_id=1, engine="calamine").to_dict(as_series=False) == {
        "member_id": ["A1", "A2"],
        "amount": [10, 20],
    }
    assert messages[0][0] == "Export Complete"


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

        assert "PDF" in meta_label.text()
        assert body is not None
        assert "not available yet" in body.toPlainText()
    finally:
        if window.output_preview_dialog is not None:
            window.output_preview_dialog.close()
        _dispose_window(qapp, window)


def test_parquet_preview_loader_sample_mode_collects_only_preview_rows(tmp_path):
    output_path = tmp_path / "preview.parquet"
    pl.DataFrame(
        {
            "claim_id": list(range(20)),
            "status": ["OPEN" if index % 2 == 0 else "CLOSED" for index in range(20)],
        }
    ).write_parquet(output_path)

    loader = _ParquetPreviewLoader(
        output_path,
        active_value_filters={},
        sort_columns=(),
        preview_mode="sample",
        preview_row_limit=5,
    )
    loaded: list[tuple[object, object, str]] = []
    failures: list[str] = []
    loader.preview_loaded.connect(lambda schema, preview, summary: loaded.append((schema, preview, summary)))
    loader.load_failed.connect(failures.append)

    loader.run()

    assert failures == []
    assert len(loaded) == 1
    _schema, preview, summary = loaded[0]
    assert preview.height == 5
    assert preview.columns == ["claim_id", "status"]
    assert "20 rows" in summary
    assert "showing sample of 5 rows" in summary


def test_parquet_preview_loader_top_mode_avoids_discarded_preview_collect(tmp_path, monkeypatch):
    output_path = tmp_path / "preview.parquet"
    pl.DataFrame(
        {
            "claim_id": list(range(20)),
            "status": ["OPEN" if index % 2 == 0 else "CLOSED" for index in range(20)],
        }
    ).write_parquet(output_path)

    real_scan_parquet = pl.scan_parquet
    collect_count = 0

    class _LazyFrameProxy:
        def __init__(self, inner):
            self._inner = inner

        def collect(self, *args, **kwargs):
            nonlocal collect_count
            collect_count += 1
            return self._inner.collect(*args, **kwargs)

        def __getattr__(self, name):
            attr = getattr(self._inner, name)
            if not callable(attr):
                return attr

            def _wrapped(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, pl.LazyFrame):
                    return _LazyFrameProxy(result)
                return result

            return _wrapped

    monkeypatch.setattr(pl, "scan_parquet", lambda *args, **kwargs: _LazyFrameProxy(real_scan_parquet(*args, **kwargs)))

    loader = _ParquetPreviewLoader(
        output_path,
        active_value_filters={},
        sort_columns=(),
        preview_mode="top",
        preview_row_limit=5,
    )
    loaded: list[tuple[object, object, str]] = []
    failures: list[str] = []
    loader.preview_loaded.connect(lambda schema, preview, summary: loaded.append((schema, preview, summary)))
    loader.load_failed.connect(failures.append)

    loader.run()

    assert failures == []
    assert len(loaded) == 1
    _schema, preview, summary = loaded[0]
    assert preview.height == 5
    assert "showing top 5 rows" in summary
    assert collect_count == 2


def test_distinct_value_filter_preserves_datetime_time_unit_precision():
    timestamp = datetime(2026, 4, 24, 12, 30, 45, 123000)
    frame = pl.DataFrame({"created_at": [timestamp]}).with_columns(
        pl.col("created_at").cast(pl.Datetime("ms"))
    )
    selected_value = frame.get_column("created_at").to_list()[0]

    result = frame.lazy().filter(
        _build_distinct_value_filter_expression(
            "created_at",
            (selected_value,),
            dtype=frame.schema["created_at"],
        )
    ).collect()

    assert result.height == 1


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
        assert window.view_stack.count() == 5
        assert window.selected_flow_name == "poller"
        assert set(window.sidebar_flow_widgets) == {"poller", "manual_review"}
        poller_widget = window.sidebar_flow_widgets["poller"]
        primary_label = next(label for label in poller_widget.findChildren(QLabel) if label.objectName() == "sidebarFlowCode")
        assert primary_label.text() == "Docs Poller"

        _click_flow_row(window, "manual_review")
        assert window.selected_flow_name == "manual_review"
    finally:
        _dispose_window(qapp, window)


def test_data_engine_window_nav_buttons_switch_views(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        assert window.view_stack.currentIndex() == 0

        window.debug_button.click()
        assert window.view_stack.currentIndex() == 2

        window.docs_button.click()
        assert window.view_stack.currentIndex() == 3

        window.settings_button.click()
        assert window.view_stack.currentIndex() == 4

        window.dataframes_button.click()
        assert window.view_stack.currentIndex() == 1

        window.home_button.click()
        assert window.view_stack.currentIndex() == 0
    finally:
        _dispose_window(qapp, window)


def test_refresh_button_reloads_flows(qapp, monkeypatch, tmp_path):
    workspace_root = tmp_path / "workspaces" / "docs"
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
        command_service=_command_service_for_test(control_application=control_application),
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: resolve_workspace_paths(
            workspace_root=workspace_root,
            workspace_id=workspace_id or "docs",
        ),
    )
    sync_calls = _attach_call_recorder(window, "_sync_from_daemon")
    try:
        window._load_flows()
        assert "poller" in window.flow_cards

        window._refresh_flows_requested()
        _process_ui_until(
            qapp,
            lambda: len(control_application.refresh_flows_calls) == 1
            and "refresh_flows" not in window._pending_control_actions,
        )
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
        _process_ui_until(qapp, lambda: len(sync_calls) == 1)
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
        _process_ui_until(qapp, lambda: len(sync_calls) == 1)
        window._flush_deferred_ui_updates()

        assert window.flow_cards == {}
        assert spawn_calls == []
        assert daemon_calls == []
        assert len(sync_calls) == 1
        assert "No discoverable flows were found yet" in window.empty_flow_message
    finally:
        _dispose_window(qapp, window)


def test_workspace_switch_remains_available_while_current_workspace_runtime_is_active(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )
    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
        log_service=_FakeLogService(),
    )
    try:
        assert window.workspace_paths.workspace_id == "docs"
        assert window.workspace_selector.count() == 2
        assert window.workspace_settings_selector.count() == 2

        window.runtime_session = replace(window.runtime_session, runtime_active=True)
        window._refresh_action_buttons()
        target_index = window.workspace_selector.findData("docs2")
        assert target_index >= 0

        assert window.workspace_selector.isEnabled() is True
        window.workspace_selector.setCurrentIndex(target_index)
        window._flush_deferred_ui_updates()

        assert window.workspace_paths.workspace_id == "docs2"
        assert window.workspace_selector.currentData() == "docs2"
        assert window.workspace_settings_selector.currentData() == "docs2"
    finally:
        _dispose_window(qapp, window)


def test_switching_workspace_immediately_syncs_daemon_state_for_selected_workspace(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)
    del monkeypatch

    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    sync_calls = _attach_call_recorder(window, "_sync_from_daemon")
    try:
        window._auto_daemon_enabled = True
        target_index = window.workspace_selector.findData("docs2")
        assert target_index >= 0

        window.workspace_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        assert window.workspace_paths.workspace_id == "docs2"
        assert len(sync_calls) == 1
    finally:
        _dispose_window(qapp, window)


def test_switching_workspaces_hides_selector_popup_before_rebind(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    popup_hide_calls: list[str] = []
    monkeypatch.setattr(window.workspace_selector, "hidePopup", lambda: popup_hide_calls.append("selector"))
    monkeypatch.setattr(window.workspace_settings_selector, "hidePopup", lambda: popup_hide_calls.append("settings"))
    try:
        target_index = window.workspace_selector.findData("docs2")
        assert target_index >= 0

        window.workspace_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        assert popup_hide_calls == ["selector", "settings"]
    finally:
        _dispose_window(qapp, window)


def test_sync_from_daemon_immediately_clears_ui_when_current_workspace_disappears(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    (docs_root / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(workspace_collection_root))

    def _discover(app_root=None, workspace_collection_root=None):
        del app_root
        root = Path(workspace_collection_root) if workspace_collection_root is not None else docs_root.parent
        discovered: list[DiscoveredWorkspace] = []
        if root.exists():
            for candidate in sorted(path for path in root.iterdir() if path.is_dir()):
                if (candidate / "flow_modules").is_dir():
                    discovered.append(DiscoveredWorkspace(workspace_id=candidate.name, workspace_root=candidate.resolve()))
        return tuple(discovered)

    window = _make_window(
        discover_workspaces_func=_discover,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: resolve_workspace_paths(
            workspace_root=docs_root if workspace_id in (None, "docs") else docs_root.parent / str(workspace_id),
            workspace_id="docs" if workspace_id in (None, "docs") else str(workspace_id),
        ),
    )
    try:
        assert window.workspace_paths.workspace_id == "docs"
        assert window.selected_flow_name == "poller"

        relocated_root = tmp_path / "elsewhere" / "docs"
        relocated_root.parent.mkdir(parents=True)
        docs_root.rename(relocated_root)

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
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )
    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
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
                line="run-docs",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-docs",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                    elapsed_seconds=0.3,
                ),
            )
        )
        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.log_view.count() == 1
        assert [group.source_label for group in window.log_store.runs_for_flow(flow_name)] == ["docs.xlsx"]

        replacement_store.append_entry(
            FlowLogEntry(
                line="run-docs2-a",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-docs2-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs2_a.xlsx",
                    status="success",
                    elapsed_seconds=0.4,
                ),
            )
        )
        replacement_store.append_entry(
            FlowLogEntry(
                line="run-docs2-b",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-docs2-b",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs2_b.xlsx",
                    status="failed",
                    elapsed_seconds=0.6,
                ),
            )
        )

        target_index = window.workspace_selector.findData("docs2")
        assert target_index >= 0
        window.workspace_selector.setCurrentIndex(target_index)
        qapp.processEvents()
        window.selected_flow_name = flow_name
        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.workspace_paths.workspace_id == "docs2"
        assert window.log_store is replacement_store
        assert [group.source_label for group in window.log_store.runs_for_flow(flow_name)] == ["docs2_a.xlsx", "docs2_b.xlsx"]
        assert _visible_log_run_primary_labels(window) == ["docs2_a.xlsx", "docs2_b.xlsx"]
    finally:
        _dispose_window(qapp, window)


def test_switching_to_workspace_with_no_flows_clears_grouped_log_pane(qapp, monkeypatch, tmp_path):
    del monkeypatch
    workspace_collection_root = tmp_path / "workspaces"
    docs_root = workspace_collection_root / "docs"
    empty_root = workspace_collection_root / "empty"
    (docs_root / "flow_modules").mkdir(parents=True)
    empty_root.mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="empty", workspace_root=empty_root),
    )

    def _resolve(workspace_id=None):
        if workspace_id == "empty":
            return resolve_workspace_paths(workspace_root=empty_root, workspace_id="empty")
        return resolve_workspace_paths(workspace_root=docs_root, workspace_id="docs")

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
                line="run-docs",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-docs",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                    elapsed_seconds=0.3,
                ),
            )
        )
        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.log_view.count() == 1
        assert _visible_log_run_primary_labels(window) == ["docs.xlsx"]

        target_index = window.workspace_selector.findData("empty")
        assert target_index >= 0
        window.workspace_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        assert window.workspace_paths.workspace_id == "empty"
        assert window.selected_flow_name is None
        assert window.log_store is replacement_store
        assert window.log_view.count() == 0
        assert _visible_log_run_primary_labels(window) == []
    finally:
        _dispose_window(qapp, window)


def test_switching_workspaces_closes_preview_dialogs(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
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
        target_index = window.workspace_selector.findData("docs2")
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
        window.workspace_snapshot = _workspace_snapshot_for_test(
            window.workspace_paths.workspace_id,
            control=ControlSnapshot(
                state="leased",
                leased_by_machine_id="other-host",
                control_status_text="other-host has control Â· takeover available in 30s",
                blocked_status_text="other-host currently has control of this workspace.",
                takeover_remaining_seconds=30,
            ),
        )
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
        window.workspace_snapshot = _workspace_snapshot_for_test(
            window.workspace_paths.workspace_id,
            control=ControlSnapshot(
                state="available",
                control_status_text="Local engine is not responding",
                blocked_status_text="Takeover available.",
            ),
        )

        window._refresh_lease_status()

        assert window.lease_status_label.text() == "Local engine is not responding"
    finally:
        _dispose_window(qapp, window)


def test_manual_run_selected_flow_turns_run_button_into_graceful_stop_and_disables_engine_start(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.flow_states["manual_review"] = "running"
        window._select_flow("manual_review")
        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Stop Flow"
        assert window.flow_run_button.property("flowRunState") == "stop"
        assert window.flow_run_button.isEnabled() is True
        assert window.engine_button.text() == "Start Engine"
        assert window.engine_button.isEnabled() is False
    finally:
        _dispose_window(qapp, window)


def test_manual_run_selected_flow_turns_run_button_into_graceful_stop_while_engine_runs_elsewhere(qapp, monkeypatch):
    del monkeypatch
    window = _make_window(
        snapshot=WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=False,
            manual_runs=("manual_review",),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="daemon",
        ),
    )
    try:
        window.runtime_session = RuntimeSessionState(
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=False,
            active_runtime_flow_names=("poller",),
            manual_runs=(),
        ).with_manual_runs_map({"Manual": "manual_review"})
        window.flow_states["manual_review"] = "running"
        window._select_flow("manual_review")
        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Stop Flow"
        assert window.flow_run_button.property("flowRunState") == "stop"
        assert window.flow_run_button.isEnabled() is True
        assert window.engine_button.text() == "Stop Engine"
        assert window.engine_button.isEnabled() is True
    finally:
        _dispose_window(qapp, window)


def test_manual_run_selected_flow_button_requests_graceful_stop_instead_of_new_run(qapp, monkeypatch):
    del monkeypatch
    control_application = _FakeControlApplication()
    control_application.stop_pipeline_result = type(
        "Result",
        (),
        {
            "requested": True,
            "sync_after": True,
            "status_text": "Stopping selected flow...",
            "error_text": None,
        },
    )()
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.flow_states["manual_review"] = "running"
        window._select_flow("manual_review")
        window._refresh_action_buttons()

        window._run_selected_flow()
        _process_ui_until(qapp, lambda: len(control_application.stop_pipeline_calls) == 1)

        assert control_application.run_selected_flow_calls == []
        assert control_application.stop_pipeline_calls[0]["selected_flow_name"] == "manual_review"
        assert control_application.stop_pipeline_calls[0]["action_context"].selected_flow.card.group == "Manual"
    finally:
        _dispose_window(qapp, window)


def test_manual_run_selected_flow_button_shows_stopping_while_stop_request_is_pending(qapp, monkeypatch):
    del monkeypatch
    control_application = _FakeControlApplication()
    release_result = threading.Event()
    control_application.stop_pipeline_result = type(
        "Result",
        (),
        {
            "requested": True,
            "sync_after": True,
            "status_text": "Stopping selected flow...",
            "error_text": None,
        },
    )()

    original_stop_pipeline = control_application.stop_pipeline

    def _delayed_stop_pipeline(**kwargs):
        release_result.wait(timeout=1.0)
        return original_stop_pipeline(**kwargs)

    control_application.stop_pipeline = _delayed_stop_pipeline
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.flow_states["manual_review"] = "running"
        window._select_flow("manual_review")
        window._refresh_action_buttons()

        window._run_selected_flow()
        _process_ui_until(qapp, lambda: "stop_pipeline" in window._pending_control_actions)

        assert window.flow_run_button.text() == "Stopping..."
        assert window.flow_run_button.isEnabled() is False
        assert window.flow_run_button.property("flowRunState") == "stop"

        release_result.set()
        _process_ui_until(qapp, lambda: "stop_pipeline" not in window._pending_control_actions)
    finally:
        release_result.set()
        _dispose_window(qapp, window)


def test_manual_run_selected_flow_button_stays_stopping_until_run_finishes(qapp, monkeypatch):
    del monkeypatch
    control_application = _FakeControlApplication()
    control_application.stop_pipeline_result = type(
        "Result",
        (),
        {
            "requested": True,
            "sync_after": False,
            "status_text": "Stopping selected flow...",
            "error_text": None,
        },
    )()
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.flow_states["manual_review"] = "running"
        window._select_flow("manual_review")
        window._refresh_action_buttons()

        window._run_selected_flow()
        _process_ui_until(
            qapp,
            lambda: len(control_application.stop_pipeline_calls) == 1
            and "stop_pipeline" not in window._pending_control_actions,
        )

        assert window.flow_run_button.text() == "Stopping..."
        assert window.flow_run_button.isEnabled() is False
        assert "Manual" in window.manual_flow_stopping_groups

        window.manual_flow_stop_events["Manual"] = threading.Event()
        window._finish_run("manual_review", [], None)

        assert window.flow_run_button.text() == "Run Once"
        assert window.flow_run_button.isEnabled() is True
        assert "Manual" not in window.manual_flow_stopping_groups
    finally:
        _dispose_window(qapp, window)


def test_lease_status_shows_refresh_due_instead_of_zero_seconds(qapp, monkeypatch):
    window = _make_window()
    try:
        del monkeypatch
        window.workspace_snapshot = _workspace_snapshot_for_test(
            window.workspace_paths.workspace_id,
            control=ControlSnapshot(
                state="available",
                control_status_text="This Workstation has control",
                blocked_status_text="Takeover available.",
            ),
        )

        window._refresh_lease_status()

        assert window.lease_status_label.text() == "This Workstation has control"
    finally:
        _dispose_window(qapp, window)


def test_show_event_reveals_action_bar_controls_after_startup_paint(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        assert window.action_bar_controls_group.isHidden() is True

        window.show()
        qapp.processEvents()

        assert window.action_bar_controls_group.isHidden() is False
    finally:
        _dispose_window(qapp, window)


def test_show_event_runs_initial_daemon_sync_for_engine_button_truth(qapp, monkeypatch):
    del monkeypatch
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
            active_engine_flow_names=("poller",),
        ),
    )
    try:
        assert window.engine_button.text() == "Start Engine"

        window.show()
        _process_ui_until(qapp, lambda: window.engine_button.text() == "Stop Engine")

        assert window.engine_button.text() == "Stop Engine"
        assert window.engine_button.isEnabled() is True
    finally:
        _dispose_window(qapp, window)


def test_local_same_machine_lease_keeps_start_engine_enabled_during_startup(qapp, monkeypatch):
    del monkeypatch
    window = _make_window(
        snapshot=WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="lease",
        ),
    )
    try:
        window.workspace_snapshot = _workspace_snapshot_for_test(
            window.workspace_paths.workspace_id,
            control=ControlSnapshot(
                state="available",
                control_status_text="This Workstation has control",
                blocked_status_text="Takeover available.",
            ),
        )

        window._sync_from_daemon()

        assert window.request_control_button.isHidden() is True
        assert window.engine_button.text() == "Start Engine"
        assert window.engine_button.isEnabled() is True
    finally:
        _dispose_window(qapp, window)


def test_refresh_lease_status_uses_existing_control_state_without_syncing_again(qapp, monkeypatch):
    window = _make_window()
    try:
        window.workspace_snapshot = _workspace_snapshot_for_test(
            window.workspace_paths.workspace_id,
            control=ControlSnapshot(
                state="available",
                control_status_text="This Workstation has control",
                blocked_status_text="Takeover available.",
            ),
        )

        monkeypatch.setattr(
            window.daemon_state_service,
            "sync",
            lambda manager: (_ for _ in ()).throw(AssertionError("refresh_lease_status should not resync")),
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
    del monkeypatch

    try:
        window._sync_from_daemon()

        assert live_calls == []
        assert window.runtime_session == RuntimeSessionState.empty()
        assert window.workspace_snapshot is None
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
        assert shared_state_service.hydrated[0][1] is window.runtime_binding.runtime_cache_ledger
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
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    del monkeypatch
    daemon_bootstrap_requests = _attach_call_recorder(window, "_ensure_daemon_started")
    sync_calls = _attach_call_recorder(window, "_sync_from_daemon")
    try:
        window.runtime_session = replace(window.runtime_session, workspace_owned=False, leased_by_machine_id="other-host")

        window._refresh_action_buttons()
        assert window.request_control_button.isHidden() is False

        QTest.mouseClick(window.request_control_button, Qt.MouseButton.LeftButton)
        _process_ui_until(
            qapp,
            lambda: len(control_application.request_control_calls) == 1
            and "request_control" not in window._pending_control_actions,
        )

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
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    capture = _attach_message_capture(window)
    try:
        window.runtime_session = replace(window.runtime_session, workspace_owned=False, leased_by_machine_id="other-host")

        window._request_control()
        _process_ui_until(
            qapp,
            lambda: len(control_application.request_control_calls) == 1
            and "request_control" not in window._pending_control_actions,
        )

        assert control_application.request_control_calls == [window._daemon_manager]
        assert any(
            entry.line == "Failed to request workspace control. The daemon returned no additional detail."
            for entry in window.log_store._entries
        )
        assert capture.shown_later_messages == [
            (
                APP_DISPLAY_NAME,
                "Failed to request workspace control. The daemon returned no additional detail.",
                "error",
            )
        ]
    finally:
        _dispose_window(qapp, window)


def test_request_control_button_shows_requesting_while_request_is_pending(qapp, monkeypatch):
    del monkeypatch
    control_application = _FakeControlApplication(
        request_control_result=type(
            "Result",
            (),
            {
                "requested": True,
                "sync_after": False,
                "ensure_daemon_started": False,
                "status_text": "Control request sent.",
                "error_text": None,
            },
        )(),
    )
    original_request_control = control_application.request_control

    def _delayed_request_control(manager):
        QTest.qWait(50)
        return original_request_control(manager)

    control_application.request_control = _delayed_request_control
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    try:
        window.runtime_session = replace(window.runtime_session, workspace_owned=False, leased_by_machine_id="other-host")
        window._refresh_action_buttons()

        window._request_control()
        _process_ui_until(qapp, lambda: "request_control" in window._pending_control_actions)

        assert window.request_control_button.text() == "Requesting..."
        assert window.request_control_button.isEnabled() is False

        _process_ui_until(qapp, lambda: "request_control" not in window._pending_control_actions)
        assert window.request_control_button.text() == "Request Control"
    finally:
        _dispose_window(qapp, window)


def test_request_control_uses_shared_state_adapter_without_recovering_live_local_owner(monkeypatch):
    class _FakeSharedStateAdapter:
        def __init__(self) -> None:
            self.recovered_calls: list[tuple[object, str, float]] = []
            self.written_requests: list[dict[str, object]] = []

        def read_lease_metadata(self, paths):
            del paths
            return {
                "machine_id": machine_id_text(),
                "pid": os.getpid(),
                "last_checkpoint_at_utc": datetime.now(UTC).isoformat(),
            }

        def recover_stale_workspace(self, paths, *, machine_id, stale_after_seconds, reclaim=True):
            del reclaim
            self.recovered_calls.append((paths, machine_id, stale_after_seconds))
            return False

        def write_control_request(self, paths, **kwargs):
            self.written_requests.append({"paths": paths, **kwargs})

        def read_control_request(self, paths):
            del paths
            return None

    adapter = _FakeSharedStateAdapter()
    manager = WorkspaceDaemonManager(resolve_workspace_paths(), shared_state_adapter=adapter)
    recovered_calls: list[bool] = []
    monkeypatch.setattr(
        "data_engine.hosts.daemon.manager._lease_pid_is_live",
        lambda metadata: recovered_calls.append(True) or True,
    )

    message = manager.request_control()

    assert message == "Control request sent."
    assert recovered_calls == [True]
    assert adapter.recovered_calls == []
    assert len(adapter.written_requests) == 1


def test_close_event_requests_stop_waits_for_workers_and_closes_ledger(qapp, monkeypatch):
    window = _make_window()
    closed = False
    join_calls: list[float | None] = []

    def mark_closed():
        nonlocal closed
        closed = True

    monkeypatch.setattr(window.runtime_binding.runtime_cache_ledger, "close", mark_closed)

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


def test_daemon_startup_worker_shuts_down_late_orphaned_daemon_when_window_closes(qapp, monkeypatch):
    window = _make_window()
    entered_spawn = threading.Event()
    release_spawn = threading.Event()
    shutdown_calls: list[object] = []
    binding_events: list[str] = []
    original_open_binding = window.runtime_binding_service.open_binding
    original_close_binding = window.runtime_binding_service.close_binding

    def _spawn_daemon(paths):
        del paths
        entered_spawn.set()
        assert release_spawn.wait(timeout=1.0)
        return type("Result", (), {"ok": True, "error": ""})()

    def _open_binding(paths):
        binding_events.append("open")
        return original_open_binding(paths)

    def _close_binding(binding):
        binding_events.append("close")
        original_close_binding(binding)

    monkeypatch.setattr(window.runtime_application, "spawn_daemon", _spawn_daemon)
    monkeypatch.setattr(window.runtime_binding_service, "open_binding", _open_binding)
    monkeypatch.setattr(window.runtime_binding_service, "close_binding", _close_binding)
    monkeypatch.setattr(window.runtime_binding_service, "count_live_client_sessions", lambda binding: 0)
    monkeypatch.setattr(window.daemon_service, "is_live", lambda paths: True)
    monkeypatch.setattr(window, "_shutdown_daemon_on_close", lambda: shutdown_calls.append(window.workspace_paths))

    worker = threading.Thread(target=window.runtime_controller.start_daemon_worker, args=(window,), daemon=True)
    worker.start()
    assert entered_spawn.wait(timeout=1.0)

    window.ui_closing = True
    release_spawn.set()
    worker.join(timeout=1.0)

    assert worker.is_alive() is False
    assert shutdown_calls == [window.workspace_paths]
    assert binding_events == ["open", "close"]
    assert window._daemon_startup_in_progress is False
    _dispose_window(qapp, window)


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
        window._sync_from_daemon()
        assert window.workspace_paths.runtime_cache_db_path.exists() is False
        assert window.workspace_paths.runtime_control_db_path.exists() is False
        assert window.workspace_paths.runtime_state_dir.exists() is False
    finally:
        _dispose_window(qapp, window)


def test_settings_workspace_selector_can_switch_the_provisioning_target(qapp, monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    try:
        target_index = window.workspace_settings_selector.findData("docs2")
        assert target_index >= 0

        window.workspace_settings_selector.setCurrentIndex(target_index)
        window._flush_deferred_ui_updates()

        assert window.workspace_paths.workspace_id == "docs"
        assert window.workspace_selector.currentData() == "docs"
        assert window.settings_workspace_target_id == "docs2"
        assert window.workspace_settings_selector.currentData() == "docs2"
        assert "docs2" in window.workspace_target_label.text()
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
                    source_label="docs.xlsx",
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
                    source_label="docs.xlsx",
                    status="success",
                ),
            )
        )
        window._select_flow("poller")
        qapp.processEvents()
        assert window.log_view.count() == 1

        window.catalog_query_service.flow_catalog_service = _RaisingFlowCatalogService("boom")

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
                    source_label="docs.xlsx",
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
                    source_label="docs.xlsx",
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
    workspace_root = tmp_path / "workspaces" / "docs"
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
        command_service=_command_service_for_test(control_application=control_application),
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: resolve_workspace_paths(
            workspace_root=workspace_root,
            workspace_id=workspace_id or "docs",
        ),
    )
    del monkeypatch
    try:
        window._load_flows()
        assert "poller" in window.flow_cards

        window._start_runtime()
        _process_ui_until(
            qapp,
            lambda: len(control_application.start_engine_calls) == 1,
        )

        assert len(control_application.start_engine_calls) == 1
    finally:
        _dispose_window(qapp, window)


def test_pending_engine_start_disables_run_once_for_selected_automated_flow(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window.selected_flow_name = "poller"
        window._pending_control_actions.add("start_runtime")

        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Run Once"
        assert window.flow_run_button.isEnabled() is False
        assert window.engine_button.text() == "Starting..."
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


def test_finish_daemon_sync_deduplicates_repeated_sync_error_logs(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    log_lines: list[tuple[str, str | None]] = []
    window._append_log_line = lambda line, flow_name=None: log_lines.append((line, flow_name))
    try:
        window.show()
        qapp.processEvents()
        token = window._workspace_binding_token()

        window._finish_daemon_sync({"workspace_token": token, "error": RuntimeError("sync failed")})
        window._finish_daemon_sync({"workspace_token": token, "error": RuntimeError("sync failed")})

        assert log_lines == [
            (
                "Daemon sync failed: sync failed",
                None,
            )
        ]

        window._finish_daemon_sync(
            {
                "workspace_token": token,
                "sync_state": type(
                    "_SyncState",
                    (),
                    {
                        "daemon_status": DaemonStatusState.empty(),
                    },
                )(),
                "projection": type(
                    "_Projection",
                    (),
                    {
                        "runtime_session": RuntimeSessionState.empty(),
                        "operation_tracker": OperationSessionState.empty(),
                        "flow_states": {},
                        "step_output_index": StepOutputIndex.empty(),
                    },
                )(),
                "workspace_snapshot": _workspace_snapshot_for_test(window.workspace_paths.workspace_id),
            }
        )
        window._finish_daemon_sync({"workspace_token": token, "error": RuntimeError("sync failed")})

        assert log_lines == [
            (
                "Daemon sync failed: sync failed",
                None,
            ),
            (
                "Daemon sync failed: sync failed",
                None,
            ),
        ]
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
        _process_ui_until(qapp, lambda: daemon_commands == ["stop_engine"])

        assert daemon_commands == ["stop_engine"]
    finally:
        _dispose_window(qapp, window)


def test_stop_runtime_failure_restores_runtime_state(qapp, monkeypatch):
    del monkeypatch
    control_application = _FakeControlApplication()
    control_application.stop_pipeline_result = type(
        "Result",
        (),
        {
            "requested": False,
            "sync_after": False,
            "status_text": None,
            "error_text": "No control",
        },
    )()
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    try:
        window.runtime_session = replace(window.runtime_session, runtime_active=True).with_active_runtime_flow_names(("poller",))
        window.flow_states["poller"] = "polling"

        window._stop_runtime()
        _process_ui_until(qapp, lambda: "stop_runtime" not in window._pending_control_actions)

        assert window.runtime_session.runtime_stopping is False
        assert window.flow_states["poller"] == "polling"
        assert window.engine_button.text() == "Stop Engine"
    finally:
        _dispose_window(qapp, window)


def test_manual_run_selected_flow_request_is_not_reissued_before_live_truth_arrives(qapp, monkeypatch):
    del monkeypatch
    control_application = _FakeControlApplication()
    control_application.run_selected_flow_result = type(
        "Result",
        (),
        {
            "requested": True,
            "sync_after": False,
            "status_text": "Starting one-time flow run: manual_review",
            "error_text": None,
        },
    )()
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    try:
        window._select_flow("manual_review")
        window._refresh_action_buttons()

        window._run_selected_flow()
        _process_ui_until(
            qapp,
            lambda: len(control_application.run_selected_flow_calls) == 1
            and "run_selected_flow" not in window._pending_control_actions,
        )

        assert window.flow_run_button.text() == "Starting..."
        assert window.flow_run_button.isEnabled() is False
        assert "Manual" in window.pending_manual_run_requests

        window._run_selected_flow()
        qapp.processEvents()

        assert len(control_application.run_selected_flow_calls) == 1
    finally:
        _dispose_window(qapp, window)


def test_manual_run_selected_flow_stays_starting_while_live_summary_is_still_idle(qapp, monkeypatch):
    del monkeypatch
    control_application = _FakeControlApplication()
    control_application.run_selected_flow_result = type(
        "Result",
        (),
        {
            "requested": True,
            "sync_after": False,
            "status_text": "Starting one-time flow run: manual_review",
            "error_text": None,
        },
    )()
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    try:
        window._select_flow("manual_review")
        window._refresh_action_buttons()

        window._run_selected_flow()
        _process_ui_until(
            qapp,
            lambda: len(control_application.run_selected_flow_calls) == 1
            and "run_selected_flow" not in window._pending_control_actions,
        )

        window.workspace_snapshot = replace(
            window.workspace_snapshot,
            flows={
                "manual_review": FlowLiveSummary(
                    flow_name="manual_review",
                    group_name="Manual",
                    state="manual",
                    active_run_count=0,
                    queued_run_count=0,
                ),
            },
        )
        window._refresh_action_buttons()
        window.runtime_controller._prune_pending_manual_run_requests(window, active_manual_groups=set())
        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Starting..."
        assert window.flow_run_button.isEnabled() is False
        assert "Manual" in window.pending_manual_run_requests
    finally:
        _dispose_window(qapp, window)


def test_engine_stop_stays_enabled_while_manual_run_is_starting_under_running_engine(qapp, monkeypatch):
    del monkeypatch
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
            active_engine_flow_names=("poller",),
        ),
    )
    try:
        window.selected_flow_name = "manual_review"
        window.runtime_session = RuntimeSessionState(
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=False,
            active_runtime_flow_names=("poller",),
            manual_runs=(),
        )
        window.pending_manual_run_requests["Manual"] = (
            "manual_review",
            datetime.now(UTC).isoformat(),
            window._monotonic(),
        )

        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Starting..."
        assert window.flow_run_button.isEnabled() is False
        assert window.engine_button.text() == "Stop Engine"
        assert window.engine_button.isEnabled() is True
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


def test_close_event_requests_engine_stop_instead_of_daemon_shutdown_when_engine_is_active(qapp, monkeypatch):
    del monkeypatch

    requests: list[dict[str, object]] = []
    ledger_service = _FakeLedgerService(remaining_counts=[0])

    window = _make_window(
        is_live_func=lambda paths: True,
        request_func=lambda paths, payload, timeout=0: requests.append({"payload": payload, "timeout": timeout}) or {"ok": True},
        ledger_service=ledger_service,
    )
    window.workspace_snapshot = WorkspaceSnapshot(
        workspace_id=window.workspace_paths.workspace_id,
        version=1,
        control=ControlSnapshot(state="available"),
        engine=EngineSnapshot(state="running", daemon_live=True, active_flow_names=("poller",)),
        flows={},
        active_runs={},
    )
    try:
        _dispose_window(qapp, window)

        assert requests == [{"payload": {"command": "stop_engine", "shutdown_when_idle": True}, "timeout": 1.5}]
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
        _process_ui_until(
            qapp,
            lambda: len(force_shutdown_calls) == 1
            and "force_shutdown_daemon" not in window._pending_control_actions,
        )

        assert force_shutdown_calls == [{"workspace": window.workspace_paths.workspace_root, "timeout": 0.5}]
        assert "force-stopped" in window.force_shutdown_daemon_status_label.text().lower()
    finally:
        _dispose_window(qapp, window)


def test_debug_nav_button_is_icon_only_and_switches_to_debug_view(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        assert window.debug_button.text() == ""
        assert window.debug_button.toolTip() == ""
        assert window.view_stack.tabText(2) == "Debug"
        assert window.view_stack.currentIndex() == 0

        window.debug_button.click()
        qapp.processEvents()

        assert window.view_stack.currentIndex() == 2
        assert window.debug_button.isChecked() is True
    finally:
        _dispose_window(qapp, window)


def test_dataframes_view_connects_single_parquet_file(qapp, tmp_path):
    output_path = tmp_path / "claims.parquet"
    pl.DataFrame({"claim_id": [1001, 1002], "status": ["OPEN", "CLOSED"]}).write_parquet(output_path)

    window = _make_window()
    try:
        window.dataframes_button.click()
        qapp.processEvents()
        window._connect_dataframe_path(output_path)

        table = window.dataframe_preview_layout.itemAt(0).widget().findChild(QTableWidget, "outputPreviewTable")
        export_button = window.findChild(QPushButton, "outputPreviewExportExcelButton")
        assert window.view_stack.currentIndex() == 1
        assert window.dataframe_source_input.text() == str(output_path)
        assert table is not None
        assert export_button is not None
        status_label = window.dataframe_preview_summary_label
        assert status_label is not None
        assert (
            window.dataframe_preview_controls_layout.indexOf(export_button)
            < window.dataframe_preview_controls_layout.indexOf(window.dataframe_preview_mode_combo)
        )
        _process_ui_until(qapp, lambda: table.rowCount() == 2)
        assert window.dataframe_preview_title_label.text() == "claims.parquet"
        assert window.dataframe_preview_summary_label.objectName() == "workspaceCountsFooter"
        assert "2 rows - 2 columns - showing top 200 rows" in window.dataframe_preview_summary_label.text()
    finally:
        _dispose_window(qapp, window)


def test_dataframes_view_connects_parquet_folder(qapp, tmp_path):
    first_path = tmp_path / "a.parquet"
    nested_dir = tmp_path / "nested"
    nested_dir.mkdir()
    second_path = nested_dir / "b.parquet"
    ignored_path = tmp_path / "notes.txt"
    pl.DataFrame({"claim_id": [1]}).write_parquet(first_path)
    pl.DataFrame({"claim_id": [2]}).write_parquet(second_path)
    ignored_path.write_text("skip", encoding="utf-8")

    window = _make_window()
    try:
        window.dataframes_button.click()
        qapp.processEvents()
        window._connect_dataframe_path(tmp_path)

        table = window.dataframe_preview_layout.itemAt(0).widget().findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        assert window.dataframe_source_input.text() == str(tmp_path)
        assert window._dataframe_preview_path.as_posix().endswith("/**/*.parquet")
        _process_ui_until(qapp, lambda: table.rowCount() == 2)
        assert window.dataframe_preview_title_label.text() == "2 parquet files"
    finally:
        _dispose_window(qapp, window)


def test_debug_view_lists_previews_and_clears_saved_debug_artifacts(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_mirror__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame({"claim_id": [1], "status": ["OPEN"]}).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_mirror",
                        "step_name": "Read Excel",
                        "run_id": "run-1",
                        "source_path": "C:/input/docs_flat_1.xlsx",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_mirror / Read Excel / 2026-04-19T00-00-00Z",
                    },
                    "info": {"note": "saved from test"},
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        assert window.debug_artifact_list.count() == 1
        assert window.debug_artifact_title_label.text() == "Dataframe"
        _process_ui_until(qapp, lambda: "2 columns" in window.debug_artifact_summary_label.text())
        assert "2 columns" in window.debug_artifact_summary_label.text()
        assert window.debug_artifact_source_label.text() == "Source: C:/input/docs_flat_1.xlsx"
        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert isinstance(table, QTableWidget)
        _process_ui_until(qapp, lambda: table.rowCount() == 2)
        header_labels = {table.horizontalHeaderItem(index).text() for index in range(table.columnCount())}
        assert header_labels == {"claim_id", "status"}
        header = table.horizontalHeader()
        metadata = getattr(header, "_header_metadata", [])
        assert metadata == [
            {"title": "claim_id", "dtype": "Int64", "filtered": False, "sort_marker": None},
            {"title": "status", "dtype": "String", "filtered": False, "sort_marker": None},
        ]

        window.clear_debug_artifacts_button.click()
        qapp.processEvents()

        assert window.debug_artifact_list.count() == 0
        assert artifact_path.exists() is False
    finally:
        _dispose_window(qapp, window)


def test_debug_view_ignores_json_only_artifacts(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Write-Target__2026-04-19T00-00-00Z__summary.json"
        artifact_path.write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Write Target",
                    },
                    "info": {"rows": 3},
                    "data": {"output_path": "C:/output/example.parquet", "row_count": 3},
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        assert window.debug_artifact_list.count() == 0
        assert window.debug_artifact_list.count() == 0
    finally:
        _dispose_window(qapp, window)


def test_debug_view_live_parquet_filters_update_preview_rows(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame(
            {
                "claim_id": [1001, 1002, 2001],
                "status": ["OPEN", "CLOSED", "OPEN"],
            }
        ).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        limit_spin = explorer.findChild(QSpinBox, "outputPreviewLimitSpin")
        assert table is not None
        assert limit_spin is not None
        assert limit_spin.maximum() == 500_000
        _process_ui_until(qapp, lambda: table.rowCount() == 3)
        assert table.rowCount() == 3

        explorer._open_filter_popup_for_index(0)
        qapp.processEvents()

        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is not None
        _process_ui_until(qapp, lambda: popup.findChild(QListWidget, "outputPreviewPopupList").count() > 0)
        search = popup.findChild(QLineEdit, "outputPreviewPopupSearch")
        select_all = popup.findChild(QPushButton, "outputPreviewSelectAllButton")
        assert search is not None
        assert select_all is not None
        search.setText("100")
        qapp.processEvents()
        _process_ui_until(qapp, lambda: select_all.property("selectAllState") == Qt.CheckState.Checked.value)
        QTest.mouseClick(select_all, Qt.MouseButton.LeftButton)
        qapp.processEvents()
        QTest.mouseClick(select_all, Qt.MouseButton.LeftButton)
        qapp.processEvents()
        buttons = popup.findChildren(QPushButton, "filterPopupActionButton")
        next(button for button in buttons if button.text() == "Apply").click()
        _process_ui_until(qapp, lambda: table.rowCount() == 2)
        assert table.rowCount() == 2

        explorer._open_filter_popup_for_index(1)
        qapp.processEvents()
        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is not None
        _process_ui_until(qapp, lambda: popup.findChild(QListWidget, "outputPreviewPopupList").count() > 0)
        search = popup.findChild(QLineEdit, "outputPreviewPopupSearch")
        select_all = popup.findChild(QPushButton, "outputPreviewSelectAllButton")
        assert search is not None
        assert select_all is not None
        search.setText("closed")
        qapp.processEvents()
        _process_ui_until(qapp, lambda: select_all.property("selectAllState") == Qt.CheckState.Checked.value)
        QTest.mouseClick(select_all, Qt.MouseButton.LeftButton)
        qapp.processEvents()
        QTest.mouseClick(select_all, Qt.MouseButton.LeftButton)
        qapp.processEvents()
        buttons = popup.findChildren(QPushButton, "filterPopupActionButton")
        next(button for button in buttons if button.text() == "Apply").click()
        _process_ui_until(qapp, lambda: table.rowCount() == 1)
        assert table.rowCount() == 1
    finally:
        _dispose_window(qapp, window)


def test_debug_view_column_filter_reopens_with_other_filters_context(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame(
            {
                "claim_id": [1001, 2001, 3001],
                "status": ["OPEN", "OPEN", "CLOSED"],
            }
        ).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 3)

        explorer.apply_distinct_filter("claim_id", (1001,), (1001, 2001, 3001), complete_domain=True)
        _process_ui_until(qapp, lambda: table.rowCount() == 1)
        explorer.apply_distinct_filter("status", ("OPEN",), ("OPEN", "CLOSED"), complete_domain=True)
        _process_ui_until(qapp, lambda: table.rowCount() == 1)

        explorer._open_filter_popup_for_index(0)
        qapp.processEvents()

        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is not None
        _process_ui_until(qapp, lambda: popup.findChild(QListWidget, "outputPreviewPopupList").count() >= 2)
        values = [popup.findChild(QListWidget, "outputPreviewPopupList").item(i).text() for i in range(popup.findChild(QListWidget, "outputPreviewPopupList").count())]
        assert "1001" in values
        assert "2001" in values
    finally:
        _dispose_window(qapp, window)


def test_debug_view_column_filter_header_click_toggles_popup(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame({"claim_id": [1001, 1002], "status": ["OPEN", "CLOSED"]}).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 2)

        header = table.horizontalHeader()
        header_rect = header.sectionRect(0)
        QTest.mouseClick(header.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, header_rect.center())
        qapp.processEvents()
        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is not None and popup.isVisible()

        QTest.mouseClick(header.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, header_rect.center())
        qapp.processEvents()
        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is None or popup.isVisible() is False
    finally:
        _dispose_window(qapp, window)


def test_debug_view_header_resize_handle_does_not_open_filter_popup(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame({"claim_id": [1001, 1002], "status": ["OPEN", "CLOSED"]}).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 2)

        header = table.horizontalHeader()
        right_edge = header.sectionViewportPosition(0) + header.sectionSize(0) - 1
        resize_point = QPoint(right_edge, max(1, header.height() // 2))
        QTest.mouseClick(header.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, resize_point)
        qapp.processEvents()

        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is None or popup.isVisible() is False
    finally:
        _dispose_window(qapp, window)


def test_debug_view_column_filter_popup_supports_multi_column_sort(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame(
            {
                "workflow": ["B", "A", "B", "A"],
                "claim_id": [2, 2, 1, 1],
                "status": ["open", "open", "closed", "closed"],
            }
        ).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 4)

        header = table.horizontalHeader()
        workflow_header_pos = header.sectionViewportPosition(0)
        workflow_header_center = header.viewport().rect().topLeft() + QPoint(
            workflow_header_pos + max(1, header.sectionSize(0) // 2),
            max(1, header.height() // 2),
        )
        QTest.mouseClick(header.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, workflow_header_center)
        qapp.processEvents()
        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is not None and popup.isVisible()
        sort_button = popup.findChild(QPushButton, "outputPreviewSortAscendingButton")
        assert sort_button is not None
        QTest.mouseClick(sort_button, Qt.MouseButton.LeftButton)
        _process_ui_until(
            qapp,
            lambda: table.isEnabled()
            and table.item(0, 0) is not None
            and table.item(1, 0) is not None
            and explorer._sort_columns == [("workflow", False)],
        )

        explorer._open_filter_popup_for_index(1)
        qapp.processEvents()
        popup = explorer._filter_popup
        assert popup is not None and popup.isVisible()
        sort_button = popup.findChild(QPushButton, "outputPreviewSortAscendingButton")
        assert sort_button is not None
        assert sort_button.toolTip() == "Then sort ascending"
        QTest.mouseClick(sort_button, Qt.MouseButton.LeftButton)
        _process_ui_until(
            qapp,
            lambda: table.item(0, 0) is not None
            and table.item(0, 1) is not None
            and table.item(1, 0) is not None
            and table.item(1, 1) is not None
            and table.item(2, 0) is not None
            and table.item(2, 1) is not None
            and table.item(3, 0) is not None
            and table.item(3, 1) is not None
            and explorer._sort_columns == [("workflow", False), ("claim_id", False)]
            and [(table.item(row, 0).text(), table.item(row, 1).text()) for row in range(4)]
            == [("A", "1"), ("A", "2"), ("B", "1"), ("B", "2")],
        )
    finally:
        _dispose_window(qapp, window)


def test_debug_view_column_filter_popup_clicking_active_sort_clears_and_renumbers(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame(
            {
                "workflow": ["B", "A", "B", "A"],
                "claim_id": [2, 2, 1, 1],
                "status": ["open", "open", "closed", "closed"],
            }
        ).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 4)

        header = table.horizontalHeader()
        for column_index in (0, 1):
            header_pos = header.sectionViewportPosition(column_index)
            header_center = header.viewport().rect().topLeft() + QPoint(
                header_pos + max(1, header.sectionSize(column_index) // 2),
                max(1, header.height() // 2),
            )
            QTest.mouseClick(header.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, header_center)
            qapp.processEvents()
            popup = explorer._filter_popup
            assert popup is not None and popup.isVisible()
            sort_button = popup.findChild(QPushButton, "outputPreviewSortAscendingButton")
            assert sort_button is not None
            QTest.mouseClick(sort_button, Qt.MouseButton.LeftButton)
            qapp.processEvents()

        _process_ui_until(
            qapp,
            lambda: explorer._sort_columns == [("workflow", False), ("claim_id", False)],
        )

        workflow_header_pos = header.sectionViewportPosition(0)
        workflow_header_center = header.viewport().rect().topLeft() + QPoint(
            workflow_header_pos + max(1, header.sectionSize(0) // 2),
            max(1, header.height() // 2),
        )
        QTest.mouseClick(header.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, workflow_header_center)
        qapp.processEvents()
        popup = explorer._filter_popup
        assert popup is not None and popup.isVisible()
        sort_button = popup.findChild(QPushButton, "outputPreviewSortAscendingButton")
        assert sort_button is not None
        assert sort_button.property("sortActive") is True
        assert sort_button.toolTip() == "Clear ascending sort"
        QTest.mouseClick(sort_button, Qt.MouseButton.LeftButton)

        _process_ui_until(
            qapp,
            lambda: explorer._sort_columns == [("claim_id", False)],
        )
    finally:
        _dispose_window(qapp, window)


def test_debug_view_search_subset_apply_filters_single_matching_value(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame(
            {
                "workflow": ["Appeals", "Enrollment", "Appeals"],
                "claim_id": [1001, 1002, 1003],
            }
        ).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 3)

        explorer._open_filter_popup_for_index(0)
        qapp.processEvents()

        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is not None
        search = popup.findChild(QLineEdit, "outputPreviewPopupSearch")
        values_list = popup.findChild(QListWidget, "outputPreviewPopupList")
        assert search is not None
        assert values_list is not None

        search.setText("Enroll")
        qapp.processEvents()
        _process_ui_until(qapp, lambda: values_list.count() == 1 and values_list.item(0).text() == "Enrollment")

        buttons = popup.findChildren(QPushButton, "filterPopupActionButton")
        next(button for button in buttons if button.text() == "Apply").click()
        _process_ui_until(qapp, lambda: table.rowCount() == 1)

        assert table.rowCount() == 1
        assert table.item(0, 0).text() == "Enrollment"
    finally:
        _dispose_window(qapp, window)


def test_debug_view_filter_popup_select_all_checkbox_tracks_visible_values(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame(
            {
                "workflow": ["Appeals", "Enrollment", "Appeals"],
                "claim_id": [1001, 1002, 1003],
            }
        ).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 3)

        explorer._open_filter_popup_for_index(0)
        qapp.processEvents()

        popup = explorer.findChild(QWidget, "outputPreviewFilterPopup")
        assert popup is not None
        values_list = popup.findChild(QListWidget, "outputPreviewPopupList")
        select_all = popup.findChild(QPushButton, "outputPreviewSelectAllButton")
        search = popup.findChild(QLineEdit, "outputPreviewPopupSearch")
        assert values_list is not None
        assert select_all is not None
        assert search is not None

        _process_ui_until(qapp, lambda: values_list.count() == 2)
        assert select_all.property("selectAllState") == Qt.CheckState.Checked.value

        QTest.mouseClick(select_all, Qt.MouseButton.LeftButton)
        qapp.processEvents()
        assert select_all.property("selectAllState") == Qt.CheckState.Unchecked.value
        assert all(values_list.item(i).checkState() == Qt.CheckState.Unchecked for i in range(values_list.count()))

        values_list.item(0).setCheckState(Qt.CheckState.Checked)
        qapp.processEvents()
        assert select_all.property("selectAllState") == Qt.CheckState.PartiallyChecked.value

        search.setText("Enroll")
        qapp.processEvents()
        _process_ui_until(qapp, lambda: values_list.count() == 1 and values_list.item(0).text() == "Enrollment")

        assert select_all.property("selectAllState") == Qt.CheckState.Unchecked.value
        QTest.mouseClick(select_all, Qt.MouseButton.LeftButton)
        qapp.processEvents()
        assert values_list.item(0).checkState() == Qt.CheckState.Checked
        assert select_all.property("selectAllState") == Qt.CheckState.Checked.value
    finally:
        _dispose_window(qapp, window)


def test_debug_dataframe_table_copies_selected_cells_to_clipboard(qapp):
    window = _make_window()
    try:
        debug_dir = window.workspace_paths.runtime_state_dir / "debug_artifacts"
        debug_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = debug_dir / "example_manual__Read-Excel__2026-04-19T00-00-00Z__artifact.parquet"
        pl.DataFrame({"claim_id": [1001, 1002], "status": ["OPEN", "CLOSED"]}).write_parquet(artifact_path)
        artifact_path.with_suffix(".json").write_text(
            json.dumps(
                {
                    "debug": {
                        "workspace_id": window.workspace_paths.workspace_id,
                        "flow_name": "example_manual",
                        "step_name": "Read Excel",
                        "artifact_kind": "dataframe",
                        "artifact_path": str(artifact_path),
                        "saved_at_utc": "2026-04-19T00:00:00+00:00",
                        "display_name": "example_manual / Read Excel / 2026-04-19T00-00-00Z",
                    }
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        window.debug_button.click()
        qapp.processEvents()

        explorer = window.debug_preview_layout.itemAt(0).widget()
        table = explorer.findChild(QTableWidget, "outputPreviewTable")
        assert table is not None
        _process_ui_until(qapp, lambda: table.rowCount() == 2)

        table.setFocus()
        table.setCurrentCell(0, 0)
        table.clearSelection()
        table.item(0, 0).setSelected(True)
        table.item(0, 1).setSelected(True)
        QTest.keyClick(table, Qt.Key.Key_C, Qt.KeyboardModifier.ControlModifier)

        assert QApplication.clipboard().text() == "1001\tOPEN"
    finally:
        _dispose_window(qapp, window)


def test_reset_flow_button_calls_persistent_reset_path(qapp, monkeypatch):
    reset_service = _FakeResetService()
    window = _make_window(command_service=_command_service_for_test(reset_service=reset_service))
    rebuild_calls = _attach_call_recorder(window, "_rebuild_runtime_snapshot")
    try:
        window._clear_logs()
        _process_ui_until(qapp, lambda: len(reset_service.flow_resets) == 1)
        _process_ui_until(qapp, lambda: len(rebuild_calls) >= 1)

        assert reset_service.flow_resets == [(window.workspace_paths, "poller")]
        assert len(rebuild_calls) >= 1
    finally:
        _dispose_window(qapp, window)


def test_force_shutdown_daemon_targets_selected_settings_workspace_without_rebinding(qapp, tmp_path, monkeypatch):
    del monkeypatch
    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)
    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    force_shutdown_calls: list[dict[str, object]] = []
    window = _make_window(
        force_shutdown_func=lambda paths, timeout=0.5: force_shutdown_calls.append(
            {"workspace_id": paths.workspace_id, "workspace": paths.workspace_root, "timeout": timeout}
        ),
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    try:
        target_index = window.workspace_settings_selector.findData("docs2")
        assert target_index >= 0
        window.workspace_settings_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        window._force_shutdown_daemon()
        _process_ui_until(
            qapp,
            lambda: len(force_shutdown_calls) == 1
            and "force_shutdown_daemon" not in window._pending_control_actions,
        )

        assert force_shutdown_calls == [
            {"workspace_id": "docs2", "workspace": docs2_root, "timeout": 0.5}
        ]
        assert window.workspace_paths.workspace_id == "docs"
    finally:
        _dispose_window(qapp, window)


def test_refresh_action_buttons_prefers_empty_daemon_live_truth_over_stale_manual_session(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=10,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window._select_flow("manual_review")

        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Run Once"
        assert window.flow_run_button.property("flowRunState") == "run"
        assert window.flow_run_button.isEnabled() is True
    finally:
        _dispose_window(qapp, window)


def test_run_selected_flow_prefers_empty_daemon_live_truth_over_stale_manual_session(qapp, monkeypatch):
    del monkeypatch
    control_application = _FakeControlApplication()
    control_application.run_selected_flow_result = type(
        "Result",
        (),
        {
            "requested": True,
            "sync_after": False,
            "status_text": None,
            "error_text": None,
        },
    )()
    window = _make_window(command_service=_command_service_for_test(control_application=control_application))
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=10,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window._select_flow("manual_review")

        window._run_selected_flow()
        _process_ui_until(qapp, lambda: len(control_application.run_selected_flow_calls) == 1)

        assert control_application.stop_pipeline_calls == []
        assert control_application.run_selected_flow_calls[0]["selected_flow_name"] == "manual_review"
    finally:
        _dispose_window(qapp, window)


def test_reset_flow_button_clears_selected_flow_logs_before_rebuild(qapp, monkeypatch):
    del monkeypatch
    reset_service = _FakeResetService()
    window = _make_window(command_service=_command_service_for_test(reset_service=reset_service))
    try:
        window.log_store.append_entry(
            FlowLogEntry(
                line="poller success",
                kind="flow",
                flow_name="poller",
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name="poller",
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                ),
                persisted_id=10,
            )
        )
        window.log_store.append_entry(
            FlowLogEntry(
                line="manual review success",
                kind="flow",
                flow_name="manual_review",
                event=RuntimeStepEvent(
                    run_id="run-2",
                    flow_name="manual_review",
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                ),
                persisted_id=11,
            )
        )
        window.step_output_index = window.step_output_index.with_flow_outputs("poller", {"Write Parquet": Path("C:/tmp/out.parquet")})

        window._clear_logs()
        _process_ui_until(
            qapp,
            lambda: (
                len(reset_service.flow_resets) == 1
                and window.log_store.entries_for_flow("poller") == ()
                and window.step_output_index.outputs_for("poller").outputs == {}
            ),
        )

        assert window.log_store.entries_for_flow("poller") == ()
        assert len(window.log_store.entries_for_flow("manual_review")) == 1
        assert window.step_output_index.outputs_for("poller").outputs == {}
    finally:
        _dispose_window(qapp, window)


def test_refresh_action_buttons_prefers_empty_daemon_live_truth_over_stale_active_flow_state(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("manual_review")
        window.flow_states["manual_review"] = "stopping flow"
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=12,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )

        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Run Once"
        assert window.flow_run_button.isEnabled() is True
    finally:
        _dispose_window(qapp, window)


def test_refresh_action_buttons_does_not_fallback_to_stale_session_without_daemon_live_truth(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("manual_review")
        window.runtime_session = replace(
            window.runtime_session,
            runtime_active=True,
            runtime_stopping=True,
        ).with_manual_runs_map({"Manual": "manual_review"})
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=7,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=False, transport="disconnected"),
            flows={},
            active_runs={},
        )

        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Run Once"
        assert window.flow_run_button.isEnabled() is True
        assert window.engine_button.text() == "Start Engine"
        assert window.engine_button.isEnabled() is True
    finally:
        _dispose_window(qapp, window)


def test_reset_flow_button_is_disabled_while_manual_run_is_active(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window._select_flow("poller")
        window._refresh_action_buttons()

        assert window.clear_flow_log_button.isEnabled() is False
    finally:
        _dispose_window(qapp, window)


def test_reset_flow_button_shows_resetting_while_request_is_pending(qapp, monkeypatch):
    del monkeypatch
    reset_service = _FakeResetService()
    original_reset_flow = reset_service.reset_flow

    def _delayed_reset_flow(*, paths, runtime_cache_ledger, flow_name):
        QTest.qWait(50)
        return original_reset_flow(paths=paths, runtime_cache_ledger=runtime_cache_ledger, flow_name=flow_name)

    reset_service.reset_flow = _delayed_reset_flow
    window = _make_window(command_service=_command_service_for_test(reset_service=reset_service))
    try:
        window._clear_logs()
        _process_ui_until(qapp, lambda: "reset_flow" in window._pending_control_actions)

        assert window.clear_flow_log_button.text() == "Resetting..."
        assert window.clear_flow_log_button.isEnabled() is False

        _process_ui_until(qapp, lambda: "reset_flow" not in window._pending_control_actions)
        assert window.clear_flow_log_button.text() == "Reset Flow"
    finally:
        _dispose_window(qapp, window)


def test_reset_workspace_button_calls_reset_service_and_rebinds(qapp, monkeypatch):
    del monkeypatch
    reset_service = _FakeResetService()
    window = _make_window(command_service=_command_service_for_test(reset_service=reset_service))
    rebind_calls = _attach_call_recorder(window, "_rebind_workspace_context")
    try:
        window._reset_workspace()
        _process_ui_until(
            qapp,
            lambda: len(reset_service.workspace_resets) == 1
            and "reset_workspace" not in window._pending_control_actions,
        )

        assert reset_service.workspace_resets == [window.workspace_paths]
        assert rebind_calls == [ ((), {"workspace_id": window.workspace_paths.workspace_id}) ]
    finally:
        _dispose_window(qapp, window)


def test_reset_workspace_button_allows_idle_live_daemon(qapp, monkeypatch):
    del monkeypatch
    reset_service = _FakeResetService()
    window = _make_window(
        command_service=_command_service_for_test(reset_service=reset_service),
        snapshot=WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="daemon",
        ),
    )
    rebind_calls = _attach_call_recorder(window, "_rebind_workspace_context")
    try:
        window._reset_workspace()
        _process_ui_until(
            qapp,
            lambda: len(reset_service.workspace_resets) == 1
            and "reset_workspace" not in window._pending_control_actions,
        )

        assert reset_service.workspace_resets == [window.workspace_paths]
        assert rebind_calls == [((), {"workspace_id": window.workspace_paths.workspace_id})]
    finally:
        _dispose_window(qapp, window)


def test_reset_workspace_targets_selected_settings_workspace_without_rebinding(qapp, tmp_path, monkeypatch):
    del monkeypatch
    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)
    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    reset_service = _FakeResetService()
    window = _make_window(
        command_service=_command_service_for_test(reset_service=reset_service),
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    rebind_calls = _attach_call_recorder(window, "_rebind_workspace_context")
    try:
        target_index = window.workspace_settings_selector.findData("docs2")
        assert target_index >= 0
        window.workspace_settings_selector.setCurrentIndex(target_index)
        qapp.processEvents()

        window._reset_workspace()
        _process_ui_until(
            qapp,
            lambda: len(reset_service.workspace_resets) == 1
            and "reset_workspace" not in window._pending_control_actions,
        )

        assert reset_service.workspace_resets == [resolve_workspace_paths(workspace_root=docs2_root, workspace_id="docs2")]
        assert rebind_calls == []
        assert window.workspace_paths.workspace_id == "docs"
    finally:
        _dispose_window(qapp, window)


def test_rebind_workspace_context_recreates_daemon_subscription_and_clears_log_caches(qapp, monkeypatch, tmp_path):
    del monkeypatch
    workspace_collection_root = tmp_path / "workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)
    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

    window = _make_window(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    try:
        original_subscription = window.daemon_subscription
        window._pending_daemon_update_batch = DaemonUpdateBatch(snapshot=window._daemon_manager.sync(), updates=())
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.operation_tracker = OperationSessionState.empty().reset_flow("manual_review", ("Build Report",))
        window.flow_states = {"manual_review": "stopping flow"}
        window.workspace_provision_status_label.setText("Provisioned docs")
        window.force_shutdown_daemon_status_label.setText("Force stop failed")
        window.reset_workspace_status_label.setText("Workspace reset failed")
        window._last_log_view_flow_name = "poller"
        window._last_log_view_run_keys = (("poller", "run-1"),)
        window._last_log_view_signature = ((("poller", "run-1"), "x", "y", "z", None),)
        window._selected_flow_has_logs = True
        window._selected_flow_has_logs_flow_name = "poller"

        window._rebind_workspace_context(workspace_id="docs2")

        assert original_subscription.stop_event.is_set() is True
        assert window.daemon_subscription is not original_subscription
        assert window.workspace_paths.workspace_id == "docs2"
        assert window.runtime_session == RuntimeSessionState.empty()
        assert window.operation_tracker.row_state("manual_review", "Build Report").status == "idle"
        assert window.flow_states.get("manual_review") != "stopping flow"
        assert window._pending_daemon_update_batch is None
        assert "Provisioned docs" not in window.workspace_provision_status_label.text()
        assert "Force stop failed" not in window.force_shutdown_daemon_status_label.text()
        assert "Workspace reset failed" not in window.reset_workspace_status_label.text()
        assert window._last_log_view_run_keys != (("poller", "run-1"),)
        assert window._last_log_view_signature != ((("poller", "run-1"), "x", "y", "z", None),)
        assert window._selected_flow_has_logs is False or window._selected_flow_has_logs_flow_name != "poller"
        assert window._pending_control_actions == set()
        assert window._pending_control_action_tokens == {}
    finally:
        _dispose_window(qapp, window)


def test_rebind_workspace_context_does_not_force_shutdown_old_workspace_daemon(qapp, monkeypatch, tmp_path):
    del monkeypatch
    workspace_collection_root = tmp_path / "workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)
    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )
    shutdown_calls: list[dict[str, object]] = []
    remove_calls: list[tuple[object, str]] = []

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
        return resolve_workspace_paths(workspace_root=target, workspace_id=target_id)

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
        request_func=lambda paths, payload, timeout=0.0: shutdown_calls.append({"paths": paths, "payload": payload, "timeout": timeout}) or {"ok": True},
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None: discovered,
        resolve_workspace_paths_func=lambda workspace_id=None, **kwargs: _resolve(workspace_id),
    )
    try:
        original_remove_client_session = window.runtime_binding_service.remove_client_session

        def _record_remove_client_session(binding, client_id):
            remove_calls.append((binding, client_id))
            return original_remove_client_session(binding, client_id)

        window.runtime_binding_service.remove_client_session = _record_remove_client_session
        window._rebind_workspace_context(workspace_id="docs2")

        assert shutdown_calls == []
        assert remove_calls == []
    finally:
        _dispose_window(qapp, window)


def test_apply_daemon_update_batch_ignores_stale_workspace_token(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        current_token = window._workspace_binding_token()
        stale_token = (current_token[0] - 1, current_token[1])
        window.runtime_session = window.runtime_session.with_runtime_flags(active=False, stopping=False)
        batch_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="daemon",
            active_engine_flow_names=("poller",),
        )
        payload = window._daemon_batch_payload(
            DaemonUpdateBatch(
                snapshot=batch_snapshot,
                updates=(DaemonLaneUpdate("engine", flow_names=("poller",)),),
            ),
            token=stale_token,
        )

        window._apply_daemon_update_batch(payload)

        assert window.runtime_session.runtime_active is False
    finally:
        _dispose_window(qapp, window)


def test_finish_control_action_ignores_stale_workspace_token(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        current_token = window._workspace_binding_token()
        stale_token = (current_token[0] - 1, current_token[1])
        window._pending_control_actions.add("request_control")
        window._pending_control_action_tokens["request_control"] = current_token
        window.runtime_session = replace(window.runtime_session, workspace_owned=True, leased_by_machine_id=None)

        window._finish_control_action(
            "request_control",
            window._control_action_payload(
                type("Result", (), {"error_text": None, "status_text": "stale", "ensure_daemon_started": False, "sync_after": False})(),
                token=stale_token,
            ),
        )

        assert "request_control" in window._pending_control_actions
    finally:
        _dispose_window(qapp, window)


def test_reset_workspace_button_is_disabled_while_active_work_is_running(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window._refresh_workspace_visibility_panel()

        assert window.reset_workspace_button.isEnabled() is False
    finally:
        _dispose_window(qapp, window)


def test_force_shutdown_button_shows_pending_label_while_request_is_in_flight(qapp, monkeypatch):
    del monkeypatch
    force_shutdown_calls: list[dict[str, object]] = []

    def _delayed_force_shutdown(paths, timeout=0.5):
        QTest.qWait(50)
        force_shutdown_calls.append({"workspace": paths.workspace_root, "timeout": timeout})

    window = _make_window(force_shutdown_func=_delayed_force_shutdown)
    try:
        window._force_shutdown_daemon()
        _process_ui_until(qapp, lambda: "force_shutdown_daemon" in window._pending_control_actions)

        assert window.force_shutdown_daemon_button.text() == "Force Stopping..."
        assert window.force_shutdown_daemon_button.isEnabled() is False

        _process_ui_until(qapp, lambda: "force_shutdown_daemon" not in window._pending_control_actions)
        assert force_shutdown_calls == [{"workspace": window.workspace_paths.workspace_root, "timeout": 0.5}]
        assert window.force_shutdown_daemon_button.text() == "Force Stop Daemon"
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
    workspace_collection_root = tmp_path / "docs_workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)

    discovered = (
        DiscoveredWorkspace(workspace_id="docs", workspace_root=docs_root),
        DiscoveredWorkspace(workspace_id="docs2", workspace_root=docs2_root),
    )

    def _resolve(workspace_id=None):
        target = docs_root if workspace_id in (None, "docs") else docs2_root
        target_id = "docs" if workspace_id in (None, "docs") else "docs2"
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

        target_index = window.workspace_selector.findData("docs2")
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
        _process_ui_until(qapp, lambda: window.flow_states.get("poller") == "stopping runtime")

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
                    workspace_id=window.workspace_paths.workspace_id,
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
            line="run=abc flow=docs_summary step=Collect Claim Files source=None status=started",
            kind="runtime",
            flow_name="docs_summary",
            event=RuntimeStepEvent(
                run_id="abc",
                flow_name="docs_summary",
                step_name="Collect Claim Files",
                source_label="-",
                status="started",
            ),
        )

        rendered = window._format_raw_log_message(entry)

        assert "docs_summary &gt; &gt;" not in rendered
        assert rendered == "docs_summary &gt; <b>Collect Claim Files</b> - <i>started</i>"
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
                    workspace_id=window.workspace_paths.workspace_id,
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


def test_refresh_sidebar_state_views_skips_rebuild_when_no_flow_states_changed(qapp, monkeypatch):
    window = _make_window()
    populate_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def _record_populate(*args, **kwargs):
        populate_calls.append((args, kwargs))

    monkeypatch.setattr(window, "_populate_flow_tree", _record_populate)

    try:
        window._refresh_sidebar_state_views(set())

        assert populate_calls == []
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_skips_row_rebuild_when_visible_runs_are_unchanged(qapp, monkeypatch):
    window = _make_window()
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        window.log_store.append_entry(
            FlowLogEntry(
                line="run-1",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                    elapsed_seconds=0.3,
                ),
            )
        )

        add_calls = 0
        from data_engine.ui.gui.presenters import logs as log_presenter

        original_add = log_presenter.add_log_run_item

        def counting_add(window_arg, run_group):
            nonlocal add_calls
            add_calls += 1
            return original_add(window_arg, run_group)

        monkeypatch.setattr(log_presenter, "add_log_run_item", counting_add)

        window._refresh_log_view(force_scroll_to_bottom=True)
        first_item = window.log_view.item(0)

        window._refresh_log_view(force_scroll_to_bottom=True)
        second_item = window.log_view.item(0)

        assert add_calls == 1
        assert first_item is second_item
        assert window.log_view.count() == 1
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_reloads_ledger_runs_when_log_store_is_unchanged(qapp, monkeypatch):
    window = _make_window()
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        started_entry = FlowLogEntry(
            line="run=run-1 flow=poller source=docs.xlsx status=started",
            kind="flow",
            flow_name=flow_name,
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name=flow_name,
                step_name=None,
                source_label="docs.xlsx",
                status="started",
                elapsed_seconds=None,
            ),
        )
        finished_entry = FlowLogEntry(
            line="run=run-1 flow=poller source=docs.xlsx status=success elapsed=8.0",
            kind="flow",
            flow_name=flow_name,
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name=flow_name,
                step_name=None,
                source_label="docs.xlsx",
                status="success",
                elapsed_seconds=8.0,
            ),
        )
        started_group = FlowRunState(
            key=(flow_name, "run-1"),
            display_label="2026-04-20 09:00:00 AM",
            source_label="docs.xlsx",
            status="started",
            elapsed_seconds=None,
            summary_entry=started_entry,
            steps=(),
            entries=(started_entry,),
        )
        finished_group = FlowRunState(
            key=(flow_name, "run-1"),
            display_label="2026-04-20 09:00:00 AM",
            source_label="docs.xlsx",
            status="success",
            elapsed_seconds=8.0,
            summary_entry=finished_entry,
            steps=(),
            entries=(finished_entry,),
        )
        ledger_results = [started_group, finished_group]

        def list_flow_runs_from_ledger(_ledger, *, flow_name=None, limit=50):
            del _ledger, limit
            if flow_name != "poller":
                return ()
            result = ledger_results.pop(0) if ledger_results else finished_group
            return (result,)

        monkeypatch.setattr(
            window.history_query_service,
            "list_flow_runs_from_ledger",
            list_flow_runs_from_ledger,
        )

        window._refresh_log_view(force_scroll_to_bottom=True)
        item = window.log_view.item(0)
        first_group = window.log_view.run_group(item)

        window._refresh_log_view(force_scroll_to_bottom=True)
        second_group = window.log_view.run_group(item)

        assert first_group is not None and first_group.status == "started"
        assert second_group is not None and second_group.status == "success"
        assert second_group.elapsed_seconds == 8.0
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_reloads_legacy_log_runs_without_cached_groups(qapp, monkeypatch):
    window = _make_window()
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        started_entry = FlowLogEntry(
            line="run=run-1 flow=poller source=docs.xlsx status=started",
            kind="flow",
            flow_name=flow_name,
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name=flow_name,
                step_name=None,
                source_label="docs.xlsx",
                status="started",
                elapsed_seconds=None,
            ),
        )
        finished_entry = FlowLogEntry(
            line="run=run-1 flow=poller source=docs.xlsx status=success elapsed=8.0",
            kind="flow",
            flow_name=flow_name,
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name=flow_name,
                step_name=None,
                source_label="docs.xlsx",
                status="success",
                elapsed_seconds=8.0,
            ),
        )
        started_group = FlowRunState(
            key=(flow_name, "run-1"),
            display_label="2026-04-20 09:00:00 AM",
            source_label="docs.xlsx",
            status="started",
            elapsed_seconds=None,
            summary_entry=started_entry,
            steps=(),
            entries=(started_entry,),
        )
        finished_group = FlowRunState(
            key=(flow_name, "run-1"),
            display_label="2026-04-20 09:00:00 AM",
            source_label="docs.xlsx",
            status="success",
            elapsed_seconds=8.0,
            summary_entry=finished_entry,
            steps=(),
            entries=(finished_entry,),
        )
        log_results = [started_group, finished_group]
        monkeypatch.setattr(
            window.history_query_service,
            "list_flow_runs_from_ledger",
            lambda _ledger, *, flow_name=None, limit=50: (),
        )

        def list_flow_runs(_store, *, flow_name=None):
            if flow_name != "poller":
                return ()
            result = log_results.pop(0) if log_results else finished_group
            return (result,)

        monkeypatch.setattr(window.history_query_service, "list_flow_runs", list_flow_runs)

        window._refresh_log_view(force_scroll_to_bottom=True)
        item = window.log_view.item(0)
        first_group = window.log_view.run_group(item)

        window._refresh_log_view(force_scroll_to_bottom=True)
        second_group = window.log_view.run_group(item)

        assert first_group is not None and first_group.status == "started"
        assert second_group is not None and second_group.status == "success"
        assert second_group.elapsed_seconds == 8.0
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_refreshes_runtime_cache_before_querying_ledger_runs(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name

        # Prime the runtime-IO read cache with an empty run list before an external daemon write.
        assert window.runtime_binding.runtime_cache_ledger.runs.list(flow_name=flow_name) == ()

        direct_ledger = RuntimeCacheLedger(window.workspace_paths.runtime_cache_db_path)
        try:
            started_at = "2026-04-20T09:00:00+00:00"
            direct_ledger.execution_state.record_run_started(
                run_id="run-1",
                flow_name=flow_name,
                group_name="Imports",
                source_path="docs.xlsx",
                started_at_utc=started_at,
            )
            direct_ledger.execution_state.record_run_finished(
                run_id="run-1",
                status="success",
                finished_at_utc="2026-04-20T09:00:08+00:00",
            )
        finally:
            direct_ledger.close()

        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.log_view.count() == 1
        item = window.log_view.item(0)
        run_group = window.log_view.run_group(item)
        assert run_group is not None
        assert run_group.key == (flow_name, "run-1")
        assert run_group.status == "success"
        assert run_group.source_label == "docs.xlsx"
    finally:
        _dispose_window(qapp, window)


def test_run_log_preview_collapses_step_started_and_finished_rows(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        entries = (
            FlowLogEntry(
                line="run=abc flow=docs_summary step=Collect Claim Files source=input.xlsx status=started",
                kind="runtime",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id="abc",
                    flow_name="docs_summary",
                    step_name="Collect Claim Files",
                    source_label="input.xlsx",
                    status="started",
                ),
            ),
            FlowLogEntry(
                line="run=abc flow=docs_summary step=Collect Claim Files source=input.xlsx status=success elapsed=0.4",
                kind="runtime",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id="abc",
                    flow_name="docs_summary",
                    step_name="Collect Claim Files",
                    source_label="input.xlsx",
                    status="success",
                    elapsed_seconds=0.4,
                ),
            ),
        )
        run_group = FlowRunState.group_entries(entries)[0]

        window._show_run_log_preview(run_group)

        assert window.run_log_preview_dialog is not None
        log_list = window.run_log_preview_dialog.findChild(QListWidget, "runLogList")
        assert log_list is not None
        assert log_list.count() == 1
        item = log_list.item(0)
        row_widget = log_list.itemWidget(item)
        assert row_widget is not None
        message = row_widget.findChild(QLabel, "rawLogMessage")
        duration = row_widget.findChild(QLabel, "logDuration")
        assert message is not None
        assert duration is not None
        assert "Collect Claim Files" in message.text()
        assert "success" in message.text()
        assert duration.text() == "400ms"
        assert duration.alignment() & Qt.AlignmentFlag.AlignRight
    finally:
        if window.run_log_preview_dialog is not None:
            window.run_log_preview_dialog.close()
        _dispose_window(qapp, window)


def test_run_log_preview_rows_are_not_created_as_top_level_windows(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        entries = (
            FlowLogEntry(
                line="run=abc flow=docs_summary step=Collect Claim Files source=input.xlsx status=success elapsed=0.4",
                kind="runtime",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id="abc",
                    flow_name="docs_summary",
                    step_name="Collect Claim Files",
                    source_label="input.xlsx",
                    status="success",
                    elapsed_seconds=0.4,
                ),
            ),
        )
        run_group = FlowRunState.group_entries(entries)[0]

        window._show_run_log_preview(run_group)

        assert window.run_log_preview_dialog is not None
        log_list = window.run_log_preview_dialog.findChild(QListWidget, "runLogList")
        assert log_list is not None
        assert log_list.count() == 1
        row_widget = log_list.itemWidget(log_list.item(0))
        assert row_widget is not None
        assert row_widget.parent() is log_list.viewport()
        assert row_widget.isWindow() is False
        assert all(child.isWindow() is False for child in row_widget.findChildren(QWidget))
    finally:
        if window.run_log_preview_dialog is not None:
            window.run_log_preview_dialog.close()
        _dispose_window(qapp, window)


def test_run_log_preview_keeps_unfinished_started_step_rows_visible(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        entries = (
            FlowLogEntry(
                line="run=abc flow=docs_summary step=Collect Claim Files source=input.xlsx status=started",
                kind="runtime",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id="abc",
                    flow_name="docs_summary",
                    step_name="Collect Claim Files",
                    source_label="input.xlsx",
                    status="started",
                ),
            ),
        )
        run_group = FlowRunState.group_entries(entries)[0]

        window._show_run_log_preview(run_group)

        assert window.run_log_preview_dialog is not None
        log_list = window.run_log_preview_dialog.findChild(QListWidget, "runLogList")
        assert log_list is not None
        assert log_list.count() == 1
        row_widget = log_list.itemWidget(log_list.item(0))
        assert row_widget is not None
        duration = row_widget.findChild(QLabel, "logDuration")
        assert duration is not None
        assert duration.isVisible() is False
    finally:
        if window.run_log_preview_dialog is not None:
            window.run_log_preview_dialog.close()
        _dispose_window(qapp, window)


def test_run_log_preview_omits_redundant_run_terminal_rows_when_step_rows_exist(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        entries = (
            FlowLogEntry(
                line="run=abc flow=docs_summary source=input.xlsx status=started",
                kind="flow",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id="abc",
                    flow_name="docs_summary",
                    step_name=None,
                    source_label="input.xlsx",
                    status="started",
                ),
            ),
            FlowLogEntry(
                line="run=abc flow=docs_summary step=Collect Claim Files source=input.xlsx status=success elapsed=0.4",
                kind="flow",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id="abc",
                    flow_name="docs_summary",
                    step_name="Collect Claim Files",
                    source_label="input.xlsx",
                    status="success",
                    elapsed_seconds=0.4,
                ),
            ),
            FlowLogEntry(
                line="run=abc flow=docs_summary source=input.xlsx status=failed elapsed=0.8",
                kind="flow",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id="abc",
                    flow_name="docs_summary",
                    step_name=None,
                    source_label="input.xlsx",
                    status="failed",
                    elapsed_seconds=0.8,
                ),
            ),
        )
        run_group = FlowRunState.group_entries(entries)[0]

        window._show_run_log_preview(run_group)

        assert window.run_log_preview_dialog is not None
        log_list = window.run_log_preview_dialog.findChild(QListWidget, "runLogList")
        assert log_list is not None
        assert log_list.count() == 2
        messages = []
        for index in range(log_list.count()):
            row_widget = log_list.itemWidget(log_list.item(index))
            assert row_widget is not None
            message = row_widget.findChild(QLabel, "rawLogMessage")
            assert message is not None
            messages.append(message.text())
        assert any("&gt; <i>started</i>" in message for message in messages)
        assert any("Collect Claim Files" in message and "success" in message for message in messages)
        assert not any("&gt; <i>failed</i>" in message for message in messages)
    finally:
        if window.run_log_preview_dialog is not None:
            window.run_log_preview_dialog.close()
        _dispose_window(qapp, window)


def test_run_log_preview_shows_full_source_path_in_header(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        source_path = r"C:\input\alternate\docs_flat_1.xlsx"
        window.runtime_binding.runtime_cache_ledger.execution_state.record_run_started(
            run_id="abc",
            flow_name="docs_summary",
            group_name="Docs",
            source_path=source_path,
            started_at_utc="2026-04-18T21:39:03+00:00",
        )
        entries = (
            FlowLogEntry(
                line="run=abc flow=docs_summary source=input.xlsx status=started",
                kind="flow",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id="abc",
                    flow_name="docs_summary",
                    step_name=None,
                    source_label="input.xlsx",
                    status="started",
                ),
            ),
        )
        run_group = FlowRunState.group_entries(entries)[0]

        window._show_run_log_preview(run_group)

        assert window.run_log_preview_dialog is not None
        path_label = window.run_log_preview_dialog.findChild(QLabel, "outputPreviewPath")
        assert path_label is not None
        assert path_label.text() == source_path
    finally:
        if window.run_log_preview_dialog is not None:
            window.run_log_preview_dialog.close()
        _dispose_window(qapp, window)


def test_run_log_preview_refreshes_runtime_cache_before_loading_source_path(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        source_path = r"C:\input\alternate\docs_flat_1.xlsx"
        run_id = "abc"
        # Prime the runtime-IO cache with a missing read, then write through a separate ledger handle.
        assert window.runtime_binding.runtime_cache_ledger.runs.get(run_id) is None
        direct_ledger = RuntimeCacheLedger(window.workspace_paths.runtime_cache_db_path)
        try:
            direct_ledger.execution_state.record_run_started(
                run_id=run_id,
                flow_name="docs_summary",
                group_name="Docs",
                source_path=source_path,
                started_at_utc="2026-04-18T21:39:03+00:00",
            )
        finally:
            direct_ledger.close()
        entries = (
            FlowLogEntry(
                line="run=abc flow=docs_summary source=input.xlsx status=started",
                kind="flow",
                flow_name="docs_summary",
                event=RuntimeStepEvent(
                    run_id=run_id,
                    flow_name="docs_summary",
                    step_name=None,
                    source_label="input.xlsx",
                    status="started",
                ),
            ),
        )
        run_group = FlowRunState.group_entries(entries)[0]

        window._show_run_log_preview(run_group)

        assert window.run_log_preview_dialog is not None
        path_label = window.run_log_preview_dialog.findChild(QLabel, "outputPreviewPath")
        assert path_label is not None
        assert path_label.text() == source_path
    finally:
        if window.run_log_preview_dialog is not None:
            window.run_log_preview_dialog.close()
        _dispose_window(qapp, window)


def test_show_run_error_details_refreshes_runtime_cache_before_loading_failed_step_text(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    capture = _attach_message_capture(window)
    try:
        run_id = "abc"
        step_name = "Write Parquet"
        # Prime the runtime-IO cache with no step rows, then write the failure through a separate ledger handle.
        assert window.runtime_binding.runtime_cache_ledger.step_outputs.list_for_run(run_id) == ()
        direct_ledger = RuntimeCacheLedger(window.workspace_paths.runtime_cache_db_path)
        try:
            direct_ledger.execution_state.record_run_started(
                run_id=run_id,
                flow_name="docs_summary",
                group_name="Docs",
                source_path=r"C:\input\alternate\docs_flat_1.xlsx",
                started_at_utc="2026-04-18T21:39:03+00:00",
            )
            step_run_id = direct_ledger.execution_state.record_step_started(
                run_id=run_id,
                flow_name="docs_summary",
                step_label=step_name,
                started_at_utc="2026-04-18T21:39:04+00:00",
            )
            direct_ledger.execution_state.record_step_finished(
                step_run_id=step_run_id,
                status="failed",
                finished_at_utc="2026-04-18T21:39:05+00:00",
                elapsed_ms=1000,
                error_text='RuntimeError: Intentional Example Mirror failure for Inspect-modal testing.',
            )
            direct_ledger.execution_state.record_run_finished(
                run_id=run_id,
                status="failed",
                finished_at_utc="2026-04-18T21:39:05+00:00",
                error_text='RuntimeError: Intentional Example Mirror failure for Inspect-modal testing.',
            )
        finally:
            direct_ledger.close()
        entry = FlowLogEntry(
            line="run=abc flow=docs_summary step=Write Parquet source=input.xlsx status=failed elapsed=1.0",
            kind="flow",
            flow_name="docs_summary",
            event=RuntimeStepEvent(
                run_id=run_id,
                flow_name="docs_summary",
                step_name=step_name,
                source_label="input.xlsx",
                status="failed",
                elapsed_seconds=1.0,
            ),
        )
        run_group = FlowRunState(
            key=("docs_summary", run_id),
            display_label="2026-04-18 09:39:03 PM",
            source_label="input.xlsx",
            status="failed",
            elapsed_seconds=1.0,
            summary_entry=entry,
            steps=(),
            entries=(entry,),
        )

        window._show_run_error_details(run_group, entry)

        assert capture.shown_messages == [
            (
                f"{step_name} Error",
                'RuntimeError: Intentional Example Mirror failure for Inspect-modal testing.',
                "error",
            )
        ]
    finally:
        _dispose_window(qapp, window)


class _QueuedLogSinkProbe:
    def append(
        self,
        *,
        level: str,
        message: str,
        created_at_utc: str,
        run_id: str | None = None,
        flow_name: str | None = None,
        step_label: str | None = None,
    ) -> None:
        del level, message, created_at_utc, run_id, flow_name, step_label


def test_runtime_log_emitter_scopes_live_queue_entries_to_workspace() -> None:
    queue: Queue[FlowLogEntry] = Queue()
    handler = QueueLogHandler(queue)
    logger = logging.getLogger("data_engine.runtime.execution.logging")
    logger.addHandler(handler)
    previous_level = logger.level
    logger.setLevel(logging.INFO)
    try:
        emitter = RuntimeLogEmitter(_QueuedLogSinkProbe(), workspace_id="docs2")
        emitter.log_flow_event(
            "run-1",
            "poller",
            Path("docs.xlsx"),
            status="success",
            elapsed=1.2,
        )
        entry = queue.get(timeout=1.0)
        assert entry.workspace_id == "docs2"
        assert entry.flow_name == "poller"
        assert entry.event is not None
        assert entry.event.status == "success"
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)


def test_finish_run_reloads_visible_run_history_from_empty_state(qapp, monkeypatch):
    window = _make_window()
    try:
        window._select_flow("manual_review")
        window._refresh_log_view(force_scroll_to_bottom=True)
        assert window.log_view.count() == 0

        reloaded = {"called": False}
        original_reload_logs = window.runtime_binding_service.reload_logs

        def _reload_logs(binding):
            reloaded["called"] = True
            flow_name = "manual_review"
            window.log_store.append_entry(
                FlowLogEntry(
                    line="manual review success",
                    kind="flow",
                    flow_name=flow_name,
                    event=RuntimeStepEvent(
                        run_id="run-1",
                        flow_name=flow_name,
                        step_name=None,
                        source_label="docs.xlsx",
                        status="success",
                        elapsed_seconds=1.2,
                    ),
                )
            )
            return original_reload_logs(binding)

        monkeypatch.setattr(window.runtime_binding_service, "reload_logs", _reload_logs)

        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.manual_flow_stop_events["Manual"] = threading.Event()
        window._finish_run("manual_review", [], None)

        assert reloaded["called"] is True
        assert window.log_view.count() == 1
        item = window.log_view.item(0)
        assert item is not None
        assert window.log_view.duration_text(item) == "1.2s"
        assert window.log_view.source_label(item) == "docs.xlsx"
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_updates_duration_when_live_row_finishes_in_place(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        window.log_store.append_entry(
            FlowLogEntry(
                line="run-1 started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="started",
                    elapsed_seconds=None,
                ),
            )
        )

        window._refresh_log_view(force_scroll_to_bottom=True)

        item = window.log_view.item(0)
        assert item is not None
        assert window.log_view.duration_text(item) in (None, "", "<1ms")

        window.log_store.append_entry(
            FlowLogEntry(
                line="run-1 success",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                    elapsed_seconds=9.9,
                ),
            )
        )

        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.log_view.duration_text(item) == "9.9s"
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_updates_only_changed_rows_when_one_live_duration_changes(qapp, monkeypatch):
    window = _make_window()
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        for run_id, status, elapsed in (
            ("run-1", "success", 1.1),
            ("run-2", "success", 2.2),
            ("run-3", "started", None),
        ):
            window.log_store.append_entry(
                FlowLogEntry(
                    line=f"{run_id} {status}",
                    kind="flow",
                    flow_name=flow_name,
                    event=RuntimeStepEvent(
                        run_id=run_id,
                        flow_name=flow_name,
                        step_name=None,
                        source_label="docs.xlsx",
                        status=status,
                        elapsed_seconds=elapsed,
                    ),
                )
            )

        window._refresh_log_view(force_scroll_to_bottom=True)

        from data_engine.ui.gui.presenters import logs as log_presenter

        updated_indexes: list[int] = []
        original_update = log_presenter.update_log_run_item

        def counting_update(window_arg, index, run_group):
            updated_indexes.append(index)
            return original_update(window_arg, index, run_group)

        monkeypatch.setattr(log_presenter, "update_log_run_item", counting_update)

        window.log_store.append_entry(
            FlowLogEntry(
                line="run-3 success",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-3",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                    elapsed_seconds=9.9,
                ),
            )
        )

        window._refresh_log_view(force_scroll_to_bottom=True)

        assert updated_indexes == [2]
    finally:
        _dispose_window(qapp, window)


def test_build_log_run_widget_is_not_created_as_top_level_window(qapp):
    window = _make_window()
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        run_group = FlowLogEntry(
            line="run-1 success",
            kind="flow",
            flow_name=flow_name,
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name=flow_name,
                step_name=None,
                source_label="docs.xlsx",
                status="success",
                elapsed_seconds=1.0,
            ),
        )
        window.log_store.append_entry(run_group)
        groups = window.history_query_service.list_flow_runs(window.runtime_binding.log_store, flow_name=flow_name)
        widget = build_log_run_widget(window, groups[0])

        assert widget.parent() is window.log_view.viewport()
        assert widget.isWindow() is False
        assert widget.findChild(QLabel, "logPrimary").isWindow() is False
        assert widget.findChild(QLabel, "logDuration").isWindow() is False
        assert widget.findChild(QLabel, "logStatusIcon").isWindow() is False
        assert widget.findChild(QPushButton, "logIconButton").isWindow() is False
    finally:
        _dispose_window(qapp, window)


def test_sidebar_row_widgets_are_not_created_as_top_level_windows(qapp):
    window = _make_window()
    try:
        card = window.flow_cards["poller"]
        flow_widget = build_flow_row_widget(window, card)
        group_widget = build_group_row_widget(window, "Example Group", [card])

        assert flow_widget.parent() is window.sidebar_content
        assert flow_widget.isWindow() is False
        assert group_widget.parent() is window.sidebar_content
        assert group_widget.isWindow() is False
        assert all(child.isWindow() is False for child in flow_widget.findChildren(QLabel))
        assert all(child.isWindow() is False for child in group_widget.findChildren(QLabel))
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_success_rows_ignore_transparent_log_button(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    shown: list[str] = []
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        window._show_run_log_preview = lambda run_group: shown.append(run_group.key[0])
        window.log_store.append_entry(
            FlowLogEntry(
                line="run-1 started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="started",
                    elapsed_seconds=None,
                ),
            )
        )

        window._refresh_log_view(force_scroll_to_bottom=True)

        item = window.log_view.item(0)
        assert item is not None
        view_rect = window.log_view.visualItemRect(item)
        button_rect = window.log_view._delegate.button_rect_for_run_group(
            view_rect.adjusted(1, 2, -1, -2),
            window.log_view.run_group(item),
        )
        click_point = button_rect.center()

        window.log_store.append_entry(
            FlowLogEntry(
                line="run-1 success",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                    elapsed_seconds=9.9,
                ),
            )
        )

        window._refresh_log_view(force_scroll_to_bottom=True)
        QTest.mouseClick(window.log_view.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, click_point)

        assert shown == [flow_name]
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_failed_button_opens_error_details_directly(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    shown_errors: list[tuple[tuple[str, str], str]] = []
    shown_logs: list[tuple[str, str]] = []
    try:
        flow_name = "poller"
        window.selected_flow_name = flow_name
        window._show_run_error_details = lambda run_group, entry: shown_errors.append((run_group.key, entry.line))
        window._show_run_log_preview = lambda run_group: shown_logs.append(run_group.key)
        window.log_store.append_entry(
            FlowLogEntry(
                line="run-1 failed",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="failed",
                    elapsed_seconds=1.0,
                ),
            )
        )

        window._refresh_log_view(force_scroll_to_bottom=True)

        item = window.log_view.item(0)
        assert item is not None
        view_rect = window.log_view.visualItemRect(item)
        button_rect = window.log_view._delegate.button_rect_for_run_group(
            view_rect.adjusted(1, 2, -1, -2),
            window.log_view.run_group(item),
        )
        QTest.mouseClick(window.log_view.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier, button_rect.center())

        assert shown_logs == [(flow_name, "run-1")]
        assert shown_errors == []
    finally:
        _dispose_window(qapp, window)


def test_refresh_selection_reuses_operation_rows_when_steps_are_unchanged(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        qapp.processEvents()

        original_row_cards = tuple(row.row_card for row in window.operation_row_widgets)

        window._refresh_selection(window.flow_cards["poller"])
        qapp.processEvents()

        assert tuple(row.row_card for row in window.operation_row_widgets) == original_row_cards
    finally:
        _dispose_window(qapp, window)


def test_sync_from_daemon_coalesces_nested_refresh_requests(qapp, monkeypatch):
    window = _make_window()
    try:
        original_sync_runtime_state = window.runtime_binding_service.sync_runtime_state
        sync_calls = 0
        nested_requested = False

        def wrapped_sync_runtime_state(*args, **kwargs):
            nonlocal sync_calls, nested_requested
            sync_calls += 1
            if not nested_requested:
                nested_requested = True
                window._sync_from_daemon()
            return original_sync_runtime_state(*args, **kwargs)

        monkeypatch.setattr(window.runtime_binding_service, "sync_runtime_state", wrapped_sync_runtime_state)

        window._sync_from_daemon()
        _process_ui_until(qapp, lambda: sync_calls == 2 and window._daemon_sync_in_progress is False)

        assert sync_calls == 2
        assert window._daemon_sync_in_progress is False
        assert window._daemon_sync_pending is False
    finally:
        _dispose_window(qapp, window)


def test_finish_daemon_sync_skips_unchanged_projection_redraw(qapp, monkeypatch):
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        window.workspace_snapshot = _workspace_snapshot_for_test(window.workspace_paths.workspace_id)
        refresh_calls = {"count": 0}
        original_refresh_selection = window.flow_controller.refresh_selection

        def wrapped_refresh_selection(*args, **kwargs):
            refresh_calls["count"] += 1
            return original_refresh_selection(*args, **kwargs)

        monkeypatch.setattr(window.flow_controller, "refresh_selection", wrapped_refresh_selection)
        payload = {
            "workspace_token": window._workspace_binding_token(),
            "sync_state": type("_SyncState", (), {"daemon_status": DaemonStatusState.empty()})(),
            "projection": WorkspaceRuntimeProjection(
                runtime_session=window.runtime_session,
                operation_tracker=window.operation_tracker,
                flow_states=dict(window.flow_states),
                active_runtime_flow_names=(),
                step_output_index=window.step_output_index,
            ),
            "workspace_snapshot": window.workspace_snapshot,
        }

        window.runtime_controller.finish_daemon_sync(window, payload)
        qapp.processEvents()

        assert refresh_calls["count"] == 0
    finally:
        _dispose_window(qapp, window)


def test_finish_daemon_sync_replaces_stale_observed_operation_tracker(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        window._apply_runtime_event(
            RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name="Read Excel",
                source_label="docs.xlsx",
                status="started",
                elapsed_seconds=None,
            )
        )
        assert window.operation_tracker.row_state("poller", "Read Excel").status == "running"
        workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=20,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=True, transport="heartbeat"),
            flows={},
            active_runs={},
        )
        payload = {
            "workspace_token": window._workspace_binding_token(),
            "sync_state": type("_SyncState", (), {"daemon_status": DaemonStatusState.empty()})(),
            "projection": WorkspaceRuntimeProjection(
                runtime_session=RuntimeSessionState.empty(),
                operation_tracker=OperationSessionState.empty(),
                flow_states={},
                active_runtime_flow_names=(),
                step_output_index=window.step_output_index,
            ),
            "workspace_snapshot": workspace_snapshot,
        }

        window.runtime_controller.finish_daemon_sync(window, payload)
        qapp.processEvents()

        assert window.operation_tracker.row_state("poller", "Read Excel") is None
        assert window.operation_row_widgets[0].row_card.property("stepState") == "idle"
        assert window.operation_row_widgets[0].duration_label.text() == ""
    finally:
        _dispose_window(qapp, window)


def test_poll_log_queue_ignores_entries_for_other_workspaces(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._select_flow("poller")
        window.log_queue.put(
            FlowLogEntry(
                line="poller event",
                kind="flow",
                flow_name="poller",
                workspace_id="other-workspace",
                event=RuntimeStepEvent(
                    run_id="run-1",
                    flow_name="poller",
                    step_name=None,
                    source_label="input.xlsx",
                    status="started",
                ),
            )
        )

        window._poll_log_queue()
        qapp.processEvents()

        assert window.log_store.entries_for_flow("poller") == ()
    finally:
        _dispose_window(qapp, window)


def test_reset_workspace_button_prefers_idle_daemon_live_truth_over_stale_manual_session(qapp, monkeypatch):
    del monkeypatch
    window = _make_window(
        snapshot=WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
            source="daemon",
        ),
    )
    try:
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=11,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )

        window._refresh_workspace_visibility_panel()

        assert window.reset_workspace_button.isEnabled() is True
    finally:
        _dispose_window(qapp, window)


def test_refresh_selection_prefers_daemon_live_step_state(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=8,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={
                "run-1": RunLiveSnapshot(
                    run_id="run-1",
                    flow_name="poller",
                    group_name="Imports",
                    source_path="docs.xlsx",
                    state="running",
                    current_step_name="Read Excel",
                    current_step_started_at_utc="2026-04-18T12:00:00+00:00",
                    started_at_utc="2026-04-18T11:59:00+00:00",
                    elapsed_seconds=60.0,
                )
            },
        )

        window._select_flow("poller")
        qapp.processEvents()

        assert window.operation_row_widgets[0].row_card.property("stepState") == "running"
        assert window.operation_row_widgets[1].row_card.property("stepState") == "idle"
    finally:
        _dispose_window(qapp, window)


def test_apply_daemon_update_batch_streams_step_events_without_log_rebuild(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=8,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window.runtime_session = RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=False)
        batch_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
            projection_version=9,
            active_runs=(
                ActiveRunState(
                    run_id="run-1",
                    flow_name="poller",
                    group_name="Imports",
                    source_path="docs.xlsx",
                    state="running",
                    current_step_name="Read Excel",
                    current_step_started_at_utc="2026-04-18T12:00:00+00:00",
                ),
            ),
            flow_activity=(FlowActivityState(flow_name="poller", active_run_count=1, engine_run_count=1),),
        )
        window._pending_daemon_update_batch = DaemonUpdateBatch(
            snapshot=batch_snapshot,
            updates=(
                DaemonLaneUpdate("run_lifecycle", flow_names=("poller",), run_ids=("run-1",)),
                DaemonLaneUpdate(
                    "step_activity",
                    flow_names=("poller",),
                    run_ids=("run-1",),
                    step_events=(
                        RuntimeStepEvent(
                            run_id="run-1",
                            flow_name="poller",
                            step_name="Read Excel",
                            source_label="docs.xlsx",
                            status="started",
                            elapsed_seconds=None,
                        ),
                    ),
                ),
            ),
            requires_full_sync=False,
        )

        def _fail_rebuild(*args, **kwargs):
            raise AssertionError("nonterminal step lane should not rebuild from logs")

        window.runtime_controller._refresh_runtime_projection_from_logs = _fail_rebuild

        window._apply_daemon_update_batch()

        assert window.operation_row_widgets[0].row_card.property("stepState") == "running"
        assert window.workspace_snapshot.active_runs["run-1"].current_step_name == "Read Excel"
    finally:
        _dispose_window(qapp, window)


def test_apply_daemon_update_batch_normalizes_previous_success_when_next_step_starts(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=8,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window.runtime_session = RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=False)

        window._apply_runtime_event(
            RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name="Read Excel",
                source_label="docs.xlsx",
                status="success",
                elapsed_seconds=0.25,
            )
        )
        window._apply_runtime_event(
            RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name="Write Parquet",
                source_label="docs.xlsx",
                status="started",
                elapsed_seconds=None,
            )
        )

        assert window.operation_row_widgets[0].row_card.property("stepState") == "idle"
        assert window.operation_row_widgets[1].row_card.property("stepState") == "running"
    finally:
        _dispose_window(qapp, window)


def test_apply_daemon_update_batch_uses_persisted_finished_step_duration_before_next_step_start(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=8,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window.runtime_session = RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=False)
        window.runtime_binding.runtime_cache_ledger.record_step_finished(
            step_run_id=window.runtime_binding.runtime_cache_ledger.record_step_started(
                run_id="run-1",
                flow_name="poller",
                step_label="Read Excel",
                started_at_utc="2026-04-18T12:00:00+00:00",
            ),
            status="success",
            finished_at_utc="2026-04-18T12:00:01+00:00",
            elapsed_ms=1000,
        )
        batch_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
            projection_version=9,
            active_runs=(
                ActiveRunState(
                    run_id="run-1",
                    flow_name="poller",
                    group_name="Imports",
                    source_path="docs.xlsx",
                    state="running",
                    current_step_name="Write Parquet",
                    current_step_started_at_utc="2026-04-18T12:00:01+00:00",
                ),
            ),
            flow_activity=(FlowActivityState(flow_name="poller", active_run_count=1, engine_run_count=1),),
        )
        window._pending_daemon_update_batch = DaemonUpdateBatch(
            snapshot=batch_snapshot,
            updates=(
                DaemonLaneUpdate("run_lifecycle", flow_names=("poller",), run_ids=("run-1",)),
                DaemonLaneUpdate(
                    "step_activity",
                    flow_names=("poller",),
                    run_ids=("run-1",),
                    step_events=(
                        RuntimeStepEvent(
                            run_id="run-1",
                            flow_name="poller",
                            step_name="Write Parquet",
                            source_label="docs.xlsx",
                            status="started",
                            elapsed_seconds=None,
                        ),
                    ),
                ),
            ),
            requires_full_sync=False,
        )

        window._apply_daemon_update_batch()

        assert window.operation_row_widgets[0].row_card.property("stepState") == "idle"
        assert window.operation_row_widgets[0].duration_label.text() == "1.0s"
        assert window.operation_row_widgets[1].row_card.property("stepState") == "running"
    finally:
        _dispose_window(qapp, window)


def test_apply_daemon_update_batch_streams_log_events_without_waiting_for_reload(qapp, monkeypatch):
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=8,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window.runtime_session = RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=False)
        window._pending_daemon_update_batch = DaemonUpdateBatch(
            snapshot=WorkspaceDaemonSnapshot(
                live=True,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="daemon",
                projection_version=9,
                active_runs=(),
            ),
            updates=(
                DaemonLaneUpdate(
                    "log_events",
                    flow_names=("poller",),
                    run_ids=("run-1",),
                    log_entries=(
                        FlowLogEntry(
                            line="poller  success  docs.xlsx",
                            kind="flow",
                            flow_name="poller",
                            event=RuntimeStepEvent(
                                run_id="run-1",
                                flow_name="poller",
                                step_name=None,
                                source_label="docs.xlsx",
                                status="success",
                                elapsed_seconds=1.25,
                            ),
                        ),
                    ),
                ),
            ),
            requires_full_sync=False,
        )

        def _fail_reload(*args, **kwargs):
            raise AssertionError("log event lane should not wait for persisted log reload")

        monkeypatch.setattr(window.runtime_binding_service, "reload_logs", _fail_reload)

        window._apply_daemon_update_batch()

        run_groups = window.log_store.runs_for_flow("poller")
        assert len(run_groups) == 1
        assert run_groups[0].status == "success"
        assert run_groups[0].elapsed_seconds == 1.25
    finally:
        _dispose_window(qapp, window)


def test_apply_daemon_update_batch_refreshes_last_7_day_run_count_on_success(qapp, monkeypatch):
    window = _make_window()
    try:
        window._load_flows()
        recent_started = datetime.now(UTC).isoformat()
        window.runtime_binding.runtime_cache_ledger.runs.record_started(
            run_id="run-existing",
            flow_name="poller",
            group_name="Imports",
            source_path="/tmp/input-existing.xlsx",
            started_at_utc=recent_started,
        )
        window._workspace_counts_footer_cache.clear()
        window._refresh_workspace_visibility_panel()
        assert window.workspace_counts_footer_label.text().endswith("1 runs last 7 days")

        window.runtime_binding.runtime_cache_ledger.runs.record_started(
            run_id="run-new",
            flow_name="poller",
            group_name="Imports",
            source_path="/tmp/input-new.xlsx",
            started_at_utc=recent_started,
        )
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=8,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window.runtime_session = RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=False)
        window._pending_daemon_update_batch = DaemonUpdateBatch(
            snapshot=WorkspaceDaemonSnapshot(
                live=True,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="daemon",
                projection_version=9,
                active_runs=(),
            ),
            updates=(
                DaemonLaneUpdate(
                    "log_events",
                    flow_names=("poller",),
                    run_ids=("run-new",),
                    completed_run_ids=("run-new",),
                    log_entries=(
                        FlowLogEntry(
                            line="poller  success  docs.xlsx",
                            kind="flow",
                            flow_name="poller",
                            event=RuntimeStepEvent(
                                run_id="run-new",
                                flow_name="poller",
                                step_name=None,
                                source_label="docs.xlsx",
                                status="success",
                                elapsed_seconds=1.25,
                            ),
                        ),
                    ),
                ),
            ),
            requires_full_sync=False,
        )

        window._apply_daemon_update_batch()

        assert window.workspace_counts_footer_label.text().endswith("2 runs last 7 days")
    finally:
        _dispose_window(qapp, window)


def test_daemon_wait_worker_schedules_sync_when_projection_changes(qapp, monkeypatch):
    window = _make_window()
    scheduled: list[str] = []
    try:
        previous_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
            projection_version=1,
        )
        next_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
            projection_version=2,
        )
        window.runtime_binding.daemon_manager._last_snapshot = previous_snapshot

        def _wait_for_update(manager, *, timeout_seconds=5.0):
            del timeout_seconds
            manager._last_snapshot = next_snapshot
            return next_snapshot

        window.daemon_state_service.wait_for_update = _wait_for_update
        monkeypatch.setattr(
            window,
            "_schedule_daemon_update_batch",
            lambda batch: scheduled.append(batch.snapshot.projection_version) or window.daemon_subscription.stop(),
        )

        window._daemon_wait_worker()

        assert scheduled == [2]
    finally:
        _dispose_window(qapp, window)


def test_daemon_wait_worker_skips_sync_when_projection_is_unchanged(qapp, monkeypatch):
    window = _make_window()
    scheduled: list[str] = []
    try:
        previous_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
            projection_version=1,
        )
        window.runtime_binding.daemon_manager._last_snapshot = previous_snapshot

        def _wait_for_update(manager, *, timeout_seconds=5.0):
            del timeout_seconds
            manager._last_snapshot = previous_snapshot
            window.daemon_subscription.stop()
            return previous_snapshot

        window.daemon_state_service.wait_for_update = _wait_for_update
        monkeypatch.setattr(window, "_schedule_daemon_update_batch", lambda batch: scheduled.append(batch.snapshot.projection_version))

        window._daemon_wait_worker()

        assert scheduled == []
    finally:
        _dispose_window(qapp, window)


def test_sync_from_daemon_preserves_daemon_owned_runtime_truth(qapp, monkeypatch):
    del monkeypatch
    snapshot = WorkspaceDaemonSnapshot(
        live=True,
        workspace_owned=True,
        leased_by_machine_id=None,
        runtime_active=False,
        runtime_stopping=False,
        manual_runs=(),
        last_checkpoint_at_utc=None,
        source="daemon",
        engine_starting=True,
        projection_version=7,
        active_engine_flow_names=("poller",),
        active_runs=(
            ActiveRunState(
                run_id="run-1",
                flow_name="poller",
                group_name="Imports",
                source_path="docs.xlsx",
                state="running",
                current_step_name="Read Excel",
                current_step_started_at_utc="2026-04-17T12:00:00+00:00",
                started_at_utc="2026-04-17T11:59:55+00:00",
                elapsed_seconds=5.0,
            ),
        ),
        flow_activity=(
            FlowActivityState(
                flow_name="poller",
                active_run_count=1,
                queued_run_count=2,
                engine_run_count=1,
                manual_run_count=0,
                stopping_run_count=0,
                running_step_counts={"Read Excel": 1},
            ),
        ),
    )
    window = _make_window(snapshot=snapshot)
    try:
        window._sync_from_daemon()
        _process_ui_until(qapp, lambda: window.workspace_snapshot is not None)

        assert window.workspace_snapshot is not None
        assert window.workspace_snapshot.version == 7
        assert window.workspace_snapshot.engine.state == "starting"
        assert window.workspace_snapshot.engine.active_flow_names == ("poller",)
        assert tuple(window.workspace_snapshot.active_runs) == ("run-1",)
        assert window.workspace_snapshot.active_runs["run-1"].current_step_name == "Read Excel"
        assert window.workspace_snapshot.flows["poller"].active_run_count == 1
        assert window.workspace_snapshot.flows["poller"].queued_run_count == 2
        assert window.workspace_snapshot.flows["poller"].running_step_counts == {"Read Excel": 1}
    finally:
        _dispose_window(qapp, window)


def test_refresh_log_view_prefers_daemon_live_runs_for_parallel_flow(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        for index in range(8):
            window.log_store.append_entry(
                FlowLogEntry(
                    line=f"run-{index} started",
                    kind="flow",
                    flow_name="poller",
                    event=RuntimeStepEvent(
                        run_id=f"run-{index}",
                        flow_name="poller",
                        step_name=None,
                        source_label=f"docs_{index}.xlsx",
                        status="started",
                    ),
                )
            )
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=9,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, active_flow_names=("poller",)),
            flows={},
            active_runs={
                f"run-{index}": RunLiveSnapshot(
                    run_id=f"run-{index}",
                    flow_name="poller",
                    group_name="Imports",
                    source_path=f"docs_{index}.xlsx",
                    state="running",
                    current_step_name="Normalize",
                    current_step_started_at_utc="2026-04-18T12:00:00+00:00",
                    started_at_utc="2026-04-18T11:59:00+00:00",
                    elapsed_seconds=60.0,
                )
                for index in range(4)
            },
        )

        window._refresh_log_view(force_scroll_to_bottom=True)

        assert window.log_view.count() == 4
    finally:
        _dispose_window(qapp, window)


def test_refresh_selection_shows_parallel_active_step_counts_without_serializing_steps(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        now = datetime.now(UTC)
        window._load_flows()
        window.flow_cards["poller"] = replace(window.flow_cards["poller"], parallelism="4")
        window._select_flow("poller")
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=10,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, active_flow_names=("poller",)),
            flows={},
            active_runs={
                "run-1": RunLiveSnapshot(
                    run_id="run-1",
                    flow_name="poller",
                    group_name="Imports",
                    source_path="docs_1.xlsx",
                    state="running",
                    current_step_name="Read Excel",
                    current_step_started_at_utc=(now - timedelta(seconds=4)).isoformat(),
                    started_at_utc=(now - timedelta(seconds=60)).isoformat(),
                    elapsed_seconds=60.0,
                ),
                "run-2": RunLiveSnapshot(
                    run_id="run-2",
                    flow_name="poller",
                    group_name="Imports",
                    source_path="docs_2.xlsx",
                    state="running",
                    current_step_name="Write Parquet",
                    current_step_started_at_utc=(now - timedelta(seconds=3)).isoformat(),
                    started_at_utc=(now - timedelta(seconds=65)).isoformat(),
                    elapsed_seconds=65.0,
                ),
                "run-3": RunLiveSnapshot(
                    run_id="run-3",
                    flow_name="poller",
                    group_name="Imports",
                    source_path="docs_3.xlsx",
                    state="running",
                    current_step_name="Write Parquet",
                    current_step_started_at_utc=(now - timedelta(seconds=2)).isoformat(),
                    started_at_utc=(now - timedelta(seconds=70)).isoformat(),
                    elapsed_seconds=70.0,
                ),
            },
        )

        window._refresh_selection(window.flow_cards["poller"])

        assert len(window.operation_row_widgets) == 2
        assert window.operation_row_widgets[0].row_card.property("stepState") == "running"
        assert window.operation_row_widgets[0].duration_label.text() == "1 active"
        assert window.operation_row_widgets[1].row_card.property("stepState") == "running"
        assert window.operation_row_widgets[1].duration_label.text() == "2 active"
    finally:
        _dispose_window(qapp, window)


def test_parallel_operation_rows_keep_last_duration_when_step_has_no_active_runs(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        now = datetime.now(UTC)
        window._load_flows()
        window.flow_cards["poller"] = replace(window.flow_cards["poller"], parallelism="4")
        window._select_flow("poller")
        window._apply_runtime_event(
            RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name="Read Excel",
                source_label="docs_1.xlsx",
                status="success",
                elapsed_seconds=1.0,
            )
        )
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=11,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, active_flow_names=("poller",)),
            flows={},
            active_runs={
                "run-2": RunLiveSnapshot(
                    run_id="run-2",
                    flow_name="poller",
                    group_name="Imports",
                    source_path="docs_2.xlsx",
                    state="running",
                    current_step_name="Write Parquet",
                    current_step_started_at_utc=(now - timedelta(seconds=3)).isoformat(),
                    started_at_utc=(now - timedelta(seconds=65)).isoformat(),
                    elapsed_seconds=65.0,
                ),
                "run-3": RunLiveSnapshot(
                    run_id="run-3",
                    flow_name="poller",
                    group_name="Imports",
                    source_path="docs_3.xlsx",
                    state="running",
                    current_step_name="Write Parquet",
                    current_step_started_at_utc=(now - timedelta(seconds=2)).isoformat(),
                    started_at_utc=(now - timedelta(seconds=70)).isoformat(),
                    elapsed_seconds=70.0,
                ),
            },
        )

        window._refresh_selection(window.flow_cards["poller"])
        window._refresh_live_operation_durations()

        assert len(window.operation_row_widgets) == 2
        assert window.operation_row_widgets[0].duration_label.text() == "1.0s"
        assert window.operation_row_widgets[1].row_card.property("stepState") == "running"
        assert window.operation_row_widgets[1].duration_label.text() == "2 active"
    finally:
        _dispose_window(qapp, window)


def test_refresh_action_buttons_prefers_daemon_live_stopping_run_after_rebind(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("manual_review")
        window.runtime_session = window.runtime_session.with_manual_runs_map({"Manual": "manual_review"})
        window.manual_flow_stopping_groups = set()
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=5,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={
                "run-manual": RunLiveSnapshot(
                    run_id="run-manual",
                    flow_name="manual_review",
                    group_name="Manual",
                    source_path=None,
                    state="stopping",
                    current_step_name="Build Report",
                    started_at_utc="2026-04-18T12:00:00+00:00",
                    finished_at_utc=None,
                    elapsed_seconds=5.0,
                )
            },
        )

        window._refresh_action_buttons()

        assert window.flow_run_button.text() == "Stopping..."
        assert window.flow_run_button.isEnabled() is False
    finally:
        _dispose_window(qapp, window)


def test_refresh_action_buttons_prefers_daemon_engine_starting_after_rebind(qapp, monkeypatch):
    del monkeypatch
    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=6,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="starting", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )

        window._refresh_action_buttons()

        assert window.engine_button.text() == "Starting..."
        assert window.engine_button.isEnabled() is False
        assert window.clear_flow_log_button.isEnabled() is False
    finally:
        _dispose_window(qapp, window)


def test_refresh_action_buttons_does_not_toggle_workspace_selectors_on_hot_path(qapp, monkeypatch):
    window = _make_window()
    try:
        selector_calls: list[tuple[str, bool]] = []

        original_workspace_set_enabled = window.workspace_selector.setEnabled
        original_settings_set_enabled = window.workspace_settings_selector.setEnabled

        def _record_workspace_enabled(value: bool) -> None:
            selector_calls.append(("workspace", value))
            original_workspace_set_enabled(value)

        def _record_settings_enabled(value: bool) -> None:
            selector_calls.append(("settings", value))
            original_settings_set_enabled(value)

        monkeypatch.setattr(window.workspace_selector, "setEnabled", _record_workspace_enabled)
        monkeypatch.setattr(window.workspace_settings_selector, "setEnabled", _record_settings_enabled)

        window._refresh_action_buttons()

        assert selector_calls == []
    finally:
        _dispose_window(qapp, window)


def test_rebuild_runtime_snapshot_drops_stopping_runtime_state_for_completed_flows(qapp, monkeypatch):
    del monkeypatch
    window = _make_window(cards=_sample_multi_active_qt_flow_cards())
    try:
        window.runtime_session = (
            window.runtime_session
            .with_runtime_flags(active=True, stopping=True)
            .with_active_runtime_flow_names(("poller_a", "poller_b"))
        )
        window.flow_states["poller_a"] = "stopping runtime"
        window.flow_states["poller_b"] = "stopping runtime"
        window.log_store.append_entry(
            FlowLogEntry(
                line="poller_a read started",
                kind="flow",
                flow_name="poller_a",
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name="poller_a",
                    step_name="Read Excel",
                    source_label="a.xlsx",
                    status="started",
                ),
            )
        )
        window.log_store.append_entry(
            FlowLogEntry(
                line="poller_b read success",
                kind="flow",
                flow_name="poller_b",
                event=RuntimeStepEvent(
                    run_id="run-b",
                    flow_name="poller_b",
                    step_name="Read Excel",
                    source_label="b.xlsx",
                    status="success",
                    elapsed_seconds=0.2,
                ),
            )
        )

        window.runtime_controller.rebuild_runtime_snapshot(window)

        assert window.runtime_session.active_runtime_flow_names == ("poller_a",)
        assert window.flow_states["poller_a"] == "stopping runtime"
        assert window.flow_states["poller_b"] == "schedule ready"
    finally:
        _dispose_window(qapp, window)


