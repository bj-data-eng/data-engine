from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest


textual = pytest.importorskip("textual")
from textual.widgets import ListView

from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
from data_engine.domain import FlowCatalogEntry, RuntimeSessionState, WorkspaceControlState
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.services import DaemonService
from data_engine.application.catalog import FlowCatalogLoadResult, FlowCatalogPresentation
from data_engine.ui.tui.bootstrap import build_tui_services
from data_engine.ui.tui.app import FlowListItem
from data_engine.ui.tui.app import RunGroupListItem
from data_engine.ui.tui.app import DataEngineTui
from data_engine.domain import FlowLogEntry
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.runtime_db import RuntimeLedger, utcnow_text
from data_engine.domain import RuntimeStepEvent
from data_engine.views.models import QtFlowCard
from data_engine.views.logs import FlowLogStore


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


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


async def _wait_for_tui_condition(pilot, condition, *, attempts: int = 20) -> None:
    for _ in range(attempts):
        if condition():
            return
        await pilot.pause()
    raise AssertionError("condition did not become true before timeout")


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


class _FakeFlowCatalogService:
    def __init__(self, cards: tuple[QtFlowCard, ...] | None = None) -> None:
        self.cards = cards or _sample_qt_flow_cards()

    def load_entries(self, *, workspace_root=None):
        del workspace_root
        return tuple(FlowCatalogEntry(**card.__dict__) for card in self.cards)


class _FakeLogService:
    def __init__(self, *, stores: tuple[FlowLogStore, ...] = ()) -> None:
        self._stores = list(stores)
        self.reload_calls: list[FlowLogStore] = []
        self.created_ledgers: list[object] = []

    def create_store(self, runtime_ledger=None):
        self.created_ledgers.append(runtime_ledger)
        if self._stores:
            return self._stores.pop(0)
        return FlowLogStore()

    def reload(self, store):
        self.reload_calls.append(store)

    def append_entry(self, store, entry):
        store.append_entry(entry)

    def clear_flow(self, store, flow_name):
        store.clear_flow(flow_name)

    def all_entries(self, store):
        return tuple(store._entries)

    def entries_for_flow(self, store, flow_name):
        return store.entries_for_flow(flow_name)

    def runs_for_flow(self, store, flow_name):
        return store.runs_for_flow(flow_name)


class _FakeSharedStateService:
    def __init__(self) -> None:
        self.hydrated: list[tuple[object, object]] = []

    def hydrate_local_runtime(self, paths, ledger) -> None:
        self.hydrated.append((paths, ledger))


class _SyncingDaemonManager(_FakeDaemonManager):
    def __init__(self, snapshot: WorkspaceDaemonSnapshot) -> None:
        super().__init__(snapshot=snapshot)

    def sync(self) -> WorkspaceDaemonSnapshot:
        return self.snapshot


class _RecordingTui(DataEngineTui):
    def __init__(self, *args, **kwargs) -> None:
        self.shown_screens: list[tuple[str, str]] = []
        super().__init__(*args, **kwargs)

    def push_screen(self, screen, *args, **kwargs):
        title = getattr(screen, "title", "")
        body = getattr(screen, "body", "")
        self.shown_screens.append((title, body))
        return None


class _RecordingStatusTui(DataEngineTui):
    def __init__(self, *args, **kwargs) -> None:
        self.status_messages: list[str] = []
        super().__init__(*args, **kwargs)

    def _set_status(self, message: str) -> None:
        self.status_messages.append(message)


class _EmptyCatalogApplication:
    def load_workspace_catalog(self, *, workspace_paths, current_state=None, missing_message="No flow modules discovered."):
        del workspace_paths, missing_message
        return FlowCatalogLoadResult(
            catalog_state=current_state.with_entries(()).with_empty_message("No flow modules discovered."),
            loaded=False,
            error_text="No flow modules discovered.",
        )

    def build_presentation(self, *, catalog_state):
        del catalog_state
        return FlowCatalogPresentation(entries=(), grouped_entries=(), selected_flow_name=None)


class _FakeRuntimeController:
    def __init__(self) -> None:
        self.sync_calls: list[object] = []

    def sync_daemon_state(self, window) -> None:
        self.sync_calls.append(window)

    def finish_daemon_startup(self, window, success: bool, error_text: str) -> None:
        window._daemon_startup_in_progress = False
        if success:
            self.sync_daemon_state(window)
            return
        if error_text:
            window._set_status(error_text)
        else:
            window._set_status("Daemon startup did not provide any additional error details.")
        self.sync_daemon_state(window)


def _make_tui(
    *,
    cards: tuple[QtFlowCard, ...] | None = None,
    snapshot: WorkspaceDaemonSnapshot | None = None,
    request_func=None,
    is_live_func=None,
    spawn_process_func=None,
    discover_workspaces_func=None,
    resolve_workspace_paths_func=None,
    settings_store: LocalSettingsStore | None = None,
    log_service=None,
    shared_state_service=None,
    daemon_state_service=None,
    flow_catalog_application=None,
    app_cls=DataEngineTui,
) -> DataEngineTui:
    manager = _FakeDaemonManager(snapshot=snapshot)
    services = build_tui_services(
        settings_store=settings_store,
        flow_catalog_service=_FakeFlowCatalogService(cards),
        daemon_service=DaemonService(
            spawn_process_func=spawn_process_func or (lambda paths: 0),
            request_func=request_func or (lambda paths, payload, timeout=0.0: {"ok": True}),
            is_live_func=is_live_func or (lambda paths: False),
            client_error_type=Exception,
        ),
        daemon_state_service=daemon_state_service or _FakeDaemonStateService(manager),
        log_service=log_service,
        shared_state_service=shared_state_service,
        flow_catalog_application=flow_catalog_application,
        discover_workspaces_func=discover_workspaces_func or (lambda **kwargs: ()),
        resolve_workspace_paths_func=resolve_workspace_paths_func or resolve_workspace_paths,
    )
    return app_cls(services=services)


def test_flow_list_item_refresh_view_updates_label():
    card = QtFlowCard(
        name="claims_summary",
        group="Claims",
        title="Claims Summary",
        description="",
        source_root="/tmp/in",
        target_root="/tmp/out",
        mode="schedule",
        interval="5s",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="schedule ready",
        valid=True,
        category="automated",
    )

    item = FlowListItem(card, "schedule ready")
    item.refresh_view("success")

    rendered = item.label.render().plain
    assert "Claims Summary" in rendered
    assert "success" in rendered


@pytest.mark.anyio
async def test_tui_disables_run_and_start_when_workspace_not_owned():
    app = _make_tui()
    async with app.run_test() as pilot:
        del pilot
        app.runtime_session = replace(app.runtime_session, workspace_owned=False, leased_by_machine_id="other-host")
        app._refresh_buttons()

        assert app.query_one("#run-once").disabled is True
        assert app.query_one("#start-engine").disabled is True


@pytest.mark.anyio
async def test_tui_tolerates_brief_daemon_sync_miss_without_flipping_to_lease_view():
    app = _make_tui()
    async with app.run_test() as pilot:
        del pilot
        app.runtime_session = replace(app.runtime_session, workspace_owned=True)
        app._daemon_manager._last_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
        )
        app._daemon_manager._sync_misses = 0

        app._sync_daemon_state()

        assert app.runtime_session.workspace_owned is True
        assert app._daemon_manager._sync_misses == 1


@pytest.mark.anyio
async def test_tui_hydrates_shared_runtime_logs_when_observing_lease():
    shared_state_service = _FakeSharedStateService()
    app = _make_tui(
        shared_state_service=shared_state_service,
        daemon_state_service=_FakeDaemonStateService(
            _SyncingDaemonManager(
                WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=False,
                    leased_by_machine_id="other-host",
                    runtime_active=False,
                    runtime_stopping=False,
                    manual_runs=(),
                    last_checkpoint_at_utc=None,
                    source="lease",
                )
            )
        ),
    )
    async with app.run_test() as pilot:
        del pilot

        app._sync_daemon_state()

        assert shared_state_service.hydrated
        assert shared_state_service.hydrated[-1][0] == app.workspace_paths
        assert shared_state_service.hydrated[-1][1] is app.runtime_ledger


@pytest.mark.anyio
async def test_tui_uses_local_workspace_collection_root_override(monkeypatch, tmp_path):
    override_root = tmp_path / "override_workspaces"
    (override_root / "example_workspace" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(tmp_path / "data_engine"))

    store = LocalSettingsStore.open_default(app_root=Path(tmp_path / "data_engine"))
    store.set_workspace_collection_root(override_root)

    app = _make_tui(settings_store=store)
    try:
        assert app.workspace_collection_root_override == override_root.resolve()
        assert app.workspace_paths.workspace_collection_root == override_root.resolve()
    finally:
        app.runtime_ledger.close()


@pytest.mark.anyio
async def test_tui_log_run_selection_updates_preview():
    app = _make_tui(log_service=_FakeLogService(), app_cls=_RecordingTui)
    async with app.run_test() as pilot:
        flow_name = app.flow_cards[0].name
        app.selected_flow_name = flow_name
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_a.xlsx",
                    status="started",
                    elapsed_seconds=0.004,
                ),
            )
        )
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a read",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name="Read",
                    source_label="file_a.xlsx",
                    status="success",
                    elapsed_seconds=0.004,
                ),
            )
        )
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-b started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-b",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_b.xlsx",
                    status="started",
                    elapsed_seconds=0.009,
                ),
            )
        )
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-b read",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-b",
                    flow_name=flow_name,
                    step_name="Read",
                    source_label="file_b.xlsx",
                    status="success",
                    elapsed_seconds=0.009,
                ),
            )
        )

        app._render_selected_flow()

        run_groups = app.log_store.runs_for_flow(flow_name)
        assert len(run_groups) == 2

        app.selected_run_key = next(run_group.key for run_group in run_groups if run_group.key[1] == "run-a")
        app.action_view_log()

        assert app.shown_screens
        assert "Run Details" in app.shown_screens[0][0]
        assert "file_a.xlsx" in app.shown_screens[0][0] or "file_a.xlsx" in app.shown_screens[0][1]


@pytest.mark.anyio
async def test_tui_selecting_log_run_opens_modal():
    app = _make_tui(log_service=_FakeLogService(), app_cls=_RecordingTui)
    async with app.run_test() as pilot:
        flow_name = app.flow_cards[0].name
        app.selected_flow_name = flow_name
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_a.xlsx",
                    status="started",
                    elapsed_seconds=0.004,
                ),
            )
        )
        app._render_selected_flow()

        run_group = app.log_store.runs_for_flow(flow_name)[0]
        app.on_list_view_selected(type("Evt", (), {"item": RunGroupListItem(run_group)})())

        assert app.shown_screens
        assert "Run Details" in app.shown_screens[0][0]


@pytest.mark.anyio
async def test_tui_run_group_row_refreshes_when_same_run_finishes(monkeypatch):
    app = _make_tui(log_service=_FakeLogService())
    async with app.run_test():
        flow_name = app.flow_cards[0].name
        app.selected_flow_name = flow_name
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_a.xlsx",
                    status="started",
                ),
            )
        )

        app._render_selected_flow()
        run_list = app.query_one("#log-run-list", ListView)
        run_item = next(child for child in run_list.children if isinstance(child, RunGroupListItem))
        assert run_item.run_group.status == "started"

        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a success",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_a.xlsx",
                    status="success",
                    elapsed_seconds=0.023,
                ),
            )
        )

        app._render_selected_flow()

        run_item = next(child for child in run_list.children if isinstance(child, RunGroupListItem))
        assert run_item.run_group.status == "success"


@pytest.mark.anyio
async def test_tui_switching_workspaces_reloads_visible_log_runs(monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "workspaces"
    claims_root = workspace_collection_root / "claims"
    claims2_root = workspace_collection_root / "claims2"
    (claims_root / "flow_modules").mkdir(parents=True)
    (claims2_root / "flow_modules").mkdir(parents=True)

    initial_store = FlowLogStore()
    replacement_store = FlowLogStore()
    app = _make_tui(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None, explicit_workspace_root=None: (
            type("DW", (), {"workspace_id": "claims", "workspace_root": claims_root})(),
            type("DW", (), {"workspace_id": "claims2", "workspace_root": claims2_root})(),
        ),
        resolve_workspace_paths_func=lambda workspace_id=None, workspace_root=None, workspace_collection_root=None, data_root=None: resolve_workspace_paths(
            workspace_root=claims_root if workspace_id in (None, "claims") else claims2_root,
            workspace_id="claims" if workspace_id in (None, "claims") else "claims2",
        ),
        log_service=_FakeLogService(stores=(initial_store, replacement_store)),
    )
    async with app.run_test() as pilot:
        flow_name = app.flow_cards[0].name
        app.selected_flow_name = flow_name
        app.log_store.append_entry(
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
        app._render_selected_flow()

        run_list = app.query_one("#log-run-list")
        initial_groups = app.log_store.runs_for_flow(flow_name)
        assert len(initial_groups) == 1
        assert [group.source_label for group in initial_groups] == ["claims.xlsx"]
        assert len([child for child in run_list.children if isinstance(child, RunGroupListItem)]) == 1

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
        app._switch_workspace("claims2")
        await _wait_for_tui_condition(
            pilot,
            lambda: len([child for child in run_list.children if isinstance(child, RunGroupListItem)]) == 2,
        )

        switched_groups = app.log_store.runs_for_flow(flow_name)
        assert app.workspace_paths.workspace_id == "claims2"
        assert app.log_store is replacement_store
        assert len(switched_groups) == 2
        assert [group.source_label for group in switched_groups] == ["claims2_a.xlsx", "claims2_b.xlsx"]
        assert len([child for child in run_list.children if isinstance(child, RunGroupListItem)]) == 2


@pytest.mark.anyio
async def test_tui_empty_workspace_reload_clears_stale_flow_rows():
    app = _make_tui(log_service=_FakeLogService())
    async with app.run_test() as pilot:
        list_view = app.query_one("#flow-list")
        initial_count = len(list_view.children)
        assert initial_count > 0
        app.flow_catalog_application = _EmptyCatalogApplication()
        app.flow_controller.workspace.flow_catalog_application = app.flow_catalog_application

        app._load_flows()
        await _wait_for_tui_condition(pilot, lambda: len(list_view.children) == 0)

        assert app.selected_flow_name is None
        assert len(list_view.children) == 0


@pytest.mark.anyio
async def test_tui_does_not_bootstrap_daemon_without_authored_workspace(tmp_path):
    spawn_calls: list[object] = []
    app = _make_tui(spawn_process_func=lambda paths: spawn_calls.append(paths) or 0)
    empty_root = tmp_path / "empty_workspace"
    empty_root.mkdir(parents=True)
    app.workspace_paths = resolve_workspace_paths(workspace_root=empty_root)

    async with app.run_test():
        assert spawn_calls == []


@pytest.mark.anyio
async def test_tui_sync_daemon_state_stops_pinging_when_workspace_root_is_missing(tmp_path):
    live_calls: list[object] = []
    app = _make_tui(is_live_func=lambda paths: live_calls.append(paths) or False)
    missing_root = tmp_path / "missing_workspace"
    app.workspace_paths = resolve_workspace_paths(workspace_root=missing_root)
    app.runtime_session = replace(app.runtime_session, runtime_active=True, workspace_owned=False)
    app.workspace_control_state = replace(app.workspace_control_state, blocked_status_text="stale")

    async with app.run_test():
        app._sync_daemon_state()

        assert live_calls == []
        assert app.runtime_session == RuntimeSessionState.empty()
        assert app.workspace_control_state == WorkspaceControlState.empty()


def test_tui_daemon_startup_uses_verbose_fallback_when_error_text_is_blank():
    app = _make_tui(app_cls=_RecordingStatusTui)
    runtime_controller = _FakeRuntimeController()
    app.runtime_controller = runtime_controller
    app._finish_daemon_startup(False, "")

    assert app.status_messages == [
        "Daemon startup did not provide any additional error details.",
    ]
    assert runtime_controller.sync_calls == [app]
