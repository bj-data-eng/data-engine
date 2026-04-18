from __future__ import annotations

# ruff: noqa: E402
import pytest

textual = pytest.importorskip("textual")

from data_engine.domain import FlowCatalogEntry, WorkspaceControlState
from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.services import DaemonService
from data_engine.services.operator_commands import OperatorCommandService
from data_engine.services.operator_queries import WorkspaceCatalogLoadResult, WorkspaceCatalogPresentation
from data_engine.services.runtime_state import ControlSnapshot, EngineSnapshot, WorkspaceSnapshot
from data_engine.ui.tui.app import DataEngineTui
from data_engine.ui.tui.bootstrap import build_tui_services
from data_engine.views.logs import FlowLogStore
from data_engine.views.models import QtFlowCard


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def workspace_snapshot_for_test(
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


def sample_qt_flow_cards() -> tuple[QtFlowCard, ...]:
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


def append_persisted_run_log(workspace_root, *, run_id: str, flow_name: str, source_path: str, status: str, elapsed: float | None = None) -> None:
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


async def wait_for_tui_condition(pilot, condition, *, attempts: int = 20) -> None:
    for _ in range(attempts):
        if condition():
            return
        await pilot.pause()
    raise AssertionError("condition did not become true before timeout")


class FakeDaemonManager:
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


class FakeDaemonStateService:
    def __init__(self, manager: FakeDaemonManager | None = None) -> None:
        self.manager = manager or FakeDaemonManager()

    def create_manager(self, paths):
        del paths
        return self.manager

    def sync(self, manager):
        return manager.sync()

    def wait_for_update(self, manager, *, timeout_seconds: float = 5.0):
        return manager.wait_for_update(timeout_seconds=timeout_seconds)

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


class FakeFlowCatalogService:
    def __init__(self, cards: tuple[QtFlowCard, ...] | None = None) -> None:
        self.cards = cards or sample_qt_flow_cards()

    def load_entries(self, *, workspace_root=None):
        del workspace_root
        return tuple(FlowCatalogEntry(**card.__dict__) for card in self.cards)


class FakeLogService:
    def __init__(self, *, stores: tuple[FlowLogStore, ...] = ()) -> None:
        self._stores = list(stores)
        self.reload_calls: list[tuple[FlowLogStore, object]] = []
        self.created_ledgers: list[object] = []

    def create_store(self, runtime_ledger=None):
        self.created_ledgers.append(runtime_ledger)
        if self._stores:
            return self._stores.pop(0)
        return FlowLogStore()

    def reload(self, store, runtime_ledger=None):
        self.reload_calls.append((store, runtime_ledger))

    def append_entry(self, store, entry):
        store.append_entry(entry)

    def clear_flow(self, store, flow_name):
        store.clear_flow(flow_name)

    def all_entries(self, store):
        return store.entries()

    def entries_for_flow(self, store, flow_name):
        return store.entries_for_flow(flow_name)

    def runs_for_flow(self, store, flow_name):
        return store.runs_for_flow(flow_name)


class FakeSharedStateService:
    def __init__(self) -> None:
        self.hydrated: list[tuple[object, object]] = []

    def hydrate_local_runtime(self, paths, ledger) -> None:
        self.hydrated.append((paths, ledger))


class FakeResetService:
    def __init__(self) -> None:
        self.flow_resets: list[tuple[object, str]] = []

    def reset_flow(self, *, paths, runtime_cache_ledger, flow_name: str) -> None:
        del runtime_cache_ledger
        self.flow_resets.append((paths, flow_name))


def command_service_for_test(*, reset_service):
    class _RuntimeApplicationForCommands:
        def force_shutdown_daemon(self, paths, timeout=0.5):
            del paths, timeout
            return type("_Result", (), {"ok": True, "error": None})()

    class _UnusedControlApplication:
        def run_selected_flow(self, **kwargs):
            del kwargs
            raise AssertionError("run_selected_flow should not be called in this TUI test helper path.")

        def start_engine(self, **kwargs):
            del kwargs
            raise AssertionError("start_engine should not be called in this TUI test helper path.")

        def stop_pipeline(self, **kwargs):
            del kwargs
            raise AssertionError("stop_pipeline should not be called in this TUI test helper path.")

        def request_control(self, manager):
            del manager
            raise AssertionError("request_control should not be called in this TUI test helper path.")

        def refresh_flows(self, **kwargs):
            del kwargs
            raise AssertionError("refresh_flows should not be called in this TUI test helper path.")

    return OperatorCommandService(
        control_application=_UnusedControlApplication(),
        runtime_application=_RuntimeApplicationForCommands(),
        reset_service=reset_service,
        workspace_provisioning_service=None,
    )


class SyncingDaemonManager(FakeDaemonManager):
    def __init__(self, snapshot: WorkspaceDaemonSnapshot) -> None:
        super().__init__(snapshot=snapshot)

    def sync(self) -> WorkspaceDaemonSnapshot:
        return self.snapshot


class RecordingTui(DataEngineTui):
    def __init__(self, *args, **kwargs) -> None:
        self.shown_screens: list[tuple[str, str]] = []
        super().__init__(*args, **kwargs)

    def push_screen(self, screen, *args, **kwargs):
        title = getattr(screen, "title", "")
        body = getattr(screen, "body", "")
        self.shown_screens.append((title, body))
        return None


class RecordingStatusTui(DataEngineTui):
    def __init__(self, *args, **kwargs) -> None:
        self.status_messages: list[str] = []
        super().__init__(*args, **kwargs)

    def _set_status(self, message: str) -> None:
        self.status_messages.append(message)


class EmptyCatalogQueryService:
    def load_workspace_catalog(self, *, workspace_root, current_state=None, missing_message="No flow modules discovered."):
        del workspace_root, missing_message
        return WorkspaceCatalogLoadResult(
            catalog_state=current_state.with_entries(()).with_empty_message("No flow modules discovered."),
            loaded=False,
            error_text="No flow modules discovered.",
        )

    def build_catalog_presentation(self, *, catalog_state):
        del catalog_state
        return WorkspaceCatalogPresentation(entries=(), grouped_entries=(), selected_flow_name=None)


class FakeRuntimeController:
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


def make_tui(
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
    command_service=None,
    shared_state_service=None,
    daemon_state_service=None,
    catalog_query_service=None,
    app_cls=DataEngineTui,
) -> DataEngineTui:
    manager = FakeDaemonManager(snapshot=snapshot)
    services = build_tui_services(
        settings_store=settings_store,
        flow_catalog_service=FakeFlowCatalogService(cards),
        daemon_service=DaemonService(
            spawn_process_func=spawn_process_func or (lambda paths: 0),
            request_func=request_func or (lambda paths, payload, timeout=0.0: {"ok": True}),
            is_live_func=is_live_func or (lambda paths: False),
            client_error_type=Exception,
        ),
        daemon_state_service=daemon_state_service or FakeDaemonStateService(manager),
        log_service=log_service,
        command_service=command_service,
        shared_state_service=shared_state_service,
        catalog_query_service=catalog_query_service,
        discover_workspaces_func=discover_workspaces_func or (lambda **kwargs: ()),
        resolve_workspace_paths_func=resolve_workspace_paths_func or resolve_workspace_paths,
    )
    return app_cls(services=services)
