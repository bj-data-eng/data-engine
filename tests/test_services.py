from __future__ import annotations

import os
from pathlib import Path
from threading import Event
from time import time_ns

import pytest

from data_engine.authoring.flow import Flow
from data_engine.core.model import FlowStoppedError, FlowValidationError
from data_engine.domain import DaemonLifecyclePolicy, FlowCatalogEntry, WorkspaceControlState
from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.workspace_models import DiscoveredWorkspace
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.platform.theme import GITHUB_DARK, GITHUB_LIGHT
import data_engine.runtime.file_watch as file_watch
from data_engine.runtime.file_watch import PollingWatcher, is_temporary_file_path, iter_candidate_paths
from data_engine.runtime.stop import RuntimeStopController
from data_engine.services.daemon import DaemonService
from data_engine.services.daemon_state import DaemonStateService
from data_engine.services.flow_catalog import FlowCatalogService, flow_catalog_entry_from_flow
from data_engine.services.flow_execution import FlowExecutionService
from data_engine.services.ledger import LedgerService
from data_engine.services.runtime_execution import RuntimeExecutionService
from data_engine.services.settings import SettingsService
from data_engine.services.shared_state import SharedStateService
from data_engine.services.theme import ThemeService
from data_engine.services.workspaces import WorkspaceService


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def _rewrite_with_new_timestamp(path: Path, contents: str, *, step_ns: int = 2_000_000_000) -> None:
    path.write_text(contents, encoding="utf-8")
    current = path.stat().st_mtime_ns
    bumped = max(current + step_ns, time_ns() + step_ns)
    os.utime(path, ns=(bumped, bumped))


def test_iter_candidate_paths_filters_temp_files_and_extensions(tmp_path):
    (tmp_path / "~$draft.xlsx").write_text("x", encoding="utf-8")
    (tmp_path / ".hidden.xlsx").write_text("x", encoding="utf-8")
    (tmp_path / "notes.csv").write_text("x", encoding="utf-8")
    good = tmp_path / "claims.xlsx"
    good.write_text("x", encoding="utf-8")

    paths = list(iter_candidate_paths(tmp_path, extensions=(".xlsx",)))

    assert paths == [good]


def test_iter_candidate_paths_respects_non_recursive_and_single_file(tmp_path):
    nested = tmp_path / "nested"
    nested.mkdir()
    top = tmp_path / "top.xlsx"
    deep = nested / "deep.xlsx"
    top.write_text("x", encoding="utf-8")
    deep.write_text("x", encoding="utf-8")

    assert list(iter_candidate_paths(tmp_path, extensions=(".xlsx",), recursive=False)) == [top]
    assert list(iter_candidate_paths(top, extensions=(".xlsx",))) == [top]


def test_iter_candidate_paths_raises_for_missing_root(tmp_path):
    with pytest.raises(FlowValidationError, match="Input root not found"):
        list(iter_candidate_paths(tmp_path / "missing"))


def test_iter_candidate_paths_can_tolerate_missing_root_when_requested(tmp_path):
    assert list(iter_candidate_paths(tmp_path / "missing", allow_missing=True)) == []


def test_iter_candidate_paths_does_not_round_trip_through_path_constructor(tmp_path, monkeypatch):
    left = tmp_path / "alpha" / "claims.xlsx"
    right = tmp_path / "beta" / "claims.xlsx"
    left.parent.mkdir(parents=True)
    right.parent.mkdir(parents=True)
    left.write_text("x", encoding="utf-8")
    right.write_text("x", encoding="utf-8")

    def _boom(*args, **kwargs):  # pragma: no cover - defensive test hook
        raise AssertionError("Path constructor should not be used while sorting candidate paths")

    monkeypatch.setattr(file_watch, "Path", _boom)

    assert list(iter_candidate_paths(tmp_path, extensions=(".xlsx",))) == [left, right]


def test_temporary_file_helper_covers_common_transient_patterns():
    assert is_temporary_file_path(Path(".~lock.report.xlsx#")) is True
    assert is_temporary_file_path(Path("report.xlsx~")) is True
    assert is_temporary_file_path(Path("report.xlsx.download")) is True
    assert is_temporary_file_path(Path("report.xlsx")) is False


def test_polling_watcher_detects_new_and_modified_files_after_settle(tmp_path):
    watcher = PollingWatcher(tmp_path, extensions=(".xlsx",), settle=1)
    watcher.start()

    created = tmp_path / "claims.xlsx"
    created.write_text("v1", encoding="utf-8")

    assert watcher.drain_events() == []
    assert watcher.drain_events() == [created]

    _rewrite_with_new_timestamp(created, "v2")

    assert watcher.drain_events() == []
    assert watcher.drain_events() == [created]


def test_polling_watcher_supports_single_file_roots_and_stop(tmp_path):
    target = tmp_path / "claims.xlsx"
    target.write_text("v1", encoding="utf-8")

    watcher = PollingWatcher(target, settle=0)
    watcher.start()
    _rewrite_with_new_timestamp(target, "v2")
    assert watcher.drain_events() == [target]

    watcher.stop()
    _rewrite_with_new_timestamp(target, "v3")
    assert watcher.drain_events() == []


def test_polling_watcher_ignores_preexisting_file_on_start(tmp_path):
    existing = tmp_path / "claims.xlsx"
    existing.write_text("v1", encoding="utf-8")

    watcher = PollingWatcher(tmp_path, extensions=(".xlsx",), settle=1)
    watcher.start()

    assert watcher.drain_events() == []


def test_polling_watcher_reprocesses_deleted_then_recreated_file(tmp_path):
    target = tmp_path / "claims.xlsx"
    target.write_text("v1", encoding="utf-8")

    watcher = PollingWatcher(tmp_path, extensions=(".xlsx",), settle=1)
    watcher.start()

    target.unlink()
    assert watcher.drain_events() == []

    _rewrite_with_new_timestamp(target, "v2")
    assert watcher.drain_events() == []
    assert watcher.drain_events() == [target]


def test_polling_watcher_can_start_before_single_file_exists(tmp_path):
    target = tmp_path / "claims.xlsx"

    watcher = PollingWatcher(target, settle=0)
    watcher.start()

    target.write_text("v1", encoding="utf-8")

    assert watcher.drain_events() == [target]


def test_polling_watcher_rejects_negative_settle(tmp_path):
    with pytest.raises(FlowValidationError, match="zero or greater"):
        PollingWatcher(tmp_path, settle=-1)


def test_daemon_service_forwards_spawn_request(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    calls: list[object] = []

    def _spawn(paths_arg, *, lifecycle_policy=DaemonLifecyclePolicy.PERSISTENT):
        calls.append((paths_arg.workspace_root, lifecycle_policy))
        return "spawned"

    service = DaemonService(spawn_process_func=_spawn, request_func=lambda *args, **kwargs: {"ok": True})

    assert service.spawn(paths, lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL) == "spawned"
    assert calls == [(paths.workspace_root, DaemonLifecyclePolicy.EPHEMERAL)]


def test_daemon_service_forwards_request_liveness_and_error_type(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    request_calls: list[tuple[Path, dict[str, object], float]] = []
    live_calls: list[Path] = []

    service = DaemonService(
        spawn_process_func=lambda paths_arg, **kwargs: None,
        request_func=lambda paths_arg, payload, timeout=0.0: request_calls.append((paths_arg.workspace_root, payload, timeout)) or {"ok": True},
        is_live_func=lambda paths_arg: live_calls.append(paths_arg.workspace_root) or True,
        client_error_type=ValueError,
    )

    assert service.request(paths, {"command": "ping"}, timeout=1.25) == {"ok": True}
    assert service.is_live(paths) is True
    assert service.client_error_type is ValueError
    assert request_calls == [(paths.workspace_root, {"command": "ping"}, 1.25)]
    assert live_calls == [paths.workspace_root]


def test_daemon_state_service_delegates_to_manager(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    snapshot = WorkspaceDaemonSnapshot(
        live=True,
        workspace_owned=True,
        leased_by_machine_id=None,
        runtime_active=False,
        runtime_stopping=False,
        manual_runs=(),
        last_checkpoint_at_utc=None,
        source="daemon",
    )

    class _Manager:
        def __init__(self):
            self.control_calls: list[bool] = []

        def sync(self):
            return snapshot

        def control_state(self, snapshot_arg, *, daemon_startup_in_progress: bool = False):
            self.control_calls.append(daemon_startup_in_progress)
            assert snapshot_arg is snapshot
            return WorkspaceControlState.empty()

        def request_control(self):
            return "sent"

    service = DaemonStateService()
    manager = _Manager()

    created = service.create_manager(paths)
    assert created.paths == paths
    assert service.sync(manager) is snapshot
    assert service.control_state(manager, snapshot, daemon_startup_in_progress=True) == WorkspaceControlState.empty()
    assert service.request_control(manager) == "sent"
    assert manager.control_calls == [True]


def test_ledger_service_delegates_to_runtime_ledger(tmp_path):
    class _Ledger:
        def __init__(self):
            self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

        def close(self):
            self.calls.append(("close", (), {}))

        def upsert_client_session(self, **kwargs):
            self.calls.append(("upsert", (), kwargs))

        def remove_client_session(self, client_id):
            self.calls.append(("remove", (client_id,), {}))

        def remove_client_sessions_for_process(self, **kwargs):
            self.calls.append(("purge", (), kwargs))

        def count_live_client_sessions(self, workspace_id, *, exclude_client_id=None):
            self.calls.append(("count", (workspace_id,), {"exclude_client_id": exclude_client_id}))
            return 3 if exclude_client_id is None else 2

    service = LedgerService()
    ledger = _Ledger()

    service.close(ledger)
    service.register_client_session(
        ledger,
        client_id="abc",
        workspace_id="workspace",
        client_kind="ui",
        pid=123,
    )
    service.remove_client_session(ledger, "abc")
    service.purge_process_client_sessions(
        ledger,
        workspace_id="workspace",
        client_kind="ui",
        pid=123,
    )

    assert service.count_live_client_sessions(ledger, "workspace") == 3
    assert service.count_live_client_sessions(ledger, "workspace", exclude_client_id="abc") == 2
    assert ledger.calls == [
        ("close", (), {}),
        ("upsert", (), {"client_id": "abc", "workspace_id": "workspace", "client_kind": "ui", "pid": 123}),
        ("remove", ("abc",), {}),
        ("purge", (), {"workspace_id": "workspace", "client_kind": "ui", "pid": 123}),
        ("count", ("workspace",), {"exclude_client_id": None}),
        ("count", ("workspace",), {"exclude_client_id": "abc"}),
    ]


def test_ledger_service_opens_workspace_ledgers_through_injected_collaborator(tmp_path):
    workspace_root = tmp_path / "workspace"
    calls: list[Path] = []
    ledger = object()
    service = LedgerService(open_ledger_func=lambda root: calls.append(root) or ledger)

    opened = service.open_for_workspace(workspace_root)

    assert opened is ledger
    assert calls == [workspace_root.resolve()]


def test_ledger_service_default_open_uses_runtime_layout_policy(tmp_path):
    expected_workspace_root = tmp_path / "workspace"
    expected_db_path = tmp_path / "runtime" / "runtime_control.sqlite"

    class _Policy:
        def resolve_paths(self, *, workspace_root=None, **kwargs):
            assert kwargs == {}
            assert workspace_root == expected_workspace_root.resolve()

            class _Paths:
                runtime_control_db_path = expected_db_path

            return _Paths()

    service = LedgerService(runtime_layout_policy=_Policy())

    ledger = service.open_for_workspace(expected_workspace_root)
    try:
        assert ledger.db_path == expected_db_path.resolve()
    finally:
        ledger.close()


def test_settings_service_reads_and_persists_workspace_collection_root(tmp_path):
    store = LocalSettingsStore(tmp_path / "app_settings.sqlite")
    service = SettingsService(store)

    assert service.workspace_collection_root() is None

    target = tmp_path / "workspaces"
    service.set_workspace_collection_root(target)
    assert service.workspace_collection_root() == target

    reopened = SettingsService.open_default(app_root=tmp_path / "app")
    assert isinstance(reopened, SettingsService)


def test_settings_service_open_default_uses_injected_store_factory(tmp_path):
    calls: list[Path | None] = []
    store = LocalSettingsStore(tmp_path / "settings.sqlite")

    opened = SettingsService.open_default(
        app_root=tmp_path / "app",
        store_factory=lambda app_root: calls.append(app_root) or store,
    )

    assert isinstance(opened, SettingsService)
    assert opened.workspace_collection_root() is None
    assert calls == [tmp_path / "app"]


def test_shared_state_service_hydrates_local_runtime(monkeypatch, tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    calls: list[tuple[object, object]] = []
    ledger = object()

    monkeypatch.setattr(
        "data_engine.services.shared_state.hydrate_local_runtime_state",
        lambda paths_arg, ledger_arg: calls.append((paths_arg, ledger_arg)),
    )

    SharedStateService().hydrate_local_runtime(paths, ledger)

    assert calls == [(paths, ledger)]


def test_theme_service_resolves_palette_and_labels():
    service = ThemeService(
        themes={"light": GITHUB_LIGHT, "dark": GITHUB_DARK},
        resolve_theme_name_func=lambda name: "dark" if name == "system" else name,
        system_theme_name_func=lambda: "dark",
        toggle_theme_name_func=lambda name: "light" if name == "dark" else "dark",
        theme_button_text_func=lambda name: f"toggle {name}",
    )

    assert service.resolve_name("system") == "dark"
    assert service.system_name() == "dark"
    assert service.toggle_name("dark") == "light"
    assert service.button_text("dark") == "toggle dark"
    assert service.palette("system") is GITHUB_DARK


def test_workspace_service_forwards_discovery_and_resolution(tmp_path):
    calls: list[tuple[str, object]] = []
    expected_paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    discovered = (DiscoveredWorkspace(workspace_id="alpha", workspace_root=tmp_path / "workspace"),)

    service = WorkspaceService(
        discover_workspaces_func=lambda **kwargs: calls.append(("discover", kwargs)) or discovered,
        resolve_workspace_paths_func=lambda **kwargs: calls.append(("resolve", kwargs)) or expected_paths,
    )

    assert service.discover(app_root=tmp_path / "app", workspace_collection_root=tmp_path / "collection") == discovered
    assert service.resolve_paths(workspace_id="alpha", workspace_root=tmp_path / "workspace") == expected_paths
    assert calls == [
        ("discover", {"app_root": tmp_path / "app", "workspace_collection_root": tmp_path / "collection"}),
        ("resolve", {"workspace_id": "alpha", "workspace_root": tmp_path / "workspace", "data_root": None, "workspace_collection_root": None}),
    ]


def test_flow_catalog_entry_from_flow_builds_expected_metadata():
    flow = Flow(name="daily_summary", group="Claims").step(lambda context: context, label="Read Claims")
    entry = flow_catalog_entry_from_flow(flow, description="Loads claims")

    assert entry == FlowCatalogEntry(
        name="daily_summary",
        group="Claims",
        title="Daily Summary",
        description="Loads claims",
        source_root="(not set)",
        target_root="(not set)",
        mode="manual",
        interval="-",
        operations="Read Claims",
        operation_items=("Read Claims",),
        state="manual",
        valid=True,
        category="manual",
    )


def test_flow_catalog_service_loads_and_sorts_entries_and_marks_invalid(tmp_path):
    good_flow = Flow(name="beta", group="Claims").step(lambda context: context, label="Good")

    class _Definition:
        def __init__(self, name, description, builder):
            self.name = name
            self.description = description
            self._builder = builder

        def build(self):
            return self._builder()

    defs = (
        _Definition("zeta", "broken", lambda: (_ for _ in ()).throw(RuntimeError("boom"))),
        _Definition("beta", "good", lambda: good_flow),
    )
    service = FlowCatalogService(discover_definitions_func=lambda **kwargs: defs)

    entries = service.load_entries(workspace_root=tmp_path)

    assert [entry.name for entry in entries] == ["beta", "zeta"]
    assert entries[0].valid is True
    assert entries[1].valid is False
    assert entries[1].error == "boom"

    empty_service = FlowCatalogService(discover_definitions_func=lambda **kwargs: ())
    with pytest.raises(FlowValidationError, match="No flow modules discovered"):
        empty_service.load_entries(workspace_root=tmp_path)


def test_flow_execution_service_uses_injected_loader_and_discovery(tmp_path):
    flow = Flow(name="claims", group="Claims")
    load_calls: list[tuple[str, Path | None]] = []
    discover_calls: list[Path | None] = []
    service = FlowExecutionService(
        load_flow_func=lambda name, *, data_root=None: load_calls.append((name, data_root)) or flow,
        discover_flows_func=lambda *, data_root=None: discover_calls.append(data_root) or (flow,),
    )

    assert service.load_flow("claims", workspace_root=tmp_path) is flow
    assert service.load_flows(("claims", "claims"), workspace_root=tmp_path) == (flow, flow)
    assert service.discover_flows(workspace_root=tmp_path) == (flow,)
    assert load_calls == [("claims", tmp_path), ("claims", tmp_path), ("claims", tmp_path)]
    assert discover_calls == [tmp_path]


def test_runtime_execution_service_constructs_runtime_objects():
    flow = Flow(name="claims", group="Claims")

    class _Runtime:
        instances: list["_Runtime"] = []

        def __init__(self, flows, *, continuous, flow_stop_event=None, runtime_ledger=None, run_stop_controller=None):
            self.flows = flows
            self.continuous = continuous
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller
            type(self).instances.append(self)

        def run(self):
            return {"flows": tuple(flow.name for flow in self.flows), "continuous": self.continuous}

        def preview(self, *, use=None):
            return {"preview": use, "continuous": self.continuous}

    class _GroupedRuntime:
        instances: list["_GroupedRuntime"] = []

        def __init__(
            self,
            flows,
            *,
            continuous,
            runtime_stop_event=None,
            flow_stop_event=None,
            runtime_ledger=None,
            run_stop_controller=None,
        ):
            self.flows = flows
            self.continuous = continuous
            self.runtime_stop_event = runtime_stop_event
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller
            type(self).instances.append(self)

        def run(self):
            return {"grouped": tuple(flow.name for flow in self.flows), "continuous": self.continuous}

    service = RuntimeExecutionService(flow_runtime_type=_Runtime, grouped_runtime_type=_GroupedRuntime)
    flow_stop = Event()
    runtime_stop = Event()
    ledger = object()

    assert service.run_once(flow, runtime_ledger=ledger, flow_stop_event=flow_stop) == {"flows": ("claims",), "continuous": False}
    assert service.preview(flow, use="csv", runtime_ledger=ledger) == {"preview": "csv", "continuous": False}
    assert service.run_manual(flow, runtime_ledger=ledger, flow_stop_event=flow_stop) == {"flows": ("claims",), "continuous": False}
    assert service.run_continuous(flow, runtime_ledger=ledger, flow_stop_event=flow_stop) == {"flows": ("claims",), "continuous": True}
    assert service.run_grouped((flow,), runtime_ledger=ledger, runtime_stop_event=runtime_stop, flow_stop_event=flow_stop) == {"grouped": ("claims",), "continuous": True}
    assert service.run_grouped_continuous((flow,), runtime_ledger=ledger, runtime_stop_event=runtime_stop, flow_stop_event=flow_stop) == {"grouped": ("claims",), "continuous": True}

    assert _Runtime.instances[0].continuous is False
    assert _Runtime.instances[1].continuous is False
    assert _Runtime.instances[2].continuous is False
    assert _Runtime.instances[3].continuous is True
    assert _GroupedRuntime.instances[0].runtime_stop_event is runtime_stop
    assert _GroupedRuntime.instances[1].flow_stop_event is flow_stop


def test_runtime_execution_service_exposes_explicit_engine_commands(tmp_path):
    flow = Flow(name="claims", group="Claims")
    source = tmp_path / "claims.csv"
    source.write_text("claim_id\n1\n", encoding="utf-8")

    class _RunExecutor:
        def run_one(self, flow, source_path, *, batch_signatures=()):
            return {
                "flow": flow.name,
                "source_path": source_path,
                "batch_signatures": batch_signatures,
            }

    class _Polling:
        def stale_batch_poll_signatures(self, flow):
            return (f"{flow.name}:signature",)

    class _Runtime:
        def __init__(self, flows, *, continuous, flow_stop_event=None, runtime_ledger=None, run_stop_controller=None):
            self.flows = flows
            self.continuous = continuous
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller
            self.run_executor = _RunExecutor()
            self.polling = _Polling()
            self.closed = False

        def _validate(self):
            return None

        def _close_owned_runtime_ledger(self):
            self.closed = True

        def run_source(self, flow, source_path):
            self._validate()
            try:
                return self.run_executor.run_one(flow, source_path)
            finally:
                self._close_owned_runtime_ledger()

        def run_batch(self, flow):
            self._validate()
            try:
                return self.run_executor.run_one(
                    flow,
                    None,
                    batch_signatures=self.polling.stale_batch_poll_signatures(flow),
                )
            finally:
                self._close_owned_runtime_ledger()

    service = RuntimeExecutionService(flow_runtime_type=_Runtime)

    assert service.run_source(flow, source) == {
        "flow": "claims",
        "source_path": source,
        "batch_signatures": (),
    }
    assert service.run_batch(flow) == {
        "flow": "claims",
        "source_path": None,
        "batch_signatures": ("claims:signature",),
    }


def test_runtime_execution_service_stop_requests_run_id_on_controller():
    controller = RuntimeStopController()
    controller.register_run("run-123")
    RuntimeExecutionService(run_stop_controller=controller).stop("run-123")

    with pytest.raises(FlowStoppedError, match="run-123"):
        controller.check_run("run-123")
