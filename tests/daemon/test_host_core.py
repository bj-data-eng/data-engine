from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import os
import threading

import pytest

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.app import (
    DAEMON_LOG_RETENTION_DAYS,
    DataEngineDaemonService,
    main as daemon_main,
)
from data_engine.hosts.daemon.bootstrap import initialize_service
from data_engine.hosts.daemon.composition import (
    DaemonHostDependencyFactories,
    DaemonHostDependencies,
    DaemonHostIdentity,
    DaemonHostState,
)
from data_engine.hosts.daemon.client import (
    _encode_message,
)
from data_engine.hosts.daemon.runtime_control import stop_active_work
from data_engine.hosts.daemon.server import serve_forever, serve_workspace_daemon
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR, machine_id_text
from data_engine.runtime.runtime_db import RuntimeCacheLedger, RuntimeControlLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state,
    claim_workspace,
    initialize_workspace_state,
    read_lease_metadata,
)

from .support import _write_demo_flow, resolve_workspace_paths

def test_daemon_service_initializes_and_serves_commands(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001 - direct daemon contract test
        assert status["ok"] is True
        assert status["status"]["workspace_id"] == "default"

        flows = service._handle_command({"command": "list_flows"})  # noqa: SLF001 - direct daemon contract test
        assert flows["ok"] is True
        assert [item["name"] for item in flows["flows"]] == ["demo"]
    finally:
        service._shutdown()  # noqa: SLF001 - direct daemon lifecycle test


def test_daemon_status_returns_unchanged_when_projection_version_matches(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        full_status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001 - direct daemon contract test
        assert full_status["ok"] is True
        version = int(full_status["status"]["projection_version"])
        event_sequence = int(full_status["status"]["event_sequence"])
        assert version >= 1

        unchanged = service._handle_command(  # noqa: SLF001 - direct daemon contract test
            {"command": "daemon_status", "since_version": version, "since_event_sequence": event_sequence}
        )

        assert unchanged["ok"] is True
        assert unchanged["status"] == {
            "workspace_id": "default",
            "daemon_id": service.daemon_id,
            "projection_version": version,
            "event_sequence": event_sequence,
            "unchanged": True,
        }
    finally:
        service._shutdown()  # noqa: SLF001 - direct daemon lifecycle test


def test_wait_for_daemon_status_returns_after_projection_change(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        initial = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        version = int(initial["status"]["projection_version"])
        event_sequence = int(initial["status"]["event_sequence"])

        def _refresh_projection() -> None:
            service.runtime_execution_ledger.execution_state.record_run_started(
                run_id="run-1",
                flow_name="demo",
                group_name="Demo",
                source_path="claims.xlsx",
                started_at_utc=utcnow_text(),
            )
            service._publish_runtime_event("runtime.execution.changed")  # noqa: SLF001

        thread = threading.Thread(target=_refresh_projection, daemon=True)
        thread.start()
        try:
            waited = service._handle_command(  # noqa: SLF001
                {
                    "command": "wait_for_daemon_status",
                    "since_version": version,
                    "since_event_sequence": event_sequence,
                    "timeout_ms": 500,
                }
            )
        finally:
            thread.join(timeout=1.0)

        assert waited["ok"] is True
        assert waited["status"]["event_sequence"] > event_sequence
        assert waited["status"]["active_runs"][0]["run_id"] == "run-1"
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_host_dependencies_build_default_opens_workspace_runtime_ledger(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    dependencies = DaemonHostDependencies.build_default(paths)
    try:
        assert isinstance(dependencies.runtime_cache_ledger, RuntimeCacheLedger)
        assert dependencies.runtime_cache_ledger.db_path.name == "runtime_cache.sqlite"
        assert dependencies.runtime_control_ledger.db_path.name == "runtime_control.sqlite"
        assert dependencies.runtime_cache_ledger.db_path.parent.parent.name == "runtime_state"
        assert dependencies.runtime_cache_ledger.db_path.exists() is True
        assert dependencies.runtime_control_ledger.db_path.exists() is True
        assert dependencies.flow_catalog_service.__class__.__name__ == "FlowCatalogService"
        assert dependencies.flow_execution_service.__class__.__name__ == "FlowExecutionService"
        assert dependencies.runtime_execution_service.__class__.__name__ == "RuntimeExecutionService"
        assert dependencies.shared_state_adapter.__class__.__name__ == "DaemonSharedStateAdapter"
    finally:
        dependencies.runtime_cache_ledger.close()
        dependencies.runtime_control_ledger.close()


def test_daemon_host_dependencies_build_default_uses_injected_ledger_service(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    calls: list[Path] = []
    ledger = RuntimeControlLedger(tmp_path / "custom" / "runtime_control.sqlite")

    class _LedgerService:
        def open_for_workspace(self, workspace_root_arg: Path) -> RuntimeControlLedger:
            calls.append(workspace_root_arg)
            return ledger

    dependencies = DaemonHostDependencies.build_default(paths, ledger_service=_LedgerService())
    try:
        assert dependencies.runtime_control_ledger is ledger
        assert calls == [paths.workspace_root]
    finally:
        ledger.close()


def test_daemon_host_dependencies_build_default_uses_injected_factories(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    calls: list[str] = []

    class _FlowCatalogService:
        def __init__(self):
            calls.append("catalog")

    class _FlowExecutionService:
        def __init__(self):
            calls.append("execution")

    class _RuntimeExecutionService:
        def __init__(self):
            calls.append("runtime")

    dependencies = DaemonHostDependencies.build_default(
        paths,
        factories=DaemonHostDependencyFactories(
            flow_catalog_service_factory=_FlowCatalogService,
            flow_execution_service_factory=_FlowExecutionService,
            runtime_execution_service_factory=_RuntimeExecutionService,
        ),
    )
    try:
        assert calls == ["catalog", "execution", "runtime"]
        assert dependencies.flow_catalog_service.__class__.__name__ == "_FlowCatalogService"
        assert dependencies.flow_execution_service.__class__.__name__ == "_FlowExecutionService"
        assert dependencies.runtime_execution_service.__class__.__name__ == "_RuntimeExecutionService"
    finally:
        dependencies.runtime_cache_ledger.close()
        dependencies.runtime_control_ledger.close()


def test_daemon_host_identity_current_process_uses_current_pid():
    identity = DaemonHostIdentity.current_process()

    assert identity.machine_id == machine_id_text()
    assert identity.pid == os.getpid()
    assert len(identity.daemon_id) == 32


def test_daemon_host_state_transitions_cover_core_mutators():
    state = DaemonHostState.build(started_at_utc="2026-04-06T00:00:00+00:00")
    runtime_stop_event = threading.Event()
    flow_stop_event = threading.Event()
    engine_thread = threading.Thread(target=lambda: None)
    manual_thread = threading.Thread(target=lambda: None)
    manual_runtime_stop_event = threading.Event()
    manual_flow_stop_event = threading.Event()

    assert state.status == "starting"
    assert state.workspace_owned is False
    assert state.runtime_active is False
    assert state.runtime_stopping is False
    assert state.listener is None

    state.claim_workspace()
    assert state.workspace_owned is True
    assert state.leased_by_machine_id is None
    assert state.status == "idle"

    state.release_workspace(leased_by_machine_id="other-machine", status="leased")
    assert state.workspace_owned is False
    assert state.leased_by_machine_id == "other-machine"
    assert state.status == "leased"

    state.begin_runtime(status="running")
    assert state.runtime_active is True
    assert state.runtime_stopping is False
    assert state.status == "running"

    state.stop_runtime(status="stopping")
    assert state.runtime_stopping is True
    assert state.status == "stopping"

    state.end_runtime(status="idle")
    assert state.runtime_active is False
    assert state.runtime_stopping is False
    assert state.status == "idle"
    assert state.engine_thread is None
    assert state.engine_runtime_stop_event.is_set() is False
    assert state.engine_flow_stop_event.is_set() is False

    state.set_checkpoint_time("2026-04-06T00:01:00+00:00", status="degraded")
    assert state.last_checkpoint_at_utc == "2026-04-06T00:01:00+00:00"
    assert state.status == "degraded"

    state.set_leased_by_machine_id("machine-b")
    assert state.leased_by_machine_id == "machine-b"
    assert state.increment_checkpoint_failures() == 1
    state.reset_checkpoint_failures()
    assert state.consecutive_checkpoint_failures == 0

    state.set_engine_threads(runtime_stop_event=runtime_stop_event, flow_stop_event=flow_stop_event, engine_thread=engine_thread)
    assert state.engine_runtime_stop_event is runtime_stop_event
    assert state.engine_flow_stop_event is flow_stop_event
    assert state.engine_thread is engine_thread

    state.end_runtime(status="idle")
    assert state.engine_thread is None
    assert state.engine_runtime_stop_event is not runtime_stop_event
    assert state.engine_flow_stop_event is not flow_stop_event
    assert state.engine_runtime_stop_event.is_set() is False
    assert state.engine_flow_stop_event.is_set() is False

    state.register_manual_run(
        "demo",
        thread=manual_thread,
        runtime_stop_event=manual_runtime_stop_event,
        flow_stop_event=manual_flow_stop_event,
    )
    assert state.manual_run_threads["demo"] is manual_thread
    assert state.manual_runtime_stop_events["demo"] is manual_runtime_stop_event
    assert state.manual_flow_stop_events["demo"] is manual_flow_stop_event
    state.unregister_manual_run("demo")
    assert state.manual_run_threads == {}
    assert state.manual_runtime_stop_events == {}
    assert state.manual_flow_stop_events == {}

    state.set_listener(object())
    assert state.listener is not None


def test_initialize_service_claims_workspace_and_records_idle_snapshot(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    initialize_service(service)
    try:
        metadata = read_lease_metadata(paths)
        assert metadata is not None
        assert metadata["machine_id"] == machine_id_text()
        assert metadata["status"] == "idle"
        assert service.host.workspace_owned is True
        assert service.host.status == "idle"
    finally:
        service._shutdown()  # noqa: SLF001


def test_initialize_service_enters_observer_mode_for_other_machine_lease(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True
    started = utcnow_text()
    checkpoint_workspace_state(
        paths,
        RuntimeCacheLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    service = DataEngineDaemonService(paths)
    initialize_service(service)
    try:
        assert service.host.workspace_owned is False
        assert service.host.leased_by_machine_id == "machine-a"
        assert service.host.status == "leased"
    finally:
        service._shutdown()  # noqa: SLF001


def test_stop_active_work_signals_running_threads_and_resets_runtime_state():
    class _State:
        def __init__(self) -> None:
            self.end_runtime_calls = 0
            self.last_status = None
            self.engine_runtime_stop_event = threading.Event()
            self.engine_flow_stop_event = threading.Event()
            self.manual_runtime_stop_events = {"manual": threading.Event()}
            self.manual_flow_stop_events = {"manual": threading.Event()}
            self.engine_thread = None
            self.manual_run_threads = {}

        def end_runtime(self, *, status: str = "idle") -> None:
            self.end_runtime_calls += 1
            self.last_status = status

    class _Service:
        def __init__(self) -> None:
            self._state_lock = threading.RLock()
            self.state = _State()
            self.state.engine_thread = threading.Thread(target=self._wait_for_engine_stop)
            self.state.manual_run_threads = {"manual": threading.Thread(target=self._wait_for_manual_stop)}
            self.published_events: list[str] = []

        def _wait_for_engine_stop(self) -> None:
            self.state.engine_runtime_stop_event.wait(timeout=1.0)

        def _wait_for_manual_stop(self) -> None:
            self.state.manual_runtime_stop_events["manual"].wait(timeout=1.0)

        def _publish_runtime_event(self, event_type: str) -> None:
            self.published_events.append(event_type)

    service = _Service()
    service.state.engine_thread.start()
    service.state.manual_run_threads["manual"].start()

    stop_active_work(service)  # noqa: SLF001 - direct lifecycle helper test

    assert service.state.engine_runtime_stop_event.is_set() is True
    assert service.state.engine_flow_stop_event.is_set() is True
    assert service.state.manual_runtime_stop_events["manual"].is_set() is True
    assert service.state.manual_flow_stop_events["manual"].is_set() is True
    assert service.state.engine_thread.is_alive() is False
    assert service.state.manual_run_threads["manual"].is_alive() is False
    assert service.state.end_runtime_calls == 1
    assert service.state.last_status == "idle"
    assert service.published_events == ["runtime.stopped"]


def test_daemon_status_includes_active_runs_from_runtime_execution_bridge(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        started_at = utcnow_text()
        service.runtime_execution_ledger.execution_state.record_run_started(
            run_id="run-1",
            flow_name="demo",
            group_name="Demo",
            source_path="claims.xlsx",
            started_at_utc=started_at,
        )
        service.runtime_execution_ledger.execution_state.record_step_started(
            run_id="run-1",
            flow_name="demo",
            step_label="Emit Value",
            started_at_utc=started_at,
        )

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001

        assert status["ok"] is True
        assert status["status"]["active_runs"] == [
            {
                "run_id": "run-1",
                "flow_name": "demo",
                "group_name": "Demo",
                "source_path": "claims.xlsx",
                "state": "running",
                "current_step_name": "Emit Value",
                "current_step_started_at_utc": started_at,
                "started_at_utc": started_at,
                "finished_at_utc": None,
                "elapsed_seconds": None,
                "error_text": None,
            }
        ]
        assert status["status"]["flow_activity"] == [
            {
                "flow_name": "demo",
                "active_run_count": 1,
                "queued_run_count": 0,
                "engine_run_count": 0,
                "manual_run_count": 1,
                "stopping_run_count": 0,
                "running_step_counts": {"Emit Value": 1},
            }
        ]
    finally:
        service._shutdown()  # noqa: SLF001


def test_runtime_execution_events_update_projection_without_full_state_refresh(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        started_at = utcnow_text()
        original_runtime_state_payload = service._runtime_state_payload
        monkeypatch.setattr(
            service,
            "_runtime_state_payload",
            lambda: (_ for _ in ()).throw(AssertionError("hot runtime events should not rebuild full state")),
        )

        service.runtime_execution_ledger.execution_state.record_run_started(
            run_id="run-1",
            flow_name="demo",
            group_name="Demo",
            source_path="claims.xlsx",
            started_at_utc=started_at,
        )
        service.runtime_execution_ledger.execution_state.record_step_started(
            run_id="run-1",
            flow_name="demo",
            step_label="Emit Value",
            started_at_utc=started_at,
        )

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001

        assert status["ok"] is True
        assert status["status"]["active_runs"] == [
            {
                "run_id": "run-1",
                "flow_name": "demo",
                "group_name": "Demo",
                "source_path": "claims.xlsx",
                "state": "running",
                "current_step_name": "Emit Value",
                "current_step_started_at_utc": started_at,
                "started_at_utc": started_at,
                "finished_at_utc": None,
                "elapsed_seconds": None,
                "error_text": None,
            }
        ]
    finally:
        monkeypatch.setattr(service, "_runtime_state_payload", original_runtime_state_payload)
        service._shutdown()  # noqa: SLF001


def test_initialize_service_reconciles_orphaned_active_runtime_rows(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started_at = utcnow_text()
    ledger.execution_state.record_run_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path="claims.xlsx",
        started_at_utc=started_at,
    )
    ledger.execution_state.record_step_started(
        run_id="run-1",
        flow_name="demo",
        step_label="Emit Value",
        started_at_utc=started_at,
    )
    ledger.close()

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001

        assert status["ok"] is True
        assert status["status"]["active_runs"] == []
        assert status["status"]["flow_activity"] == []
        assert service.runtime_cache_ledger.runs.list_active() == ()
        assert service.runtime_cache_ledger.step_outputs.list_active() == ()
    finally:
        service._shutdown()  # noqa: SLF001


def test_serve_workspace_daemon_passes_lifecycle_policy_to_service_type(tmp_path):
    workspace_root = tmp_path / "shared" / "default"

    calls: list[tuple[Path, DaemonLifecyclePolicy]] = []

    class _Service:
        def __init__(self, paths, *, lifecycle_policy: DaemonLifecyclePolicy) -> None:
            calls.append((paths.workspace_root, lifecycle_policy))

        def serve_forever(self) -> None:
            calls.append((workspace_root.resolve(), DaemonLifecyclePolicy.EPHEMERAL))

    result = serve_workspace_daemon(
        _Service,
        workspace_root=workspace_root,
        lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL,
    )

    assert result == 0
    assert calls[0] == (workspace_root.resolve(), DaemonLifecyclePolicy.EPHEMERAL)


def test_serve_workspace_daemon_uses_injected_workspace_service(tmp_path):
    workspace_root = tmp_path / "shared" / "default"
    calls: list[Path | None] = []

    class _WorkspaceService:
        def resolve_paths(self, *, workspace_root=None, workspace_id=None):
            del workspace_id
            calls.append(workspace_root)
            return resolve_workspace_paths(workspace_root=workspace_root)

    class _Service:
        def __init__(self, paths, *, lifecycle_policy: DaemonLifecyclePolicy) -> None:
            assert lifecycle_policy is DaemonLifecyclePolicy.EPHEMERAL
            self.paths = paths

        def serve_forever(self) -> None:
            assert self.paths.workspace_root == workspace_root.resolve()

    result = serve_workspace_daemon(
        _Service,
        workspace_root=workspace_root,
        lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL,
        workspace_service=_WorkspaceService(),
    )

    assert result == 0
    assert calls == [workspace_root]


def test_serve_workspace_daemon_uses_injected_resolve_paths_func(tmp_path):
    workspace_root = tmp_path / "shared" / "default"
    calls: list[tuple[Path | None, str | None]] = []
    resolved = resolve_workspace_paths(workspace_root=workspace_root)

    class _Service:
        def __init__(self, paths, *, lifecycle_policy: DaemonLifecyclePolicy) -> None:
            assert lifecycle_policy is DaemonLifecyclePolicy.EPHEMERAL
            self.paths = paths

        def serve_forever(self) -> None:
            assert self.paths is resolved

    result = serve_workspace_daemon(
        _Service,
        workspace_root=workspace_root,
        workspace_id="default",
        lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL,
        resolve_paths_func=lambda *, workspace_root=None, workspace_id=None: calls.append((workspace_root, workspace_id)) or resolved,
    )

    assert result == 0
    assert calls == [(workspace_root, "default")]


def test_daemon_main_uses_injected_resolve_paths_func(monkeypatch, tmp_path):
    workspace_root = tmp_path / "shared" / "default"
    resolved = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="default")
    resolve_calls: list[tuple[Path | None, str | None]] = []
    serve_calls: list[tuple[Path, str, str]] = []

    monkeypatch.setattr(
        "data_engine.hosts.daemon.app.serve_workspace_daemon",
        lambda **kwargs: serve_calls.append(
            (
                kwargs["workspace_root"],
                kwargs["workspace_id"],
                kwargs["lifecycle_policy"],
            )
        )
        or 0,
    )

    result = daemon_main(
        [
            "--workspace",
            str(workspace_root),
            "--workspace-id",
            "default",
            "--lifecycle-policy",
            "ephemeral",
        ],
        resolve_paths_func=lambda *, workspace_root=None, workspace_id=None: resolve_calls.append((workspace_root, workspace_id)) or resolved,
    )

    assert result == 0
    assert resolve_calls == [(workspace_root.resolve(), "default")]
    assert serve_calls == [(resolved.workspace_root, resolved.workspace_id, "ephemeral")]


def test_serve_forever_processes_one_command_then_shuts_down(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    class _Connection:
        def __init__(self) -> None:
            self.sent_payloads: list[bytes] = []

        def recv_bytes(self) -> bytes:
            return _encode_message({"command": "daemon_ping"})

        def send_bytes(self, payload: bytes) -> None:
            self.sent_payloads.append(payload)
            service.host.shutdown_event.set()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Listener:
        def __init__(self, *args, **kwargs) -> None:
            self.connection = _Connection()

        def accept(self):
            return self.connection

        def close(self):
            return None

    class _Service:
        def __init__(self, paths) -> None:
            self.paths = paths
            self.initialize_calls = 0
            self.handle_calls: list[dict[str, object]] = []
            self.shutdown_calls = 0
            self.state = type("_State", (), {"checkpoint_thread": None})()
            self.host = type(
                "_Host",
                (),
                {"shutdown_event": threading.Event(), "listener": None},
            )()

        def initialize(self) -> None:
            self.initialize_calls += 1

        def _checkpoint_loop(self) -> None:
            return None

        def _debug_log(self, message: str) -> None:
            del message

        def _handle_command(self, payload):
            self.handle_calls.append(payload)
            return {"ok": True, "command": payload.get("command")}

        def _shutdown(self) -> None:
            self.shutdown_calls += 1

    service = _Service(paths)
    monkeypatch.setattr("data_engine.hosts.daemon.server.Listener", _Listener)

    serve_forever(service)  # noqa: SLF001 - direct server loop test

    assert service.initialize_calls == 1
    assert service.handle_calls == [{"command": "daemon_ping"}]
    assert service.shutdown_calls == 1


def test_serve_forever_handles_second_request_while_first_is_still_running(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    first_started = threading.Event()
    release_first = threading.Event()
    second_handled = threading.Event()

    class _Connection:
        def __init__(self, command: str) -> None:
            self.command = command
            self.sent_payloads: list[bytes] = []

        def recv_bytes(self) -> bytes:
            return _encode_message({"command": self.command})

        def send_bytes(self, payload: bytes) -> None:
            self.sent_payloads.append(payload)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Listener:
        def __init__(self, *args, **kwargs) -> None:
            self._connections = [_Connection("run_flow"), _Connection("daemon_status")]

        def accept(self):
            if self._connections:
                return self._connections.pop(0)
            while not service.host.shutdown_event.wait(0.01):
                continue
            raise OSError("listener closed")

        def close(self):
            return None

    class _Service:
        def __init__(self, paths) -> None:
            self.paths = paths
            self.initialize_calls = 0
            self.shutdown_calls = 0
            self.state = type("_State", (), {"checkpoint_thread": None})()
            self.host = type(
                "_Host",
                (),
                {"shutdown_event": threading.Event(), "listener": None},
            )()

        def initialize(self) -> None:
            self.initialize_calls += 1

        def _checkpoint_loop(self) -> None:
            return None

        def _debug_log(self, message: str) -> None:
            del message

        def _handle_command(self, payload):
            command = payload.get("command")
            if command == "run_flow":
                first_started.set()
                release_first.wait(timeout=1.0)
                return {"ok": True, "command": command}
            if command == "daemon_status":
                second_handled.set()
                self.host.shutdown_event.set()
                return {"ok": True, "command": command}
            return {"ok": True, "command": command}

        def _shutdown(self) -> None:
            self.shutdown_calls += 1

    service = _Service(paths)
    monkeypatch.setattr("data_engine.hosts.daemon.server.Listener", _Listener)

    server_thread = threading.Thread(target=serve_forever, args=(service,), daemon=True)
    server_thread.start()
    try:
        assert first_started.wait(timeout=1.0) is True
        assert second_handled.wait(timeout=1.0) is True
    finally:
        release_first.set()
        service.host.shutdown_event.set()
        server_thread.join(timeout=1.0)

    assert service.initialize_calls == 1
    assert service.shutdown_calls == 1


def test_daemon_initialize_writes_lease_metadata_before_first_checkpoint(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    monkeypatch.setattr(service, "_checkpoint_once", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        service.initialize()

    metadata = read_lease_metadata(paths)
    assert metadata is not None
    assert metadata["status"] == "starting"
    assert metadata["machine_id"] == machine_id_text()


def test_daemon_service_can_start_in_observer_mode_when_workspace_is_leased(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True
    started = utcnow_text()
    checkpoint_workspace_state(
        paths,
        RuntimeCacheLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["ok"] is True
        assert status["status"]["status"] == "leased"
        assert status["status"]["workspace_owned"] is False
        assert status["status"]["leased_by_machine_id"] == "machine-a"

        flows = service._handle_command({"command": "list_flows"})  # noqa: SLF001
        assert flows["ok"] is True
        assert [item["name"] for item in flows["flows"]] == ["demo"]

        denied = service._handle_command({"command": "start_engine"})  # noqa: SLF001
        assert denied["ok"] is False
        assert "leased by machine-a" in denied["error"]
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_service_reclaims_unreachable_same_machine_lease(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True
    started = (datetime.now(UTC) - timedelta(seconds=120)).isoformat()
    checkpoint_workspace_state(
        paths,
        RuntimeCacheLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id=machine_id_text(),
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["ok"] is True
        assert status["status"]["status"] == "idle"
        assert status["status"]["workspace_owned"] is True
        assert status["status"]["leased_by_machine_id"] is None
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_debug_log_keeps_only_last_30_days(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)

    now = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)
    old = (now - timedelta(days=DAEMON_LOG_RETENTION_DAYS + 1)).isoformat()
    recent = (now - timedelta(days=2)).isoformat()
    paths.runtime_state_dir.mkdir(parents=True, exist_ok=True)
    paths.daemon_log_path.write_text(
        f"{old} pid=1 workspace=default old entry\n"
        f"{recent} pid=1 workspace=default recent entry\n"
        "not-a-timestamp pid=1 workspace=default malformed entry\n",
        encoding="utf-8",
    )

    monkeypatch.setattr("data_engine.hosts.daemon.app.datetime", type("_FrozenDateTime", (), {"now": staticmethod(lambda tz=None: now)}))
    monkeypatch.setattr("data_engine.hosts.daemon.app.utcnow_text", lambda: now.isoformat())

    service._debug_log("fresh entry")  # noqa: SLF001

    contents = paths.daemon_log_path.read_text(encoding="utf-8")
    assert "old entry" not in contents
    assert "recent entry" in contents
    assert "malformed entry" in contents
    assert "fresh entry" in contents
