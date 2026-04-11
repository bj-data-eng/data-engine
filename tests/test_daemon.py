from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import os
import subprocess
import threading
from types import SimpleNamespace

import pytest

import data_engine.hosts.daemon.client as daemon_client
from data_engine.authoring.builder import Flow
from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.app import (
    DAEMON_LOG_RETENTION_DAYS,
    DataEngineDaemonService,
    WorkspaceLeaseError,
    _remove_stale_unix_endpoint,
    main as daemon_main,
    spawn_daemon_process,
)
from data_engine.hosts.daemon.bootstrap import initialize_service
from data_engine.hosts.daemon.composition import (
    DaemonHostDependencyFactories,
    DaemonHostDependencies,
    DaemonHostIdentity,
    DaemonHostState,
)
from data_engine.hosts.daemon.client import (
    DaemonClientError,
    _decode_message,
    _encode_message,
    _pid_is_live,
    daemon_authkey,
    force_shutdown_daemon_process,
)
from data_engine.hosts.daemon.lifecycle import relinquish_workspace_for_control_request
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, _lease_pid_is_live
from data_engine.hosts.daemon.ownership import honor_control_request_if_needed, try_claim_requested_control
from data_engine.hosts.daemon.runtime_control import stop_active_work
from data_engine.hosts.daemon.server import serve_forever, serve_workspace_daemon
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR, machine_id_text
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.runtime_db import RuntimeControlLedger, RuntimeLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state,
    claim_workspace,
    initialize_workspace_state,
    read_control_request,
    read_lease_metadata,
    release_workspace,
    remove_lease_metadata,
    write_control_request,
)
from data_engine.views.models import QtFlowCard


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def _write_demo_flow(workspace_root: Path) -> None:
    flow_dir = workspace_root / "flow_modules"
    flow_dir.mkdir(parents=True, exist_ok=True)
    (flow_dir / "demo.py").write_text(
        """
from data_engine import Flow

DESCRIPTION = "Simple daemon test flow."

def emit_value(context):
    return 1

def build():
    return Flow(name="demo", label="demo", group="Demo").step(emit_value, label="Emit Value")
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_blocking_group_flows(workspace_root: Path) -> None:
    flow_dir = workspace_root / "flow_modules"
    flow_dir.mkdir(parents=True, exist_ok=True)
    (flow_dir / "alpha.py").write_text(
        """
from data_engine import Flow

DESCRIPTION = "Blocking group flow alpha."

def build():
    return Flow(name="alpha", label="alpha", group="Shared").step(lambda context: 1, label="Emit Alpha")
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (flow_dir / "beta.py").write_text(
        """
from data_engine import Flow

DESCRIPTION = "Blocking group flow beta."

def build():
    return Flow(name="beta", label="beta", group="Shared").step(lambda context: 2, label="Emit Beta")
""".strip()
        + "\n",
        encoding="utf-8",
    )


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


def test_daemon_host_dependencies_build_default_opens_workspace_runtime_ledger(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    dependencies = DaemonHostDependencies.build_default(paths)
    try:
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
    manual_stop_event = threading.Event()

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

    state.register_manual_run("demo", thread=manual_thread, stop_event=manual_stop_event)
    assert state.manual_run_threads["demo"] is manual_thread
    assert state.manual_stop_events["demo"] is manual_stop_event
    state.unregister_manual_run("demo")
    assert state.manual_run_threads == {}
    assert state.manual_stop_events == {}

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
        RuntimeLedger(paths.runtime_db_path),
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
            self.manual_stop_events = {"manual": threading.Event()}
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

        def _wait_for_engine_stop(self) -> None:
            self.state.engine_runtime_stop_event.wait(timeout=1.0)

        def _wait_for_manual_stop(self) -> None:
            self.state.manual_stop_events["manual"].wait(timeout=1.0)

    service = _Service()
    service.state.engine_thread.start()
    service.state.manual_run_threads["manual"].start()

    stop_active_work(service)  # noqa: SLF001 - direct lifecycle helper test

    assert service.state.engine_runtime_stop_event.is_set() is True
    assert service.state.engine_flow_stop_event.is_set() is True
    assert service.state.manual_stop_events["manual"].is_set() is True
    assert service.state.engine_thread.is_alive() is False
    assert service.state.manual_run_threads["manual"].is_alive() is False
    assert service.state.end_runtime_calls == 1
    assert service.state.last_status == "idle"


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
        RuntimeLedger(paths.runtime_db_path),
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
        RuntimeLedger(paths.runtime_db_path),
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


def test_workspace_daemon_manager_auto_recovers_dead_same_machine_lease(tmp_path, monkeypatch):
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
        RuntimeLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id=machine_id_text(),
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.manager._lease_pid_is_live", lambda metadata: False)

    manager = WorkspaceDaemonManager(paths)
    snapshot = manager.sync()

    assert snapshot.workspace_owned is True
    assert snapshot.leased_by_machine_id is None


def test_lease_pid_is_live_delegates_to_pid_helper(monkeypatch):
    metadata = {"pid": 123}

    monkeypatch.setattr("data_engine.hosts.daemon.manager._pid_is_live", lambda pid: pid == 123)

    assert _lease_pid_is_live(metadata) is True
    assert _lease_pid_is_live({"pid": 456}) is False


def test_pid_is_live_uses_windows_helper_without_ps(monkeypatch):
    monkeypatch.setattr("data_engine.hosts.daemon.client.os.name", "nt")
    monkeypatch.setattr("data_engine.hosts.daemon.client.process_is_running", lambda pid: pid == 123)
    monkeypatch.setattr(
        "data_engine.platform.processes.subprocess.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Windows PID checks should not use ps")),
    )

    assert _pid_is_live(123) is True
    assert _pid_is_live(456) is False


def test_force_shutdown_daemon_process_kills_local_pid_and_cleans_up_lease(tmp_path, monkeypatch):
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
        RuntimeLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id=machine_id_text(),
        daemon_id="daemon-a",
        pid=321,
        status="running",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    endpoint_path = Path(paths.daemon_endpoint_path)
    if paths.daemon_endpoint_kind == "unix":
        endpoint_path.parent.mkdir(parents=True, exist_ok=True)
        endpoint_path.write_text("", encoding="utf-8")
    killed_pids: list[int] = []
    pid_live = {"value": True}

    monkeypatch.setattr(
        "data_engine.hosts.daemon.client.daemon_request",
        lambda paths, payload, timeout=0.0: (_ for _ in ()).throw(DaemonClientError("unreachable")),
    )
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client.time.sleep", lambda _seconds: None)
    monkeypatch.setattr("data_engine.hosts.daemon.client._pid_is_live", lambda pid: pid_live["value"])

    def _kill(pid: int) -> None:
        killed_pids.append(pid)
        pid_live["value"] = False

    monkeypatch.setattr("data_engine.hosts.daemon.client._kill_pid", _kill)

    force_shutdown_daemon_process(paths)

    assert killed_pids == [321]
    assert read_lease_metadata(paths) is None
    if paths.daemon_endpoint_kind == "unix":
        assert endpoint_path.exists() is False


def test_checkpoint_once_raises_when_local_daemon_state_write_fails(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        calls: list[str] = []

        def _checkpoint_workspace_state(*args, **kwargs):
            del args, kwargs
            calls.append("shared")

        monkeypatch.setattr(
            service.shared_state_adapter,
            "checkpoint_workspace_state",
            _checkpoint_workspace_state,
        )
        monkeypatch.setattr(
            service.runtime_control_ledger,
            "upsert_daemon_state",
            lambda **kwargs: (_ for _ in ()).throw(PermissionError("db locked")),
        )

        with pytest.raises(PermissionError, match="db locked"):
            service._checkpoint_once(status="idle")  # noqa: SLF001 - ownership-critical checkpointing must fail hard

        assert calls == ["shared"]
        assert service.host.workspace_owned is True
    finally:
        service._shutdown()  # noqa: SLF001 - direct daemon lifecycle test


def test_force_shutdown_daemon_process_returns_when_nothing_is_running(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)

    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)

    force_shutdown_daemon_process(paths)


def test_spawn_daemon_process_waits_for_fresh_same_machine_startup(tmp_path, monkeypatch):
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
        RuntimeLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id=machine_id_text(),
        daemon_id="daemon-a",
        pid=101,
        status="starting",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    live_checks = iter([False, False, True])
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: next(live_checks))
    monkeypatch.setattr("data_engine.hosts.daemon.client.time.sleep", lambda _seconds: None)

    def _fail_popen(*args, **kwargs):
        raise AssertionError("spawn_daemon_process should not launch a second daemon during startup grace")

    monkeypatch.setattr("data_engine.hosts.daemon.client.subprocess.Popen", _fail_popen)

    assert spawn_daemon_process(paths) == 0


def test_spawn_daemon_process_does_not_recover_recent_same_machine_unreachable_lease(tmp_path, monkeypatch):
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
        RuntimeLedger(paths.runtime_db_path),
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
    monkeypatch.setattr("data_engine.hosts.daemon.client.time.sleep", lambda _seconds: None)

    def _fail_popen(*args, **kwargs):
        raise AssertionError("spawn_daemon_process should not launch over a recent local lease")

    monkeypatch.setattr("data_engine.hosts.daemon.client.subprocess.Popen", _fail_popen)

    with pytest.raises(Exception) as excinfo:
        spawn_daemon_process(paths)
    assert "already has control" in str(excinfo.value)


def test_spawn_daemon_process_does_not_launch_duplicate_local_owner(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True
    started = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    checkpoint_workspace_state(
        paths,
        RuntimeLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id=machine_id_text(),
        daemon_id="daemon-a",
        pid=99999,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    monkeypatch.setattr("data_engine.hosts.daemon.client._pid_is_live", lambda pid: pid == 99999)
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client.time.sleep", lambda _seconds: None)

    def _fail_popen(*args, **kwargs):
        raise AssertionError("spawn_daemon_process should not launch a duplicate local daemon")

    monkeypatch.setattr("data_engine.hosts.daemon.client.subprocess.Popen", _fail_popen)

    with pytest.raises(Exception) as excinfo:
        spawn_daemon_process(paths)
    assert "already owns this workspace" in str(excinfo.value)


def test_spawn_daemon_process_uses_windows_creation_flags(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    monkeypatch.setattr("data_engine.hosts.daemon.client.os.name", "nt")
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client._wait_for_fresh_local_daemon", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client._same_machine_live_lease_process", lambda paths: None)
    monkeypatch.setattr("data_engine.hosts.daemon.client._should_force_recover_local_lease", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client._same_machine_unreachable_lease_metadata", lambda paths: None)
    monkeypatch.setattr("data_engine.hosts.daemon.client._acquire_startup_lock", lambda paths: True)
    monkeypatch.setattr("data_engine.hosts.daemon.client._wait_for_daemon_live", lambda paths, timeout_seconds: True)

    captured: dict[str, object] = {}

    def _fake_popen(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr("data_engine.hosts.daemon.client.subprocess.Popen", _fake_popen)

    assert spawn_daemon_process(paths) == 0
    assert captured["command"][1:3] == ["-m", "data_engine.hosts.daemon.app"]
    assert "creationflags" in captured["kwargs"]
    assert captured["kwargs"]["creationflags"] != 0
    assert "start_new_session" not in captured["kwargs"]


def test_remove_stale_unix_endpoint_deletes_dead_socket_file(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    if paths.daemon_endpoint_kind != "unix":
        pytest.skip("Unix socket cleanup only applies on unix endpoints.")
    endpoint_path = Path(paths.daemon_endpoint_path)
    endpoint_path.parent.mkdir(parents=True, exist_ok=True)
    endpoint_path.write_text("stale", encoding="utf-8")
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)

    _remove_stale_unix_endpoint(paths)

    assert endpoint_path.exists() is False


def test_daemon_authkey_is_stable_per_workspace(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    first = daemon_authkey(paths)
    second = daemon_authkey(paths)

    assert first == second
    assert len(first) == 32


def test_daemon_authkey_hardens_created_file(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    hardened: list[Path] = []
    monkeypatch.setattr("data_engine.hosts.daemon.client._harden_private_file_permissions", lambda path: hardened.append(path))

    authkey = daemon_authkey(paths)

    assert len(authkey) == 32
    assert hardened == [paths.runtime_state_dir / daemon_client.DAEMON_AUTHKEY_FILE_NAME]


def test_daemon_message_encoding_requires_json_object():
    encoded = _encode_message({"command": "daemon_ping", "ok": True})

    assert _decode_message(encoded) == {"command": "daemon_ping", "ok": True}

    with pytest.raises(Exception, match="JSON object"):
        _encode_message(["daemon_ping"])  # type: ignore[arg-type]


def test_daemon_service_refuses_same_machine_observer_mode(tmp_path, monkeypatch):
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
        RuntimeLedger(paths.runtime_db_path),
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
    with pytest.raises(WorkspaceLeaseError, match="already leased locally"):
        service.initialize()


def test_manual_run_does_not_break_daemon_shutdown_cleanup(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    run_response = service._handle_command({"command": "run_flow", "name": "demo", "wait": True})  # noqa: SLF001
    assert run_response["ok"] is True

    service._checkpoint_once(status="idle")  # noqa: SLF001 - proves daemon ledger remains usable after the manual run
    service._shutdown()  # noqa: SLF001

    assert read_lease_metadata(paths) is None
    assert (paths.available_markers_dir / paths.workspace_id).exists() is True
    assert (paths.leased_markers_dir / paths.workspace_id).exists() is False


def test_run_flow_rejects_second_manual_run_in_same_group(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_blocking_group_flows(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    release_gate = threading.Event()
    started = threading.Event()

    def _run_manual(flow, *, runtime_ledger, flow_stop_event):
        del flow, runtime_ledger, flow_stop_event
        started.set()
        release_gate.wait(timeout=1.0)

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        first = service._handle_command({"command": "run_flow", "name": "alpha", "wait": False})  # noqa: SLF001
        assert first["ok"] is True
        assert started.wait(timeout=1.0) is True

        second = service._handle_command({"command": "run_flow", "name": "beta", "wait": False})  # noqa: SLF001
        assert second["ok"] is False
        assert "Group Shared already has alpha running." == second["error"]
    finally:
        release_gate.set()
        service._shutdown()  # noqa: SLF001


def test_control_handoff_stops_in_flight_manual_run(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    stop_seen = threading.Event()
    release_gate = threading.Event()

    def _run_manual(flow, *, runtime_ledger, flow_stop_event):
        del flow, runtime_ledger
        flow_stop_event.wait(timeout=1.0)
        if flow_stop_event.is_set():
            stop_seen.set()
        release_gate.wait(timeout=1.0)

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001
        assert response["ok"] is True

        relinquish_workspace_for_control_request(service, "machine-b")
        release_gate.set()

        assert stop_seen.wait(timeout=1.0) is True
        assert service.host.workspace_owned is False
        assert service.host.shutdown_event.is_set() is True
    finally:
        release_gate.set()
        service._shutdown()  # noqa: SLF001


def test_daemon_service_honors_control_request_from_another_machine(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        write_control_request(
            paths,
            workspace_id="default",
            requester_machine_id="machine-b",
            requester_host_name="machine-b",
            requester_pid=202,
            requester_client_kind="ui",
            requested_at_utc=utcnow_text(),
        )

        assert honor_control_request_if_needed(service) is True
        assert service.host.workspace_owned is False
        assert service.host.leased_by_machine_id == "machine-b"
        assert service.host.shutdown_event.is_set() is True
        assert read_lease_metadata(paths) is None
        assert (paths.available_markers_dir / paths.workspace_id).exists() is True
    finally:
        service._shutdown()  # noqa: SLF001


def test_observer_daemon_claims_workspace_after_local_control_request(tmp_path, monkeypatch):
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
        RuntimeLedger(paths.runtime_db_path),
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
        assert service.host.workspace_owned is False
        write_control_request(
            paths,
            workspace_id="default",
            requester_machine_id=machine_id_text(),
            requester_host_name=machine_id_text(),
            requester_pid=303,
            requester_client_kind="ui",
            requested_at_utc=utcnow_text(),
        )
        remove_lease_metadata(paths)
        release_workspace(paths)

        assert try_claim_requested_control(service) is True
        assert service.host.workspace_owned is True
        assert read_control_request(paths) is None
    finally:
        service._shutdown()  # noqa: SLF001


def test_start_engine_retries_after_empty_automated_flow_snapshot(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        calls: list[bool] = []

        def _fake_load_flow_cards(*, force: bool = False):
            calls.append(force)
            if len(calls) == 1:
                return ()
            return (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Recovered automated flow.",
                    source_root="/tmp/input",
                    target_root="/tmp/output",
                    mode="poll",
                    interval="5s",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="poll ready",
                    valid=True,
                    category="automated",
                ),
            )

        monkeypatch.setattr(service, "_load_flow_cards", _fake_load_flow_cards)
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: Flow(name=name, group="Demo").step(lambda context: 1, label="Emit Value"),
        )
        monkeypatch.setattr(
            service.runtime_execution_service,
            "run_grouped",
            lambda flows, runtime_ledger, runtime_stop_event, flow_stop_event: [],
        )

        response = service._handle_command({"command": "start_engine"})  # noqa: SLF001

        assert response["ok"] is True
        assert calls == [True, True]
    finally:
        service._shutdown()  # noqa: SLF001


def test_run_flow_returns_build_failure_details_without_starting_thread(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: (_ for _ in ()).throw(RuntimeError("build boom")),
        )

        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001

        assert response["ok"] is False
        assert response["error"] == "build boom"
        assert service.state.manual_run_threads == {}
    finally:
        service._shutdown()  # noqa: SLF001


def test_run_flow_rejects_duplicate_start_while_first_start_is_loading(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    load_started = threading.Event()
    release_load = threading.Event()
    execution_started = threading.Event()

    def _load_flow(name, workspace_root=None):
        del name, workspace_root
        load_started.set()
        release_load.wait(timeout=1.0)
        return Flow(name="demo", group="Demo").step(lambda context: 1, label="Emit Value")

    def _run_manual(flow, *, runtime_ledger, flow_stop_event):
        del flow, runtime_ledger, flow_stop_event
        execution_started.set()

    monkeypatch.setattr(service.flow_execution_service, "load_flow", _load_flow)
    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        first_result: dict[str, object] = {}
        first_thread = threading.Thread(
            target=lambda: first_result.update(service._handle_command({"command": "run_flow", "name": "demo", "wait": False})),  # noqa: SLF001
            daemon=True,
        )
        first_thread.start()
        assert load_started.wait(timeout=1.0) is True

        second = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001

        release_load.set()
        first_thread.join(timeout=1.0)

        assert first_result["ok"] is True
        assert second["ok"] is False
        assert second["error"] == "Flow demo is already running."
        assert execution_started.wait(timeout=1.0) is True
    finally:
        release_load.set()
        service._shutdown()  # noqa: SLF001


def test_start_engine_returns_build_failure_details(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        monkeypatch.setattr(
            service,
            "_load_flow_cards",
            lambda force=False: (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Broken automated flow.",
                    source_root="/tmp/input",
                    target_root="/tmp/output",
                    mode="poll",
                    interval="5s",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="poll ready",
                    valid=True,
                    category="automated",
                ),
            ),
        )
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: (_ for _ in ()).throw(RuntimeError(f"{name} build boom")),
        )

        response = service._handle_command({"command": "start_engine"})  # noqa: SLF001

        assert response["ok"] is False
        assert response["error"] == "demo_poll build boom"
    finally:
        service._shutdown()  # noqa: SLF001


def test_start_engine_coalesces_duplicate_start_while_first_start_is_loading(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        monkeypatch.setattr(
            service,
            "_load_flow_cards",
            lambda force=False: (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Automated flow.",
                    source_root="/tmp/input",
                    target_root="/tmp/output",
                    mode="poll",
                    interval="5s",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="poll ready",
                    valid=True,
                    category="automated",
                ),
            ),
        )
        load_started = threading.Event()
        release_load = threading.Event()
        load_calls: list[tuple[str, ...]] = []

        def _load_flows(flow_names, workspace_root=None):
            del workspace_root
            load_calls.append(tuple(flow_names))
            load_started.set()
            release_load.wait(timeout=1.0)
            return [Flow(name=flow_name, group="Demo").step(lambda context: 1, label="Emit Value") for flow_name in flow_names]

        monkeypatch.setattr(service.flow_execution_service, "load_flows", _load_flows)
        monkeypatch.setattr(
            service.runtime_execution_service,
            "run_grouped",
            lambda flows, runtime_ledger, runtime_stop_event, flow_stop_event: [],
        )

        first_result: dict[str, object] = {}
        first_thread = threading.Thread(
            target=lambda: first_result.update(service._handle_command({"command": "start_engine"})),  # noqa: SLF001
            daemon=True,
        )
        first_thread.start()
        assert load_started.wait(timeout=1.0) is True

        second = service._handle_command({"command": "start_engine"})  # noqa: SLF001

        release_load.set()
        first_thread.join(timeout=1.0)

        assert first_result["ok"] is True
        assert second["ok"] is True
        assert load_calls == [("demo_poll",)]
    finally:
        release_load.set()
        service._shutdown()  # noqa: SLF001


def test_run_flow_refreshes_flow_cards_before_lookup(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        calls: list[bool] = []

        def _fake_load_flow_cards(*, force: bool = False):
            calls.append(force)
            return (
                QtFlowCard(
                    name="demo",
                    group="Demo",
                    title="Demo",
                    description="Freshly loaded flow.",
                    source_root="(not set)",
                    target_root="(not set)",
                    mode="manual",
                    interval="-",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="manual",
                    valid=True,
                    category="manual",
                ),
            )

        monkeypatch.setattr(service, "_load_flow_cards", _fake_load_flow_cards)
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: Flow(name=name, group="Demo").step(lambda context: 1, label="Emit Value"),
        )

        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": True})  # noqa: SLF001

        assert response["ok"] is True
        assert calls == [True]
    finally:
        service._shutdown()  # noqa: SLF001


def test_shutdown_releases_workspace_even_if_final_checkpoint_fails(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    monkeypatch.setattr(service, "_checkpoint_once", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    service._shutdown()  # noqa: SLF001

    assert read_lease_metadata(paths) is None
    assert (paths.available_markers_dir / paths.workspace_id).exists() is True
    assert (paths.leased_markers_dir / paths.workspace_id).exists() is False


def test_shutdown_creates_runtime_snapshot_parquets(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": True})  # noqa: SLF001
        assert response["ok"] is True

        for path in (paths.shared_runs_path, paths.shared_step_runs_path, paths.shared_logs_path):
            if path.exists():
                path.unlink()

        assert paths.shared_runs_path.exists() is False
        assert paths.shared_step_runs_path.exists() is False
        assert paths.shared_logs_path.exists() is False

        service._shutdown()  # noqa: SLF001

        assert paths.shared_runs_path.exists() is True
        assert paths.shared_step_runs_path.exists() is True
        assert paths.shared_logs_path.exists() is True
    finally:
        service._shutdown()  # noqa: SLF001


def test_shutdown_request_returns_without_waking_listener(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        monkeypatch.setattr(service, "_wake_listener", lambda: (_ for _ in ()).throw(AssertionError("shutdown request should not wake the listener synchronously")))

        response = service._handle_command({"command": "shutdown_daemon"})  # noqa: SLF001

        assert response["ok"] is True
        assert service.host.shutdown_event.is_set() is True
    finally:
        service._shutdown()  # noqa: SLF001


def test_checkpoint_failures_release_workspace_and_request_shutdown(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    service.host.runtime_active = True
    service.host.status = "running"

    class _SequenceEvent:
        def __init__(self) -> None:
            self.calls = 0
            self._set = False

        def wait(self, _seconds: float) -> bool:
            self.calls += 1
            return self._set or self.calls >= 4

        def is_set(self) -> bool:
            return self._set

        def set(self) -> None:
            self._set = True

    service.host.shutdown_event = _SequenceEvent()  # type: ignore[assignment]
    monkeypatch.setattr(service, "_checkpoint_once", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    tick = {"value": 0.0}

    def _fake_monotonic() -> float:
        tick["value"] += 31.0
        return tick["value"]

    monkeypatch.setattr("data_engine.hosts.daemon.lifecycle.time.monotonic", _fake_monotonic)

    service._checkpoint_loop()  # noqa: SLF001 - direct lifecycle failure policy test

    assert service.host.shutdown_event.is_set() is True
    assert service.host.workspace_owned is False
    assert service.host.status == "failed"
    assert service.host.runtime_active is False
    assert service.state.consecutive_checkpoint_failures == 3
    assert read_lease_metadata(paths) is None
    assert (paths.available_markers_dir / paths.workspace_id).exists() is True
    assert (paths.leased_markers_dir / paths.workspace_id).exists() is False

    service._shutdown()  # noqa: SLF001


def test_observer_daemon_requests_shutdown_when_lease_disappears(tmp_path, monkeypatch):
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
        RuntimeLedger(paths.runtime_db_path),
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
        assert service.host.workspace_owned is False
        remove_lease_metadata(paths)
        release_workspace(paths)

        service._refresh_observer_snapshot()  # noqa: SLF001

        assert service.host.shutdown_event.is_set() is True
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_requests_shutdown_when_workspace_root_is_moved(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    moved_root = tmp_path / "shared" / "default_moved"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    service.host.runtime_active = True
    service.host.status = "running"

    class _SequenceEvent:
        def __init__(self) -> None:
            self.calls = 0
            self._set = False

        def wait(self, _seconds: float) -> bool:
            self.calls += 1
            return self._set or self.calls >= 2

        def is_set(self) -> bool:
            return self._set

        def set(self) -> None:
            self._set = True

    service.host.shutdown_event = _SequenceEvent()  # type: ignore[assignment]
    workspace_root.rename(moved_root)

    service._checkpoint_loop()  # noqa: SLF001 - direct lifecycle relocation test

    assert service.host.shutdown_event.is_set() is True
    assert service.host.workspace_owned is False
    assert service.host.runtime_active is False
    assert service.host.status == "workspace missing"

    service._shutdown()  # noqa: SLF001


def test_ephemeral_daemon_stays_alive_when_no_live_clients_remain_during_active_runtime(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths, lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL)
    service.initialize()
    service.host.runtime_active = True
    service.host.status = "running"

    class _SequenceEvent:
        def __init__(self) -> None:
            self.calls = 0
            self._set = False

        def wait(self, _seconds: float) -> bool:
            self.calls += 1
            return self._set or self.calls >= 2

        def is_set(self) -> bool:
            return self._set

        def set(self) -> None:
            self._set = True

    service.host.shutdown_event = _SequenceEvent()  # type: ignore[assignment]
    monkeypatch.setattr(service.runtime_control_ledger, "count_live_client_sessions", lambda workspace_id: 0)

    service._checkpoint_loop()  # noqa: SLF001 - direct lifecycle ephemeral policy test

    assert service.host.shutdown_event.is_set() is False
    assert service.host.workspace_owned is True
    assert service.host.runtime_active is True
    assert service.host.status == "running"

    service._shutdown()  # noqa: SLF001


def test_ephemeral_idle_daemon_requests_shutdown_when_no_live_clients_remain(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths, lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL)
    service.initialize()
    service.host.status = "idle"

    class _SequenceEvent:
        def __init__(self) -> None:
            self.calls = 0
            self._set = False

        def wait(self, _seconds: float) -> bool:
            self.calls += 1
            return self._set or self.calls >= 2

        def is_set(self) -> bool:
            return self._set

        def set(self) -> None:
            self._set = True

    service.host.shutdown_event = _SequenceEvent()  # type: ignore[assignment]
    monkeypatch.setattr(service.runtime_control_ledger, "count_live_client_sessions", lambda workspace_id: 0)

    service._checkpoint_loop()  # noqa: SLF001 - direct lifecycle ephemeral policy test

    assert service.host.shutdown_event.is_set() is True
    assert service.host.workspace_owned is False
    assert service.host.runtime_active is False
    assert service.host.status == "client disconnected"

    service._shutdown()  # noqa: SLF001


def test_persistent_daemon_stays_alive_when_no_live_clients_remain(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths, lifecycle_policy=DaemonLifecyclePolicy.PERSISTENT)
    service.initialize()
    service.host.runtime_active = True
    service.host.status = "running"

    class _SequenceEvent:
        def __init__(self) -> None:
            self.calls = 0
            self._set = False

        def wait(self, _seconds: float) -> bool:
            self.calls += 1
            return self._set or self.calls >= 2

        def is_set(self) -> bool:
            return self._set

        def set(self) -> None:
            self._set = True

    service.host.shutdown_event = _SequenceEvent()  # type: ignore[assignment]
    monkeypatch.setattr(service.runtime_control_ledger, "count_live_client_sessions", lambda workspace_id: 0)

    service._checkpoint_loop()  # noqa: SLF001 - direct lifecycle persistent policy test

    assert service.host.shutdown_event.is_set() is False
    assert service.host.workspace_owned is True
    assert service.host.runtime_active is True
    assert service.host.status == "running"

    service._shutdown()  # noqa: SLF001


def test_spawn_daemon_process_waits_on_existing_startup_lock(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    monkeypatch.setattr("data_engine.hosts.daemon.client.os.name", "posix")
    paths.runtime_state_dir.mkdir(parents=True, exist_ok=True)
    (paths.runtime_state_dir / ".daemon-start.lock").write_text("123", encoding="utf-8")

    live_checks = iter([False, False, True])
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: next(live_checks))
    monkeypatch.setattr("data_engine.hosts.daemon.client.time.sleep", lambda _seconds: None)

    def _fail_popen(*args, **kwargs):
        raise AssertionError("spawn_daemon_process should not launch when another process holds the startup lock")

    monkeypatch.setattr("data_engine.hosts.daemon.client.subprocess.Popen", _fail_popen)

    assert spawn_daemon_process(paths) == 0


def test_windows_startup_lock_uses_named_mutex_without_lock_file(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    monkeypatch.setattr("data_engine.hosts.daemon.client.os.name", "nt")

    handles: list[int] = []
    released: list[int] = []
    closed: list[int] = []
    state = {"last_error": 0}

    class _Kernel32:
        def CreateMutexW(self, _security, _initial_owner, _name):
            handle = 1234
            handles.append(handle)
            state["last_error"] = 0 if len(handles) == 1 else daemon_client._WINDOWS_ERROR_ALREADY_EXISTS
            return handle

        def GetLastError(self):
            return state["last_error"]

        def ReleaseMutex(self, handle):
            released.append(handle)
            return 1

        def CloseHandle(self, handle):
            closed.append(handle)
            return 1

    monkeypatch.setattr(daemon_client.ctypes, "windll", SimpleNamespace(kernel32=_Kernel32()))

    assert daemon_client._acquire_startup_lock(paths) is True
    assert (paths.runtime_state_dir / ".daemon-start.lock").exists() is False
    assert daemon_client._acquire_startup_lock(paths) is False

    daemon_client._release_startup_lock(paths)

    assert released == [1234]
    assert closed == [1234, 1234]
