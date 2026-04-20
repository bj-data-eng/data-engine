from __future__ import annotations

from types import SimpleNamespace


import data_engine.hosts.daemon.client as daemon_client
from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.app import (
    DataEngineDaemonService,
    spawn_daemon_process,
)
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state,
    claim_workspace,
    initialize_workspace_state,
    read_lease_metadata,
    release_workspace,
    remove_lease_metadata,
)

from .support import _write_demo_flow, resolve_workspace_paths

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


def test_shutdown_request_wakes_listener_to_exit_accept_loop(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        wake_calls: list[str] = []
        monkeypatch.setattr(service, "_wake_listener", lambda: wake_calls.append("wake"))
        started_threads: list[object] = []

        class _InlineThread:
            def __init__(self, *, target, daemon):
                self._target = target
                self.daemon = daemon

            def start(self):
                started_threads.append(self)
                self._target()

        monkeypatch.setattr("data_engine.hosts.daemon.commands.threading.Thread", _InlineThread)

        response = service._handle_command({"command": "shutdown_daemon"})  # noqa: SLF001

        assert response["ok"] is True
        assert service.host.shutdown_event.is_set() is True
        assert len(started_threads) == 1
        assert wake_calls == ["wake"]
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
            return self._set or self.calls >= 4

        def is_set(self) -> bool:
            return self._set

        def set(self) -> None:
            self._set = True

    service.host.shutdown_event = _SequenceEvent()  # type: ignore[assignment]
    monkeypatch.setattr(service.runtime_control_ledger.client_sessions, "count_live", lambda workspace_id: 0)
    tick = {"value": 0.0}

    def _fake_monotonic() -> float:
        tick["value"] += 1.0
        return tick["value"]

    monkeypatch.setattr("data_engine.hosts.daemon.lifecycle.time.monotonic", _fake_monotonic)

    service._checkpoint_loop()  # noqa: SLF001 - direct lifecycle ephemeral policy test

    assert service.host.shutdown_event.is_set() is False
    assert service.host.workspace_owned is True
    assert service.host.runtime_active is True
    assert service.host.status == "client disconnected"
    assert service.state.shutdown_when_idle is True
    assert service.state.engine_runtime_stop_event.is_set() is True

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
            return self._set or self.calls >= 4

        def is_set(self) -> bool:
            return self._set

        def set(self) -> None:
            self._set = True

    service.host.shutdown_event = _SequenceEvent()  # type: ignore[assignment]
    monkeypatch.setattr(service.runtime_control_ledger.client_sessions, "count_live", lambda workspace_id: 0)
    tick = {"value": 0.0}

    def _fake_monotonic() -> float:
        tick["value"] += 1.0
        return tick["value"]

    monkeypatch.setattr("data_engine.hosts.daemon.lifecycle.time.monotonic", _fake_monotonic)

    service._checkpoint_loop()  # noqa: SLF001 - direct lifecycle ephemeral policy test

    assert service.host.shutdown_event.is_set() is True
    assert service.host.workspace_owned is False
    assert service.host.runtime_active is False
    assert service.host.status == "client disconnected"

    service._shutdown()  # noqa: SLF001


def test_ephemeral_daemon_ignores_transient_zero_client_gap_during_workspace_handoff(tmp_path, monkeypatch):
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
            return self._set or self.calls >= 3

        def is_set(self) -> bool:
            return self._set

        def set(self) -> None:
            self._set = True

    client_counts = iter([0, 1, 1])
    service.host.shutdown_event = _SequenceEvent()  # type: ignore[assignment]
    monkeypatch.setattr(
        service.runtime_control_ledger.client_sessions,
        "count_live",
        lambda workspace_id: next(client_counts),
    )

    service._checkpoint_loop()  # noqa: SLF001 - transient no-client gap should not trigger stop/shutdown

    assert service.host.shutdown_event.is_set() is False
    assert service.host.workspace_owned is True
    assert service.host.runtime_active is True
    assert service.host.status == "running"
    assert service.state.shutdown_when_idle is False
    assert service.state.engine_runtime_stop_event.is_set() is False

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
    monkeypatch.setattr(service.runtime_control_ledger.client_sessions, "count_live", lambda workspace_id: 0)

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
