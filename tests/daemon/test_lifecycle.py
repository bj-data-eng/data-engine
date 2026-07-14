from __future__ import annotations

import threading
from types import SimpleNamespace
import shutil


import data_engine.hosts.daemon.client as daemon_client
from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.app import (
    DataEngineDaemonService,
    spawn_daemon_process,
)
from data_engine.hosts.daemon.lifecycle import (
    complete_requested_shutdown,
    relinquish_workspace_for_control_request,
)
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state,
    claim_workspace,
    initialize_workspace_state,
    read_lease_metadata,
    read_runtime_snapshot_generation,
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


def test_control_handoff_keeps_ownership_until_noncooperative_manual_worker_exits(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    worker_started = threading.Event()
    release_worker = threading.Event()

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id
        worker_started.set()
        release_worker.wait()

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)
    monkeypatch.setattr("data_engine.hosts.daemon.runtime_control.ACTIVE_WORK_DRAIN_TIMEOUT_SECONDS", 0.01)

    service.initialize()
    try:
        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001
        assert response["ok"] is True
        assert worker_started.wait(timeout=1.0) is True
        manual_thread = service.state.manual_run_threads["demo"]

        assert relinquish_workspace_for_control_request(service, "machine-b") is False

        assert service.host.workspace_owned is True
        assert service.host.shutdown_event.is_set() is False
        assert service.state.manual_run_threads == {"demo": manual_thread}
        assert read_lease_metadata(paths) is not None
        assert service._handle_command({"command": "run_flow", "name": "demo", "wait": False}) == {  # noqa: SLF001
            "ok": False,
            "error": "Runtime work is stopping.",
        }
        assert service._handle_command({"command": "start_engine"}) == {  # noqa: SLF001
            "ok": False,
            "error": "Runtime work is stopping.",
        }

        release_worker.set()
        manual_thread.join(timeout=1.0)
        assert manual_thread.is_alive() is False
        assert relinquish_workspace_for_control_request(service, "machine-b") is True

        assert service.host.workspace_owned is False
        assert service.host.shutdown_event.is_set() is True
        assert read_lease_metadata(paths) is None
    finally:
        release_worker.set()
        service._shutdown()  # noqa: SLF001


def test_final_shutdown_waits_for_worker_before_releasing_or_closing_storage(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    worker_started = threading.Event()
    stop_seen = threading.Event()
    release_worker = threading.Event()
    close_calls: list[str] = []

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_ledger, flow_stop_event, workspace_id
        worker_started.set()
        runtime_stop_event.wait()
        stop_seen.set()
        release_worker.wait()

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)
    monkeypatch.setattr("data_engine.hosts.daemon.runtime_control.ACTIVE_WORK_DRAIN_TIMEOUT_SECONDS", 0.01)
    original_cache_close = service.runtime_cache_ledger.close
    original_control_close = service.runtime_control_ledger.close
    monkeypatch.setattr(
        service.runtime_cache_ledger,
        "close",
        lambda: (close_calls.append("cache"), original_cache_close())[1],
    )
    monkeypatch.setattr(
        service.runtime_control_ledger,
        "close",
        lambda: (close_calls.append("control"), original_control_close())[1],
    )

    service.initialize()
    response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001
    assert response["ok"] is True
    assert worker_started.wait(timeout=1.0) is True
    shutdown_thread = threading.Thread(target=service._shutdown, daemon=True)  # noqa: SLF001
    shutdown_thread.start()
    try:
        assert stop_seen.wait(timeout=1.0) is True
        threading.Event().wait(0.05)

        assert shutdown_thread.is_alive() is True
        assert service.host.workspace_owned is True
        assert read_lease_metadata(paths) is not None
        assert close_calls == []
    finally:
        release_worker.set()
        shutdown_thread.join(timeout=3.0)

    assert shutdown_thread.is_alive() is False
    assert service.host.workspace_owned is False
    assert read_lease_metadata(paths) is None
    assert close_calls == ["cache", "control"]


def test_shutdown_command_stays_reachable_until_noncooperative_worker_drains(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    worker_started = threading.Event()
    release_worker = threading.Event()

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id
        worker_started.set()
        release_worker.wait()

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        assert service._handle_command({"command": "run_flow", "name": "demo", "wait": False})["ok"] is True  # noqa: SLF001
        assert worker_started.wait(timeout=1.0) is True
        manual_thread = service.state.manual_run_threads["demo"]

        response = service._handle_command({"command": "shutdown_daemon"})  # noqa: SLF001

        assert response == {
            "ok": True,
            "draining": True,
            "active_workers": ["manual runtime:demo"],
        }
        assert service.state.shutdown_requested is True
        assert service.host.shutdown_event.is_set() is False
        assert service.host.workspace_owned is True
        assert read_lease_metadata(paths) is not None

        release_worker.set()
        manual_thread.join(timeout=1.0)
        assert complete_requested_shutdown(service) is True

        assert service.state.shutdown_requested is False
        assert service.host.shutdown_event.is_set() is True
        assert service.host.workspace_owned is True
    finally:
        release_worker.set()
        service._shutdown()  # noqa: SLF001


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

        shutil.rmtree(paths.shared_snapshot_generations_dir, ignore_errors=True)
        paths.shared_snapshot_manifest_path.unlink(missing_ok=True)

        assert read_runtime_snapshot_generation(paths) is None

        service._shutdown()  # noqa: SLF001

        generation_id = read_runtime_snapshot_generation(paths)
        assert generation_id is not None
        generation_dir = paths.shared_snapshot_generations_dir / generation_id
        assert (generation_dir / "runs.parquet").is_file()
        assert (generation_dir / "step_runs.parquet").is_file()
        assert (generation_dir / "logs.parquet").is_file()
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


def test_checkpoint_failures_release_workspace_when_control_state_publication_fails(tmp_path, monkeypatch):
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
    checkpoint_statuses: list[str] = []
    original_checkpoint_once = service._checkpoint_once  # noqa: SLF001 - deterministic failure-path injection

    def _checkpoint_once(*, status: str) -> None:
        checkpoint_statuses.append(status)
        original_checkpoint_once(status=status)

    control_state_statuses: list[str] = []

    def _fail_control_state_upsert(**kwargs: object) -> None:
        status = kwargs["status"]
        assert isinstance(status, str)
        control_state_statuses.append(status)
        raise RuntimeError("control state unavailable")

    monkeypatch.setattr(service, "_checkpoint_once", _checkpoint_once)
    monkeypatch.setattr(service.runtime_control_ledger.daemon_state, "upsert", _fail_control_state_upsert)
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
    assert checkpoint_statuses == ["running", "degraded", "degraded"]
    assert control_state_statuses == ["running", "degraded", "degraded", "degraded", "failed"]
    assert read_lease_metadata(paths) is None
    assert (paths.available_markers_dir / paths.workspace_id).exists() is True
    assert (paths.leased_markers_dir / paths.workspace_id).exists() is False

    service._shutdown()  # noqa: SLF001


def test_checkpoint_failure_relinquish_retries_after_noncooperative_worker_exits(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    worker_started = threading.Event()
    stop_seen = threading.Event()
    release_worker = threading.Event()

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_ledger, flow_stop_event, workspace_id
        worker_started.set()
        runtime_stop_event.wait()
        stop_seen.set()
        release_worker.wait()

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)
    monkeypatch.setattr("data_engine.hosts.daemon.runtime_control.ACTIVE_WORK_DRAIN_TIMEOUT_SECONDS", 0.01)

    service.initialize()
    try:
        assert service._handle_command({"command": "run_flow", "name": "demo", "wait": False})["ok"] is True  # noqa: SLF001
        assert worker_started.wait(timeout=1.0) is True
        manual_thread = service.state.manual_run_threads["demo"]
        checkpoint_calls: list[int] = []

        def _checkpoint_once(*, status: str) -> None:
            del status
            checkpoint_calls.append(len(checkpoint_calls) + 1)
            if len(checkpoint_calls) <= 3:
                raise RuntimeError("checkpoint unavailable")
            raise AssertionError("terminal checkpoint failure must not be revived by a later checkpoint")

        monkeypatch.setattr(service, "_checkpoint_once", _checkpoint_once)
        monkeypatch.setattr("data_engine.hosts.daemon.lifecycle.CHECKPOINT_INTERVAL_SECONDS", 0.0)
        monkeypatch.setattr("data_engine.hosts.daemon.lifecycle.CONTROL_REQUEST_POLL_INTERVAL_SECONDS", 0.01)
        checkpoint_thread = threading.Thread(target=service._checkpoint_loop, daemon=True)  # noqa: SLF001
        service.state.checkpoint_thread = checkpoint_thread
        checkpoint_thread.start()

        for _ in range(100):
            with service._state_lock:
                relinquish_pending = service.state.checkpoint_failure_relinquish_requested
            if relinquish_pending:
                break
            threading.Event().wait(0.01)

        assert relinquish_pending is True, (
            checkpoint_calls,
            service.state.consecutive_checkpoint_failures,
            checkpoint_thread.is_alive(),
        )
        assert stop_seen.wait(timeout=1.0) is True
        assert service.host.workspace_owned is True
        assert service.host.shutdown_event.is_set() is False
        assert read_lease_metadata(paths) is not None
        assert checkpoint_calls == [1, 2, 3]

        release_worker.set()
        manual_thread.join(timeout=1.0)
        checkpoint_thread.join(timeout=2.0)

        assert checkpoint_thread.is_alive() is False
        assert service.host.workspace_owned is False
        assert service.host.shutdown_event.is_set() is True
        assert service.state.checkpoint_failure_relinquish_requested is False
        assert read_lease_metadata(paths) is None
        assert checkpoint_calls == [1, 2, 3]
    finally:
        release_worker.set()
        service.host.shutdown_event.set()
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
        host_name="test-host",
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

    monkeypatch.setattr(daemon_client.ctypes, "windll", SimpleNamespace(kernel32=_Kernel32()), raising=False)

    assert daemon_client._acquire_startup_lock(paths) is True
    assert (paths.runtime_state_dir / ".daemon-start.lock").exists() is False
    assert daemon_client._acquire_startup_lock(paths) is False

    daemon_client._release_startup_lock(paths)

    assert released == [1234]
    assert closed == [1234, 1234]
