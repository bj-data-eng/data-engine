from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
from pathlib import Path

import pytest

from data_engine.domain import ActiveRunState, FlowActivityState
import data_engine.hosts.daemon.client as daemon_client
from data_engine.hosts.daemon.app import (
    DataEngineDaemonService,
    WorkspaceLeaseError,
    _remove_stale_unix_endpoint,
    spawn_daemon_process,
)
from data_engine.hosts.daemon.client import (
    DaemonClientError,
    _decode_message,
    _encode_message,
    _pid_is_live,
    daemon_authkey,
    force_shutdown_daemon_process,
)
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, _lease_pid_is_live
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR, machine_id_text
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state,
    claim_workspace,
    initialize_workspace_state,
    read_lease_metadata,
)
from data_engine.services.workspace_io import WorkspaceIoLayer

from .support import _write_demo_flow, resolve_workspace_paths

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
    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.manager._lease_pid_is_live", lambda metadata: False)

    manager = WorkspaceDaemonManager(paths)
    snapshot = manager.sync()

    assert snapshot.workspace_owned is True
    assert snapshot.leased_by_machine_id is None


def test_workspace_daemon_manager_treats_live_same_machine_lease_as_locally_owned(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True
    started = datetime.now(UTC).isoformat()
    checkpoint_workspace_state(
        paths,
        RuntimeCacheLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id=machine_id_text(),
        daemon_id="daemon-a",
        pid=os.getpid(),
        status="starting",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.manager._lease_pid_is_live", lambda metadata: True)

    manager = WorkspaceDaemonManager(paths)
    snapshot = manager.sync()

    assert snapshot.workspace_owned is True
    assert snapshot.leased_by_machine_id is None
    assert snapshot.source == "lease"


def test_daemon_shared_state_adapter_caches_lease_metadata_reads_briefly(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)

    current_time = datetime(2026, 4, 18, tzinfo=UTC)
    reads = {"count": 0}

    def _read(_paths):
        reads["count"] += 1
        return {"workspace_id": "default", "last_checkpoint_at_utc": current_time.isoformat()}

    monkeypatch.setattr("data_engine.services.workspace_io.read_lease_metadata", _read)
    adapter = DaemonSharedStateAdapter(workspace_io=WorkspaceIoLayer(read_interval_seconds=0.5))

    assert adapter.read_lease_metadata(paths) == {"workspace_id": "default", "last_checkpoint_at_utc": current_time.isoformat()}
    assert adapter.read_lease_metadata(paths) == {"workspace_id": "default", "last_checkpoint_at_utc": current_time.isoformat()}
    assert reads["count"] == 1


def test_daemon_shared_state_adapter_invalidates_lease_cache_after_write(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)

    reads = {"count": 0}

    def _read(_paths):
        reads["count"] += 1
        return {"workspace_id": "default", "last_checkpoint_at_utc": datetime(2026, 4, 18, tzinfo=UTC).isoformat()}

    writes = {"count": 0}

    def _write(*args, **kwargs):
        del args, kwargs
        writes["count"] += 1

    monkeypatch.setattr("data_engine.services.workspace_io.read_lease_metadata", _read)
    monkeypatch.setattr("data_engine.services.workspace_io.write_lease_metadata", _write)
    adapter = DaemonSharedStateAdapter(workspace_io=WorkspaceIoLayer(read_interval_seconds=30.0))

    adapter.read_lease_metadata(paths)
    adapter.write_lease_metadata(
        paths,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=datetime(2026, 4, 18, tzinfo=UTC).isoformat(),
        last_checkpoint_at_utc=datetime(2026, 4, 18, tzinfo=UTC).isoformat(),
        app_version="0.1.0",
    )
    adapter.read_lease_metadata(paths)

    assert writes["count"] == 1
    assert reads["count"] == 2


def test_shared_state_service_and_daemon_adapter_share_one_workspace_io_cache(tmp_path, monkeypatch):
    from data_engine.services.shared_state import SharedStateService

    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)

    reads = {"count": 0}

    def _read(_paths):
        reads["count"] += 1
        return {"workspace_id": "default", "last_checkpoint_at_utc": datetime(2026, 4, 18, tzinfo=UTC).isoformat()}

    monkeypatch.setattr("data_engine.services.workspace_io.read_lease_metadata", _read)
    workspace_io = WorkspaceIoLayer(read_interval_seconds=1.0)
    shared_state_service = SharedStateService(workspace_io=workspace_io)
    daemon_adapter = DaemonSharedStateAdapter(workspace_io=workspace_io)

    assert shared_state_service.read_lease_metadata(paths) is not None
    assert daemon_adapter.read_lease_metadata(paths) is not None
    assert reads["count"] == 1


def test_workspace_daemon_manager_unconfigured_sync_does_not_create_runtime_state(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ROOT", raising=False)
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ID", raising=False)
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", raising=False)

    paths = resolve_workspace_paths()
    manager = WorkspaceDaemonManager(paths)

    snapshot = manager.sync()

    assert paths.workspace_configured is False
    assert snapshot.source == "none"
    assert snapshot.workspace_owned is True
    assert paths.runtime_state_dir.exists() is False


def test_workspace_daemon_manager_reuses_cached_snapshot_when_projection_is_unchanged(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    requests: list[dict[str, object]] = []
    responses = iter(
        (
            {
                "ok": True,
                "status": {
                    "workspace_id": "default",
                    "daemon_id": "daemon-a",
                    "workspace_owned": True,
                    "leased_by_machine_id": None,
                    "engine_active": True,
                    "engine_stopping": False,
                    "engine_starting": False,
                    "active_engine_flow_names": ["demo_poll"],
                    "active_runs": [
                        {
                            "run_id": "run-1",
                            "flow_name": "demo_poll",
                            "group_name": "Demo",
                            "state": "running",
                            "current_step_name": "Emit Value",
                            "current_step_started_at_utc": "2026-04-17T00:00:05+00:00",
                            "started_at_utc": "2026-04-17T00:00:00+00:00",
                            "elapsed_seconds": 5.0,
                        }
                    ],
                    "flow_activity": [
                        {
                            "flow_name": "demo_poll",
                            "active_run_count": 1,
                            "queued_run_count": 0,
                            "engine_run_count": 1,
                            "manual_run_count": 0,
                            "stopping_run_count": 0,
                            "running_step_counts": {"Emit Value": 1},
                        }
                    ],
                    "manual_runs": [],
                    "last_checkpoint_at_utc": "2026-04-17T00:00:10+00:00",
                    "projection_version": 7,
                },
            },
            {
                "ok": True,
                "status": {
                    "workspace_id": "default",
                    "daemon_id": "daemon-a",
                    "projection_version": 7,
                    "unchanged": True,
                },
            },
        )
    )

    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: True)

    def _daemon_request(_paths, payload, timeout=0.0):
        del timeout
        requests.append(dict(payload))
        return next(responses)

    monkeypatch.setattr("data_engine.hosts.daemon.manager.daemon_request", _daemon_request)

    manager = WorkspaceDaemonManager(paths)
    first = manager.sync()
    manager._sync_misses = 2  # noqa: SLF001 - verify successful unchanged sync clears stale miss state
    second = manager.sync()

    assert requests[0]["command"] == "daemon_status"
    assert "since_version" not in requests[0]
    assert requests[1]["since_version"] == 7
    assert first.projection_version == 7
    assert first.daemon_id == "daemon-a"
    assert first.transport_mode == "heartbeat"
    assert second.source == "daemon"
    assert second.live is True
    assert second.projection_version == 7
    assert second.daemon_id == "daemon-a"
    assert second.transport_mode == "heartbeat"
    assert second.active_engine_flow_names == ("demo_poll",)
    assert second.active_runs == (
        ActiveRunState(
            run_id="run-1",
            flow_name="demo_poll",
            group_name="Demo",
            source_path=None,
            state="running",
            current_step_name="Emit Value",
            current_step_started_at_utc="2026-04-17T00:00:05+00:00",
            started_at_utc="2026-04-17T00:00:00+00:00",
            finished_at_utc=None,
            elapsed_seconds=5.0,
            error_text=None,
        ),
    )
    assert second.flow_activity == (
        FlowActivityState(
            flow_name="demo_poll",
            active_run_count=1,
            queued_run_count=0,
            engine_run_count=1,
            manual_run_count=0,
            stopping_run_count=0,
            running_step_counts={"Emit Value": 1},
        ),
    )
    assert manager._sync_misses == 0  # noqa: SLF001 - successful unchanged sync should clear retry debt


def test_workspace_daemon_manager_wait_for_update_uses_wait_command_and_reuses_status_normalization(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    requests: list[dict[str, object]] = []
    responses = iter(
        (
            {
                "ok": True,
                "status": {
                    "workspace_id": "default",
                    "daemon_id": "daemon-a",
                    "workspace_owned": True,
                    "leased_by_machine_id": None,
                    "engine_active": False,
                    "engine_stopping": False,
                    "engine_starting": False,
                    "active_engine_flow_names": [],
                    "active_runs": [],
                    "flow_activity": [],
                    "manual_runs": [],
                    "last_checkpoint_at_utc": "2026-04-17T00:00:10+00:00",
                    "projection_version": 7,
                },
            },
            {
                "ok": True,
                "status": {
                    "workspace_id": "default",
                    "daemon_id": "daemon-a",
                    "workspace_owned": True,
                    "leased_by_machine_id": None,
                    "engine_active": True,
                    "engine_stopping": False,
                    "engine_starting": False,
                    "active_engine_flow_names": ["demo_poll"],
                    "active_runs": [
                        {
                            "run_id": "run-2",
                            "flow_name": "demo_poll",
                            "group_name": "Demo",
                            "state": "running",
                            "started_at_utc": "2026-04-17T00:01:00+00:00",
                        }
                    ],
                    "flow_activity": [
                        {
                            "flow_name": "demo_poll",
                            "active_run_count": 1,
                            "queued_run_count": 0,
                            "engine_run_count": 1,
                            "manual_run_count": 0,
                            "stopping_run_count": 0,
                            "running_step_counts": {},
                        }
                    ],
                    "manual_runs": [],
                    "last_checkpoint_at_utc": "2026-04-17T00:01:00+00:00",
                    "projection_version": 8,
                },
            },
        )
    )

    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: True)

    def _daemon_request(_paths, payload, timeout=0.0):
        del timeout
        requests.append(dict(payload))
        return next(responses)

    monkeypatch.setattr("data_engine.hosts.daemon.manager.daemon_request", _daemon_request)

    manager = WorkspaceDaemonManager(paths)
    first = manager.sync()
    second = manager.wait_for_update(timeout_seconds=1.5)

    assert first.projection_version == 7
    assert first.daemon_id == "daemon-a"
    assert first.transport_mode == "heartbeat"
    assert requests[1]["command"] == "wait_for_daemon_status"
    assert requests[1]["since_version"] == 7
    assert requests[1]["timeout_ms"] == 1500
    assert second.projection_version == 8
    assert second.daemon_id == "daemon-a"
    assert second.transport_mode == "subscription"
    assert second.runtime_active is True
    assert second.active_engine_flow_names == ("demo_poll",)
    assert tuple(run.run_id for run in second.active_runs) == ("run-2",)


def test_workspace_daemon_manager_detects_daemon_restart_even_when_projection_version_repeats(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    requests: list[dict[str, object]] = []
    responses = iter(
        (
            {
                "ok": True,
                "status": {
                    "workspace_id": "default",
                    "daemon_id": "daemon-a",
                    "workspace_owned": True,
                    "leased_by_machine_id": None,
                    "engine_active": False,
                    "engine_stopping": False,
                    "engine_starting": False,
                    "active_engine_flow_names": [],
                    "active_runs": [],
                    "flow_activity": [],
                    "manual_runs": [],
                    "last_checkpoint_at_utc": "2026-04-17T00:00:10+00:00",
                    "projection_version": 7,
                },
            },
            {
                "ok": True,
                "status": {
                    "workspace_id": "default",
                    "daemon_id": "daemon-b",
                    "workspace_owned": True,
                    "leased_by_machine_id": None,
                    "engine_active": False,
                    "engine_stopping": False,
                    "engine_starting": False,
                    "active_engine_flow_names": [],
                    "active_runs": [],
                    "flow_activity": [],
                    "manual_runs": [],
                    "last_checkpoint_at_utc": "2026-04-17T00:00:10+00:00",
                    "projection_version": 7,
                },
            },
        )
    )

    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: True)

    def _daemon_request(_paths, payload, timeout=0.0):
        del timeout
        requests.append(dict(payload))
        return next(responses)

    monkeypatch.setattr("data_engine.hosts.daemon.manager.daemon_request", _daemon_request)

    manager = WorkspaceDaemonManager(paths)
    first = manager.sync()
    second = manager.wait_for_update(timeout_seconds=1.5)

    assert requests[1]["command"] == "wait_for_daemon_status"
    assert requests[1]["since_version"] == 7
    assert first.daemon_id == "daemon-a"
    assert first.transport_mode == "heartbeat"
    assert second.daemon_id == "daemon-b"
    assert second.transport_mode == "subscription"
    assert second != first


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
        RuntimeCacheLedger(paths.runtime_db_path),
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
            service.runtime_control_ledger.daemon_state,
            "upsert",
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
        RuntimeCacheLedger(paths.runtime_db_path),
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
        RuntimeCacheLedger(paths.runtime_db_path),
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
    with pytest.raises(WorkspaceLeaseError, match="already leased locally"):
        service.initialize()
