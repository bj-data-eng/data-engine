from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import ctypes
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import signal
import sqlite3
import threading
from types import SimpleNamespace

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
    _harden_private_file_permissions,
    _pid_is_live,
    daemon_authkey,
    force_shutdown_daemon_process,
)
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, WorkspaceDaemonSnapshot, _lease_pid_is_live
from data_engine.platform.machine_identity import host_name_text, machine_id_text
import data_engine.platform.posix_watchdog as posix_watchdog
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.runtime.runtime_db import RuntimeCacheLedger, RuntimeControlLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state as _checkpoint_workspace_state,
    claim_workspace as _claim_workspace,
    initialize_workspace_state,
    read_lease_metadata,
    resolve_workspace_bundle,
)
from data_engine.services.workspace_io import WorkspaceIoLayer

from .support import (
    _TEST_CONTAINMENT_NONCE,
    _owner_process_kwargs,
    _test_process_identity,
    _write_demo_flow,
    resolve_workspace_paths,
)


def claim_workspace(paths) -> bool:
    return _claim_workspace(paths) is not None


def checkpoint_workspace_state(paths, ledger, **kwargs):
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token is not None
    kwargs = {**_owner_process_kwargs(int(kwargs["pid"])), **kwargs}
    return _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=bundle.lease_token,
        **kwargs,
    )


def test_importing_daemon_client_does_not_create_fake_ctypes_windll():
    if hasattr(ctypes, "windll"):
        pytest.skip("ctypes.windll is natively available on this platform.")

    assert hasattr(daemon_client.ctypes, "windll") is False


def test_windows_subprocess_creationflags_are_zero_on_non_windows(monkeypatch):
    monkeypatch.setattr("data_engine.platform.processes.os.name", "posix")

    assert daemon_client.windows_subprocess_creationflags(new_process_group=True, no_window=True, detached=True) == 0


def test_windows_subprocess_creationflags_uses_numeric_fallbacks_when_simulating_windows(monkeypatch):
    monkeypatch.setattr("data_engine.platform.processes.os.name", "nt")
    monkeypatch.delattr("data_engine.platform.processes.subprocess.CREATE_NEW_PROCESS_GROUP", raising=False)
    monkeypatch.delattr("data_engine.platform.processes.subprocess.CREATE_NO_WINDOW", raising=False)
    monkeypatch.delattr("data_engine.platform.processes.subprocess.DETACHED_PROCESS", raising=False)

    assert daemon_client.windows_subprocess_creationflags(new_process_group=True, no_window=True, detached=True) == (
        0x00000200 | 0x08000000 | 0x00000008
    )


def test_windows_startup_lock_requires_windll_when_simulating_windows(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    monkeypatch.setattr(daemon_client.os, "name", "nt")
    monkeypatch.delattr(daemon_client.ctypes, "windll", raising=False)

    with pytest.raises(DaemonClientError, match="ctypes.windll"):
        daemon_client._acquire_startup_lock(paths)


@pytest.mark.skipif(os.name == "nt", reason="POSIX file locking is required")
def test_posix_startup_lock_does_not_expire_while_held(tmp_path, monkeypatch):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    assert daemon_client._acquire_startup_lock(paths) is True
    lock_path = daemon_client._startup_lock_path(paths)
    try:
        os.utime(lock_path, (0, 0))
        assert daemon_client._acquire_startup_lock(paths) is False
    finally:
        daemon_client._release_startup_lock(paths)

    assert lock_path.exists()
    assert daemon_client._acquire_startup_lock(paths) is True
    daemon_client._release_startup_lock(paths)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is unavailable")
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded.*:DeprecationWarning"
)
def test_fork_child_cleanup_closes_inherited_startup_lock_descriptions(
    tmp_path,
    monkeypatch,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    monkeypatch.setattr(daemon_client.os, "name", "posix")
    assert daemon_client._acquire_startup_lock(paths) is True
    lock_path = daemon_client._startup_lock_path(paths)
    inherited_lock_fd = daemon_client._POSIX_STARTUP_LOCK_FDS[lock_path]
    read_fd, write_fd = os.pipe()
    try:
        child_pid = os.fork()
        if child_pid == 0:
            os.close(read_fd)
            try:
                os.fstat(inherited_lock_fd)
            except OSError:
                result = b"closed"
            else:
                result = b"open"
            os.write(write_fd, result)
            os.close(write_fd)
            os._exit(0)
        os.close(write_fd)
        write_fd = -1
        assert os.read(read_fd, 32) == b"closed"
        waited_pid, status = os.waitpid(child_pid, 0)
        assert waited_pid == child_pid
        assert os.waitstatus_to_exitcode(status) == 0
        assert daemon_client._acquire_startup_lock(paths) is False
    finally:
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)
        daemon_client._release_startup_lock(paths)


@pytest.mark.skipif(os.name != "posix", reason="POSIX launch pipes are required")
def test_posix_gated_launch_closes_first_pipe_when_second_allocation_fails(
    monkeypatch,
):
    pipe_calls = 0
    closed_fds: list[int] = []

    def _pipe():
        nonlocal pipe_calls
        pipe_calls += 1
        if pipe_calls == 1:
            return 101, 102
        raise OSError("descriptor pressure")

    monkeypatch.setattr(daemon_client.os, "pipe", _pipe)
    monkeypatch.setattr(daemon_client.os, "close", closed_fds.append)
    monkeypatch.setattr(
        daemon_client.subprocess,
        "Popen",
        lambda *args, **kwargs: pytest.fail("launch must not begin without both pipes"),
    )

    with pytest.raises(OSError, match="descriptor pressure"):
        daemon_client._launch_contained_daemon(
            ["python"],
            containment_nonce=_TEST_CONTAINMENT_NONCE,
            on_identity_ready=lambda _identity: None,
        )

    assert closed_fds == [101, 102]


def test_posix_reaper_start_failure_kills_retained_child_without_watchdog_request(
    monkeypatch,
):
    expected = _test_process_identity(321)
    events = []
    pipe_results = iter(((101, 102), (103, 104)))

    class _Process:
        pid = expected.pid

        def poll(self):
            events.append("poll")
            return None

        def kill(self):
            events.append("kill")

        def wait(self, *, timeout):
            events.append(("wait", timeout))
            return -signal.SIGKILL

    process = _Process()
    monkeypatch.setattr(daemon_client.os, "name", "posix")
    monkeypatch.setattr(daemon_client.os, "pipe", lambda: next(pipe_results))
    monkeypatch.setattr(
        daemon_client.os,
        "close",
        lambda fd: events.append(("close", fd)),
    )
    monkeypatch.setattr(
        daemon_client.os,
        "write",
        lambda fd, payload: events.append(("write", fd, payload)) or len(payload),
    )
    monkeypatch.setattr(
        daemon_client.subprocess,
        "Popen",
        lambda *args, **kwargs: events.append("popen") or process,
    )
    monkeypatch.setattr(
        daemon_client,
        "_read_posix_launch_identity",
        lambda fd, *, expected_pid: events.append(
            ("read-identity", fd, expected_pid)
        )
        or expected,
    )
    monkeypatch.setattr(
        daemon_client,
        "_start_posix_daemon_reaper",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no threads")),
    )
    monkeypatch.setattr(
        daemon_client,
        "force_kill_verified_contained_process_tree",
        lambda *args, **kwargs: pytest.fail("no watchdog request is safe before READY"),
    )
    monkeypatch.setattr(
        daemon_client,
        "wait_for_posix_process_group_exit",
        lambda group_id, *, timeout_seconds: events.append(
            ("group-drain", group_id, timeout_seconds)
        )
        or True,
    )
    monkeypatch.setattr(
        daemon_client,
        "_cleanup_posix_watchdog_endpoint",
        lambda nonce: events.append(("endpoint-cleanup", nonce)),
    )

    with pytest.raises(DaemonClientError, match="Unable to supervise"):
        daemon_client._launch_contained_daemon(
            ["python"],
            containment_nonce=_TEST_CONTAINMENT_NONCE,
            on_identity_ready=lambda identity: events.append(
                ("identity-ready", identity)
            ),
            on_verified_drain=lambda: events.append("verified-drain"),
        )

    assert events == [
        "popen",
        ("close", 102),
        ("close", 103),
        ("read-identity", 101, expected.pid),
        ("identity-ready", expected),
        ("write", 104, b"1"),
        ("close", 101),
        ("close", 104),
        "poll",
        "kill",
        ("wait", 5.0),
        ("group-drain", expected.pid, 2.0),
        ("endpoint-cleanup", _TEST_CONTAINMENT_NONCE),
        "verified-drain",
    ]


def test_posix_launch_requires_gated_stable_identity(monkeypatch):
    monkeypatch.setattr(daemon_client.os, "name", "posix")
    monkeypatch.setattr(
        daemon_client.subprocess,
        "Popen",
        lambda *args, **kwargs: pytest.fail("ungated POSIX launch must not begin"),
    )

    with pytest.raises(DaemonClientError, match="gated stable-identity callback"):
        daemon_client._launch_contained_daemon(
            ["python"],
            containment_nonce=_TEST_CONTAINMENT_NONCE,
        )


def test_spawn_refuses_live_local_tombstone_and_releases_startup_lock(
    tmp_path,
    monkeypatch,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    expected = _test_process_identity(321)
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    release_calls = []
    monotonic_values = iter((0.0, 3.0))
    monkeypatch.setattr(daemon_client, "is_daemon_live", lambda paths: False)
    monkeypatch.setattr(daemon_client, "_wait_for_fresh_local_daemon", lambda paths: False)
    monkeypatch.setattr(daemon_client, "_same_machine_live_lease_process", lambda paths: None)
    monkeypatch.setattr(daemon_client, "_should_force_recover_local_lease", lambda paths: False)
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_unreachable_lease_metadata",
        lambda paths: None,
    )
    monkeypatch.setattr(daemon_client, "_acquire_startup_lock", lambda paths: True)
    monkeypatch.setattr(
        daemon_client,
        "_release_startup_lock",
        lambda selected_paths: release_calls.append(selected_paths),
    )
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: record,
    )
    monkeypatch.setattr(daemon_client, "inspect_process_identity", lambda pid: expected)
    monkeypatch.setattr(daemon_client.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(
        daemon_client,
        "_launch_contained_daemon",
        lambda *args, **kwargs: pytest.fail("a live tombstone must block replacement launch"),
    )

    with pytest.raises(DaemonClientError, match="still shutting down"):
        spawn_daemon_process(paths)

    assert release_calls == [paths]


@pytest.mark.parametrize("platform_name", ["posix", "nt"])
def test_prior_dead_tombstone_drains_platform_containment(platform_name, monkeypatch):
    expected = _test_process_identity(321)
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    events = []
    monkeypatch.setattr(daemon_client.os, "name", platform_name)
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: events.append("record") or record,
    )
    monkeypatch.setattr(
        daemon_client,
        "inspect_process_identity",
        lambda pid: events.append(("inspect", pid)) or None,
    )
    monkeypatch.setattr(
        daemon_client,
        "ensure_windows_containment_job_stopped",
        lambda nonce, *, timeout_seconds: events.append(
            ("windows-drain", nonce, timeout_seconds)
        ),
    )
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_posix_daemon_group_exit",
        lambda identity, *, timeout_seconds: events.append(
            ("posix-drain", identity, timeout_seconds)
        ),
    )
    monkeypatch.setattr(
        daemon_client,
        "_cleanup_posix_watchdog_endpoint",
        lambda nonce: events.append(("posix-cleanup", nonce)),
    )

    daemon_client._wait_for_prior_local_daemon_release(SimpleNamespace())

    expected_drain = (
        [("windows-drain", _TEST_CONTAINMENT_NONCE, 2.0)]
        if platform_name == "nt"
        else [
            ("posix-drain", expected, 2.0),
            ("posix-cleanup", _TEST_CONTAINMENT_NONCE),
        ]
    )
    assert events == ["record", ("inspect", expected.pid), *expected_drain]


def test_provisional_launch_record_persists_exact_identity_before_daemon_ready(
    tmp_path,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    identity = _test_process_identity(321)

    daemon_client._persist_provisional_daemon_launch(
        paths,
        identity,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )

    ledger = RuntimeControlLedger(paths.runtime_control_db_path)
    try:
        persisted = ledger.daemon_state.get(paths.workspace_id)
    finally:
        ledger.close()
    assert persisted is not None
    assert persisted.pid == identity.pid
    assert persisted.process_start_key == identity.start_key
    assert persisted.process_executable_path == identity.executable_path
    assert persisted.process_group_id == identity.process_group_id
    assert persisted.process_session_id == identity.process_session_id
    assert persisted.containment_nonce == _TEST_CONTAINMENT_NONCE
    assert persisted.status == "launching"
    assert persisted.workspace_root == str(paths.workspace_root)


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
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
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


def test_workspace_daemon_manager_does_not_recover_fresh_claiming_bundle(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    lease_token = _claim_workspace(paths)
    assert isinstance(lease_token, str)
    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: False)
    manager = WorkspaceDaemonManager(paths)
    recovery_calls: list[tuple[str, str, float]] = []
    monkeypatch.setattr(
        manager.shared_state_adapter,
        "recover_stale_workspace",
        lambda _paths, *, lease_token, machine_id, stale_after_seconds: recovery_calls.append(
            (lease_token, machine_id, stale_after_seconds)
        )
        or True,
    )

    snapshot = manager.sync()
    message = manager.request_control()

    assert snapshot.workspace_owned is False
    assert snapshot.source == "lease"
    assert snapshot.leased_by_machine_id is None
    assert message == "Control request sent."
    assert recovery_calls == []
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token == lease_token


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
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
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


def test_same_hostname_with_different_installation_id_is_remote(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True
    remote_machine_id = machine_id_text(
        settings_path=tmp_path / "cloned-installation" / "app_settings.sqlite"
    )
    assert remote_machine_id != machine_id_text(app_root=paths.app_root)
    started = datetime.now(UTC).isoformat()
    checkpoint_workspace_state(
        paths,
        RuntimeCacheLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id=remote_machine_id,
        host_name=host_name_text(),
        daemon_id="daemon-a",
        pid=os.getpid(),
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: False)

    snapshot = WorkspaceDaemonManager(paths).sync()

    assert snapshot.workspace_owned is False
    assert snapshot.leased_by_machine_id == remote_machine_id
    assert snapshot.leased_by_host_name == host_name_text()
    assert daemon_client._same_machine_lease_process(paths) is None


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
        lease_token="a" * 32,
        workspace_id="default",
        machine_id="machine-a",
        host_name="test-host",
        daemon_id="daemon-a",
        pid=101,
        **_owner_process_kwargs(101),
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


def test_workspace_daemon_manager_clears_liveness_when_status_request_fails(tmp_path, monkeypatch):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: True)
    monkeypatch.setattr(
        "data_engine.hosts.daemon.manager.daemon_request",
        lambda paths, payload, timeout=0.0: (_ for _ in ()).throw(DaemonClientError("status failed")),
    )
    manager = WorkspaceDaemonManager(paths)
    manager._last_snapshot = WorkspaceDaemonSnapshot(  # noqa: SLF001 - reproduce the cached-status failure path
        live=True,
        workspace_owned=True,
        leased_by_machine_id=None,
        runtime_active=False,
        runtime_stopping=False,
        manual_runs=(),
        last_checkpoint_at_utc="2026-07-13T12:00:00+00:00",
        source="daemon",
    )

    snapshot = manager.sync()

    assert snapshot.live is False
    assert snapshot.source == "cached"
    assert manager.daemon_live is False


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


def test_windows_job_cleanup_refuses_ambiguous_process_inspection(monkeypatch):
    expected = replace(
        _test_process_identity(321),
        process_group_id=None,
        process_session_id=7,
    )
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    monkeypatch.setattr(daemon_client.os, "name", "nt")
    monkeypatch.setattr(
        daemon_client,
        "open_verified_windows_kill_on_close_job",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            daemon_client.ProcessInspectionError("access denied")
        ),
    )
    monkeypatch.setattr(
        daemon_client,
        "inspect_process_identity",
        lambda pid: (_ for _ in ()).throw(
            daemon_client.ProcessInspectionError("access denied")
        ),
    )
    monkeypatch.setattr(daemon_client, "process_is_running", lambda pid: False)
    monkeypatch.setattr(
        daemon_client,
        "ensure_windows_containment_job_stopped",
        lambda *args, **kwargs: pytest.fail("ambiguous identity must not terminate a Job"),
    )

    with pytest.raises(DaemonClientError, match="Unable to verify local daemon process"):
        daemon_client._open_verified_windows_daemon_job(record)


def test_zero_timeout_force_shutdown_skips_unbounded_graceful_request(monkeypatch):
    expected = _test_process_identity(321)
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    running = {"value": True}
    events: list[str] = []
    monkeypatch.setattr(
        daemon_client,
        "daemon_request",
        lambda *args, **kwargs: pytest.fail("zero timeout must skip graceful IPC"),
    )
    monkeypatch.setattr(
        daemon_client,
        "_expected_process_is_running",
        lambda identity: running["value"],
    )

    def _kill(identity, *, containment_nonce):
        del identity, containment_nonce
        events.append("kill")
        running["value"] = False

    monkeypatch.setattr(daemon_client, "force_kill_verified_contained_process_tree", _kill)
    monkeypatch.setattr(
        daemon_client,
        "_finish_verified_daemon_exit",
        lambda *args, **kwargs: events.append("finish"),
    )

    daemon_client._force_shutdown_daemon_process(
        SimpleNamespace(),
        process_record=record,
        identity_error=None,
        windows_job=None,
        timeout=0.0,
    )

    assert events == ["kill", "finish"]


@pytest.mark.parametrize("timeout", [-1.0, float("nan"), float("inf"), True])
def test_force_shutdown_rejects_invalid_timeout_before_state_access(
    tmp_path,
    monkeypatch,
    timeout,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    monkeypatch.setattr(
        daemon_client,
        "_local_daemon_process",
        lambda paths: pytest.fail("invalid timeout must fail before state access"),
    )

    with pytest.raises(ValueError, match="finite nonnegative"):
        force_shutdown_daemon_process(paths, timeout=timeout)


def test_force_shutdown_daemon_process_releases_verified_lease_immediately(
    tmp_path,
    monkeypatch,
):
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
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
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
    expected_identity = _test_process_identity(321)
    RuntimeControlLedger(paths.runtime_control_db_path).daemon_state.upsert(
        workspace_id=paths.workspace_id,
        daemon_id="daemon-a",
        pid=expected_identity.pid,
        process_start_key=expected_identity.start_key,
        process_executable_path=expected_identity.executable_path,
        process_group_id=expected_identity.process_group_id,
        process_session_id=expected_identity.process_session_id,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        endpoint_kind=paths.daemon_endpoint_kind,
        endpoint_path=paths.daemon_endpoint_path,
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        status="running",
        app_root=str(paths.app_root),
        workspace_root=str(paths.workspace_root),
    )
    killed_records: list[tuple[object, str]] = []
    pid_live = {"value": True}

    monkeypatch.setattr(
        "data_engine.hosts.daemon.client.daemon_request",
        lambda paths, payload, timeout=0.0: (_ for _ in ()).throw(DaemonClientError("unreachable")),
    )
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        "data_engine.hosts.daemon.client.inspect_process_identity",
        lambda pid: expected_identity if pid_live["value"] else None,
    )

    def _kill(identity, *, containment_nonce: str) -> None:
        killed_records.append((identity, containment_nonce))
        pid_live["value"] = False

    monkeypatch.setattr(
        "data_engine.hosts.daemon.client.force_kill_verified_contained_process_tree",
        _kill,
    )
    monkeypatch.setattr(
        daemon_client,
        "wait_for_posix_process_group_exit",
        lambda process_group_id, *, timeout_seconds: True,
    )

    force_shutdown_daemon_process(paths, timeout=0.0)

    assert killed_records == [(expected_identity, _TEST_CONTAINMENT_NONCE)]
    assert read_lease_metadata(paths) is None
    assert claim_workspace(paths) is True
    if paths.daemon_endpoint_kind == "unix":
        assert endpoint_path.exists() is True


def test_failed_launch_cleanup_releases_only_matching_nonce_lease(
    tmp_path,
    monkeypatch,
):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    started = utcnow_text()

    assert claim_workspace(paths) is True
    checkpoint_workspace_state(
        paths,
        RuntimeCacheLedger(paths.runtime_db_path),
        workspace_id=paths.workspace_id,
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
        daemon_id="daemon-matching",
        pid=321,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        status="starting",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    daemon_client._release_failed_launch_lease(
        paths,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )

    assert read_lease_metadata(paths) is None
    assert claim_workspace(paths) is True
    other_nonce = "cd" * 32
    checkpoint_workspace_state(
        paths,
        RuntimeCacheLedger(paths.runtime_db_path),
        workspace_id=paths.workspace_id,
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
        daemon_id="daemon-other",
        pid=322,
        containment_nonce=other_nonce,
        status="starting",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    daemon_client._release_failed_launch_lease(
        paths,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )

    metadata = read_lease_metadata(paths)
    assert metadata is not None
    assert metadata["containment_nonce"] == other_nonce


def test_force_shutdown_rejects_malformed_reachable_identity_without_fallback(
    tmp_path,
    monkeypatch,
):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    def _request(_paths, payload, timeout=0.0):
        del _paths, timeout
        if payload["command"] == "daemon_status":
            return {
                "ok": True,
                "status": {
                    "workspace_id": paths.workspace_id,
                    "workspace_root": str(paths.workspace_root),
                    "machine_id": machine_id_text(app_root=paths.app_root),
                    "daemon_id": "daemon-a",
                    "pid": 321,
                },
            }
        raise DaemonClientError("unreachable")

    monkeypatch.setattr(daemon_client, "daemon_request", _request)
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_lease_process",
        lambda paths: pytest.fail("malformed reachable status must not fall back to a lease"),
    )
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: pytest.fail("malformed reachable status must not fall back to local state"),
    )
    monkeypatch.setattr(
        daemon_client,
        "force_kill_verified_contained_process_tree",
        lambda *args, **kwargs: pytest.fail("malformed identity must never be signaled"),
    )

    with pytest.raises(DaemonClientError, match="incomplete or malformed"):
        force_shutdown_daemon_process(paths, timeout=0.0)


def test_force_shutdown_treats_pid_reuse_as_original_daemon_exit(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    expected = _test_process_identity(321)
    replacement = replace(expected, start_key="replacement-process")
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    cleanup_calls: list[tuple[object, object]] = []
    drained_groups: list[int] = []

    monkeypatch.setattr(daemon_client, "_local_daemon_process", lambda paths: record)
    monkeypatch.setattr(
        daemon_client,
        "daemon_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(DaemonClientError("unreachable")),
    )
    monkeypatch.setattr(daemon_client, "inspect_process_identity", lambda pid: replacement)
    monkeypatch.setattr(
        daemon_client,
        "force_kill_verified_contained_process_tree",
        lambda *args, **kwargs: pytest.fail("a replacement process must never be signaled"),
    )
    monkeypatch.setattr(
        daemon_client,
        "_cleanup_forced_shutdown",
        lambda selected_paths, *, process_record=None: cleanup_calls.append(
            (selected_paths, process_record)
        ),
    )
    monkeypatch.setattr(
        daemon_client,
        "wait_for_posix_process_group_exit",
        lambda process_group_id, *, timeout_seconds: drained_groups.append(process_group_id)
        or True,
    )

    force_shutdown_daemon_process(paths, timeout=0.0)

    assert cleanup_calls == [(paths, record)]
    assert drained_groups == []


def test_force_shutdown_fails_when_exited_posix_leader_has_remaining_descendants(
    tmp_path,
    monkeypatch,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    expected = _test_process_identity(321)
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )

    monkeypatch.setattr(daemon_client.os, "name", "posix")
    monkeypatch.setattr(daemon_client, "_local_daemon_process", lambda paths: record)
    monkeypatch.setattr(
        daemon_client,
        "daemon_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(DaemonClientError("unreachable")),
    )
    monkeypatch.setattr(daemon_client, "inspect_process_identity", lambda pid: None)
    monkeypatch.setattr(
        daemon_client,
        "wait_for_posix_process_group_exit",
        lambda process_group_id, *, timeout_seconds: False,
    )
    monkeypatch.setattr(
        daemon_client,
        "force_kill_verified_contained_process_tree",
        lambda *args, **kwargs: pytest.fail("an exited leader cannot be identity-verified"),
    )

    with pytest.raises(DaemonClientError, match="still has running descendants"):
        force_shutdown_daemon_process(paths, timeout=0.0)


def test_windows_force_shutdown_retains_verified_job_across_leader_exit(
    tmp_path,
    monkeypatch,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    expected = replace(
        _test_process_identity(321),
        process_group_id=None,
        process_session_id=7,
    )
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    events = []

    class _Job:
        def terminate(self, *, timeout_seconds: float) -> None:
            events.append(("terminate", timeout_seconds))

        def close(self) -> None:
            events.append(("close",))

    job = _Job()
    monkeypatch.setattr(daemon_client.os, "name", "nt")
    monkeypatch.setattr(daemon_client, "_local_daemon_process", lambda paths: record)
    monkeypatch.setattr(
        daemon_client,
        "open_verified_windows_kill_on_close_job",
        lambda identity, *, nonce: job,
    )
    monkeypatch.setattr(
        daemon_client,
        "ensure_windows_containment_job_stopped",
        lambda *args, **kwargs: pytest.fail("the retained verified Job must be used"),
    )
    monkeypatch.setattr(
        daemon_client,
        "daemon_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(DaemonClientError("unreachable")),
    )
    monkeypatch.setattr(daemon_client, "_expected_process_is_running", lambda identity: False)
    monkeypatch.setattr(
        daemon_client,
        "_cleanup_forced_shutdown",
        lambda selected_paths, *, process_record=None: events.append(
            ("cleanup", selected_paths, process_record)
        ),
    )
    monkeypatch.setattr(
        daemon_client,
        "force_kill_verified_contained_process_tree",
        lambda *args, **kwargs: pytest.fail("the leader already exited"),
    )

    force_shutdown_daemon_process(paths, timeout=0.0)

    assert events == [
        ("terminate", 2.0),
        ("cleanup", paths, record),
        ("close",),
    ]


def test_windows_startup_timeout_uses_retained_job_after_leader_exit(monkeypatch):
    expected = replace(
        _test_process_identity(321),
        process_group_id=None,
        process_session_id=7,
    )
    events = []

    class _Job:
        def terminate(self, *, timeout_seconds: float) -> None:
            events.append(("terminate", timeout_seconds))

        def close(self) -> None:
            events.append(("close",))

    job = _Job()
    paths = SimpleNamespace()
    monkeypatch.setattr(daemon_client.os, "name", "nt")
    with daemon_client._WINDOWS_LAUNCH_JOBS_LOCK:
        daemon_client._WINDOWS_LAUNCH_JOBS[expected] = job
    try:
        monkeypatch.setattr(
            daemon_client,
            "force_kill_verified_contained_process_tree",
            lambda *args, **kwargs: pytest.fail("the retained startup Job must be used"),
        )
        monkeypatch.setattr(
            daemon_client,
            "_release_failed_launch_lease",
            lambda selected_paths, *, containment_nonce: events.append(
                ("release_lease", selected_paths, containment_nonce)
            ),
        )

        daemon_client._cleanup_failed_daemon_startup(
            paths,
            expected,
            containment_nonce=_TEST_CONTAINMENT_NONCE,
        )
    finally:
        daemon_client._release_windows_launch_job(expected)

    assert events == [
        ("terminate", 2.0),
        ("release_lease", paths, _TEST_CONTAINMENT_NONCE),
        ("close",),
    ]


def test_force_shutdown_never_falls_back_when_containment_verification_fails(
    tmp_path,
    monkeypatch,
):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    expected = _test_process_identity(321)
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    kill_calls: list[tuple[object, str]] = []

    monkeypatch.setattr(daemon_client, "_local_daemon_process", lambda paths: record)
    monkeypatch.setattr(
        daemon_client,
        "daemon_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(DaemonClientError("unreachable")),
    )
    monkeypatch.setattr(daemon_client, "inspect_process_identity", lambda pid: expected)

    def _refuse(identity, *, containment_nonce):
        kill_calls.append((identity, containment_nonce))
        raise daemon_client.ProcessInspectionError("process is not in the named Job")

    monkeypatch.setattr(
        daemon_client,
        "force_kill_verified_contained_process_tree",
        _refuse,
    )

    with pytest.raises(DaemonClientError, match="Refused to terminate unverified"):
        force_shutdown_daemon_process(paths, timeout=0.0)

    assert kill_calls == [(expected, _TEST_CONTAINMENT_NONCE)]


def test_remote_lease_allows_verified_machine_local_observer_selection(
    tmp_path,
    monkeypatch,
):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    identity = _test_process_identity(444)
    now = utcnow_text()
    ledger = RuntimeControlLedger(paths.runtime_control_db_path)
    ledger.daemon_state.upsert(
        workspace_id=paths.workspace_id,
        daemon_id="observer-a",
        pid=identity.pid,
        process_start_key=identity.start_key,
        process_executable_path=identity.executable_path,
        process_group_id=identity.process_group_id,
        process_session_id=identity.process_session_id,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        endpoint_kind=paths.daemon_endpoint_kind,
        endpoint_path=paths.daemon_endpoint_path,
        started_at_utc=now,
        last_checkpoint_at_utc=now,
        status="leased",
        app_root=str(paths.app_root),
        workspace_root=str(paths.workspace_root),
    )
    monkeypatch.setattr(daemon_client, "_reachable_daemon_process", lambda paths: None)
    monkeypatch.setattr(
        daemon_client._SHARED_STATE_ADAPTER,
        "read_lease_metadata",
        lambda paths: {"machine_id": "another-installation"},
    )

    selected = daemon_client._local_daemon_process(paths)

    assert selected is not None
    assert selected.daemon_id == "observer-a"
    assert selected.process_identity == identity
    assert selected.containment_nonce == _TEST_CONTAINMENT_NONCE


def test_force_shutdown_rejects_same_machine_lease_replayed_into_another_workspace(
    tmp_path,
    monkeypatch,
):
    app_root = tmp_path / "data_engine"
    workspace_a = tmp_path / "shared" / "workspace-a"
    workspace_b = tmp_path / "shared" / "workspace-b"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_a)
    _write_demo_flow(workspace_b)
    paths_a = resolve_workspace_paths(
        workspace_root=workspace_a,
        workspace_id="shared-id",
    )
    paths_b = resolve_workspace_paths(
        workspace_root=workspace_b,
        workspace_id="shared-id",
    )
    identity = _test_process_identity(444)
    now = utcnow_text()
    RuntimeControlLedger(paths_a.runtime_control_db_path).daemon_state.upsert(
        workspace_id=paths_a.workspace_id,
        daemon_id="daemon-a",
        pid=identity.pid,
        process_start_key=identity.start_key,
        process_executable_path=identity.executable_path,
        process_group_id=identity.process_group_id,
        process_session_id=identity.process_session_id,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        endpoint_kind=paths_a.daemon_endpoint_kind,
        endpoint_path=paths_a.daemon_endpoint_path,
        started_at_utc=now,
        last_checkpoint_at_utc=now,
        status="running",
        app_root=str(paths_a.app_root),
        workspace_root=str(paths_a.workspace_root),
    )
    initialize_workspace_state(paths_b)
    assert claim_workspace(paths_b) is True
    checkpoint_workspace_state(
        paths_b,
        RuntimeCacheLedger(paths_b.runtime_db_path),
        workspace_id=paths_b.workspace_id,
        machine_id=machine_id_text(app_root=paths_b.app_root),
        host_name="test-host",
        daemon_id="daemon-a",
        pid=identity.pid,
        status="running",
        started_at_utc=now,
        last_checkpoint_at_utc=now,
        app_version="0.1.0",
    )

    monkeypatch.setattr(
        daemon_client,
        "daemon_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(DaemonClientError("unreachable")),
    )
    monkeypatch.setattr(daemon_client, "is_daemon_live", lambda paths: False)
    monkeypatch.setattr(
        daemon_client,
        "force_kill_verified_contained_process_tree",
        lambda *args, **kwargs: pytest.fail("replayed shared state must never authorize signaling"),
    )

    with pytest.raises(DaemonClientError, match="not corroborated"):
        force_shutdown_daemon_process(paths_b, timeout=0.0)


@pytest.mark.parametrize("mismatch", ["daemon_id", "start_key", "containment_nonce"])
def test_local_daemon_process_rejects_mismatched_local_corroboration(
    tmp_path,
    monkeypatch,
    mismatch,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    lease_record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=_test_process_identity(321),
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    local_record = lease_record
    if mismatch == "daemon_id":
        local_record = replace(local_record, daemon_id="daemon-b")
    elif mismatch == "start_key":
        local_record = replace(
            local_record,
            process_identity=replace(
                local_record.process_identity,
                start_key="replacement-start-key",
            ),
        )
    else:
        local_record = replace(local_record, containment_nonce="b" * 64)

    monkeypatch.setattr(daemon_client, "_reachable_daemon_process", lambda paths: None)
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_lease_process",
        lambda paths: lease_record,
    )
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: local_record,
    )

    with pytest.raises(DaemonClientError, match="does not match"):
        daemon_client._local_daemon_process(paths)


def test_recorded_local_daemon_accepts_windows_path_casing_variants(
    tmp_path,
    monkeypatch,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = replace(
        resolve_workspace_paths(workspace_root=workspace_root),
        daemon_endpoint_kind="pipe",
        daemon_endpoint_path=r"\\.\pipe\DataEngine-Default",
    )
    identity = _test_process_identity(321)
    started = utcnow_text()
    ledger = RuntimeControlLedger(paths.runtime_control_db_path)
    ledger.daemon_state.upsert(
        workspace_id=paths.workspace_id,
        daemon_id="daemon-a",
        pid=identity.pid,
        process_start_key=identity.start_key,
        process_executable_path=identity.executable_path,
        process_group_id=identity.process_group_id,
        process_session_id=identity.process_session_id,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        endpoint_kind="pipe",
        endpoint_path=paths.daemon_endpoint_path.swapcase(),
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        status="running",
        app_root=str(paths.app_root).swapcase(),
        workspace_root=str(paths.workspace_root).swapcase(),
    )
    ledger.close()
    monkeypatch.setattr(daemon_client.os, "name", "nt")

    record = daemon_client._recorded_local_daemon_process(paths)

    assert record is not None
    assert record.process_identity == identity
    assert record.containment_nonce == _TEST_CONTAINMENT_NONCE


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


def test_force_shutdown_preserves_different_installation_lease_with_same_hostname(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True
    local_machine_id = machine_id_text(app_root=paths.app_root)
    remote_machine_id = (
        "2a0ec090-7599-4578-a726-fd760f76f7f8"
        if local_machine_id != "2a0ec090-7599-4578-a726-fd760f76f7f8"
        else "ac066dc8-4491-44e5-932d-f0ce1a701c26"
    )
    started = utcnow_text()
    checkpoint_workspace_state(
        paths,
        RuntimeCacheLedger(paths.runtime_db_path),
        workspace_id="default",
        machine_id=remote_machine_id,
        host_name=host_name_text(),
        daemon_id="remote-daemon",
        pid=9876,
        status="running",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    monkeypatch.setattr("data_engine.hosts.daemon.client._reachable_daemon_process", lambda paths: None)
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)

    force_shutdown_daemon_process(paths)

    metadata = read_lease_metadata(paths)
    assert metadata is not None
    assert metadata["machine_id"] == remote_machine_id
    assert metadata["host_name"] == host_name_text()
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.state == "leased"
    assert not (paths.available_markers_dir / paths.workspace_id).exists()


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
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
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
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
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
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
        daemon_id="daemon-a",
        pid=99999,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    monkeypatch.setattr("data_engine.hosts.daemon.client._expected_process_is_running", lambda identity: True)
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client.time.sleep", lambda _seconds: None)

    def _fail_popen(*args, **kwargs):
        raise AssertionError("spawn_daemon_process should not launch a duplicate local daemon")

    monkeypatch.setattr("data_engine.hosts.daemon.client.subprocess.Popen", _fail_popen)

    with pytest.raises(Exception) as excinfo:
        spawn_daemon_process(paths)
    assert "already owns this workspace" in str(excinfo.value)


def test_spawn_waiter_takes_over_after_competing_launcher_releases_lock(
    tmp_path,
    monkeypatch,
):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    identity = _test_process_identity(321)
    acquire_results = iter((False, True))
    acquire_calls: list[object] = []
    launch_calls: list[object] = []
    release_calls: list[object] = []
    sleep_calls: list[float] = []

    def _acquire(selected_paths):
        acquire_calls.append(selected_paths)
        return next(acquire_results)

    monkeypatch.setattr(daemon_client, "is_daemon_live", lambda selected_paths: False)
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_fresh_local_daemon",
        lambda selected_paths: False,
    )
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_live_lease_process",
        lambda selected_paths: None,
    )
    monkeypatch.setattr(
        daemon_client,
        "_should_force_recover_local_lease",
        lambda selected_paths: False,
    )
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_unreachable_lease_metadata",
        lambda selected_paths: None,
    )
    monkeypatch.setattr(daemon_client, "_acquire_startup_lock", _acquire)
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_prior_local_daemon_release",
        lambda selected_paths: None,
    )
    monkeypatch.setattr(
        daemon_client,
        "new_process_containment_nonce",
        lambda: _TEST_CONTAINMENT_NONCE,
    )
    monkeypatch.setattr(
        daemon_client,
        "_launch_contained_daemon",
        lambda command, **kwargs: launch_calls.append(command) or identity,
    )
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_expected_daemon_live",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(daemon_client.time, "sleep", sleep_calls.append)
    monkeypatch.setattr(
        daemon_client,
        "_release_startup_lock",
        lambda selected_paths: release_calls.append(selected_paths),
    )

    assert spawn_daemon_process(paths) == 0

    assert acquire_calls == [paths, paths]
    assert len(launch_calls) == 1
    assert sleep_calls and sleep_calls[0] <= 0.1
    assert release_calls == [paths]


def test_spawn_migrates_and_discards_legacy_pid_only_daemon_state(
    tmp_path,
    monkeypatch,
):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    db_path = paths.runtime_control_db_path
    assert db_path is not None
    db_path.parent.mkdir(parents=True, exist_ok=True)
    now = utcnow_text()
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE daemon_state (
                workspace_id TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                endpoint_kind TEXT NOT NULL,
                endpoint_path TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                last_checkpoint_at_utc TEXT NOT NULL,
                status TEXT NOT NULL,
                app_root TEXT NOT NULL,
                workspace_root TEXT NOT NULL,
                version_text TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO daemon_state(
                workspace_id, pid, endpoint_kind, endpoint_path,
                started_at_utc, last_checkpoint_at_utc, status,
                app_root, workspace_root, version_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                paths.workspace_id,
                999_999,
                paths.daemon_endpoint_kind,
                paths.daemon_endpoint_path,
                now,
                now,
                "idle",
                str(paths.app_root),
                str(paths.workspace_root),
                "0.1.0",
            ),
        )

    identity = _test_process_identity(321)
    release_calls: list[object] = []
    monkeypatch.setattr(daemon_client, "is_daemon_live", lambda selected_paths: False)
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_fresh_local_daemon",
        lambda selected_paths: False,
    )
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_live_lease_process",
        lambda selected_paths: None,
    )
    monkeypatch.setattr(
        daemon_client,
        "_should_force_recover_local_lease",
        lambda selected_paths: False,
    )
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_unreachable_lease_metadata",
        lambda selected_paths: None,
    )
    monkeypatch.setattr(
        daemon_client,
        "_acquire_startup_lock_or_wait_for_daemon",
        lambda selected_paths, *, timeout_seconds: True,
    )
    monkeypatch.setattr(
        daemon_client,
        "new_process_containment_nonce",
        lambda: _TEST_CONTAINMENT_NONCE,
    )
    monkeypatch.setattr(
        daemon_client,
        "inspect_process_identity",
        lambda pid: pytest.fail("legacy PID-only state must not authorize inspection"),
    )

    def _launch(
        command,
        *,
        containment_nonce,
        on_identity_ready,
        on_verified_drain,
    ):
        assert command
        assert containment_nonce == _TEST_CONTAINMENT_NONCE
        assert callable(on_verified_drain)
        on_identity_ready(identity)
        return identity

    monkeypatch.setattr(daemon_client, "_launch_contained_daemon", _launch)
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_expected_daemon_live",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        daemon_client,
        "_release_startup_lock",
        lambda selected_paths: release_calls.append(selected_paths),
    )

    assert spawn_daemon_process(paths) == 0

    ledger = RuntimeControlLedger(db_path)
    try:
        state = ledger.daemon_state.get(paths.workspace_id)
    finally:
        ledger.close()
    assert state is not None
    assert state.daemon_id == f"launch-{_TEST_CONTAINMENT_NONCE}"
    assert state.pid == identity.pid
    assert state.process_start_key == identity.start_key
    assert state.containment_nonce == _TEST_CONTAINMENT_NONCE
    assert release_calls == [paths]


def test_spawn_daemon_process_uses_atomic_windows_containment(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "folder-name"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="explicit-id")
    monkeypatch.setattr("data_engine.hosts.daemon.client.os.name", "nt")
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client._wait_for_fresh_local_daemon", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client._same_machine_live_lease_process", lambda paths: None)
    monkeypatch.setattr("data_engine.hosts.daemon.client._should_force_recover_local_lease", lambda paths: False)
    monkeypatch.setattr("data_engine.hosts.daemon.client._same_machine_unreachable_lease_metadata", lambda paths: None)
    monkeypatch.setattr("data_engine.hosts.daemon.client._acquire_startup_lock", lambda paths: True)
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_expected_daemon_live",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        "data_engine.hosts.daemon.client.new_process_containment_nonce",
        lambda: _TEST_CONTAINMENT_NONCE,
    )

    captured: dict[str, object] = {}

    def _fake_spawn(
        executable,
        arguments,
        *,
        containment_nonce,
        before_resume=None,
        after_verified_cleanup=None,
    ):
        captured["executable"] = executable
        captured["arguments"] = arguments
        captured["containment_nonce"] = containment_nonce
        captured["after_verified_cleanup"] = after_verified_cleanup
        identity = _test_process_identity(123)
        if before_resume is not None:
            before_resume(identity)
        return SimpleNamespace(process_identity=identity)

    class _RetainedJob:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    retained_job = _RetainedJob()

    monkeypatch.setattr(
        "data_engine.hosts.daemon.client.spawn_windows_contained_process",
        _fake_spawn,
    )
    monkeypatch.setattr(
        daemon_client,
        "open_verified_windows_kill_on_close_job",
        lambda identity, *, nonce: retained_job,
    )

    assert spawn_daemon_process(paths) == 0
    arguments = captured["arguments"]
    assert isinstance(arguments, list)
    assert arguments[:3] == ["-P", "-m", "data_engine.daemon_bootstrap"]
    assert arguments[arguments.index("--workspace-id") + 1] == "explicit-id"
    assert arguments[arguments.index("--containment-nonce") + 1] == _TEST_CONTAINMENT_NONCE
    assert captured["executable"] == daemon_client.sys.executable
    assert captured["containment_nonce"] == _TEST_CONTAINMENT_NONCE
    assert callable(captured["after_verified_cleanup"])
    assert retained_job.closed is True


def test_spawn_timeout_terminates_exact_launched_containment_and_releases_lock(
    tmp_path,
    monkeypatch,
):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    identity = _test_process_identity(321)
    cleanup_calls: list[tuple[object, str]] = []
    release_calls: list[object] = []

    monkeypatch.setattr(daemon_client, "is_daemon_live", lambda paths: False)
    monkeypatch.setattr(daemon_client, "_wait_for_fresh_local_daemon", lambda paths: False)
    monkeypatch.setattr(daemon_client, "_same_machine_live_lease_process", lambda paths: None)
    monkeypatch.setattr(daemon_client, "_should_force_recover_local_lease", lambda paths: False)
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_unreachable_lease_metadata",
        lambda paths: None,
    )
    monkeypatch.setattr(daemon_client, "_acquire_startup_lock", lambda paths: True)
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_expected_daemon_live",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(
        daemon_client,
        "new_process_containment_nonce",
        lambda: _TEST_CONTAINMENT_NONCE,
    )
    monkeypatch.setattr(
        daemon_client,
        "_launch_contained_daemon",
        lambda command, *, containment_nonce, **kwargs: identity,
    )
    monkeypatch.setattr(
        daemon_client,
        "_cleanup_failed_daemon_startup",
        lambda selected_paths, selected_identity, *, containment_nonce: cleanup_calls.append(
            (selected_identity, containment_nonce)
        ),
    )
    monkeypatch.setattr(
        daemon_client,
        "_release_startup_lock",
        lambda selected_paths: release_calls.append(selected_paths),
    )

    with pytest.raises(DaemonClientError, match="Timed out waiting for daemon startup"):
        spawn_daemon_process(paths)

    assert cleanup_calls == [(identity, _TEST_CONTAINMENT_NONCE)]
    assert release_calls == [paths]


def test_spawn_readiness_error_terminates_launched_containment_and_releases_lock(
    tmp_path,
    monkeypatch,
):
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    identity = _test_process_identity(321)
    cleanup_calls: list[tuple[object, str]] = []
    release_calls: list[object] = []

    monkeypatch.setattr(daemon_client, "is_daemon_live", lambda paths: False)
    monkeypatch.setattr(daemon_client, "_wait_for_fresh_local_daemon", lambda paths: False)
    monkeypatch.setattr(daemon_client, "_same_machine_live_lease_process", lambda paths: None)
    monkeypatch.setattr(daemon_client, "_should_force_recover_local_lease", lambda paths: False)
    monkeypatch.setattr(
        daemon_client,
        "_same_machine_unreachable_lease_metadata",
        lambda paths: None,
    )
    monkeypatch.setattr(daemon_client, "_acquire_startup_lock", lambda paths: True)
    monkeypatch.setattr(
        daemon_client,
        "new_process_containment_nonce",
        lambda: _TEST_CONTAINMENT_NONCE,
    )
    monkeypatch.setattr(
        daemon_client,
        "_launch_contained_daemon",
        lambda command, *, containment_nonce, **kwargs: identity,
    )
    monkeypatch.setattr(
        daemon_client,
        "_wait_for_expected_daemon_live",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            DaemonClientError("different generation")
        ),
    )
    monkeypatch.setattr(
        daemon_client,
        "_cleanup_failed_daemon_startup",
        lambda selected_paths, selected_identity, *, containment_nonce: cleanup_calls.append(
            (selected_identity, containment_nonce)
        ),
    )
    monkeypatch.setattr(
        daemon_client,
        "_release_startup_lock",
        lambda selected_paths: release_calls.append(selected_paths),
    )

    with pytest.raises(DaemonClientError, match="different generation"):
        spawn_daemon_process(paths)

    assert cleanup_calls == [(identity, _TEST_CONTAINMENT_NONCE)]
    assert release_calls == [paths]


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_posix_daemon_launcher_reaps_exited_leader_before_group_drain(tmp_path):
    process_metadata_path = tmp_path / "contained-processes.txt"
    containment_nonce = daemon_client.new_process_containment_nonce()
    harness_source = r"""
import importlib.util
import json
import os
import subprocess
import sys
import time

package_root, module_path, metadata_path, containment_nonce = sys.argv[1:5]
ready_fd = int(sys.argv[sys.argv.index("--launch-ready-fd") + 1])
release_fd = int(sys.argv[sys.argv.index("--launch-release-fd") + 1])
spec = importlib.util.spec_from_file_location("_data_engine_posix_watchdog", module_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
watchdog = module.arm_posix_process_group_watchdog(containment_nonce=containment_nonce)
sys.path.insert(0, package_root)
from data_engine.platform.processes import inspect_process_identity

identity = inspect_process_identity(os.getpid())
if identity is None:
    raise RuntimeError("unable to inspect contained harness identity")
payload = json.dumps(
    {
        "pid": identity.pid,
        "start_key": identity.start_key,
        "executable_path": identity.executable_path,
        "process_group_id": identity.process_group_id,
        "process_session_id": identity.process_session_id,
    },
    separators=(",", ":"),
).encode("utf-8")
os.write(ready_fd, payload)
os.close(ready_fd)
if os.read(release_fd, 1) != b"1":
    raise RuntimeError("launcher did not release contained harness")
os.close(release_fd)
child = subprocess.Popen(
    [sys.executable, "-I", "-S", "-c", "import time; time.sleep(60)"],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    close_fds=True,
)
with open(metadata_path, "w", encoding="ascii") as stream:
    stream.write(f"{watchdog.pid} {child.pid}")
time.sleep(0.1)
"""
    command = [
        daemon_client.sys.executable,
        "-I",
        "-S",
        "-c",
        harness_source,
        str(Path(daemon_client.__file__).resolve().parents[3]),
        str(Path(posix_watchdog.__file__).resolve()),
        str(process_metadata_path),
        containment_nonce,
    ]
    identity = daemon_client._launch_contained_daemon(
        command,
        containment_nonce=containment_nonce,
        on_identity_ready=lambda _identity: None,
    )
    cleanup_pids = [identity.pid]
    try:
        daemon_client._wait_for_posix_daemon_group_exit(
            identity,
            timeout_seconds=3.0,
        )
        assert process_metadata_path.is_file()
        cleanup_pids.extend(
            int(value)
            for value in process_metadata_path.read_text(encoding="ascii").split()
        )
        assert identity not in daemon_client._POSIX_DAEMON_PROCESSES
    finally:
        if process_metadata_path.is_file():
            cleanup_pids.extend(
                int(value)
                for value in process_metadata_path.read_text(encoding="ascii").split()
            )
        for pid in set(cleanup_pids):
            try:
                if os.getpgid(pid) == identity.pid:
                    os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass


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


def _authkey_test_paths(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    authkey_path = paths.runtime_state_dir / daemon_client.DAEMON_AUTHKEY_FILE_NAME
    authkey_path.parent.mkdir(parents=True, exist_ok=True)
    return paths, authkey_path


def test_daemon_authkey_is_stable_per_workspace(tmp_path, monkeypatch):
    paths, _ = _authkey_test_paths(tmp_path, monkeypatch)

    first = daemon_authkey(paths)
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: pytest.fail("valid authkey reads must not inspect daemon ownership"),
    )
    second = daemon_authkey(paths)

    assert first == second
    assert len(first) == 32


@pytest.mark.parametrize(
    "invalid_bytes",
    [
        pytest.param(b"not hexadecimal", id="malformed-syntax"),
        pytest.param(b"ab" * 31, id="wrong-decoded-length"),
        pytest.param(b"\xff\xfe", id="non-ascii"),
    ],
)
def test_daemon_authkey_quarantines_invalid_unowned_file_and_regenerates(
    tmp_path,
    monkeypatch,
    invalid_bytes,
):
    paths, authkey_path = _authkey_test_paths(tmp_path, monkeypatch)
    authkey_path.write_bytes(invalid_bytes)
    replacement = bytes(range(32))
    monkeypatch.setattr(daemon_client.secrets, "token_bytes", lambda length: replacement)

    authkey = daemon_authkey(paths)

    quarantined = tuple(authkey_path.parent.glob(f"{authkey_path.name}.invalid-*"))
    assert authkey == replacement
    assert authkey_path.read_text(encoding="ascii") == replacement.hex()
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == invalid_bytes
    if os.name != "nt":
        assert quarantined[0].stat().st_mode & 0o777 == 0o600
    assert (authkey_path.parent / daemon_client._DAEMON_AUTHKEY_LOCK_FILE_NAME).is_file()


def test_daemon_authkey_serializes_concurrent_malformed_file_recovery(tmp_path, monkeypatch):
    paths, authkey_path = _authkey_test_paths(tmp_path, monkeypatch)
    authkey_path.write_text("malformed", encoding="ascii")
    real_read = daemon_client._read_daemon_authkey
    initial_read_barrier = threading.Barrier(2)
    local_state = threading.local()
    generated_keys: list[bytes] = []
    generated_keys_lock = threading.Lock()

    def _synchronized_initial_read(path):
        result = real_read(path)
        if not getattr(local_state, "performed_initial_read", False):
            local_state.performed_initial_read = True
            initial_read_barrier.wait(timeout=2.0)
        return result

    def _next_key(length):
        with generated_keys_lock:
            key = bytes([len(generated_keys) + 1]) * length
            generated_keys.append(key)
            return key

    monkeypatch.setattr(daemon_client, "_read_daemon_authkey", _synchronized_initial_read)
    monkeypatch.setattr(daemon_client.secrets, "token_bytes", _next_key)
    monkeypatch.setattr(daemon_client.secrets, "token_hex", lambda length: "ab" * length)

    with ThreadPoolExecutor(max_workers=2) as executor:
        authkeys = tuple(executor.map(lambda _: daemon_authkey(paths), range(2)))

    assert authkeys == (authkeys[0], authkeys[0])
    assert generated_keys == [authkeys[0]]
    assert authkey_path.read_text(encoding="ascii") == authkeys[0].hex()
    assert len(tuple(authkey_path.parent.glob(f"{authkey_path.name}.invalid-*"))) == 1


def test_daemon_authkey_refuses_recovery_while_recorded_local_daemon_is_live(tmp_path, monkeypatch):
    paths, authkey_path = _authkey_test_paths(tmp_path, monkeypatch)
    authkey_path.write_text("malformed", encoding="ascii")
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=_test_process_identity(4321),
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: record,
    )
    monkeypatch.setattr(
        daemon_client,
        "_expected_process_is_running",
        lambda identity: True,
    )

    with pytest.raises(DaemonClientError, match="local daemon may still own"):
        daemon_authkey(paths)

    assert authkey_path.read_text(encoding="ascii") == "malformed"
    assert tuple(authkey_path.parent.glob(f"{authkey_path.name}.invalid-*")) == ()


def test_daemon_authkey_refuses_recovery_while_local_workspace_lease_pid_is_live(tmp_path, monkeypatch):
    paths, authkey_path = _authkey_test_paths(tmp_path, monkeypatch)
    authkey_path.write_text("malformed", encoding="ascii")
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: None,
    )
    monkeypatch.setattr(
        daemon_client,
        "_expected_process_is_running",
        lambda identity: True,
    )
    monkeypatch.setattr(
        daemon_client._SHARED_STATE_ADAPTER,
        "read_lease_metadata",
        lambda paths: {
            "machine_id": machine_id_text(app_root=paths.app_root),
            "daemon_id": "daemon-a",
            "pid": 4321,
            **_owner_process_kwargs(4321),
        },
    )

    with pytest.raises(DaemonClientError, match="local daemon may still own"):
        daemon_authkey(paths)

    assert authkey_path.read_text(encoding="ascii") == "malformed"


def test_daemon_authkey_refuses_recovery_when_local_owner_state_is_uncertain(tmp_path, monkeypatch):
    paths, authkey_path = _authkey_test_paths(tmp_path, monkeypatch)
    authkey_path.write_text("malformed", encoding="ascii")

    def _fail_state_read(paths):
        raise daemon_client.sqlite3.DatabaseError("unreadable local state")

    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        _fail_state_read,
    )

    with pytest.raises(DaemonClientError, match="local daemon may still own"):
        daemon_authkey(paths)

    assert authkey_path.read_text(encoding="ascii") == "malformed"


def test_daemon_authkey_recovers_despite_remote_workspace_lease(tmp_path, monkeypatch):
    paths, authkey_path = _authkey_test_paths(tmp_path, monkeypatch)
    authkey_path.write_text("malformed", encoding="ascii")
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: None,
    )
    monkeypatch.setattr(
        daemon_client._SHARED_STATE_ADAPTER,
        "read_lease_metadata",
        lambda paths: {"machine_id": "another-machine", "pid": 9876},
    )

    replacement = bytes(range(32))
    monkeypatch.setattr(daemon_client.secrets, "token_bytes", lambda length: replacement)

    assert daemon_authkey(paths) == replacement
    assert authkey_path.read_text(encoding="ascii") == replacement.hex()


def test_daemon_authkey_recovers_when_tombstone_pid_was_reused(tmp_path, monkeypatch):
    paths, authkey_path = _authkey_test_paths(tmp_path, monkeypatch)
    authkey_path.write_text("malformed", encoding="ascii")
    expected = _test_process_identity(4321)
    replacement_process = replace(expected, start_key="replacement-start-key")
    record = daemon_client._DaemonProcessRecord(
        daemon_id="daemon-a",
        process_identity=expected,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    replacement_authkey = bytes(range(32))
    monkeypatch.setattr(
        daemon_client,
        "_recorded_local_daemon_process",
        lambda paths: record,
    )
    monkeypatch.setattr(
        daemon_client,
        "inspect_process_identity",
        lambda pid: replacement_process,
    )
    monkeypatch.setattr(
        daemon_client._SHARED_STATE_ADAPTER,
        "read_lease_metadata",
        lambda paths: None,
    )
    monkeypatch.setattr(
        daemon_client.secrets,
        "token_bytes",
        lambda length: replacement_authkey,
    )

    assert daemon_authkey(paths) == replacement_authkey
    assert authkey_path.read_text(encoding="ascii") == replacement_authkey.hex()


def test_daemon_authkey_recovers_despite_stale_unix_endpoint_file(tmp_path, monkeypatch):
    paths, authkey_path = _authkey_test_paths(tmp_path, monkeypatch)
    if paths.daemon_endpoint_kind != "unix":
        pytest.skip("Stale Unix endpoint files only apply to Unix sockets.")
    authkey_path.write_text("malformed", encoding="ascii")
    endpoint_path = Path(paths.daemon_endpoint_path)
    endpoint_path.write_text("stale", encoding="ascii")
    replacement = bytes(range(32))
    monkeypatch.setattr(daemon_client.secrets, "token_bytes", lambda length: replacement)

    try:
        assert daemon_authkey(paths) == replacement
    finally:
        endpoint_path.unlink(missing_ok=True)

    assert authkey_path.read_text(encoding="ascii") == replacement.hex()


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


def test_harden_private_file_permissions_uses_no_window_creationflags_on_windows(tmp_path, monkeypatch):
    path = tmp_path / "secret.txt"
    path.write_text("secret", encoding="utf-8")
    calls: list[dict[str, object]] = []

    monkeypatch.setenv("USERNAME", "codex-user")
    monkeypatch.setattr(daemon_client.os, "name", "nt")
    monkeypatch.setattr(
        daemon_client.subprocess,
        "run",
        lambda command, **kwargs: calls.append({"command": command, **kwargs}),
    )

    _harden_private_file_permissions(path)

    assert len(calls) == 1
    assert calls[0]["command"] == [
        "icacls",
        str(path),
        "/inheritance:r",
        "/grant:r",
        "codex-user:(F)",
    ]
    assert calls[0]["creationflags"] == daemon_client.windows_subprocess_creationflags(no_window=True)


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
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
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
