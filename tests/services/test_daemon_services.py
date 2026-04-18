from __future__ import annotations

import threading

from data_engine.domain import DaemonLifecyclePolicy, WorkspaceControlState
from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
from data_engine.services.daemon import DaemonService
from data_engine.services.daemon_state import DaemonStateService

from tests.services.support import resolve_workspace_paths


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
    request_calls: list[tuple[object, dict[str, object], float]] = []
    live_calls: list[object] = []

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
            self.wait_calls: list[float] = []

        def sync(self):
            return snapshot

        def wait_for_update(self, *, timeout_seconds: float = 5.0):
            self.wait_calls.append(timeout_seconds)
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
    assert service.wait_for_update(manager, timeout_seconds=1.25) is snapshot
    assert manager.control_calls == [True]
    assert manager.wait_calls == [1.25]


def test_daemon_state_service_subscription_loop_emits_only_changed_updates():
    snapshots = [
        WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
            projection_version=1,
        ),
        WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
            projection_version=1,
        ),
        WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=True,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
            projection_version=2,
        ),
    ]

    class _Manager:
        def __init__(self) -> None:
            self.index = 0
            self._last_snapshot = None

        def wait_for_update(self, *, timeout_seconds: float = 5.0):
            del timeout_seconds
            snapshot = snapshots[self.index]
            self.index += 1
            self._last_snapshot = snapshot
            return snapshot

    manager = _Manager()
    service = DaemonStateService()
    updates: list[int] = []
    stop_event = threading.Event()

    def _on_update(snapshot):
        updates.append(snapshot.projection_version)
        if snapshot.projection_version >= 2:
            stop_event.set()

    service.run_subscription_loop(
        manager,
        stop_event=stop_event,
        workspace_available=lambda: True,
        on_update=_on_update,
        timeout_seconds=0.0,
    )

    assert updates == [1, 2]


def test_daemon_state_service_subscription_loop_waits_while_workspace_is_unavailable():
    snapshot = WorkspaceDaemonSnapshot(
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

    class _Manager:
        def __init__(self) -> None:
            self.wait_calls = 0
            self._last_snapshot = None

        def wait_for_update(self, *, timeout_seconds: float = 5.0):
            del timeout_seconds
            self.wait_calls += 1
            self._last_snapshot = snapshot
            return snapshot

    manager = _Manager()
    service = DaemonStateService()
    stop_event = threading.Event()
    availability_checks = {"count": 0}
    updates: list[int] = []

    def _workspace_available() -> bool:
        availability_checks["count"] += 1
        return availability_checks["count"] >= 2

    def _on_update(snapshot_arg):
        updates.append(snapshot_arg.projection_version)
        stop_event.set()

    service.run_subscription_loop(
        manager,
        stop_event=stop_event,
        workspace_available=_workspace_available,
        on_update=_on_update,
        timeout_seconds=0.0,
    )

    assert manager.wait_calls == 1
    assert updates == [1]


def test_daemon_state_service_heartbeat_policy_prefers_healthy_subscription():
    service = DaemonStateService()

    assert (
        service.should_run_heartbeat(
            daemon_live=True,
            transport_mode="subscription",
            wait_worker_alive=True,
            now_monotonic=110.0,
            last_sync_monotonic=100.0,
            last_subscription_monotonic=109.0,
            stale_after_seconds=15.0,
        )
        is False
    )
    assert (
        service.should_run_heartbeat(
            daemon_live=True,
            transport_mode="subscription",
            wait_worker_alive=True,
            now_monotonic=130.5,
            last_sync_monotonic=100.0,
            last_subscription_monotonic=109.0,
            stale_after_seconds=15.0,
        )
        is True
    )
