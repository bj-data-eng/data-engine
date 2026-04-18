from __future__ import annotations

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
