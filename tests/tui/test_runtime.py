from __future__ import annotations

from dataclasses import replace

import pytest

from data_engine.domain import RuntimeSessionState
from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
from data_engine.services.runtime_state import ControlSnapshot, EngineSnapshot, RunLiveSnapshot, WorkspaceSnapshot

from tests.tui.support import (
    FakeDaemonStateService,
    FakeRuntimeController,
    FakeSharedStateService,
    RecordingStatusTui,
    SyncingDaemonManager,
    make_tui,
    resolve_workspace_paths,
)


@pytest.mark.anyio
async def test_tui_disables_run_and_start_when_workspace_not_owned():
    app = make_tui()
    async with app.run_test():
        app.runtime_session = replace(app.runtime_session, workspace_owned=False, leased_by_machine_id="other-host")
        app._refresh_buttons()

        assert app.query_one("#run-once").disabled is True
        assert app.query_one("#start-engine").disabled is True


@pytest.mark.anyio
async def test_tui_refresh_buttons_prefers_daemon_live_stopping_run_after_rebind():
    app = make_tui()
    async with app.run_test():
        app.selected_flow_name = "manual_review"
        app.runtime_session = RuntimeSessionState.empty()
        app.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=app.workspace_paths.workspace_id,
            version=4,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={
                "run-manual": RunLiveSnapshot(
                    run_id="run-manual",
                    flow_name="manual_review",
                    group_name="Manual",
                    source_path=None,
                    state="stopping",
                    current_step_name="Build Report",
                    started_at_utc="2026-04-18T12:00:00+00:00",
                    elapsed_seconds=5.0,
                )
            },
        )

        app._refresh_buttons()

        assert app.query_one("#run-once").disabled is True
        assert app.query_one("#clear-flow-log").disabled is True


@pytest.mark.anyio
async def test_tui_refresh_buttons_disable_runtime_controls_while_engine_is_starting():
    app = make_tui()
    async with app.run_test():
        app.selected_flow_name = "poller"
        app.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=app.workspace_paths.workspace_id,
            version=5,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="starting", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )

        app._refresh_buttons()

        assert app.query_one("#refresh").disabled is True
        assert app.query_one("#run-once").disabled is True
        assert app.query_one("#start-engine").disabled is True
        assert app.query_one("#stop-engine").disabled is True
        assert app.query_one("#clear-flow-log").disabled is True


@pytest.mark.anyio
async def test_tui_refresh_buttons_preserve_workspace_selector_disabled_state():
    app = make_tui()
    async with app.run_test():
        selector = app.query_one("#workspace-select")
        selector.disabled = True

        app._refresh_buttons()

        assert selector.disabled is True


@pytest.mark.anyio
async def test_tui_tolerates_brief_daemon_sync_miss_without_flipping_to_lease_view():
    app = make_tui()
    async with app.run_test():
        app.runtime_session = replace(app.runtime_session, workspace_owned=True)
        app._daemon_manager._last_snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="daemon",
        )
        app._daemon_manager._sync_misses = 0

        app._sync_daemon_state()

        assert app.runtime_session.workspace_owned is True
        assert app._daemon_manager._sync_misses == 1


@pytest.mark.anyio
async def test_tui_hydrates_shared_runtime_logs_when_observing_lease():
    shared_state_service = FakeSharedStateService()
    app = make_tui(
        shared_state_service=shared_state_service,
        daemon_state_service=FakeDaemonStateService(
            SyncingDaemonManager(
                WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=False,
                    leased_by_machine_id="other-host",
                    runtime_active=False,
                    runtime_stopping=False,
                    manual_runs=(),
                    last_checkpoint_at_utc=None,
                    source="lease",
                )
            )
        ),
    )
    async with app.run_test() as pilot:
        del pilot

        app._sync_daemon_state()

        assert shared_state_service.hydrated
        assert shared_state_service.hydrated[-1][0] == app.workspace_paths
        assert shared_state_service.hydrated[-1][1] is app.runtime_binding.runtime_cache_ledger


def test_tui_daemon_wait_worker_schedules_sync_when_projection_changes(monkeypatch):
    app = make_tui()
    scheduled: list[str] = []
    previous_snapshot = WorkspaceDaemonSnapshot(
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
    next_snapshot = WorkspaceDaemonSnapshot(
        live=True,
        workspace_owned=True,
        leased_by_machine_id=None,
        runtime_active=False,
        runtime_stopping=False,
        manual_runs=(),
        last_checkpoint_at_utc=None,
        source="daemon",
        projection_version=2,
    )
    app.runtime_binding.daemon_manager._last_snapshot = previous_snapshot

    def _wait_for_update(manager, *, timeout_seconds: float = 5.0):
        del timeout_seconds
        manager._last_snapshot = next_snapshot
        return next_snapshot

    app.daemon_state_service.wait_for_update = _wait_for_update
    monkeypatch.setattr(app, "_schedule_daemon_update_sync", lambda: scheduled.append("sync") or app.daemon_subscription.stop())

    app.runtime_controller.daemon_wait_worker(app)

    assert scheduled == ["sync"]


def test_tui_daemon_wait_worker_skips_sync_when_projection_is_unchanged(monkeypatch):
    app = make_tui()
    scheduled: list[str] = []
    previous_snapshot = WorkspaceDaemonSnapshot(
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
    app.runtime_binding.daemon_manager._last_snapshot = previous_snapshot

    def _wait_for_update(manager, *, timeout_seconds: float = 5.0):
        del timeout_seconds
        manager._last_snapshot = previous_snapshot
        app.daemon_subscription.stop()
        return previous_snapshot

    app.daemon_state_service.wait_for_update = _wait_for_update
    monkeypatch.setattr(app, "_schedule_daemon_update_sync", lambda: scheduled.append("sync"))

    app.runtime_controller.daemon_wait_worker(app)

    assert scheduled == []


@pytest.mark.anyio
async def test_tui_does_not_bootstrap_daemon_without_authored_workspace(tmp_path):
    spawn_calls: list[object] = []
    app = make_tui(spawn_process_func=lambda paths: spawn_calls.append(paths) or 0)
    empty_root = tmp_path / "empty_workspace"
    empty_root.mkdir(parents=True)
    app.workspace_paths = resolve_workspace_paths(workspace_root=empty_root)

    async with app.run_test():
        assert spawn_calls == []


@pytest.mark.anyio
async def test_tui_sync_daemon_state_stops_pinging_when_workspace_root_is_missing(tmp_path):
    live_calls: list[object] = []
    app = make_tui(is_live_func=lambda paths: live_calls.append(paths) or False)
    missing_root = tmp_path / "missing_workspace"
    app.workspace_paths = resolve_workspace_paths(workspace_root=missing_root)
    app.runtime_session = replace(app.runtime_session, runtime_active=True, workspace_owned=False)

    async with app.run_test():
        app._sync_daemon_state()

        assert live_calls == []
        assert app.runtime_session == RuntimeSessionState.empty()
        assert app.workspace_snapshot is None


def test_tui_daemon_startup_uses_verbose_fallback_when_error_text_is_blank():
    app = make_tui(app_cls=RecordingStatusTui)
    runtime_controller = FakeRuntimeController()
    app.runtime_controller = runtime_controller
    app._finish_daemon_startup(False, "")

    assert app.status_messages == [
        "Daemon startup did not provide any additional error details.",
    ]
    assert runtime_controller.sync_calls == [app]
