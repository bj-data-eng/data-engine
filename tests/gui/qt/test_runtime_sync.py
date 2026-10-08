# ruff: noqa: F401

from dataclasses import replace
from types import SimpleNamespace

import pytest

from .support import (
    test_daemon_wait_worker_schedules_sync_when_projection_changes,
    test_daemon_wait_worker_skips_sync_when_projection_is_unchanged,
    test_finish_daemon_sync_deduplicates_repeated_sync_error_logs,
    test_finish_daemon_sync_preserves_persisted_step_duration_on_flow_switch,
    test_finish_daemon_sync_replaces_stale_observed_operation_tracker,
    test_finish_daemon_sync_skips_unchanged_projection_redraw,
    test_sync_from_daemon_coalesces_nested_refresh_requests,
    test_sync_from_daemon_preserves_daemon_owned_runtime_truth,
)


@pytest.mark.parametrize("streamed", [False, True])
def test_queued_pre_crash_sync_cannot_restore_dead_generation(qapp, streamed):
    from data_engine.domain import DaemonStatusState
    from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
    from data_engine.services.daemon_state import DaemonUpdateBatch
    from data_engine.services.runtime_state import ControlSnapshot, EngineSnapshot, WorkspaceSnapshot
    from .support import _make_window, _dispose_window

    window = _make_window()
    try:
        window._load_flows()
        window.daemon_status = replace(
            DaemonStatusState.empty(), source="exited", daemon_id="dead", projection_version=7,
        )
        dead_status = window.daemon_status
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id, version=7,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="idle", daemon_live=False, local_process_dead=True),
            flows={}, active_runs={},
        )
        previous_snapshot = window.workspace_snapshot
        if streamed:
            snapshot = WorkspaceDaemonSnapshot(
                live=True, workspace_owned=True, leased_by_machine_id=None,
                runtime_active=True, runtime_stopping=False, manual_runs=(),
                last_checkpoint_at_utc=None, source="daemon", daemon_id="dead", projection_version=8,
            )
            window.runtime_controller.apply_daemon_update_batch(window, DaemonUpdateBatch(snapshot, ()))
        else:
            window.runtime_controller.finish_daemon_sync(window, {
                "workspace_token": window._workspace_binding_token(),
                "sync_state": SimpleNamespace(daemon_status=replace(
                    dead_status, source="daemon", engine_active=True, projection_version=8,
                )),
                "projection": object(),
                "workspace_snapshot": object(),
            })
        assert window.daemon_status is dead_status
        assert window.workspace_snapshot is previous_snapshot
        assert not window._daemon_sync_in_progress
    finally:
        _dispose_window(qapp, window)


def test_manual_reservation_keeps_stop_flow_available_without_active_file(qapp):
    from data_engine.domain import ManualRunState
    from data_engine.services.runtime_state import ControlSnapshot, EngineSnapshot, WorkspaceSnapshot
    from .support import _make_window, _dispose_window

    window = _make_window()
    try:
        window._load_flows()
        window._select_flow("poller")
        card = window.flow_cards["poller"]
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id, version=1,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, active_flow_names=(card.name,)),
            flows={}, active_runs={}, manual_runs=(ManualRunState(card.group, card.name),),
        )
        window.flow_controller.refresh_action_buttons(window)
        assert window.flow_run_button.text() == "Stop Flow"
        assert window.flow_run_button.isEnabled()
        assert window.engine_button.text() == "Stop Engine"
        assert window.engine_button.isEnabled()
    finally:
        _dispose_window(qapp, window)
