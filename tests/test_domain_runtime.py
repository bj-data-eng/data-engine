from __future__ import annotations

from data_engine.domain import DaemonStatusState, FlowCatalogEntry, RuntimeSessionState, WorkspaceControlState
from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot


def _sample_cards() -> tuple[FlowCatalogEntry, ...]:
    return (
        FlowCatalogEntry(
            name="poller",
            group="Imports",
            title="Claims Poller",
            description="",
            source_root="/tmp/input",
            target_root="/tmp/output",
            mode="poll",
            interval="30s",
            operations="Read -> Write",
            operation_items=("Read", "Write"),
            state="poll ready",
            valid=True,
            category="automated",
        ),
        FlowCatalogEntry(
            name="manual_review",
            group="Manual",
            title="Manual Review",
            description="",
            source_root="/tmp/input",
            target_root="/tmp/output",
            mode="manual",
            interval="-",
            operations="Build",
            operation_items=("Build",),
            state="manual",
            valid=True,
            category="manual",
        ),
    )


def test_runtime_session_state_projects_daemon_snapshot_into_surface_state():
    session = RuntimeSessionState.from_daemon_snapshot(
        WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=False,
            leased_by_machine_id="other-host",
            runtime_active=True,
            runtime_stopping=False,
            manual_runs=("manual_review",),
            last_checkpoint_at_utc=None,
            source="daemon",
        ),
        _sample_cards(),
    )

    assert session.workspace_owned is False
    assert session.leased_by_machine_id == "other-host"
    assert session.runtime_active is True
    assert session.active_runtime_flow_names == ("poller",)
    assert session.active_manual_runs == {"Manual": "manual_review"}
    assert session.manual_run_active is True
    assert session.control_available is False


def test_daemon_status_state_projects_idle_snapshot_to_empty_runtime_session():
    status = DaemonStatusState.from_snapshot(
        WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=True,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc="2026-04-04T12:00:00+00:00",
            source="lease",
        )
    )

    assert status.source == "lease"
    assert status.as_runtime_session(_sample_cards()) == RuntimeSessionState.empty()


def test_workspace_control_state_derives_control_and_blocked_text():
    control = WorkspaceControlState.from_snapshot(
        WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=False,
            leased_by_machine_id="other-host",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="lease",
        ),
        daemon_live=False,
        local_machine_id="local-host",
        control_request=None,
        daemon_startup_in_progress=False,
    )

    assert control.daemon_status.leased_by_machine_id == "other-host"
    assert control.control_status_text == "other-host has control"
    assert control.blocked_status_text == "other-host currently has control of this workspace."


def test_workspace_control_state_reports_local_request_pending():
    control = WorkspaceControlState.from_snapshot(
        WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=False,
            leased_by_machine_id="other-host",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="lease",
        ),
        daemon_live=False,
        local_machine_id="local-host",
        control_request={"requester_machine_id": "local-host"},
        daemon_startup_in_progress=False,
    )

    assert control.local_request_pending is True
    assert control.control_status_text == "Control requested from other-host"
