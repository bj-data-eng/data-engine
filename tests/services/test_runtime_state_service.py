from __future__ import annotations

from data_engine.domain import (
    ActiveRunState,
    DaemonStatusState,
    FlowCatalogEntry,
    OperationSessionState,
    RuntimeSessionState,
    WorkspaceControlState,
)
from data_engine.services.runtime_state import RuntimeStateService


def test_runtime_state_service_returns_unified_workspace_snapshot():
    sync_state = type(
        "_SyncState",
        (),
        {
            "daemon_status": DaemonStatusState(
                workspace_owned=True,
                leased_by_machine_id=None,
                engine_active=True,
                engine_stopping=False,
                manual_run_names=(),
                last_checkpoint_at_utc=None,
                source="daemon",
            ),
            "workspace_control_state": WorkspaceControlState.empty(),
            "runtime_session": RuntimeSessionState(
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=True,
                runtime_stopping=False,
                active_runtime_flow_names=("poller", "scheduler"),
                manual_runs=(),
            ),
            "snapshot_source": "daemon",
            "snapshot": type("_Snapshot", (), {"live": True, "active_engine_flow_names": ("poller",)})(),
        },
    )()

    class _BindingService:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def sync_runtime_state(self, binding, *, runtime_application, flow_cards, daemon_startup_in_progress=False):
            del binding, runtime_application, flow_cards, daemon_startup_in_progress
            self.calls.append("sync")
            return sync_state

        def reload_logs(self, binding) -> None:
            del binding
            self.calls.append("reload")

        def rebuild_step_outputs(self, binding, flow_cards):
            del binding, flow_cards
            self.calls.append("step_outputs")
            return "step-index"

    class _LogService:
        def all_entries(self, log_store):
            assert log_store == "log-store"
            return ("log-a", "log-b")

        def runs_for_flow(self, log_store, flow_name):
            assert log_store == "log-store"
            assert flow_name == "poller"
            return ()

    class _RuntimeApp:
        def build_runtime_snapshot(self, *, flow_cards, log_entries, runtime_session, now):
            assert tuple(flow_cards) == (card,)
            assert log_entries == ("log-a", "log-b")
            assert runtime_session is sync_state.runtime_session
            assert now == 123.0
            return type(
                "_Presentation",
                (),
                {
                    "operation_tracker": OperationSessionState.empty(),
                    "flow_states": {"poller": "polling"},
                    "active_runtime_flow_names": ("poller",),
                },
            )()

    card = FlowCatalogEntry(
        name="poller",
        group="Imports",
        title="Poller",
        description="",
        source_root="inbox",
        target_root="outbox",
        mode="poll",
        interval="5s",
        settle="1",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="poll ready",
        valid=True,
        category="automated",
    )
    binding = type(
        "_Binding",
        (),
        {
            "log_store": "log-store",
            "workspace_paths": type("_Paths", (), {"workspace_id": "claims2"})(),
        },
    )()
    service = RuntimeStateService(runtime_binding_service=_BindingService(), log_service=_LogService())

    snapshot = service.current_snapshot(
        binding,
        runtime_application=_RuntimeApp(),
        flow_cards=(card,),
        now=123.0,
    )

    assert snapshot.workspace_id == "claims2"
    assert snapshot.version == 1
    assert snapshot.control.state == "available"
    assert snapshot.control.blocked_status_text == "Takeover available."
    assert snapshot.engine.state == "running"
    assert snapshot.engine.daemon_live is True
    assert snapshot.engine.active_flow_names == ("poller",)
    assert snapshot.active_runs == {}
    assert snapshot.flows["poller"].flow_name == "poller"
    assert snapshot.flows["poller"].group_name == "Imports"
    assert snapshot.flows["poller"].state == "polling"
    assert snapshot.flows["poller"].active_run_count == 0

    projection = service.rebuild_projection(
        binding,
        runtime_application=_RuntimeApp(),
        flow_cards=(card,),
        runtime_session=sync_state.runtime_session,
        now=123.0,
    )
    rebuilt = service.snapshot_from_projection(
        binding=binding,
        flow_cards=(card,),
        projection=projection,
        workspace_control_state=sync_state.workspace_control_state,
        daemon_live=True,
        daemon_active_flow_names=("poller",),
    )

    assert rebuilt == snapshot
    assert projection.runtime_session == sync_state.runtime_session.with_active_runtime_flow_names(("poller",))
    assert projection.operation_tracker == OperationSessionState.empty()
    assert projection.flow_states == {"poller": "polling"}
    assert projection.active_runtime_flow_names == ("poller",)
    assert projection.step_output_index == "step-index"


def test_runtime_state_service_emits_snapshot_events_to_subscribers():
    sync_state = type(
        "_SyncState",
        (),
        {
            "daemon_status": DaemonStatusState(
                workspace_owned=True,
                leased_by_machine_id=None,
                engine_active=True,
                engine_stopping=False,
                manual_run_names=(),
                last_checkpoint_at_utc=None,
                source="daemon",
            ),
            "workspace_control_state": WorkspaceControlState.empty(),
            "runtime_session": RuntimeSessionState(
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=True,
                runtime_stopping=False,
                active_runtime_flow_names=("poller",),
                manual_runs=(),
            ),
            "snapshot_source": "daemon",
            "snapshot": type("_Snapshot", (), {"live": True})(),
        },
    )()

    class _BindingService:
        def sync_runtime_state(self, binding, *, runtime_application, flow_cards, daemon_startup_in_progress=False):
            del binding, runtime_application, flow_cards, daemon_startup_in_progress
            return sync_state

        def reload_logs(self, binding) -> None:
            del binding

        def rebuild_step_outputs(self, binding, flow_cards):
            del binding, flow_cards
            return "step-index"

    class _LogService:
        def all_entries(self, log_store):
            del log_store
            return ()

        def runs_for_flow(self, log_store, flow_name):
            del log_store, flow_name
            return ()

    class _RuntimeApp:
        def build_runtime_snapshot(self, *, flow_cards, log_entries, runtime_session, now):
            del flow_cards, log_entries, runtime_session, now
            return type(
                "_Presentation",
                (),
                {
                    "operation_tracker": OperationSessionState.empty(),
                    "flow_states": {"poller": "polling"},
                    "active_runtime_flow_names": ("poller",),
                },
            )()

    card = FlowCatalogEntry(
        name="poller",
        group="Imports",
        title="Poller",
        description="",
        source_root="inbox",
        target_root="outbox",
        mode="poll",
        interval="5s",
        settle="1",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="poll ready",
        valid=True,
        category="automated",
    )
    binding = type(
        "_Binding",
        (),
        {
            "log_store": "log-store",
            "workspace_paths": type("_Paths", (), {"workspace_id": "claims2"})(),
        },
    )()
    service = RuntimeStateService(runtime_binding_service=_BindingService(), log_service=_LogService())
    events = []
    token = service.subscribe(workspace_id="claims2", callback=events.append)

    snapshot = service.current_snapshot(
        binding,
        runtime_application=_RuntimeApp(),
        flow_cards=(card,),
        now=123.0,
    )

    service.unsubscribe(token)

    assert snapshot.version == 1
    assert [event.event_type for event in events] == [
        "control.changed",
        "engine.changed",
        "flow.changed",
    ]
    assert all(event.workspace_id == "claims2" for event in events)


def test_runtime_state_service_uses_daemon_projection_version_and_engine_starting():
    sync_state = type(
        "_SyncState",
        (),
        {
            "daemon_status": DaemonStatusState(
                workspace_owned=True,
                leased_by_machine_id=None,
                engine_active=False,
                engine_stopping=False,
                engine_starting=True,
                projection_version=7,
                manual_run_names=(),
                last_checkpoint_at_utc=None,
                source="daemon",
            ),
            "workspace_control_state": WorkspaceControlState.empty(),
            "runtime_session": RuntimeSessionState(
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                active_runtime_flow_names=(),
                manual_runs=(),
            ),
            "snapshot_source": "daemon",
            "snapshot": type("_Snapshot", (), {"live": True, "engine_starting": True, "projection_version": 7})(),
        },
    )()

    class _BindingService:
        def sync_runtime_state(self, binding, *, runtime_application, flow_cards, daemon_startup_in_progress=False):
            del binding, runtime_application, flow_cards, daemon_startup_in_progress
            return sync_state

        def reload_logs(self, binding) -> None:
            del binding

        def rebuild_step_outputs(self, binding, flow_cards):
            del binding, flow_cards
            return {}

    class _LogService:
        def all_entries(self, log_store):
            del log_store
            return ()

        def runs_for_flow(self, log_store, flow_name):
            del log_store, flow_name
            return ()

    class _RuntimeApp:
        def build_runtime_snapshot(self, *, flow_cards, log_entries, runtime_session, now):
            del flow_cards, log_entries, runtime_session, now
            return type(
                "_Presentation",
                (),
                {
                    "operation_tracker": OperationSessionState.empty(),
                    "flow_states": {},
                    "active_runtime_flow_names": (),
                },
            )()

    binding = type(
        "_Binding",
        (),
        {
            "log_store": "log-store",
            "workspace_paths": type("_Paths", (), {"workspace_id": "claims2"})(),
        },
    )()
    service = RuntimeStateService(runtime_binding_service=_BindingService(), log_service=_LogService())

    snapshot = service.current_snapshot(
        binding,
        runtime_application=_RuntimeApp(),
        flow_cards=(),
        now=123.0,
    )

    assert snapshot.version == 7
    assert snapshot.engine.state == "starting"


def test_runtime_state_service_prefers_daemon_active_runs_over_log_reconstruction():
    active_run = ActiveRunState(
        run_id="run-1",
        flow_name="poller",
        group_name="Imports",
        source_path="claims.xlsx",
        state="running",
        current_step_name="Emit Value",
        started_at_utc="2026-04-17T00:00:00+00:00",
        elapsed_seconds=12.5,
    )
    sync_state = type(
        "_SyncState",
        (),
        {
            "daemon_status": DaemonStatusState(
                workspace_owned=True,
                leased_by_machine_id=None,
                engine_active=True,
                engine_stopping=False,
                active_runs=(active_run,),
                manual_run_names=(),
                last_checkpoint_at_utc=None,
                source="daemon",
            ),
            "workspace_control_state": WorkspaceControlState.empty(),
            "runtime_session": RuntimeSessionState(
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=True,
                runtime_stopping=False,
                active_runtime_flow_names=("poller",),
                manual_runs=(),
            ),
            "snapshot_source": "daemon",
            "snapshot": type(
                "_Snapshot",
                (),
                {
                    "live": True,
                    "active_engine_flow_names": ("poller",),
                    "active_runs": (active_run,),
                },
            )(),
        },
    )()

    class _BindingService:
        def sync_runtime_state(self, binding, *, runtime_application, flow_cards, daemon_startup_in_progress=False):
            del binding, runtime_application, flow_cards, daemon_startup_in_progress
            return sync_state

        def reload_logs(self, binding) -> None:
            del binding

        def rebuild_step_outputs(self, binding, flow_cards):
            del binding, flow_cards
            return {}

    class _LogService:
        def all_entries(self, log_store):
            del log_store
            return ()

        def runs_for_flow(self, log_store, flow_name):
            del log_store, flow_name
            return ()

    class _RuntimeApp:
        def build_runtime_snapshot(self, *, flow_cards, log_entries, runtime_session, now):
            del flow_cards, log_entries, runtime_session, now
            return type(
                "_Presentation",
                (),
                {
                    "operation_tracker": OperationSessionState.empty(),
                    "flow_states": {"poller": "polling"},
                    "active_runtime_flow_names": ("poller",),
                },
            )()

    card = FlowCatalogEntry(
        name="poller",
        group="Imports",
        title="Poller",
        description="",
        source_root="inbox",
        target_root="outbox",
        mode="poll",
        interval="5s",
        settle="1",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="poll ready",
        valid=True,
        category="automated",
    )
    binding = type(
        "_Binding",
        (),
        {
            "log_store": "log-store",
            "workspace_paths": type("_Paths", (), {"workspace_id": "claims2"})(),
        },
    )()
    service = RuntimeStateService(runtime_binding_service=_BindingService(), log_service=_LogService())

    snapshot = service.current_snapshot(
        binding,
        runtime_application=_RuntimeApp(),
        flow_cards=(card,),
        now=123.0,
    )

    assert tuple(snapshot.active_runs) == ("run-1",)
    assert snapshot.active_runs["run-1"].current_step_name == "Emit Value"
    assert snapshot.active_runs["run-1"].elapsed_seconds == 12.5
    assert snapshot.flows["poller"].active_run_count == 1
