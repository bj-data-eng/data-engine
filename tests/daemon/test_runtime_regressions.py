from __future__ import annotations

from contextlib import nullcontext
from threading import Event, RLock, Thread
from types import SimpleNamespace

import pytest

from data_engine.authoring.flow import Flow
from data_engine.hosts.daemon.app import DataEngineDaemonService
from data_engine.hosts.daemon.composition import DaemonHostDependencies, DaemonHostIdentity
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, WorkspaceDaemonSnapshot
from data_engine.hosts.daemon.runtime_events import DaemonRuntimeEvent, DaemonRuntimeProjector
from data_engine.hosts.daemon.runtime_ledger import DaemonRuntimeCacheProxy
from data_engine.hosts.daemon.state_sync import DaemonStateSyncHandler
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.services.runtime_execution import RuntimeExecutionService

from .support import _TEST_CONTAINMENT_NONCE, _test_process_identity
from tests.services.support import resolve_workspace_paths


@pytest.mark.parametrize("owner_alive", [False, True])
def test_disconnection_clears_work_only_after_exact_local_owner_exits(tmp_path, monkeypatch, owner_alive):
    from data_engine.hosts.daemon import client
    from data_engine.domain import ActiveRunState, RuntimeSessionState

    manager = WorkspaceDaemonManager(resolve_workspace_paths(workspace_root=tmp_path / "workspace"))
    manager._last_snapshot = WorkspaceDaemonSnapshot(
        live=True, workspace_owned=True, leased_by_machine_id=None, runtime_active=True,
        runtime_stopping=False, manual_runs=("poll",), last_checkpoint_at_utc=utcnow_text(),
        source="daemon", daemon_id="old", active_engine_flow_names=("poll",),
        active_runs=(ActiveRunState("run", "poll", "Poll", None, "running"),),
    )
    manager.shared_state_adapter.read_lease_metadata = lambda paths: {
        "machine_id": manager.machine_id, "last_checkpoint_at_utc": utcnow_text(),
    }
    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: False)
    monkeypatch.setattr(client, "_same_machine_lease_process", lambda paths: SimpleNamespace(daemon_id="old", process_identity=_test_process_identity(123)))
    monkeypatch.setattr(client, "_expected_process_is_running", lambda identity: owner_alive)
    previous = manager._last_snapshot
    snapshot = manager.sync()
    if owner_alive:
        assert snapshot.source == "cached"
        assert snapshot.runtime_active and snapshot.active_runs
    else:
        assert snapshot.source == "exited"
        assert not snapshot.runtime_active and not snapshot.active_runs and not snapshot.manual_runs
        assert not RuntimeSessionState.from_daemon_snapshot(snapshot, ()).control_available
        for starting in (False, True):
            control = manager.control_state(snapshot, daemon_startup_in_progress=starting)
            assert control.control_status_text == "Local engine is unavailable"
        delayed = _status_handler("old", owned=True, active=True).status_payload()
        assert manager._snapshot_from_status_dict(
            delayed, assume_live=True, transport_mode="subscription", requested_from=previous,
        ) is snapshot
        assert manager._snapshot_from_status_dict(
            None, assume_live=True, transport_mode="heartbeat", requested_from=snapshot,
        ) is snapshot
        monkeypatch.setattr(client, "_same_machine_lease_process", lambda paths: None)
        assert manager.sync() is snapshot
        replacement = _status_handler("replacement", owned=True, active=False).status_payload()
        restored = manager._snapshot_from_status_dict(
            replacement, assume_live=True, transport_mode="heartbeat", requested_from=snapshot,
        )
        assert restored.live and restored.daemon_id == "replacement"


@pytest.fixture
def service(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    paths = SimpleNamespace(
        workspace_id="test", workspace_root=tmp_path, runtime_state_dir=tmp_path,
        daemon_log_path=tmp_path / "daemon.log",
    )
    dependencies = DaemonHostDependencies(
        runtime_cache_ledger=ledger,
        runtime_control_ledger=SimpleNamespace(),
        flow_catalog_service=SimpleNamespace(),
        flow_execution_service=SimpleNamespace(load_flows=lambda *args, **kwargs: (Flow(name="poll", group="Poll"),)),
        runtime_execution_service=RuntimeExecutionService(),
        shared_state_adapter=SimpleNamespace(assert_workspace_lease=lambda *args, **kwargs: None),
    )
    identity = DaemonHostIdentity(
        machine_id="test-machine", host_name="test-host", daemon_id="test-daemon",
        process_identity=_test_process_identity(123), containment_nonce=_TEST_CONTAINMENT_NONCE,
    )
    obj = DataEngineDaemonService(paths, dependencies=dependencies, identity=identity)
    obj._timed_operation = lambda *args, **kwargs: nullcontext()
    obj._debug_log = lambda *args: None
    obj.state.claim_workspace("test-token")
    obj._load_flow_cards = lambda **kwargs: (
        SimpleNamespace(name="poll", valid=True, mode="poll", group="Poll", manual_inputs=()),
        SimpleNamespace(name="manual", valid=True, mode="manual", group="Manual", manual_inputs=()),
    )
    yield obj
    ledger.close()


def test_dead_local_lease_recovers_without_waiting_for_staleness(tmp_path, monkeypatch):
    from data_engine.hosts.daemon import client
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    record = SimpleNamespace(daemon_id="old", process_identity=_test_process_identity(123))
    metadata = {"lease_token": "token", "last_checkpoint_at_utc": utcnow_text()}
    drained = []
    monkeypatch.setattr(client, "_same_machine_unreachable_lease_metadata", lambda paths: metadata)
    monkeypatch.setattr(client, "_same_machine_lease_process", lambda paths: record)
    monkeypatch.setattr(client, "_expected_process_is_running", lambda identity: False)
    monkeypatch.setattr(client._SHARED_STATE_ADAPTER, "lease_is_stale", lambda *args, **kwargs: False)
    monkeypatch.setattr(client, "_finish_verified_daemon_exit", lambda paths, owner, **kwargs: drained.append(owner))
    monkeypatch.setattr(client._SHARED_STATE_ADAPTER, "recover_stale_workspace", lambda *args, **kwargs: False)
    assert client._should_force_recover_local_lease(paths)
    assert client._recover_broken_local_lease(paths)
    assert drained == [record]


def test_remote_or_unverifiable_owner_is_not_confirmed_dead(tmp_path, monkeypatch):
    from data_engine.hosts.daemon import client
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    monkeypatch.setattr(client, "_same_machine_lease_process", lambda paths: None)
    assert not client.local_daemon_has_exited(paths)

    def unknown(paths):
        raise client.DaemonClientError("identity cannot be verified")

    monkeypatch.setattr(client, "_same_machine_lease_process", unknown)
    assert not client.local_daemon_has_exited(paths)


@pytest.mark.parametrize("shutdown_when_idle", [False, True])
def test_stop_during_reserved_startup_cancels_attempt_and_allows_next_start(service, shutdown_when_idle):
    loading, release_load, running = Event(), Event(), Event()
    result = {}

    def load(*args, **kwargs):
        loading.set()
        assert release_load.wait(3)
        return (Flow(name="poll", group="Poll"),)

    def run(flows, *, runtime_stop_event, **kwargs):
        running.set()
        assert runtime_stop_event.wait(3)

    service.flow_execution_service.load_flows = load
    service.runtime_execution_service.run_automated = run
    starter = Thread(target=lambda: result.update(service.command_handler.runtime_commands.start_engine()))
    starter.start()
    try:
        assert loading.wait(3)
        assert service.command_handler.runtime_commands.stop_engine(shutdown_when_idle=shutdown_when_idle) == {"ok": True}
        assert service.state.engine_runtime_stop_event.is_set()
        release_load.set()
        starter.join(3)
        assert not starter.is_alive()
        assert result == {"ok": False, "error": "Engine startup was cancelled."}
        assert not running.is_set()
        assert not service.state.engine_starting
        assert not service.state.runtime_active
        assert not service.state.runtime_stopping
        assert service.command_handler.runtime_commands.start_engine() == {"ok": True}
        engine = service.state.engine_thread
        assert running.wait(3)
        assert not service.state.engine_runtime_stop_event.is_set()
        assert not service.state.shutdown_when_idle
        assert service.command_handler.runtime_commands.stop_engine() == {"ok": True}
        engine.join(3)
        assert not engine.is_alive()
    finally:
        release_load.set()
        service.state.engine_runtime_stop_event.set()
        starter.join(3)
        if service.state.engine_thread is not None:
            service.state.engine_thread.join(3)


def test_concurrent_full_state_publication_cannot_roll_back_engine_start(service):
    captured, release, starting, started = Event(), Event(), Event(), Event()
    original = service._runtime_state_payload
    failures = []

    def capture():
        payload = original()
        if not payload["runtime_active"]:
            captured.set()
            assert release.wait(3)
        return payload

    service._runtime_state_payload = capture

    def checkpoint():
        try:
            service._publish_runtime_event("workspace.checkpointed")
        except BaseException as exc:
            failures.append(exc)

    def start():
        try:
            starting.set()
            with service._state_lock:
                service.state.begin_runtime(active_flow_names=("poll",))
                service._publish_runtime_event("engine.started")
            started.set()
        except BaseException as exc:
            failures.append(exc)

    older = Thread(target=checkpoint)
    newer = Thread(target=start)
    older.start()
    try:
        assert captured.wait(3)
        newer.start()
        assert starting.wait(3)
        # A start must not overtake the earlier full-state publication.
        overtook = started.wait(0.1)
        release.set()
        older.join(3)
        newer.join(3)
        assert not older.is_alive() and not newer.is_alive()
        assert not failures
        assert service.runtime_projector.snapshot().runtime_active is True
        assert not overtook
    finally:
        release.set()
        older.join(3)
        if newer.ident is not None:
            newer.join(3)


def test_checkpoint_cannot_resurrect_run_finished_during_state_capture(service):
    writer = DaemonRuntimeCacheProxy(service.runtime_cache_ledger, publish_event=service._publish_runtime_event).execution_state
    writer.record_run_started(run_id="owned", flow_name="poll", group_name="Poll", source_path=None)
    captured, release, finishing, finished = Event(), Event(), Event(), Event()
    original = service._runtime_state_payload
    failures = []

    def capture():
        payload = original()
        captured.set()
        assert release.wait(3)
        return payload

    service._runtime_state_payload = capture

    def checkpoint():
        try:
            service._publish_runtime_event("checkpoint.recorded")
        except BaseException as exc:
            failures.append(exc)

    def finish():
        try:
            finishing.set()
            writer.record_run_finished(run_id="owned", status="success", finished_at_utc=utcnow_text())
            finished.set()
        except BaseException as exc:
            failures.append(exc)

    older, newer = Thread(target=checkpoint), Thread(target=finish)
    older.start()
    try:
        assert captured.wait(3)
        newer.start()
        assert finishing.wait(3)
        overtook = finished.wait(0.1)
        release.set()
        older.join(3)
        newer.join(3)
        assert not older.is_alive() and not newer.is_alive()
        assert not failures
        assert service.runtime_projector.snapshot().active_runs == ()
        assert not overtook
    finally:
        release.set()
        older.join(3)
        if newer.ident is not None:
            newer.join(3)


def test_engine_reconciliation_preserves_live_manual_run_and_successful_work(service):
    engine_running, manual_running, release_manual, stopped = Event(), Event(), Event(), Event()
    service.runtime_event_bus.subscribe(lambda event: stopped.set() if event.event_type == "engine.stopped" else None)

    def automated(flows, *, runtime_ledger, runtime_stop_event, **kwargs):
        writer = runtime_ledger.execution_state
        writer.record_run_started(run_id="engine-orphan", flow_name="poll", group_name="Poll", source_path=None)
        writer.record_step_started(run_id="engine-orphan", flow_name="poll", step_label="Interrupted")
        writer.record_run_started(run_id="engine-completed", flow_name="poll", group_name="Poll", source_path=None)
        writer.record_run_finished(run_id="engine-completed", status="completed", finished_at_utc=utcnow_text())
        engine_running.set()
        assert runtime_stop_event.wait(3)

    def manual_step(context):
        manual_running.set()
        assert release_manual.wait(3)
        return 42

    service.runtime_execution_service.run_automated = automated
    service.flow_execution_service.load_flow = lambda *args, **kwargs: Flow(name="manual", group="Manual").step(manual_step)
    service.runtime_cache_ledger.execution_state.record_run_started(
        run_id="previous-engine", flow_name="poll", group_name="Poll", source_path=None,
    )
    assert service._handle_command({"command": "start_engine"}) == {"ok": True}
    engine = service.state.engine_thread
    manual = None
    try:
        assert engine_running.wait(3)
        assert service._handle_command({"command": "run_flow", "name": "manual"}) == {"ok": True}
        manual = service.state.manual_run_threads["manual"]
        assert manual_running.wait(3)
        manual_run = service.runtime_cache_ledger.runs.list(flow_name="manual")[0]
        assert service._handle_command({"command": "stop_engine"}) == {"ok": True}
        assert stopped.wait(3)
        assert manual.is_alive()
        assert service.runtime_cache_ledger.runs.get(manual_run.run_id).status == "started"
        assert service.runtime_cache_ledger.step_outputs.list_for_run(manual_run.run_id)[0].status == "started"
        assert service.runtime_cache_ledger.runs.get("engine-orphan").status == "stopped"
        assert service.runtime_cache_ledger.step_outputs.list_for_run("engine-orphan")[0].status == "stopped"
        assert service.runtime_cache_ledger.runs.get("engine-completed").status == "completed"
        assert service.runtime_cache_ledger.runs.get("previous-engine").status == "started"
        release_manual.set()
        manual.join(3)
        assert not manual.is_alive()
        assert service.runtime_cache_ledger.runs.get(manual_run.run_id).status == "success"
        assert service.runtime_cache_ledger.step_outputs.list_for_run(manual_run.run_id)[0].status == "success"
    finally:
        release_manual.set()
        service.state.engine_runtime_stop_event.set()
        engine.join(3)
        if manual is not None:
            manual.join(3)


def test_engine_scope_tracks_steps_independently_and_releases_completed_ids(service):
    proxy = DaemonRuntimeCacheProxy(service.runtime_cache_ledger, publish_event=service._publish_runtime_event)
    writer = proxy.execution_state
    writer.record_run_started(run_id="owned", flow_name="poll", group_name="Poll", source_path=None)
    step_id = writer.record_step_started(run_id="owned", flow_name="poll", step_label="Interrupted")
    writer.record_run_finished(run_id="owned", status="completed", finished_at_utc=utcnow_text())
    assert not writer._active_run_ids
    assert proxy.reconcile_owned_activity(status="stopped", finished_at_utc=utcnow_text()) == (0, 1)
    assert service.runtime_cache_ledger.runs.get("owned").status == "completed"
    assert service.runtime_cache_ledger.step_outputs.get(step_id).status == "stopped"
    assert not writer._active_step_ids
    assert proxy.reconcile_owned_activity(status="stopped", finished_at_utc=utcnow_text()) == (0, 0)


def test_engine_finalization_clears_runtime_state_when_reconciliation_fails(service, monkeypatch):
    running, stopped = Event(), Event()
    logs = []
    service._debug_log = logs.append
    service.runtime_event_bus.subscribe(lambda event: stopped.set() if event.event_type == "engine.stopped" else None)

    def automated(flows, *, runtime_stop_event, **kwargs):
        running.set()
        assert runtime_stop_event.wait(3)

    def fail(*args, **kwargs):
        raise OSError("cache unavailable")

    service.runtime_execution_service.run_automated = automated
    monkeypatch.setattr(DaemonRuntimeCacheProxy, "reconcile_owned_activity", fail)
    assert service._handle_command({"command": "start_engine"}) == {"ok": True}
    engine = service.state.engine_thread
    try:
        assert running.wait(3)
        assert service._handle_command({"command": "stop_engine"}) == {"ok": True}
        assert stopped.wait(3)
        engine.join(3)
        assert not engine.is_alive()
        assert not service.state.runtime_active
        assert not service.state.runtime_stopping
        assert service.state.engine_thread is None
        assert service.state.finishing_engine_thread is None
        assert "engine-owned activity reconciliation failed" in logs
    finally:
        service.state.engine_runtime_stop_event.set()
        engine.join(3)


def _status_handler(daemon_id, *, owned, active):
    projector = DaemonRuntimeProjector(workspace_id="test", initial_state={})
    projector.handle(DaemonRuntimeEvent(
        "test", "engine.changed", utcnow_text(), None,
        {"state": {"status": "running" if active else "leased", "workspace_owned": owned, "runtime_active": active}},
    ))
    return DaemonStateSyncHandler(SimpleNamespace(
        runtime_projector=projector,
        paths=SimpleNamespace(workspace_id="test", workspace_root="workspace"),
        daemon_id=daemon_id, machine_id="test-machine", host_name="test-host", daemon_process_metadata={},
    ))


@pytest.mark.parametrize("command", ["status", "wait"])
@pytest.mark.parametrize("since_daemon_id", [None, "previous"])
def test_status_cursor_returns_full_new_generation_without_waiting(command, since_daemon_id, monkeypatch):
    old = _status_handler("previous", owned=True, active=True)
    new = _status_handler("next", owned=False, active=False)
    previous = old.status_payload()
    current = new.status_payload()
    assert previous["projection_version"] == current["projection_version"]
    assert previous["event_sequence"] == current["event_sequence"]
    monkeypatch.setattr(new.service.runtime_projector, "wait_for_change", lambda **kwargs: pytest.fail("must not wait on another generation"))
    kwargs = dict(since_version=previous["projection_version"], since_event_sequence=previous["event_sequence"], since_daemon_id=since_daemon_id)
    payload = new.status_payload(**kwargs) if command == "status" else new.wait_for_status_payload(**kwargs, timeout_seconds=30)
    assert not payload.get("unchanged")
    assert payload["daemon_id"] == "next"
    assert not payload["workspace_owned"]
    assert not payload["engine_active"]


def test_manager_refetches_full_state_for_mismatched_unchanged_generation(monkeypatch):
    manager = WorkspaceDaemonManager.__new__(WorkspaceDaemonManager)
    manager._last_snapshot = None
    manager._snapshot_lock = RLock()
    manager._sync_misses = 0
    manager.paths = object()
    old = _status_handler("previous", owned=True, active=True).status_payload()
    new = _status_handler("next", owned=False, active=False).status_payload()
    manager._snapshot_from_status_dict(old, assume_live=True, transport_mode="heartbeat")
    requests = []

    def request(paths, payload, timeout):
        requests.append(payload)
        return {"ok": True, "status": new}

    monkeypatch.setattr("data_engine.hosts.daemon.manager.daemon_request", request)
    snapshot = manager._snapshot_from_status_dict(
        {"daemon_id": "next", "unchanged": True}, assume_live=True, transport_mode="subscription",
    )
    assert requests == [{"command": "daemon_status"}]
    assert snapshot.daemon_id == "next"
    assert not snapshot.workspace_owned
    assert not snapshot.runtime_active


@pytest.mark.parametrize("new_generation", [False, True])
def test_delayed_long_poll_cannot_replace_newer_sync_state(tmp_path, monkeypatch, new_generation):
    manager = WorkspaceDaemonManager(resolve_workspace_paths(workspace_root=tmp_path / "workspace"))
    old = _status_handler("previous", owned=True, active=False).status_payload()
    new = dict(_status_handler("next" if new_generation else "previous", owned=True, active=True).status_payload())
    if not new_generation:
        new["event_sequence"] = old["event_sequence"] + 1
        new["projection_version"] = old["projection_version"] + 1
    manager._snapshot_from_status_dict(old, assume_live=True, transport_mode="heartbeat")
    waiting, release = Event(), Event()
    returned, failures = [], []

    def request(paths, payload, timeout):
        if payload["command"] == "wait_for_daemon_status":
            waiting.set()
            assert release.wait(3)
            return {"ok": True, "status": old}
        return {"ok": True, "status": new}

    monkeypatch.setattr("data_engine.hosts.daemon.manager.is_daemon_live", lambda paths: True)
    monkeypatch.setattr("data_engine.hosts.daemon.manager.daemon_request", request)

    def wait():
        try:
            returned.append(manager.wait_for_update(timeout_seconds=0.1))
        except BaseException as exc:
            failures.append(exc)

    thread = Thread(target=wait)
    thread.start()
    try:
        assert waiting.wait(3)
        current = manager.sync()
        release.set()
        thread.join(3)
        assert not thread.is_alive()
        assert not failures
        assert returned == [current]
        assert manager._last_snapshot.runtime_active
    finally:
        release.set()
        thread.join(3)
