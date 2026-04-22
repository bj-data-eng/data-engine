from __future__ import annotations

import threading
import time


from data_engine.authoring.flow import Flow
from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.app import (
    DataEngineDaemonService,
)
from data_engine.hosts.daemon.lifecycle import relinquish_workspace_for_control_request
from data_engine.hosts.daemon.ownership import honor_control_request_if_needed, try_claim_requested_control
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR, machine_id_text
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state,
    claim_workspace,
    initialize_workspace_state,
    read_control_request,
    read_lease_metadata,
    release_workspace,
    remove_lease_metadata,
    write_control_request,
)
from data_engine.views.models import QtFlowCard

from .support import _write_blocking_group_flows, _write_demo_flow, resolve_workspace_paths


def _wait_until(predicate, *, timeout: float = 1.5) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()

def test_manual_run_does_not_break_daemon_shutdown_cleanup(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    run_response = service._handle_command({"command": "run_flow", "name": "demo", "wait": True})  # noqa: SLF001
    assert run_response["ok"] is True

    service._checkpoint_once(status="idle")  # noqa: SLF001 - proves daemon ledger remains usable after the manual run
    service._shutdown()  # noqa: SLF001

    assert read_lease_metadata(paths) is None
    assert (paths.available_markers_dir / paths.workspace_id).exists() is True
    assert (paths.leased_markers_dir / paths.workspace_id).exists() is False


def test_run_flow_rejects_second_manual_run_in_same_group(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_blocking_group_flows(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    release_gate = threading.Event()
    started = threading.Event()

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id
        started.set()
        release_gate.wait(timeout=1.0)

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        first = service._handle_command({"command": "run_flow", "name": "alpha", "wait": False})  # noqa: SLF001
        assert first["ok"] is True
        assert started.wait(timeout=1.0) is True

        second = service._handle_command({"command": "run_flow", "name": "beta", "wait": False})  # noqa: SLF001
        assert second["ok"] is False
        assert "Group Shared already has alpha running." == second["error"]
    finally:
        release_gate.set()
        service._shutdown()  # noqa: SLF001


def test_control_handoff_stops_in_flight_manual_run(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    stop_seen = threading.Event()
    release_gate = threading.Event()

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_ledger, workspace_id
        runtime_stop_event.wait(timeout=1.0)
        if runtime_stop_event.is_set():
            stop_seen.set()
        release_gate.wait(timeout=1.0)

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001
        assert response["ok"] is True

        relinquish_workspace_for_control_request(service, "machine-b")
        release_gate.set()

        assert stop_seen.wait(timeout=1.0) is True
        assert service.host.workspace_owned is False
        assert service.host.shutdown_event.is_set() is True
    finally:
        release_gate.set()
        service._shutdown()  # noqa: SLF001


def test_daemon_service_honors_control_request_from_another_machine(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        write_control_request(
            paths,
            workspace_id="default",
            requester_machine_id="machine-b",
            requester_host_name="machine-b",
            requester_pid=202,
            requester_client_kind="ui",
            requested_at_utc=utcnow_text(),
        )

        assert honor_control_request_if_needed(service) is True
        assert service.host.workspace_owned is False
        assert service.host.leased_by_machine_id == "machine-b"
        assert service.host.shutdown_event.is_set() is True
        assert read_lease_metadata(paths) is None
        assert (paths.available_markers_dir / paths.workspace_id).exists() is True
    finally:
        service._shutdown()  # noqa: SLF001


def test_observer_daemon_claims_workspace_after_local_control_request(tmp_path, monkeypatch):
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
        write_control_request(
            paths,
            workspace_id="default",
            requester_machine_id=machine_id_text(),
            requester_host_name=machine_id_text(),
            requester_pid=303,
            requester_client_kind="ui",
            requested_at_utc=utcnow_text(),
        )
        remove_lease_metadata(paths)
        release_workspace(paths)

        assert try_claim_requested_control(service) is True
        assert service.host.workspace_owned is True
        assert read_control_request(paths) is None
    finally:
        service._shutdown()  # noqa: SLF001


def test_start_engine_retries_after_empty_automated_flow_snapshot(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        calls: list[bool] = []

        def _fake_load_flow_cards(*, force: bool = False):
            calls.append(force)
            if len(calls) == 1:
                return ()
            return (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Recovered automated flow.",
                    source_root="/tmp/input",
                    target_root="/tmp/output",
                    mode="poll",
                    interval="5s",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="poll ready",
                    valid=True,
                    category="automated",
                ),
            )

        monkeypatch.setattr(service, "_load_flow_cards", _fake_load_flow_cards)
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: Flow(name=name, group="Demo").step(lambda context: 1, label="Emit Value"),
        )
        monkeypatch.setattr(
            service.runtime_execution_service,
            "run_automated",
            lambda flows, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None: [],
        )

        response = service._handle_command({"command": "start_engine"})  # noqa: SLF001

        assert response["ok"] is True
        assert calls == [True, True]
    finally:
        service._shutdown()  # noqa: SLF001


def test_run_flow_returns_build_failure_details_after_async_start(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: (_ for _ in ()).throw(RuntimeError("build boom")),
        )

        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001

        assert response["ok"] is True
        thread = service.state.manual_run_threads["demo"]
        thread.join(timeout=1.0)
        assert thread.is_alive() is False
        assert service.state.manual_run_threads == {}
        assert "build boom" in service.paths.daemon_log_path.read_text(encoding="utf-8")
    finally:
        service._shutdown()  # noqa: SLF001


def test_run_flow_uses_cached_flow_cards_before_forcing_refresh(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        calls: list[bool] = []

        def _fake_load_flow_cards(*, force: bool = False):
            calls.append(force)
            return (
                QtFlowCard(
                    name="demo",
                    group="Demo",
                    title="demo",
                    description="Simple daemon test flow.",
                    source_root="-",
                    target_root="-",
                    mode="manual",
                    interval="-",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="manual",
                    valid=True,
                    category="manual",
                ),
            )

        monkeypatch.setattr(service, "_load_flow_cards", _fake_load_flow_cards)
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: Flow(name=name, group="Demo").step(lambda context: 1, label="Emit Value"),
        )
        monkeypatch.setattr(
            service.runtime_execution_service,
            "run_manual",
            lambda flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None: [],
        )

        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001

        assert response["ok"] is True
        assert calls == [False]
    finally:
        service._shutdown()  # noqa: SLF001


def test_run_flow_rejects_duplicate_start_while_first_start_is_loading(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    load_started = threading.Event()
    release_load = threading.Event()
    execution_started = threading.Event()

    def _load_flow(name, workspace_root=None):
        del name, workspace_root
        load_started.set()
        release_load.wait(timeout=1.0)
        return Flow(name="demo", group="Demo").step(lambda context: 1, label="Emit Value")

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id
        execution_started.set()

    monkeypatch.setattr(service.flow_execution_service, "load_flow", _load_flow)
    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        first_result: dict[str, object] = {}
        first_thread = threading.Thread(
            target=lambda: first_result.update(service._handle_command({"command": "run_flow", "name": "demo", "wait": False})),  # noqa: SLF001
            daemon=True,
        )
        first_thread.start()
        assert load_started.wait(timeout=1.0) is True

        second = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001

        release_load.set()
        first_thread.join(timeout=1.0)

        assert first_result["ok"] is True
        assert second["ok"] is False
        assert second["error"] == "Flow demo is already running."
        assert execution_started.wait(timeout=1.0) is True
    finally:
        release_load.set()
        service._shutdown()  # noqa: SLF001


def test_run_flow_returns_before_slow_flow_load_finishes(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    load_started = threading.Event()
    release_load = threading.Event()

    def _load_flow(name, workspace_root=None):
        del name, workspace_root
        load_started.set()
        release_load.wait(timeout=1.0)
        return Flow(name="demo", group="Demo").step(lambda context: 1, label="Emit Value")

    monkeypatch.setattr(service.flow_execution_service, "load_flow", _load_flow)
    monkeypatch.setattr(
        service.runtime_execution_service,
        "run_manual",
        lambda flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None: [],
    )

    service.initialize()
    try:
        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001

        assert response["ok"] is True
        assert load_started.wait(timeout=1.0) is True
        assert "demo" in service.state.manual_run_threads
    finally:
        release_load.set()
        service._shutdown()  # noqa: SLF001


def test_start_engine_returns_build_failure_details(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        monkeypatch.setattr(
            service,
            "_load_flow_cards",
            lambda force=False: (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Broken automated flow.",
                    source_root="/tmp/input",
                    target_root="/tmp/output",
                    mode="poll",
                    interval="5s",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="poll ready",
                    valid=True,
                    category="automated",
                ),
            ),
        )
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: (_ for _ in ()).throw(RuntimeError(f"{name} build boom")),
        )

        response = service._handle_command({"command": "start_engine"})  # noqa: SLF001

        assert response["ok"] is False
        assert response["error"] == "demo_poll build boom"
    finally:
        service._shutdown()  # noqa: SLF001


def test_start_engine_coalesces_duplicate_start_while_first_start_is_loading(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        monkeypatch.setattr(
            service,
            "_load_flow_cards",
            lambda force=False: (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Automated flow.",
                    source_root="/tmp/input",
                    target_root="/tmp/output",
                    mode="poll",
                    interval="5s",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="poll ready",
                    valid=True,
                    category="automated",
                ),
            ),
        )
        load_started = threading.Event()
        release_load = threading.Event()
        load_calls: list[tuple[str, ...]] = []

        def _load_flows(flow_names, workspace_root=None):
            del workspace_root
            load_calls.append(tuple(flow_names))
            load_started.set()
            release_load.wait(timeout=1.0)
            return [Flow(name=flow_name, group="Demo").step(lambda context: 1, label="Emit Value") for flow_name in flow_names]

        monkeypatch.setattr(service.flow_execution_service, "load_flows", _load_flows)
        monkeypatch.setattr(
            service.runtime_execution_service,
            "run_automated",
            lambda flows, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None: [],
        )

        first_result: dict[str, object] = {}
        first_thread = threading.Thread(
            target=lambda: first_result.update(service._handle_command({"command": "start_engine"})),  # noqa: SLF001
            daemon=True,
        )
        first_thread.start()
        assert load_started.wait(timeout=1.0) is True

        second = service._handle_command({"command": "start_engine"})  # noqa: SLF001

        release_load.set()
        first_thread.join(timeout=1.0)

        assert first_result["ok"] is True
        assert second["ok"] is True
        assert load_calls == [("demo_poll",)]
    finally:
        release_load.set()
        service._shutdown()  # noqa: SLF001


def test_stop_engine_requests_graceful_runtime_stop_without_flow_interrupt(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        with service._state_lock:
            service.state.runtime_active = True
            service.state.runtime_stopping = False
            service.state.engine_runtime_stop_event.clear()
            service.state.engine_flow_stop_event.clear()

        response = service._handle_command({"command": "stop_engine"})  # noqa: SLF001

        assert response["ok"] is True
        assert service.state.engine_runtime_stop_event.is_set() is True
        assert service.state.engine_flow_stop_event.is_set() is False
        assert service.state.runtime_stopping is True
    finally:
        service._shutdown()  # noqa: SLF001

def test_stop_engine_can_request_shutdown_when_idle_for_last_client_disconnect(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths, lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL)
    service.initialize()
    try:
        release_engine = threading.Event()
        monkeypatch.setattr(service.runtime_control_ledger.client_sessions, "count_live", lambda workspace_id: 0)
        monkeypatch.setattr(
            service,
            "_load_flow_cards",
            lambda *, force=False: (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Automated demo flow.",
                    source_root="(not set)",
                    target_root="(not set)",
                    mode="poll",
                    interval="5s",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="poll ready",
                    valid=True,
                    category="automated",
                ),
            ),
        )
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flows",
            lambda flow_names, workspace_root=None: [
                Flow(name=flow_name, group="Demo").step(lambda context: 1, label="Emit Value") for flow_name in flow_names
            ],
        )

        def _blocking_run(flows, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
            del flows, runtime_ledger, flow_stop_event, workspace_id
            runtime_stop_event.wait(timeout=1.0)
            release_engine.wait(timeout=1.0)
            return []

        monkeypatch.setattr(service.runtime_execution_service, "run_automated", _blocking_run)

        assert service._handle_command({"command": "start_engine"})["ok"] is True  # noqa: SLF001
        assert service._handle_command({"command": "stop_engine", "shutdown_when_idle": True})["ok"] is True  # noqa: SLF001

        release_engine.set()

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if service.host.shutdown_event.is_set():
                break
            threading.Event().wait(0.01)

        assert service.host.shutdown_event.is_set() is True
        assert service.host.workspace_owned is False
        assert service.state.shutdown_when_idle is False
    finally:
        release_engine.set()
        service._shutdown()  # noqa: SLF001


def test_run_flow_refreshes_flow_cards_before_lookup(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        calls: list[bool] = []

        def _fake_load_flow_cards(*, force: bool = False):
            calls.append(force)
            return (
                QtFlowCard(
                    name="demo",
                    group="Demo",
                    title="Demo",
                    description="Freshly loaded flow.",
                    source_root="(not set)",
                    target_root="(not set)",
                    mode="manual",
                    interval="-",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="manual",
                    valid=True,
                    category="manual",
                ),
            )

        monkeypatch.setattr(service, "_load_flow_cards", _fake_load_flow_cards)
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flow",
            lambda name, workspace_root=None: Flow(name=name, group="Demo").step(lambda context: 1, label="Emit Value"),
        )

        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": True})  # noqa: SLF001

        assert response["ok"] is True
        assert calls == [False]
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_projection_tracks_manual_run_registration(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        release_run = threading.Event()
        observed_events: list[str] = []
        service.runtime_event_bus.subscribe(lambda event: observed_events.append(event.event_type))

        def _blocking_run(*args, **kwargs):
            del args, kwargs
            release_run.wait(timeout=1.0)
            return 1

        monkeypatch.setattr(service.runtime_execution_service, "run_once", _blocking_run)

        thread = threading.Thread(
            target=lambda: service._handle_command({"command": "run_flow", "name": "demo", "wait": False}),  # noqa: SLF001
            daemon=True,
        )
        thread.start()
        _wait_until(lambda: "manual.run_registered" in observed_events)

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["ok"] is True
        assert status["status"]["manual_runs"] == ["demo"]
        assert status["status"]["projection_version"] >= 1

        release_run.set()
        thread.join(timeout=1.0)
        _wait_until(lambda: "manual.run_unregistered" in observed_events)

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["status"]["manual_runs"] == []
    finally:
        release_run.set()
        service._shutdown()  # noqa: SLF001


def test_daemon_projection_tracks_engine_lifecycle(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        release_engine = threading.Event()
        observed_events: list[str] = []
        service.runtime_event_bus.subscribe(lambda event: observed_events.append(event.event_type))
        monkeypatch.setattr(
            service,
            "_load_flow_cards",
            lambda *, force=False: (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Automated demo flow.",
                    source_root="(not set)",
                    target_root="(not set)",
                    mode="poll",
                    interval="5s",
                    operations="Emit Value",
                    operation_items=("Emit Value",),
                    state="poll ready",
                    valid=True,
                    category="automated",
                ),
            ),
        )
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flows",
            lambda flow_names, workspace_root=None: [
                Flow(name=flow_name, group="Demo").step(lambda context: 1, label="Emit Value") for flow_name in flow_names
            ],
        )

        def _blocking_run(flows, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
            del flows, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id
            release_engine.wait(timeout=1.0)
            return []

        monkeypatch.setattr(service.runtime_execution_service, "run_automated", _blocking_run)

        response = service._handle_command({"command": "start_engine"})  # noqa: SLF001
        assert response["ok"] is True
        _wait_until(lambda: "engine.started" in observed_events)

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["status"]["engine_active"] is True
        assert status["status"]["active_engine_flow_names"] == ["demo_poll"]

        stop_response = service._handle_command({"command": "stop_engine"})  # noqa: SLF001
        assert stop_response["ok"] is True
        _wait_until(lambda: "engine.stop_requested" in observed_events)

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["status"]["engine_stopping"] is True

        release_engine.set()
        _wait_until(lambda: "engine.stopped" in observed_events)

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["status"]["engine_active"] is False
        assert status["status"]["engine_stopping"] is False
        assert status["status"]["active_engine_flow_names"] == []
        assert "engine.start_reserved" in observed_events
    finally:
        release_engine.set()
        service._shutdown()  # noqa: SLF001

