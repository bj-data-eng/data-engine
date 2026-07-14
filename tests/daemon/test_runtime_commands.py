from __future__ import annotations

from datetime import UTC, datetime, timedelta
import threading
import time

import pytest

import data_engine.runtime.shared_state as shared_state_module
from data_engine.authoring.flow import Flow
from data_engine.core.primitives import DateRangeInputValue, ManualInputSpec
from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.app import (
    DataEngineDaemonService,
)
from data_engine.hosts.daemon.lifecycle import (
    relinquish_workspace_for_control_request,
    shutdown_for_requested_idle_disconnect,
)
from data_engine.hosts.daemon.ownership import (
    ensure_workspace_lease_current,
    honor_control_request_if_needed,
    release_workspace_claim,
    try_claim_released_workspace,
    try_claim_requested_control,
)
from data_engine.hosts.daemon.runtime_control import stop_active_work
from data_engine.platform.machine_identity import host_name_text, machine_id_text
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.runtime.runtime_db import RuntimeCacheLedger, parse_utc_text, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state as _checkpoint_workspace_state,
    claim_workspace as _claim_workspace,
    hydrate_local_runtime_state,
    initialize_workspace_state,
    read_control_request,
    read_lease_metadata,
    recover_stale_workspace,
    release_workspace as _release_workspace,
    remove_lease_metadata as _remove_lease_metadata,
    resolve_workspace_bundle,
    write_control_request,
    write_lease_metadata,
)
from data_engine.views.models import QtFlowCard

from .support import _write_blocking_group_flows, _write_demo_flow, resolve_workspace_paths


def claim_workspace(paths) -> bool:
    return _claim_workspace(paths) is not None


def checkpoint_workspace_state(paths, ledger, **kwargs):
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token is not None
    return _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=bundle.lease_token,
        **kwargs,
    )


def release_workspace(paths) -> None:
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token is not None
    _release_workspace(paths, lease_token=bundle.lease_token)


def remove_lease_metadata(paths) -> None:
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token is not None
    _remove_lease_metadata(paths, lease_token=bundle.lease_token)


def _wait_until(predicate, *, timeout: float = 1.5) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


@pytest.mark.parametrize(
    "command",
    (
        {"command": "run_flow", "name": "demo", "wait": False},
        {"command": "start_engine"},
    ),
)
def test_runtime_command_does_not_reclaim_when_drain_begins_at_claim_boundary(
    tmp_path,
    monkeypatch,
    command,
):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        release_workspace_claim(service)
        filesystem_claim_calls: list[object] = []
        monkeypatch.setattr(
            service.shared_state_adapter,
            "claim_workspace",
            lambda candidate_paths: filesystem_claim_calls.append(candidate_paths) or True,
        )

        def _begin_drain_before_claim(candidate_service):
            with candidate_service._state_lock:
                candidate_service.state.begin_work_drain()
            return try_claim_released_workspace(candidate_service)

        monkeypatch.setattr(
            "data_engine.hosts.daemon.runtime_commands.try_claim_released_workspace",
            _begin_drain_before_claim,
        )

        response = service._handle_command(command)  # noqa: SLF001

        assert response == {"ok": False, "error": "Runtime work is stopping."}
        assert filesystem_claim_calls == []
        assert service.host.workspace_owned is False
        assert read_lease_metadata(paths) is None
    finally:
        service._shutdown()  # noqa: SLF001


def test_released_workspace_claim_holds_admission_lock_through_filesystem_commit(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    filesystem_claim_entered = threading.Event()
    allow_filesystem_claim = threading.Event()
    drain_attempted = threading.Event()
    drain_finished = threading.Event()
    claim_results: list[bool] = []
    original_claim = service.shared_state_adapter.claim_workspace

    def _blocking_claim(candidate_paths):
        filesystem_claim_entered.set()
        allow_filesystem_claim.wait()
        return original_claim(candidate_paths)

    def _claim() -> None:
        claim_results.append(try_claim_released_workspace(service))

    def _drain() -> None:
        drain_attempted.set()
        stop_active_work(service, timeout_seconds=0.0)
        drain_finished.set()

    release_workspace_claim(service)
    monkeypatch.setattr(service.shared_state_adapter, "claim_workspace", _blocking_claim)
    claim_thread = threading.Thread(target=_claim, daemon=True)
    drain_thread = threading.Thread(target=_drain, daemon=True)
    claim_thread.start()
    try:
        assert filesystem_claim_entered.wait(timeout=1.0) is True
        state_lock_was_available = service._state_lock.acquire(blocking=False)  # noqa: SLF001
        if state_lock_was_available:
            service._state_lock.release()  # noqa: SLF001
        assert state_lock_was_available is False

        drain_thread.start()
        assert drain_attempted.wait(timeout=1.0) is True
        assert drain_finished.is_set() is False

        allow_filesystem_claim.set()
        claim_thread.join(timeout=1.0)
        drain_thread.join(timeout=1.0)

        assert claim_thread.is_alive() is False
        assert drain_thread.is_alive() is False
        assert claim_results == [True]
        assert service.state.work_draining is True
    finally:
        allow_filesystem_claim.set()
        claim_thread.join(timeout=1.0)
        if drain_thread.ident is not None:
            drain_thread.join(timeout=1.0)
        service._shutdown()  # noqa: SLF001


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


def test_manual_run_closes_worker_sqlite_connection(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    worker_thread_ids: list[int] = []

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_stop_event, flow_stop_event, workspace_id
        worker_thread_ids.append(threading.get_ident())
        runtime_ledger.runs.list()

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        baseline_connection_ids = set(service.runtime_cache_ledger._connections)  # noqa: SLF001 - test inspects connection retention

        for _ in range(2):
            response = service._handle_command({"command": "run_flow", "name": "demo", "wait": True})  # noqa: SLF001
            assert response["ok"] is True
            worker_thread_id = worker_thread_ids[-1]
            assert worker_thread_id not in service.runtime_cache_ledger._connections  # noqa: SLF001 - proves worker connection was closed

        assert set(service.runtime_cache_ledger._connections) == baseline_connection_ids  # noqa: SLF001 - no worker-thread connections accumulate
    finally:
        service._shutdown()  # noqa: SLF001


def test_run_flow_forwards_manual_inputs_to_runtime(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    captured_inputs: list[object] = []

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None, inputs=None):
        del flow, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id
        captured_inputs.append(inputs)

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        service._cached_flow_cards = (  # noqa: SLF001
            QtFlowCard(
                name="demo",
                group="Demo",
                title="Demo",
                description="",
                source_root="(not set)",
                target_root="(not set)",
                mode="manual",
                interval="-",
                operations="Run",
                operation_items=("Run",),
                state="manual",
                valid=True,
                category="manual",
                manual_inputs=(ManualInputSpec(name="period", label="Reporting Period", kind="date_range"),),
            ),
        )
        response = service._handle_command(  # noqa: SLF001
            {
                "command": "run_flow",
                "name": "demo",
                "wait": True,
                "inputs": {"period": {"start": "2026-01-01", "end": "2026-01-31"}},
            }
        )

        assert response["ok"] is True
        assert captured_inputs == [{"period": DateRangeInputValue(start="2026-01-01", end="2026-01-31", inclusive=True)}]
    finally:
        service._shutdown()  # noqa: SLF001


def test_run_flow_rejects_missing_required_manual_inputs_before_starting(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    started = threading.Event()

    def _run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None, inputs=None):
        del flow, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id, inputs
        started.set()

    monkeypatch.setattr(service.runtime_execution_service, "run_manual", _run_manual)

    service.initialize()
    try:
        service._cached_flow_cards = (  # noqa: SLF001
            QtFlowCard(
                name="demo",
                group="Demo",
                title="Demo",
                description="",
                source_root="(not set)",
                target_root="(not set)",
                mode="manual",
                interval="-",
                operations="Run",
                operation_items=("Run",),
                state="manual",
                valid=True,
                category="manual",
                manual_inputs=(ManualInputSpec(name="period", label="Reporting Period", kind="date_range"),),
            ),
        )

        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": True})  # noqa: SLF001

        assert response == {"ok": False, "error": "Manual input 'period' is required."}
        assert started.is_set() is False
    finally:
        service._shutdown()  # noqa: SLF001


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

        relinquish_workspace_for_control_request(service, "machine-b", "host-b")
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
        host_name="test-host",
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
            requester_machine_id=machine_id_text(app_root=paths.app_root),
            requester_host_name=host_name_text(),
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


def test_start_engine_clears_reservation_after_catalog_load_failure(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        catalog_calls: list[bool] = []

        def _fake_load_flow_cards(*, force: bool = False):
            catalog_calls.append(force)
            if len(catalog_calls) == 1:
                raise RuntimeError("catalog boom")
            return (
                QtFlowCard(
                    name="demo_poll",
                    group="Demo",
                    title="Demo Poll",
                    description="Recovered automated flow.",
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
            )

        monkeypatch.setattr(service, "_load_flow_cards", _fake_load_flow_cards)
        monkeypatch.setattr(
            service.flow_execution_service,
            "load_flows",
            lambda flow_names, workspace_root=None: [
                Flow(name=flow_name, group="Demo").step(lambda context: 1, label="Emit Value")
                for flow_name in flow_names
            ],
        )
        monkeypatch.setattr(
            service.runtime_execution_service,
            "run_automated",
            lambda flows, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None: [],
        )

        with pytest.raises(RuntimeError, match="catalog boom"):
            service._handle_command({"command": "start_engine"})  # noqa: SLF001
        assert service.state.engine_starting is False

        second_response = service._handle_command({"command": "start_engine"})  # noqa: SLF001

        assert second_response["ok"] is True
        assert catalog_calls == [True, True]
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
        engine_thread = service.state.engine_thread
        assert engine_thread is not None
        assert service._handle_command({"command": "stop_engine", "shutdown_when_idle": True})["ok"] is True  # noqa: SLF001

        release_engine.set()
        engine_thread.join(timeout=1.0)
        assert engine_thread.is_alive() is False
        assert shutdown_for_requested_idle_disconnect(service, reason="test runtime drained") is True

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


def test_engine_stop_reconciles_orphaned_active_runtime_rows(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
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
                    parallelism="2",
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

        def _orphaning_run(flows, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
            del flows, runtime_stop_event, flow_stop_event, workspace_id
            started_at = utcnow_text()
            runtime_ledger.execution_state.record_run_started(
                run_id="run-1",
                flow_name="demo_poll",
                group_name="Demo",
                source_path="docs.xlsx",
                started_at_utc=started_at,
            )
            runtime_ledger.execution_state.record_step_started(
                run_id="run-1",
                flow_name="demo_poll",
                step_label="Emit Value",
                started_at_utc=started_at,
            )
            return []

        monkeypatch.setattr(service.runtime_execution_service, "run_automated", _orphaning_run)

        response = service._handle_command({"command": "start_engine"})  # noqa: SLF001
        assert response["ok"] is True
        _wait_until(lambda: "engine.stopped" in observed_events)

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["status"]["engine_active"] is False
        assert status["status"]["active_runs"] == []
        assert status["status"]["flow_activity"] == []
        assert service.runtime_cache_ledger.runs.list_active() == ()
        assert service.runtime_cache_ledger.step_outputs.list_active() == ()
    finally:
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


def test_runtime_command_drains_immediately_when_retained_token_is_replaced(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    service.initialize()
    lease_token_a = service.state.lease_token
    assert lease_token_a is not None
    old_time = "2000-01-01T00:00:00+00:00"
    write_lease_metadata(
        paths,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id=service.machine_id,
        host_name=service.host_name,
        daemon_id=service.daemon_id,
        pid=service.pid,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    assert recover_stale_workspace(
        paths,
        lease_token=lease_token_a,
        machine_id="machine-b",
        stale_after_seconds=1.0,
    ) is True
    lease_token_b = _claim_workspace(paths)
    assert isinstance(lease_token_b, str)
    release_calls: list[str] = []
    observed_events: list[str] = []
    service.runtime_event_bus.subscribe(lambda event: observed_events.append(event.event_type))
    monkeypatch.setattr(
        service.shared_state_adapter,
        "release_workspace",
        lambda _paths, *, lease_token: release_calls.append(lease_token),
    )
    try:
        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001

        assert response == {"ok": False, "error": "Workspace lease was lost; runtime work is stopping."}
        assert service.state.work_draining is True
        assert service.state.workspace_owned is False
        assert service.state.lease_token is None
        assert service.state.status == "lease lost"
        assert service.host.shutdown_event.is_set() is True
        assert "workspace.lease_lost" in observed_events
        assert "workspace.released" not in observed_events
        assert release_calls == []
        bundle = resolve_workspace_bundle(paths)
        assert bundle is not None and bundle.lease_token == lease_token_b
    finally:
        service._shutdown()  # noqa: SLF001


def test_runtime_command_drains_immediately_on_corrupt_marker_topology(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    service.initialize()
    lease_token_a = service.state.lease_token
    assert lease_token_a is not None
    lease_token_b = "b" * 32 if lease_token_a != "b" * 32 else "c" * 32
    conflicting_marker = paths.leased_markers_dir / f"{paths.workspace_id}__{lease_token_b}"
    conflicting_marker.mkdir()
    release_calls: list[str] = []
    observed_events: list[str] = []
    service.runtime_event_bus.subscribe(lambda event: observed_events.append(event.event_type))
    monkeypatch.setattr(
        service.shared_state_adapter,
        "release_workspace",
        lambda _paths, *, lease_token: release_calls.append(lease_token),
    )
    try:
        response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001

        assert response == {"ok": False, "error": "Workspace lease was lost; runtime work is stopping."}
        assert service.state.work_draining is True
        assert service.state.workspace_owned is False
        assert service.state.lease_token is None
        assert service.state.status == "lease lost"
        assert service.host.shutdown_event.is_set() is True
        assert "workspace.lease_lost" in observed_events
        assert "workspace.released" not in observed_events
        assert release_calls == []
        assert (paths.leased_markers_dir / f"{paths.workspace_id}__{lease_token_a}").is_dir()
        assert conflicting_marker.is_dir()
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_flow_reset_updates_owner_and_shared_runtime_state(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    service.initialize()
    hydrated_ledger = RuntimeCacheLedger(tmp_path / "hydrated.sqlite")
    try:
        started = utcnow_text()
        for flow_name in ("alpha", "beta"):
            service.runtime_cache_ledger.runs.record_started(
                run_id=f"run-{flow_name}",
                flow_name=flow_name,
                group_name="Demo",
                source_path=None,
                started_at_utc=started,
            )
            service.runtime_cache_ledger.logs.append(
                level="INFO",
                message=flow_name,
                created_at_utc=started,
                run_id=f"run-{flow_name}",
                flow_name=flow_name,
            )
        service._checkpoint_once(status="idle")  # noqa: SLF001

        response = service._handle_command({"command": "reset_flow", "name": "alpha"})  # noqa: SLF001

        assert response == {"ok": True}
        assert [run.flow_name for run in service.runtime_cache_ledger.runs.list()] == ["beta"]
        assert hydrate_local_runtime_state(paths, hydrated_ledger) is True
        assert [run.flow_name for run in hydrated_ledger.runs.list()] == ["beta"]
        assert [entry.flow_name for entry in hydrated_ledger.logs.list()] == ["beta"]
    finally:
        hydrated_ledger.close()
        service._shutdown()  # noqa: SLF001


def test_daemon_flow_reset_fences_recovery_until_fresh_checkpoint(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    service.initialize()
    lease_token = service.state.lease_token
    assert lease_token is not None
    old_time = "2000-01-01T00:00:00+00:00"
    write_lease_metadata(
        paths,
        lease_token=lease_token,
        workspace_id=paths.workspace_id,
        machine_id=service.machine_id,
        host_name=service.host_name,
        daemon_id=service.daemon_id,
        pid=service.pid,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    reset_entered = threading.Event()
    allow_reset = threading.Event()
    recovery_done = threading.Event()
    command_result: dict[str, object] = {}
    recovery_result: list[bool] = []
    original_reset_flow = service.runtime_cache_ledger.reset_flow

    def blocking_reset(flow_name):
        reset_entered.set()
        allow_reset.wait(timeout=2.0)
        original_reset_flow(flow_name)

    monkeypatch.setattr(service.runtime_cache_ledger, "reset_flow", blocking_reset)
    command_thread = threading.Thread(
        target=lambda: command_result.update(
            service._handle_command({"command": "reset_flow", "name": "demo"})  # noqa: SLF001
        ),
        daemon=True,
    )

    def recover() -> None:
        recovery_result.append(
            recover_stale_workspace(
                paths,
                lease_token=lease_token,
                machine_id="machine-b",
                stale_after_seconds=1.0,
            )
        )
        recovery_done.set()

    recovery_thread = threading.Thread(target=recover, daemon=True)
    command_thread.start()
    try:
        assert reset_entered.wait(timeout=1.0) is True
        recovery_thread.start()
        assert recovery_done.wait(timeout=0.1) is False
        bundle = resolve_workspace_bundle(paths)
        assert bundle is not None and bundle.lease_token == lease_token

        allow_reset.set()
        command_thread.join(timeout=2.0)
        recovery_thread.join(timeout=2.0)

        assert command_thread.is_alive() is False
        assert recovery_thread.is_alive() is False
        assert command_result == {"ok": True}
        assert recovery_result == [False]
        bundle = resolve_workspace_bundle(paths)
        assert bundle is not None and bundle.lease_token == lease_token
    finally:
        allow_reset.set()
        command_thread.join(timeout=2.0)
        if recovery_thread.ident is not None:
            recovery_thread.join(timeout=2.0)
        service._shutdown()  # noqa: SLF001


def test_flow_reset_serializes_behind_older_checkpoint_without_resurrection(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    service.initialize()
    hydrated_ledger = RuntimeCacheLedger(tmp_path / "hydrated.sqlite")
    export_paused = threading.Event()
    resume_export = threading.Event()
    checkpoint_errors: list[BaseException] = []
    reset_result: dict[str, object] = {}
    started = utcnow_text()
    service.runtime_cache_ledger.runs.record_started(
        run_id="run-old",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    service._checkpoint_once(status="idle")  # noqa: SLF001
    service.runtime_cache_ledger.logs.append(
        level="INFO",
        message="pre-reset change",
        created_at_utc=started,
        run_id="run-old",
        flow_name="demo",
    )
    original_export = service.runtime_cache_ledger.snapshots.export

    def pause_old_export():
        exported = original_export()
        if threading.current_thread().name == "periodic-checkpoint":
            export_paused.set()
            resume_export.wait(timeout=2.0)
        return exported

    monkeypatch.setattr(service.runtime_cache_ledger.snapshots, "export", pause_old_export)

    def checkpoint() -> None:
        try:
            service._checkpoint_once(status="idle")  # noqa: SLF001
        except BaseException as exc:  # pragma: no cover - asserted below
            checkpoint_errors.append(exc)

    checkpoint_thread = threading.Thread(target=checkpoint, name="periodic-checkpoint", daemon=True)
    reset_thread = threading.Thread(
        target=lambda: reset_result.update(
            service._handle_command({"command": "reset_flow", "name": "demo"})  # noqa: SLF001
        ),
        daemon=True,
    )
    checkpoint_thread.start()
    try:
        assert export_paused.wait(timeout=1.0) is True
        reset_thread.start()
        reset_thread.join(timeout=0.1)
        assert reset_thread.is_alive() is True
        assert [run.run_id for run in service.runtime_cache_ledger.runs.list(flow_name="demo")] == ["run-old"]

        resume_export.set()
        checkpoint_thread.join(timeout=2.0)
        reset_thread.join(timeout=2.0)

        assert checkpoint_thread.is_alive() is False
        assert reset_thread.is_alive() is False
        assert checkpoint_errors == []
        assert reset_result == {"ok": True}
        assert service.runtime_cache_ledger.runs.list(flow_name="demo") == ()
        assert hydrate_local_runtime_state(paths, hydrated_ledger) is True
        assert hydrated_ledger.runs.list(flow_name="demo") == ()
        assert hydrated_ledger.logs.list(flow_name="demo") == ()
    finally:
        resume_export.set()
        checkpoint_thread.join(timeout=2.0)
        if reset_thread.ident is not None:
            reset_thread.join(timeout=2.0)
        hydrated_ledger.close()
        service._shutdown()  # noqa: SLF001


def test_long_checkpoint_renews_lease_and_finishes_with_fresh_timestamp(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.setattr("data_engine.hosts.daemon.state_sync.CHECKPOINT_INTERVAL_SECONDS", 0.01)
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    service.initialize()
    lease_token = service.state.lease_token
    assert lease_token is not None
    service.runtime_cache_ledger.logs.append(
        level="INFO",
        message="force changed checkpoint",
        created_at_utc=utcnow_text(),
    )
    export_paused = threading.Event()
    resume_export = threading.Event()
    heartbeat_entered = threading.Event()
    release_heartbeat = threading.Event()
    heartbeat_written = threading.Event()
    checkpoint_errors: list[BaseException] = []
    original_export = service.runtime_cache_ledger.snapshots.export
    original_write_metadata = shared_state_module._write_lease_metadata_owned  # noqa: SLF001
    initial_clock = datetime.now(UTC)

    class ControlledDateTime:
        current = initial_clock

        @classmethod
        def now(cls, tz=None):
            if threading.current_thread().name == "data-engine-lease-heartbeat":
                heartbeat_entered.set()
                assert release_heartbeat.wait(timeout=2.0) is True
            return cls.current if tz is None else cls.current.astimezone(tz)

        @staticmethod
        def fromtimestamp(timestamp, tz=None):
            return datetime.fromtimestamp(timestamp, tz=tz)

    def pause_export():
        exported = original_export()
        export_paused.set()
        resume_export.wait(timeout=2.0)
        return exported

    def observe_heartbeat_write(*args, **kwargs):
        is_heartbeat = threading.current_thread().name == "data-engine-lease-heartbeat"
        result = original_write_metadata(*args, **kwargs)
        if is_heartbeat:
            heartbeat_written.set()
        return result

    monkeypatch.setattr(service.runtime_cache_ledger.snapshots, "export", pause_export)
    monkeypatch.setattr(shared_state_module, "datetime", ControlledDateTime)
    monkeypatch.setattr(shared_state_module, "_write_lease_metadata_owned", observe_heartbeat_write)

    def checkpoint() -> None:
        try:
            service._checkpoint_once(status="idle")  # noqa: SLF001
        except BaseException as exc:  # pragma: no cover - asserted below
            checkpoint_errors.append(exc)

    checkpoint_thread = threading.Thread(target=checkpoint, daemon=True)
    checkpoint_thread.start()
    try:
        assert export_paused.wait(timeout=1.0) is True
        assert heartbeat_entered.wait(timeout=1.0) is True
        ControlledDateTime.current = initial_clock + timedelta(seconds=30)
        release_heartbeat.set()
        assert heartbeat_written.wait(timeout=1.0) is True
        heartbeat_metadata = read_lease_metadata(paths)
        assert heartbeat_metadata is not None
        heartbeat_at = parse_utc_text(str(heartbeat_metadata["last_checkpoint_at_utc"]))
        assert heartbeat_at == ControlledDateTime.current
        assert recover_stale_workspace(
            paths,
            lease_token=lease_token,
            machine_id="machine-b",
            stale_after_seconds=10.0,
        ) is False
        assert checkpoint_thread.is_alive() is True

        resume_export.set()
        checkpoint_thread.join(timeout=2.0)

        assert checkpoint_thread.is_alive() is False
        assert checkpoint_errors == []
        completed_metadata = read_lease_metadata(paths)
        assert completed_metadata is not None
        completed_at = parse_utc_text(str(completed_metadata["last_checkpoint_at_utc"]))
        state_completed_at = parse_utc_text(service.state.last_checkpoint_at_utc)
        assert completed_at is not None and completed_at >= heartbeat_at
        assert state_completed_at is not None
    finally:
        release_heartbeat.set()
        resume_export.set()
        checkpoint_thread.join(timeout=2.0)
        service._shutdown()  # noqa: SLF001


def test_flow_reset_rechecks_admission_after_waiting_for_checkpoint_lock(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    service.initialize()
    started = utcnow_text()
    service.runtime_cache_ledger.runs.record_started(
        run_id="run-old",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    reset_validated = threading.Event()
    runtime_started = threading.Event()
    release_runtime = threading.Event()
    reset_result: dict[str, object] = {}
    original_ensure = ensure_workspace_lease_current

    def observe_initial_validation(candidate_service):
        if threading.current_thread().name == "reset-command":
            reset_validated.set()
        return original_ensure(candidate_service)

    def run_manual(flow, *, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id=None):
        del flow, runtime_ledger, runtime_stop_event, flow_stop_event, workspace_id
        runtime_started.set()
        release_runtime.wait(timeout=2.0)

    monkeypatch.setattr(
        "data_engine.hosts.daemon.runtime_commands.ensure_workspace_lease_current",
        observe_initial_validation,
    )
    monkeypatch.setattr(service.runtime_execution_service, "run_manual", run_manual)
    service._checkpoint_operation_lock.acquire()  # noqa: SLF001 - hold reset between its two admission checks
    reset_thread = threading.Thread(
        target=lambda: reset_result.update(
            service._handle_command({"command": "reset_flow", "name": "demo"})  # noqa: SLF001
        ),
        name="reset-command",
        daemon=True,
    )
    reset_thread.start()
    lock_held = True
    try:
        assert reset_validated.wait(timeout=1.0) is True
        run_response = service._handle_command({"command": "run_flow", "name": "demo", "wait": False})  # noqa: SLF001
        assert run_response == {"ok": True}
        assert runtime_started.wait(timeout=1.0) is True

        service._checkpoint_operation_lock.release()  # noqa: SLF001
        lock_held = False
        reset_thread.join(timeout=2.0)

        assert reset_thread.is_alive() is False
        assert reset_result == {"ok": False, "error": "Stop active runtime work before resetting a flow."}
        assert [run.run_id for run in service.runtime_cache_ledger.runs.list(flow_name="demo")] == ["run-old"]
    finally:
        if lock_held:
            service._checkpoint_operation_lock.release()  # noqa: SLF001
        release_runtime.set()
        reset_thread.join(timeout=2.0)
        service._shutdown()  # noqa: SLF001


def test_flow_reset_rejects_live_finishing_worker_without_deleting(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    service.initialize()
    service.runtime_cache_ledger.runs.record_started(
        run_id="run-old",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=utcnow_text(),
    )
    release_worker = threading.Event()
    worker = threading.Thread(target=release_worker.wait, daemon=True)
    worker.start()
    with service._state_lock:  # noqa: SLF001 - model the retained finalization window
        service.state.finishing_manual_run_threads["demo"] = worker
    try:
        response = service._handle_command({"command": "reset_flow", "name": "demo"})  # noqa: SLF001

        assert response == {"ok": False, "error": "Stop active runtime work before resetting a flow."}
        assert [run.run_id for run in service.runtime_cache_ledger.runs.list(flow_name="demo")] == ["run-old"]
    finally:
        release_worker.set()
        worker.join(timeout=1.0)
        service._shutdown()  # noqa: SLF001
