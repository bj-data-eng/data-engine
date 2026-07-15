from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
import os
import threading

import pytest

import data_engine.hosts.daemon.server as daemon_server
from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.app import (
    DAEMON_LOG_RETENTION_DAYS,
    DataEngineDaemonService,
    main as daemon_main,
)
from data_engine.hosts.daemon.bootstrap import initialize_service
from data_engine.hosts.daemon.composition import (
    DaemonHostDependencyFactories,
    DaemonHostDependencies,
    DaemonHostIdentity,
    DaemonHostState,
)
from data_engine.hosts.daemon.client import (
    _encode_message,
)
from data_engine.hosts.daemon.runtime_control import stop_active_work
from data_engine.hosts.daemon.server import serve_forever, serve_workspace_daemon
from data_engine.platform.machine_identity import host_name_text, machine_id_text
from data_engine.platform.processes import ProcessInspectionError
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.runtime.runtime_db import RuntimeCacheLedger, RuntimeControlLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state as _checkpoint_workspace_state,
    claim_workspace as _claim_workspace,
    initialize_workspace_state,
    read_lease_metadata,
    resolve_workspace_bundle,
    write_lease_metadata,
)

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


@pytest.fixture
def verified_server_containment(monkeypatch):
    """Keep listener-loop tests focused on server behavior after containment."""
    monkeypatch.setattr(
        daemon_server,
        "_require_current_process_containment",
        lambda containment_nonce, *, expected_process_identity=None: (
            expected_process_identity
        ),
    )


def test_daemon_service_initializes_and_serves_commands(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001 - direct daemon contract test
        assert status["ok"] is True
        assert status["status"]["workspace_id"] == "default"
        assert status["status"]["machine_id"] == machine_id_text(app_root=paths.app_root)
        assert status["status"]["host_name"] == host_name_text()
        assert status["status"]["pid"] == service.process_identity.pid
        assert status["status"]["process_start_key"] == service.process_identity.start_key
        assert (
            status["status"]["process_executable_path"]
            == service.process_identity.executable_path
        )
        assert status["status"]["process_group_id"] == service.process_identity.process_group_id
        assert (
            status["status"]["process_session_id"]
            == service.process_identity.process_session_id
        )
        assert status["status"]["containment_nonce"] == service.containment_nonce

        flows = service._handle_command({"command": "list_flows"})  # noqa: SLF001 - direct daemon contract test
        assert flows["ok"] is True
        assert [item["name"] for item in flows["flows"]] == ["demo"]
    finally:
        service._shutdown()  # noqa: SLF001 - direct daemon lifecycle test


def test_daemon_status_returns_unchanged_when_projection_version_matches(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        full_status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001 - direct daemon contract test
        assert full_status["ok"] is True
        version = int(full_status["status"]["projection_version"])
        event_sequence = int(full_status["status"]["event_sequence"])
        assert version >= 1

        unchanged = service._handle_command(  # noqa: SLF001 - direct daemon contract test
            {"command": "daemon_status", "since_version": version, "since_event_sequence": event_sequence}
        )

        assert unchanged["ok"] is True
        assert unchanged["status"] == {
            "workspace_id": "default",
            "daemon_id": service.daemon_id,
            "projection_version": version,
            "event_sequence": event_sequence,
            "unchanged": True,
        }
    finally:
        service._shutdown()  # noqa: SLF001 - direct daemon lifecycle test


def test_wait_for_daemon_status_returns_after_projection_change(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        initial = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        version = int(initial["status"]["projection_version"])
        event_sequence = int(initial["status"]["event_sequence"])

        def _refresh_projection() -> None:
            service.runtime_execution_ledger.execution_state.record_run_started(
                run_id="run-1",
                flow_name="demo",
                group_name="Demo",
                source_path="docs.xlsx",
                started_at_utc=utcnow_text(),
            )
            service._publish_runtime_event("runtime.execution.changed")  # noqa: SLF001

        thread = threading.Thread(target=_refresh_projection, daemon=True)
        thread.start()
        try:
            waited = service._handle_command(  # noqa: SLF001
                {
                    "command": "wait_for_daemon_status",
                    "since_version": version,
                    "since_event_sequence": event_sequence,
                    "timeout_ms": 500,
                }
            )
        finally:
            thread.join(timeout=1.0)

        assert waited["ok"] is True
        assert waited["status"]["event_sequence"] > event_sequence
        assert waited["status"]["active_runs"][0]["run_id"] == "run-1"
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_host_dependencies_build_default_opens_workspace_runtime_ledger(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "folder-name"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="explicit-id")

    dependencies = DaemonHostDependencies.build_default(paths)
    try:
        assert isinstance(dependencies.runtime_cache_ledger, RuntimeCacheLedger)
        assert dependencies.runtime_cache_ledger.db_path.name == "runtime_cache.sqlite"
        assert dependencies.runtime_control_ledger.db_path.name == "runtime_control.sqlite"
        assert dependencies.runtime_control_ledger.db_path == paths.runtime_control_db_path
        assert dependencies.runtime_cache_ledger.db_path.parent.parent.name == "runtime_state"
        assert dependencies.runtime_cache_ledger.db_path.exists() is True
        assert dependencies.runtime_control_ledger.db_path.exists() is True
        assert dependencies.flow_catalog_service.__class__.__name__ == "FlowCatalogService"
        assert dependencies.flow_execution_service.__class__.__name__ == "FlowExecutionService"
        assert dependencies.runtime_execution_service.__class__.__name__ == "RuntimeExecutionService"
        assert dependencies.shared_state_adapter.__class__.__name__ == "DaemonSharedStateAdapter"
    finally:
        dependencies.runtime_cache_ledger.close()
        dependencies.runtime_control_ledger.close()


def test_daemon_host_dependencies_build_default_uses_injected_ledger_service(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    calls: list[Path] = []
    ledger = RuntimeControlLedger(tmp_path / "custom" / "runtime_control.sqlite")

    class _LedgerService:
        def open_control_store(self, db_path: Path) -> RuntimeControlLedger:
            calls.append(db_path)
            return ledger

    dependencies = DaemonHostDependencies.build_default(paths, ledger_service=_LedgerService())
    try:
        assert dependencies.runtime_control_ledger is ledger
        assert calls == [paths.runtime_control_db_path]
    finally:
        ledger.close()


def test_daemon_host_dependencies_build_default_uses_injected_factories(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    calls: list[str] = []

    class _FlowCatalogService:
        def __init__(self):
            calls.append("catalog")

    class _FlowExecutionService:
        def __init__(self):
            calls.append("execution")

    class _RuntimeExecutionService:
        def __init__(self):
            calls.append("runtime")

    dependencies = DaemonHostDependencies.build_default(
        paths,
        factories=DaemonHostDependencyFactories(
            flow_catalog_service_factory=_FlowCatalogService,
            flow_execution_service_factory=_FlowExecutionService,
            runtime_execution_service_factory=_RuntimeExecutionService,
        ),
    )
    try:
        assert calls == ["catalog", "execution", "runtime"]
        assert dependencies.flow_catalog_service.__class__.__name__ == "_FlowCatalogService"
        assert dependencies.flow_execution_service.__class__.__name__ == "_FlowExecutionService"
        assert dependencies.runtime_execution_service.__class__.__name__ == "_RuntimeExecutionService"
    finally:
        dependencies.runtime_cache_ledger.close()
        dependencies.runtime_control_ledger.close()


def test_daemon_host_identity_current_process_uses_current_pid(tmp_path):
    app_root = tmp_path / "data_engine"
    identity = DaemonHostIdentity.current_process(app_root=app_root)

    assert identity.machine_id == machine_id_text(app_root=app_root)
    assert identity.host_name == host_name_text()
    assert identity.pid == os.getpid()
    assert len(identity.daemon_id) == 32


def test_daemon_host_state_transitions_cover_core_mutators():
    state = DaemonHostState.build(started_at_utc="2026-04-06T00:00:00+00:00")
    runtime_stop_event = threading.Event()
    flow_stop_event = threading.Event()
    engine_thread = threading.Thread(target=lambda: None)
    manual_thread = threading.Thread(target=lambda: None)
    manual_runtime_stop_event = threading.Event()
    manual_flow_stop_event = threading.Event()

    assert state.status == "starting"
    assert state.workspace_owned is False
    assert state.runtime_active is False
    assert state.runtime_stopping is False
    assert state.listener is None

    state.claim_workspace("a" * 32)
    assert state.workspace_owned is True
    assert state.leased_by_machine_id is None
    assert state.status == "idle"

    state.release_workspace(
        leased_by_machine_id="other-machine",
        leased_by_host_name="other-host",
        status="leased",
    )
    assert state.workspace_owned is False
    assert state.leased_by_machine_id == "other-machine"
    assert state.leased_by_host_name == "other-host"
    assert state.status == "leased"

    state.begin_runtime(status="running")
    assert state.runtime_active is True
    assert state.runtime_stopping is False
    assert state.status == "running"

    state.stop_runtime(status="stopping")
    assert state.runtime_stopping is True
    assert state.status == "stopping"

    state.end_runtime(status="idle")
    assert state.runtime_active is False
    assert state.runtime_stopping is False
    assert state.status == "idle"
    assert state.engine_thread is None
    assert state.engine_runtime_stop_event.is_set() is False
    assert state.engine_flow_stop_event.is_set() is False

    state.set_checkpoint_time("2026-04-06T00:01:00+00:00", status="degraded")
    assert state.last_checkpoint_at_utc == "2026-04-06T00:01:00+00:00"
    assert state.status == "degraded"

    state.set_lease_owner("machine-b", "host-b")
    assert state.leased_by_machine_id == "machine-b"
    assert state.leased_by_host_name == "host-b"
    assert state.increment_checkpoint_failures() == 1
    state.reset_checkpoint_failures()
    assert state.consecutive_checkpoint_failures == 0

    state.set_engine_threads(runtime_stop_event=runtime_stop_event, flow_stop_event=flow_stop_event, engine_thread=engine_thread)
    assert state.engine_runtime_stop_event is runtime_stop_event
    assert state.engine_flow_stop_event is flow_stop_event
    assert state.engine_thread is engine_thread

    state.end_runtime(status="idle")
    assert state.engine_thread is None
    assert state.engine_runtime_stop_event is not runtime_stop_event
    assert state.engine_flow_stop_event is not flow_stop_event
    assert state.engine_runtime_stop_event.is_set() is False
    assert state.engine_flow_stop_event.is_set() is False

    state.register_manual_run(
        "demo",
        thread=manual_thread,
        runtime_stop_event=manual_runtime_stop_event,
        flow_stop_event=manual_flow_stop_event,
    )
    assert state.manual_run_threads["demo"] is manual_thread
    assert state.manual_runtime_stop_events["demo"] is manual_runtime_stop_event
    assert state.manual_flow_stop_events["demo"] is manual_flow_stop_event
    state.unregister_manual_run("demo")
    assert state.manual_run_threads == {}
    assert state.manual_runtime_stop_events == {}
    assert state.manual_flow_stop_events == {}

    state.set_listener(object())
    assert state.listener is not None


def test_initialize_service_claims_workspace_and_records_idle_snapshot(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    initialize_service(service)
    try:
        metadata = read_lease_metadata(paths)
        assert metadata is not None
        assert metadata["machine_id"] == machine_id_text(app_root=paths.app_root)
        assert metadata["host_name"] == host_name_text()
        assert metadata["status"] == "idle"
        assert service.host.workspace_owned is True
        assert service.host.status == "idle"
    finally:
        service._shutdown()  # noqa: SLF001


def test_initialize_service_enters_observer_mode_for_other_machine_lease(tmp_path, monkeypatch):
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
    initialize_service(service)
    try:
        assert service.host.workspace_owned is False
        assert service.host.leased_by_machine_id == "machine-a"
        assert service.host.status == "leased"
        events, _ = service.runtime_projector.events_since(0)
        event_types = tuple(event["event_type"] for event in events)
        assert "workspace.lease_observed" in event_types
        assert "workspace.released" not in event_types
    finally:
        service._shutdown()  # noqa: SLF001


def test_stop_active_work_signals_running_threads_and_resets_runtime_state():
    class _Service:
        def __init__(self) -> None:
            self._state_lock = threading.RLock()
            self.state = DaemonHostState.build(started_at_utc="2026-04-06T00:00:00+00:00")
            engine_thread = threading.Thread(target=self._wait_for_engine_stop)
            manual_thread = threading.Thread(target=self._wait_for_manual_stop)
            self.engine_runtime_stop_event = threading.Event()
            self.engine_flow_stop_event = threading.Event()
            self.state.set_engine_threads(
                runtime_stop_event=self.engine_runtime_stop_event,
                flow_stop_event=self.engine_flow_stop_event,
                engine_thread=engine_thread,
            )
            self.state.begin_runtime()
            self.manual_runtime_stop_event = threading.Event()
            self.manual_flow_stop_event = threading.Event()
            self.state.register_manual_run(
                "manual",
                thread=manual_thread,
                runtime_stop_event=self.manual_runtime_stop_event,
                flow_stop_event=self.manual_flow_stop_event,
            )
            self.published_events: list[str] = []

        def _wait_for_engine_stop(self) -> None:
            self.state.engine_runtime_stop_event.wait(timeout=1.0)

        def _wait_for_manual_stop(self) -> None:
            self.state.manual_runtime_stop_events["manual"].wait(timeout=1.0)

        def _publish_runtime_event(self, event_type: str) -> None:
            self.published_events.append(event_type)

    service = _Service()
    service.state.engine_thread.start()
    service.state.manual_run_threads["manual"].start()

    result = stop_active_work(service)  # noqa: SLF001 - direct lifecycle helper test

    assert result.complete is True
    assert service.engine_runtime_stop_event.is_set() is True
    assert service.engine_flow_stop_event.is_set() is True
    assert service.manual_runtime_stop_event.is_set() is True
    assert service.manual_flow_stop_event.is_set() is True
    assert service.state.engine_thread is None
    assert service.state.manual_run_threads == {}
    assert service.state.runtime_active is False
    assert service.state.status == "idle"
    assert service.state.work_draining is True
    assert service.published_events == ["runtime.stopped"]


def test_stop_active_work_retains_noncooperative_engine_until_later_retry():
    release_worker = threading.Event()
    worker_started = threading.Event()

    class _Service:
        def __init__(self) -> None:
            self._state_lock = threading.RLock()
            self.state = DaemonHostState.build(started_at_utc="2026-04-06T00:00:00+00:00")
            self.published_events: list[str] = []

        def _publish_runtime_event(self, event_type: str) -> None:
            self.published_events.append(event_type)

    def _ignore_stop_request() -> None:
        worker_started.set()
        release_worker.wait()

    service = _Service()
    engine_thread = threading.Thread(target=_ignore_stop_request, daemon=True)
    service.state.set_engine_threads(
        runtime_stop_event=threading.Event(),
        flow_stop_event=threading.Event(),
        engine_thread=engine_thread,
    )
    service.state.begin_runtime()
    engine_thread.start()
    assert worker_started.wait(timeout=1.0) is True

    first_result = stop_active_work(service, timeout_seconds=0.01)

    assert first_result.complete is False
    assert first_result.remaining_workers == ("engine runtime",)
    assert service.state.engine_thread is engine_thread
    assert service.state.runtime_active is True
    assert service.state.runtime_stopping is True
    assert service.state.work_draining is True
    assert service.published_events == []

    release_worker.set()
    engine_thread.join(timeout=1.0)
    second_result = stop_active_work(service, timeout_seconds=0.0)

    assert second_result.complete is True
    assert service.state.engine_thread is None
    assert service.state.runtime_active is False
    assert service.published_events == ["runtime.stopped"]


def test_stop_active_work_retains_noncooperative_manual_worker_until_later_retry():
    release_worker = threading.Event()
    worker_started = threading.Event()

    class _Service:
        def __init__(self) -> None:
            self._state_lock = threading.RLock()
            self.state = DaemonHostState.build(started_at_utc="2026-04-06T00:00:00+00:00")
            self.published_events: list[str] = []

        def _publish_runtime_event(self, event_type: str) -> None:
            self.published_events.append(event_type)

    def _ignore_stop_request() -> None:
        worker_started.set()
        release_worker.wait()

    service = _Service()
    manual_thread = threading.Thread(target=_ignore_stop_request, daemon=True)
    service.state.register_manual_run(
        "manual",
        thread=manual_thread,
        runtime_stop_event=threading.Event(),
        flow_stop_event=threading.Event(),
    )
    manual_thread.start()
    assert worker_started.wait(timeout=1.0) is True

    first_result = stop_active_work(service, timeout_seconds=0.01)

    assert first_result.complete is False
    assert first_result.remaining_workers == ("manual runtime:manual",)
    assert service.state.manual_run_threads == {"manual": manual_thread}
    assert service.state.work_draining is True
    assert service.published_events == []

    release_worker.set()
    manual_thread.join(timeout=1.0)
    second_result = stop_active_work(service, timeout_seconds=0.0)

    assert second_result.complete is True
    assert service.state.manual_run_threads == {}
    assert service.published_events == ["runtime.stopped"]


def test_daemon_status_includes_active_runs_from_runtime_execution_bridge(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        started_at = utcnow_text()
        service.runtime_execution_ledger.execution_state.record_run_started(
            run_id="run-1",
            flow_name="demo",
            group_name="Demo",
            source_path="docs.xlsx",
            started_at_utc=started_at,
        )
        service.runtime_execution_ledger.execution_state.record_step_started(
            run_id="run-1",
            flow_name="demo",
            step_label="Emit Value",
            started_at_utc=started_at,
        )

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001

        assert status["ok"] is True
        assert status["status"]["active_runs"] == [
            {
                "run_id": "run-1",
                "flow_name": "demo",
                "group_name": "Demo",
                "source_path": "docs.xlsx",
                "state": "running",
                "current_step_name": "Emit Value",
                "current_step_started_at_utc": started_at,
                "started_at_utc": started_at,
                "finished_at_utc": None,
                "elapsed_seconds": None,
                "error_text": None,
            }
        ]
        assert status["status"]["flow_activity"] == [
            {
                "flow_name": "demo",
                "active_run_count": 1,
                "queued_run_count": 0,
                "engine_run_count": 0,
                "manual_run_count": 1,
                "stopping_run_count": 0,
                "running_step_counts": {"Emit Value": 1},
            }
        ]
    finally:
        service._shutdown()  # noqa: SLF001


def test_runtime_execution_events_update_projection_without_full_state_refresh(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        started_at = utcnow_text()
        original_runtime_state_payload = service._runtime_state_payload
        monkeypatch.setattr(
            service,
            "_runtime_state_payload",
            lambda: (_ for _ in ()).throw(AssertionError("hot runtime events should not rebuild full state")),
        )

        service.runtime_execution_ledger.execution_state.record_run_started(
            run_id="run-1",
            flow_name="demo",
            group_name="Demo",
            source_path="docs.xlsx",
            started_at_utc=started_at,
        )
        service.runtime_execution_ledger.execution_state.record_step_started(
            run_id="run-1",
            flow_name="demo",
            step_label="Emit Value",
            started_at_utc=started_at,
        )

        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001

        assert status["ok"] is True
        assert status["status"]["active_runs"] == [
            {
                "run_id": "run-1",
                "flow_name": "demo",
                "group_name": "Demo",
                "source_path": "docs.xlsx",
                "state": "running",
                "current_step_name": "Emit Value",
                "current_step_started_at_utc": started_at,
                "started_at_utc": started_at,
                "finished_at_utc": None,
                "elapsed_seconds": None,
                "error_text": None,
            }
        ]
    finally:
        monkeypatch.setattr(service, "_runtime_state_payload", original_runtime_state_payload)
        service._shutdown()  # noqa: SLF001


def test_initialize_service_reconciles_orphaned_active_runtime_rows(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started_at = utcnow_text()
    ledger.execution_state.record_run_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path="docs.xlsx",
        started_at_utc=started_at,
    )
    ledger.execution_state.record_step_started(
        run_id="run-1",
        flow_name="demo",
        step_label="Emit Value",
        started_at_utc=started_at,
    )
    ledger.close()

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001

        assert status["ok"] is True
        assert status["status"]["active_runs"] == []
        assert status["status"]["flow_activity"] == []
        assert service.runtime_cache_ledger.runs.list_active() == ()
        assert service.runtime_cache_ledger.step_outputs.list_active() == ()
    finally:
        service._shutdown()  # noqa: SLF001


def test_serve_workspace_daemon_passes_lifecycle_policy_to_service_type(
    tmp_path,
    verified_server_containment,
):
    del verified_server_containment
    workspace_root = tmp_path / "shared" / "default"

    calls: list[tuple[Path, DaemonLifecyclePolicy]] = []

    class _Service:
        def __init__(
            self,
            paths,
            *,
            containment_nonce: str,
            lifecycle_policy: DaemonLifecyclePolicy,
        ) -> None:
            assert containment_nonce == _TEST_CONTAINMENT_NONCE
            calls.append((paths.workspace_root, lifecycle_policy))

        def serve_forever(self) -> None:
            calls.append((workspace_root.resolve(), DaemonLifecyclePolicy.EPHEMERAL))

    result = serve_workspace_daemon(
        _Service,
        workspace_root=workspace_root,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL,
    )

    assert result == 0
    assert calls[0] == (workspace_root.resolve(), DaemonLifecyclePolicy.EPHEMERAL)


def test_serve_workspace_daemon_uses_injected_workspace_service(
    tmp_path,
    verified_server_containment,
):
    del verified_server_containment
    workspace_root = tmp_path / "shared" / "default"
    calls: list[Path | None] = []

    class _WorkspaceService:
        def resolve_paths(self, *, workspace_root=None, workspace_id=None):
            del workspace_id
            calls.append(workspace_root)
            return resolve_workspace_paths(workspace_root=workspace_root)

    class _Service:
        def __init__(
            self,
            paths,
            *,
            containment_nonce: str,
            lifecycle_policy: DaemonLifecyclePolicy,
        ) -> None:
            assert containment_nonce == _TEST_CONTAINMENT_NONCE
            assert lifecycle_policy is DaemonLifecyclePolicy.EPHEMERAL
            self.paths = paths

        def serve_forever(self) -> None:
            assert self.paths.workspace_root == workspace_root.resolve()

    result = serve_workspace_daemon(
        _Service,
        workspace_root=workspace_root,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL,
        workspace_service=_WorkspaceService(),
    )

    assert result == 0
    assert calls == [workspace_root]


def test_serve_workspace_daemon_uses_injected_resolve_paths_func(
    tmp_path,
    verified_server_containment,
):
    del verified_server_containment
    workspace_root = tmp_path / "shared" / "default"
    calls: list[tuple[Path | None, str | None]] = []
    resolved = resolve_workspace_paths(workspace_root=workspace_root)

    class _Service:
        def __init__(
            self,
            paths,
            *,
            containment_nonce: str,
            lifecycle_policy: DaemonLifecyclePolicy,
        ) -> None:
            assert containment_nonce == _TEST_CONTAINMENT_NONCE
            assert lifecycle_policy is DaemonLifecyclePolicy.EPHEMERAL
            self.paths = paths

        def serve_forever(self) -> None:
            assert self.paths is resolved

    result = serve_workspace_daemon(
        _Service,
        workspace_root=workspace_root,
        workspace_id="default",
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL,
        resolve_paths_func=lambda *, workspace_root=None, workspace_id=None: calls.append((workspace_root, workspace_id)) or resolved,
    )

    assert result == 0
    assert calls == [(workspace_root, "default")]


def test_serve_workspace_daemon_preserves_explicit_workspace_identity(
    tmp_path,
    monkeypatch,
    verified_server_containment,
):
    del verified_server_containment
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "folder-name"
    workspace_id = "explicit-id"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    parent_paths = resolve_workspace_paths(workspace_root=workspace_root, workspace_id=workspace_id)
    child_paths = []

    class _Service:
        def __init__(
            self,
            paths,
            *,
            containment_nonce: str,
            lifecycle_policy: DaemonLifecyclePolicy,
        ) -> None:
            assert containment_nonce == _TEST_CONTAINMENT_NONCE
            assert lifecycle_policy is DaemonLifecyclePolicy.EPHEMERAL
            child_paths.append(paths)

        def serve_forever(self) -> None:
            return None

    result = serve_workspace_daemon(
        _Service,
        workspace_root=workspace_root,
        workspace_id=workspace_id,
        containment_nonce=_TEST_CONTAINMENT_NONCE,
        lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL,
    )

    assert result == 0
    assert workspace_root.name != workspace_id
    assert child_paths[0].workspace_id == parent_paths.workspace_id
    assert child_paths[0].runtime_control_db_path == parent_paths.runtime_control_db_path
    assert child_paths[0].daemon_endpoint_kind == parent_paths.daemon_endpoint_kind
    assert child_paths[0].daemon_endpoint_path == parent_paths.daemon_endpoint_path


def test_daemon_main_uses_injected_resolve_paths_func(monkeypatch, tmp_path):
    workspace_root = tmp_path / "shared" / "folder-name"
    resolved = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="explicit-id")
    resolve_calls: list[tuple[Path | None, str | None]] = []
    serve_calls: list[tuple[Path, str, str, str]] = []
    startup_order: list[str] = []

    monkeypatch.setattr(
        "data_engine.hosts.daemon.app.serve_workspace_daemon",
        lambda **kwargs: startup_order.append("serve")
        or serve_calls.append(
            (
                kwargs["workspace_root"],
                kwargs["workspace_id"],
                kwargs["containment_nonce"],
                kwargs["lifecycle_policy"],
            )
        )
        or 0,
    )

    result = daemon_main(
        [
            "--workspace",
            str(workspace_root),
            "--workspace-id",
            "explicit-id",
            "--containment-nonce",
            _TEST_CONTAINMENT_NONCE,
            "--lifecycle-policy",
            "ephemeral",
        ],
        resolve_paths_func=lambda *, workspace_root=None, workspace_id=None: startup_order.append("resolve")
        or resolve_calls.append((workspace_root, workspace_id))
        or resolved,
        arm_process_group_watchdog_func=lambda **kwargs: startup_order.append(
            f"watchdog:{kwargs['containment_nonce']}"
        ),
    )

    assert result == 0
    assert startup_order == [
        f"watchdog:{_TEST_CONTAINMENT_NONCE}",
        "resolve",
        "serve",
    ]
    assert resolve_calls == [(workspace_root.resolve(), "explicit-id")]
    assert serve_calls == [
        (
            resolved.workspace_root,
            resolved.workspace_id,
            _TEST_CONTAINMENT_NONCE,
            "ephemeral",
        )
    ]


def test_containment_guard_refuses_a_mismatched_current_process(monkeypatch):
    expected = _test_process_identity(321)
    replacement = replace(expected, start_key="replacement-process")
    monkeypatch.setattr(daemon_server.os, "getpid", lambda: expected.pid)
    monkeypatch.setattr(
        daemon_server,
        "inspect_process_identity",
        lambda pid: replacement,
    )
    monkeypatch.setattr(
        daemon_server,
        "arm_posix_process_group_watchdog",
        lambda **kwargs: pytest.fail("identity mismatch must fail before containment"),
    )

    with pytest.raises(ProcessInspectionError, match="does not match its recorded identity"):
        daemon_server._require_current_process_containment(
            _TEST_CONTAINMENT_NONCE,
            expected_process_identity=expected,
        )


def test_daemon_main_fails_closed_before_path_resolution_without_containment(
    monkeypatch,
    tmp_path,
):
    workspace_root = tmp_path / "shared" / "default"
    events = []

    def _refuse_uncontained_entrypoint(containment_nonce: str):
        events.append(("containment", containment_nonce))
        raise ProcessInspectionError("current process is not contained")

    monkeypatch.setattr(
        "data_engine.hosts.daemon.entrypoints._require_current_process_containment",
        _refuse_uncontained_entrypoint,
    )

    with pytest.raises(ProcessInspectionError, match="not contained"):
        daemon_main(
            [
                "--workspace",
                str(workspace_root),
                "--containment-nonce",
                _TEST_CONTAINMENT_NONCE,
            ],
            resolve_paths_func=lambda **kwargs: pytest.fail(
                f"uncontained entrypoint must not resolve paths: {kwargs!r}"
            ),
        )

    assert events == [("containment", _TEST_CONTAINMENT_NONCE)]


def test_serve_workspace_daemon_fails_closed_before_path_resolution_without_containment(
    monkeypatch,
    tmp_path,
):
    workspace_root = tmp_path / "shared" / "default"
    events = []

    def _refuse_uncontained_server(containment_nonce: str):
        events.append(("containment", containment_nonce))
        raise ProcessInspectionError("current process is not contained")

    monkeypatch.setattr(
        daemon_server,
        "_require_current_process_containment",
        _refuse_uncontained_server,
    )

    with pytest.raises(ProcessInspectionError, match="not contained"):
        serve_workspace_daemon(
            object,
            workspace_root=workspace_root,
            containment_nonce=_TEST_CONTAINMENT_NONCE,
            resolve_paths_func=lambda **kwargs: pytest.fail(
                f"uncontained server must not resolve paths: {kwargs!r}"
            ),
        )

    assert events == [("containment", _TEST_CONTAINMENT_NONCE)]


def test_serve_forever_fails_closed_before_initialize_when_watchdog_is_unarmed(
    monkeypatch,
):
    expected = _test_process_identity(321)
    initialize_calls = []

    class _Service:
        process_identity = expected
        containment_nonce = _TEST_CONTAINMENT_NONCE

        def initialize(self) -> None:
            initialize_calls.append(True)

    monkeypatch.setattr(daemon_server, "_HOST_OS_NAME", "posix")
    monkeypatch.setattr(daemon_server.os, "getpid", lambda: expected.pid)
    monkeypatch.setattr(
        daemon_server,
        "inspect_process_identity",
        lambda pid: expected,
    )
    monkeypatch.setattr(
        daemon_server,
        "arm_posix_process_group_watchdog",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError("watchdog is not armed")
        ),
    )

    with pytest.raises(RuntimeError, match="watchdog is not armed"):
        serve_forever(_Service())

    assert initialize_calls == []


def test_containment_guard_verifies_and_closes_the_current_windows_job(monkeypatch):
    expected = replace(
        _test_process_identity(321),
        process_group_id=None,
        process_session_id=7,
    )
    events = []

    class _Job:
        def close(self) -> None:
            events.append("close")

    monkeypatch.setattr(daemon_server, "_HOST_OS_NAME", "nt")
    monkeypatch.setattr(daemon_server.os, "getpid", lambda: expected.pid)
    monkeypatch.setattr(
        daemon_server,
        "inspect_process_identity",
        lambda pid: events.append(("inspect", pid)) or expected,
    )
    monkeypatch.setattr(
        daemon_server,
        "open_verified_windows_kill_on_close_job",
        lambda identity, *, nonce: events.append(("job", identity, nonce)) or _Job(),
    )

    actual = daemon_server._require_current_process_containment(
        _TEST_CONTAINMENT_NONCE,
        expected_process_identity=expected,
    )

    assert actual == expected
    assert events == [
        ("inspect", expected.pid),
        ("job", expected, _TEST_CONTAINMENT_NONCE),
        "close",
    ]


def test_serve_forever_processes_one_command_then_shuts_down(
    tmp_path,
    monkeypatch,
    verified_server_containment,
):
    del verified_server_containment
    monkeypatch.setattr(daemon_server, "deliver_challenge", lambda *_args: None)
    monkeypatch.setattr(daemon_server, "answer_challenge", lambda *_args: None)
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    second_accept_started = threading.Event()
    wake_requested = threading.Event()
    wake_connection_closed = threading.Event()
    wake_connection_read = threading.Event()
    response_shutdown_states: list[bool] = []

    class _Connection:
        def __init__(self) -> None:
            self.sent_payloads: list[bytes] = []

        def recv_bytes(self, maxlength=None) -> bytes:
            assert maxlength == daemon_server._MAX_DAEMON_REQUEST_BYTES  # noqa: SLF001
            return _encode_message({"command": "shutdown_daemon"})

        def send_bytes(self, payload: bytes) -> None:
            self.sent_payloads.append(payload)
            assert second_accept_started.wait(timeout=1.0) is True
            response_shutdown_states.append(service.host.shutdown_event.is_set())

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _WakeConnection:
        def close(self) -> None:
            wake_connection_closed.set()

        def recv_bytes(self, maxlength=None) -> bytes:
            del maxlength
            wake_connection_read.set()
            raise AssertionError("shutdown wake connection reached a command worker")

        def send_bytes(self, payload: bytes) -> None:
            del payload

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Listener:
        def __init__(self, *args, **kwargs) -> None:
            assert "authkey" not in kwargs
            self.connection = _Connection()
            self.wake_connection = _WakeConnection()
            self.accept_count = 0

        def accept(self):
            self.accept_count += 1
            if self.accept_count == 1:
                return self.connection
            second_accept_started.set()
            assert wake_requested.wait(timeout=1.0) is True
            assert service.host.shutdown_event.is_set() is True
            return self.wake_connection

        def close(self):
            return None

    class _Service:
        def __init__(self, paths) -> None:
            self.paths = paths
            self.process_identity = _test_process_identity(os.getpid())
            self.containment_nonce = _TEST_CONTAINMENT_NONCE
            self._state_lock = threading.RLock()
            self.initialize_calls = 0
            self.handle_calls: list[dict[str, object]] = []
            self.shutdown_calls = 0
            self.state = DaemonHostState.build(started_at_utc="2026-04-06T00:00:00+00:00")
            self.host = type(
                "_Host",
                (),
                {"shutdown_event": threading.Event(), "listener": None},
            )()

        def initialize(self) -> None:
            self.initialize_calls += 1

        def _checkpoint_loop(self) -> None:
            return None

        def _debug_log(self, message: str) -> None:
            del message

        def _handle_command(self, payload):
            self.handle_calls.append(payload)
            return {"ok": True, "draining": False}

        def _wake_listener(self) -> None:
            wake_requested.set()

        def _shutdown(self) -> None:
            self.shutdown_calls += 1

        def _publish_runtime_event(self, event_type: str) -> None:
            del event_type

    service = _Service(paths)
    monkeypatch.setattr("data_engine.hosts.daemon.server.Listener", _Listener)

    serve_forever(service)  # noqa: SLF001 - direct server loop test

    assert service.initialize_calls == 1
    assert service.handle_calls == [{"command": "shutdown_daemon"}]
    assert service.shutdown_calls == 1
    assert response_shutdown_states == [False]
    assert service.host.listener.connection.sent_payloads == [
        _encode_message({"ok": True, "draining": False})
    ]
    assert wake_connection_closed.is_set() is True
    assert wake_connection_read.is_set() is False


def test_accepted_shutdown_proceeds_when_response_delivery_fails():
    wake_requested = threading.Event()
    debug_messages: list[str] = []

    class _Connection:
        def recv_bytes(self, maxlength=None) -> bytes:
            assert maxlength == daemon_server._MAX_DAEMON_REQUEST_BYTES  # noqa: SLF001
            return _encode_message({"command": "shutdown_daemon"})

        def send_bytes(self, payload: bytes) -> None:
            assert payload == _encode_message({"ok": True, "draining": False})
            raise BrokenPipeError("requester disconnected")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Service:
        def __init__(self) -> None:
            self.host = type(
                "_Host",
                (),
                {"shutdown_event": threading.Event()},
            )()

        def _handle_command(self, payload):
            assert payload == {"command": "shutdown_daemon"}
            return {"ok": True, "draining": False}

        def _debug_log(self, message: str) -> None:
            debug_messages.append(message)

        def _wake_listener(self) -> None:
            wake_requested.set()

    service = _Service()

    daemon_server._serve_connection(  # noqa: SLF001 - direct transport boundary regression
        service,
        _Connection(),
        family="AF_UNIX",
    )

    assert service.host.shutdown_event.is_set() is True
    assert wake_requested.wait(timeout=1.0) is True
    assert len(debug_messages) == 1
    assert "requester disconnected" in debug_messages[0]


def test_serve_forever_handles_second_request_while_first_is_still_running(
    tmp_path,
    monkeypatch,
    verified_server_containment,
):
    del verified_server_containment
    monkeypatch.setattr(daemon_server, "deliver_challenge", lambda *_args: None)
    monkeypatch.setattr(daemon_server, "answer_challenge", lambda *_args: None)
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    first_started = threading.Event()
    release_first = threading.Event()
    second_handled = threading.Event()

    class _Connection:
        def __init__(self, command: str) -> None:
            self.command = command
            self.sent_payloads: list[bytes] = []

        def recv_bytes(self, maxlength=None) -> bytes:
            assert maxlength == daemon_server._MAX_DAEMON_REQUEST_BYTES  # noqa: SLF001
            return _encode_message({"command": self.command})

        def send_bytes(self, payload: bytes) -> None:
            self.sent_payloads.append(payload)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Listener:
        def __init__(self, *args, **kwargs) -> None:
            self._connections = [_Connection("run_flow"), _Connection("daemon_status")]

        def accept(self):
            if self._connections:
                return self._connections.pop(0)
            while not service.host.shutdown_event.wait(0.01):
                continue
            raise OSError("listener closed")

        def close(self):
            return None

    class _Service:
        def __init__(self, paths) -> None:
            self.paths = paths
            self.process_identity = _test_process_identity(os.getpid())
            self.containment_nonce = _TEST_CONTAINMENT_NONCE
            self._state_lock = threading.RLock()
            self.initialize_calls = 0
            self.shutdown_calls = 0
            self.state = DaemonHostState.build(started_at_utc="2026-04-06T00:00:00+00:00")
            self.host = type(
                "_Host",
                (),
                {"shutdown_event": threading.Event(), "listener": None},
            )()

        def initialize(self) -> None:
            self.initialize_calls += 1

        def _checkpoint_loop(self) -> None:
            return None

        def _debug_log(self, message: str) -> None:
            del message

        def _handle_command(self, payload):
            command = payload.get("command")
            if command == "run_flow":
                first_started.set()
                release_first.wait(timeout=1.0)
                return {"ok": True, "command": command}
            if command == "daemon_status":
                second_handled.set()
                self.host.shutdown_event.set()
                return {"ok": True, "command": command}
            return {"ok": True, "command": command}

        def _shutdown(self) -> None:
            self.shutdown_calls += 1

        def _publish_runtime_event(self, event_type: str) -> None:
            del event_type

    service = _Service(paths)
    monkeypatch.setattr("data_engine.hosts.daemon.server.Listener", _Listener)

    server_thread = threading.Thread(target=serve_forever, args=(service,), daemon=True)
    server_thread.start()
    try:
        assert first_started.wait(timeout=1.0) is True
        assert second_handled.wait(timeout=1.0) is True
    finally:
        release_first.set()
        service.host.shutdown_event.set()
        server_thread.join(timeout=1.0)

    assert service.initialize_calls == 1
    assert service.shutdown_calls == 1


def test_serve_forever_rejects_connections_above_worker_limit(
    tmp_path,
    monkeypatch,
    verified_server_containment,
):
    del verified_server_containment
    monkeypatch.setattr(daemon_server, "deliver_challenge", lambda *_args: None)
    monkeypatch.setattr(daemon_server, "answer_challenge", lambda *_args: None)
    monkeypatch.setattr(daemon_server, "_MAX_CONNECTION_WORKERS", 1)
    workspace_root = tmp_path / "shared" / "default"
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    release_first = threading.Event()
    second_closed = threading.Event()

    class _Connection:
        def __init__(self, *, rejected: bool = False) -> None:
            self.rejected = rejected

        def close(self) -> None:
            if self.rejected:
                second_closed.set()
                release_first.set()
                service.host.shutdown_event.set()

        def recv_bytes(self, maxlength=None) -> bytes:
            assert maxlength == daemon_server._MAX_DAEMON_REQUEST_BYTES  # noqa: SLF001
            release_first.wait(timeout=1.0)
            return _encode_message({"command": "daemon_ping"})

        def send_bytes(self, _payload: bytes) -> None:
            return None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    first_connection = _Connection()
    second_connection = _Connection(rejected=True)

    class _Listener:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs
            self.connections = [first_connection, second_connection]

        def accept(self):
            if self.connections:
                return self.connections.pop(0)
            service.host.shutdown_event.wait(timeout=1.0)
            raise OSError("listener closed")

        def close(self) -> None:
            return None

    class _Service:
        def __init__(self) -> None:
            self.paths = paths
            self.process_identity = _test_process_identity(os.getpid())
            self.containment_nonce = _TEST_CONTAINMENT_NONCE
            self._state_lock = threading.RLock()
            self.state = DaemonHostState.build(
                started_at_utc="2026-04-06T00:00:00+00:00"
            )
            self.host = type(
                "_Host",
                (),
                {"shutdown_event": threading.Event(), "listener": None},
            )()

        def initialize(self) -> None:
            return None

        def _checkpoint_loop(self) -> None:
            return None

        def _debug_log(self, _message: str) -> None:
            return None

        def _handle_command(self, payload):
            return {"ok": True, "command": payload["command"]}

        def _publish_runtime_event(self, _event_type: str) -> None:
            return None

        def _shutdown(self) -> None:
            return None

    service = _Service()
    monkeypatch.setattr(daemon_server, "Listener", _Listener)

    serve_forever(service)

    assert second_closed.is_set() is True


def test_serve_forever_closes_idle_connection_before_storage_shutdown(
    tmp_path,
    monkeypatch,
    verified_server_containment,
):
    del verified_server_containment
    monkeypatch.setattr(daemon_server, "deliver_challenge", lambda *_args: None)
    monkeypatch.setattr(daemon_server, "answer_challenge", lambda *_args: None)
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    receive_started = threading.Event()
    connection_closed = threading.Event()
    command_worker_exited = threading.Event()
    storage_shutdown = threading.Event()

    class _IdleConnection:
        def recv_bytes(self, maxlength=None) -> bytes:
            assert maxlength == daemon_server._MAX_DAEMON_REQUEST_BYTES  # noqa: SLF001
            receive_started.set()
            connection_closed.wait()
            raise EOFError("connection closed")

        def send_bytes(self, payload: bytes) -> None:
            del payload
            raise OSError("connection closed")

        def close(self) -> None:
            connection_closed.set()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            command_worker_exited.set()
            return False

    connection = _IdleConnection()

    class _Listener:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs
            self._accepted = False

        def accept(self):
            if not self._accepted:
                self._accepted = True
                return connection
            service.host.shutdown_event.wait()
            raise OSError("listener closed")

        def close(self) -> None:
            return None

    class _Service:
        def __init__(self) -> None:
            self.paths = paths
            self.process_identity = _test_process_identity(os.getpid())
            self.containment_nonce = _TEST_CONTAINMENT_NONCE
            self._state_lock = threading.RLock()
            self.state = DaemonHostState.build(started_at_utc="2026-04-06T00:00:00+00:00")
            self.host = type(
                "_Host",
                (),
                {"shutdown_event": threading.Event(), "listener": None},
            )()

        def initialize(self) -> None:
            return None

        def _checkpoint_loop(self) -> None:
            return None

        def _debug_log(self, message: str) -> None:
            del message

        def _handle_command(self, payload):
            raise AssertionError(f"idle connection unexpectedly delivered {payload!r}")

        def _publish_runtime_event(self, event_type: str) -> None:
            del event_type

        def _shutdown(self) -> None:
            assert command_worker_exited.is_set() is True
            storage_shutdown.set()

    service = _Service()
    monkeypatch.setattr("data_engine.hosts.daemon.server.Listener", _Listener)
    server_thread = threading.Thread(target=serve_forever, args=(service,), daemon=True)
    server_thread.start()
    try:
        assert receive_started.wait(timeout=1.0) is True
        service.host.shutdown_event.set()
        server_thread.join(timeout=1.0)

        assert server_thread.is_alive() is False
        assert connection_closed.is_set() is True
        assert command_worker_exited.is_set() is True
        assert storage_shutdown.is_set() is True
    finally:
        connection_closed.set()
        service.host.shutdown_event.set()
        server_thread.join(timeout=1.0)


def test_fatal_listener_exit_joins_checkpoint_before_closing_ledgers(
    tmp_path,
    monkeypatch,
    verified_server_containment,
):
    del verified_server_containment
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)
    checkpoint_started = threading.Event()
    shutdown_seen = threading.Event()
    release_checkpoint = threading.Event()
    checkpoint_finished = threading.Event()
    ledger_closes: list[tuple[str, bool]] = []
    server_errors: list[BaseException] = []

    def _checkpoint_loop() -> None:
        checkpoint_started.set()
        service.host.shutdown_event.wait()
        shutdown_seen.set()
        release_checkpoint.wait()
        checkpoint_finished.set()

    monkeypatch.setattr(service, "_checkpoint_loop", _checkpoint_loop)
    original_cache_close = service.runtime_cache_ledger.close
    original_control_close = service.runtime_control_ledger.close
    monkeypatch.setattr(
        service.runtime_cache_ledger,
        "close",
        lambda: (ledger_closes.append(("cache", checkpoint_finished.is_set())), original_cache_close())[1],
    )
    monkeypatch.setattr(
        service.runtime_control_ledger,
        "close",
        lambda: (ledger_closes.append(("control", checkpoint_finished.is_set())), original_control_close())[1],
    )

    class _FatalListener:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def accept(self):
            assert checkpoint_started.wait(timeout=1.0) is True
            raise RuntimeError("fatal listener failure")

        def close(self) -> None:
            return None

    def _run_server() -> None:
        try:
            serve_forever(service)
        except BaseException as exc:  # pragma: no branch - captures the expected fatal boundary
            server_errors.append(exc)

    monkeypatch.setattr("data_engine.hosts.daemon.server.Listener", _FatalListener)
    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()
    try:
        assert checkpoint_started.wait(timeout=1.0) is True
        assert shutdown_seen.wait(timeout=1.0) is True
        assert server_thread.is_alive() is True
        assert ledger_closes == []

        release_checkpoint.set()
        server_thread.join(timeout=2.0)

        assert server_thread.is_alive() is False
        assert checkpoint_finished.is_set() is True
        assert service.state.checkpoint_thread is not None
        assert service.state.checkpoint_thread.is_alive() is False
        assert [(name, finished) for name, finished in ledger_closes] == [
            ("cache", True),
            ("control", True),
        ]
        assert len(server_errors) == 1
        assert isinstance(server_errors[0], RuntimeError)
        assert str(server_errors[0]) == "fatal listener failure"
    finally:
        release_checkpoint.set()
        service.host.shutdown_event.set()
        server_thread.join(timeout=2.0)


def test_daemon_initialize_writes_lease_metadata_before_first_checkpoint(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    service = DataEngineDaemonService(paths)
    monkeypatch.setattr(service, "_checkpoint_once", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="boom"):
        service.initialize()

    metadata = read_lease_metadata(paths)
    assert metadata is not None
    assert metadata["status"] == "starting"
    assert metadata["machine_id"] == machine_id_text(app_root=paths.app_root)


def test_daemon_service_can_start_in_observer_mode_when_workspace_is_leased(tmp_path, monkeypatch):
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
        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["ok"] is True
        assert status["status"]["status"] == "leased"
        assert status["status"]["workspace_owned"] is False
        assert status["status"]["leased_by_machine_id"] == "machine-a"
        assert status["status"]["leased_by_host_name"] == "test-host"

        flows = service._handle_command({"command": "list_flows"})  # noqa: SLF001
        assert flows["ok"] is True
        assert [item["name"] for item in flows["flows"]] == ["demo"]

        denied = service._handle_command({"command": "start_engine"})  # noqa: SLF001
        assert denied["ok"] is False
        assert "leased by test-host" in denied["error"]
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_service_reclaims_unreachable_same_machine_lease(tmp_path, monkeypatch):
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
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token is not None
    write_lease_metadata(
        paths,
        lease_token=bundle.lease_token,
        workspace_id="default",
        machine_id=machine_id_text(app_root=paths.app_root),
        host_name="test-host",
        daemon_id="daemon-a",
        pid=101,
        **_owner_process_kwargs(101),
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )
    monkeypatch.setattr("data_engine.hosts.daemon.client.is_daemon_live", lambda paths: False)

    service = DataEngineDaemonService(paths)
    service.initialize()
    try:
        status = service._handle_command({"command": "daemon_status"})  # noqa: SLF001
        assert status["ok"] is True
        assert status["status"]["status"] == "idle"
        assert status["status"]["workspace_owned"] is True
        assert status["status"]["leased_by_machine_id"] is None
    finally:
        service._shutdown()  # noqa: SLF001


def test_daemon_debug_log_keeps_only_last_30_days(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    _write_demo_flow(workspace_root)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    service = DataEngineDaemonService(paths)

    now = datetime(2026, 4, 8, 12, 0, tzinfo=UTC)
    old = (now - timedelta(days=DAEMON_LOG_RETENTION_DAYS + 1)).isoformat()
    recent = (now - timedelta(days=2)).isoformat()
    paths.runtime_state_dir.mkdir(parents=True, exist_ok=True)
    paths.daemon_log_path.write_text(
        f"{old} pid=1 workspace=default old entry\n"
        f"{recent} pid=1 workspace=default recent entry\n"
        "not-a-timestamp pid=1 workspace=default malformed entry\n",
        encoding="utf-8",
    )

    monkeypatch.setattr("data_engine.hosts.daemon.app.datetime", type("_FrozenDateTime", (), {"now": staticmethod(lambda tz=None: now)}))
    monkeypatch.setattr("data_engine.hosts.daemon.app.utcnow_text", lambda: now.isoformat())

    service._debug_log("fresh entry")  # noqa: SLF001

    contents = paths.daemon_log_path.read_text(encoding="utf-8")
    assert "old entry" not in contents
    assert "recent entry" in contents
    assert "malformed entry" in contents
    assert "fresh entry" in contents
