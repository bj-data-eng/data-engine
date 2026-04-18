from __future__ import annotations

from data_engine.runtime.runtime_cache_store import RuntimeCacheLedger
from data_engine.runtime.runtime_control_store import RuntimeControlLedger
from data_engine.runtime.runtime_db import utcnow_text
from data_engine.runtime.shared_state import checkpoint_workspace_state, hydrate_local_runtime_state, write_control_request
from data_engine.services.reset import ResetService

from tests.services.support import record_flow_state, resolve_workspace_paths


def test_reset_service_resets_one_flow_history_and_poll_freshness(tmp_path):
    workspace_root = tmp_path / "workspace"
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="workspace")
    alpha_source = tmp_path / "alpha.xlsx"
    beta_source = tmp_path / "beta.xlsx"
    alpha_source.write_text("alpha", encoding="utf-8")
    beta_source.write_text("beta", encoding="utf-8")

    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    record_flow_state(ledger, flow_name="alpha", source_path=alpha_source, run_id="run-alpha")
    record_flow_state(ledger, flow_name="beta", source_path=beta_source, run_id="run-beta")
    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=100,
        status="idle",
        started_at_utc=utcnow_text(),
        last_checkpoint_at_utc=utcnow_text(),
        app_version="test",
    )

    ResetService().reset_flow(paths=paths, runtime_cache_ledger=ledger, flow_name="alpha")

    assert ledger.runs.list(flow_name="alpha") == ()
    assert ledger.logs.list(flow_name="alpha") == ()
    assert ledger.source_signatures.list_file_states(flow_name="alpha") == ()
    assert len(ledger.runs.list(flow_name="beta")) == 1
    assert len(ledger.logs.list(flow_name="beta")) == 1
    assert len(ledger.source_signatures.list_file_states(flow_name="beta")) == 1

    hydrated = RuntimeCacheLedger(tmp_path / "hydrated.sqlite")
    try:
        hydrate_local_runtime_state(paths, hydrated)
        assert hydrated.runs.list(flow_name="alpha") == ()
        assert hydrated.logs.list(flow_name="alpha") == ()
        assert hydrated.source_signatures.list_file_states(flow_name="alpha") == ()
        assert len(hydrated.runs.list(flow_name="beta")) == 1
    finally:
        hydrated.close()
        ledger.close()


def test_reset_service_resets_workspace_local_and_shared_state(tmp_path):
    workspace_root = tmp_path / "workspace"
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="workspace")
    source_path = tmp_path / "alpha.xlsx"
    source_path.write_text("alpha", encoding="utf-8")

    runtime_cache_ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    runtime_control_ledger = RuntimeControlLedger(paths.runtime_control_db_path)
    record_flow_state(runtime_cache_ledger, flow_name="alpha", source_path=source_path, run_id="run-alpha")
    runtime_control_ledger.daemon_state.upsert(
        workspace_id=paths.workspace_id,
        pid=100,
        endpoint_kind="tcp",
        endpoint_path="127.0.0.1:1234",
        started_at_utc=utcnow_text(),
        last_checkpoint_at_utc=utcnow_text(),
        status="idle",
        app_root=str(paths.app_root),
        workspace_root=str(paths.workspace_root),
        version_text="test",
    )
    runtime_control_ledger.client_sessions.upsert(
        client_id="client-1",
        workspace_id=paths.workspace_id,
        client_kind="ui",
        pid=999999,
    )
    checkpoint_workspace_state(
        paths,
        runtime_cache_ledger,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=100,
        status="idle",
        started_at_utc=utcnow_text(),
        last_checkpoint_at_utc=utcnow_text(),
        app_version="test",
    )
    write_control_request(
        paths,
        workspace_id=paths.workspace_id,
        requester_machine_id="machine-b",
        requester_host_name="host-b",
        requester_pid=200,
        requester_client_kind="ui",
        requested_at_utc=utcnow_text(),
    )
    paths.daemon_log_path.parent.mkdir(parents=True, exist_ok=True)
    paths.daemon_log_path.write_text("daemon log", encoding="utf-8")

    ResetService().reset_workspace(
        paths=paths,
        runtime_cache_ledger=runtime_cache_ledger,
        runtime_control_ledger=runtime_control_ledger,
    )

    assert paths.runtime_cache_db_path.exists() is True
    assert paths.runtime_control_db_path.exists() is True
    assert paths.daemon_log_path.exists() is True
    assert paths.daemon_log_path.read_text(encoding="utf-8") == ""
    reset_runtime_cache_ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    reset_runtime_control_ledger = RuntimeControlLedger(paths.runtime_control_db_path)
    try:
        assert reset_runtime_cache_ledger.runs.list() == ()
        assert reset_runtime_cache_ledger.logs.list() == ()
        assert reset_runtime_cache_ledger.source_signatures.list_file_states() == ()
        assert reset_runtime_control_ledger.daemon_state.get(paths.workspace_id) is None
        assert reset_runtime_control_ledger.client_sessions.count_live(paths.workspace_id) == 0
    finally:
        reset_runtime_cache_ledger.close()
        reset_runtime_control_ledger.close()
    assert paths.shared_runs_path.exists() is False
    assert paths.shared_step_runs_path.exists() is False
    assert paths.shared_logs_path.exists() is False
    assert paths.shared_file_state_path.exists() is False
    assert paths.control_request_path.exists() is False
    assert (paths.available_markers_dir / paths.workspace_id).is_dir()
    assert paths.lease_metadata_path.exists() is True
