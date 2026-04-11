from __future__ import annotations

from pathlib import Path

import polars as pl

from data_engine.domain.source_state import SourceSignature
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state,
    claim_workspace,
    hydrate_local_runtime_state,
    initialize_workspace_state,
    read_control_request,
    read_lease_metadata,
    recover_stale_workspace,
    remove_control_request,
    release_workspace,
    write_control_request,
)


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def test_initialize_claim_and_release_workspace_markers(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    initialize_workspace_state(paths)

    assert (paths.available_markers_dir / "default").exists()
    assert not (paths.leased_markers_dir / "default").exists()

    assert claim_workspace(paths) is True
    assert not (paths.available_markers_dir / "default").exists()
    assert (paths.leased_markers_dir / "default").exists()

    release_workspace(paths)
    assert (paths.available_markers_dir / "default").exists()
    assert not (paths.leased_markers_dir / "default").exists()


def test_checkpoint_and_hydrate_workspace_state(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    source_ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    source_ledger.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    source_ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    source_ledger.logs.append(
        level="INFO",
        message="run=run-1 flow=demo source=None status=success elapsed=0.001000",
        created_at_utc=started,
        run_id="run-1",
        flow_name="demo",
    )

    checkpoint_workspace_state(
        paths,
        source_ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    metadata = read_lease_metadata(paths)
    assert metadata is not None
    assert metadata["workspace_id"] == "default"

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)
    assert [run.run_id for run in target_ledger.runs.list()] == ["run-1"]
    assert [entry.run_id for entry in target_ledger.logs.list(flow_name="demo")] == ["run-1"]


def test_checkpoint_workspace_state_handles_late_string_values_after_many_nulls(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    for index in range(120):
        run_id = f"run-{index}"
        ledger.runs.record_started(
            run_id=run_id,
            flow_name="demo",
            group_name="Demo",
            source_path=None,
            started_at_utc=started,
        )
        ledger.runs.record_finished(run_id=run_id, status="success", finished_at_utc=started)

    ledger.runs.record_started(
        run_id="run-failed",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    ledger.runs.record_finished(
        run_id="run-failed",
        status="failed",
        finished_at_utc=started,
        error_text="late error text",
    )

    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    assert paths.shared_runs_path.exists() is True


def test_checkpoint_workspace_state_writes_typed_parquet_when_optional_columns_are_all_null(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    ledger.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    ledger.logs.append(
        level="INFO",
        message="run=run-1 flow=demo source=None status=success elapsed=0.001000",
        created_at_utc=started,
        run_id="run-1",
        flow_name="demo",
    )

    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version=None,
    )

    runs_schema = pl.read_parquet_schema(paths.shared_runs_path)
    logs_schema = pl.read_parquet_schema(paths.shared_logs_path)

    assert runs_schema["snapshot_generation_id"] == pl.String
    assert runs_schema["source_path"] == pl.String
    assert runs_schema["error_text"] == pl.String
    assert logs_schema["snapshot_generation_id"] == pl.String
    assert logs_schema["run_id"] == pl.String
    assert logs_schema["step_label"] == pl.String


def test_hydrate_local_runtime_state_ignores_mixed_snapshot_generations(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    ledger.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    ledger.logs.append(
        level="INFO",
        message="run=run-1 flow=demo source=None status=success elapsed=0.001000",
        created_at_utc=started,
        run_id="run-1",
        flow_name="demo",
    )

    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    runs_frame = pl.read_parquet(paths.shared_runs_path)
    logs_frame = pl.read_parquet(paths.shared_logs_path)
    runs_frame = runs_frame.with_columns(pl.lit("generation-a").alias("snapshot_generation_id"))
    logs_frame = logs_frame.with_columns(pl.lit("generation-b").alias("snapshot_generation_id"))
    runs_frame.write_parquet(paths.shared_runs_path)
    logs_frame.write_parquet(paths.shared_logs_path)

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)

    assert list(target_ledger.runs.list()) == []
    assert list(target_ledger.logs.list()) == []


def test_hydrate_local_runtime_state_retries_after_torn_snapshot_read(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    ledger.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    ledger.logs.append(
        level="INFO",
        message="run=run-1 flow=demo source=None status=success elapsed=0.001000",
        created_at_utc=started,
        run_id="run-1",
        flow_name="demo",
    )

    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    original_read_parquet = pl.read_parquet
    attempts = {"count": 0}

    def flaky_read_parquet_with_retries(path, *args, **kwargs):
        if not Path(path).is_file():
            return pl.DataFrame()
        frame = original_read_parquet(path, *args, **kwargs)
        attempts["count"] += 1
        if attempts["count"] == 3 and Path(path) == paths.shared_logs_path:
            return frame.with_columns(pl.lit("generation-b").alias("snapshot_generation_id"))
        return frame

    monkeypatch.setattr("data_engine.runtime.shared_state._read_parquet_with_retries", flaky_read_parquet_with_retries)

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)

    assert [run.run_id for run in target_ledger.runs.list()] == ["run-1"]


def test_hydrate_local_runtime_state_reassigns_local_log_ids(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    ledger.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    ledger.logs.append(
        level="INFO",
        message="first log",
        created_at_utc=started,
        run_id="run-1",
        flow_name="demo",
    )
    ledger.logs.append(
        level="ERROR",
        message="second log",
        created_at_utc=started,
        run_id="run-1",
        flow_name="demo",
    )

    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    logs_frame = pl.read_parquet(paths.shared_logs_path)
    assert logs_frame.height == 2
    logs_frame = logs_frame.with_columns(pl.lit(1).alias("id"))
    logs_frame.write_parquet(paths.shared_logs_path)

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)

    logs = target_ledger.logs.list(flow_name="demo")
    assert [entry.message for entry in logs] == ["first log", "second log"]
    assert [entry.id for entry in logs] == [1, 2]


def test_hydrate_local_runtime_state_deduplicates_file_state_rows(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    ledger.source_signatures.upsert_file_state(
        flow_name="demo",
        signature=SourceSignature(source_path="/tmp/input.xlsx", mtime_ns=1, size_bytes=10),
        status="success",
        run_id="run-1",
        finished_at_utc=started,
    )

    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    file_state_frame = pl.read_parquet(paths.shared_file_state_path)
    assert file_state_frame.height == 1
    duplicate_frame = file_state_frame.with_columns(
        pl.lit(2, dtype=pl.Int64).alias("mtime_ns"),
        pl.lit(20, dtype=pl.Int64).alias("size_bytes"),
        pl.lit("run-2").alias("last_success_run_id"),
    )
    pl.concat([file_state_frame, duplicate_frame], how="vertical").write_parquet(paths.shared_file_state_path)

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)

    rows = target_ledger.source_signatures.list_file_states()
    assert len(rows) == 1
    assert rows[0].flow_name == "demo"
    assert rows[0].source_path == "/tmp/input.xlsx"
    assert rows[0].mtime_ns == 2
    assert rows[0].size_bytes == 20
    assert rows[0].last_success_run_id == "run-2"


def test_recover_stale_workspace_quarantines_old_lease(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    old_time = "2000-01-01T00:00:00+00:00"
    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="0.1.0",
    )

    recovered = recover_stale_workspace(paths, machine_id="machine-b", stale_after_seconds=1.0)

    assert recovered is True
    assert (paths.leased_markers_dir / "default").exists()
    assert any(paths.stale_markers_dir.iterdir())


def test_recover_stale_workspace_without_reclaim_restores_available_marker(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    assert claim_workspace(paths) is True

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    old_time = "2000-01-01T00:00:00+00:00"
    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="0.1.0",
    )

    recovered = recover_stale_workspace(
        paths,
        machine_id="machine-b",
        stale_after_seconds=1.0,
        reclaim=False,
    )

    assert recovered is True
    assert (paths.available_markers_dir / "default").exists()
    assert not (paths.leased_markers_dir / "default").exists()
    assert any(paths.stale_markers_dir.iterdir())


def test_write_and_remove_control_request(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)

    write_control_request(
        paths,
        workspace_id="default",
        requester_machine_id="machine-b",
        requester_host_name="machine-b",
        requester_pid=202,
        requester_client_kind="ui",
        requested_at_utc="2026-03-30T00:00:00+00:00",
    )

    metadata = read_control_request(paths)
    assert metadata is not None
    assert metadata["requester_machine_id"] == "machine-b"
    assert metadata["requester_client_kind"] == "ui"

    remove_control_request(paths)

    assert read_control_request(paths) is None


def test_hydrate_local_runtime_state_retries_until_snapshot_generations_match(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    source_ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    source_ledger.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    source_ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    checkpoint_workspace_state(
        paths,
        source_ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    original_read_parquet = pl.read_parquet
    attempts = {"count": 0}

    def flaky_read_parquet_with_retries(path, *args, **kwargs):
        if not Path(path).is_file():
            return pl.DataFrame()
        frame = original_read_parquet(path, *args, **kwargs)
        attempts["count"] += 1
        if attempts["count"] == 3 and Path(path) == paths.shared_logs_path:
            return frame.with_columns(pl.lit("gen-b").alias("snapshot_generation_id"))
        return frame

    monkeypatch.setattr("data_engine.runtime.shared_state._read_parquet_with_retries", flaky_read_parquet_with_retries)

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)

    assert [run.run_id for run in target_ledger.runs.list()] == ["run-1"]


def test_read_lease_metadata_retries_after_transient_parquet_error(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    checkpoint_workspace_state(
        paths,
        ledger,
        workspace_id="default",
        machine_id="machine-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    original_read_parquet = pl.read_parquet
    attempts = {"count": 0}

    def flaky_read_parquet(path, *args, **kwargs):
        if Path(path) == paths.lease_metadata_path and attempts["count"] == 0:
            attempts["count"] += 1
            raise FileNotFoundError("transient rename window")
        return original_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pl, "read_parquet", flaky_read_parquet)

    metadata = read_lease_metadata(paths)

    assert metadata is not None
    assert metadata["workspace_id"] == "default"
