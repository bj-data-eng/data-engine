from __future__ import annotations

import os
from pathlib import Path
import threading

import polars as pl

import data_engine.runtime.shared_state as shared_state_module
from data_engine.domain.source_state import SourceSignature
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state,
    claim_workspace,
    hydrate_local_runtime_state,
    initialize_workspace_state,
    read_control_request,
    read_lease_metadata,
    read_runtime_snapshot_generation,
    recover_stale_workspace,
    remove_control_request,
    release_workspace,
    write_control_request,
)
from data_engine.services.workspace_io import WorkspaceIoLayer

from tests.services.support import resolve_workspace_paths


def _committed_artifact_paths(paths):
    generation_id = read_runtime_snapshot_generation(paths)
    assert generation_id is not None
    generation_dir = paths.shared_snapshot_generations_dir / generation_id
    return {
        "generation_id": generation_id,
        "runs": generation_dir / "runs.parquet",
        "step_runs": generation_dir / "step_runs.parquet",
        "logs": generation_dir / "logs.parquet",
        "file_state": generation_dir / "file_state.parquet",
    }


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
    source_ledger.runs.record_started(run_id="run-1", flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
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
        host_name="test-host",
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
    assert metadata["machine_id"] == "machine-a"
    assert metadata["host_name"] == "test-host"

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)
    assert [run.run_id for run in target_ledger.runs.list()] == ["run-1"]
    assert [entry.run_id for entry in target_ledger.logs.list(flow_name="demo")] == ["run-1"]


def test_shared_state_helpers_accept_protocol_shaped_snapshot_store(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    source_ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    source_ledger.runs.record_started(run_id="run-1", flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
    source_ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    source_ledger.logs.append(
        level="INFO",
        message="run=run-1 flow=demo source=None status=success elapsed=0.001000",
        created_at_utc=started,
        run_id="run-1",
        flow_name="demo",
    )

    class SnapshotStore:
        def __init__(self, ledger):
            self.runs = ledger.runs
            self.step_outputs = ledger.step_outputs
            self.logs = ledger.logs
            self.source_signatures = ledger.source_signatures
            self.snapshots = ledger.snapshots

    checkpoint_workspace_state(
        paths,
        SnapshotStore(source_ledger),
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

    target_path = app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite"
    target_ledger = RuntimeCacheLedger(target_path)
    hydrate_local_runtime_state(paths, SnapshotStore(target_ledger))

    assert [run.run_id for run in target_ledger.runs.list()] == ["run-1"]


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
        ledger.runs.record_started(run_id=run_id, flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
        ledger.runs.record_finished(run_id=run_id, status="success", finished_at_utc=started)

    ledger.runs.record_started(run_id="run-failed", flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
    ledger.runs.record_finished(run_id="run-failed", status="failed", finished_at_utc=started, error_text="late error text")

    checkpoint_workspace_state(
        paths,
        ledger,
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

    assert _committed_artifact_paths(paths)["runs"].exists() is True


def test_checkpoint_workspace_state_writes_typed_parquet_when_optional_columns_are_all_null(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    ledger.runs.record_started(run_id="run-1", flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
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
        host_name="test-host",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version=None,
    )

    artifacts = _committed_artifact_paths(paths)
    runs_schema = pl.read_parquet_schema(artifacts["runs"])
    logs_schema = pl.read_parquet_schema(artifacts["logs"])

    assert runs_schema["snapshot_generation_id"] == pl.String
    assert runs_schema["source_path"] == pl.String
    assert runs_schema["error_text"] == pl.String
    assert logs_schema["snapshot_generation_id"] == pl.String
    assert logs_schema["run_id"] == pl.String
    assert logs_schema["step_label"] == pl.String


def test_checkpoint_workspace_state_commits_all_four_typed_empty_artifacts(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()

    checkpoint_workspace_state(
        paths,
        ledger,
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

    artifacts = _committed_artifact_paths(paths)
    expected_columns = {
        "runs": tuple(shared_state_module._RUNS_SCHEMA),  # noqa: SLF001 - format contract
        "step_runs": tuple(shared_state_module._STEP_RUNS_SCHEMA),  # noqa: SLF001 - format contract
        "logs": tuple(shared_state_module._LOGS_SCHEMA),  # noqa: SLF001 - format contract
        "file_state": tuple(shared_state_module._FILE_STATE_SCHEMA),  # noqa: SLF001 - format contract
    }
    for artifact_name, columns in expected_columns.items():
        frame = pl.read_parquet(artifacts[artifact_name])
        assert frame.height == 0
        assert tuple(frame.columns) == columns


def test_hydrate_same_generation_skips_all_parquet_and_sqlite_replacement(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    source = RuntimeCacheLedger(paths.runtime_db_path)
    target = RuntimeCacheLedger(tmp_path / "target.sqlite")
    started = utcnow_text()
    source.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    checkpoint_workspace_state(
        paths,
        source,
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
    assert hydrate_local_runtime_state(paths, target) is True

    monkeypatch.setattr(
        shared_state_module,
        "_read_parquet_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Parquet should not be read")),
    )
    monkeypatch.setattr(
        target.snapshots,
        "replace",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("SQLite should not be replaced")),
    )

    assert hydrate_local_runtime_state(paths, target) is False


def test_hydrate_local_runtime_state_ignores_mixed_snapshot_generations(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    ledger.runs.record_started(run_id="run-1", flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
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
        host_name="test-host",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    artifacts = _committed_artifact_paths(paths)
    logs_frame = pl.read_parquet(artifacts["logs"]).with_columns(
        pl.lit("generation-b").alias("snapshot_generation_id")
    )
    logs_frame.write_parquet(artifacts["logs"])

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
    ledger.runs.record_started(run_id="run-1", flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
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
        host_name="test-host",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    original_read_parquet = pl.read_parquet
    attempts = {"count": 0}
    retry_delays: list[float] = []

    def flaky_read_parquet_with_retries(path, *args, **kwargs):
        kwargs.pop("required", None)
        if not Path(path).is_file():
            return pl.DataFrame()
        frame = original_read_parquet(path, *args, **kwargs)
        attempts["count"] += 1
        if attempts["count"] == 3 and Path(path).name == "logs.parquet":
            return frame.with_columns(pl.lit("generation-b").alias("snapshot_generation_id"))
        return frame

    monkeypatch.setattr("data_engine.runtime.shared_state._read_parquet_with_retries", flaky_read_parquet_with_retries)
    monkeypatch.setattr("data_engine.runtime.shared_state.time.sleep", retry_delays.append)

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)

    assert [run.run_id for run in target_ledger.runs.list()] == ["run-1"]
    assert retry_delays == [0.02]


def test_hydrate_local_runtime_state_preserves_shared_step_and_log_ids(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    ledger.runs.record_started(run_id="run-1", flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
    ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    step_run_id = ledger.step_outputs.record_started(
        run_id="run-1",
        flow_name="demo",
        step_label="Transform",
        started_at_utc=started,
    )
    ledger.step_outputs.record_finished(
        step_run_id=step_run_id,
        status="success",
        finished_at_utc=started,
        elapsed_ms=5,
    )
    ledger.logs.append(level="INFO", message="first log", created_at_utc=started, run_id="run-1", flow_name="demo")
    ledger.logs.append(level="ERROR", message="second log", created_at_utc=started, run_id="run-1", flow_name="demo")

    checkpoint_workspace_state(
        paths,
        ledger,
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

    artifacts = _committed_artifact_paths(paths)
    step_runs_frame = pl.read_parquet(artifacts["step_runs"]).with_columns(
        pl.lit(303, dtype=pl.Int64).alias("id")
    )
    step_runs_frame.write_parquet(artifacts["step_runs"])
    logs_frame = pl.read_parquet(artifacts["logs"])
    assert logs_frame.height == 2
    logs_frame = logs_frame.with_columns(pl.Series("id", [101, 202], dtype=pl.Int64))
    logs_frame.write_parquet(artifacts["logs"])

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)

    logs = target_ledger.logs.list(flow_name="demo")
    step_runs = target_ledger.step_outputs.list_for_run("run-1")
    assert [entry.id for entry in step_runs] == [303]
    assert [entry.message for entry in logs] == ["first log", "second log"]
    assert [entry.id for entry in logs] == [101, 202]


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
        host_name="test-host",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="0.1.0",
    )

    artifacts = _committed_artifact_paths(paths)
    file_state_frame = pl.read_parquet(artifacts["file_state"])
    assert file_state_frame.height == 1
    duplicate_frame = file_state_frame.with_columns(
        pl.lit(2, dtype=pl.Int64).alias("mtime_ns"),
        pl.lit(20, dtype=pl.Int64).alias("size_bytes"),
        pl.lit("run-2").alias("last_success_run_id"),
    )
    pl.concat([file_state_frame, duplicate_frame], how="vertical").write_parquet(artifacts["file_state"])

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)

    rows = target_ledger.source_signatures.list_file_states()
    assert len(rows) == 1
    assert rows[0].flow_name == "demo"
    assert rows[0].source_path == "/tmp/input.xlsx"
    assert rows[0].mtime_ns == 2
    assert rows[0].size_bytes == 20
    assert rows[0].last_success_run_id == "run-2"


def test_hydration_retries_when_gc_removes_the_selected_generation(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    source = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    source.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )

    def checkpoint() -> None:
        checkpoint_workspace_state(
            paths,
            source,
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

    checkpoint()
    selected_generation = read_runtime_snapshot_generation(paths)
    assert selected_generation is not None
    original_read = shared_state_module._read_parquet_with_retries  # noqa: SLF001 - deterministic GC race
    raced = {"value": False}

    def publish_during_read(path, **kwargs):
        if not raced["value"]:
            raced["value"] = True
            for index in range(4):
                source.logs.append(
                    level="INFO",
                    message=f"checkpoint {index}",
                    created_at_utc=started,
                    run_id="run-1",
                    flow_name="demo",
                )
                checkpoint()
            assert not (paths.shared_snapshot_generations_dir / selected_generation).exists()
        return original_read(path, **kwargs)

    monkeypatch.setattr(shared_state_module, "_read_parquet_with_retries", publish_during_read)
    target = RuntimeCacheLedger(tmp_path / "target.sqlite")

    assert hydrate_local_runtime_state(paths, target) is True
    assert raced["value"] is True
    assert [run.run_id for run in target.runs.list()] == ["run-1"]
    assert len(tuple(paths.shared_snapshot_generations_dir.iterdir())) == 3


def test_checkpoint_succeeds_when_generation_gc_stat_races(tmp_path, monkeypatch):
    workspace_root = tmp_path / "shared" / "default"
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    first_generation = checkpoint_workspace_state(
        paths,
        ledger,
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
    original_stat = Path.stat

    def flaky_generation_stat(path, *args, **kwargs):
        if path.parent == paths.shared_snapshot_generations_dir and len(path.name) == 32:
            raise FileNotFoundError(path)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", flaky_generation_stat)

    second_generation = checkpoint_workspace_state(
        paths,
        ledger,
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

    assert second_generation != first_generation
    assert read_runtime_snapshot_generation(paths) == second_generation


def test_generation_gc_pins_the_manifest_selected_generation(tmp_path):
    workspace_root = tmp_path / "shared" / "default"
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()
    manifest_generation = checkpoint_workspace_state(
        paths,
        ledger,
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
    manifest_generation_dir = paths.shared_snapshot_generations_dir / manifest_generation
    os.utime(manifest_generation_dir, ns=(1, 1))
    synthetic_generations = tuple(f"{index:032x}" for index in range(1, 4))
    for index, generation_id in enumerate(synthetic_generations, start=2):
        generation_dir = paths.shared_snapshot_generations_dir / generation_id
        generation_dir.mkdir()
        os.utime(generation_dir, ns=(index, index))

    shared_state_module._garbage_collect_snapshot_generations(  # noqa: SLF001 - GC invariant
        paths,
        committed_generation_id=synthetic_generations[-1],
        protected_generation_ids=frozenset((synthetic_generations[-1],)),
    )

    assert manifest_generation_dir.is_dir()


def test_workspace_io_idle_checkpoint_updates_heartbeat_without_snapshot_scan(tmp_path, monkeypatch):
    workspace_root = tmp_path / "shared" / "default"
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    workspace_io = WorkspaceIoLayer()
    started = utcnow_text()

    def checkpoint(checkpoint_at: str) -> None:
        workspace_io.checkpoint_workspace_state(
            paths,
            ledger,
            workspace_id="default",
            machine_id="machine-a",
            host_name="test-host",
            daemon_id="daemon-a",
            pid=101,
            status="idle",
            started_at_utc=started,
            last_checkpoint_at_utc=checkpoint_at,
            app_version="0.1.0",
        )

    checkpoint("2026-07-13T00:00:00+00:00")
    first_generation = read_runtime_snapshot_generation(paths)
    original_export = ledger.snapshots.export
    monkeypatch.setattr(
        ledger.snapshots,
        "export",
        lambda: (_ for _ in ()).throw(AssertionError("idle checkpoint scanned runtime tables")),
    )

    checkpoint("2026-07-13T00:00:30+00:00")

    assert read_runtime_snapshot_generation(paths) == first_generation
    assert read_lease_metadata(paths)["last_checkpoint_at_utc"] == "2026-07-13T00:00:30+00:00"
    assert len(tuple(paths.shared_snapshot_generations_dir.iterdir())) == 1

    monkeypatch.setattr(ledger.snapshots, "export", original_export)
    ledger.logs.append(level="INFO", message="changed", created_at_utc=started)
    checkpoint("2026-07-13T00:01:00+00:00")

    assert read_runtime_snapshot_generation(paths) != first_generation


def test_workspace_io_checkpoint_republishes_for_a_distinct_ledger_incarnation(tmp_path):
    workspace_root = tmp_path / "shared" / "default"
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    workspace_io = WorkspaceIoLayer()
    started = utcnow_text()

    def populated_ledger(db_path: Path, *, run_id: str) -> RuntimeCacheLedger:
        ledger = RuntimeCacheLedger(db_path)
        ledger.runs.record_started(
            run_id=run_id,
            flow_name="demo",
            group_name="Demo",
            source_path=None,
            started_at_utc=started,
        )
        return ledger

    first = populated_ledger(tmp_path / "first.sqlite", run_id="run-first")
    second = populated_ledger(tmp_path / "second.sqlite", run_id="run-second")
    checkpoint_kwargs = {
        "workspace_id": "default",
        "machine_id": "machine-a",
        "host_name": "test-host",
        "daemon_id": "daemon-a",
        "pid": 101,
        "status": "idle",
        "started_at_utc": started,
        "last_checkpoint_at_utc": started,
        "app_version": "0.1.0",
    }

    workspace_io.checkpoint_workspace_state(paths, first, **checkpoint_kwargs)
    first_generation = read_runtime_snapshot_generation(paths)
    first.close()
    workspace_io.checkpoint_workspace_state(paths, second, **checkpoint_kwargs)
    second_generation = read_runtime_snapshot_generation(paths)

    target = RuntimeCacheLedger(tmp_path / "target.sqlite")
    assert first_generation != second_generation
    assert hydrate_local_runtime_state(paths, target) is True
    assert [run.run_id for run in target.runs.list()] == ["run-second"]


def test_workspace_io_hydration_cadence_is_scoped_to_the_target_ledger(tmp_path):
    workspace_root = tmp_path / "shared" / "default"
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    source = RuntimeCacheLedger(tmp_path / "source.sqlite")
    started = utcnow_text()
    source.runs.record_started(
        run_id="run-1",
        flow_name="demo",
        group_name="Demo",
        source_path=None,
        started_at_utc=started,
    )
    checkpoint_workspace_state(
        paths,
        source,
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
    workspace_io = WorkspaceIoLayer(hydrate_interval_seconds=60.0)
    first_target = RuntimeCacheLedger(tmp_path / "first-target.sqlite")
    second_target = RuntimeCacheLedger(tmp_path / "second-target.sqlite")

    assert workspace_io.hydrate_local_runtime(paths, first_target) is True
    assert workspace_io.hydrate_local_runtime(paths, second_target) is True
    assert [run.run_id for run in second_target.runs.list()] == ["run-1"]


def test_concurrent_snapshot_publishers_serialize_manifest_and_gc(tmp_path, monkeypatch):
    workspace_root = tmp_path / "shared" / "default"
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    started = utcnow_text()

    def populated_ledger(db_path: Path, *, run_id: str) -> RuntimeCacheLedger:
        ledger = RuntimeCacheLedger(db_path)
        ledger.runs.record_started(
            run_id=run_id,
            flow_name="demo",
            group_name="Demo",
            source_path=None,
            started_at_utc=started,
        )
        return ledger

    first = populated_ledger(tmp_path / "first.sqlite", run_id="run-first")
    second = populated_ledger(tmp_path / "second.sqlite", run_id="run-second")
    first_gc_entered = threading.Event()
    release_first_gc = threading.Event()
    second_started = threading.Event()
    original_gc = shared_state_module._garbage_collect_snapshot_generations  # noqa: SLF001
    gc_calls = {"count": 0}

    def delayed_first_gc(*args, **kwargs):
        gc_calls["count"] += 1
        if gc_calls["count"] == 1:
            first_gc_entered.set()
            assert release_first_gc.wait(timeout=2.0)
        return original_gc(*args, **kwargs)

    monkeypatch.setattr(shared_state_module, "_garbage_collect_snapshot_generations", delayed_first_gc)
    generations: dict[str, str] = {}
    errors: list[BaseException] = []

    def publish(name: str, ledger: RuntimeCacheLedger) -> None:
        if name == "second":
            second_started.set()
        try:
            generations[name] = checkpoint_workspace_state(
                paths,
                ledger,
                workspace_id="default",
                machine_id="machine-a",
                host_name="test-host",
                daemon_id=f"daemon-{name}",
                pid=101,
                status="idle",
                started_at_utc=started,
                last_checkpoint_at_utc=started,
                app_version="0.1.0",
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first_thread = threading.Thread(target=publish, args=("first", first))
    second_thread = threading.Thread(target=publish, args=("second", second))
    first_thread.start()
    assert first_gc_entered.wait(timeout=2.0)
    second_thread.start()
    assert second_started.wait(timeout=2.0)
    assert second_thread.is_alive()
    release_first_gc.set()
    first_thread.join(timeout=3.0)
    second_thread.join(timeout=3.0)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert errors == []
    assert read_runtime_snapshot_generation(paths) == generations["second"]
    target = RuntimeCacheLedger(tmp_path / "target.sqlite")
    assert hydrate_local_runtime_state(paths, target) is True
    assert [run.run_id for run in target.runs.list()] == ["run-second"]


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
        host_name="test-host",
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
        host_name="test-host",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="0.1.0",
    )

    recovered = recover_stale_workspace(paths, machine_id="machine-b", stale_after_seconds=1.0, reclaim=False)

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
        requester_host_name="host-b",
        requester_pid=202,
        requester_client_kind="ui",
        requested_at_utc="2026-03-30T00:00:00+00:00",
    )

    metadata = read_control_request(paths)
    assert metadata is not None
    assert metadata["requester_machine_id"] == "machine-b"
    assert metadata["requester_host_name"] == "host-b"
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
    source_ledger.runs.record_started(run_id="run-1", flow_name="demo", group_name="Demo", source_path=None, started_at_utc=started)
    source_ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started)
    checkpoint_workspace_state(
        paths,
        source_ledger,
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

    original_read_parquet = pl.read_parquet
    attempts = {"count": 0}

    def flaky_read_parquet_with_retries(path, *args, **kwargs):
        kwargs.pop("required", None)
        if not Path(path).is_file():
            return pl.DataFrame()
        frame = original_read_parquet(path, *args, **kwargs)
        attempts["count"] += 1
        if attempts["count"] == 3 and Path(path).name == "logs.parquet":
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
        host_name="test-host",
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


def test_checkpoint_workspace_state_retries_atomic_replace_after_access_denied(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    claim_workspace(paths)

    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    started = utcnow_text()

    original_replace = os.replace
    attempts = {"count": 0}

    def flaky_replace(source, target):
        if Path(target) == paths.lease_metadata_path and attempts["count"] == 0:
            attempts["count"] += 1
            error = PermissionError(13, "Access is denied")
            error.winerror = 5
            raise error
        return original_replace(source, target)

    monkeypatch.setattr("data_engine.helpers.polars.os.replace", flaky_replace)

    checkpoint_workspace_state(
        paths,
        ledger,
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

    metadata = read_lease_metadata(paths)

    assert attempts["count"] == 1
    assert metadata is not None
    assert metadata["workspace_id"] == "default"
