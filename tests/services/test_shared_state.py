from __future__ import annotations

import os
import multiprocessing
from pathlib import Path
import threading

import polars as pl
import pytest

import data_engine.runtime.shared_state as shared_state_module
from data_engine.domain.source_state import SourceSignature
from data_engine.platform.processes import ProcessIdentity
from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.runtime.shared_state import (
    WorkspaceLeaseLostError,
    WorkspaceStateCorruptError,
    checkpoint_workspace_state as _runtime_checkpoint_workspace_state,
    claim_workspace,
    daemon_process_lease_identity,
    daemon_process_lease_metadata,
    hydrate_local_runtime_state,
    initialize_workspace_state,
    read_control_request,
    read_lease_metadata,
    read_runtime_snapshot_generation,
    recover_stale_workspace,
    remove_control_request,
    release_workspace,
    reset_workspace_state,
    resolve_workspace_bundle,
    write_lease_metadata as _runtime_write_lease_metadata,
    workspace_lease_operation,
    write_control_request,
)
from data_engine.services.workspace_io import WorkspaceIoLayer

from tests.services.support import resolve_workspace_paths


_TEST_CONTAINMENT_NONCE = "a" * 64


def _test_process_identity(pid: int) -> ProcessIdentity:
    return ProcessIdentity(
        pid=pid,
        start_key=f"test-start-{pid}",
        executable_path="/test/python",
        process_group_id=None if os.name == "nt" else pid,
        process_session_id=pid,
    )


def _owner_process_kwargs(pid: int) -> dict[str, object]:
    return {
        "process_identity": _test_process_identity(pid),
        "containment_nonce": _TEST_CONTAINMENT_NONCE,
    }


def _committed_artifact_paths(paths):
    generation_id = read_runtime_snapshot_generation(paths)
    assert generation_id is not None
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None
    generation_dir = bundle.snapshot_generations_dir / generation_id
    return {
        "generation_id": generation_id,
        "runs": generation_dir / "runs.parquet",
        "step_runs": generation_dir / "step_runs.parquet",
        "logs": generation_dir / "logs.parquet",
        "file_state": generation_dir / "file_state.parquet",
    }


def _claim(paths) -> str:
    initialize_workspace_state(paths)
    lease_token = claim_workspace(paths)
    assert isinstance(lease_token, str)
    return lease_token


def _current_lease_token(paths) -> str:
    initialize_workspace_state(paths)
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None
    if bundle.state == "available":
        return _claim(paths)
    assert bundle.lease_token is not None
    return bundle.lease_token


def _current_bundle(paths):
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None
    return bundle


def _checkpoint_workspace_state(paths, ledger, **kwargs):
    pid = kwargs["pid"]
    kwargs = {**_owner_process_kwargs(pid), **kwargs}
    return _runtime_checkpoint_workspace_state(paths, ledger, **kwargs)


def checkpoint_workspace_state(paths, ledger, **kwargs):
    """Checkpoint through the current explicit test lease."""
    return _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=_current_lease_token(paths),
        **kwargs,
    )


def write_lease_metadata(paths, **kwargs):
    """Write test lease metadata with one complete process identity."""
    pid = kwargs["pid"]
    kwargs = {**_owner_process_kwargs(pid), **kwargs}
    return _runtime_write_lease_metadata(paths, **kwargs)


def _claim_in_subprocess(paths, start_event, result_queue) -> None:
    start_event.wait()
    result_queue.put(_claim_workspace_result(paths))


def _claim_workspace_result(paths) -> str | None:
    return claim_workspace(paths)


def test_initialize_claim_and_release_workspace_markers(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    initialize_workspace_state(paths)

    assert (paths.available_markers_dir / "default").exists()
    assert not (paths.leased_markers_dir / "default").exists()

    lease_token = _claim(paths)
    assert not (paths.available_markers_dir / "default").exists()
    assert (paths.leased_markers_dir / f"default__{lease_token}").exists()

    release_workspace(paths, lease_token=lease_token)
    assert (paths.available_markers_dir / "default").exists()
    assert not (paths.leased_markers_dir / f"default__{lease_token}").exists()


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
    assert daemon_process_lease_identity(metadata).process_identity == _test_process_identity(101)
    assert metadata["containment_nonce"] == _TEST_CONTAINMENT_NONCE

    target_ledger = RuntimeCacheLedger(app_root / "artifacts" / "workspaces" / "default" / "runtime_state" / "second.sqlite")
    hydrate_local_runtime_state(paths, target_ledger)
    assert [run.run_id for run in target_ledger.runs.list()] == ["run-1"]
    assert [entry.run_id for entry in target_ledger.logs.list(flow_name="demo")] == ["run-1"]


def test_daemon_process_lease_metadata_round_trips_nullable_grouping() -> None:
    identity = ProcessIdentity(
        pid=101,
        start_key="windows-start-key",
        executable_path="c:/python/python.exe",
        process_group_id=None,
        process_session_id=None,
    )

    metadata = daemon_process_lease_metadata(identity, _TEST_CONTAINMENT_NONCE)

    assert daemon_process_lease_identity(metadata).process_identity == identity
    assert daemon_process_lease_identity(metadata).containment_nonce == _TEST_CONTAINMENT_NONCE


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    (
        ("pid", 0),
        ("process_start_key", ""),
        ("process_executable_path", ""),
        ("process_group_id", -1),
        ("process_session_id", True),
        ("containment_nonce", "A" * 64),
    ),
)
def test_daemon_process_lease_identity_rejects_malformed_fields(field_name, bad_value) -> None:
    metadata = daemon_process_lease_metadata(_test_process_identity(101), _TEST_CONTAINMENT_NONCE)
    metadata[field_name] = bad_value

    with pytest.raises(WorkspaceStateCorruptError, match="invalid process identity"):
        daemon_process_lease_identity(metadata)


def test_daemon_process_lease_identity_rejects_missing_fields() -> None:
    metadata = daemon_process_lease_metadata(_test_process_identity(101), _TEST_CONTAINMENT_NONCE)
    metadata.pop("process_start_key")

    with pytest.raises(WorkspaceStateCorruptError, match="missing required process identity fields"):
        daemon_process_lease_identity(metadata)


@pytest.mark.parametrize(
    ("process_identity", "containment_nonce", "error_text"),
    (
        (_test_process_identity(202), _TEST_CONTAINMENT_NONCE, "must match"),
        (_test_process_identity(101), "bad", "64 lowercase hexadecimal"),
    ),
)
def test_owner_write_rejects_incomplete_process_contract(
    tmp_path,
    process_identity,
    containment_nonce,
    error_text,
) -> None:
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    lease_token = _claim(paths)
    started = utcnow_text()

    with pytest.raises(ValueError, match=error_text):
        _runtime_write_lease_metadata(
            paths,
            lease_token=lease_token,
            workspace_id=paths.workspace_id,
            machine_id="machine-a",
            host_name="host-a",
            daemon_id="daemon-a",
            pid=101,
            process_identity=process_identity,
            containment_nonce=containment_nonce,
            status="idle",
            started_at_utc=started,
            last_checkpoint_at_utc=started,
            app_version="test",
        )

    assert _current_bundle(paths).lease_metadata_path.exists() is False


def test_persisted_owner_row_without_process_identity_fails_closed(tmp_path) -> None:
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    lease_token = _claim(paths)
    bundle = _current_bundle(paths)
    pl.DataFrame(
        [
            {
                "workspace_id": paths.workspace_id,
                "lease_token": lease_token,
                "pid": 101,
                "status": "idle",
            }
        ]
    ).write_parquet(bundle.lease_metadata_path)

    with pytest.raises(WorkspaceStateCorruptError, match="missing required process identity fields"):
        read_lease_metadata(paths)


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
            assert not (_current_bundle(paths).snapshot_generations_dir / selected_generation).exists()
        return original_read(path, **kwargs)

    monkeypatch.setattr(shared_state_module, "_read_parquet_with_retries", publish_during_read)
    target = RuntimeCacheLedger(tmp_path / "target.sqlite")

    assert hydrate_local_runtime_state(paths, target) is True
    assert raced["value"] is True
    assert [run.run_id for run in target.runs.list()] == ["run-1"]
    assert len(tuple(_current_bundle(paths).snapshot_generations_dir.iterdir())) == 3


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
        if path.parent == _current_bundle(paths).snapshot_generations_dir and len(path.name) == 32:
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
    manifest_generation_dir = _current_bundle(paths).snapshot_generations_dir / manifest_generation
    os.utime(manifest_generation_dir, ns=(1, 1))
    synthetic_generations = tuple(f"{index:032x}" for index in range(1, 4))
    for index, generation_id in enumerate(synthetic_generations, start=2):
        generation_dir = _current_bundle(paths).snapshot_generations_dir / generation_id
        generation_dir.mkdir()
        os.utime(generation_dir, ns=(index, index))

    shared_state_module._garbage_collect_snapshot_generations(  # noqa: SLF001 - GC invariant
        paths,
        lease_token=_current_lease_token(paths),
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
            lease_token=_current_lease_token(paths),
            workspace_id="default",
            machine_id="machine-a",
            host_name="test-host",
            daemon_id="daemon-a",
            pid=101,
            **_owner_process_kwargs(101),
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
    assert len(tuple(_current_bundle(paths).snapshot_generations_dir.iterdir())) == 1

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
        "lease_token": _current_lease_token(paths),
        "workspace_id": "default",
        "machine_id": "machine-a",
        "host_name": "test-host",
        "daemon_id": "daemon-a",
        "pid": 101,
        **_owner_process_kwargs(101),
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


def test_recover_stale_workspace_fences_old_lease_and_restores_available(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    lease_token = _claim(paths)

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
    write_lease_metadata(
        paths,
        lease_token=lease_token,
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

    recovered = recover_stale_workspace(
        paths,
        lease_token=lease_token,
        machine_id="machine-b",
        stale_after_seconds=1.0,
    )

    assert recovered is True
    assert (paths.available_markers_dir / "default").exists()
    assert not (paths.leased_markers_dir / f"default__{lease_token}").exists()
    assert any(paths.stale_markers_dir.iterdir())


def test_recover_stale_workspace_without_reclaim_restores_available_marker(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    workspace_root = tmp_path / "shared" / "default"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    initialize_workspace_state(paths)
    lease_token = _claim(paths)

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
    write_lease_metadata(
        paths,
        lease_token=lease_token,
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

    recovered = recover_stale_workspace(
        paths,
        lease_token=lease_token,
        machine_id="machine-b",
        stale_after_seconds=1.0,
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
        if Path(path) == _current_bundle(paths).lease_metadata_path and attempts["count"] == 0:
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
        if Path(target) == _current_bundle(paths).lease_metadata_path and attempts["count"] == 0:
            attempts["count"] += 1
            error = PermissionError(13, "Access is denied")
            error.winerror = 5
            raise error
        return original_replace(source, target)

    monkeypatch.setattr("data_engine.runtime.shared_state.os.replace", flaky_replace)

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


def test_first_use_claim_is_serialized_between_threads(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    start = threading.Barrier(3)
    results: list[str | None] = []

    def claim() -> None:
        start.wait()
        results.append(claim_workspace(paths))

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=3.0)

    assert all(not thread.is_alive() for thread in threads)
    assert sum(isinstance(result, str) for result in results) == 1
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.state == "leased"


def test_first_use_claim_is_serialized_between_processes(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    result_queue = context.Queue()
    processes = [
        context.Process(target=_claim_in_subprocess, args=(paths, start_event, result_queue))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start_event.set()
    results = [result_queue.get(timeout=10.0) for _ in processes]
    for process in processes:
        process.join(timeout=10.0)

    assert all(process.exitcode == 0 for process in processes)
    assert sum(isinstance(result, str) for result in results) == 1
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.state == "leased"


def test_same_root_workspace_aliases_fail_closed_instead_of_claiming_twice(tmp_path):
    workspace_root = tmp_path / "workspace"
    paths_a = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="a")
    paths_ab = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="a__b")
    initialize_workspace_state(paths_a)
    token_a = claim_workspace(paths_a)
    assert isinstance(token_a, str)
    assert read_lease_metadata(paths_a) == {
        "workspace_id": "a",
        "lease_token": token_a,
        "status": "claiming",
    }
    with pytest.raises(WorkspaceStateCorruptError):
        initialize_workspace_state(paths_ab)
    with pytest.raises(WorkspaceStateCorruptError):
        claim_workspace(paths_ab)
    release_workspace(paths_a, lease_token=token_a)
    assert resolve_workspace_bundle(paths_a).state == "available"


@pytest.mark.parametrize(
    "marker_name",
    (
        "workspace",
        "workspace__bad",
        ".recovering__workspace",
    ),
)
def test_malformed_or_tokenless_lease_markers_fail_closed(tmp_path, marker_name):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    initialize_workspace_state(paths)
    (paths.available_markers_dir / paths.workspace_id).rmdir()
    (paths.leased_markers_dir / marker_name).mkdir()

    with pytest.raises(WorkspaceStateCorruptError):
        resolve_workspace_bundle(paths)
    with pytest.raises(WorkspaceStateCorruptError):
        claim_workspace(paths)


def test_broken_available_symlink_fails_closed(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    initialize_workspace_state(paths)
    available = paths.available_markers_dir / paths.workspace_id
    available.rmdir()
    try:
        available.symlink_to(tmp_path / "missing", target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"Symlink creation is unavailable: {exc}")

    with pytest.raises(WorkspaceStateCorruptError):
        resolve_workspace_bundle(paths)
    with pytest.raises(WorkspaceStateCorruptError):
        claim_workspace(paths)


@pytest.mark.parametrize("marker_kind", ("available", "leased", "recovery"))
def test_directory_junction_markers_fail_closed(tmp_path, monkeypatch, marker_kind):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    initialize_workspace_state(paths)
    lease_token = None
    if marker_kind == "available":
        marker = paths.available_markers_dir / paths.workspace_id
    else:
        lease_token = claim_workspace(paths)
        assert isinstance(lease_token, str)
        marker = paths.leased_markers_dir / f"{paths.workspace_id}__{lease_token}"
        if marker_kind == "recovery":
            recovery_marker = paths.leased_markers_dir / f".recovering__{paths.workspace_id}__{lease_token}"
            marker.rename(recovery_marker)
            marker = recovery_marker
    original_is_junction = Path.is_junction
    monkeypatch.setattr(Path, "is_junction", lambda candidate: candidate == marker or original_is_junction(candidate))

    with pytest.raises(WorkspaceStateCorruptError):
        resolve_workspace_bundle(paths)
    if marker_kind == "leased":
        with pytest.raises(WorkspaceStateCorruptError):
            shared_state_module._assert_exact_lease_path(paths, lease_token)  # noqa: SLF001


def test_redirected_snapshot_root_cannot_write_or_gc_external_generations(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token = _claim(paths)
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None
    external_root = tmp_path / "external-snapshots"
    sentinel_names = ("a" * 32, "b" * 32)
    for sentinel_name in sentinel_names:
        sentinel_dir = external_root / sentinel_name
        sentinel_dir.mkdir(parents=True)
        (sentinel_dir / "sentinel.txt").write_text(sentinel_name, encoding="utf-8")
    try:
        bundle.snapshot_generations_dir.symlink_to(external_root, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"Symlink creation is unavailable: {exc}")
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started = utcnow_text()

    with pytest.raises(WorkspaceStateCorruptError):
        _checkpoint_workspace_state(
            paths,
            ledger,
            lease_token=lease_token,
            workspace_id=paths.workspace_id,
            machine_id="machine-a",
            host_name="host-a",
            daemon_id="daemon-a",
            pid=101,
            status="idle",
            started_at_utc=started,
            last_checkpoint_at_utc=started,
            app_version="test",
        )
    with pytest.raises(WorkspaceStateCorruptError):
        shared_state_module._garbage_collect_snapshot_generations(  # noqa: SLF001
            paths,
            lease_token=lease_token,
            committed_generation_id="c" * 32,
        )

    assert {entry.name for entry in external_root.iterdir()} == set(sentinel_names)
    for sentinel_name in sentinel_names:
        assert (external_root / sentinel_name / "sentinel.txt").read_text(encoding="utf-8") == sentinel_name
    assert not bundle.snapshot_manifest_path.exists()


@pytest.mark.parametrize("redirect_kind", ("lease_metadata", "manifest", "generation", "artifact"))
def test_nested_bundle_read_redirects_fail_closed(tmp_path, redirect_kind):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started = utcnow_text()
    generation_id = _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="test",
    )
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None
    generation_dir = bundle.snapshot_generations_dir / generation_id
    if redirect_kind == "lease_metadata":
        redirected_path = bundle.lease_metadata_path
        external_path = tmp_path / "external-lease.parquet"
    elif redirect_kind == "manifest":
        redirected_path = bundle.snapshot_manifest_path
        external_path = tmp_path / "external-manifest.json"
    elif redirect_kind == "generation":
        redirected_path = generation_dir
        external_path = tmp_path / "external-generation"
    else:
        redirected_path = generation_dir / "runs.parquet"
        external_path = tmp_path / "external-runs.parquet"
    redirected_path.rename(external_path)
    try:
        redirected_path.symlink_to(external_path, target_is_directory=external_path.is_dir())
    except OSError as exc:
        external_path.rename(redirected_path)
        pytest.skip(f"Symlink creation is unavailable: {exc}")

    with pytest.raises(WorkspaceStateCorruptError):
        if redirect_kind == "lease_metadata":
            read_lease_metadata(paths)
        elif redirect_kind == "manifest":
            read_runtime_snapshot_generation(paths, lease_token=lease_token)
        else:
            hydrate_local_runtime_state(paths, RuntimeCacheLedger(tmp_path / "hydrated.sqlite"))

    assert external_path.exists()


def test_multiple_valid_lease_markers_fail_closed(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    initialize_workspace_state(paths)
    (paths.available_markers_dir / paths.workspace_id).rmdir()
    for token in ("a" * 32, "b" * 32):
        (paths.leased_markers_dir / f"{paths.workspace_id}__{token}").mkdir()

    with pytest.raises(WorkspaceStateCorruptError):
        resolve_workspace_bundle(paths)


def test_workspace_reset_rejects_conflicting_topology_before_deleting_snapshot(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started = utcnow_text()
    generation_id = _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="test",
    )
    conflict_token = "b" * 32 if lease_token != "b" * 32 else "c" * 32
    (paths.leased_markers_dir / f"{paths.workspace_id}__{conflict_token}").mkdir()

    with pytest.raises(WorkspaceStateCorruptError):
        reset_workspace_state(paths, lease_token=lease_token)

    owner_marker = paths.leased_markers_dir / f"{paths.workspace_id}__{lease_token}"
    assert (owner_marker / "snapshot_manifest.json").is_file()
    assert (owner_marker / "snapshots" / generation_id).is_dir()


def test_snapshot_survives_release_and_new_token_claim(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token_a = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started = utcnow_text()
    generation_id = _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="test",
    )

    release_workspace(paths, lease_token=lease_token_a)
    assert read_runtime_snapshot_generation(paths) == generation_id
    lease_token_b = claim_workspace(paths)
    assert isinstance(lease_token_b, str) and lease_token_b != lease_token_a
    assert read_runtime_snapshot_generation(paths, lease_token=lease_token_b) == generation_id


def test_observer_retries_snapshot_read_across_bundle_rename(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token_a = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started = utcnow_text()
    generation_id = _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="test",
    )
    original_read = shared_state_module._read_snapshot_manifest  # noqa: SLF001
    renamed = {"value": False}

    def rename_during_read(path):
        if not renamed["value"]:
            renamed["value"] = True
            release_workspace(paths, lease_token=lease_token_a)
            assert claim_workspace(paths) is not None
        return original_read(path)

    monkeypatch.setattr(shared_state_module, "_read_snapshot_manifest", rename_during_read)

    assert read_runtime_snapshot_generation(paths) == generation_id
    assert renamed["value"] is True


def test_recovery_restores_lease_when_heartbeat_commits_before_fence(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    old_time = "2000-01-01T00:00:00+00:00"
    _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    write_lease_metadata(
        paths,
        lease_token=lease_token,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    owner_bundle = resolve_workspace_bundle(paths)
    assert owner_bundle is not None
    original_rename = Path.rename
    heartbeat_written = {"value": False}

    def heartbeat_before_recovery_rename(source, target):
        if source == owner_bundle.root and not heartbeat_written["value"]:
            heartbeat_written["value"] = True
            write_lease_metadata(
                paths,
                lease_token=lease_token,
                workspace_id=paths.workspace_id,
                machine_id="machine-a",
                host_name="host-a",
                daemon_id="daemon-a",
                pid=101,
                status="idle",
                started_at_utc=old_time,
                last_checkpoint_at_utc=utcnow_text(),
                app_version="test",
            )
        return original_rename(source, target)

    monkeypatch.setattr(Path, "rename", heartbeat_before_recovery_rename)

    assert recover_stale_workspace(
        paths,
        lease_token=lease_token,
        machine_id="machine-b",
        stale_after_seconds=1.0,
    ) is False
    assert resolve_workspace_bundle(paths).lease_token == lease_token
    assert heartbeat_written["value"] is True


def test_stale_owner_writes_and_release_cannot_mutate_successor_bundle(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token_a = _claim(paths)
    ledger_a = RuntimeCacheLedger(tmp_path / "a.sqlite")
    old_time = "2000-01-01T00:00:00+00:00"
    _checkpoint_workspace_state(
        paths,
        ledger_a,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    write_lease_metadata(
        paths,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
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
    lease_token_b = claim_workspace(paths)
    assert isinstance(lease_token_b, str)
    ledger_b = RuntimeCacheLedger(tmp_path / "b.sqlite")
    ledger_b.logs.append(level="INFO", message="successor", created_at_utc=utcnow_text())
    generation_b = _checkpoint_workspace_state(
        paths,
        ledger_b,
        lease_token=lease_token_b,
        workspace_id=paths.workspace_id,
        machine_id="machine-b",
        host_name="host-b",
        daemon_id="daemon-b",
        pid=202,
        status="idle",
        started_at_utc=utcnow_text(),
        last_checkpoint_at_utc=utcnow_text(),
        app_version="test",
    )
    metadata_b = read_lease_metadata(paths)

    with pytest.raises(WorkspaceLeaseLostError):
        _checkpoint_workspace_state(
            paths,
            ledger_a,
            lease_token=lease_token_a,
            workspace_id=paths.workspace_id,
            machine_id="machine-a",
            host_name="host-a",
            daemon_id="daemon-a",
            pid=101,
            status="idle",
            started_at_utc=old_time,
            last_checkpoint_at_utc=utcnow_text(),
            app_version="test",
        )
    with pytest.raises(WorkspaceLeaseLostError):
        write_lease_metadata(
            paths,
            lease_token=lease_token_a,
            workspace_id=paths.workspace_id,
            machine_id="machine-a",
            host_name="host-a",
            daemon_id="daemon-a",
            pid=101,
            status="idle",
            started_at_utc=old_time,
            last_checkpoint_at_utc=utcnow_text(),
            app_version="test",
        )
    with pytest.raises(WorkspaceLeaseLostError):
        release_workspace(paths, lease_token=lease_token_a)

    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token == lease_token_b
    assert read_runtime_snapshot_generation(paths) == generation_b
    assert read_lease_metadata(paths) == metadata_b
    assert not (paths.leased_markers_dir / f"{paths.workspace_id}__{lease_token_a}").exists()


def test_open_parquet_write_cannot_follow_recovered_bundle(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token_a = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    old_time = "2000-01-01T00:00:00+00:00"
    _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    write_lease_metadata(
        paths,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    original_write = pl.DataFrame.write_parquet
    successor_token: dict[str, str] = {}
    raced = {"value": False}

    def recover_after_temp_write(frame, path, *args, **kwargs):
        result = original_write(frame, path, *args, **kwargs)
        if not raced["value"]:
            raced["value"] = True
            assert recover_stale_workspace(
                paths,
                lease_token=lease_token_a,
                machine_id="machine-b",
                stale_after_seconds=1.0,
            ) is True
            claimed = claim_workspace(paths)
            assert isinstance(claimed, str)
            successor_token["value"] = claimed
        return result

    monkeypatch.setattr(pl.DataFrame, "write_parquet", recover_after_temp_write)

    with pytest.raises(WorkspaceLeaseLostError):
        write_lease_metadata(
            paths,
            lease_token=lease_token_a,
            workspace_id=paths.workspace_id,
            machine_id="machine-a",
            host_name="host-a",
            daemon_id="daemon-a",
            pid=101,
            status="idle",
            started_at_utc=old_time,
            last_checkpoint_at_utc=utcnow_text(),
            app_version="test",
        )

    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token == successor_token["value"]
    assert not (paths.leased_markers_dir / f"{paths.workspace_id}__{lease_token_a}").exists()
    assert not any(
        entry.name.endswith(".tmp") and lease_token_a in entry.name
        for entry in bundle.root.rglob("*")
    )


def test_generation_staging_write_cannot_commit_manifest_into_successor(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token_a = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    old_time = "2000-01-01T00:00:00+00:00"
    baseline_generation = _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    original_write = pl.DataFrame.write_parquet
    successor_token: dict[str, str] = {}
    raced = {"value": False}

    def recover_during_generation_write(frame, path, *args, **kwargs):
        result = original_write(frame, path, *args, **kwargs)
        candidate = Path(path)
        if not raced["value"] and candidate.parent.parent.name == "snapshots":
            raced["value"] = True
            assert recover_stale_workspace(
                paths,
                lease_token=lease_token_a,
                machine_id="machine-b",
                stale_after_seconds=1.0,
            ) is True
            claimed = claim_workspace(paths)
            assert isinstance(claimed, str)
            successor_token["value"] = claimed
        return result

    monkeypatch.setattr(pl.DataFrame, "write_parquet", recover_during_generation_write)

    with pytest.raises(WorkspaceLeaseLostError):
        _checkpoint_workspace_state(
            paths,
            ledger,
            lease_token=lease_token_a,
            workspace_id=paths.workspace_id,
            machine_id="machine-a",
            host_name="host-a",
            daemon_id="daemon-a",
            pid=101,
            status="idle",
            started_at_utc=old_time,
            last_checkpoint_at_utc=old_time,
            app_version="test",
        )

    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None and bundle.lease_token == successor_token["value"]
    assert read_runtime_snapshot_generation(paths) == baseline_generation
    assert not (bundle.snapshot_generations_dir / baseline_generation).is_symlink()
    assert not any(
        entry.name.endswith(".tmp") and lease_token_a in entry.name
        for entry in bundle.root.rglob("*")
    )


def test_stale_token_entering_gc_cannot_delete_successor_generations(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token_a = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    old_time = "2000-01-01T00:00:00+00:00"
    committed_generation = _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    write_lease_metadata(
        paths,
        lease_token=lease_token_a,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=old_time,
        last_checkpoint_at_utc=old_time,
        app_version="test",
    )
    bundle_a = resolve_workspace_bundle(paths)
    assert bundle_a is not None
    generation_names = {committed_generation, *(str(index) * 32 for index in range(1, 6))}
    for generation_name in generation_names:
        (bundle_a.snapshot_generations_dir / generation_name).mkdir(parents=True, exist_ok=True)
    original_read_manifest = shared_state_module._read_snapshot_manifest  # noqa: SLF001
    successor_token: dict[str, str] = {}
    raced = {"value": False}

    def recover_before_gc_deletion(path):
        if not raced["value"]:
            raced["value"] = True
            assert recover_stale_workspace(
                paths,
                lease_token=lease_token_a,
                machine_id="machine-b",
                stale_after_seconds=1.0,
            ) is True
            claimed = claim_workspace(paths)
            assert isinstance(claimed, str)
            successor_token["value"] = claimed
        return original_read_manifest(path)

    monkeypatch.setattr(shared_state_module, "_read_snapshot_manifest", recover_before_gc_deletion)

    with pytest.raises(WorkspaceLeaseLostError):
        shared_state_module._garbage_collect_snapshot_generations(  # noqa: SLF001
            paths,
            lease_token=lease_token_a,
            committed_generation_id=committed_generation,
        )

    bundle_b = resolve_workspace_bundle(paths)
    assert bundle_b is not None and bundle_b.lease_token == successor_token["value"]
    assert {entry.name for entry in bundle_b.snapshot_generations_dir.iterdir()} == generation_names


def test_release_permission_error_preserves_exact_owner(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token = _claim(paths)
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None
    original_rename = Path.rename

    def deny_release(source, target):
        if source == bundle.root and target == paths.available_markers_dir / paths.workspace_id:
            raise PermissionError("rename denied")
        return original_rename(source, target)

    monkeypatch.setattr(Path, "rename", deny_release)

    with pytest.raises(PermissionError, match="rename denied"):
        release_workspace(paths, lease_token=lease_token)
    current = resolve_workspace_bundle(paths)
    assert current is not None and current.lease_token == lease_token


def test_checkpoint_succeeds_when_post_commit_generation_gc_is_access_denied(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started = utcnow_text()
    for index in range(4):
        ledger.logs.append(level="INFO", message=f"checkpoint-{index}", created_at_utc=started)
        _checkpoint_workspace_state(
            paths,
            ledger,
            lease_token=lease_token,
            workspace_id=paths.workspace_id,
            machine_id="machine-a",
            host_name="host-a",
            daemon_id="daemon-a",
            pid=101,
            status="idle",
            started_at_utc=started,
            last_checkpoint_at_utc=started,
            app_version="test",
        )
    original_rmtree = shared_state_module.shutil.rmtree
    denied_paths: list[Path] = []

    def deny_committed_generation_cleanup(path, *args, **kwargs):
        candidate = Path(path)
        if shared_state_module._GENERATION_ID_PATTERN.fullmatch(candidate.name):  # noqa: SLF001
            denied_paths.append(candidate)
            error = PermissionError(13, "Access is denied")
            error.winerror = 5
            raise error
        return original_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(shared_state_module.shutil, "rmtree", deny_committed_generation_cleanup)
    ledger.logs.append(level="INFO", message="latest", created_at_utc=started)

    latest_generation = _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="test",
    )

    assert denied_paths
    assert read_runtime_snapshot_generation(paths, lease_token=lease_token) == latest_generation
    assert resolve_workspace_bundle(paths).lease_token == lease_token


def test_checkpoint_succeeds_when_post_commit_generation_listing_is_access_denied(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token = _claim(paths)
    ledger = RuntimeCacheLedger(paths.runtime_cache_db_path)
    started = utcnow_text()
    original_iterdir = Path.iterdir
    denied = {"value": False}
    bundle = resolve_workspace_bundle(paths)
    assert bundle is not None
    snapshots_root = bundle.snapshot_generations_dir

    def deny_generation_listing(path):
        if path == snapshots_root:
            denied["value"] = True
            error = PermissionError(13, "Access is denied")
            error.winerror = 5
            raise error
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", deny_generation_listing)

    latest_generation = _checkpoint_workspace_state(
        paths,
        ledger,
        lease_token=lease_token,
        workspace_id=paths.workspace_id,
        machine_id="machine-a",
        host_name="host-a",
        daemon_id="daemon-a",
        pid=101,
        status="idle",
        started_at_utc=started,
        last_checkpoint_at_utc=started,
        app_version="test",
    )

    assert denied["value"] is True
    assert read_runtime_snapshot_generation(paths, lease_token=lease_token) == latest_generation
    assert resolve_workspace_bundle(paths).lease_token == lease_token


def test_topology_lock_releases_after_guard_exception(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id="workspace")
    lease_token = _claim(paths)

    with pytest.raises(RuntimeError, match="boom"):
        with workspace_lease_operation(paths, lease_token=lease_token):
            raise RuntimeError("boom")

    release_workspace(paths, lease_token=lease_token)
    assert resolve_workspace_bundle(paths).state == "available"
