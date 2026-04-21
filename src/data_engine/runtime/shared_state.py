"""Workspace coordination and runtime snapshot helpers."""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
import shutil
from uuid import uuid4
from typing import Any, Protocol

import polars as pl

from data_engine.helpers.polars import write_parquet_atomic
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.ledger_models import (
    PersistedFileState,
    PersistedLogEntry,
    PersistedRun,
    PersistedStepRun,
)
from data_engine.runtime.runtime_db import parse_utc_text


class _RunRepository(Protocol):
    def list(self) -> tuple[PersistedRun, ...]: ...


class _StepOutputRepository(Protocol):
    def list_for_run(self, run_id: str) -> tuple[PersistedStepRun, ...]: ...


class _LogRepository(Protocol):
    def list(self) -> tuple[PersistedLogEntry, ...]: ...


class _SourceSignatureRepository(Protocol):
    def list_file_states(self) -> tuple[PersistedFileState, ...]: ...


class _SnapshotRepository(Protocol):
    def replace(
        self,
        *,
        runs: tuple[PersistedRun, ...],
        step_runs: tuple[PersistedStepRun, ...],
        logs: tuple[PersistedLogEntry, ...],
        file_states: tuple[PersistedFileState, ...],
    ) -> None: ...


class RuntimeSnapshotStore(Protocol):
    """Minimal runtime snapshot surface used by workspace coordination."""

    runs: _RunRepository
    step_outputs: _StepOutputRepository
    logs: _LogRepository
    source_signatures: _SourceSignatureRepository
    snapshots: _SnapshotRepository


_LEASE_METADATA_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_generation_id": pl.String,
    "workspace_id": pl.String,
    "machine_id": pl.String,
    "host_name": pl.String,
    "daemon_id": pl.String,
    "pid": pl.Int64,
    "status": pl.String,
    "last_checkpoint_at_utc": pl.String,
    "started_at_utc": pl.String,
    "app_version": pl.String,
}

_CONTROL_REQUEST_SCHEMA: dict[str, pl.DataType] = {
    "workspace_id": pl.String,
    "requester_machine_id": pl.String,
    "requester_host_name": pl.String,
    "requester_pid": pl.Int64,
    "requester_client_kind": pl.String,
    "requested_at_utc": pl.String,
}

_RUNS_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_generation_id": pl.String,
    "run_id": pl.String,
    "flow_name": pl.String,
    "group_name": pl.String,
    "source_path": pl.String,
    "status": pl.String,
    "started_at_utc": pl.String,
    "finished_at_utc": pl.String,
    "error_text": pl.String,
}

_STEP_RUNS_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_generation_id": pl.String,
    "id": pl.Int64,
    "run_id": pl.String,
    "flow_name": pl.String,
    "step_label": pl.String,
    "status": pl.String,
    "started_at_utc": pl.String,
    "finished_at_utc": pl.String,
    "elapsed_ms": pl.Int64,
    "error_text": pl.String,
    "output_path": pl.String,
}

_LOGS_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_generation_id": pl.String,
    "id": pl.Int64,
    "run_id": pl.String,
    "flow_name": pl.String,
    "step_label": pl.String,
    "level": pl.String,
    "message": pl.String,
    "created_at_utc": pl.String,
}

_FILE_STATE_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_generation_id": pl.String,
    "flow_name": pl.String,
    "source_path": pl.String,
    "mtime_ns": pl.Int64,
    "size_bytes": pl.Int64,
    "last_success_run_id": pl.String,
    "last_success_at_utc": pl.String,
    "last_status": pl.String,
    "last_error_text": pl.String,
}

_PARQUET_READ_RETRIES = 3


def initialize_workspace_state(paths: WorkspacePaths) -> None:
    """Ensure the shared-state folder tree and initial availability marker exist."""
    for directory in (
        paths.workspace_state_dir,
        paths.available_markers_dir,
        paths.leased_markers_dir,
        paths.stale_markers_dir,
        paths.lease_metadata_dir,
        paths.control_requests_dir,
        paths.shared_state_dir / "runs",
        paths.shared_state_dir / "step_runs",
        paths.shared_state_dir / "logs",
        paths.shared_state_dir / "file_state",
    ):
        directory.mkdir(parents=True, exist_ok=True)
    available = paths.available_markers_dir / paths.workspace_id
    leased = paths.leased_markers_dir / paths.workspace_id
    if available.exists() and leased.exists():
        raise RuntimeError(f"Workspace {paths.workspace_id!r} has invalid marker state: both available and leased exist.")
    if not available.exists() and not leased.exists():
        available.mkdir(parents=True, exist_ok=True)


def claim_workspace(paths: WorkspacePaths) -> bool:
    """Try to claim the workspace by renaming available marker to leased."""
    initialize_workspace_state(paths)
    available = paths.available_markers_dir / paths.workspace_id
    leased = paths.leased_markers_dir / paths.workspace_id
    if leased.exists() and not available.exists():
        return False
    if not available.exists():
        available.mkdir(parents=True, exist_ok=True)
    try:
        available.rename(leased)
    except FileNotFoundError:
        return False
    except OSError:
        return False
    return True


def release_workspace(paths: WorkspacePaths) -> None:
    """Return the claimed workspace marker to available state."""
    available = paths.available_markers_dir / paths.workspace_id
    leased = paths.leased_markers_dir / paths.workspace_id
    if leased.exists():
        if available.exists():
            shutil.rmtree(available)
        leased.rename(available)


def lease_is_stale(paths: WorkspacePaths, *, stale_after_seconds: float) -> bool:
    """Return whether the current lease metadata is stale enough for recovery."""
    metadata = read_lease_metadata(paths)
    if metadata is None:
        return True
    parsed = parse_utc_text(str(metadata.get("last_checkpoint_at_utc")))
    if parsed is None:
        return True
    return datetime.now(UTC) - parsed > timedelta(seconds=max(stale_after_seconds, 0.0))


def recover_stale_workspace(
    paths: WorkspacePaths,
    *,
    machine_id: str,
    stale_after_seconds: float,
    reclaim: bool = True,
) -> bool:
    """Recover one stale workspace by quarantining the leased marker and optionally reclaiming it."""
    leased = paths.leased_markers_dir / paths.workspace_id
    if not leased.exists():
        return False
    if not lease_is_stale(paths, stale_after_seconds=stale_after_seconds):
        return False
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    stale_bundle = paths.stale_markers_dir / f"{paths.workspace_id}__{timestamp}__{machine_id}"
    stale_bundle.parent.mkdir(parents=True, exist_ok=True)
    try:
        leased.rename(stale_bundle)
    except OSError:
        return False
    if paths.lease_metadata_path.exists():
        (stale_bundle / "metadata").mkdir(parents=True, exist_ok=True)
        try:
            paths.lease_metadata_path.rename(stale_bundle / "metadata" / "lease.parquet")
        except OSError:
            pass
    available = paths.available_markers_dir / paths.workspace_id
    if not available.exists():
        available.mkdir(parents=True, exist_ok=True)
    if not reclaim:
        return True
    return claim_workspace(paths)


def checkpoint_workspace_state(
    paths: WorkspacePaths,
    ledger: RuntimeSnapshotStore,
    *,
    workspace_id: str,
    machine_id: str,
    daemon_id: str,
    pid: int,
    status: str,
    started_at_utc: str,
    last_checkpoint_at_utc: str,
    app_version: str | None,
) -> None:
    """Write shared runtime snapshots and workspace lease metadata."""
    initialize_workspace_state(paths)
    snapshot_generation_id = uuid4().hex
    _write_shared_runtime_snapshot(paths, ledger, snapshot_generation_id=snapshot_generation_id)
    _write_lease_metadata(
        paths.lease_metadata_path,
        {
            "snapshot_generation_id": snapshot_generation_id,
            "workspace_id": workspace_id,
            "machine_id": machine_id,
            "host_name": machine_id,
            "daemon_id": daemon_id,
            "pid": pid,
            "status": status,
            "last_checkpoint_at_utc": last_checkpoint_at_utc,
            "started_at_utc": started_at_utc,
            "app_version": app_version,
        },
    )


def write_lease_metadata(
    paths: WorkspacePaths,
    *,
    workspace_id: str,
    machine_id: str,
    daemon_id: str,
    pid: int,
    status: str,
    started_at_utc: str,
    last_checkpoint_at_utc: str,
    app_version: str | None,
) -> None:
    """Write lease metadata without rewriting the shared runtime snapshot."""
    initialize_workspace_state(paths)
    _write_lease_metadata(
        paths.lease_metadata_path,
        {
            "snapshot_generation_id": uuid4().hex,
            "workspace_id": workspace_id,
            "machine_id": machine_id,
            "host_name": machine_id,
            "daemon_id": daemon_id,
            "pid": pid,
            "status": status,
            "last_checkpoint_at_utc": last_checkpoint_at_utc,
            "started_at_utc": started_at_utc,
            "app_version": app_version,
        },
    )


def hydrate_local_runtime_state(paths: WorkspacePaths, ledger: RuntimeSnapshotStore) -> None:
    """Replace local SQLite runtime tables from shared parquet snapshots when present."""
    snapshot = _read_consistent_runtime_snapshot(paths)
    if snapshot is None:
        return
    runs, step_runs, logs, file_states = snapshot
    ledger.snapshots.replace(runs=runs, step_runs=step_runs, logs=logs, file_states=file_states)


def read_lease_metadata(paths: WorkspacePaths) -> dict[str, Any] | None:
    """Return shared lease metadata for one workspace when present."""
    return _read_single_row_parquet(paths.lease_metadata_path)


def read_control_request(paths: WorkspacePaths) -> dict[str, Any] | None:
    """Return one pending control-request row when present."""
    return _read_single_row_parquet(paths.control_request_path)


def remove_lease_metadata(paths: WorkspacePaths) -> None:
    """Delete the shared lease metadata parquet when present."""
    try:
        paths.lease_metadata_path.unlink()
    except FileNotFoundError:
        pass


def write_control_request(
    paths: WorkspacePaths,
    *,
    workspace_id: str,
    requester_machine_id: str,
    requester_host_name: str,
    requester_pid: int,
    requester_client_kind: str,
    requested_at_utc: str,
) -> None:
    """Persist one pending request to transfer workspace control."""
    write_parquet_atomic(
        _frame_with_schema(
            [
                {
                    "workspace_id": workspace_id,
                    "requester_machine_id": requester_machine_id,
                    "requester_host_name": requester_host_name,
                    "requester_pid": requester_pid,
                    "requester_client_kind": requester_client_kind,
                    "requested_at_utc": requested_at_utc,
                }
            ],
            _CONTROL_REQUEST_SCHEMA,
        ),
        paths.control_request_path,
    )


def _write_shared_runtime_snapshot(
    paths: WorkspacePaths,
    ledger: RuntimeSnapshotStore,
    *,
    snapshot_generation_id: str,
) -> None:
    runs = ledger.runs.list()
    _write_runs(paths.shared_runs_path, runs, snapshot_generation_id=snapshot_generation_id)
    step_runs = tuple(step for run in runs for step in ledger.step_outputs.list_for_run(run.run_id))
    _write_step_runs(paths.shared_step_runs_path, step_runs, snapshot_generation_id=snapshot_generation_id)
    _write_logs(paths.shared_logs_path, ledger.logs.list(), snapshot_generation_id=snapshot_generation_id)
    _write_file_states(
        paths.shared_file_state_path,
        ledger.source_signatures.list_file_states(),
        snapshot_generation_id=snapshot_generation_id,
    )


def _replace_shared_runtime_snapshot(
    paths: WorkspacePaths,
    *,
    runs: tuple[PersistedRun, ...],
    step_runs: tuple[PersistedStepRun, ...],
    logs: tuple[PersistedLogEntry, ...],
    file_states: tuple[PersistedFileState, ...],
) -> None:
    snapshot_generation_id = uuid4().hex
    _write_runs(paths.shared_runs_path, runs, snapshot_generation_id=snapshot_generation_id)
    _write_step_runs(paths.shared_step_runs_path, step_runs, snapshot_generation_id=snapshot_generation_id)
    _write_logs(paths.shared_logs_path, logs, snapshot_generation_id=snapshot_generation_id)
    _write_file_states(paths.shared_file_state_path, file_states, snapshot_generation_id=snapshot_generation_id)


def remove_control_request(paths: WorkspacePaths) -> None:
    """Delete one pending control-request parquet when present."""
    try:
        paths.control_request_path.unlink()
    except FileNotFoundError:
        pass


def reset_flow_state(paths: WorkspacePaths, *, flow_name: str) -> None:
    """Delete one flow's shared snapshot history and freshness state."""
    snapshot = _read_consistent_runtime_snapshot(paths)
    if snapshot is None:
        return
    runs, step_runs, logs, file_states = snapshot
    removed_run_ids = {run.run_id for run in runs if run.flow_name == flow_name}
    _replace_shared_runtime_snapshot(
        paths,
        runs=tuple(run for run in runs if run.flow_name != flow_name),
        step_runs=tuple(
            step_run
            for step_run in step_runs
            if step_run.flow_name != flow_name and step_run.run_id not in removed_run_ids
        ),
        logs=tuple(
            log
            for log in logs
            if log.flow_name != flow_name and (log.run_id is None or log.run_id not in removed_run_ids)
        ),
        file_states=tuple(file_state for file_state in file_states if file_state.flow_name != flow_name),
    )


def reset_workspace_state(paths: WorkspacePaths) -> None:
    """Delete shared runtime snapshots and pending control-transfer state for one workspace."""
    remove_file_if_exists(paths.shared_runs_path)
    remove_file_if_exists(paths.shared_step_runs_path)
    remove_file_if_exists(paths.shared_logs_path)
    remove_file_if_exists(paths.shared_file_state_path)
    remove_control_request(paths)
    if paths.stale_markers_dir.is_dir():
        for stale_path in paths.stale_markers_dir.glob(f"{paths.workspace_id}__*"):
            if stale_path.is_dir():
                shutil.rmtree(stale_path, ignore_errors=True)
            else:
                remove_file_if_exists(stale_path)


def _frame_with_schema(rows: list[dict[str, Any]], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Build one parquet-ready frame with stable column dtypes, even when values are all null."""
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema, infer_schema_length=None)


def _write_lease_metadata(path: Path, row: dict[str, Any]) -> None:
    write_parquet_atomic(_frame_with_schema([row], _LEASE_METADATA_SCHEMA), path)


def _write_runs(path: Path, rows: tuple[PersistedRun, ...], *, snapshot_generation_id: str) -> None:
    if not rows:
        remove_file_if_exists(path)
        return
    write_parquet_atomic(
        _frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _RUNS_SCHEMA,
        ),
        path,
    )


def _write_step_runs(path: Path, rows: tuple[PersistedStepRun, ...], *, snapshot_generation_id: str) -> None:
    if not rows:
        remove_file_if_exists(path)
        return
    write_parquet_atomic(
        _frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _STEP_RUNS_SCHEMA,
        ),
        path,
    )


def _write_logs(path: Path, rows: tuple[PersistedLogEntry, ...], *, snapshot_generation_id: str) -> None:
    if not rows:
        write_parquet_atomic(_frame_with_schema([], _LOGS_SCHEMA), path)
        return
    write_parquet_atomic(
        _frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _LOGS_SCHEMA,
        ),
        path,
    )


def _write_file_states(path: Path, rows: tuple[PersistedFileState, ...], *, snapshot_generation_id: str) -> None:
    if not rows:
        remove_file_if_exists(path)
        return
    write_parquet_atomic(
        _frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _FILE_STATE_SCHEMA,
        ),
        path,
    )


def remove_file_if_exists(path: Path) -> None:
    """Delete one file when it exists."""
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _read_runs(path: Path) -> tuple[PersistedRun, ...]:
    if not path.is_file():
        return ()
    frame = _read_parquet_with_retries(path)
    return tuple(PersistedRun(**_drop_snapshot_generation_id(row)) for row in frame.to_dicts())


def _read_step_runs(path: Path) -> tuple[PersistedStepRun, ...]:
    if not path.is_file():
        return ()
    frame = _read_parquet_with_retries(path)
    return tuple(PersistedStepRun(**_drop_snapshot_generation_id(row)) for row in frame.to_dicts())


def _read_logs(path: Path) -> tuple[PersistedLogEntry, ...]:
    if not path.is_file():
        return ()
    frame = _read_parquet_with_retries(path)
    return tuple(PersistedLogEntry(**_drop_snapshot_generation_id(row)) for row in frame.to_dicts())


def _read_file_states(path: Path) -> tuple[PersistedFileState, ...]:
    if not path.is_file():
        return ()
    frame = _read_parquet_with_retries(path)
    return tuple(PersistedFileState(**_drop_snapshot_generation_id(row)) for row in frame.to_dicts())


def _snapshot_generation_id_from_frame(frame: pl.DataFrame) -> str | None:
    if frame.height == 0 or "snapshot_generation_id" not in frame.columns:
        return None
    generation_ids = [value for value in frame.get_column("snapshot_generation_id").drop_nulls().unique().to_list() if isinstance(value, str) and value.strip()]
    if len(generation_ids) != 1:
        return None
    return generation_ids[0]


def _drop_snapshot_generation_id(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row.pop("snapshot_generation_id", None)
    return row


def _read_parquet_with_retries(path: Path, *, retries: int = _PARQUET_READ_RETRIES) -> pl.DataFrame:
    last_error: Exception | None = None
    for _ in range(max(retries, 1)):
        if not path.is_file():
            return pl.DataFrame()
        try:
            return pl.read_parquet(path)
        except (FileNotFoundError, OSError, pl.exceptions.PolarsError) as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    return pl.DataFrame()


def _read_single_row_parquet(path: Path) -> dict[str, Any] | None:
    frame = _read_parquet_with_retries(path)
    if frame.height == 0:
        return None
    return frame.row(0, named=True)


def _read_consistent_runtime_snapshot(
    paths: WorkspacePaths,
    *,
    retries: int = _PARQUET_READ_RETRIES,
) -> tuple[
    tuple[PersistedRun, ...],
    tuple[PersistedStepRun, ...],
    tuple[PersistedLogEntry, ...],
    tuple[PersistedFileState, ...],
] | None:
    for _ in range(max(retries, 1)):
        runs_frame = _read_parquet_with_retries(paths.shared_runs_path)
        step_runs_frame = _read_parquet_with_retries(paths.shared_step_runs_path)
        logs_frame = _read_parquet_with_retries(paths.shared_logs_path)
        file_states_frame = _read_parquet_with_retries(paths.shared_file_state_path)
        generations = {
            generation
            for generation in (
                _snapshot_generation_id_from_frame(runs_frame),
                _snapshot_generation_id_from_frame(step_runs_frame),
                _snapshot_generation_id_from_frame(logs_frame),
                _snapshot_generation_id_from_frame(file_states_frame),
            )
            if generation is not None
        }
        if len(generations) <= 1:
            return (
                tuple(PersistedRun(**_drop_snapshot_generation_id(row)) for row in runs_frame.to_dicts()),
                tuple(PersistedStepRun(**_drop_snapshot_generation_id(row)) for row in step_runs_frame.to_dicts()),
                tuple(PersistedLogEntry(**_drop_snapshot_generation_id(row)) for row in logs_frame.to_dicts()),
                tuple(PersistedFileState(**_drop_snapshot_generation_id(row)) for row in file_states_frame.to_dicts()),
            )
    return None


__all__ = [
    "checkpoint_workspace_state",
    "claim_workspace",
    "hydrate_local_runtime_state",
    "initialize_workspace_state",
    "lease_is_stale",
    "read_control_request",
    "read_lease_metadata",
    "recover_stale_workspace",
    "remove_control_request",
    "reset_flow_state",
    "reset_workspace_state",
    "release_workspace",
    "remove_lease_metadata",
    "RuntimeSnapshotStore",
    "write_control_request",
    "write_lease_metadata",
]
