"""Workspace coordination and runtime snapshot helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import re
import shutil
import time
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


class _SnapshotRepository(Protocol):
    def export(
        self,
    ) -> tuple[
        tuple[PersistedRun, ...],
        tuple[PersistedStepRun, ...],
        tuple[PersistedLogEntry, ...],
        tuple[PersistedFileState, ...],
    ]: ...

    def applied_generation_id(self) -> str | None: ...

    def replace(
        self,
        *,
        generation_id: str,
        runs: tuple[PersistedRun, ...],
        step_runs: tuple[PersistedStepRun, ...],
        logs: tuple[PersistedLogEntry, ...],
        file_states: tuple[PersistedFileState, ...],
    ) -> bool: ...


class RuntimeSnapshotStore(Protocol):
    """Minimal runtime snapshot surface used by workspace coordination."""

    snapshots: _SnapshotRepository

    def snapshot_change_token(self) -> object: ...


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
_SNAPSHOT_FORMAT_VERSION = 1
_SNAPSHOT_GENERATIONS_TO_KEEP = 3
_SNAPSHOT_ARTIFACT_NAMES = {
    "runs": "runs.parquet",
    "step_runs": "step_runs.parquet",
    "logs": "logs.parquet",
    "file_state": "file_state.parquet",
}
_GENERATION_ID_PATTERN = re.compile(r"[0-9a-f]{32}")


@dataclass(frozen=True)
class _CommittedRuntimeSnapshot:
    generation_id: str
    runs: tuple[PersistedRun, ...]
    step_runs: tuple[PersistedStepRun, ...]
    logs: tuple[PersistedLogEntry, ...]
    file_states: tuple[PersistedFileState, ...]


def initialize_workspace_state(paths: WorkspacePaths) -> None:
    """Ensure the shared-state folder tree and initial availability marker exist."""
    for directory in (
        paths.workspace_state_dir,
        paths.available_markers_dir,
        paths.leased_markers_dir,
        paths.stale_markers_dir,
        paths.lease_metadata_dir,
        paths.control_requests_dir,
        paths.shared_snapshot_generations_dir,
        paths.shared_snapshot_manifest_path.parent,
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
    host_name: str,
    daemon_id: str,
    pid: int,
    status: str,
    started_at_utc: str,
    last_checkpoint_at_utc: str,
    app_version: str | None,
) -> str:
    """Write shared runtime snapshots and workspace lease metadata."""
    initialize_workspace_state(paths)
    snapshot_generation_id = uuid4().hex
    runs, step_runs, logs, file_states = ledger.snapshots.export()
    _publish_shared_runtime_snapshot(
        paths,
        snapshot_generation_id=snapshot_generation_id,
        runs=runs,
        step_runs=step_runs,
        logs=logs,
        file_states=file_states,
    )
    _write_lease_metadata(
        paths.lease_metadata_path,
        {
            "snapshot_generation_id": snapshot_generation_id,
            "workspace_id": workspace_id,
            "machine_id": machine_id,
            "host_name": host_name,
            "daemon_id": daemon_id,
            "pid": pid,
            "status": status,
            "last_checkpoint_at_utc": last_checkpoint_at_utc,
            "started_at_utc": started_at_utc,
            "app_version": app_version,
        },
    )
    return snapshot_generation_id


def write_lease_metadata(
    paths: WorkspacePaths,
    *,
    workspace_id: str,
    machine_id: str,
    host_name: str,
    daemon_id: str,
    pid: int,
    status: str,
    started_at_utc: str,
    last_checkpoint_at_utc: str,
    app_version: str | None,
) -> None:
    """Write lease metadata without rewriting the shared runtime snapshot."""
    initialize_workspace_state(paths)
    snapshot_generation_id = read_runtime_snapshot_generation(paths)
    _write_lease_metadata(
        paths.lease_metadata_path,
        {
            "snapshot_generation_id": snapshot_generation_id,
            "workspace_id": workspace_id,
            "machine_id": machine_id,
            "host_name": host_name,
            "daemon_id": daemon_id,
            "pid": pid,
            "status": status,
            "last_checkpoint_at_utc": last_checkpoint_at_utc,
            "started_at_utc": started_at_utc,
            "app_version": app_version,
        },
    )


def hydrate_local_runtime_state(paths: WorkspacePaths, ledger: RuntimeSnapshotStore) -> bool:
    """Replace local SQLite runtime tables from shared parquet snapshots when present."""
    snapshot_generation_id = read_runtime_snapshot_generation(paths)
    if snapshot_generation_id is None:
        return False
    if ledger.snapshots.applied_generation_id() == snapshot_generation_id:
        return False
    snapshot = _read_consistent_runtime_snapshot(paths)
    if snapshot is None:
        return False
    return ledger.snapshots.replace(
        generation_id=snapshot.generation_id,
        runs=snapshot.runs,
        step_runs=snapshot.step_runs,
        logs=snapshot.logs,
        file_states=snapshot.file_states,
    )


def read_runtime_snapshot_generation(paths: WorkspacePaths) -> str | None:
    """Return the currently committed shared snapshot generation."""
    manifest = _read_snapshot_manifest(paths.shared_snapshot_manifest_path)
    if manifest is None:
        return None
    return _manifest_generation_id(manifest)


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


def _publish_shared_runtime_snapshot(
    paths: WorkspacePaths,
    *,
    snapshot_generation_id: str,
    runs: tuple[PersistedRun, ...],
    step_runs: tuple[PersistedStepRun, ...],
    logs: tuple[PersistedLogEntry, ...],
    file_states: tuple[PersistedFileState, ...],
) -> None:
    if _GENERATION_ID_PATTERN.fullmatch(snapshot_generation_id) is None:
        raise ValueError(f"Invalid snapshot generation id: {snapshot_generation_id!r}")
    generations_dir = paths.shared_snapshot_generations_dir
    generations_dir.mkdir(parents=True, exist_ok=True)
    staging_dir = generations_dir / f".{snapshot_generation_id}.{uuid4().hex}.tmp"
    committed_dir = generations_dir / snapshot_generation_id
    staging_dir.mkdir()
    try:
        artifact_paths = _snapshot_artifact_paths(staging_dir)
        _write_runs(artifact_paths["runs"], runs, snapshot_generation_id=snapshot_generation_id)
        _write_step_runs(artifact_paths["step_runs"], step_runs, snapshot_generation_id=snapshot_generation_id)
        _write_logs(artifact_paths["logs"], logs, snapshot_generation_id=snapshot_generation_id)
        _write_file_states(artifact_paths["file_state"], file_states, snapshot_generation_id=snapshot_generation_id)
        _replace_path_with_retries(staging_dir, committed_dir)
        _write_snapshot_manifest_atomic(
            paths.shared_snapshot_manifest_path,
            {
                "format_version": _SNAPSHOT_FORMAT_VERSION,
                "generation_id": snapshot_generation_id,
                "artifacts": dict(_SNAPSHOT_ARTIFACT_NAMES),
            },
        )
    except BaseException:
        shutil.rmtree(staging_dir, ignore_errors=True)
        if committed_dir.is_dir() and read_runtime_snapshot_generation(paths) != snapshot_generation_id:
            shutil.rmtree(committed_dir, ignore_errors=True)
        raise
    _garbage_collect_snapshot_generations(paths, committed_generation_id=snapshot_generation_id)


def _snapshot_artifact_paths(generation_dir: Path) -> dict[str, Path]:
    return {name: generation_dir / filename for name, filename in _SNAPSHOT_ARTIFACT_NAMES.items()}


def _write_snapshot_manifest_atomic(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary_path.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(manifest, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        _replace_path_with_retries(temporary_path, path)
    except BaseException:
        remove_file_if_exists(temporary_path)
        raise


def _replace_path_with_retries(source_path: Path, target_path: Path) -> None:
    last_error: PermissionError | None = None
    for delay_seconds in (0.0, 0.02, 0.05, 0.1, 0.2):
        if delay_seconds:
            time.sleep(delay_seconds)
        try:
            os.replace(source_path, target_path)
            return
        except PermissionError as exc:
            if getattr(exc, "winerror", None) != 5:
                raise
            last_error = exc
    if last_error is not None:
        raise last_error


def _garbage_collect_snapshot_generations(
    paths: WorkspacePaths,
    *,
    committed_generation_id: str,
) -> None:
    try:
        candidates = tuple(paths.shared_snapshot_generations_dir.iterdir())
    except OSError:
        return
    committed_directories: list[tuple[int, Path]] = []
    for candidate in candidates:
        try:
            is_committed_directory = (
                candidate.is_dir() and _GENERATION_ID_PATTERN.fullmatch(candidate.name) is not None
            )
            modified_at_ns = candidate.stat().st_mtime_ns
        except OSError:
            continue
        if not is_committed_directory:
            continue
        committed_directories.append((modified_at_ns, candidate))
    committed_directories.sort(key=lambda item: item[0], reverse=True)
    keep_names = {committed_generation_id}
    retained_older_names = (
        candidate.name
        for _, candidate in committed_directories
        if candidate.name != committed_generation_id
    )
    for _ in range(max(_SNAPSHOT_GENERATIONS_TO_KEEP - 1, 0)):
        retained_name = next(retained_older_names, None)
        if retained_name is None:
            break
        keep_names.add(retained_name)
    for _, candidate in committed_directories:
        if candidate.name not in keep_names:
            shutil.rmtree(candidate, ignore_errors=True)


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
    removed_run_ids = {run.run_id for run in snapshot.runs if run.flow_name == flow_name}
    _publish_shared_runtime_snapshot(
        paths,
        snapshot_generation_id=uuid4().hex,
        runs=tuple(run for run in snapshot.runs if run.flow_name != flow_name),
        step_runs=tuple(
            step_run
            for step_run in snapshot.step_runs
            if step_run.flow_name != flow_name and step_run.run_id not in removed_run_ids
        ),
        logs=tuple(
            log
            for log in snapshot.logs
            if log.flow_name != flow_name and (log.run_id is None or log.run_id not in removed_run_ids)
        ),
        file_states=tuple(file_state for file_state in snapshot.file_states if file_state.flow_name != flow_name),
    )


def reset_workspace_state(paths: WorkspacePaths) -> None:
    """Delete shared runtime snapshots and pending control-transfer state for one workspace."""
    remove_file_if_exists(paths.shared_snapshot_manifest_path)
    shutil.rmtree(paths.shared_snapshot_generations_dir, ignore_errors=True)
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
    write_parquet_atomic(
        _frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _RUNS_SCHEMA,
        ),
        path,
    )


def _write_step_runs(path: Path, rows: tuple[PersistedStepRun, ...], *, snapshot_generation_id: str) -> None:
    write_parquet_atomic(
        _frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _STEP_RUNS_SCHEMA,
        ),
        path,
    )


def _write_logs(path: Path, rows: tuple[PersistedLogEntry, ...], *, snapshot_generation_id: str) -> None:
    write_parquet_atomic(
        _frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _LOGS_SCHEMA,
        ),
        path,
    )


def _write_file_states(path: Path, rows: tuple[PersistedFileState, ...], *, snapshot_generation_id: str) -> None:
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


def _read_parquet_with_retries(
    path: Path,
    *,
    retries: int = _PARQUET_READ_RETRIES,
    required: bool = False,
) -> pl.DataFrame:
    last_error: Exception | None = None
    for _ in range(max(retries, 1)):
        if not path.is_file():
            if not required:
                return pl.DataFrame()
            last_error = FileNotFoundError(path)
            continue
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


def _read_snapshot_manifest(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _manifest_generation_id(manifest: dict[str, Any]) -> str | None:
    if manifest.get("format_version") != _SNAPSHOT_FORMAT_VERSION:
        return None
    generation_id = manifest.get("generation_id")
    if not isinstance(generation_id, str) or _GENERATION_ID_PATTERN.fullmatch(generation_id) is None:
        return None
    if manifest.get("artifacts") != _SNAPSHOT_ARTIFACT_NAMES:
        return None
    return generation_id


def _frame_matches_generation(
    frame: pl.DataFrame,
    *,
    schema: dict[str, pl.DataType],
    generation_id: str,
) -> bool:
    if dict(frame.schema) != schema:
        return False
    if frame.height == 0:
        return True
    return _snapshot_generation_id_from_frame(frame) == generation_id


def _read_consistent_runtime_snapshot(
    paths: WorkspacePaths,
    *,
    retries: int = _PARQUET_READ_RETRIES,
) -> _CommittedRuntimeSnapshot | None:
    retry_count = max(retries, 1)
    for attempt in range(retry_count):
        manifest = _read_snapshot_manifest(paths.shared_snapshot_manifest_path)
        generation_id = _manifest_generation_id(manifest) if manifest is not None else None
        if generation_id is None:
            return None
        artifact_paths = _snapshot_artifact_paths(paths.shared_snapshot_generations_dir / generation_id)
        try:
            runs_frame = _read_parquet_with_retries(artifact_paths["runs"], required=True)
            step_runs_frame = _read_parquet_with_retries(artifact_paths["step_runs"], required=True)
            logs_frame = _read_parquet_with_retries(artifact_paths["logs"], required=True)
            file_states_frame = _read_parquet_with_retries(artifact_paths["file_state"], required=True)
        except (FileNotFoundError, OSError, pl.exceptions.PolarsError):
            _wait_before_snapshot_retry(attempt, retry_count=retry_count)
            continue
        if not all(
            (
                _frame_matches_generation(runs_frame, schema=_RUNS_SCHEMA, generation_id=generation_id),
                _frame_matches_generation(step_runs_frame, schema=_STEP_RUNS_SCHEMA, generation_id=generation_id),
                _frame_matches_generation(logs_frame, schema=_LOGS_SCHEMA, generation_id=generation_id),
                _frame_matches_generation(file_states_frame, schema=_FILE_STATE_SCHEMA, generation_id=generation_id),
            )
        ):
            _wait_before_snapshot_retry(attempt, retry_count=retry_count)
            continue
        final_manifest = _read_snapshot_manifest(paths.shared_snapshot_manifest_path)
        if final_manifest != manifest:
            _wait_before_snapshot_retry(attempt, retry_count=retry_count)
            continue
        return _CommittedRuntimeSnapshot(
            generation_id=generation_id,
            runs=tuple(PersistedRun(**_drop_snapshot_generation_id(row)) for row in runs_frame.to_dicts()),
            step_runs=tuple(
                PersistedStepRun(**_drop_snapshot_generation_id(row)) for row in step_runs_frame.to_dicts()
            ),
            logs=tuple(PersistedLogEntry(**_drop_snapshot_generation_id(row)) for row in logs_frame.to_dicts()),
            file_states=tuple(
                PersistedFileState(**_drop_snapshot_generation_id(row)) for row in file_states_frame.to_dicts()
            ),
        )
    return None


def _wait_before_snapshot_retry(attempt: int, *, retry_count: int) -> None:
    if attempt + 1 >= retry_count:
        return
    time.sleep((0.02, 0.05, 0.1)[min(attempt, 2)])


__all__ = [
    "checkpoint_workspace_state",
    "claim_workspace",
    "hydrate_local_runtime_state",
    "initialize_workspace_state",
    "lease_is_stale",
    "read_control_request",
    "read_lease_metadata",
    "read_runtime_snapshot_generation",
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
