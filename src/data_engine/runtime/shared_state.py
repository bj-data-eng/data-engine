"""Token-fenced workspace ownership and runtime snapshot helpers."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import threading
import time
from typing import Any, Iterator, Literal, Mapping, Protocol
from uuid import uuid4

import polars as pl

from data_engine.helpers.polars import write_parquet_atomic
from data_engine.platform.processes import ProcessIdentity
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.ledger_models import (
    PersistedFileState,
    PersistedLogEntry,
    PersistedRun,
    PersistedStepRun,
)
from data_engine.runtime.runtime_db import parse_utc_text


class WorkspaceLeaseLostError(RuntimeError):
    """Raised when an owner no longer holds its immutable workspace lease token."""


class WorkspaceStateCorruptError(RuntimeError):
    """Raised when workspace marker topology cannot identify one safe owner."""


class WorkspaceTransitionInProgressError(RuntimeError):
    """Raised when a stale-lease recovery rename is currently in progress."""


class WorkspaceUnavailableForResetError(RuntimeError):
    """Raised when destructive maintenance cannot acquire an available workspace."""


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

    def snapshot_incarnation(self) -> str: ...


_LEASE_METADATA_SCHEMA: dict[str, pl.DataType] = {
    "snapshot_generation_id": pl.String,
    "workspace_id": pl.String,
    "lease_token": pl.String,
    "machine_id": pl.String,
    "host_name": pl.String,
    "daemon_id": pl.String,
    "pid": pl.Int64,
    "process_start_key": pl.String,
    "process_executable_path": pl.String,
    "process_group_id": pl.Int64,
    "process_session_id": pl.Int64,
    "containment_nonce": pl.String,
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
_LEASE_METADATA_FILE_NAME = "lease.parquet"
_SNAPSHOT_MANIFEST_FILE_NAME = "snapshot_manifest.json"
_SNAPSHOT_GENERATIONS_DIR_NAME = "snapshots"
_LEASE_TOKEN_PATTERN = re.compile(r"[0-9a-f]{32}")
_CONTAINMENT_NONCE_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_GENERATION_ID_PATTERN = re.compile(r"[0-9a-f]{32}")
_ROOT_TEMP_PATTERN = re.compile(r"\.(?:l|m)\.(?P<token>[0-9a-f]{32})\.[0-9a-f]{16}\.tmp")
_STAGING_TEMP_PATTERN = re.compile(
    r"\.s\.(?P<token>[0-9a-f]{32})\.[0-9a-f]{8}\.tmp"
)
_RECOVERY_PREFIX = ".recovering__"
_SNAPSHOT_PUBLICATION_LOCKS_GUARD = threading.Lock()
_SNAPSHOT_PUBLICATION_LOCKS: dict[str, threading.RLock] = {}
_TOPOLOGY_THREAD_LOCKS_GUARD = threading.Lock()
_TOPOLOGY_THREAD_LOCKS: dict[str, threading.RLock] = {}
_TOPOLOGY_LOCK_DEPTH = threading.local()


@dataclass(frozen=True)
class WorkspaceBundlePaths:
    """Resolved paths for one exact available or token-owned workspace bundle."""

    state: Literal["available", "leased"]
    root: Path
    lease_token: str | None
    lease_metadata_path: Path
    snapshot_generations_dir: Path
    snapshot_manifest_path: Path

    @property
    def topology_signature(self) -> tuple[str, str, str | None]:
        """Return a cache signature that changes across every ownership rename."""
        return (self.state, str(self.root), self.lease_token)


@dataclass(frozen=True, slots=True)
class DaemonProcessLeaseIdentity:
    """Verified process identity and containment key persisted for one daemon lease.

    Attributes:
        process_identity: Immutable operating-system identity for the daemon process.
        containment_nonce: Canonical 256-bit nonce naming its containment boundary.
    """

    process_identity: ProcessIdentity
    containment_nonce: str


@dataclass(frozen=True)
class _CommittedRuntimeSnapshot:
    generation_id: str
    runs: tuple[PersistedRun, ...]
    step_runs: tuple[PersistedStepRun, ...]
    logs: tuple[PersistedLogEntry, ...]
    file_states: tuple[PersistedFileState, ...]


def _bundle_paths(
    *,
    root: Path,
    state: Literal["available", "leased"],
    lease_token: str | None,
) -> WorkspaceBundlePaths:
    return WorkspaceBundlePaths(
        state=state,
        root=root,
        lease_token=lease_token,
        lease_metadata_path=root / _LEASE_METADATA_FILE_NAME,
        snapshot_generations_dir=root / _SNAPSHOT_GENERATIONS_DIR_NAME,
        snapshot_manifest_path=root / _SNAPSHOT_MANIFEST_FILE_NAME,
    )


def _available_bundle_path(paths: WorkspacePaths) -> Path:
    return paths.available_markers_dir / paths.workspace_id


def _leased_bundle_path(paths: WorkspacePaths, lease_token: str) -> Path:
    _validate_lease_token(lease_token)
    return paths.leased_markers_dir / f"{paths.workspace_id}__{lease_token}"


def _recovery_bundle_path(paths: WorkspacePaths, lease_token: str) -> Path:
    return paths.leased_markers_dir / f"{_RECOVERY_PREFIX}{paths.workspace_id}__{lease_token}"


def _validate_lease_token(lease_token: str) -> str:
    token = str(lease_token)
    if _LEASE_TOKEN_PATTERN.fullmatch(token) is None:
        raise ValueError(f"Invalid workspace lease token: {lease_token!r}")
    return token


def _directory_entries(path: Path) -> tuple[Path, ...]:
    try:
        return tuple(path.iterdir())
    except FileNotFoundError:
        return ()


def _is_redirecting_path(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


def _assert_real_directory(
    path: Path,
    *,
    workspace_id: str,
    description: str,
    allow_missing: bool = False,
) -> bool:
    if _is_redirecting_path(path):
        raise WorkspaceStateCorruptError(
            f"Workspace {workspace_id!r} {description} redirects outside its bundle."
        )
    if not path.exists():
        if allow_missing:
            return False
        raise WorkspaceStateCorruptError(
            f"Workspace {workspace_id!r} {description} is missing."
        )
    if not path.is_dir():
        raise WorkspaceStateCorruptError(
            f"Workspace {workspace_id!r} {description} is not a directory."
        )
    return True


def _assert_regular_file(
    path: Path,
    *,
    workspace_id: str,
    description: str,
    allow_missing: bool = False,
) -> bool:
    if _is_redirecting_path(path):
        raise WorkspaceStateCorruptError(
            f"Workspace {workspace_id!r} {description} redirects outside its bundle."
        )
    if not path.exists():
        if allow_missing:
            return False
        raise WorkspaceStateCorruptError(
            f"Workspace {workspace_id!r} {description} is missing."
        )
    if not path.is_file():
        raise WorkspaceStateCorruptError(
            f"Workspace {workspace_id!r} {description} is not a regular file."
        )
    return True


def _remove_temp_artifact_best_effort(path: Path) -> None:
    try:
        if path.is_symlink():
            path.unlink()
        elif path.is_junction():
            path.rmdir()
        elif path.is_dir():
            shutil.rmtree(path)
        elif path.is_file():
            path.unlink()
    except OSError:
        pass


def _scrub_noncurrent_temp_artifacts(
    bundle_root: Path,
    *,
    workspace_id: str,
    lease_token: str,
) -> None:
    try:
        root_entries = tuple(bundle_root.iterdir())
    except OSError:
        root_entries = ()
    for entry in root_entries:
        match = _ROOT_TEMP_PATTERN.fullmatch(entry.name)
        if match is not None and match.group("token") != lease_token:
            _remove_temp_artifact_best_effort(entry)
    snapshots_root = bundle_root / _SNAPSHOT_GENERATIONS_DIR_NAME
    if not _assert_real_directory(
        snapshots_root,
        workspace_id=workspace_id,
        description="snapshot root",
        allow_missing=True,
    ):
        return
    try:
        snapshot_entries = tuple(snapshots_root.iterdir())
    except OSError:
        return
    for entry in snapshot_entries:
        match = _STAGING_TEMP_PATTERN.fullmatch(entry.name)
        if match is not None and match.group("token") != lease_token:
            _remove_temp_artifact_best_effort(entry)


def _marker_token(entry: Path, *, workspace_id: str, recovery: bool = False) -> str | None:
    marker_name = entry.name
    if recovery:
        if not marker_name.startswith(_RECOVERY_PREFIX):
            return None
        marker_name = marker_name[len(_RECOVERY_PREFIX) :]
    elif marker_name.startswith(_RECOVERY_PREFIX):
        return None
    if marker_name == workspace_id:
        raise WorkspaceStateCorruptError(f"Workspace {workspace_id!r} has a tokenless lease marker.")
    marker_workspace_id, separator, token = marker_name.rpartition("__")
    if not separator or marker_workspace_id != workspace_id:
        return None
    if _LEASE_TOKEN_PATTERN.fullmatch(token) is None:
        raise WorkspaceStateCorruptError(f"Workspace {workspace_id!r} has an invalid lease marker name.")
    if _is_redirecting_path(entry) or not entry.is_dir():
        raise WorkspaceStateCorruptError(f"Workspace {workspace_id!r} has a non-directory lease marker.")
    return token


def _locate_workspace_bundle(paths: WorkspacePaths) -> WorkspaceBundlePaths | None:
    available_path = _available_bundle_path(paths)
    for entry in _directory_entries(paths.available_markers_dir):
        if entry.name != paths.workspace_id:
            raise WorkspaceStateCorruptError(
                f"Workspace root for {paths.workspace_id!r} contains marker state for another workspace id."
            )
    available_redirects = _is_redirecting_path(available_path)
    if available_redirects or (available_path.exists() and not available_path.is_dir()):
        raise WorkspaceStateCorruptError(f"Workspace {paths.workspace_id!r} has a non-directory available marker.")
    available = available_path.is_dir() and not available_redirects
    leased: list[tuple[str, Path]] = []
    recovering: list[tuple[str, Path]] = []
    for entry in _directory_entries(paths.leased_markers_dir):
        token = _marker_token(entry, workspace_id=paths.workspace_id)
        if token is not None:
            leased.append((token, entry))
            continue
        recovery_token = _marker_token(entry, workspace_id=paths.workspace_id, recovery=True)
        if recovery_token is not None:
            recovering.append((recovery_token, entry))
            continue
        raise WorkspaceStateCorruptError(
            f"Workspace root for {paths.workspace_id!r} contains lease state for another workspace id."
        )
    if recovering:
        if len(recovering) != 1 or available or leased:
            raise WorkspaceStateCorruptError(
                f"Workspace {paths.workspace_id!r} has conflicting recovery marker state."
            )
        raise WorkspaceTransitionInProgressError(
            f"Workspace {paths.workspace_id!r} lease recovery is in progress."
        )
    if len(leased) > 1 or (available and leased):
        raise WorkspaceStateCorruptError(f"Workspace {paths.workspace_id!r} has conflicting marker state.")
    if leased:
        token, root = leased[0]
        return _bundle_paths(root=root, state="leased", lease_token=token)
    if available:
        return _bundle_paths(root=available_path, state="available", lease_token=None)
    return None


def resolve_workspace_bundle(
    paths: WorkspacePaths,
    *,
    retries: int = _PARQUET_READ_RETRIES,
) -> WorkspaceBundlePaths | None:
    """Resolve the single current bundle, retrying across a recovery rename."""
    retry_count = max(retries, 1)
    for attempt in range(retry_count):
        try:
            return _locate_workspace_bundle(paths)
        except WorkspaceTransitionInProgressError:
            _wait_before_snapshot_retry(attempt, retry_count=retry_count)
    raise WorkspaceTransitionInProgressError(
        f"Workspace {paths.workspace_id!r} lease recovery did not settle."
    )


def _topology_thread_lock(paths: WorkspacePaths) -> threading.RLock:
    key = str((paths.workspace_state_dir / ".lease-topology.lock").absolute())
    with _TOPOLOGY_THREAD_LOCKS_GUARD:
        lock = _TOPOLOGY_THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _TOPOLOGY_THREAD_LOCKS[key] = lock
        return lock


@contextmanager
def _topology_lock(paths: WorkspacePaths) -> Iterator[None]:
    """Serialize cold marker transitions across threads and processes."""
    paths.workspace_state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = paths.workspace_state_dir / ".lease-topology.lock"
    thread_lock = _topology_thread_lock(paths)
    with thread_lock:
        lock_key = str(lock_path.absolute())
        depths = getattr(_TOPOLOGY_LOCK_DEPTH, "by_path", None)
        if depths is None:
            depths = {}
            _TOPOLOGY_LOCK_DEPTH.by_path = depths
        if depths.get(lock_key, 0) > 0:
            depths[lock_key] += 1
            try:
                yield
            finally:
                depths[lock_key] -= 1
            return
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            if os.name == "nt":
                import msvcrt

                if os.fstat(descriptor).st_size == 0:
                    os.write(descriptor, b"\0")
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.lockf(descriptor, fcntl.LOCK_EX, 1, 0, os.SEEK_SET)
            depths[lock_key] = 1
            try:
                yield
            finally:
                depths.pop(lock_key, None)
                try:
                    if os.name == "nt":
                        import msvcrt

                        os.lseek(descriptor, 0, os.SEEK_SET)
                        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.lockf(descriptor, fcntl.LOCK_UN, 1, 0, os.SEEK_SET)
                except OSError:
                    pass
        finally:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _repair_abandoned_recovery_locked(paths: WorkspacePaths) -> None:
    recovering: list[tuple[str, Path]] = []
    for entry in _directory_entries(paths.leased_markers_dir):
        token = _marker_token(entry, workspace_id=paths.workspace_id, recovery=True)
        if token is not None:
            recovering.append((token, entry))
    if not recovering:
        return
    if len(recovering) != 1:
        raise WorkspaceStateCorruptError(f"Workspace {paths.workspace_id!r} has multiple recovery markers.")
    token, recovery_path = recovering[0]
    canonical_path = _leased_bundle_path(paths, token)
    available_path = _available_bundle_path(paths)
    if (
        available_path.exists()
        or _is_redirecting_path(available_path)
        or canonical_path.exists()
        or _is_redirecting_path(canonical_path)
    ):
        raise WorkspaceStateCorruptError(f"Workspace {paths.workspace_id!r} has conflicting recovery markers.")
    recovery_path.rename(canonical_path)


def _initialize_workspace_state_locked(paths: WorkspacePaths) -> None:
    for directory in (
        paths.workspace_state_dir,
        paths.available_markers_dir,
        paths.leased_markers_dir,
        paths.stale_markers_dir,
        paths.control_requests_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    _repair_abandoned_recovery_locked(paths)
    current = _locate_workspace_bundle(paths)
    if current is not None:
        return
    available = _available_bundle_path(paths)
    try:
        available.mkdir()
    except FileExistsError:
        pass
    current = _locate_workspace_bundle(paths)
    if current is None:
        raise WorkspaceStateCorruptError(f"Workspace {paths.workspace_id!r} has no stable marker state.")


def initialize_workspace_state(paths: WorkspacePaths) -> None:
    """Create the marker roots and exactly one initial available bundle."""
    with _topology_lock(paths):
        _initialize_workspace_state_locked(paths)


def assert_workspace_lease(paths: WorkspacePaths, lease_token: str) -> WorkspaceBundlePaths:
    """Return exact owner bundle paths or raise when the token is no longer current."""
    token = _validate_lease_token(lease_token)
    retry_count = _PARQUET_READ_RETRIES
    for attempt in range(retry_count):
        try:
            current = _locate_workspace_bundle(paths)
        except WorkspaceTransitionInProgressError:
            recovery_tokens = {
                recovery_token
                for entry in _directory_entries(paths.leased_markers_dir)
                if (recovery_token := _marker_token(
                    entry,
                    workspace_id=paths.workspace_id,
                    recovery=True,
                ))
                is not None
            }
            if recovery_tokens != {token}:
                break
            _wait_before_snapshot_retry(attempt, retry_count=retry_count)
            continue
        if current is not None and current.state == "leased" and current.lease_token == token:
            return current
        break
    raise WorkspaceLeaseLostError(
        f"Workspace {paths.workspace_id!r} lease token {token} is no longer current."
    )


def _assert_exact_lease_path(paths: WorkspacePaths, lease_token: str) -> WorkspaceBundlePaths:
    """Check the exact immutable token path without scanning marker directories."""
    token = _validate_lease_token(lease_token)
    root = _leased_bundle_path(paths, token)
    if _is_redirecting_path(root):
        raise WorkspaceStateCorruptError(
            f"Workspace {paths.workspace_id!r} exact lease marker redirects outside its bundle."
        )
    if not root.is_dir():
        raise WorkspaceLeaseLostError(
            f"Workspace {paths.workspace_id!r} lease token {token} is no longer current."
        )
    return _bundle_paths(root=root, state="leased", lease_token=token)


@contextmanager
def workspace_lease_operation(paths: WorkspacePaths, *, lease_token: str) -> Iterator[None]:
    """Fence a rare destructive operation against concurrent ownership turnover."""
    with _topology_lock(paths):
        assert_workspace_lease(paths, lease_token)
        yield


def claim_workspace(paths: WorkspacePaths) -> str | None:
    """Atomically claim an available bundle and return a new immutable lease token."""
    with _topology_lock(paths):
        _initialize_workspace_state_locked(paths)
        current = _locate_workspace_bundle(paths)
        if current is None or current.state != "available":
            return None
        lease_token = secrets.token_hex(16)
        leased = _leased_bundle_path(paths, lease_token)
        try:
            os.utime(current.root)
            _scrub_noncurrent_temp_artifacts(
                current.root,
                workspace_id=paths.workspace_id,
                lease_token=lease_token,
            )
            remove_file_if_exists(current.lease_metadata_path)
            current.root.rename(leased)
        except FileNotFoundError as exc:
            raise WorkspaceLeaseLostError(
                f"Workspace {paths.workspace_id!r} claim lost during marker transition."
            ) from exc
        return lease_token


def claim_daemon_workspace(
    paths: WorkspacePaths,
    *,
    workspace_id: str,
    machine_id: str,
    host_name: str,
    daemon_id: str,
    pid: int,
    process_identity: ProcessIdentity,
    containment_nonce: str,
    status: str,
    started_at_utc: str,
    last_checkpoint_at_utc: str,
    app_version: str | None,
) -> str | None:
    """Atomically claim a workspace with a complete daemon owner record.

    The full identity row is published inside the available bundle before its
    single rename to the token-named leased bundle. A crash before the rename
    therefore leaves an available workspace, while every visible daemon lease
    already carries the identity and containment capability needed for exact
    recovery.
    """
    if workspace_id != paths.workspace_id:
        raise ValueError("A daemon claim workspace id must match its workspace paths.")
    with _topology_lock(paths):
        _initialize_workspace_state_locked(paths)
        current = _locate_workspace_bundle(paths)
        if current is None or current.state != "available":
            return None
        lease_token = secrets.token_hex(16)
        leased = _leased_bundle_path(paths, lease_token)
        manifest = _read_snapshot_manifest(current.snapshot_manifest_path)
        snapshot_generation_id = (
            _manifest_generation_id(manifest) if manifest is not None else None
        )
        row = _metadata_row(
            snapshot_generation_id=snapshot_generation_id,
            workspace_id=workspace_id,
            lease_token=lease_token,
            machine_id=machine_id,
            host_name=host_name,
            daemon_id=daemon_id,
            pid=pid,
            process_identity=process_identity,
            containment_nonce=containment_nonce,
            status=status,
            started_at_utc=started_at_utc,
            last_checkpoint_at_utc=last_checkpoint_at_utc,
            app_version=app_version,
        )
        try:
            os.utime(current.root)
            _scrub_noncurrent_temp_artifacts(
                current.root,
                workspace_id=paths.workspace_id,
                lease_token=lease_token,
            )
            remove_file_if_exists(current.lease_metadata_path)
            _write_initial_daemon_lease_metadata(
                current,
                lease_token=lease_token,
                row=row,
            )
            current.root.rename(leased)
        except FileNotFoundError as exc:
            raise WorkspaceLeaseLostError(
                f"Workspace {paths.workspace_id!r} claim lost during marker transition."
            ) from exc
        return lease_token


def _write_initial_daemon_lease_metadata(
    bundle: WorkspaceBundlePaths,
    *,
    lease_token: str,
    row: dict[str, Any],
) -> None:
    temporary_path = bundle.lease_metadata_path.with_name(f".l.{lease_token}.{uuid4().hex[:16]}.tmp")
    try:
        _frame_with_schema([row], _LEASE_METADATA_SCHEMA).write_parquet(temporary_path)
        _sync_file_contents(temporary_path)
        os.replace(temporary_path, bundle.lease_metadata_path)
    finally:
        remove_file_if_exists(temporary_path)


def release_workspace(paths: WorkspacePaths, *, lease_token: str) -> None:
    """Return only the exact token-owned bundle to available state."""
    token = _validate_lease_token(lease_token)
    with _topology_lock(paths):
        _repair_abandoned_recovery_locked(paths)
        current = assert_workspace_lease(paths, token)
        available = _available_bundle_path(paths)
        if available.exists():
            raise WorkspaceStateCorruptError(
                f"Workspace {paths.workspace_id!r} cannot release over an existing available bundle."
            )
        try:
            current.root.rename(available)
        except FileNotFoundError as exc:
            raise WorkspaceLeaseLostError(
                f"Workspace {paths.workspace_id!r} lease {token} disappeared before release."
            ) from exc


def _lease_metadata_placeholder(bundle: WorkspaceBundlePaths, *, workspace_id: str) -> dict[str, Any]:
    return {
        "workspace_id": workspace_id,
        "lease_token": bundle.lease_token,
        "status": "claiming",
    }


def _validated_daemon_process_fields(
    *,
    pid: int,
    process_identity: ProcessIdentity,
    containment_nonce: str,
) -> dict[str, Any]:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError("A daemon process id must be a positive integer.")
    if not isinstance(process_identity, ProcessIdentity):
        raise ValueError("A daemon owner write requires a ProcessIdentity.")
    if process_identity.pid != pid:
        raise ValueError("The daemon process id must match its verified process identity.")
    if not isinstance(process_identity.start_key, str) or not process_identity.start_key.strip():
        raise ValueError("A daemon process identity requires a non-empty start key.")
    if not isinstance(process_identity.executable_path, str) or not process_identity.executable_path.strip():
        raise ValueError("A daemon process identity requires a non-empty executable path.")
    for field_name, value in (
        ("process group id", process_identity.process_group_id),
        ("process session id", process_identity.process_session_id),
    ):
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
            raise ValueError(f"A daemon {field_name} must be a positive integer or null.")
    if not isinstance(containment_nonce, str) or _CONTAINMENT_NONCE_PATTERN.fullmatch(containment_nonce) is None:
        raise ValueError("A daemon containment nonce must be exactly 64 lowercase hexadecimal characters.")
    return {
        "pid": process_identity.pid,
        "process_start_key": process_identity.start_key,
        "process_executable_path": process_identity.executable_path,
        "process_group_id": process_identity.process_group_id,
        "process_session_id": process_identity.process_session_id,
        "containment_nonce": containment_nonce,
    }


def daemon_process_lease_metadata(
    process_identity: ProcessIdentity,
    containment_nonce: str,
) -> dict[str, object]:
    """Serialize one verified process identity into canonical flat lease fields.

    Args:
        process_identity: Immutable operating-system identity for the daemon process.
        containment_nonce: Canonical 256-bit nonce naming its containment boundary.

    Returns:
        Canonical flat fields suitable for lease, status, or local control metadata.

    Raises:
        ValueError: If the identity or containment nonce is incomplete or malformed.
    """
    if not isinstance(process_identity, ProcessIdentity):
        raise ValueError("A daemon owner write requires a ProcessIdentity.")
    return _validated_daemon_process_fields(
        pid=process_identity.pid,
        process_identity=process_identity,
        containment_nonce=containment_nonce,
    )


def daemon_process_lease_identity(metadata: Mapping[str, Any]) -> DaemonProcessLeaseIdentity:
    """Reconstruct the complete verified daemon identity from one persisted lease row.

    Args:
        metadata: Lease metadata returned by :func:`read_lease_metadata`.

    Returns:
        The strict process identity and containment nonce recorded by the owner.

    Raises:
        WorkspaceStateCorruptError: If any identity field is missing or malformed.
    """
    required_fields = {
        "pid",
        "process_start_key",
        "process_executable_path",
        "process_group_id",
        "process_session_id",
        "containment_nonce",
    }
    missing_fields = sorted(required_fields.difference(metadata))
    if missing_fields:
        raise WorkspaceStateCorruptError(
            "Daemon lease metadata is missing required process identity fields: "
            + ", ".join(missing_fields)
            + "."
        )
    pid = metadata["pid"]
    process_identity = ProcessIdentity(
        pid=pid,
        start_key=metadata["process_start_key"],
        executable_path=metadata["process_executable_path"],
        process_group_id=metadata["process_group_id"],
        process_session_id=metadata["process_session_id"],
    )
    containment_nonce = metadata["containment_nonce"]
    try:
        _validated_daemon_process_fields(
            pid=pid,
            process_identity=process_identity,
            containment_nonce=containment_nonce,
        )
    except (TypeError, ValueError) as exc:
        raise WorkspaceStateCorruptError("Daemon lease metadata has an invalid process identity.") from exc
    return DaemonProcessLeaseIdentity(
        process_identity=process_identity,
        containment_nonce=containment_nonce,
    )


def _read_lease_metadata_from_bundle(
    bundle: WorkspaceBundlePaths,
    *,
    workspace_id: str,
) -> dict[str, Any]:
    metadata = (
        _read_single_row_parquet(bundle.lease_metadata_path)
        if _assert_regular_file(
            bundle.lease_metadata_path,
            workspace_id=workspace_id,
            description="lease metadata",
            allow_missing=True,
        )
        else None
    )
    if metadata is None:
        return _lease_metadata_placeholder(bundle, workspace_id=workspace_id)
    if metadata.get("workspace_id") != workspace_id or metadata.get("lease_token") != bundle.lease_token:
        raise WorkspaceStateCorruptError(
            f"Workspace {workspace_id!r} lease metadata does not match its marker token."
        )
    daemon_process_lease_identity(metadata)
    return metadata


def read_lease_metadata(paths: WorkspacePaths) -> dict[str, Any] | None:
    """Return lease metadata, preserving leased/unknown state during initial claim."""
    retry_count = _PARQUET_READ_RETRIES
    last_corruption: WorkspaceStateCorruptError | None = None
    for attempt in range(retry_count):
        bundle = resolve_workspace_bundle(paths)
        if bundle is None or bundle.state == "available":
            return None
        try:
            metadata = _read_lease_metadata_from_bundle(bundle, workspace_id=paths.workspace_id)
        except WorkspaceStateCorruptError as exc:
            last_corruption = exc
            _wait_before_snapshot_retry(attempt, retry_count=retry_count)
            continue
        final_bundle = resolve_workspace_bundle(paths)
        if final_bundle is not None and final_bundle.topology_signature == bundle.topology_signature:
            return metadata
        _wait_before_snapshot_retry(attempt, retry_count=retry_count)
    if last_corruption is not None:
        raise last_corruption
    raise WorkspaceTransitionInProgressError(
        f"Workspace {paths.workspace_id!r} lease metadata changed during every read."
    )


def _lease_bundle_is_stale(
    bundle: WorkspaceBundlePaths,
    *,
    workspace_id: str,
    stale_after_seconds: float,
) -> bool:
    metadata = _read_lease_metadata_from_bundle(bundle, workspace_id=workspace_id)
    parsed = parse_utc_text(str(metadata.get("last_checkpoint_at_utc")))
    if parsed is None:
        try:
            modified_at = datetime.fromtimestamp(bundle.root.stat().st_mtime, tz=UTC)
        except OSError:
            return False
        parsed = modified_at
    return datetime.now(UTC) - parsed > timedelta(seconds=max(stale_after_seconds, 0.0))


def lease_is_stale(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    stale_after_seconds: float,
) -> bool:
    """Return whether one exact current token has exceeded its heartbeat deadline."""
    bundle = assert_workspace_lease(paths, lease_token)
    return _lease_bundle_is_stale(
        bundle,
        workspace_id=paths.workspace_id,
        stale_after_seconds=stale_after_seconds,
    )


def recover_stale_workspace(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    machine_id: str,
    stale_after_seconds: float,
) -> bool:
    """Fence, revalidate, and recover one exact stale lease to available state."""
    token = _validate_lease_token(lease_token)
    with _topology_lock(paths):
        _repair_abandoned_recovery_locked(paths)
        current = assert_workspace_lease(paths, token)
        if not _lease_bundle_is_stale(
            current,
            workspace_id=paths.workspace_id,
            stale_after_seconds=stale_after_seconds,
        ):
            return False
        recovery_path = _recovery_bundle_path(paths, token)
        current.root.rename(recovery_path)
        recovery_bundle = _bundle_paths(root=recovery_path, state="leased", lease_token=token)
        try:
            if not _lease_bundle_is_stale(
                recovery_bundle,
                workspace_id=paths.workspace_id,
                stale_after_seconds=stale_after_seconds,
            ):
                recovery_path.rename(current.root)
                return False
            available = _available_bundle_path(paths)
            if available.exists():
                raise WorkspaceStateCorruptError(
                    f"Workspace {paths.workspace_id!r} recovery found an available bundle."
                )
            recovery_path.rename(available)
        except BaseException:
            if recovery_path.exists() and not current.root.exists() and not _available_bundle_path(paths).exists():
                recovery_path.rename(current.root)
            raise
        _write_recovery_tombstone(paths, lease_token=token, machine_id=machine_id)
        return True


def _write_recovery_tombstone(paths: WorkspacePaths, *, lease_token: str, machine_id: str) -> None:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    tombstone = paths.stale_markers_dir / f"{paths.workspace_id}__{lease_token}__{timestamp}.json"
    try:
        tombstone.write_text(
            json.dumps(
                {
                    "workspace_id": paths.workspace_id,
                    "lease_token": lease_token,
                    "recovered_by_machine_id": machine_id,
                    "recovered_at_utc": datetime.now(UTC).isoformat(),
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    except OSError:
        pass


def _metadata_row(
    *,
    snapshot_generation_id: str | None,
    workspace_id: str,
    lease_token: str,
    machine_id: str,
    host_name: str,
    daemon_id: str,
    pid: int,
    process_identity: ProcessIdentity,
    containment_nonce: str,
    status: str,
    started_at_utc: str,
    last_checkpoint_at_utc: str,
    app_version: str | None,
) -> dict[str, Any]:
    process_fields = _validated_daemon_process_fields(
        pid=pid,
        process_identity=process_identity,
        containment_nonce=containment_nonce,
    )
    return {
        "snapshot_generation_id": snapshot_generation_id,
        "workspace_id": workspace_id,
        "lease_token": lease_token,
        "machine_id": machine_id,
        "host_name": host_name,
        "daemon_id": daemon_id,
        **process_fields,
        "status": status,
        "last_checkpoint_at_utc": last_checkpoint_at_utc,
        "started_at_utc": started_at_utc,
        "app_version": app_version,
    }


def checkpoint_workspace_state(
    paths: WorkspacePaths,
    ledger: RuntimeSnapshotStore,
    *,
    lease_token: str,
    workspace_id: str,
    machine_id: str,
    host_name: str,
    daemon_id: str,
    pid: int,
    process_identity: ProcessIdentity,
    containment_nonce: str,
    status: str,
    started_at_utc: str,
    last_checkpoint_at_utc: str,
    app_version: str | None,
    heartbeat_interval_seconds: float | None = None,
) -> str:
    """Heartbeat first, then atomically publish a token-fenced runtime snapshot."""
    current_generation = read_runtime_snapshot_generation(paths, lease_token=lease_token)
    _write_lease_metadata_owned(
        paths,
        lease_token=lease_token,
        row=_metadata_row(
            snapshot_generation_id=current_generation,
            workspace_id=workspace_id,
            lease_token=lease_token,
            machine_id=machine_id,
            host_name=host_name,
            daemon_id=daemon_id,
            pid=pid,
            process_identity=process_identity,
            containment_nonce=containment_nonce,
            status=status,
            started_at_utc=started_at_utc,
            last_checkpoint_at_utc=last_checkpoint_at_utc,
            app_version=app_version,
        ),
    )
    heartbeat_stop = threading.Event()
    heartbeat_errors: list[Exception] = []
    heartbeat_thread: threading.Thread | None = None
    heartbeat_interval = max(float(heartbeat_interval_seconds or 0.0), 0.0)
    if heartbeat_interval > 0.0:

        def renew_lease_heartbeat() -> None:
            while not heartbeat_stop.wait(heartbeat_interval):
                try:
                    _write_lease_metadata_owned(
                        paths,
                        lease_token=lease_token,
                        row=_metadata_row(
                            snapshot_generation_id=current_generation,
                            workspace_id=workspace_id,
                            lease_token=lease_token,
                            machine_id=machine_id,
                            host_name=host_name,
                            daemon_id=daemon_id,
                            pid=pid,
                            process_identity=process_identity,
                            containment_nonce=containment_nonce,
                            status=status,
                            started_at_utc=started_at_utc,
                            last_checkpoint_at_utc=datetime.now(UTC).isoformat(),
                            app_version=app_version,
                        ),
                    )
                except Exception as exc:  # pragma: no cover - asserted through the owner call
                    heartbeat_errors.append(exc)
                    heartbeat_stop.set()
                    return

        heartbeat_thread = threading.Thread(
            target=renew_lease_heartbeat,
            name="data-engine-lease-heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()
    snapshot_generation_id = uuid4().hex
    try:
        runs, step_runs, logs, file_states = ledger.snapshots.export()
        _publish_shared_runtime_snapshot(
            paths,
            lease_token=lease_token,
            snapshot_generation_id=snapshot_generation_id,
            runs=runs,
            step_runs=step_runs,
            logs=logs,
            file_states=file_states,
        )
    finally:
        heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join()
    if heartbeat_errors:
        raise heartbeat_errors[0]
    completed_checkpoint_at_utc = datetime.now(UTC).isoformat()
    _write_lease_metadata_owned(
        paths,
        lease_token=lease_token,
        row=_metadata_row(
            snapshot_generation_id=snapshot_generation_id,
            workspace_id=workspace_id,
            lease_token=lease_token,
            machine_id=machine_id,
            host_name=host_name,
            daemon_id=daemon_id,
            pid=pid,
            process_identity=process_identity,
            containment_nonce=containment_nonce,
            status=status,
            started_at_utc=started_at_utc,
            last_checkpoint_at_utc=completed_checkpoint_at_utc,
            app_version=app_version,
        ),
    )
    return snapshot_generation_id


def write_lease_metadata(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    workspace_id: str,
    machine_id: str,
    host_name: str,
    daemon_id: str,
    pid: int,
    process_identity: ProcessIdentity,
    containment_nonce: str,
    status: str,
    started_at_utc: str,
    last_checkpoint_at_utc: str,
    app_version: str | None,
) -> None:
    """Write a lightweight heartbeat for one exact lease token."""
    snapshot_generation_id = read_runtime_snapshot_generation(paths, lease_token=lease_token)
    _write_lease_metadata_owned(
        paths,
        lease_token=lease_token,
        row=_metadata_row(
            snapshot_generation_id=snapshot_generation_id,
            workspace_id=workspace_id,
            lease_token=lease_token,
            machine_id=machine_id,
            host_name=host_name,
            daemon_id=daemon_id,
            pid=pid,
            process_identity=process_identity,
            containment_nonce=containment_nonce,
            status=status,
            started_at_utc=started_at_utc,
            last_checkpoint_at_utc=last_checkpoint_at_utc,
            app_version=app_version,
        ),
    )


def hydrate_local_runtime_state(paths: WorkspacePaths, ledger: RuntimeSnapshotStore) -> bool:
    """Replace local SQLite runtime tables from the current committed bundle."""
    snapshot_generation_id = read_runtime_snapshot_generation(paths)
    if snapshot_generation_id is None or ledger.snapshots.applied_generation_id() == snapshot_generation_id:
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


def read_runtime_snapshot_generation(
    paths: WorkspacePaths,
    *,
    lease_token: str | None = None,
) -> str | None:
    """Return the committed generation after resolving the bundle consistently."""
    retry_count = _PARQUET_READ_RETRIES
    for attempt in range(retry_count):
        bundle = (
            assert_workspace_lease(paths, lease_token)
            if lease_token is not None
            else resolve_workspace_bundle(paths)
        )
        if bundle is None:
            return None
        snapshot_root_present = _assert_real_directory(
            bundle.snapshot_generations_dir,
            workspace_id=paths.workspace_id,
            description="snapshot root",
            allow_missing=True,
        )
        manifest = (
            _read_snapshot_manifest(bundle.snapshot_manifest_path)
            if _assert_regular_file(
                bundle.snapshot_manifest_path,
                workspace_id=paths.workspace_id,
                description="snapshot manifest",
                allow_missing=True,
            )
            else None
        )
        generation_id = _manifest_generation_id(manifest) if manifest is not None else None
        if generation_id is not None:
            if not snapshot_root_present:
                raise WorkspaceStateCorruptError(
                    f"Workspace {paths.workspace_id!r} snapshot root is missing for its manifest."
                )
            _assert_real_directory(
                bundle.snapshot_generations_dir / generation_id,
                workspace_id=paths.workspace_id,
                description=f"snapshot generation {generation_id!r}",
            )
        final_bundle = (
            assert_workspace_lease(paths, lease_token)
            if lease_token is not None
            else resolve_workspace_bundle(paths)
        )
        if final_bundle is not None and final_bundle.topology_signature == bundle.topology_signature:
            return generation_id
        _wait_before_snapshot_retry(attempt, retry_count=retry_count)
    raise WorkspaceTransitionInProgressError(
        f"Workspace {paths.workspace_id!r} snapshot generation changed location during every read."
    )


def read_control_request(paths: WorkspacePaths) -> dict[str, Any] | None:
    """Return one pending control-request row when present."""
    return _read_single_row_parquet(paths.control_request_path)


def remove_lease_metadata(paths: WorkspacePaths, *, lease_token: str) -> None:
    """Remove metadata only from the exact current token bundle."""
    bundle = assert_workspace_lease(paths, lease_token)
    try:
        bundle.lease_metadata_path.unlink()
    except FileNotFoundError:
        assert_workspace_lease(paths, lease_token)
    _assert_exact_lease_path(paths, lease_token)


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


def remove_control_request(paths: WorkspacePaths) -> None:
    """Delete one pending control-request parquet when present."""
    remove_file_if_exists(paths.control_request_path)


def _publish_shared_runtime_snapshot(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    snapshot_generation_id: str,
    runs: tuple[PersistedRun, ...],
    step_runs: tuple[PersistedStepRun, ...],
    logs: tuple[PersistedLogEntry, ...],
    file_states: tuple[PersistedFileState, ...],
) -> None:
    with _snapshot_publication_lock(paths, lease_token=lease_token):
        _publish_shared_runtime_snapshot_locked(
            paths,
            lease_token=lease_token,
            snapshot_generation_id=snapshot_generation_id,
            runs=runs,
            step_runs=step_runs,
            logs=logs,
            file_states=file_states,
        )


def _publish_shared_runtime_snapshot_locked(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    snapshot_generation_id: str,
    runs: tuple[PersistedRun, ...],
    step_runs: tuple[PersistedStepRun, ...],
    logs: tuple[PersistedLogEntry, ...],
    file_states: tuple[PersistedFileState, ...],
) -> None:
    if _GENERATION_ID_PATTERN.fullmatch(snapshot_generation_id) is None:
        raise ValueError(f"Invalid snapshot generation id: {snapshot_generation_id!r}")
    bundle = assert_workspace_lease(paths, lease_token)
    generations_dir = bundle.snapshot_generations_dir
    if not _assert_real_directory(
        generations_dir,
        workspace_id=paths.workspace_id,
        description="snapshot root",
        allow_missing=True,
    ):
        try:
            generations_dir.mkdir()
        except FileNotFoundError as exc:
            raise WorkspaceLeaseLostError(
                f"Workspace {paths.workspace_id!r} lease disappeared before snapshot staging."
            ) from exc
    _assert_real_directory(
        generations_dir,
        workspace_id=paths.workspace_id,
        description="snapshot root",
    )
    _assert_exact_lease_path(paths, lease_token)
    staging_dir = generations_dir / f".s.{lease_token}.{uuid4().hex[:8]}.tmp"
    committed_dir = generations_dir / snapshot_generation_id
    try:
        staging_dir.mkdir()
    except FileNotFoundError as exc:
        raise WorkspaceLeaseLostError(
            f"Workspace {paths.workspace_id!r} lease disappeared before snapshot staging."
        ) from exc
    _assert_real_directory(
        staging_dir,
        workspace_id=paths.workspace_id,
        description="snapshot staging directory",
    )
    try:
        artifact_paths = _snapshot_artifact_paths(staging_dir)
        _write_runs(
            paths,
            lease_token=lease_token,
            path=artifact_paths["runs"],
            rows=runs,
            snapshot_generation_id=snapshot_generation_id,
        )
        _write_step_runs(
            paths,
            lease_token=lease_token,
            path=artifact_paths["step_runs"],
            rows=step_runs,
            snapshot_generation_id=snapshot_generation_id,
        )
        _write_logs(
            paths,
            lease_token=lease_token,
            path=artifact_paths["logs"],
            rows=logs,
            snapshot_generation_id=snapshot_generation_id,
        )
        _write_file_states(
            paths,
            lease_token=lease_token,
            path=artifact_paths["file_state"],
            rows=file_states,
            snapshot_generation_id=snapshot_generation_id,
        )
        _assert_exact_lease_path(paths, lease_token)
        _replace_owned_path(paths, lease_token=lease_token, source_path=staging_dir, target_path=committed_dir)
        _assert_real_directory(
            committed_dir,
            workspace_id=paths.workspace_id,
            description=f"snapshot generation {snapshot_generation_id!r}",
        )
        bundle = _assert_exact_lease_path(paths, lease_token)
        _write_snapshot_manifest_atomic(
            paths,
            lease_token=lease_token,
            path=bundle.snapshot_manifest_path,
            manifest={
                "format_version": _SNAPSHOT_FORMAT_VERSION,
                "generation_id": snapshot_generation_id,
                "artifacts": dict(_SNAPSHOT_ARTIFACT_NAMES),
            },
        )
    except BaseException:
        _remove_temp_artifact_best_effort(staging_dir)
        try:
            current_generation = read_runtime_snapshot_generation(paths, lease_token=lease_token)
        except WorkspaceLeaseLostError:
            raise
        if committed_dir.is_dir() and current_generation != snapshot_generation_id:
            shutil.rmtree(committed_dir, ignore_errors=True)
        raise
    _garbage_collect_snapshot_generations(
        paths,
        lease_token=lease_token,
        committed_generation_id=snapshot_generation_id,
        protected_generation_ids=frozenset((snapshot_generation_id,)),
    )


def _snapshot_publication_lock(paths: WorkspacePaths, *, lease_token: str) -> threading.RLock:
    _validate_lease_token(lease_token)
    lock_key = str(paths.workspace_state_dir.absolute())
    with _SNAPSHOT_PUBLICATION_LOCKS_GUARD:
        lock = _SNAPSHOT_PUBLICATION_LOCKS.get(lock_key)
        if lock is None:
            lock = threading.RLock()
            _SNAPSHOT_PUBLICATION_LOCKS[lock_key] = lock
        return lock


def _snapshot_artifact_paths(generation_dir: Path) -> dict[str, Path]:
    return {name: generation_dir / filename for name, filename in _SNAPSHOT_ARTIFACT_NAMES.items()}


def _validated_snapshot_artifact_paths(
    paths: WorkspacePaths,
    *,
    generation_dir: Path,
    generation_id: str,
) -> dict[str, Path]:
    _assert_real_directory(
        generation_dir,
        workspace_id=paths.workspace_id,
        description=f"snapshot generation {generation_id!r}",
    )
    artifact_paths = _snapshot_artifact_paths(generation_dir)
    for artifact_name, artifact_path in artifact_paths.items():
        if not _assert_regular_file(
            artifact_path,
            workspace_id=paths.workspace_id,
            description=f"snapshot artifact {artifact_name!r}",
            allow_missing=True,
        ):
            raise FileNotFoundError(artifact_path)
    return artifact_paths


def _write_snapshot_manifest_atomic(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    path: Path,
    manifest: dict[str, Any],
) -> None:
    _assert_exact_lease_path(paths, lease_token)
    temporary_path = path.with_name(f".m.{lease_token}.{uuid4().hex[:16]}.tmp")
    try:
        with temporary_path.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(manifest, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        _replace_owned_path(paths, lease_token=lease_token, source_path=temporary_path, target_path=path)
    except FileNotFoundError as exc:
        raise WorkspaceLeaseLostError(
            f"Workspace {paths.workspace_id!r} lease disappeared during manifest publication."
        ) from exc
    finally:
        remove_file_if_exists(temporary_path)


def _replace_owned_path(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    source_path: Path,
    target_path: Path,
) -> None:
    _assert_exact_lease_path(paths, lease_token)
    try:
        _replace_path_with_retries(source_path, target_path)
    except FileNotFoundError as exc:
        raise WorkspaceLeaseLostError(
            f"Workspace {paths.workspace_id!r} lease disappeared during atomic publication."
        ) from exc
    _assert_exact_lease_path(paths, lease_token)


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
    lease_token: str,
    committed_generation_id: str,
    protected_generation_ids: frozenset[str] = frozenset(),
) -> None:
    bundle = _assert_exact_lease_path(paths, lease_token)
    if not _assert_real_directory(
        bundle.snapshot_generations_dir,
        workspace_id=paths.workspace_id,
        description="snapshot root",
        allow_missing=True,
    ):
        assert_workspace_lease(paths, lease_token)
        return
    try:
        candidates = tuple(bundle.snapshot_generations_dir.iterdir())
    except OSError:
        assert_workspace_lease(paths, lease_token)
        return
    committed_directories: list[tuple[int, Path]] = []
    for candidate in candidates:
        if _GENERATION_ID_PATTERN.fullmatch(candidate.name) is None:
            continue
        if _is_redirecting_path(candidate):
            raise WorkspaceStateCorruptError(
                f"Workspace {paths.workspace_id!r} snapshot generation {candidate.name!r} redirects outside its bundle."
            )
        try:
            candidate_stat = candidate.stat()
        except OSError:
            continue
        if not stat.S_ISDIR(candidate_stat.st_mode):
            raise WorkspaceStateCorruptError(
                f"Workspace {paths.workspace_id!r} snapshot generation {candidate.name!r} is not a directory."
            )
        committed_directories.append((candidate_stat.st_mtime_ns, candidate))
    committed_directories.sort(key=lambda item: item[0], reverse=True)
    manifest = (
        _read_snapshot_manifest(bundle.snapshot_manifest_path)
        if _assert_regular_file(
            bundle.snapshot_manifest_path,
            workspace_id=paths.workspace_id,
            description="snapshot manifest",
            allow_missing=True,
        )
        else None
    )
    manifest_generation_id = _manifest_generation_id(manifest) if manifest is not None else None
    keep_names = {committed_generation_id, *protected_generation_ids}
    if manifest_generation_id is not None:
        keep_names.add(manifest_generation_id)
    older_names = (candidate.name for _, candidate in committed_directories if candidate.name != committed_generation_id)
    for _ in range(max(_SNAPSHOT_GENERATIONS_TO_KEEP - 1, 0)):
        retained_name = next(older_names, None)
        if retained_name is None:
            break
        keep_names.add(retained_name)
    for _, candidate in committed_directories:
        _assert_exact_lease_path(paths, lease_token)
        current_manifest_payload = (
            _read_snapshot_manifest(bundle.snapshot_manifest_path)
            if _assert_regular_file(
                bundle.snapshot_manifest_path,
                workspace_id=paths.workspace_id,
                description="snapshot manifest",
                allow_missing=True,
            )
            else None
        )
        current_manifest = (
            _manifest_generation_id(current_manifest_payload)
            if current_manifest_payload is not None
            else None
        )
        if current_manifest is not None:
            keep_names.add(current_manifest)
        if candidate.name in keep_names:
            continue
        _assert_exact_lease_path(paths, lease_token)
        if _is_redirecting_path(candidate):
            raise WorkspaceStateCorruptError(
                f"Workspace {paths.workspace_id!r} snapshot generation {candidate.name!r} redirects outside its bundle."
            )
        try:
            candidate_stat = candidate.stat()
        except OSError:
            assert_workspace_lease(paths, lease_token)
            continue
        if not stat.S_ISDIR(candidate_stat.st_mode):
            raise WorkspaceStateCorruptError(
                f"Workspace {paths.workspace_id!r} snapshot generation {candidate.name!r} is not a directory."
            )
        try:
            shutil.rmtree(candidate)
        except OSError:
            assert_workspace_lease(paths, lease_token)
            continue
    _scrub_noncurrent_temp_artifacts(
        bundle.root,
        workspace_id=paths.workspace_id,
        lease_token=lease_token,
    )
    assert_workspace_lease(paths, lease_token)


def reset_flow_state(paths: WorkspacePaths, *, lease_token: str, flow_name: str) -> None:
    """Delete one flow from snapshots while holding an exclusive maintenance lease."""
    snapshot = _read_consistent_runtime_snapshot(paths, lease_token=lease_token)
    if snapshot is None:
        return
    removed_run_ids = {run.run_id for run in snapshot.runs if run.flow_name == flow_name}
    _publish_shared_runtime_snapshot(
        paths,
        lease_token=lease_token,
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
        file_states=tuple(
            file_state for file_state in snapshot.file_states if file_state.flow_name != flow_name
        ),
    )


def reset_workspace_state(paths: WorkspacePaths, *, lease_token: str) -> None:
    """Delete shared snapshots while holding an exclusive maintenance lease."""
    assert_workspace_lease(paths, lease_token)
    with _snapshot_publication_lock(paths, lease_token=lease_token):
        bundle = _assert_exact_lease_path(paths, lease_token)
        manifest_present = _assert_regular_file(
            bundle.snapshot_manifest_path,
            workspace_id=paths.workspace_id,
            description="snapshot manifest",
            allow_missing=True,
        )
        snapshots_present = _assert_real_directory(
            bundle.snapshot_generations_dir,
            workspace_id=paths.workspace_id,
            description="snapshot root",
            allow_missing=True,
        )
        if manifest_present:
            remove_file_if_exists(bundle.snapshot_manifest_path)
        if snapshots_present:
            shutil.rmtree(bundle.snapshot_generations_dir)
        _assert_exact_lease_path(paths, lease_token)
    remove_control_request(paths)
    for stale_path in _directory_entries(paths.stale_markers_dir):
        prefix = f"{paths.workspace_id}__"
        if not stale_path.name.startswith(prefix):
            continue
        if stale_path.is_symlink() or stale_path.is_file():
            remove_file_if_exists(stale_path)
        elif stale_path.is_dir():
            shutil.rmtree(stale_path)


def _frame_with_schema(rows: list[dict[str, Any]], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema, infer_schema_length=None)


def _sync_file_contents(path: Path) -> None:
    """Flush a completed file through a descriptor writable on every host."""
    with path.open("r+b") as stream:
        os.fsync(stream.fileno())


def _write_parquet_atomic_owned(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    frame: pl.DataFrame,
    path: Path,
) -> None:
    _assert_exact_lease_path(paths, lease_token)
    temporary_path = path.with_name(f".l.{lease_token}.{uuid4().hex[:16]}.tmp")
    try:
        frame.write_parquet(temporary_path)
        _sync_file_contents(temporary_path)
        _replace_owned_path(paths, lease_token=lease_token, source_path=temporary_path, target_path=path)
    except FileNotFoundError as exc:
        raise WorkspaceLeaseLostError(
            f"Workspace {paths.workspace_id!r} lease disappeared during parquet publication."
        ) from exc
    finally:
        remove_file_if_exists(temporary_path)


def _write_parquet_staged_owned(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    frame: pl.DataFrame,
    path: Path,
) -> None:
    """Write one artifact inside an unpublished, token-owned staging directory."""
    _assert_exact_lease_path(paths, lease_token)
    try:
        frame.write_parquet(path)
        _sync_file_contents(path)
        _assert_exact_lease_path(paths, lease_token)
    except FileNotFoundError as exc:
        raise WorkspaceLeaseLostError(
            f"Workspace {paths.workspace_id!r} lease disappeared during parquet staging."
        ) from exc


def _write_lease_metadata_owned(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    row: dict[str, Any],
) -> None:
    bundle = _assert_exact_lease_path(paths, lease_token)
    _write_parquet_atomic_owned(
        paths,
        lease_token=lease_token,
        frame=_frame_with_schema([row], _LEASE_METADATA_SCHEMA),
        path=bundle.lease_metadata_path,
    )


def _write_runs(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    path: Path,
    rows: tuple[PersistedRun, ...],
    snapshot_generation_id: str,
) -> None:
    _write_parquet_staged_owned(
        paths,
        lease_token=lease_token,
        frame=_frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _RUNS_SCHEMA,
        ),
        path=path,
    )


def _write_step_runs(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    path: Path,
    rows: tuple[PersistedStepRun, ...],
    snapshot_generation_id: str,
) -> None:
    _write_parquet_staged_owned(
        paths,
        lease_token=lease_token,
        frame=_frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _STEP_RUNS_SCHEMA,
        ),
        path=path,
    )


def _write_logs(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    path: Path,
    rows: tuple[PersistedLogEntry, ...],
    snapshot_generation_id: str,
) -> None:
    _write_parquet_staged_owned(
        paths,
        lease_token=lease_token,
        frame=_frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _LOGS_SCHEMA,
        ),
        path=path,
    )


def _write_file_states(
    paths: WorkspacePaths,
    *,
    lease_token: str,
    path: Path,
    rows: tuple[PersistedFileState, ...],
    snapshot_generation_id: str,
) -> None:
    _write_parquet_staged_owned(
        paths,
        lease_token=lease_token,
        frame=_frame_with_schema(
            [{"snapshot_generation_id": snapshot_generation_id, **asdict(row)} for row in rows],
            _FILE_STATE_SCHEMA,
        ),
        path=path,
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
    generation_ids = [
        value
        for value in frame.get_column("snapshot_generation_id").drop_nulls().unique().to_list()
        if isinstance(value, str) and value.strip()
    ]
    if len(generation_ids) != 1:
        return None
    return generation_ids[0]


def _drop_snapshot_generation_id(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized.pop("snapshot_generation_id", None)
    return normalized


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
    return payload if isinstance(payload, dict) else None


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
    lease_token: str | None = None,
    retries: int = _PARQUET_READ_RETRIES,
) -> _CommittedRuntimeSnapshot | None:
    retry_count = max(retries, 1)
    for attempt in range(retry_count):
        bundle = (
            assert_workspace_lease(paths, lease_token)
            if lease_token is not None
            else resolve_workspace_bundle(paths)
        )
        if bundle is None:
            return None
        snapshot_root_present = _assert_real_directory(
            bundle.snapshot_generations_dir,
            workspace_id=paths.workspace_id,
            description="snapshot root",
            allow_missing=True,
        )
        manifest = (
            _read_snapshot_manifest(bundle.snapshot_manifest_path)
            if _assert_regular_file(
                bundle.snapshot_manifest_path,
                workspace_id=paths.workspace_id,
                description="snapshot manifest",
                allow_missing=True,
            )
            else None
        )
        generation_id = _manifest_generation_id(manifest) if manifest is not None else None
        if generation_id is None:
            return None
        if not snapshot_root_present:
            raise WorkspaceStateCorruptError(
                f"Workspace {paths.workspace_id!r} snapshot root is missing for its manifest."
            )
        try:
            artifact_paths = _validated_snapshot_artifact_paths(
                paths,
                generation_dir=bundle.snapshot_generations_dir / generation_id,
                generation_id=generation_id,
            )
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
        final_bundle = (
            assert_workspace_lease(paths, lease_token)
            if lease_token is not None
            else resolve_workspace_bundle(paths)
        )
        final_manifest = (
            _read_snapshot_manifest(final_bundle.snapshot_manifest_path)
            if final_bundle is not None
            and _assert_regular_file(
                final_bundle.snapshot_manifest_path,
                workspace_id=paths.workspace_id,
                description="snapshot manifest",
                allow_missing=True,
            )
            else None
        )
        if (
            final_bundle is None
            or final_bundle.topology_signature != bundle.topology_signature
            or final_manifest != manifest
        ):
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
    if attempt + 1 < retry_count:
        time.sleep((0.02, 0.05, 0.1)[min(attempt, 2)])


__all__ = [
    "assert_workspace_lease",
    "checkpoint_workspace_state",
    "claim_daemon_workspace",
    "claim_workspace",
    "daemon_process_lease_identity",
    "daemon_process_lease_metadata",
    "DaemonProcessLeaseIdentity",
    "hydrate_local_runtime_state",
    "initialize_workspace_state",
    "lease_is_stale",
    "read_control_request",
    "read_lease_metadata",
    "read_runtime_snapshot_generation",
    "recover_stale_workspace",
    "release_workspace",
    "remove_control_request",
    "remove_lease_metadata",
    "reset_flow_state",
    "reset_workspace_state",
    "resolve_workspace_bundle",
    "RuntimeSnapshotStore",
    "WorkspaceBundlePaths",
    "WorkspaceLeaseLostError",
    "WorkspaceStateCorruptError",
    "WorkspaceTransitionInProgressError",
    "WorkspaceUnavailableForResetError",
    "workspace_lease_operation",
    "write_control_request",
    "write_lease_metadata",
]
