"""Persisted runtime ledger record models."""

from __future__ import annotations

from dataclasses import dataclass

from data_engine.domain.time import parse_utc_text


def elapsed_seconds(started_at_utc: str, finished_at_utc: str | None) -> float | None:
    """Return elapsed seconds derived from persisted UTC text timestamps."""
    started = parse_utc_text(started_at_utc)
    finished = parse_utc_text(finished_at_utc)
    if started is None or finished is None:
        return None
    return max((finished - started).total_seconds(), 0.0)


@dataclass(frozen=True)
class PersistedRun:
    """One persisted runtime run summary."""

    run_id: str
    flow_name: str
    group_name: str
    source_path: str | None
    status: str
    started_at_utc: str
    finished_at_utc: str | None
    error_text: str | None

    @property
    def elapsed_seconds(self) -> float | None:
        return elapsed_seconds(self.started_at_utc, self.finished_at_utc)


@dataclass(frozen=True)
class PersistedStepRun:
    """One persisted runtime step execution."""

    id: int
    run_id: str
    flow_name: str
    step_label: str
    status: str
    started_at_utc: str
    finished_at_utc: str | None
    elapsed_ms: int | None
    error_text: str | None
    output_path: str | None


@dataclass(frozen=True)
class PersistedLogEntry:
    """One persisted runtime log line."""

    id: int
    run_id: str | None
    flow_name: str | None
    step_label: str | None
    level: str
    message: str
    created_at_utc: str


@dataclass(frozen=True)
class PersistedFileState:
    """One persisted current file freshness row."""

    flow_name: str
    source_path: str
    mtime_ns: int
    size_bytes: int
    last_success_run_id: str | None
    last_success_at_utc: str | None
    last_status: str
    last_error_text: str | None


@dataclass(frozen=True)
class PersistedDaemonState:
    """One persisted daemon metadata row."""

    workspace_id: str
    pid: int
    endpoint_kind: str
    endpoint_path: str
    started_at_utc: str
    last_checkpoint_at_utc: str
    status: str
    app_root: str
    workspace_root: str
    version_text: str | None


@dataclass(frozen=True)
class PersistedClientSession:
    """One persisted local client session row."""

    client_id: str
    workspace_id: str
    client_kind: str
    pid: int
    started_at_utc: str
    updated_at_utc: str


__all__ = [
    "PersistedClientSession",
    "PersistedDaemonState",
    "PersistedFileState",
    "PersistedLogEntry",
    "PersistedRun",
    "PersistedStepRun",
    "elapsed_seconds",
]
