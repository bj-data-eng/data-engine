"""Runtime store protocols used by service and operator boundaries."""

from __future__ import annotations

from typing import Protocol

from data_engine.domain.source_state import SourceSignature
from data_engine.runtime.ledger_models import PersistedFileState, PersistedLogEntry, PersistedRun, PersistedStepRun


class RuntimeRunReader(Protocol):
    def get(self, run_id: str): ...

    def list(self, *, flow_name: str | None = None) -> tuple[PersistedRun, ...]: ...


class RuntimeStepOutputReader(Protocol):
    def get(self, step_run_id: int): ...

    def list_for_run(self, run_id: str) -> tuple[PersistedStepRun, ...]: ...

    def list(
        self,
        *,
        flow_name: str | None = None,
        after_id: int | None = None,
    ) -> tuple[PersistedStepRun, ...]: ...


class RuntimeLogReader(Protocol):
    def list(
        self,
        *,
        flow_name: str | None = None,
        run_id: str | None = None,
        after_id: int | None = None,
    ) -> tuple[PersistedLogEntry, ...]: ...


class RuntimeSourceSignatureStore(Protocol):
    def list_file_states(self, *, flow_name: str | None = None) -> tuple[PersistedFileState, ...]: ...

    def upsert_file_state(
        self,
        *,
        flow_name: str,
        signature: SourceSignature,
        status: str,
        run_id: str | None = None,
        finished_at_utc: str | None = None,
        error_text: str | None = None,
    ) -> None: ...


class RuntimeExecutionStateWriter(Protocol):
    def record_run_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        group_name: str,
        source_path: str | None,
        started_at_utc: str | None = None,
    ) -> None: ...

    def record_step_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        step_label: str,
        started_at_utc: str | None = None,
    ) -> int: ...


class RuntimeCacheStore(Protocol):
    """Cache/runtime-history store surface used above the runtime store layer."""

    runs: RuntimeRunReader
    step_outputs: RuntimeStepOutputReader
    logs: RuntimeLogReader
    source_signatures: RuntimeSourceSignatureStore
    execution_state: RuntimeExecutionStateWriter

    def close(self) -> None: ...


class RuntimeClientSessionStore(Protocol):
    def upsert(self, **kwargs: object) -> None: ...

    def remove(self, client_id: str) -> None: ...

    def remove_for_process(self, *, workspace_id: str, client_kind: str, pid: int) -> None: ...

    def count_live(self, workspace_id: str, *, exclude_client_id: str | None = None) -> int: ...


class RuntimeControlStore(Protocol):
    """Control/session store surface used by operator bindings."""

    client_sessions: RuntimeClientSessionStore

    def close(self) -> None: ...


__all__ = [
    "RuntimeCacheStore",
    "RuntimeClientSessionStore",
    "RuntimeControlStore",
    "RuntimeExecutionStateWriter",
    "RuntimeLogReader",
    "RuntimeRunReader",
    "RuntimeSourceSignatureStore",
    "RuntimeStepOutputReader",
]
