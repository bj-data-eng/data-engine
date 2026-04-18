"""Daemon-owned runtime-ledger bridge for projection event publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from data_engine.runtime.runtime_db import RuntimeCacheLedger

if TYPE_CHECKING:
    from data_engine.domain.source_state import SourceSignature


class _RuntimeEventPublisher(Protocol):
    """Callable contract for publishing daemon runtime events."""

    def __call__(self, event_type: str) -> None: ...


@dataclass(frozen=True)
class DaemonRuntimeExecutionStatePublisher:
    """Wrap execution-state writes and publish daemon runtime events."""

    delegate: object
    publish_event: _RuntimeEventPublisher

    def record_run_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        group_name: str,
        source_path: str | None,
        started_at_utc: str,
    ) -> None:
        self.delegate.record_run_started(
            run_id=run_id,
            flow_name=flow_name,
            group_name=group_name,
            source_path=source_path,
            started_at_utc=started_at_utc,
        )
        self.publish_event("runtime.run_started")

    def record_run_finished(
        self,
        *,
        run_id: str,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> None:
        self.delegate.record_run_finished(
            run_id=run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )
        self.publish_event("runtime.run_finished")

    def record_step_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        step_label: str,
        started_at_utc: str,
    ) -> int:
        step_run_id = self.delegate.record_step_started(
            run_id=run_id,
            flow_name=flow_name,
            step_label=step_label,
            started_at_utc=started_at_utc,
        )
        self.publish_event("runtime.step_started")
        return step_run_id

    def record_step_finished(
        self,
        *,
        step_run_id: int,
        status: str,
        finished_at_utc: str,
        elapsed_ms: int | None,
        error_text: str | None = None,
        output_path: str | None = None,
    ) -> None:
        self.delegate.record_step_finished(
            step_run_id=step_run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            elapsed_ms=elapsed_ms,
            error_text=error_text,
            output_path=output_path,
        )
        self.publish_event("runtime.step_finished")

    def upsert_file_state(
        self,
        *,
        flow_name: str,
        signature: "SourceSignature",
        status: str,
        run_id: str | None = None,
        finished_at_utc: str | None = None,
        error_text: str | None = None,
    ) -> None:
        self.delegate.upsert_file_state(
            flow_name=flow_name,
            signature=signature,
            status=status,
            run_id=run_id,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )


class DaemonRuntimeCacheProxy:
    """Borrow one daemon runtime cache ledger while publishing projection events."""

    def __init__(self, runtime_cache_ledger: RuntimeCacheLedger, *, publish_event: _RuntimeEventPublisher) -> None:
        self._delegate = runtime_cache_ledger
        self.runs = runtime_cache_ledger.runs
        self.step_outputs = runtime_cache_ledger.step_outputs
        self.logs = runtime_cache_ledger.logs
        self.source_signatures = runtime_cache_ledger.source_signatures
        self.execution_state = DaemonRuntimeExecutionStatePublisher(
            delegate=runtime_cache_ledger.execution_state,
            publish_event=publish_event,
        )

    def close(self) -> None:
        """Keep the borrowed daemon ledger open for the owning service."""
        return


__all__ = ["DaemonRuntimeCacheProxy", "DaemonRuntimeExecutionStatePublisher"]
