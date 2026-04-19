"""Daemon-owned runtime-ledger bridge for projection event publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import TYPE_CHECKING, Protocol

from data_engine.services.runtime_ports import RuntimeCacheStore

if TYPE_CHECKING:
    from data_engine.domain.source_state import SourceSignature


class _RuntimeEventPublisher(Protocol):
    """Callable contract for publishing daemon runtime events."""

    def __call__(self, event_type: str, *, payload: dict[str, Any] | None = None) -> None: ...


@dataclass(frozen=True)
class DaemonRuntimeExecutionStatePublisher:
    """Wrap execution-state writes and publish daemon runtime events."""

    delegate: object
    runtime_cache_ledger: RuntimeCacheStore
    publish_event: _RuntimeEventPublisher

    def record_run_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        group_name: str,
        source_path: str | None,
        started_at_utc: str | None = None,
    ) -> None:
        self.delegate.record_run_started(
            run_id=run_id,
            flow_name=flow_name,
            group_name=group_name,
            source_path=source_path,
            started_at_utc=started_at_utc,
        )
        run = self.runtime_cache_ledger.runs.get(run_id)
        self.publish_event(
            "runtime.run_started",
            payload={
                "run_id": run_id,
                "flow_name": flow_name,
                "group_name": group_name,
                "source_path": source_path,
                "started_at_utc": run.started_at_utc if run is not None else started_at_utc,
            },
        )

    def record_run_finished(
        self,
        *,
        run_id: str,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> None:
        run = self.runtime_cache_ledger.runs.get(run_id)
        self.delegate.record_run_finished(
            run_id=run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )
        self.publish_event(
            "runtime.run_finished",
            payload={
                "run_id": run_id,
                "flow_name": run.flow_name if run is not None else None,
                "group_name": run.group_name if run is not None else None,
                "source_path": run.source_path if run is not None else None,
                "started_at_utc": run.started_at_utc if run is not None else None,
                "finished_at_utc": finished_at_utc,
                "status": status,
                "error_text": error_text,
            },
        )

    def record_step_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        step_label: str,
        started_at_utc: str | None = None,
    ) -> int:
        step_run_id = self.delegate.record_step_started(
            run_id=run_id,
            flow_name=flow_name,
            step_label=step_label,
            started_at_utc=started_at_utc,
        )
        run = self.runtime_cache_ledger.runs.get(run_id)
        step_run = self.runtime_cache_ledger.step_outputs.get(step_run_id)
        self.publish_event(
            "runtime.step_started",
            payload={
                "step_run_id": step_run_id,
                "run_id": run_id,
                "flow_name": flow_name,
                "step_label": step_label,
                "source_path": run.source_path if run is not None else None,
                "started_at_utc": step_run.started_at_utc if step_run is not None else started_at_utc,
            },
        )
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
        step_run = self.runtime_cache_ledger.step_outputs.get(step_run_id)
        run = self.runtime_cache_ledger.runs.get(step_run.run_id) if step_run is not None else None
        self.delegate.record_step_finished(
            step_run_id=step_run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            elapsed_ms=elapsed_ms,
            error_text=error_text,
            output_path=output_path,
        )
        self.publish_event(
            "runtime.step_finished",
            payload={
                "step_run_id": step_run_id,
                "run_id": step_run.run_id if step_run is not None else None,
                "flow_name": step_run.flow_name if step_run is not None else None,
                "step_label": step_run.step_label if step_run is not None else None,
                "source_path": run.source_path if run is not None else None,
                "started_at_utc": step_run.started_at_utc if step_run is not None else None,
                "finished_at_utc": finished_at_utc,
                "status": status,
                "elapsed_ms": elapsed_ms,
                "error_text": error_text,
                "output_path": output_path,
            },
        )

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

    def __init__(self, runtime_cache_ledger: RuntimeCacheStore, *, publish_event: _RuntimeEventPublisher) -> None:
        self._delegate = runtime_cache_ledger
        self.runs = runtime_cache_ledger.runs
        self.step_outputs = runtime_cache_ledger.step_outputs
        self.logs = runtime_cache_ledger.logs
        self.source_signatures = runtime_cache_ledger.source_signatures
        self.execution_state = DaemonRuntimeExecutionStatePublisher(
            delegate=runtime_cache_ledger.execution_state,
            runtime_cache_ledger=runtime_cache_ledger,
            publish_event=publish_event,
        )

    def close(self) -> None:
        """Keep the borrowed daemon ledger open for the owning service."""
        return

    def __getattr__(self, name: str):
        """Forward unknown runtime-ledger attributes to the wrapped cache store."""
        return getattr(self._delegate, name)


__all__ = ["DaemonRuntimeCacheProxy", "DaemonRuntimeExecutionStatePublisher"]
