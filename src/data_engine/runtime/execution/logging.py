"""Runtime log and ledger emission helpers for authored flows."""

from __future__ import annotations

import logging
from pathlib import Path
from queue import Empty, Queue
import threading
import weakref
from typing import Protocol

from data_engine.runtime.ledger_models import PersistedLogEntry
from data_engine.domain.time import utcnow_text

LOGGER = logging.getLogger(__name__)


class RuntimeLogSink(Protocol):
    """Interface for persisted runtime log writes."""

    def append(
        self,
        *,
        level: str,
        message: str,
        created_at_utc: str,
        run_id: str | None = None,
        flow_name: str | None = None,
        step_label: str | None = None,
    ) -> None:
        """Persist one runtime log line."""


class _RuntimeLogBatchSink(RuntimeLogSink, Protocol):
    """Optional batch append contract for persisted runtime log sinks."""

    def append_many(self, rows: tuple[PersistedLogEntry, ...]) -> None:
        """Persist multiple runtime log rows in one batch."""


class _SharedQueuedRuntimeLogSink:
    """Own one background log writer for a shared persisted runtime log sink."""

    def __init__(
        self,
        log_sink: RuntimeLogSink,
        *,
        flush_interval_seconds: float = 0.05,
        max_batch_size: int = 100,
    ) -> None:
        self.log_sink = log_sink
        self.flush_interval_seconds = flush_interval_seconds
        self.max_batch_size = max(int(max_batch_size), 1)
        self._queue: Queue[PersistedLogEntry | None] = Queue()
        self._lock = threading.RLock()
        self._refcount = 0
        self._closed = False
        self._failure: Exception | None = None
        self._worker = threading.Thread(target=self._run, name="data-engine-log-writer", daemon=True)
        self._worker.start()

    def acquire(self) -> None:
        with self._lock:
            self._raise_if_failed()
            if self._closed:
                raise RuntimeError("Queued runtime log sink is already closed.")
            self._refcount += 1

    def release(self) -> None:
        should_close = False
        with self._lock:
            if self._refcount > 0:
                self._refcount -= 1
            should_close = self._refcount == 0 and not self._closed
            if should_close:
                self._closed = True
        if should_close:
            self._queue.put(None)
            self._worker.join()
            self._raise_if_failed()

    def append(self, row: PersistedLogEntry) -> None:
        with self._lock:
            self._raise_if_failed()
            if self._closed:
                raise RuntimeError("Queued runtime log sink is already closed.")
        self._queue.put(row)

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def _run(self) -> None:
        try:
            while True:
                row = self._queue.get()
                if row is None:
                    break
                batch = [row]
                self._drain_batch(batch)
                self._append_batch(tuple(batch))
            self._flush_remaining()
        except Exception as exc:  # pragma: no cover - failure path depends on sink faults
            with self._lock:
                self._failure = exc

    def _drain_batch(self, batch: list[PersistedLogEntry]) -> None:
        while len(batch) < self.max_batch_size:
            try:
                row = self._queue.get(timeout=self.flush_interval_seconds)
            except Empty:
                return
            if row is None:
                self._queue.put(None)
                return
            batch.append(row)

    def _flush_remaining(self) -> None:
        batch: list[PersistedLogEntry] = []
        while True:
            try:
                row = self._queue.get_nowait()
            except Empty:
                break
            if row is None:
                continue
            batch.append(row)
            if len(batch) >= self.max_batch_size:
                self._append_batch(tuple(batch))
                batch = []
        if batch:
            self._append_batch(tuple(batch))

    def _append_batch(self, rows: tuple[PersistedLogEntry, ...]) -> None:
        if not rows:
            return
        batch_sink = self.log_sink if hasattr(self.log_sink, "append_many") else None
        if batch_sink is not None:
            batch_sink.append_many(rows)
            return
        for row in rows:
            self.log_sink.append(
                level=row.level,
                message=row.message,
                created_at_utc=row.created_at_utc,
                run_id=row.run_id,
                flow_name=row.flow_name,
                step_label=row.step_label,
            )

    def _raise_if_failed(self) -> None:
        if self._failure is not None:
            raise RuntimeError("Queued runtime log sink failed.") from self._failure


_SHARED_QUEUED_SINKS: weakref.WeakKeyDictionary[object, _SharedQueuedRuntimeLogSink] = weakref.WeakKeyDictionary()
_SHARED_QUEUED_SINKS_LOCK = threading.RLock()


class QueuedRuntimeLogSinkHandle:
    """Reference-counted handle for one shared queued runtime log sink."""

    def __init__(self, shared_sink: _SharedQueuedRuntimeLogSink) -> None:
        self._shared_sink = shared_sink
        self._closed = False
        self._shared_sink.acquire()

    def append(
        self,
        *,
        level: str,
        message: str,
        created_at_utc: str,
        run_id: str | None = None,
        flow_name: str | None = None,
        step_label: str | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("Queued runtime log sink handle is already closed.")
        self._shared_sink.append(
            PersistedLogEntry(
                id=-1,
                run_id=run_id,
                flow_name=flow_name,
                step_label=step_label,
                level=level,
                message=message,
                created_at_utc=created_at_utc,
            )
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._shared_sink.release()


def acquire_queued_runtime_log_sink(
    log_sink: RuntimeLogSink,
    *,
    flush_interval_seconds: float = 0.05,
    max_batch_size: int = 100,
) -> QueuedRuntimeLogSinkHandle:
    """Return a shared queued log-sink handle for one persisted runtime log repository."""
    with _SHARED_QUEUED_SINKS_LOCK:
        shared_sink = _SHARED_QUEUED_SINKS.get(log_sink)
        if shared_sink is None or shared_sink.closed:
            shared_sink = _SharedQueuedRuntimeLogSink(
                log_sink,
                flush_interval_seconds=flush_interval_seconds,
                max_batch_size=max_batch_size,
            )
            _SHARED_QUEUED_SINKS[log_sink] = shared_sink
        return QueuedRuntimeLogSinkHandle(shared_sink)


class RuntimeLogEmitter:
    """Own runtime log persistence and logger emission."""

    def __init__(self, log_sink: RuntimeLogSink, *, workspace_id: str | None = None) -> None:
        self.log_sink = log_sink
        self.workspace_id = str(workspace_id).strip() if workspace_id is not None else None

    def log_runtime_message(
        self,
        message: str,
        *,
        level: str,
        run_id: str | None,
        flow_name: str | None,
        step_label: str | None = None,
        exc_info: bool = False,
    ) -> None:
        created_at_utc = utcnow_text()
        self.log_sink.append(
            level=level.upper(),
            message=message,
            created_at_utc=created_at_utc,
            run_id=run_id,
            flow_name=flow_name,
            step_label=step_label,
        )
        logger_method = LOGGER.error if level == "error" else LOGGER.info
        extra: dict[str, object] | None = None
        if self.workspace_id is not None:
            extra = {"workspace_id": self.workspace_id}
        logger_method(message, exc_info=exc_info, extra=extra)

    def log_flow_event(
        self,
        run_id: str,
        flow_name: str,
        source_path: Path | None,
        *,
        status: str,
        elapsed: float | None = None,
        level: str = "info",
        exc_info: bool = False,
    ) -> None:
        message = f"run={run_id} flow={flow_name} source={source_path} status={status}"
        if elapsed is not None:
            message = f"{message} elapsed={elapsed:.6f}"
        self.log_runtime_message(message, level=level, run_id=run_id, flow_name=flow_name, exc_info=exc_info)

    def log_step_event(
        self,
        run_id: str,
        flow_name: str,
        step_label: str,
        source_path: Path | None,
        *,
        status: str,
        elapsed: float | None = None,
        level: str = "info",
        exc_info: bool = False,
    ) -> None:
        message = f"run={run_id} flow={flow_name} step={step_label} source={source_path} status={status}"
        if elapsed is not None:
            message = f"{message} elapsed={elapsed:.6f}"
        self.log_runtime_message(
            message,
            level=level,
            run_id=run_id,
            flow_name=flow_name,
            step_label=step_label,
            exc_info=exc_info,
        )


__all__ = [
    "QueuedRuntimeLogSinkHandle",
    "RuntimeLogEmitter",
    "RuntimeLogSink",
    "acquire_queued_runtime_log_sink",
]
