"""Runtime wiring helpers for the Data Engine desktop UI."""

from __future__ import annotations

import logging
from queue import Empty, Full, Queue

from PySide6.QtCore import QObject, Signal

from data_engine.domain import FlowLogEntry, format_log_line, parse_runtime_event


class QueueLogHandler(logging.Handler):
    """Logging handler that forwards formatted runtime lines into the UI queue."""

    def __init__(self, queue: Queue[FlowLogEntry]) -> None:
        super().__init__(level=logging.INFO)
        self.queue = queue

    def emit(self, record: logging.LogRecord) -> None:
        """Convert one log record into a UI entry and enqueue it."""
        try:
            workspace_id = str(getattr(record, "workspace_id", "") or "").strip() or None
            if workspace_id is None:
                # The GUI no longer treats the shared process logger as an
                # authoritative workspace event bus unless the record is
                # explicitly scoped to a workspace.
                return
            event = parse_runtime_event(record)
            kind = "flow" if event is not None and event.flow_name is not None else "system"
            self._enqueue_entry(
                FlowLogEntry(
                    line=format_log_line(record),
                    kind=kind,
                    event=event,
                    flow_name=event.flow_name if event is not None else None,
                    workspace_id=workspace_id,
                )
            )
        except Exception:
            self.handleError(record)

    def _enqueue_entry(self, entry: FlowLogEntry) -> None:
        """Enqueue one UI log entry while bounding burst memory usage."""
        try:
            self.queue.put_nowait(entry)
            return
        except Full:
            pass
        try:
            self.queue.get_nowait()
        except Empty:
            pass
        self.queue.put_nowait(entry)


class UiSignals(QObject):
    """Cross-thread Qt signals used by background runtime workers."""

    run_finished = Signal(object, object, object)
    runtime_finished = Signal(object, object, object)
    daemon_startup_finished = Signal(bool, str)
    daemon_sync_finished = Signal(object)
    control_action_finished = Signal(str, object)
    daemon_update_available = Signal()
    daemon_update_batch_available = Signal(object)


__all__ = ["FlowLogEntry", "QueueLogHandler", "UiSignals"]
