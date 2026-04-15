"""Runtime wiring helpers for the Data Engine desktop UI."""

from __future__ import annotations

import logging
from queue import Queue

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
            event = parse_runtime_event(record)
            kind = "flow" if event is not None and event.flow_name is not None else "system"
            self.queue.put_nowait(
                FlowLogEntry(
                    line=format_log_line(record),
                    kind=kind,
                    event=event,
                    flow_name=event.flow_name if event is not None else None,
                )
            )
        except Exception:
            self.handleError(record)


class UiSignals(QObject):
    """Cross-thread Qt signals used by background runtime workers."""

    run_finished = Signal(object, object, object)
    runtime_finished = Signal(object, object, object)
    daemon_startup_finished = Signal(bool, str)
    control_action_finished = Signal(str, object)


__all__ = ["FlowLogEntry", "QueueLogHandler", "UiSignals"]
