"""Runtime/logging helpers for the terminal UI."""

from __future__ import annotations

import logging
from queue import Queue

from data_engine.domain import FlowLogEntry, format_log_line, parse_runtime_event


class QueueLogHandler(logging.Handler):
    """Logging handler that forwards runtime lines into a queue."""

    def __init__(self, queue: Queue[FlowLogEntry]) -> None:
        super().__init__(level=logging.INFO)
        self.queue = queue

    def emit(self, record: logging.LogRecord) -> None:
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


__all__ = ["QueueLogHandler"]
