"""Domain models and parsing helpers for runtime log-entry state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import logging
from pathlib import Path
import re
from typing import Literal

LogKind = Literal["flow", "system"]


@dataclass(frozen=True)
class RuntimeStepEvent:
    """Parsed runtime event derived from one builder log record."""

    flow_name: str
    step_name: str | None
    source_label: str
    status: str
    elapsed_seconds: float | None = None
    run_id: str | None = None
    started_at_utc: str | None = None


@dataclass(frozen=True)
class FlowLogEntry:
    """One runtime log entry captured for operator surfaces."""

    line: str
    kind: LogKind
    event: RuntimeStepEvent | None = None
    flow_name: str | None = None
    workspace_id: str | None = None
    created_at_utc: datetime = field(default_factory=lambda: datetime.now(UTC))
    persisted_id: int | None = None

    @staticmethod
    def format_runtime_message(message: str) -> str:
        """Render a runtime message into a compact operator-facing single line."""
        return format_runtime_message(message)

    def fingerprint(self) -> tuple[object, ...]:
        """Return a stable identity fingerprint for one visible log entry."""
        event = self.event
        event_key = (
            event.run_id,
            event.flow_name,
            event.step_name,
            event.source_label,
            event.status,
            event.elapsed_seconds,
            event.started_at_utc,
        ) if event is not None else None
        created_at_key = None if event is not None and event.run_id is not None else self.created_at_utc
        return (
            self.kind,
            self.flow_name,
            self.line,
            created_at_key,
            event_key,
        )


def short_source_label(value: str | None) -> str:
    """Collapse a source path down to a filename-style label."""
    if value in (None, "None", ""):
        return "-"
    return Path(str(value)).name


def format_runtime_message(message: str) -> str:
    """Render a runtime message into a compact operator-facing single line."""
    step_match = re.search(r"flow=(?P<flow>\S+) step=(?P<step>.+?) source=(?P<source>.+?) status=(?P<status>\S+)", message)
    if step_match is not None:
        source = short_source_label(step_match.group("source"))
        return f"{step_match.group('flow')}  {step_match.group('step')}  {step_match.group('status')}  {source}"

    flow_match = re.search(r"flow=(?P<flow>\S+) source=(?P<source>.+?) status=(?P<status>\S+)", message)
    if flow_match is not None:
        source = short_source_label(flow_match.group("source"))
        return f"{flow_match.group('flow')}  {flow_match.group('status')}  {source}"

    return re.sub(r"/[^ ]+", lambda match: Path(match.group(0)).name, message)


def format_log_line(record: logging.LogRecord) -> str:
    """Render runtime logs into a compact operator-facing single line."""
    return format_runtime_message(record.getMessage())


def parse_runtime_message(message: str) -> RuntimeStepEvent | None:
    """Parse one runtime message into structured flow/step event data when possible."""
    step_match = re.search(
        r"run=(?P<run>\S+) flow=(?P<flow>\S+) step=(?P<step>.+?) source=(?P<source>.+?) status=(?P<status>\S+)(?: elapsed=(?P<elapsed>\S+))?",
        message,
    )
    if step_match is not None:
        elapsed = step_match.group("elapsed")
        return RuntimeStepEvent(
            run_id=step_match.group("run"),
            flow_name=step_match.group("flow"),
            step_name=step_match.group("step"),
            source_label=short_source_label(step_match.group("source")),
            status=step_match.group("status"),
            elapsed_seconds=float(elapsed) if elapsed is not None else None,
        )

    flow_match = re.search(
        r"run=(?P<run>\S+) flow=(?P<flow>\S+) source=(?P<source>.+?) status=(?P<status>\S+)(?: elapsed=(?P<elapsed>\S+))?",
        message,
    )
    if flow_match is not None:
        elapsed = flow_match.group("elapsed")
        return RuntimeStepEvent(
            run_id=flow_match.group("run"),
            flow_name=flow_match.group("flow"),
            step_name=None,
            source_label=short_source_label(flow_match.group("source")),
            status=flow_match.group("status"),
            elapsed_seconds=float(elapsed) if elapsed is not None else None,
        )
    return None


def parse_runtime_event(record: logging.LogRecord) -> RuntimeStepEvent | None:
    """Parse one runtime log record into structured flow/step event data when possible."""
    return parse_runtime_message(record.getMessage())


__all__ = [
    "FlowLogEntry",
    "LogKind",
    "RuntimeStepEvent",
    "format_log_line",
    "format_runtime_message",
    "parse_runtime_event",
    "parse_runtime_message",
    "short_source_label",
]
