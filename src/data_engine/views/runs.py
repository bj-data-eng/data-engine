"""Shared run-group presentation helpers across operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from data_engine.domain import FlowLogEntry, FlowRunState
from data_engine.views.presentation import format_seconds


@dataclass(frozen=True)
class RunGroupDisplay:
    """Canonical GUI-first presentation state for one grouped run."""

    primary_label: str
    source_label: str
    status_text: str
    status_visual_state: str
    duration_text: str | None

    @classmethod
    def from_run(cls, run_state: FlowRunState) -> "RunGroupDisplay":
        status_text = "Running" if run_state.status == "started" else "Stopping" if run_state.status == "stopping" else run_state.status.title()
        duration_seconds = _display_duration_seconds(run_state)
        return cls(
            primary_label=run_state.display_label,
            source_label=run_state.source_label,
            status_text=status_text,
            status_visual_state=_status_visual_state(run_state.status),
            duration_text=format_seconds(duration_seconds) if duration_seconds is not None else None,
        )


def format_raw_log_message(entry: FlowLogEntry) -> str:
    """Return canonical user-facing log text for one raw runtime/log entry."""
    from html import escape

    event = entry.event
    if event is None:
        return escape(entry.line)
    flow_name = escape(event.flow_name)
    source_label = escape(event.source_label)
    status = escape(event.status)
    elapsed = (
        f" ({escape(format_seconds(event.elapsed_seconds))})"
        if event.elapsed_seconds is not None
        else ""
    )
    has_source = event.source_label not in {"", "-"}
    if event.step_name is None:
        if has_source:
            return f"{flow_name} &gt; {source_label} &gt; <i>{status}</i>{elapsed}"
        return f"{flow_name} &gt; <i>{status}</i>{elapsed}"
    step_name = escape(event.step_name.replace(":", "::", 1))
    if has_source:
        return f"{flow_name} &gt; {source_label} &gt; <b>{step_name}</b> - <i>{status}</i>{elapsed}"
    return f"{flow_name} &gt; <b>{step_name}</b> - <i>{status}</i>{elapsed}"


def _status_visual_state(status: str) -> str:
    if status in {"failed", "stopped"}:
        return "failed"
    if status in {"started", "stopping"}:
        return "started"
    return "finished"


def _display_duration_seconds(run_state: FlowRunState) -> float | None:
    if run_state.status in {"started", "stopping"}:
        started_at = (
            run_state.entries[0].created_at_utc
            if run_state.entries
            else (run_state.summary_entry.created_at_utc if run_state.summary_entry is not None else None)
        )
        if started_at is not None:
            now_utc = datetime.now(UTC)
            return max((now_utc - started_at.astimezone(UTC)).total_seconds(), 0.0)
    return run_state.elapsed_seconds


__all__ = ["RunGroupDisplay", "format_raw_log_message"]
