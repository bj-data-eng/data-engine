"""Shared run-group presentation helpers across operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass

from data_engine.domain import FlowLogEntry, FlowRunState, RunDetailState
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
        detail = RunDetailState.from_run(run_state)
        return cls(
            primary_label=detail.display_label,
            source_label=detail.source_label,
            status_text=detail.status.title(),
            status_visual_state=_status_visual_state(detail.status),
            duration_text=format_seconds(detail.elapsed_seconds) if detail.elapsed_seconds is not None else None,
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
    has_source = event.source_label not in {"", "-"}
    if event.step_name is None:
        if has_source:
            return f"{flow_name} &gt; {source_label} &gt; <i>{status}</i>"
        return f"{flow_name} &gt; <i>{status}</i>"
    step_name = escape(event.step_name.replace(":", "::", 1))
    if has_source:
        return f"{flow_name} &gt; {source_label} &gt; <b>{step_name}</b> - <i>{status}</i>"
    return f"{flow_name} &gt; <b>{step_name}</b> - <i>{status}</i>"


def _status_visual_state(status: str) -> str:
    if status in {"failed", "stopped"}:
        return "failed"
    if status == "started":
        return "started"
    return "finished"


__all__ = ["RunGroupDisplay", "format_raw_log_message"]
