"""Shared plain-text rendering helpers for terminal-style surfaces."""

from __future__ import annotations

from data_engine.domain import FlowRunState, OperationSessionState, RunDetailState, SelectedFlowDetailState
from data_engine.views.runs import RunGroupDisplay
from data_engine.views.models import QtFlowCard
from data_engine.views.presentation import format_seconds, operation_marker


def pad(value: str, width: int) -> str:
    """Pad or truncate one text cell to a fixed width."""
    text = str(value)
    if len(text) > width:
        if width <= 1:
            return text[:width]
        return text[: width - 1] + "…"
    return text.ljust(width)


def short_datetime(text: str) -> str:
    """Collapse one display timestamp to a shorter day/time string."""
    parts = text.split()
    if len(parts) >= 3:
        return f"{parts[1]} {parts[2]}"
    return text


def format_optional_seconds(seconds: float | None) -> str:
    """Format an optional duration using the shared compact duration style."""
    if seconds is None:
        return "-"
    return format_seconds(seconds)


def run_group_row_text(run_state: FlowRunState) -> str:
    """Render one compact run-group row for list displays."""
    detail = RunDetailState.from_run(run_state)
    display = RunGroupDisplay.from_run(run_state)
    status = display.status_text.upper()
    duration = format_optional_seconds(detail.elapsed_seconds)
    return (
        f"{pad(short_datetime(display.primary_label), 11)} "
        f"{pad(status, 8)} "
        f"{pad(duration, 6)} "
        f"{display.source_label}"
    )


def render_run_group_lines(run_state: FlowRunState) -> tuple[str, ...]:
    """Render one run-group detail block for modal/detail text surfaces."""
    detail = RunDetailState.from_run(run_state)
    display = RunGroupDisplay.from_run(run_state)
    status = display.status_text.upper()
    duration = format_optional_seconds(detail.elapsed_seconds)
    header = (
        f"{pad(display.primary_label, 22)} "
        f"{pad(f'[{status}]', 10)} "
        f"{pad(duration, 5)} "
        f"{display.source_label}"
    )
    lines = [header]
    for row in detail.step_rows:
        elapsed = format_optional_seconds(row.elapsed_seconds)
        lines.append(
            f"  {operation_marker(row.status)} "
            f"{pad(row.step_name, 24)} "
            f"{pad(row.status, 7)} "
            f"{elapsed}"
        )
    return tuple(lines)


def render_operation_lines(card: QtFlowCard, tracker: OperationSessionState) -> tuple[str, ...]:
    """Render one compact step table for terminal/detail displays."""
    detail = SelectedFlowDetailState.from_flow(card, tracker)
    lines: list[str] = ["  Step                     Time", "  -----------------------  -----"]
    for row in detail.operation_rows:
        duration = "-"
        if row.elapsed_seconds is not None:
            duration = format_seconds(row.elapsed_seconds)
        lines.append(
            f"  {pad(row.name, 23)} "
            f"{duration}"
        )
    return tuple(lines)


def render_selected_flow_lines(card: QtFlowCard, tracker: OperationSessionState) -> tuple[str, ...]:
    """Render the selected-flow detail block used by terminal-style surfaces."""
    detail = SelectedFlowDetailState.from_flow(card, tracker)
    lines = [detail.title, "", "Selected Flow", ""]
    lines.extend(render_operation_lines(card, tracker))
    if detail.description:
        lines.extend(["", "Description", f"  {detail.description}"])
    if detail.error:
        lines.extend(["", "Error", f"  {detail.error}"])
    return tuple(lines)


__all__ = [
    "format_optional_seconds",
    "pad",
    "render_operation_lines",
    "render_run_group_lines",
    "render_selected_flow_lines",
    "run_group_row_text",
    "short_datetime",
]
