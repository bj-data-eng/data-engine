"""Step-list presentation helpers for the desktop UI."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from PySide6.QtWidgets import QFrame

from data_engine.domain import RuntimeStepEvent
from data_engine.domain.time import parse_utc_text
from data_engine.services import runtime_session_from_workspace_snapshot
from data_engine.views.models import default_flow_state
from data_engine.views.presentation import format_seconds
from data_engine.views.runs import RunGroupDisplay

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def reset_operation_state(window: "DataEngineWindow", flow_name: str) -> None:
    window.operation_tracker = window.operation_tracker.reset_flow(flow_name, window.flow_cards[flow_name].operation_items)
    if window.selected_flow_name == flow_name:
        render_operation_durations(window, flow_name)


def apply_runtime_event(window: "DataEngineWindow", event: RuntimeStepEvent) -> None:
    if event.flow_name not in window.flow_cards:
        return
    flow_name = event.flow_name
    card = window.flow_cards[flow_name]
    if event.step_name is None:
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        if event.status == "failed":
            window._set_flow_state(flow_name, "failed")
        elif event.status in {"started", "success", "stopped"}:
            current_runtime_session = (
                runtime_session_from_workspace_snapshot(workspace_snapshot)
                if workspace_snapshot is not None
                else window.runtime_session
            )
            if workspace_snapshot is not None:
                flow_summary = workspace_snapshot.flows.get(flow_name)
                if flow_summary is not None:
                    if flow_summary.state == "failed":
                        window._set_flow_state(flow_name, "failed")
                    elif flow_summary.state == "idle":
                        window._set_flow_state(flow_name, default_flow_state(card.mode))
                    else:
                        window._set_flow_state(
                            flow_name,
                            "polling" if card.mode == "poll" and flow_summary.state == "running"
                            else "scheduled" if card.mode == "schedule" and flow_summary.state == "running"
                            else "stopping runtime" if card.mode in {"poll", "schedule"} and flow_summary.state == "stopping"
                            else "stopping flow" if flow_summary.state == "stopping"
                            else flow_summary.state,
                        )
            elif (
                current_runtime_session.runtime_active
                and flow_name in current_runtime_session.active_runtime_flow_names
            ):
                window._set_flow_state(
                    flow_name,
                    "polling" if card.mode == "poll" else "scheduled" if card.mode == "schedule" else default_flow_state(card.mode),
                )
            else:
                window._set_flow_state(flow_name, default_flow_state(card.mode))
        return
    window.operation_tracker, flash_index = window.operation_tracker.apply_event(
        flow_name,
        card.operation_items,
        event,
        now=window._monotonic(),
    )
    if flash_index is not None and window.selected_flow_name == flow_name:
        flash_operation_row(window, flash_index)
    if window.selected_flow_name == flow_name:
        render_operation_durations(window, flow_name)


def render_operation_durations(window: "DataEngineWindow", flow_name: str) -> None:
    card = window.flow_cards.get(flow_name)
    state = window.operation_tracker.state_for(flow_name)
    if card is None or state is None:
        for row_widgets in window.operation_row_widgets:
            row_widgets.duration_label.setText("")
        return
    container = window.operation_scroll.viewport()
    container.setUpdatesEnabled(False)
    try:
        for index, operation_name in enumerate(card.operation_items):
            if index >= len(window.operation_row_widgets):
                break
            row_widgets = window.operation_row_widgets[index]
            row_card = row_widgets.row_card
            duration_label = row_widgets.duration_label
            row_state = window.operation_tracker.row_state(flow_name, operation_name)
            apply_operation_row_state(row_card, row_state.status if row_state is not None else "idle")
            duration_label.setText(duration_text(window, flow_name, operation_name))
        window._refresh_operation_buttons(flow_name)
    finally:
        container.setUpdatesEnabled(True)
        container.update()


def duration_text(window: "DataEngineWindow", flow_name: str, operation_name: str) -> str:
    return window.operation_tracker.duration_text(flow_name, operation_name, now=window._monotonic(), formatter=format_seconds)


def refresh_live_operation_durations(window: "DataEngineWindow") -> None:
    if window.selected_flow_name is None:
        return
    card = window.flow_cards.get(window.selected_flow_name)
    state = window.operation_tracker.state_for(window.selected_flow_name)
    if card is None or state is None:
        return
    if _refresh_parallel_live_operation_rows(window, card):
        _refresh_live_log_durations(window)
        return
    container = window.operation_scroll.viewport()
    container.setUpdatesEnabled(False)
    try:
        for index, operation_name in enumerate(card.operation_items):
            if index >= len(window.operation_row_widgets):
                break
            row_state = window.operation_tracker.row_state(window.selected_flow_name, operation_name)
            if row_state is None or row_state.status != "running":
                continue
            row_widgets = window.operation_row_widgets[index]
            row_widgets.duration_label.setText(duration_text(window, window.selected_flow_name, operation_name))
    finally:
        container.setUpdatesEnabled(True)
        container.update()
    _refresh_live_log_durations(window)


def _refresh_live_log_durations(window: "DataEngineWindow") -> None:
    selected_flow_name = window.selected_flow_name
    if selected_flow_name is None:
        return
    log_view = window.log_view
    if log_view.count() == 0:
        return
    updated = False
    viewport = log_view.viewport()
    viewport.setUpdatesEnabled(False)
    try:
        for index in range(log_view.count()):
            item = log_view.item(index)
            run_group = log_view.run_group(item)
            if run_group is None or run_group.status not in {"started", "stopping"}:
                continue
            next_duration = RunGroupDisplay.from_run(run_group).duration_text
            if log_view.duration_text(item) == next_duration:
                continue
            log_view.set_run_group(item, run_group)
            viewport.update(log_view.visualItemRect(item))
            updated = True
    finally:
        viewport.setUpdatesEnabled(True)
        if updated:
            viewport.update()


def _refresh_parallel_live_operation_rows(window: "DataEngineWindow", card) -> bool:
    try:
        parallel = max(int(getattr(card, "parallelism", "1") or "1"), 1)
    except (TypeError, ValueError):
        parallel = 1
    if parallel <= 1:
        return False
    workspace_snapshot = getattr(window, "workspace_snapshot", None)
    if workspace_snapshot is None or not workspace_snapshot.engine.daemon_live:
        return False
    active_by_step: dict[str, int] = {}
    duration_by_step: dict[str, float | None] = {}
    started_at_by_step: dict[str, str | None] = {}
    status_by_step: dict[str, str] = {}
    for run in workspace_snapshot.active_runs.values():
        if run.flow_name != card.name:
            continue
        step_name = run.current_step_name
        if not step_name:
            continue
        active_by_step[step_name] = active_by_step.get(step_name, 0) + 1
        if active_by_step[step_name] == 1:
            duration_by_step[step_name] = _live_step_elapsed_seconds(run.current_step_started_at_utc, run.elapsed_seconds)
            started_at_by_step[step_name] = run.current_step_started_at_utc
        else:
            duration_by_step[step_name] = None
            started_at_by_step[step_name] = None
        next_status = "stopping" if run.state == "stopping" else "running"
        previous_status = status_by_step.get(step_name)
        if previous_status != "stopping":
            status_by_step[step_name] = next_status
    container = window.operation_scroll.viewport()
    container.setUpdatesEnabled(False)
    try:
        for index, operation_name in enumerate(card.operation_items):
            if index >= len(window.operation_row_widgets):
                break
            row_widgets = window.operation_row_widgets[index]
            count = active_by_step.get(operation_name, 0)
            status = status_by_step.get(operation_name, "idle") if count > 0 else "idle"
            apply_operation_row_state(row_widgets.row_card, status)
            if count > 1:
                row_widgets.duration_label.setText(f"{count} active")
            elif count == 1 and isinstance(duration_by_step.get(operation_name), (int, float)):
                row_widgets.duration_label.setText(format_seconds(duration_by_step[operation_name]))
            else:
                row_widgets.duration_label.setText("")
    finally:
        container.setUpdatesEnabled(True)
        container.update()
    return True


def _live_step_elapsed_seconds(
    started_at_utc: str | None,
    fallback_elapsed_seconds: float | None,
) -> float | None:
    started = parse_utc_text(started_at_utc)
    if started is not None:
        return max((datetime.now(UTC) - started.astimezone(UTC)).total_seconds(), 0.0)
    return fallback_elapsed_seconds


def apply_operation_row_state(row_card: QFrame, status: str) -> None:
    if row_card.property("stepState") == status:
        return
    row_card.setProperty("stepState", status)
    style = row_card.style()
    style.unpolish(row_card)
    style.polish(row_card)
    row_card.update()


def flash_operation_row(window: "DataEngineWindow", index: int) -> None:
    del window, index


def normalize_completed_operation_rows(window: "DataEngineWindow", flow_name: str) -> None:
    window.operation_tracker = window.operation_tracker.normalize_completed(flow_name)
    if window.selected_flow_name == flow_name:
        render_operation_durations(window, flow_name)


__all__ = [
    "apply_runtime_event",
    "duration_text",
    "flash_operation_row",
    "format_seconds",
    "normalize_completed_operation_rows",
    "refresh_live_operation_durations",
    "render_operation_durations",
    "reset_operation_state",
]
