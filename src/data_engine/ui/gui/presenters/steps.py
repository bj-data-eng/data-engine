"""Step-list presentation helpers for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QFrame

from data_engine.domain import RuntimeStepEvent
from data_engine.views.models import default_flow_state
from data_engine.views.presentation import format_seconds

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
        if event.status == "failed":
            window._set_flow_state(flow_name, "failed")
        elif event.status in {"started", "success", "stopped"}:
            if window.runtime_session.runtime_active and flow_name in window.runtime_session.active_runtime_flow_names:
                window._set_flow_state(
                    flow_name,
                    "polling" if card.mode == "poll" else "scheduled" if card.mode == "schedule" else default_flow_state(card.mode),
                )
            elif window.flow_states.get(flow_name) == "failed":
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


def duration_text(window: "DataEngineWindow", flow_name: str, operation_name: str) -> str:
    return window.operation_tracker.duration_text(flow_name, operation_name, now=window._monotonic(), formatter=format_seconds)


def refresh_live_operation_durations(window: "DataEngineWindow") -> None:
    if window.selected_flow_name is None:
        return
    card = window.flow_cards.get(window.selected_flow_name)
    state = window.operation_tracker.state_for(window.selected_flow_name)
    if card is None or state is None:
        return
    for index, operation_name in enumerate(card.operation_items):
        if index >= len(window.operation_row_widgets):
            break
        row_state = window.operation_tracker.row_state(window.selected_flow_name, operation_name)
        if row_state is None or row_state.status != "running":
            continue
        row_widgets = window.operation_row_widgets[index]
        row_widgets.duration_label.setText(duration_text(window, window.selected_flow_name, operation_name))


def apply_operation_row_state(row_card: QFrame, status: str) -> None:
    if row_card.property("stepState") == status:
        return
    row_card.setProperty("stepState", status)
    style = row_card.style()
    style.unpolish(row_card)
    style.polish(row_card)
    row_card.update()


def flash_operation_row(window: "DataEngineWindow", index: int) -> None:
    if index >= len(window.operation_row_widgets):
        return
    row_card = window.operation_row_widgets[index].row_card
    row_card.setProperty("flashState", "complete")
    style = row_card.style()
    style.unpolish(row_card)
    style.polish(row_card)
    row_card.update()

    timer = QTimer(window)
    timer.setSingleShot(True)

    def clear_flash() -> None:
        try:
            row_card.setProperty("flashState", "")
            style = row_card.style()
            style.unpolish(row_card)
            style.polish(row_card)
            row_card.update()
        except RuntimeError:
            pass
        if timer in window.operation_flash_timers:
            window.operation_flash_timers.remove(timer)

    timer.timeout.connect(clear_flash)
    window.operation_flash_timers.append(timer)
    timer.start(140)


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
