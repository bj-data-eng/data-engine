"""Log-list presentation helpers for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtWidgets import QListWidgetItem

from data_engine.views import format_raw_log_message as shared_format_raw_log_message
from data_engine.ui.gui.widgets.logs import build_log_run_widget

if TYPE_CHECKING:
    from data_engine.domain import FlowLogEntry, FlowRunState
    from data_engine.ui.gui.app import DataEngineWindow


def next_log_scroll_value(
    *,
    previous_value: int,
    previous_maximum: int,
    current_maximum: int,
    force_scroll_to_bottom: bool = False,
) -> int:
    """Return the target vertical scrollbar value after one log-view refresh."""
    should_follow_tail = force_scroll_to_bottom or previous_value >= max(previous_maximum - 1, 0)
    if should_follow_tail:
        return current_maximum
    return min(previous_value, current_maximum)


def refresh_log_view(window: "DataEngineWindow", *, force_scroll_to_bottom: bool = False) -> None:
    scrollbar = window.log_view.verticalScrollBar()
    previous_value = scrollbar.value()
    previous_maximum = scrollbar.maximum()

    window.log_view.setUpdatesEnabled(False)
    window.log_view.clear()
    card = window.flow_cards.get(window.selected_flow_name or "")
    run_groups = window.log_service.runs_for_flow(window.runtime_binding.log_store, card.name) if card is not None else ()
    presentation = window.detail_application.build_selected_flow_presentation(
        card=card,
        tracker=window.operation_tracker,
        flow_states=window.flow_states,
        run_groups=tuple(run_groups),
        selected_run_key=None,
        max_visible_runs=window._MAX_VISIBLE_LOG_RUNS,
    )
    for run_group in presentation.visible_run_groups:
        add_log_run_item(window, run_group)
    window.log_view.setUpdatesEnabled(True)

    scrollbar = window.log_view.verticalScrollBar()
    scrollbar.setValue(
        next_log_scroll_value(
            previous_value=previous_value,
            previous_maximum=previous_maximum,
            current_maximum=scrollbar.maximum(),
            force_scroll_to_bottom=force_scroll_to_bottom,
        )
    )


def add_log_run_item(window: "DataEngineWindow", run_group: "FlowRunState") -> None:
    item = QListWidgetItem(run_group.display_label)
    widget = build_log_run_widget(window, run_group)
    item.setSizeHint(widget.sizeHint())
    window.log_view.addItem(item)
    window.log_view.setItemWidget(item, widget)


def format_raw_log_message(entry: "FlowLogEntry") -> str:
    return shared_format_raw_log_message(entry)


__all__ = ["add_log_run_item", "format_raw_log_message", "next_log_scroll_value", "refresh_log_view"]
