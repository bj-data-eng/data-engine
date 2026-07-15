"""Log-list presentation helpers for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtWidgets import QListWidgetItem

from data_engine.views import (
    RunGroupDisplay,
    build_selected_flow_presentation,
    format_raw_log_message as shared_format_raw_log_message,
)
if TYPE_CHECKING:
    from data_engine.domain import FlowLogEntry, FlowRunState
    from data_engine.ui.gui.app import DataEngineWindow


def refresh_log_view(window: "DataEngineWindow") -> None:
    card = window.flow_cards.get(window.selected_flow_name or "")
    current_flow_name = card.name if card is not None else None
    refresh_external_state = getattr(window.runtime_binding.runtime_cache_ledger, "refresh_external_state", None)
    if callable(refresh_external_state):
        refresh_external_state()
    run_groups = tuple(
        window.history_query_service.list_flow_runs_from_ledger(
            window.runtime_binding.runtime_cache_ledger,
            flow_name=current_flow_name,
            limit=window._MAX_VISIBLE_LOG_RUNS,
        )
    )
    if not run_groups:
        run_groups = tuple(
            window.history_query_service.list_flow_runs(
                window.runtime_binding.log_store,
                flow_name=current_flow_name,
            )
        )
    workspace_snapshot = getattr(window, "workspace_snapshot", None)
    presentation = build_selected_flow_presentation(
        card=card,
        tracker=window.operation_tracker,
        flow_states=window.flow_states,
        run_groups=tuple(run_groups),
        selected_run_key=None,
        max_visible_runs=window._MAX_VISIBLE_LOG_RUNS,
        live_runs=(
            workspace_snapshot.active_runs
            if workspace_snapshot is not None and workspace_snapshot.engine.daemon_live
            else None
        ),
        live_truth_authoritative=bool(
            workspace_snapshot is not None and workspace_snapshot.engine.daemon_live
        ),
    )
    visible_run_groups = tuple(reversed(presentation.visible_run_groups))
    visible_run_key_signature = tuple(run_group.key for run_group in visible_run_groups)
    visible_row_signature = tuple(_row_signature(run_group) for run_group in visible_run_groups)
    if (
        current_flow_name == window._last_log_view_flow_name
        and visible_row_signature == window._last_log_view_signature
    ):
        return

    window.log_view.setUpdatesEnabled(False)
    if (
        current_flow_name == window._last_log_view_flow_name
        and visible_run_key_signature == window._last_log_view_run_keys
        and window.log_view.count() == len(visible_run_groups)
    ):
        previous_row_signature = window._last_log_view_signature
        for index, run_group in enumerate(visible_run_groups):
            if index < len(previous_row_signature) and previous_row_signature[index] == visible_row_signature[index]:
                continue
            update_log_run_item(window, index, run_group)
    else:
        window.log_view.clear()
        for run_group in visible_run_groups:
            add_log_run_item(window, run_group)
    window.log_view.setUpdatesEnabled(True)
    window._last_log_view_flow_name = current_flow_name
    window._last_log_view_run_keys = visible_run_key_signature
    window._last_log_view_signature = visible_row_signature

def add_log_run_item(window: "DataEngineWindow", run_group: "FlowRunState") -> None:
    item = QListWidgetItem(run_group.display_label)
    window.log_view.set_run_group(item, run_group)
    window.log_view.addItem(item)


def update_log_run_item(window: "DataEngineWindow", index: int, run_group: "FlowRunState") -> None:
    item = window.log_view.item(index)
    if item is None:
        return
    window.log_view.set_run_group(item, run_group)


def format_raw_log_message(entry: "FlowLogEntry") -> str:
    return shared_format_raw_log_message(entry)


def _row_signature(run_group: "FlowRunState") -> tuple[tuple[str, str], str, str, str, str | None]:
    display = RunGroupDisplay.from_run(run_group)
    return (
        run_group.key,
        display.primary_label,
        display.source_label,
        display.status_text,
        display.duration_text,
    )


__all__ = ["add_log_run_item", "format_raw_log_message", "refresh_log_view"]
