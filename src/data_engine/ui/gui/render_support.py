"""Rendering and widget-presentation helpers for the GUI application shell."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QIcon, QPixmap
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QStyle, QWidget

from data_engine.domain import FlowLogEntry, RuntimeStepEvent
from data_engine.ui.gui.helpers import (
    action_bar_icon as helper_action_bar_icon,
    apply_theme as helper_apply_theme,
    artifact_key_for_operation as helper_artifact_key_for_operation,
    capture_step_outputs as helper_capture_step_outputs,
    group_icon as helper_group_icon,
    group_icon_color as helper_group_icon_color,
    inspect_step_output as helper_inspect_step_output,
    is_inspectable_operation as helper_is_inspectable_operation,
    log_icon as helper_log_icon,
    refresh_operation_buttons as helper_refresh_operation_buttons,
    rehydrate_step_outputs_from_ledger as helper_rehydrate_step_outputs_from_ledger,
    render_group_icon_pixmap as helper_render_group_icon_pixmap,
    render_svg_icon_pixmap as helper_render_svg_icon_pixmap,
    show_config_preview as helper_show_config_preview,
    show_output_preview as helper_show_output_preview,
    sync_theme_to_system as helper_sync_theme_to_system,
    toggle_theme as helper_toggle_theme,
    update_operation_scroll_cues as helper_update_operation_scroll_cues,
    update_sidebar_scroll_cues as helper_update_sidebar_scroll_cues,
    view_rail_icon as helper_view_rail_icon,
)
from data_engine.ui.gui.presenters import refresh_sidebar_selection, refresh_sidebar_state_views, repolish_widget_tree, set_hovered
from data_engine.ui.gui.presenters import (
    apply_runtime_event as present_runtime_event,
    duration_text as present_duration_text,
    format_raw_log_message as present_format_raw_log_message,
    format_seconds as present_format_seconds,
    normalize_completed_operation_rows as present_normalize_completed_operation_rows,
    refresh_live_operation_durations as present_refresh_live_operation_durations,
    render_operation_durations as present_render_operation_durations,
    reset_operation_state as present_reset_operation_state,
)
from data_engine.ui.gui.widgets import (
    build_flow_row_widget,
    build_group_row_widget,
    format_operation_title as view_format_operation_title,
    set_operation_cards as view_set_operation_cards,
)

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow
    from data_engine.views.models import QtFlowCard


class GuiRenderingMixin:
    """Widget rendering and visual-state helpers for the GUI shell."""

    def _format_raw_log_message(self: "DataEngineWindow", entry: FlowLogEntry) -> str:
        return present_format_raw_log_message(entry)

    def _set_operation_cards(self: "DataEngineWindow", operation_items: tuple[str, ...]) -> None:
        view_set_operation_cards(self, operation_items)

    def _update_operation_scroll_cues(self: "DataEngineWindow", *args) -> None:
        helper_update_operation_scroll_cues(self, *args)

    def _update_sidebar_scroll_cues(self: "DataEngineWindow", *args) -> None:
        helper_update_sidebar_scroll_cues(self, *args)

    def _format_operation_title(self: "DataEngineWindow", operation_name: str) -> str:
        return view_format_operation_title(operation_name)

    def _reset_operation_state(self: "DataEngineWindow", flow_name: str) -> None:
        present_reset_operation_state(self, flow_name)

    def _apply_runtime_event(self: "DataEngineWindow", event: RuntimeStepEvent) -> None:
        present_runtime_event(self, event)

    def _render_operation_durations(self: "DataEngineWindow", flow_name: str) -> None:
        present_render_operation_durations(self, flow_name)

    def _duration_text(self: "DataEngineWindow", flow_name: str, operation_name: str) -> str:
        return present_duration_text(self, flow_name, operation_name)

    def _refresh_live_operation_durations(self: "DataEngineWindow") -> None:
        present_refresh_live_operation_durations(self)

    def _apply_operation_row_state(self: "DataEngineWindow", row_card: QFrame, row_state) -> None:
        status = row_state.status if row_state is not None else "idle"
        if row_card.property("stepState") == status:
            return
        row_card.setProperty("stepState", status)
        style = row_card.style()
        style.unpolish(row_card)
        style.polish(row_card)
        row_card.update()

    def _flash_operation_row(self: "DataEngineWindow", index: int) -> None:
        from data_engine.ui.gui.presenters.steps import flash_operation_row

        flash_operation_row(self, index)

    def _normalize_completed_operation_rows(self: "DataEngineWindow", flow_name: str) -> None:
        present_normalize_completed_operation_rows(self, flow_name)

    def _format_seconds(self: "DataEngineWindow", seconds: float) -> str:
        return present_format_seconds(seconds)

    def _group_icon(self: "DataEngineWindow", group_name: str) -> QIcon:
        return helper_group_icon(self, group_name)

    def _group_icon_color(self: "DataEngineWindow") -> QColor:
        return helper_group_icon_color(self)

    def _render_svg_icon_pixmap(
        self: "DataEngineWindow", icon_name: str, size: int, *, fill_color: str | None = None
    ) -> QPixmap:
        return helper_render_svg_icon_pixmap(self, icon_name, size, fill_color=fill_color)

    def _view_rail_icon(self: "DataEngineWindow", view_name: str) -> QIcon:
        return helper_view_rail_icon(self, view_name)

    def _action_bar_icon(self: "DataEngineWindow", action_name: str) -> QIcon:
        return helper_action_bar_icon(self, action_name)

    def _log_icon(self: "DataEngineWindow", icon_name: str, size: int = 16) -> QIcon:
        return helper_log_icon(self, icon_name, size)

    def _render_group_icon_pixmap(self: "DataEngineWindow", group_name: str, size: int) -> QPixmap:
        return helper_render_group_icon_pixmap(self, group_name, size)

    def _build_group_row_widget(self: "DataEngineWindow", group_name: str, entries: list["QtFlowCard"]) -> QFrame:
        return build_group_row_widget(self, group_name, entries)

    def _build_flow_row_widget(self: "DataEngineWindow", card: "QtFlowCard") -> QFrame:
        return build_flow_row_widget(self, card)

    def _build_log_run_widget(self: "DataEngineWindow", run_group) -> QFrame:
        frame = QFrame()
        frame.setObjectName("logRunRow")
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(10)

        title_row = QHBoxLayout()
        title_row.setContentsMargins(0, 0, 0, 0)
        title_row.setSpacing(8)
        title = QLabel(run_group.display_label)
        title.setObjectName("logPrimary")
        title_row.addWidget(title, 0, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        if run_group.elapsed_seconds is not None:
            duration = QLabel(self._format_seconds(run_group.elapsed_seconds))
            duration.setObjectName("logDuration")
            title_row.addWidget(duration, 0, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        title_row.addStretch(1)
        layout.addLayout(title_row, 1)

        status_name = "failed" if run_group.status in {"failed", "stopped"} else "started" if run_group.status == "started" else "finished"
        status_icon = QLabel()
        status_icon.setObjectName("logStatusIcon")
        status_icon.setPixmap(self._render_svg_icon_pixmap(self._LOG_ICON_NAMES[status_name], 16, fill_color=self._LOG_ICON_COLORS[status_name]))
        status_icon.setToolTip("")
        layout.addWidget(status_icon, 0, Qt.AlignmentFlag.AlignVCenter)

        view_button = QPushButton()
        view_button.setObjectName("logIconButton")
        view_button.setIcon(self._log_icon("view_log"))
        view_button.setIconSize(QPixmap(16, 16).size())
        view_button.clicked.connect(lambda _checked=False, group=run_group: self._show_run_log_preview(group))
        layout.addWidget(view_button, 0, Qt.AlignmentFlag.AlignVCenter)

        return frame

    def _refresh_sidebar_selection(self: "DataEngineWindow") -> None:
        refresh_sidebar_selection(self)

    def _refresh_sidebar_state_views(self: "DataEngineWindow", changed_flow_names: set[str]) -> None:
        """Refresh sidebar labels/colors in place for state-only changes."""
        if refresh_sidebar_state_views(self, changed_flow_names):
            self._populate_flow_tree()

    def _set_hovered(self: "DataEngineWindow", widget: QFrame, hovered: bool) -> None:
        """Update one sidebar row hover property and repolish it."""
        set_hovered(widget, hovered)

    def _repolish_widget_tree(self: "DataEngineWindow", widget: QWidget) -> None:
        """Reapply stylesheet state to one widget and its child widgets."""
        repolish_widget_tree(widget)

    def _flow_icon(self: "DataEngineWindow", card: "QtFlowCard") -> QIcon:
        """Return a left-side icon for one flow row."""
        style = self.style()
        state = self.flow_states.get(card.name, card.state)
        if state == "failed":
            return style.standardIcon(QStyle.StandardPixmap.SP_MessageBoxCritical)
        if state in {"running", "polling", "scheduled"}:
            return style.standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)
        if state in {"stopping flow", "stopping runtime"}:
            return style.standardIcon(QStyle.StandardPixmap.SP_BrowserStop)
        if card.mode == "schedule":
            return style.standardIcon(QStyle.StandardPixmap.SP_FileDialogDetailedView)
        if card.mode == "poll":
            return style.standardIcon(QStyle.StandardPixmap.SP_BrowserReload)
        return style.standardIcon(QStyle.StandardPixmap.SP_FileIcon)

    def _toggle_theme(self: "DataEngineWindow") -> None:
        helper_toggle_theme(self)

    def _sync_theme_to_system(self: "DataEngineWindow", *args) -> None:
        helper_sync_theme_to_system(self, *args)

    def _apply_theme(self: "DataEngineWindow") -> None:
        helper_apply_theme(self)

    def _is_inspectable_operation(self: "DataEngineWindow", operation_name: str) -> bool:
        return helper_is_inspectable_operation(operation_name)

    def _artifact_key_for_operation(self: "DataEngineWindow", operation_name: str) -> str | None:
        return helper_artifact_key_for_operation(operation_name)

    def _capture_step_outputs(self: "DataEngineWindow", flow_name: str, results: object) -> None:
        helper_capture_step_outputs(self, flow_name, results)

    def _rehydrate_step_outputs_from_ledger(self: "DataEngineWindow") -> None:
        helper_rehydrate_step_outputs_from_ledger(self)

    def _refresh_operation_buttons(self: "DataEngineWindow", flow_name: str) -> None:
        helper_refresh_operation_buttons(self, flow_name)

    def _inspect_step_output(self: "DataEngineWindow", operation_name: str) -> None:
        helper_inspect_step_output(self, operation_name)

    def _show_output_preview(self: "DataEngineWindow", operation_name: str, output_path: Path) -> None:
        helper_show_output_preview(self, operation_name, output_path)

    def _show_config_preview(self: "DataEngineWindow") -> None:
        helper_show_config_preview(self)
