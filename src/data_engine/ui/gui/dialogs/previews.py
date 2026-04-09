"""Preview dialog helpers for the desktop UI."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from data_engine.ui.gui.preview_models import ConfigPreviewRequest, OutputPreviewRequest, RunLogPreviewRequest
from data_engine.ui.gui.rendering import populate_output_preview, render_svg_icon_pixmap
from data_engine.ui.gui.widgets import build_config_value, make_label_selectable

if TYPE_CHECKING:
    from data_engine.domain import FlowLogEntry, FlowRunState
    from data_engine.views.models import QtFlowCard
    from data_engine.ui.gui.app import DataEngineWindow


def show_run_log_preview(window: "DataEngineWindow", request: RunLogPreviewRequest) -> QDialog:
    detail = request.detail
    dialog = QDialog(window)
    dialog.setWindowTitle("Run Log")
    dialog.setObjectName("outputPreviewDialog")
    dialog.resize(760, 520)
    layout = QVBoxLayout(dialog)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(12)

    header = QFrame()
    header.setObjectName("outputPreviewHeader")
    header_layout = QVBoxLayout(header)
    header_layout.setContentsMargins(14, 14, 14, 14)
    header_layout.setSpacing(4)

    title_label = QLabel(detail.display_label)
    title_label.setObjectName("heroTitle")
    header_layout.addWidget(title_label)

    summary_parts = [detail.status.title()]
    if detail.elapsed_seconds is not None:
        summary_parts.append(window._format_seconds(detail.elapsed_seconds))
    summary_label = QLabel("  •  ".join(summary_parts))
    summary_label.setObjectName("sectionMeta")
    header_layout.addWidget(summary_label)
    layout.addWidget(header)

    log_list = QListWidget()
    log_list.setObjectName("runLogList")
    log_list.setSpacing(6)
    for entry in request.run_group.entries:
        item = QListWidgetItem(entry.line)
        widget = _build_raw_log_entry_widget(window, entry, run_group=request.run_group)
        item.setSizeHint(widget.sizeHint())
        log_list.addItem(item)
        log_list.setItemWidget(item, widget)
    layout.addWidget(log_list, 1)

    _present_dialog(dialog)
    return dialog


def _build_raw_log_entry_widget(window: "DataEngineWindow", entry: "FlowLogEntry", *, run_group: "FlowRunState") -> QFrame:
    frame = QFrame()
    frame.setObjectName("rawLogRow")
    frame.setMinimumHeight(40)
    layout = QHBoxLayout(frame)
    layout.setContentsMargins(8, 6, 8, 6)
    layout.setSpacing(10)

    timestamp = QLabel(entry.created_at_utc.astimezone().strftime("%I:%M:%S %p"))
    timestamp.setObjectName("rawLogTimestamp")
    make_label_selectable(timestamp)
    layout.addWidget(timestamp, 0, Qt.AlignmentFlag.AlignVCenter)

    message = QLabel(window._format_raw_log_message(entry))
    message.setObjectName("rawLogMessage")
    message.setTextFormat(Qt.TextFormat.RichText)
    message.setWordWrap(False)
    make_label_selectable(message)
    layout.addWidget(message, 1, Qt.AlignmentFlag.AlignVCenter)

    event = entry.event
    layout.addStretch(0)
    inspect_slot = QWidget()
    inspect_slot.setObjectName("rawLogInspectSlot")
    inspect_slot.setFixedWidth(108)
    inspect_slot_layout = QHBoxLayout(inspect_slot)
    inspect_slot_layout.setContentsMargins(0, 0, 0, 0)
    inspect_slot_layout.setSpacing(0)
    inspect_slot_layout.addStretch(1)
    inspect_button = QPushButton("Inspect")
    inspect_button.setObjectName("inspectOutputButton")
    inspect_button.setFixedWidth(96)
    inspect_button.setEnabled(False)
    inspect_slot_layout.addWidget(inspect_button, 0, Qt.AlignmentFlag.AlignVCenter)

    icon_slot = QWidget()
    icon_slot.setObjectName("rawLogIconSlot")
    icon_slot.setFixedWidth(20)
    icon_slot_layout = QHBoxLayout(icon_slot)
    icon_slot_layout.setContentsMargins(0, 0, 0, 0)
    icon_slot_layout.setSpacing(0)
    icon_slot_layout.addStretch(1)

    if event is not None:
        if event.status == "failed":
            inspect_button.setEnabled(True)
            inspect_button.clicked.connect(
                lambda _checked=False, group=run_group, failed_entry=entry: window._show_run_error_details(group, failed_entry)
            )
        if event.status in {"started", "failed", "success"}:
            status_name = "failed" if event.status == "failed" else "started" if event.status == "started" else "finished"
            status_icon = QLabel()
            status_icon.setObjectName("rawLogStatusIcon")
            fill = window._LOG_ICON_COLORS.get(status_name, window._group_icon_color().name())
            status_icon.setPixmap(
                render_svg_icon_pixmap(
                    icon_name=window._LOG_ICON_NAMES[status_name],
                    size=14,
                    device_pixel_ratio=window.devicePixelRatioF(),
                    fill_color=fill,
                    default_fill_color=window._group_icon_color(),
                )
            )
            status_icon.setToolTip(event.status.title())
            icon_slot_layout.addWidget(status_icon, 0, Qt.AlignmentFlag.AlignVCenter)

    layout.addWidget(inspect_slot, 0, Qt.AlignmentFlag.AlignVCenter)
    layout.addWidget(icon_slot, 0, Qt.AlignmentFlag.AlignVCenter)
    return frame


def show_output_preview(window: "DataEngineWindow", request: OutputPreviewRequest) -> QDialog:
    dialog = QDialog(window)
    dialog.setWindowTitle("Inspect Output")
    dialog.setObjectName("outputPreviewDialog")
    dialog.resize(860, 520)
    layout = QVBoxLayout(dialog)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(12)

    header = QFrame()
    header.setObjectName("outputPreviewHeader")
    header_layout = QVBoxLayout(header)
    header_layout.setContentsMargins(14, 14, 14, 14)
    header_layout.setSpacing(0)

    path_label = QLabel(str(request.output_path))
    path_label.setObjectName("outputPreviewPath")
    path_label.setWordWrap(True)
    make_label_selectable(path_label)
    header_layout.addWidget(path_label)
    layout.addWidget(header)

    populate_output_preview(layout, request.output_path)

    _present_dialog(dialog)
    return dialog


def show_config_preview(window: "DataEngineWindow", request: ConfigPreviewRequest) -> QDialog:
    preview_state = request.preview
    dialog = QDialog(window)
    dialog.setWindowTitle(preview_state.title)
    dialog.setObjectName("outputPreviewDialog")
    dialog.resize(560, 420)
    layout = QVBoxLayout(dialog)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(12)

    header = QFrame()
    header.setObjectName("outputPreviewHeader")
    header_layout = QVBoxLayout(header)
    header_layout.setContentsMargins(14, 14, 14, 14)
    header_layout.setSpacing(4)

    title_label = QLabel(preview_state.title)
    title_label.setObjectName("heroTitle")
    title_label.setWordWrap(True)
    header_layout.addWidget(title_label)
    if preview_state.description:
        description_label = QLabel(preview_state.description)
        description_label.setObjectName("bodyText")
        description_label.setWordWrap(True)
        header_layout.addWidget(description_label)
    layout.addWidget(header)

    body = QFrame()
    body.setObjectName("configPreviewBody")
    body_layout = QVBoxLayout(body)
    body_layout.setContentsMargins(14, 14, 14, 14)
    body_layout.setSpacing(2)
    for row in preview_state.summary.rows:
        row_value = build_config_value(body_layout, row.label)
        row_value.setText(row.value)
    body_layout.addStretch(1)
    layout.addWidget(body, 1)

    _present_dialog(dialog)
    return dialog


def _present_dialog(dialog: QDialog) -> None:
    dialog.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
    dialog.show()
    dialog.raise_()
    dialog.activateWindow()


__all__ = ["show_config_preview", "show_output_preview", "show_run_log_preview"]
