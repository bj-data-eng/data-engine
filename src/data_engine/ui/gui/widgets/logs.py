"""Log-list row builders for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton

from data_engine.views import RunGroupDisplay

if TYPE_CHECKING:
    from data_engine.domain import FlowRunState
    from data_engine.ui.gui.app import DataEngineWindow


def build_log_run_widget(window: "DataEngineWindow", run_group: "FlowRunState") -> QFrame:
    display = RunGroupDisplay.from_run(run_group)
    parent = window.log_view.viewport()
    frame = QFrame(parent)
    frame.setObjectName("logRunRow")
    setattr(frame, "_run_group", run_group)
    layout = QHBoxLayout(frame)
    layout.setContentsMargins(8, 6, 8, 6)
    layout.setSpacing(10)

    title_row = QHBoxLayout()
    title_row.setContentsMargins(0, 0, 0, 0)
    title_row.setSpacing(8)
    title = QLabel(display.primary_label, frame)
    title.setObjectName("logPrimary")
    title_row.addWidget(title, 0, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    duration = QLabel(display.duration_text or "", frame)
    duration.setObjectName("logDuration")
    duration.setVisible(display.duration_text is not None)
    title_row.addWidget(duration, 0, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    title_row.addStretch(1)
    layout.addLayout(title_row, 1)

    status_icon = QLabel(frame)
    status_icon.setObjectName("logStatusIcon")
    layout.addWidget(status_icon, 0, Qt.AlignmentFlag.AlignVCenter)

    view_button = QPushButton(frame)
    view_button.setObjectName("logIconButton")
    view_button.setIcon(window._log_icon("view_log"))
    view_button.setIconSize(QPixmap(16, 16).size())
    view_button.setToolTip("View Log")
    def callback(_checked: bool = False, *, host: QFrame = frame) -> None:
        del _checked
        group = getattr(host, "_run_group", None)
        if group is not None:
            window._show_run_log_preview(group)
    setattr(view_button, "_run_group_callback", callback)
    view_button.clicked.connect(callback)
    layout.addWidget(view_button, 0, Qt.AlignmentFlag.AlignVCenter)
    update_log_run_widget(frame, window, run_group)
    return frame


def update_log_run_widget(frame: QFrame, window: "DataEngineWindow", run_group: "FlowRunState") -> None:
    display = RunGroupDisplay.from_run(run_group)
    setattr(frame, "_run_group", run_group)
    frame.setProperty("sourceLabel", display.source_label)
    title = frame.findChild(QLabel, "logPrimary")
    if title is not None:
        title.setText(display.primary_label)
    duration = frame.findChild(QLabel, "logDuration")
    if duration is not None:
        duration.setVisible(display.duration_text is not None)
        if display.duration_text is not None:
            duration.setText(display.duration_text)
    status_icon = frame.findChild(QLabel, "logStatusIcon")
    if status_icon is not None:
        status_icon.setPixmap(
            window._render_svg_icon_pixmap(
                window._LOG_ICON_NAMES[display.status_visual_state],
                16,
                fill_color=window._LOG_ICON_COLORS[display.status_visual_state],
            )
        )
        status_icon.setToolTip(display.status_text)
__all__ = ["build_log_run_widget", "update_log_run_widget"]
