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
    frame = QFrame()
    frame.setObjectName("logRunRow")
    frame.setProperty("sourceLabel", display.source_label)
    layout = QHBoxLayout(frame)
    layout.setContentsMargins(8, 6, 8, 6)
    layout.setSpacing(10)

    title_row = QHBoxLayout()
    title_row.setContentsMargins(0, 0, 0, 0)
    title_row.setSpacing(8)
    title = QLabel(display.primary_label)
    title.setObjectName("logPrimary")
    title_row.addWidget(title, 0, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    if display.duration_text is not None:
        duration = QLabel(display.duration_text)
        duration.setObjectName("logDuration")
        title_row.addWidget(duration, 0, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    title_row.addStretch(1)
    layout.addLayout(title_row, 1)

    status_icon = QLabel()
    status_icon.setObjectName("logStatusIcon")
    status_icon.setPixmap(
        window._render_svg_icon_pixmap(
            window._LOG_ICON_NAMES[display.status_visual_state],
            16,
            fill_color=window._LOG_ICON_COLORS[display.status_visual_state],
        )
    )
    status_icon.setToolTip(display.status_text)
    layout.addWidget(status_icon, 0, Qt.AlignmentFlag.AlignVCenter)

    view_button = QPushButton()
    view_button.setObjectName("logIconButton")
    view_button.setIcon(window._log_icon("view_log"))
    view_button.setIconSize(QPixmap(16, 16).size())
    view_button.setToolTip("View Log")
    view_button.clicked.connect(lambda _checked=False, group=run_group: window._show_run_log_preview(group))
    layout.addWidget(view_button, 0, Qt.AlignmentFlag.AlignVCenter)
    return frame


__all__ = ["build_log_run_widget"]
