"""Sidebar row builders for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import QIcon, QPixmap
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QSizePolicy, QVBoxLayout

from data_engine.views.flow_display import FlowRowDisplay, GroupRowDisplay
from data_engine.views.presentation import status_color_name as shared_status_color_name

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow
    from data_engine.views.models import QtFlowCard


def group_secondary_text(window: "DataEngineWindow", group_name: str, entries: list["QtFlowCard"]) -> str:
    return GroupRowDisplay.from_group(group_name, entries, window.flow_states).secondary


def flow_secondary_text(window: "DataEngineWindow", card: "QtFlowCard") -> str:
    state = window.flow_states.get(card.name, card.state)
    return FlowRowDisplay.from_card(card, state, primary="name").secondary


def flow_primary_text(card: "QtFlowCard") -> str:
    return card.title


def status_color_name(state: str) -> str:
    return shared_status_color_name(state)


def icon_label(icon: QIcon, size: int = 18, *, parent: QFrame | None = None) -> QLabel:
    label = QLabel(parent)
    label.setObjectName("sidebarIcon")
    pixmap = icon.pixmap(size, size)
    label.setPixmap(QPixmap(pixmap))
    label.setFixedSize(size + 8, size + 8)
    label.setAlignment(Qt.AlignmentFlag.AlignCenter)
    return label


def build_group_row_widget(window: "DataEngineWindow", group_name: str, entries: list["QtFlowCard"]) -> QFrame:
    group_display = GroupRowDisplay.from_group(group_name, entries, window.flow_states)
    frame = QFrame(window.sidebar_content)
    frame.setObjectName("sidebarGroupRow")
    frame.setProperty("groupName", group_name)
    frame.setProperty("hovered", False)
    frame.setFixedHeight(44)
    row = QHBoxLayout(frame)
    row.setContentsMargins(0, 8, 0, 2)
    row.setSpacing(8)
    icon = QLabel(frame)
    icon.setObjectName("sidebarIcon")
    icon.setPixmap(window._render_group_icon_pixmap(group_name, 16))
    icon.setFixedSize(24, 24)
    icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
    row.addWidget(icon)

    text_col = QVBoxLayout()
    text_col.setContentsMargins(0, 0, 0, 0)
    text_col.setSpacing(1)
    title = QLabel(group_display.title, frame)
    title.setObjectName("sidebarGroupTitle")
    subtitle = QLabel(group_display.secondary, frame)
    subtitle.setObjectName("sidebarGroupMeta")
    text_col.addWidget(title)
    text_col.addWidget(subtitle)
    row.addLayout(text_col, 1)
    frame.enterEvent = lambda event, widget=frame: window._set_hovered(widget, True)
    frame.leaveEvent = lambda event, widget=frame: window._set_hovered(widget, False)
    return frame


def build_flow_row_widget(window: "DataEngineWindow", card: "QtFlowCard") -> QFrame:
    flow_display = FlowRowDisplay.from_card(card, window.flow_states.get(card.name, card.state), primary="name")
    frame = QFrame(window.sidebar_content)
    frame.setObjectName("sidebarFlowRow")
    frame.setProperty("selected", False)
    frame.setProperty("hovered", False)
    frame.setFixedHeight(42)
    row = QHBoxLayout(frame)
    row.setContentsMargins(12, 4, 8, 4)
    row.setSpacing(10)
    number = QLabel("00", frame)
    number.setObjectName("sidebarFlowNumber")
    row.addWidget(number)

    text_col = QVBoxLayout()
    text_col.setContentsMargins(0, 0, 0, 0)
    text_col.setSpacing(1)
    title = QLabel(flow_primary_text(card), frame)
    title.setObjectName("sidebarFlowCode")
    subtitle = QLabel(flow_display.secondary, frame)
    subtitle.setObjectName("sidebarFlowMeta")
    subtitle.setProperty("stateColor", flow_display.state_color)
    text_col.addWidget(title)
    text_col.addWidget(subtitle)
    row.addLayout(text_col, 1)

    state_dot = QLabel("\u25cf", frame)
    state_dot.setObjectName("sidebarStateDot")
    state_dot.setProperty("stateColor", flow_display.state_color)
    state_dot.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
    state_dot.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
    row.addWidget(state_dot)
    frame.setToolTip("")
    frame.mousePressEvent = lambda event, flow_name=card.name: window._select_flow(flow_name)
    frame.enterEvent = lambda event, widget=frame: window._set_hovered(widget, True)
    frame.leaveEvent = lambda event, widget=frame: window._set_hovered(widget, False)
    return frame


def group_label(group_name: str) -> str:
    return GroupRowDisplay.from_group(group_name, [], {}).title


__all__ = [
    "build_flow_row_widget",
    "build_group_row_widget",
    "flow_primary_text",
    "flow_secondary_text",
    "group_label",
    "group_secondary_text",
    "icon_label",
    "status_color_name",
]
