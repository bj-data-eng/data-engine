"""Small config/detail widget helpers for the desktop GUI."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QVBoxLayout


def make_label_selectable(label: QLabel) -> QLabel:
    """Enable text selection on one read-only label."""
    label.setTextInteractionFlags(
        Qt.TextInteractionFlag.TextSelectableByMouse | Qt.TextInteractionFlag.TextSelectableByKeyboard
    )
    return label


def build_config_value(layout: QVBoxLayout, label: str) -> QLabel:
    """Build one two-column config row and return the mutable value label."""
    row_frame = QFrame(layout.parentWidget())
    row_frame.setObjectName("configRow")
    row_layout = QHBoxLayout(row_frame)
    row_layout.setContentsMargins(0, 6, 0, 6)
    row_layout.setSpacing(10)

    title = QLabel(label, row_frame)
    title.setObjectName("fieldLabel")
    value = QLabel("-", row_frame)
    value.setWordWrap(True)
    make_label_selectable(value)
    value.setObjectName("fieldValue")
    value.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
    title.setMinimumWidth(92)
    title.setMaximumWidth(92)
    title.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
    row_layout.addWidget(title, 0)
    row_layout.addWidget(value, 1)
    layout.addWidget(row_frame)
    return value


__all__ = ["build_config_value", "make_label_selectable"]
