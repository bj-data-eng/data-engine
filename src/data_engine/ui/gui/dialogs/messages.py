"""Message dialog helpers for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtWidgets import QDialog, QGridLayout, QHBoxLayout, QLabel, QPushButton, QTextEdit, QVBoxLayout
from data_engine.domain import StructuredErrorState
from data_engine.ui.gui.widgets import make_label_selectable

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def structured_error_content(text: str) -> StructuredErrorState | None:
    """Parse one developer-facing flow-module error into dialog sections when possible."""
    return StructuredErrorState.parse(text)


def show_message_box(window: "DataEngineWindow", *, title: str, text: str, tone: str) -> None:
    """Show one simple application dialog for info/error messages."""
    dialog = QDialog(window)
    dialog.setWindowTitle(title)
    dialog.setModal(True)
    dialog.resize(520, 220)
    layout = QVBoxLayout(dialog)
    layout.setContentsMargins(20, 18, 20, 18)
    layout.setSpacing(14)

    structured = structured_error_content(text) if tone == "error" else None
    title_label = QLabel(structured.title if structured is not None else title, dialog)
    title_label.setObjectName("sectionTitle")
    layout.addWidget(title_label)

    if structured is None:
        body_label = QLabel(text, dialog)
        body_label.setWordWrap(True)
        body_label.setObjectName("errorText" if tone == "error" else "bodyText")
        make_label_selectable(body_label)
        layout.addWidget(body_label, 1)
    else:
        summary_grid = QGridLayout()
        summary_grid.setContentsMargins(0, 0, 0, 0)
        summary_grid.setHorizontalSpacing(10)
        summary_grid.setVerticalSpacing(6)
        summary_grid.setColumnStretch(0, 1)
        summary_grid.setColumnStretch(1, 2)
        for row_index, field in enumerate(structured.fields):
            label = QLabel(field.label, dialog)
            label.setObjectName("fieldLabel")
            value = QLabel(field.value, dialog)
            value.setObjectName("fieldValue")
            value.setWordWrap(True)
            make_label_selectable(value)
            summary_grid.addWidget(label, row_index, 0)
            summary_grid.addWidget(value, row_index, 1)
        layout.addLayout(summary_grid)

        detail_label = QLabel("Error", dialog)
        detail_label.setObjectName("fieldLabel")
        layout.addWidget(detail_label)

        detail_body = QTextEdit(dialog)
        detail_body.setObjectName("outputPreviewText")
        detail_body.setReadOnly(True)
        detail_body.setPlainText(structured.detail)
        detail_body.setMinimumHeight(88)
        layout.addWidget(detail_body, 1)

        raw_label = QLabel("Details", dialog)
        raw_label.setObjectName("fieldLabel")
        layout.addWidget(raw_label)

        raw_body = QTextEdit(dialog)
        raw_body.setObjectName("outputPreviewText")
        raw_body.setReadOnly(True)
        raw_body.setPlainText(structured.raw_text)
        raw_body.setMinimumHeight(88)
        layout.addWidget(raw_body, 1)

    action_row = QHBoxLayout()
    action_row.addStretch(1)
    close_button = QPushButton("OK", dialog)
    close_button.clicked.connect(dialog.accept)
    action_row.addWidget(close_button)
    layout.addLayout(action_row)
    dialog.exec()


__all__ = ["show_message_box", "structured_error_content"]
