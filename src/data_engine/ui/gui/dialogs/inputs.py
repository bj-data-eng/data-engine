"""Manual flow input dialogs for the desktop UI."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from PySide6.QtCore import QDate
from PySide6.QtWidgets import QDateEdit, QDialog, QFrame, QHBoxLayout, QLabel, QPushButton, QVBoxLayout

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow
    from data_engine.views.models import QtFlowCard


def _today_qdate() -> QDate:
    today = date.today()
    return QDate(today.year, today.month, today.day)


def show_manual_inputs_dialog(window: "DataEngineWindow", card: "QtFlowCard") -> dict[str, object] | None:
    """Collect pre-run manual input values for one selected flow."""
    if not card.manual_inputs:
        return {}

    date_range_specs = tuple(spec for spec in card.manual_inputs if spec.kind == "date_range")
    dialog = QDialog(window)
    dialog.setWindowTitle(date_range_specs[0].label if len(date_range_specs) == 1 else card.title)
    dialog.setModal(True)
    dialog.resize(360, 140)
    layout = QVBoxLayout(dialog)
    layout.setContentsMargins(16, 14, 16, 14)
    layout.setSpacing(10)

    error_label = QLabel("", dialog)
    error_label.setObjectName("errorText")
    error_label.setWordWrap(True)
    error_label.setVisible(False)
    layout.addWidget(error_label)

    date_range_widgets: dict[str, tuple[QDateEdit, QDateEdit]] = {}
    for spec in date_range_specs:
        frame = QFrame(dialog)
        frame.setObjectName("configRow")
        row = QHBoxLayout(frame)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(10)

        start_edit = QDateEdit(frame)
        start_edit.setObjectName(f"{spec.name}StartDate")
        start_edit.setCalendarPopup(True)
        start_edit.setDisplayFormat("yyyy-MM-dd")
        start_edit.setDate(_today_qdate())
        row.addWidget(start_edit, 1)

        end_edit = QDateEdit(frame)
        end_edit.setObjectName(f"{spec.name}EndDate")
        end_edit.setCalendarPopup(True)
        end_edit.setDisplayFormat("yyyy-MM-dd")
        end_edit.setDate(_today_qdate())
        row.addWidget(end_edit, 1)

        date_range_widgets[spec.name] = (start_edit, end_edit)
        layout.addWidget(frame)

    action_row = QHBoxLayout()
    action_row.addStretch(1)
    cancel_button = QPushButton("Cancel", dialog)
    cancel_button.clicked.connect(dialog.reject)
    action_row.addWidget(cancel_button)
    run_button = QPushButton("Run", dialog)
    action_row.addWidget(run_button)
    layout.addLayout(action_row)

    submitted: dict[str, object] | None = None

    def _submit() -> None:
        nonlocal submitted
        values: dict[str, object] = {}
        for spec in card.manual_inputs:
            if spec.kind != "date_range":
                continue
            start_edit, end_edit = date_range_widgets[spec.name]
            if start_edit.date() > end_edit.date():
                error_label.setText(f"{spec.label} start date must be on or before end date.")
                error_label.setVisible(True)
                return
            values[spec.name] = {
                "start": start_edit.date().toString("yyyy-MM-dd"),
                "end": end_edit.date().toString("yyyy-MM-dd"),
            }
        submitted = values
        dialog.accept()

    run_button.clicked.connect(_submit)
    dialog.exec()
    return submitted


__all__ = ["show_manual_inputs_dialog"]
