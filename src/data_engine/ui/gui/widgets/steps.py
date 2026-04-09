"""Step-list view helpers for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton
from data_engine.ui.gui.cache_models import OperationRowWidgets

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def set_operation_cards(window: "DataEngineWindow", operation_items: tuple[str, ...]) -> None:
    for timer in list(window.operation_flash_timers):
        timer.stop()
    window.operation_flash_timers.clear()

    while window.operation_layout.count() > 1:
        item = window.operation_layout.takeAt(0)
        widget = item.widget()
        if widget is not None:
            widget.deleteLater()
        window.operation_row_widgets = []

    if not operation_items:
        empty = QFrame()
        empty.setObjectName("operationCard")
        row = QHBoxLayout(empty)
        row.setContentsMargins(12, 9, 12, 9)
        label = QLabel("No steps configured.")
        label.setObjectName("bodyText")
        row.addWidget(label)
        window.operation_layout.insertWidget(0, empty)
        window._update_operation_scroll_cues()
        return

    for index, name in enumerate(operation_items, start=1):
        card = QFrame()
        card.setObjectName("operationCard")
        row = QHBoxLayout(card)
        row.setContentsMargins(12, 9, 12, 9)
        row.setSpacing(10)
        step = QLabel(f"{index:02d}")
        step.setObjectName("operationStep")
        title = QLabel(format_operation_title(name))
        title.setObjectName("operationTitle")
        title.setTextFormat(Qt.TextFormat.RichText)
        duration = QLabel("")
        duration.setObjectName("operationDuration")
        duration.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        inspect_button: QPushButton | None = None
        row.addWidget(step)
        row.addWidget(title, 1)
        if window._is_inspectable_operation(name):
            inspect_button = QPushButton("Inspect")
            inspect_button.setObjectName("inspectOutputButton")
            inspect_button.clicked.connect(lambda _checked=False, operation_name=name: window._inspect_step_output(operation_name))
            row.addWidget(inspect_button)
        row.addWidget(duration)
        window.operation_layout.insertWidget(index - 1, card)
        window.operation_row_widgets.append(
            OperationRowWidgets(
                row_card=card,
                title_label=title,
                duration_label=duration,
                inspect_button=inspect_button,
                operation_name=name,
            )
        )
    if window.selected_flow_name is not None:
        window._refresh_operation_buttons(window.selected_flow_name)
    window._update_operation_scroll_cues()


def format_operation_title(operation_name: str) -> str:
    head, separator, tail = operation_name.partition(":")
    if not separator:
        return f"<b>{head}</b>"
    return f"<b>{head}</b><span style='font-weight: 400;'> </span><i><span style='font-weight: 400;'>{tail}</span></i>"


__all__ = ["format_operation_title", "set_operation_cards"]
