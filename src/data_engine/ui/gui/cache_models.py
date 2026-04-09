"""Explicit GUI-local cache models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from PySide6.QtWidgets import QFrame, QLabel, QPushButton


@dataclass(frozen=True)
class OperationRowWidgets:
    """Cached widget references for one operation row."""

    row_card: "QFrame"
    title_label: "QLabel"
    duration_label: "QLabel"
    inspect_button: "QPushButton | None"
    operation_name: str


__all__ = ["OperationRowWidgets"]
