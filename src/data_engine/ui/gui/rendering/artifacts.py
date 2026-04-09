"""Artifact classification and preview rendering helpers."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHeaderView,
    QLabel,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
)

from data_engine.views import ArtifactPreviewSpec, classify_artifact_preview


def populate_output_preview(layout: QVBoxLayout, output_path: Path, preview_spec: ArtifactPreviewSpec | None = None) -> None:
    """Populate one dialog layout with the appropriate artifact preview widgets."""
    preview_spec = preview_spec or classify_artifact_preview(output_path)
    if preview_spec.kind == "parquet":
        _add_tabular_preview(layout, pl.read_parquet(output_path), preview_spec.label)
        return
    if preview_spec.kind == "excel":
        _add_tabular_preview(layout, pl.read_excel(output_path, sheet_id=1, engine="calamine"), preview_spec.label)
        return
    if preview_spec.kind == "text":
        _add_text_preview(layout, output_path, preview_spec.label)
        return
    if preview_spec.kind == "pdf":
        _add_placeholder_preview(
            layout,
            heading=preview_spec.label,
            message=preview_spec.placeholder_message or "PDF artifacts are recognized, but in-app PDF text inspection is not available yet.",
            output_path=output_path,
        )
        return
    _add_placeholder_preview(
        layout,
        heading=preview_spec.label,
        message=preview_spec.placeholder_message or "This artifact type is not previewable in the UI yet.",
        output_path=output_path,
    )


def _add_tabular_preview(layout: QVBoxLayout, frame: pl.DataFrame, heading: str) -> None:
    meta_label = QLabel(f"{heading}  {frame.height} row(s) x {len(frame.columns)} column(s)  Previewing up to 200 rows")
    meta_label.setObjectName("sectionMeta")
    layout.addWidget(meta_label)

    table = QTableWidget()
    table.setObjectName("outputPreviewTable")
    table.setColumnCount(len(frame.columns))
    table.setHorizontalHeaderLabels(frame.columns)
    preview = frame.head(200)
    table.setRowCount(preview.height)
    table.setAlternatingRowColors(True)
    table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
    table.setShowGrid(False)
    table.verticalHeader().setVisible(False)
    table.horizontalHeader().setStretchLastSection(True)
    table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
    for row_index in range(preview.height):
        for column_index, column_name in enumerate(preview.columns):
            table.setItem(row_index, column_index, QTableWidgetItem(str(preview[row_index, column_name])))
    layout.addWidget(table, 1)


def _add_text_preview(layout: QVBoxLayout, output_path: Path, heading: str) -> None:
    meta_label = QLabel(heading)
    meta_label.setObjectName("sectionMeta")
    layout.addWidget(meta_label)
    body = QTextEdit()
    body.setObjectName("outputPreviewText")
    body.setReadOnly(True)
    body.setPlainText(output_path.read_text(encoding="utf-8"))
    layout.addWidget(body, 1)


def _add_placeholder_preview(layout: QVBoxLayout, *, heading: str, message: str, output_path: Path) -> None:
    size_bytes = output_path.stat().st_size if output_path.exists() else 0
    meta_label = QLabel(f"{heading}  {size_bytes:,} bytes")
    meta_label.setObjectName("sectionMeta")
    layout.addWidget(meta_label)
    body = QTextEdit()
    body.setObjectName("outputPreviewText")
    body.setReadOnly(True)
    body.setPlainText(message)
    layout.addWidget(body, 1)
__all__ = ["ArtifactPreviewSpec", "classify_artifact_preview", "populate_output_preview"]
