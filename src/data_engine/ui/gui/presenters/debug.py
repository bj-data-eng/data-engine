"""Debug-artifact presentation helpers for the desktop GUI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel, QListWidgetItem

from data_engine.services.debug_artifacts import clear_debug_artifacts, list_debug_artifacts
from data_engine.ui.gui.rendering import populate_json_value_preview, populate_output_preview

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def refresh_debug_artifacts(window: "DataEngineWindow") -> None:
    """Reload the debug-artifact selector for the current workspace."""
    list_widget = getattr(window, "debug_artifact_list", None)
    if list_widget is None:
        return
    selected_stem = None
    current_item = list_widget.currentItem()
    if current_item is not None:
        record = current_item.data(Qt.ItemDataRole.UserRole)
        selected_stem = getattr(record, "stem", None)
    records = list_debug_artifacts(window.workspace_paths.runtime_state_dir)
    window._debug_artifact_records = records
    list_widget.blockSignals(True)
    list_widget.clear()
    selected_index = 0
    for index, record in enumerate(records):
        item = QListWidgetItem(_debug_item_text(record))
        item.setData(Qt.ItemDataRole.UserRole, record)
        item.setToolTip("")
        list_widget.addItem(item)
        if selected_stem is not None and record.stem == selected_stem:
            selected_index = index
    list_widget.blockSignals(False)
    status_label = getattr(window, "debug_status_label", None)
    if status_label is not None:
        status_label.setText(f"{len(records)} saved artifact(s)" if records else "No saved debug artifacts yet.")
    if records:
        list_widget.setCurrentRow(selected_index)
        show_selected_debug_artifact(window)
        return
    _clear_debug_preview(window, message="No saved debug artifacts yet.")


def clear_workspace_debug_artifacts(window: "DataEngineWindow") -> None:
    """Delete all saved debug artifacts for the current workspace and refresh the pane."""
    clear_debug_artifacts(window.workspace_paths.runtime_state_dir)
    refresh_debug_artifacts(window)


def show_selected_debug_artifact(window: "DataEngineWindow") -> None:
    """Render the currently selected debug artifact in the preview pane."""
    list_widget = getattr(window, "debug_artifact_list", None)
    if list_widget is None:
        return
    item = list_widget.currentItem()
    record = item.data(Qt.ItemDataRole.UserRole) if item is not None else None
    if record is None:
        _clear_debug_preview(window, message="Choose a saved debug artifact to preview it here.")
        return
    path_label = getattr(window, "debug_artifact_path_label", None)
    if path_label is not None:
        path_label.setText(str(record.artifact_path))
    source_label = getattr(window, "debug_artifact_source_label", None)
    if source_label is not None:
        source_label.setText(record.source_path or "")
        source_label.setVisible(bool(record.source_path))
    metadata_layout = getattr(window, "debug_metadata_layout", None)
    if metadata_layout is not None:
        _clear_layout_widgets(metadata_layout)
        if record.metadata:
            populate_json_value_preview(metadata_layout, record.metadata, heading="Metadata table preview")
        else:
            placeholder = QLabel("No metadata saved for this artifact.")
            placeholder.setObjectName("bodyText")
            placeholder.setWordWrap(True)
            metadata_layout.addWidget(placeholder)
            metadata_layout.addStretch(1)
    layout = getattr(window, "debug_preview_layout", None)
    if layout is None:
        return
    _clear_layout_widgets(layout)
    populate_output_preview(layout, record.artifact_path)


def _clear_debug_preview(window: "DataEngineWindow", *, message: str) -> None:
    path_label = getattr(window, "debug_artifact_path_label", None)
    if path_label is not None:
        path_label.setText("")
    source_label = getattr(window, "debug_artifact_source_label", None)
    if source_label is not None:
        source_label.setText("")
        source_label.setVisible(False)
    metadata_layout = getattr(window, "debug_metadata_layout", None)
    if metadata_layout is not None:
        _clear_layout_widgets(metadata_layout)
    layout = getattr(window, "debug_preview_layout", None)
    if layout is None:
        return
    _clear_layout_widgets(layout)
    placeholder = QLabel(message)
    placeholder.setObjectName("bodyText")
    placeholder.setWordWrap(True)
    layout.addWidget(placeholder)
    layout.addStretch(1)


def _clear_layout_widgets(layout) -> None:
    while layout.count():
        item = layout.takeAt(0)
        widget = item.widget()
        child_layout = item.layout()
        if widget is not None:
            widget.deleteLater()
        elif child_layout is not None:
            _clear_layout_widgets(child_layout)


def _debug_item_text(record) -> str:
    display = record.display_name or record.stem
    if record.kind == "parquet":
        return f"{display}  [DataFrame]"
    if record.kind == "json":
        return f"{display}  [JSON]"
    return f"{display}  [{record.kind}]"


__all__ = ["clear_workspace_debug_artifacts", "refresh_debug_artifacts", "show_selected_debug_artifact"]
