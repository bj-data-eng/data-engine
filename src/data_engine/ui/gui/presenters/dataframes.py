"""Dataframe source view presentation helpers."""

from __future__ import annotations

import ctypes
import gc
import os
from pathlib import Path
from typing import TYPE_CHECKING

from PySide6.QtWidgets import QFileDialog, QLabel

from data_engine.ui.gui.rendering import classify_artifact_preview, populate_output_preview

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def browse_dataframe_file(window: "DataEngineWindow") -> None:
    """Open a parquet-file picker and connect the selected file."""
    selected_path, _selected_filter = QFileDialog.getOpenFileName(
        window,
        "Select Parquet File",
        "",
        "Parquet Files (*.parquet)",
    )
    if selected_path:
        connect_dataframe_path(window, Path(selected_path))


def browse_dataframe_folder(window: "DataEngineWindow") -> None:
    """Open a folder picker and connect parquet files from the selected folder."""
    selected_path = QFileDialog.getExistingDirectory(window, "Select Parquet Folder", "")
    if selected_path:
        connect_dataframe_path(window, Path(selected_path))


def connect_dataframe_path(window: "DataEngineWindow", path: Path) -> None:
    """Connect one parquet file or a folder containing parquet files to the dataframe view."""
    source_path = Path(path)
    window.dataframe_source_input.setText(str(source_path))
    if source_path.is_file() and source_path.suffix.lower() == ".parquet":
        show_dataframe_source(window, source_path, label=source_path.name)
    elif source_path.is_dir():
        parquet_files = tuple(sorted(source_path.rglob("*.parquet"), key=lambda item: str(item).lower()))
        if parquet_files:
            show_dataframe_source(
                window,
                _recursive_parquet_glob(source_path),
                label=f"{len(parquet_files)} parquet files",
            )
            return
        clear_dataframe_preview(window, "No parquet files found.")
    else:
        clear_dataframe_preview(window, "Choose a .parquet file or a folder containing .parquet files.")


def show_dataframe_source(window: "DataEngineWindow", path: Path, *, label: str) -> None:
    """Render one parquet file or parquet glob in the dataframe preview pane."""
    window._dataframe_preview_path = path
    window.dataframe_preview_title_label.setText(label)
    window.dataframe_source_input.setToolTip(str(path))
    if hasattr(window, "dataframe_clear_button"):
        window.dataframe_clear_button.setEnabled(True)
    window.dataframe_preview_summary_label.setText("")
    window.dataframe_preview_summary_label.setVisible(True)
    window.dataframe_preview_mode_combo.setVisible(True)
    window.dataframe_preview_limit_spin.setVisible(True)
    _clear_layout_widgets(window.dataframe_preview_layout)
    preview_spec = classify_artifact_preview(path)
    populate_output_preview(
        window.dataframe_preview_layout,
        path,
        preview_spec=preview_spec,
        show_summary=False,
        timing_log_path=getattr(window, "_ui_timing_log_path", None),
        external_preview_controls=(
            window.dataframe_preview_mode_combo,
            window.dataframe_preview_limit_spin,
            window.dataframe_preview_controls_layout,
            window.dataframe_preview_summary_label,
        ),
    )


def show_selected_dataframe_file(window: "DataEngineWindow") -> None:
    """Render the currently connected dataframe source."""
    path = getattr(window, "_dataframe_preview_path", None)
    if not isinstance(path, Path):
        clear_dataframe_preview(window, "Choose a parquet source to preview it here.")
        return
    show_dataframe_source(window, path, label=window.dataframe_preview_title_label.text() or path.name)


def clear_dataframe_preview(window: "DataEngineWindow", message: str) -> None:
    """Clear the dataframe preview pane and show a placeholder message."""
    window._dataframe_preview_path = None
    window.dataframe_source_input.clear()
    window.dataframe_source_input.setToolTip("")
    if hasattr(window, "dataframe_clear_button"):
        window.dataframe_clear_button.setEnabled(False)
    window.dataframe_preview_title_label.setText("Preview")
    window.dataframe_preview_summary_label.setText("")
    window.dataframe_preview_summary_label.setVisible(False)
    window.dataframe_preview_mode_combo.setVisible(False)
    window.dataframe_preview_limit_spin.setVisible(False)
    _clear_layout_widgets(window.dataframe_preview_layout)
    placeholder = QLabel(message)
    placeholder.setObjectName("bodyText")
    placeholder.setWordWrap(True)
    window.dataframe_preview_layout.addWidget(placeholder)
    window.dataframe_preview_layout.addStretch(1)
    _release_unused_preview_memory()


def _recursive_parquet_glob(path: Path) -> Path:
    return Path(path) / "**" / "*.parquet"


def _release_unused_preview_memory() -> None:
    """Ask Python and native allocators to release unused preview memory."""
    gc.collect()
    try:
        import pyarrow as pa

        pa.default_memory_pool().release_unused()
    except Exception:
        pass
    if os.name == "nt":
        try:
            ctypes.CDLL("msvcrt")._heapmin()
        except Exception:
            pass
        return
    try:
        ctypes.CDLL(None).malloc_trim(0)
    except Exception:
        pass


def _clear_layout_widgets(layout) -> None:
    while layout.count():
        item = layout.takeAt(0)
        widget = item.widget()
        child_layout = item.layout()
        if widget is not None:
            shutdown = getattr(widget, "shutdown_background_work", None)
            if callable(shutdown):
                shutdown()
            widget.deleteLater()
        elif child_layout is not None:
            _clear_layout_widgets(child_layout)


__all__ = [
    "browse_dataframe_file",
    "browse_dataframe_folder",
    "clear_dataframe_preview",
    "connect_dataframe_path",
    "show_dataframe_source",
    "show_selected_dataframe_file",
]
