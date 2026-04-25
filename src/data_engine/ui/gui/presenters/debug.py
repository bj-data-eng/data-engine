"""Debug-artifact presentation helpers for the desktop GUI."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from PySide6.QtCore import QSize, Qt
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QListWidgetItem, QVBoxLayout

from data_engine.platform.instrumentation import append_timing_line, new_request_id
from data_engine.services.debug_artifacts import clear_debug_artifacts, list_debug_artifacts
from data_engine.ui.gui.presenters.sidebar import repolish_widget_tree, set_hovered
from data_engine.ui.gui.rendering import build_preview_summary_text, populate_output_preview
from data_engine.views import classify_artifact_preview

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
        item = QListWidgetItem("")
        item.setData(Qt.ItemDataRole.UserRole, record)
        item.setToolTip("")
        item.setSizeHint(QSize(0, 42))
        list_widget.addItem(item)
        widget = _build_debug_artifact_row_widget(window, record)
        list_widget.setItemWidget(item, widget)
        if selected_stem is not None and record.stem == selected_stem:
            selected_index = index
    list_widget.blockSignals(False)
    if records:
        list_widget.setCurrentRow(selected_index)
        _refresh_debug_artifact_selection(list_widget)
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
    _refresh_debug_artifact_selection(list_widget)
    request_id = new_request_id("gui-select")
    append_timing_line(
        getattr(window, "_ui_timing_log_path", None),
        scope="gui.debug",
        event="select_artifact",
        phase="start",
        fields={
            "request_id": request_id,
            "artifact_path": record.artifact_path,
            "display_name": record.display_name or record.stem,
        },
    )
    title_label = getattr(window, "debug_artifact_title_label", None)
    if title_label is not None:
        title_label.setText("Dataframe")
    summary_label = getattr(window, "debug_artifact_summary_label", None)
    preview_spec = classify_artifact_preview(record.artifact_path)
    limit_spin = getattr(window, "debug_preview_limit_spin", None)
    if limit_spin is not None:
        show_controls = preview_spec.kind == "parquet"
        limit_spin.setVisible(show_controls)
    if summary_label is not None:
        if preview_spec.kind == "parquet":
            summary_label.setText("")
            summary_label.setVisible(True)
        else:
            summary_label.setText(build_preview_summary_text(record.artifact_path, preview_spec))
            summary_label.setVisible(True)
    source_label = getattr(window, "debug_artifact_source_label", None)
    if source_label is not None:
        source_label.setText(f"Source: {record.source_path}" if record.source_path else "")
        source_label.setVisible(bool(record.source_path))
    layout = getattr(window, "debug_preview_layout", None)
    if layout is None:
        return
    _clear_layout_widgets(layout)
    preview_widget = populate_output_preview(
        layout,
        record.artifact_path,
        preview_spec=preview_spec,
        show_summary=False,
        timing_log_path=getattr(window, "_ui_timing_log_path", None),
        external_preview_controls=(
            limit_spin,
            getattr(window, "debug_preview_controls_layout", None),
            summary_label,
        )
        if limit_spin is not None and getattr(window, "debug_preview_controls_layout", None) is not None and summary_label is not None
        else None,
    )
    append_timing_line(
        getattr(window, "_ui_timing_log_path", None),
        scope="gui.debug",
        event="select_artifact",
        phase="end",
        fields={
            "request_id": request_id,
            "artifact_path": record.artifact_path,
            "preview_kind": preview_spec.kind,
        },
    )
    if summary_label is not None and preview_spec.kind != "parquet" and hasattr(preview_widget, "summary_changed"):
        preview_widget.summary_changed.connect(summary_label.setText)


def _clear_debug_preview(window: "DataEngineWindow", *, message: str) -> None:
    title_label = getattr(window, "debug_artifact_title_label", None)
    if title_label is not None:
        title_label.setText("Dataframe")
    summary_label = getattr(window, "debug_artifact_summary_label", None)
    if summary_label is not None:
        summary_label.setText("")
        summary_label.setVisible(False)
    source_label = getattr(window, "debug_artifact_source_label", None)
    if source_label is not None:
        source_label.setText("")
        source_label.setVisible(False)
    limit_spin = getattr(window, "debug_preview_limit_spin", None)
    if limit_spin is not None:
        limit_spin.setVisible(False)
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
            shutdown = getattr(widget, "shutdown_background_work", None)
            if callable(shutdown):
                shutdown()
            widget.deleteLater()
        elif child_layout is not None:
            _clear_layout_widgets(child_layout)


def _debug_item_text(record) -> str:
    display = record.display_name or record.stem
    return display


def _build_debug_artifact_row_widget(window: "DataEngineWindow", record) -> QFrame:
    frame = QFrame(window.debug_artifact_list)
    frame.setObjectName("sidebarFlowRow")
    frame.setProperty("selected", False)
    frame.setProperty("hovered", False)
    frame.setFixedHeight(42)
    row = QHBoxLayout(frame)
    row.setContentsMargins(12, 4, 8, 4)
    row.setSpacing(10)

    text_col = QVBoxLayout()
    text_col.setContentsMargins(0, 0, 0, 0)
    text_col.setSpacing(1)

    title = QLabel(_debug_item_primary_text(record), frame)
    title.setObjectName("sidebarFlowCode")
    subtitle = QLabel(_debug_item_secondary_text(record), frame)
    subtitle.setObjectName("sidebarFlowMeta")

    text_col.addWidget(title)
    text_col.addWidget(subtitle)
    row.addLayout(text_col, 1)

    frame.enterEvent = lambda event, widget=frame: set_hovered(widget, True)
    frame.leaveEvent = lambda event, widget=frame: set_hovered(widget, False)
    return frame


def _refresh_debug_artifact_selection(list_widget) -> None:
    for index in range(list_widget.count()):
        item = list_widget.item(index)
        widget = list_widget.itemWidget(item)
        if widget is None:
            continue
        selected = item is list_widget.currentItem()
        if widget.property("selected") != selected:
            widget.setProperty("selected", selected)
            repolish_widget_tree(widget)


def _debug_item_primary_text(record) -> str:
    debug_info = record.metadata.get("debug") if isinstance(record.metadata.get("debug"), dict) else {}
    flow_name = str(debug_info.get("flow_name", record.flow_name) or record.flow_name).strip()
    return flow_name or (record.display_name or record.stem)


def _debug_item_secondary_text(record) -> str:
    debug_info = record.metadata.get("debug") if isinstance(record.metadata.get("debug"), dict) else {}
    step_name = str(debug_info.get("step_name", record.step_name or "") or "").strip()
    saved_at_utc = str(debug_info.get("saved_at_utc", record.created_at_utc) or record.created_at_utc).strip()
    parts: list[str] = []
    if step_name:
        parts.append(step_name)
    if saved_at_utc:
        parts.append(_format_debug_timestamp(saved_at_utc, include_milliseconds=True))
    return "  •  ".join(parts)


def _format_debug_timestamp(raw_value: str, *, include_milliseconds: bool = False) -> str:
    text = raw_value.strip()
    if not text:
        return text
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if include_milliseconds:
        return f"{parsed:%Y-%m-%d %H:%M:%S}.{parsed.microsecond // 1000:03d}"
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


__all__ = ["clear_workspace_debug_artifacts", "refresh_debug_artifacts", "show_selected_debug_artifact"]
