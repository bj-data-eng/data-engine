"""Preview dialog helpers for the desktop UI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from data_engine.ui.gui.preview_models import ConfigPreviewRequest, OutputPreviewRequest, RunLogPreviewRequest
from data_engine.ui.gui.rendering import (
    build_preview_summary_text,
    classify_artifact_preview,
    populate_output_preview,
    render_svg_icon_pixmap,
)
from data_engine.ui.gui.widgets import build_config_value, make_label_selectable
from data_engine.views.presentation import format_seconds

if TYPE_CHECKING:
    from data_engine.domain import FlowLogEntry, FlowRunState
    from data_engine.ui.gui.app import DataEngineWindow


@dataclass(frozen=True)
class _RunLogPreviewRow:
    entry: "FlowLogEntry"
    message_html: str
    duration_text: str | None
    status_name: str | None
    failed_entry: "FlowLogEntry | None" = None


def show_run_log_preview(window: "DataEngineWindow", request: RunLogPreviewRequest) -> QDialog:
    detail = request.detail
    dialog = QDialog(window)
    dialog.setWindowTitle("Run Log")
    dialog.setObjectName("outputPreviewDialog")
    dialog.resize(760, 520)
    layout = QVBoxLayout(dialog)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(12)

    header = QFrame()
    header.setObjectName("outputPreviewHeader")
    header_layout = QVBoxLayout(header)
    header_layout.setContentsMargins(14, 14, 14, 14)
    header_layout.setSpacing(4)

    title_label = QLabel(detail.display_label)
    title_label.setObjectName("heroTitle")
    header_layout.addWidget(title_label)

    summary_parts = [detail.status.title()]
    if detail.elapsed_seconds is not None:
        summary_parts.append(window._format_seconds(detail.elapsed_seconds))
    summary_label = QLabel("  •  ".join(summary_parts))
    summary_label.setObjectName("sectionMeta")
    header_layout.addWidget(summary_label)
    if request.source_path not in {None, "", "-"}:
        source_path_label = QLabel(str(request.source_path))
        source_path_label.setObjectName("outputPreviewPath")
        source_path_label.setWordWrap(True)
        make_label_selectable(source_path_label)
        header_layout.addWidget(source_path_label)
    layout.addWidget(header)

    log_list = QListWidget()
    log_list.setObjectName("runLogList")
    log_list.setSpacing(6)
    for row in _build_run_log_preview_rows(window, request.run_group):
        item = QListWidgetItem(row.entry.line)
        widget = _build_raw_log_entry_widget(window, row, run_group=request.run_group)
        item.setSizeHint(widget.sizeHint())
        log_list.addItem(item)
        log_list.setItemWidget(item, widget)
    layout.addWidget(log_list, 1)

    _present_dialog(dialog)
    return dialog


def _build_run_log_preview_rows(window: "DataEngineWindow", run_group: "FlowRunState") -> tuple[_RunLogPreviewRow, ...]:
    pending_step_starts: dict[str, FlowLogEntry] = {}
    rows: list[_RunLogPreviewRow] = []
    has_step_entries = any(
        entry.event is not None and entry.event.step_name is not None
        for entry in run_group.entries
    )
    for entry in run_group.entries:
        event = entry.event
        if event is None or event.step_name is None:
            if (
                has_step_entries
                and event is not None
                and event.step_name is None
                and event.status in {"success", "failed", "stopped"}
            ):
                continue
            rows.append(_preview_row_for_entry(window, entry))
            continue
        if event.status == "started":
            pending_step_starts[event.step_name] = entry
            continue
        if event.status in {"success", "failed", "stopped"} and event.step_name in pending_step_starts:
            pending_step_starts.pop(event.step_name, None)
            rows.append(_preview_row_for_entry(window, entry))
            continue
        rows.append(_preview_row_for_entry(window, entry))
    for entry in pending_step_starts.values():
        rows.append(_preview_row_for_entry(window, entry))
    return tuple(rows)


def _preview_row_for_entry(window: "DataEngineWindow", entry: "FlowLogEntry") -> _RunLogPreviewRow:
    event = entry.event
    status_name: str | None = None
    failed_entry: FlowLogEntry | None = None
    if event is not None and event.status in {"started", "failed", "success", "stopped"}:
        status_name = "failed" if event.status == "failed" else "started" if event.status == "started" else "finished"
        if event.status == "failed":
            failed_entry = entry
    duration_text = None
    if event is not None and isinstance(event.elapsed_seconds, (int, float)):
        duration_text = format_seconds(event.elapsed_seconds)
    return _RunLogPreviewRow(
        entry=entry,
        message_html=_format_preview_log_message(entry),
        duration_text=duration_text,
        status_name=status_name,
        failed_entry=failed_entry,
    )


def _format_preview_log_message(entry: "FlowLogEntry") -> str:
    from html import escape

    event = entry.event
    if event is None:
        return escape(entry.line)
    flow_name = escape(event.flow_name)
    source_label = escape(event.source_label)
    status = escape(event.status)
    has_source = event.source_label not in {"", "-"}
    if event.step_name is None:
        if has_source:
            return f"{flow_name} &gt; {source_label} &gt; <i>{status}</i>"
        return f"{flow_name} &gt; <i>{status}</i>"
    step_name = escape(event.step_name.replace(":", "::", 1))
    if has_source:
        return f"{flow_name} &gt; {source_label} &gt; <b>{step_name}</b> - <i>{status}</i>"
    return f"{flow_name} &gt; <b>{step_name}</b> - <i>{status}</i>"


def _build_raw_log_entry_widget(window: "DataEngineWindow", row: _RunLogPreviewRow, *, run_group: "FlowRunState") -> QFrame:
    frame = QFrame()
    frame.setObjectName("rawLogRow")
    frame.setMinimumHeight(40)
    layout = QHBoxLayout(frame)
    layout.setContentsMargins(8, 6, 8, 6)
    layout.setSpacing(10)

    timestamp = QLabel(row.entry.created_at_utc.astimezone().strftime("%I:%M:%S %p"))
    timestamp.setObjectName("rawLogTimestamp")
    make_label_selectable(timestamp)
    layout.addWidget(timestamp, 0, Qt.AlignmentFlag.AlignVCenter)

    message = QLabel(row.message_html)
    message.setObjectName("rawLogMessage")
    message.setTextFormat(Qt.TextFormat.RichText)
    message.setWordWrap(False)
    make_label_selectable(message)
    layout.addWidget(message, 1, Qt.AlignmentFlag.AlignVCenter)

    duration = QLabel(row.duration_text or "")
    duration.setObjectName("logDuration")
    duration.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
    duration.setVisible(row.duration_text is not None)
    layout.addWidget(duration, 0, Qt.AlignmentFlag.AlignVCenter)

    inspect_slot = QWidget()
    inspect_slot.setObjectName("rawLogInspectSlot")
    inspect_slot.setFixedWidth(108)
    inspect_slot_layout = QHBoxLayout(inspect_slot)
    inspect_slot_layout.setContentsMargins(0, 0, 0, 0)
    inspect_slot_layout.setSpacing(0)
    inspect_slot_layout.addStretch(1)
    inspect_button = QPushButton("Inspect")
    inspect_button.setObjectName("inspectOutputButton")
    inspect_button.setFixedWidth(96)
    inspect_button.setEnabled(False)
    inspect_slot_layout.addWidget(inspect_button, 0, Qt.AlignmentFlag.AlignVCenter)

    icon_slot = QWidget()
    icon_slot.setObjectName("rawLogIconSlot")
    icon_slot.setFixedWidth(20)
    icon_slot_layout = QHBoxLayout(icon_slot)
    icon_slot_layout.setContentsMargins(0, 0, 0, 0)
    icon_slot_layout.setSpacing(0)
    icon_slot_layout.addStretch(1)

    if row.failed_entry is not None:
        inspect_button.setEnabled(True)
        inspect_button.clicked.connect(
            lambda _checked=False, group=run_group, failed_entry=row.failed_entry: window._show_run_error_details(group, failed_entry)
        )
    if row.status_name is not None:
        status_icon = QLabel()
        status_icon.setObjectName("rawLogStatusIcon")
        fill = window._LOG_ICON_COLORS.get(row.status_name, window._group_icon_color().name())
        status_icon.setPixmap(
            render_svg_icon_pixmap(
                icon_name=window._LOG_ICON_NAMES[row.status_name],
                size=14,
                device_pixel_ratio=window.devicePixelRatioF(),
                fill_color=fill,
                default_fill_color=window._group_icon_color(),
            )
        )
        status_icon.setToolTip("")
        icon_slot_layout.addWidget(status_icon, 0, Qt.AlignmentFlag.AlignVCenter)

    layout.addWidget(inspect_slot, 0, Qt.AlignmentFlag.AlignVCenter)
    layout.addWidget(icon_slot, 0, Qt.AlignmentFlag.AlignVCenter)
    return frame


def show_output_preview(window: "DataEngineWindow", request: OutputPreviewRequest) -> QDialog:
    dialog = QDialog(window)
    dialog.setWindowTitle("Inspect Output")
    dialog.setObjectName("outputPreviewDialog")
    target_width = max(min(window.width() - 50, 1600), 720)
    dialog.resize(target_width, 520)
    layout = QVBoxLayout(dialog)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(12)

    header = QFrame()
    header.setObjectName("outputPreviewHeader")
    header_layout = QVBoxLayout(header)
    header_layout.setContentsMargins(14, 14, 14, 14)
    header_layout.setSpacing(6)

    preview_spec = classify_artifact_preview(request.output_path)

    top_row = QHBoxLayout()
    top_row.setContentsMargins(0, 0, 0, 0)
    top_row.setSpacing(8)

    title_label = QLabel("Dataframe")
    title_label.setObjectName("heroTitle")
    top_row.addWidget(title_label, 0, Qt.AlignmentFlag.AlignVCenter)
    top_row.addStretch(1)

    mode_combo = QComboBox()
    mode_combo.setObjectName("outputPreviewModeCombo")
    mode_combo.setFixedHeight(36)
    limit_spin = QSpinBox()
    limit_spin.setObjectName("outputPreviewLimitSpin")
    limit_spin.setFixedHeight(36)
    show_controls = preview_spec.kind == "parquet"
    mode_combo.setVisible(show_controls)
    limit_spin.setVisible(show_controls)
    top_row.addWidget(mode_combo, 0, Qt.AlignmentFlag.AlignVCenter)
    top_row.addWidget(limit_spin, 0, Qt.AlignmentFlag.AlignVCenter)
    header_layout.addLayout(top_row)

    summary_label = QLabel(
        "Loading preview…" if preview_spec.kind == "parquet" else build_preview_summary_text(request.output_path, preview_spec)
    )
    summary_label.setObjectName("sectionMeta")
    header_layout.addWidget(summary_label)

    path_label = QLabel(f"Source: {request.output_path}")
    path_label.setObjectName("outputPreviewPath")
    path_label.setWordWrap(True)
    make_label_selectable(path_label)
    header_layout.addWidget(path_label)
    layout.addWidget(header)

    preview_widget = populate_output_preview(
        layout,
        request.output_path,
        preview_spec=preview_spec,
        show_summary=False,
        external_preview_controls=(mode_combo, limit_spin),
    )
    if hasattr(preview_widget, "summary_changed"):
        preview_widget.summary_changed.connect(summary_label.setText)

    _present_dialog(dialog)
    return dialog


def show_config_preview(window: "DataEngineWindow", request: ConfigPreviewRequest) -> QDialog:
    preview_state = request.preview
    dialog = QDialog(window)
    dialog.setWindowTitle(preview_state.title)
    dialog.setObjectName("outputPreviewDialog")
    dialog.resize(560, 420)
    layout = QVBoxLayout(dialog)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(12)

    header = QFrame()
    header.setObjectName("outputPreviewHeader")
    header_layout = QVBoxLayout(header)
    header_layout.setContentsMargins(14, 14, 14, 14)
    header_layout.setSpacing(4)

    title_label = QLabel(preview_state.title)
    title_label.setObjectName("heroTitle")
    title_label.setWordWrap(True)
    header_layout.addWidget(title_label)
    if preview_state.description:
        description_label = QLabel(preview_state.description)
        description_label.setObjectName("bodyText")
        description_label.setWordWrap(True)
        header_layout.addWidget(description_label)
    layout.addWidget(header)

    body = QFrame()
    body.setObjectName("configPreviewBody")
    body_layout = QVBoxLayout(body)
    body_layout.setContentsMargins(14, 14, 14, 14)
    body_layout.setSpacing(2)
    for row in preview_state.summary.rows:
        row_value = build_config_value(body_layout, row.label)
        row_value.setText(row.value)
    body_layout.addStretch(1)
    layout.addWidget(body, 1)

    _present_dialog(dialog)
    return dialog


def _present_dialog(dialog: QDialog) -> None:
    dialog.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
    dialog.show()
    dialog.raise_()
    dialog.activateWindow()


__all__ = ["show_config_preview", "show_output_preview", "show_run_log_preview"]
