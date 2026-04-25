"""Artifact classification and preview rendering helpers."""

from __future__ import annotations

from pathlib import Path
import random
import polars as pl
from PySide6.QtCore import QEvent, QPoint, QRect, QSize, QThread, QTimer, Qt, Signal
from PySide6.QtGui import QColor, QFont, QFontMetrics, QIcon, QKeySequence, QPainter
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QFileDialog,
    QFrame,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QMenu,
    QPushButton,
    QSpinBox,
    QStyle,
    QStyleOptionHeader,
    QStyleOptionViewItem,
    QStyledItemDelegate,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from data_engine.helpers import write_excel_atomic
from data_engine.platform.instrumentation import append_timing_line, new_request_id
from data_engine.ui.gui.rendering.icons import render_svg_icon_pixmap
from data_engine.views import ArtifactPreviewSpec, classify_artifact_preview

_PREVIEW_ROW_LIMIT = 200
_PREVIEW_ROW_LIMIT_MIN = 1
_PREVIEW_ROW_LIMIT_MAX = 500_000
_TABLE_RENDER_BATCH_SIZE = 500
_PREVIEW_DISTINCT_VALUE_LIMIT = 500
_PREVIEW_MODE_TOP = "top"
_PREVIEW_MODE_BOTTOM = "bottom"
_PREVIEW_MODE_SAMPLE = "sample"
_NULL_FILTER_VALUE = object()


class _PreviewHeaderView(QHeaderView):
    """Header view that paints title, dtype, sort badge, and dropdown caret separately."""

    def __init__(self, orientation: Qt.Orientation, parent: QWidget | None = None) -> None:
        super().__init__(orientation, parent)
        self._header_metadata: list[dict[str, object]] = []

    def set_preview_metadata(self, metadata: list[dict[str, object]]) -> None:
        self._header_metadata = metadata
        self.viewport().update()

    def _theme_colors(self) -> tuple[QColor, QColor, QColor, QColor]:
        widget: QWidget | None = self
        while widget is not None:
            theme_service = getattr(widget, "theme_service", None)
            theme_name = getattr(widget, "theme_name", None)
            if theme_service is not None and isinstance(theme_name, str):
                palette = theme_service.palette(theme_name)
                return (
                    QColor(palette.text),
                    QColor(palette.section_text),
                    QColor(palette.hover_bg),
                    QColor(palette.text),
                )
            parent_widget = widget.parentWidget()
            if parent_widget is widget:
                break
            widget = parent_widget
        return QColor("#1f2328"), QColor("#57606a"), QColor("#0969da"), QColor("#ffffff")

    def paintSection(self, painter: QPainter, rect: QRect, logical_index: int) -> None:  # noqa: N802
        if not rect.isValid():
            return
        option = QStyleOptionHeader()
        self.initStyleOption(option)
        option.rect = rect
        option.section = logical_index
        option.text = ""
        self.style().drawControl(QStyle.ControlElement.CE_HeaderSection, option, painter, self)
        if logical_index < 0 or logical_index >= len(self._header_metadata):
            return

        metadata = self._header_metadata[logical_index]
        title = str(metadata.get("title", ""))
        dtype_text = str(metadata.get("dtype", ""))
        sort_marker = metadata.get("sort_marker")
        if bool(metadata.get("filtered", False)):
            title = f"* {title}"

        text_color, muted_color, badge_bg, badge_text = self._theme_colors()
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.TextAntialiasing, True)

        content_rect = rect.adjusted(10, 4, -28, -3)

        badge_rect: QRect | None = None
        if isinstance(sort_marker, tuple):
            sort_rank, descending = sort_marker
            badge_label = f"{sort_rank}{'↓' if descending else '↑'}"
            badge_font = QFont(self.font())
            badge_font.setPointSize(max(7, badge_font.pointSize() - 3))
            badge_font.setBold(True)
            badge_metrics = QFontMetrics(badge_font)
            badge_width = max(16, badge_metrics.horizontalAdvance(badge_label) + 6)
            badge_height = max(13, badge_metrics.height() + 1)
            badge_rect = QRect(rect.right() - badge_width - 6, rect.center().y() - (badge_height // 2), badge_width, badge_height)
            badge_fill = QColor(badge_bg)
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(badge_fill)
            painter.drawRect(badge_rect)
            painter.setPen(badge_text)
            painter.setFont(badge_font)
            painter.drawText(badge_rect, int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter), badge_label)

        title_rect = QRect(content_rect.left(), content_rect.top(), content_rect.width(), 14)
        dtype_rect = QRect(content_rect.left(), title_rect.bottom(), content_rect.width(), 11)

        title_font = QFont(self.font())
        title_font.setBold(True)
        title_font.setPointSize(max(8, title_font.pointSize() - 1))
        painter.setPen(text_color)
        painter.setFont(title_font)
        painter.drawText(title_rect, int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop | Qt.TextFlag.TextSingleLine), title)

        dtype_font = QFont(self.font())
        dtype_font.setBold(False)
        dtype_font.setItalic(True)
        dtype_font.setPointSize(max(7, dtype_font.pointSize() - 3))
        painter.setPen(muted_color)
        painter.setFont(dtype_font)
        painter.drawText(dtype_rect, int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop | Qt.TextFlag.TextSingleLine), dtype_text)

        painter.restore()


class _PreviewBodyItemDelegate(QStyledItemDelegate):
    """Delegate that applies the intended preview table body font during paint."""

    def initStyleOption(self, option: QStyleOptionViewItem, index) -> None:  # noqa: N802
        super().initStyleOption(option, index)
        body_font = QFont(option.font)
        body_font.setFamilies(["Segoe UI", "Helvetica", "Arial"])
        body_font.setPointSize(9)
        body_font.setWeight(QFont.Weight.Medium)
        option.font = body_font
        option.fontMetrics = QFontMetrics(body_font)


class _ParquetPreviewLoader(QThread):
    """Background loader for parquet preview slices and summary text."""

    preview_loaded = Signal(object, object, str)
    load_failed = Signal(str)

    def __init__(
        self,
        output_path: Path,
        *,
        active_value_filters: dict[str, tuple[object, ...]],
        sort_columns: tuple[tuple[str, bool], ...],
        preview_mode: str,
        preview_row_limit: int,
    ) -> None:
        super().__init__()
        self._output_path = Path(output_path)
        self._active_value_filters = dict(active_value_filters)
        self._sort_columns = tuple((str(column_name), bool(descending)) for column_name, descending in sort_columns)
        self._preview_mode = preview_mode
        self._preview_row_limit = max(_PREVIEW_ROW_LIMIT_MIN, min(preview_row_limit, _PREVIEW_ROW_LIMIT_MAX))

    def run(self) -> None:
        try:
            lazy_frame = pl.scan_parquet(self._output_path)
            schema = lazy_frame.collect_schema()
            query = lazy_frame
            for column_name, selected_values in self._active_value_filters.items():
                expression = _build_distinct_value_filter_expression(
                    column_name,
                    selected_values,
                    dtype=schema[column_name],
                )
                if expression is not None:
                    query = query.filter(expression)
            row_count = (
                query.select(pl.len().alias("__row_count__"))
                .collect()
                .get_column("__row_count__")
                .item()
            )
            if self._sort_columns:
                query = query.sort(
                    [column_name for column_name, _descending in self._sort_columns],
                    descending=[descending for _column_name, descending in self._sort_columns],
                )
            if self._preview_mode == _PREVIEW_MODE_BOTTOM:
                preview = query.tail(self._preview_row_limit).collect()
                preview_label = f"Showing bottom {self._preview_row_limit} rows"
            elif self._preview_mode == _PREVIEW_MODE_SAMPLE:
                sample_size = min(self._preview_row_limit, row_count)
                if sample_size <= 0:
                    preview = query.head(0).collect()
                elif sample_size >= row_count:
                    preview = query.collect()
                else:
                    sample_indices = sorted(random.Random(0).sample(range(int(row_count)), int(sample_size)))
                    preview = (
                        query.with_row_index("__preview_row_index")
                        .filter(pl.col("__preview_row_index").is_in(sample_indices))
                        .drop("__preview_row_index")
                        .collect()
                    )
                preview_label = f"Showing sample of {self._preview_row_limit} rows"
            else:
                preview = query.head(self._preview_row_limit).collect()
                preview_label = f"Showing top {self._preview_row_limit} rows"
            summary = f"{row_count} row(s)  •  {len(schema.names())} column(s)  •  {preview_label}"
            self.preview_loaded.emit(schema, preview, summary)
        except Exception as exc:  # pragma: no cover - defensive UI fallback
            self.load_failed.emit(str(exc))


class _DistinctValueLoader(QThread):
    """Background loader for searched parquet column values."""

    values_loaded = Signal(str, int, object, bool)
    load_failed = Signal(str, int, str)

    def __init__(
        self,
        output_path: Path,
        column_name: str,
        *,
        token: int,
        active_value_filters: dict[str, tuple[object, ...]],
        search_text: str,
        value_limit: int = 500,
    ) -> None:
        super().__init__()
        self._output_path = Path(output_path)
        self._column_name = column_name
        self._token = token
        self._active_value_filters = dict(active_value_filters)
        self._search_text = search_text.strip().lower()
        self._value_limit = max(1, value_limit)

    def run(self) -> None:
        try:
            query = pl.scan_parquet(self._output_path)
            schema = query.collect_schema()
            for active_name, selected_values in self._active_value_filters.items():
                if active_name == self._column_name:
                    continue
                expression = _build_distinct_value_filter_expression(
                    active_name,
                    selected_values,
                    dtype=schema[active_name],
                )
                if expression is not None:
                    query = query.filter(expression)
            column = pl.col(self._column_name)
            if self._search_text:
                query = query.filter(
                    column.cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.contains(
                        self._search_text,
                        literal=True,
                    )
                )
            series = (
                query.select(column.unique(maintain_order=True).head(self._value_limit + 1).alias(self._column_name))
                .collect()
                .get_column(self._column_name)
            )
            raw_values = series.to_list()
            truncated = len(raw_values) > self._value_limit
            values: list[tuple[str, object]] = []
            for value in raw_values[: self._value_limit]:
                if value is None:
                    values.append(("(blank)", _NULL_FILTER_VALUE))
                else:
                    values.append((str(value), value))
            self.values_loaded.emit(self._column_name, self._token, values, truncated)
        except Exception as exc:  # pragma: no cover - defensive UI fallback
            self.load_failed.emit(self._column_name, self._token, str(exc))


class _ParquetFilterPopup(QFrame):
    """Small Excel-style popup for one parquet column filter."""

    def __init__(
        self,
        explorer: "_ParquetExplorerWidget",
        *,
        column_name: str,
        dtype: pl.DataType,
        values: list[tuple[str, object]],
    ) -> None:
        super().__init__(explorer, Qt.WindowType.Tool | Qt.WindowType.FramelessWindowHint)
        self.setObjectName("outputPreviewFilterPopup")
        self._explorer = explorer
        self._column_name = column_name
        self._values = values
        self._search_token = 0
        self._value_domain_complete = False
        self._sort_ascending_button: QPushButton | None = None
        self._sort_descending_button: QPushButton | None = None
        self._select_all_button: QPushButton | None = None
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating, True)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        title = QLabel(f"{column_name} ({dtype})", self)
        title.setObjectName("sectionTitle")
        layout.addWidget(title)

        self.search_input = QLineEdit(self)
        self.search_input.setObjectName("outputPreviewPopupSearch")
        self.search_input.setPlaceholderText("Search values")
        self.search_input.setClearButtonEnabled(True)
        layout.addWidget(self.search_input)

        self.status_label = QLabel("", self)
        self.status_label.setObjectName("sectionMeta")
        self.status_label.setWordWrap(True)
        self.status_label.setVisible(False)
        layout.addWidget(self.status_label)

        controls_frame = QFrame(self)
        controls_frame.setObjectName("outputPreviewControlBar")
        layout.addWidget(controls_frame)

        sort_actions = QHBoxLayout(controls_frame)
        sort_actions.setContentsMargins(6, 6, 6, 6)
        sort_actions.setSpacing(6)
        self._select_all_button = QPushButton("", self)
        self._select_all_button.setObjectName("outputPreviewSelectAllButton")
        self._select_all_button.setFixedSize(28, 28)
        self._select_all_button.setIconSize(QSize(16, 16))
        self._select_all_button.clicked.connect(self._toggle_select_all)
        sort_actions.addWidget(self._select_all_button)
        sort_actions.addStretch(1)
        self._sort_ascending_button = QPushButton("", self)
        self._sort_ascending_button.setObjectName("outputPreviewSortAscendingButton")
        self._sort_ascending_button.setFixedSize(28, 28)
        self._sort_ascending_button.setIconSize(QSize(16, 16))
        self._sort_ascending_button.clicked.connect(lambda: self._apply_sort(descending=False))
        sort_actions.addWidget(self._sort_ascending_button)
        self._sort_descending_button = QPushButton("", self)
        self._sort_descending_button.setObjectName("outputPreviewSortDescendingButton")
        self._sort_descending_button.setFixedSize(28, 28)
        self._sort_descending_button.setIconSize(QSize(16, 16))
        self._sort_descending_button.clicked.connect(lambda: self._apply_sort(descending=True))
        sort_actions.addWidget(self._sort_descending_button)
        self._refresh_sort_button_state()

        self.values_list = QListWidget(self)
        self.values_list.setObjectName("outputPreviewPopupList")
        self.values_list.setMinimumWidth(220)
        self.values_list.setMinimumHeight(240)
        self.values_list.itemChanged.connect(self._sync_select_all_button)
        layout.addWidget(self.values_list, 1)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(6)
        footer.addStretch(1)
        cancel_button = QPushButton("Cancel", self)
        cancel_button.setObjectName("filterPopupActionButton")
        cancel_button.clicked.connect(self.close)
        footer.addWidget(cancel_button)
        apply_button = QPushButton("Apply", self)
        apply_button.setObjectName("filterPopupActionButton")
        apply_button.clicked.connect(self._apply_selection)
        footer.addWidget(apply_button)
        layout.addLayout(footer)

        self._search_timer = QTimer(self)
        self._search_timer.setSingleShot(True)
        self._search_timer.setInterval(180)
        self._search_timer.timeout.connect(self._dispatch_search)
        self.search_input.textChanged.connect(self._queue_search)
        self.set_values(values)

    def showEvent(self, event) -> None:  # noqa: N802
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self)
        super().showEvent(event)

    def changeEvent(self, event) -> None:  # noqa: N802
        if isinstance(event, QEvent) and event.type() in {
            QEvent.Type.PaletteChange,
            QEvent.Type.StyleChange,
            QEvent.Type.ApplicationPaletteChange,
        }:
            self._refresh_sort_button_state()
            self._sync_select_all_button()
        super().changeEvent(event)

    def hideEvent(self, event) -> None:  # noqa: N802
        app = QApplication.instance()
        if app is not None:
            app.removeEventFilter(self)
        if self._explorer._filter_popup is self:
            self._explorer._filter_popup = None
            self._explorer._open_filter_column_index = None
        super().hideEvent(event)

    def eventFilter(self, watched: object, event: object) -> bool:
        if isinstance(event, QEvent):
            if event.type() == QEvent.Type.MouseButtonPress:
                global_position = getattr(event, "globalPosition", None)
                if callable(global_position):
                    global_point = global_position().toPoint()
                else:
                    global_pos = getattr(event, "globalPos", lambda: None)()
                    global_point = global_pos
                if global_point is not None and not self.frameGeometry().contains(global_point):
                    self.close()
                    return True
            elif event.type() == QEvent.Type.KeyPress and getattr(event, "key", lambda: None)() == Qt.Key.Key_Escape:
                self.close()
                return True
        return super().eventFilter(watched, event)

    def set_values(
        self,
        values: list[tuple[str, object]],
        *,
        loading: bool = False,
        note: str = "",
        complete_domain: bool = False,
    ) -> None:
        self._values = values
        self._value_domain_complete = complete_domain
        self.values_list.setEnabled(bool(values) or not loading)
        self.search_input.setEnabled(True)
        self.status_label.setVisible(loading or bool(note))
        if loading:
            self.status_label.setText("Loading values...")
        else:
            self.status_label.setText(note)
        self._populate_items()

    def set_error(self, message: str) -> None:
        self.values_list.clear()
        self.values_list.setEnabled(False)
        self.search_input.setEnabled(True)
        self.status_label.setVisible(True)
        self.status_label.setText(message)

    def _populate_items(self) -> None:
        self.values_list.clear()
        selected_values = self._explorer.selected_filter_values(self._column_name)
        for label, value in self._values:
            item = QListWidgetItem(label)
            item.setData(Qt.ItemDataRole.UserRole, value)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(
                Qt.CheckState.Checked
                if selected_values is None or value in selected_values
                else Qt.CheckState.Unchecked
            )
            self.values_list.addItem(item)
        self._sync_select_all_button()

    def _apply_select_all_state(self, state: Qt.CheckState | int) -> None:
        if state == Qt.CheckState.PartiallyChecked:
            return
        target_state = Qt.CheckState.Checked if state == Qt.CheckState.Checked else Qt.CheckState.Unchecked
        self.values_list.blockSignals(True)
        try:
            for index in range(self.values_list.count()):
                item = self.values_list.item(index)
                if not item.isHidden():
                    item.setCheckState(target_state)
        finally:
            self.values_list.blockSignals(False)
        self._sync_select_all_button()

    def _select_all_check_state(self) -> Qt.CheckState:
        visible_total = 0
        visible_checked = 0
        for index in range(self.values_list.count()):
            item = self.values_list.item(index)
            if item.isHidden():
                continue
            visible_total += 1
            if item.checkState() == Qt.CheckState.Checked:
                visible_checked += 1
        if visible_total == 0:
            return Qt.CheckState.Unchecked
        if visible_checked == 0:
            return Qt.CheckState.Unchecked
        if visible_checked == visible_total:
            return Qt.CheckState.Checked
        return Qt.CheckState.PartiallyChecked

    def _sync_select_all_button(self) -> None:
        if self._select_all_button is None:
            return
        state = self._select_all_check_state()
        icon_name = {
            Qt.CheckState.Checked: "select-all-all",
            Qt.CheckState.PartiallyChecked: "select-all-partial",
            Qt.CheckState.Unchecked: "select-all-none",
        }[state]
        icon_fill = self._icon_fill_color()
        self._select_all_button.setIcon(
            QIcon(
                render_svg_icon_pixmap(
                    icon_name=icon_name,
                    size=24,
                    device_pixel_ratio=max(1.0, self.devicePixelRatioF()),
                    default_fill_color=icon_fill,
                )
            )
        )
        self._select_all_button.setToolTip(
            {
                Qt.CheckState.Checked: "All visible values selected",
                Qt.CheckState.PartiallyChecked: "Some visible values selected",
                Qt.CheckState.Unchecked: "No visible values selected",
            }[state]
        )
        self._select_all_button.setAccessibleName("Select all values")
        self._select_all_button.setProperty("selectAllState", int(state.value))
        style = self._select_all_button.style()
        style.unpolish(self._select_all_button)
        style.polish(self._select_all_button)
        self._select_all_button.update()

    def _icon_fill_color(self):
        window = self.window()
        theme_service = getattr(window, "theme_service", None)
        theme_name = getattr(window, "theme_name", None)
        if theme_service is not None and isinstance(theme_name, str):
            return theme_service.palette(theme_name).text
        return self.palette().buttonText().color()

    def _toggle_select_all(self) -> None:
        current_state = self._select_all_check_state()
        target_state = Qt.CheckState.Unchecked if current_state == Qt.CheckState.Checked else Qt.CheckState.Checked
        self._apply_select_all_state(target_state)

    def _apply_selection(self) -> None:
        selected_values: list[object] = []
        total_values: list[object] = []
        for index in range(self.values_list.count()):
            item = self.values_list.item(index)
            value = item.data(Qt.ItemDataRole.UserRole)
            total_values.append(value)
            if item.checkState() == Qt.CheckState.Checked:
                selected_values.append(value)
        self._explorer.apply_distinct_filter(
            self._column_name,
            tuple(selected_values),
            tuple(total_values),
            complete_domain=self._value_domain_complete,
        )
        self.close()

    def _queue_search(self) -> None:
        self._search_timer.start()

    def _dispatch_search(self) -> None:
        search_text = self.search_input.text().strip()
        self._search_token += 1
        self._explorer.request_filter_values(self._column_name, search_text, self._search_token)

    def _sort_should_append(self) -> bool:
        primary_sort_column = self._explorer.primary_sort_column()
        return primary_sort_column is not None and primary_sort_column != self._column_name

    def _sort_direction_for_column(self) -> bool | None:
        return self._explorer.sort_direction_for_column(self._column_name)

    def _refresh_sort_button_state(self) -> None:
        if self._sort_ascending_button is None or self._sort_descending_button is None:
            return
        append_mode = self._sort_should_append()
        action_prefix = "Then sort" if append_mode else "Sort"
        icon_fill = self._icon_fill_color()
        active_direction = self._sort_direction_for_column()
        for button, icon_name, label in (
            (self._sort_ascending_button, "sort-ascending", "ascending"),
            (self._sort_descending_button, "sort-descending", "descending"),
        ):
            button.setIcon(
                QIcon(
                    render_svg_icon_pixmap(
                        icon_name=icon_name,
                        size=24,
                        device_pixel_ratio=max(1.0, self.devicePixelRatioF()),
                        default_fill_color=icon_fill,
                        colorize_stroke=True,
                    )
                )
            )
            is_active = active_direction is not None and active_direction == (label == "descending")
            button.setProperty("sortActive", is_active)
            button.setToolTip(f"Clear {label} sort" if is_active else f"{action_prefix} {label}")
            button.setAccessibleName(f"Clear {label} sort" if is_active else f"{action_prefix} {label}")
            style = button.style()
            style.unpolish(button)
            style.polish(button)
            button.update()

    def _apply_sort(self, *, descending: bool) -> None:
        active_direction = self._sort_direction_for_column()
        if active_direction == descending:
            self._explorer.remove_column_sort(self._column_name)
        else:
            self._explorer.apply_column_sort(
                self._column_name,
                descending=descending,
                append=self._sort_should_append(),
            )
        self.close()


class _ParquetExplorerWidget(QWidget):
    """Lazy parquet preview with Excel-style header filter popups."""

    summary_changed = Signal(str)

    def __init__(
        self,
        output_path: Path,
        *,
        timing_log_path: Path | None = None,
        external_preview_controls: tuple[QComboBox, QSpinBox] | None = None,
    ) -> None:
        super().__init__()
        self.setObjectName("outputPreviewExplorer")
        self._output_path = Path(output_path)
        self._timing_log_path = timing_log_path
        self._lazy_frame = pl.scan_parquet(self._output_path)
        self._schema = None
        self._current_preview = pl.DataFrame()
        self._active_value_filters: dict[str, tuple[object, ...]] = {}
        self._filter_popup: _ParquetFilterPopup | None = None
        self._distinct_loaders: list[_DistinctValueLoader] = []
        self._preview_loader: _ParquetPreviewLoader | None = None
        self._pending_preview_refresh = False
        self._preview_mode = _PREVIEW_MODE_TOP
        self._active_preview_request_id: str | None = None
        self._active_distinct_requests: dict[tuple[str, int], str] = {}
        self._open_filter_column_index: int | None = None
        self._owns_preview_controls = external_preview_controls is None
        self._table_render_generation = 0
        self._sort_columns: list[tuple[str, bool]] = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        if external_preview_controls is None:
            controls = QHBoxLayout()
            controls.setContentsMargins(0, 0, 0, 0)
            controls.setSpacing(8)
            controls.addStretch(1)
            self.preview_mode_combo = QComboBox()
            self.preview_mode_combo.setObjectName("outputPreviewModeCombo")
            self.preview_mode_combo.setFixedHeight(22)
            controls.addWidget(self.preview_mode_combo)
            self.preview_limit_spin = QSpinBox()
            self.preview_limit_spin.setObjectName("outputPreviewLimitSpin")
            self.preview_limit_spin.setFixedHeight(22)
            controls.addWidget(self.preview_limit_spin)
            layout.addLayout(controls)
        else:
            self.preview_mode_combo, self.preview_limit_spin = external_preview_controls

        self._configure_preview_controls()

        export_row = QHBoxLayout()
        export_row.setContentsMargins(0, 0, 0, 0)
        export_row.setSpacing(8)
        self.status_label = QLabel("Loading preview…")
        self.status_label.setObjectName("sectionMeta")
        self.status_label.setWordWrap(True)
        export_row.addWidget(self.status_label, 1, Qt.AlignmentFlag.AlignVCenter)
        export_row.addStretch(1)
        self.export_excel_button = QPushButton("Export Excel")
        self.export_excel_button.setObjectName("outputPreviewExportExcelButton")
        self.export_excel_button.setFixedHeight(22)
        self.export_excel_button.setToolTip("Export the visible preview rows to an Excel workbook.")
        self.export_excel_button.setEnabled(False)
        self.export_excel_button.clicked.connect(self._export_current_preview)
        export_row.addWidget(self.export_excel_button)
        layout.addLayout(export_row)

        self.table = _build_dataframe_table(pl.DataFrame())
        self.table.horizontalHeader().setSectionsClickable(True)
        self.table.horizontalHeader().viewport().installEventFilter(self)
        layout.addWidget(self.table, 1)
        self._refresh_preview()

    def _configure_preview_controls(self) -> None:
        self.preview_mode_combo.blockSignals(True)
        self.preview_mode_combo.clear()
        self.preview_mode_combo.addItem("Top N", _PREVIEW_MODE_TOP)
        self.preview_mode_combo.addItem("Bottom N", _PREVIEW_MODE_BOTTOM)
        self.preview_mode_combo.addItem("Sample", _PREVIEW_MODE_SAMPLE)
        self.preview_mode_combo.setCurrentIndex(0)
        self.preview_mode_combo.blockSignals(False)
        self.preview_mode_combo.currentIndexChanged.connect(self._handle_preview_controls_changed)
        self.preview_mode_combo.setVisible(True)
        self.preview_mode_combo.setEnabled(True)

        self.preview_limit_spin.blockSignals(True)
        self.preview_limit_spin.setRange(_PREVIEW_ROW_LIMIT_MIN, _PREVIEW_ROW_LIMIT_MAX)
        self.preview_limit_spin.setSingleStep(25)
        self.preview_limit_spin.setValue(_PREVIEW_ROW_LIMIT)
        self.preview_limit_spin.setKeyboardTracking(False)
        self.preview_limit_spin.blockSignals(False)
        self.preview_limit_spin.valueChanged.connect(self._handle_preview_controls_changed)
        self.preview_limit_spin.setVisible(True)
        self.preview_limit_spin.setEnabled(True)

    def shutdown_background_work(self) -> None:
        if self._filter_popup is not None:
            self._filter_popup.close()
            self._filter_popup = None
            self._open_filter_column_index = None
        try:
            self.preview_mode_combo.currentIndexChanged.disconnect(self._handle_preview_controls_changed)
        except (RuntimeError, TypeError):
            pass
        try:
            self.preview_limit_spin.valueChanged.disconnect(self._handle_preview_controls_changed)
        except (RuntimeError, TypeError):
            pass
        if not self._owns_preview_controls:
            self.preview_mode_combo.setVisible(False)
            self.preview_limit_spin.setVisible(False)
        try:
            self.export_excel_button.clicked.disconnect(self._export_current_preview)
        except (RuntimeError, TypeError):
            pass
        preview_loader = self._preview_loader
        self._preview_loader = None
        if preview_loader is not None:
            preview_loader.wait(5000)
        distinct_loaders = list(self._distinct_loaders)
        self._distinct_loaders.clear()
        for loader in distinct_loaders:
            loader.wait(5000)

    def selected_filter_values(self, column_name: str) -> tuple[object, ...] | None:
        return self._active_value_filters.get(column_name)

    def apply_distinct_filter(
        self,
        column_name: str,
        selected_values: tuple[object, ...],
        all_values: tuple[object, ...],
        *,
        complete_domain: bool,
    ) -> None:
        if not selected_values or (complete_domain and len(selected_values) == len(all_values)):
            self._active_value_filters.pop(column_name, None)
        else:
            self._active_value_filters[column_name] = selected_values
        self._refresh_preview()

    def request_filter_values(self, column_name: str, search_text: str, token: int) -> None:
        if self._filter_popup is None or self._filter_popup._column_name != column_name:
            return
        self._filter_popup.set_values(
            self._merge_selected_values(column_name, self._preview_distinct_values(column_name)),
            loading=True,
            complete_domain=False,
        )
        request_id = new_request_id("gui-filter")
        self._active_distinct_requests[(column_name, token)] = request_id
        append_timing_line(
            self._timing_log_path,
            scope="gui.debug",
            event="distinct_search",
            phase="start",
            fields={
                "request_id": request_id,
                "artifact_path": self._output_path,
                "column_name": column_name,
                "search_text": search_text,
                "active_filter_count": len(self._active_value_filters),
            },
        )
        loader = _DistinctValueLoader(
            self._output_path,
            column_name,
            token=token,
            active_value_filters=self._active_value_filters,
            search_text=search_text,
        )
        loader.values_loaded.connect(self._handle_distinct_values_loaded)
        loader.load_failed.connect(self._handle_distinct_values_failed)
        loader.finished.connect(lambda: self._drop_distinct_loader(loader))
        self._distinct_loaders.append(loader)
        loader.start()

    def _refresh_preview(self) -> None:
        if self._preview_loader is not None:
            self._pending_preview_refresh = True
            return
        request_id = new_request_id("gui-preview")
        self._active_preview_request_id = request_id
        append_timing_line(
            self._timing_log_path,
            scope="gui.debug",
            event="preview_load",
            phase="start",
            fields={
                "request_id": request_id,
                "artifact_path": self._output_path,
                "preview_mode": self._preview_mode,
                "row_limit": self.preview_limit_spin.value(),
                "active_filter_count": len(self._active_value_filters),
                "sort_column_count": len(self._sort_columns),
            },
        )
        self.status_label.setText("Loading preview…")
        self.status_label.setVisible(True)
        self.table.setEnabled(False)
        self.export_excel_button.setEnabled(False)
        loader = _ParquetPreviewLoader(
            self._output_path,
            active_value_filters=self._active_value_filters,
            sort_columns=tuple(self._sort_columns),
            preview_mode=self._preview_mode,
            preview_row_limit=self.preview_limit_spin.value(),
        )
        loader.preview_loaded.connect(self._handle_preview_loaded)
        loader.load_failed.connect(self._handle_preview_failed)
        loader.finished.connect(self._handle_preview_finished)
        self._preview_loader = loader
        loader.start()

    def _open_filter_popup_for_index(self, index: int) -> None:
        if self._schema is None:
            return
        column_names = self._schema.names()
        if index < 0 or index >= len(column_names):
            return
        column_name = column_names[index]
        if self._close_popup_for_column(index):
            return
        append_timing_line(
            self._timing_log_path,
            scope="gui.debug",
            event="open_filter_popup",
            phase="start",
            fields={
                "artifact_path": self._output_path,
                "column_name": column_name,
                "preview_row_count": self._current_preview.height,
                "active_filter_count": len(self._active_value_filters),
            },
        )
        if self._filter_popup is not None:
            self._filter_popup.close()
        popup = _ParquetFilterPopup(
            self,
            column_name=column_name,
            dtype=self._schema[column_name],
            values=self._preview_distinct_values(column_name),
        )
        self._filter_popup = popup
        self._open_filter_column_index = index
        popup.move(self._popup_position_for_column(index, popup))
        popup.show()
        popup.raise_()
        popup.activateWindow()
        self.request_filter_values(column_name, "", popup._search_token)
        append_timing_line(
            self._timing_log_path,
            scope="gui.debug",
            event="open_filter_popup",
            phase="end",
            fields={
                "artifact_path": self._output_path,
                "column_name": column_name,
                "value_count": popup.values_list.count(),
            },
        )

    def _popup_position_for_column(self, index: int, popup: QWidget) -> QPoint:
        header = self.table.horizontalHeader()
        viewport = header.viewport()
        section_left = header.sectionViewportPosition(index)
        section_bottom = viewport.mapToGlobal(QPoint(section_left, viewport.height()))
        return QPoint(section_bottom.x(), section_bottom.y() + 2)

    def _handle_distinct_values_loaded(self, column_name: str, token: int, values: object, truncated: bool) -> None:
        loaded_values = self._merge_selected_values(column_name, list(values))
        popup_search_text = ""
        if (
            self._filter_popup is not None
            and self._filter_popup._column_name == column_name
            and self._filter_popup._search_token == token
        ):
            popup_search_text = self._filter_popup.search_input.text().strip()
        complete_domain = not truncated and not popup_search_text
        note = "Showing first 500 matching values." if truncated else ""
        request_id = self._active_distinct_requests.pop((column_name, token), None)
        append_timing_line(
            self._timing_log_path,
            scope="gui.debug",
            event="distinct_search",
            phase="end",
            fields={
                "request_id": request_id,
                "artifact_path": self._output_path,
                "column_name": column_name,
                "match_count": len(loaded_values),
                "truncated": truncated,
            },
        )
        if (
            self._filter_popup is not None
            and self._filter_popup._column_name == column_name
            and self._filter_popup._search_token == token
        ):
            self._filter_popup.set_values(loaded_values, note=note, complete_domain=complete_domain)

    def _handle_distinct_values_failed(self, column_name: str, token: int, message: str) -> None:
        request_id = self._active_distinct_requests.pop((column_name, token), None)
        append_timing_line(
            self._timing_log_path,
            scope="gui.debug",
            event="distinct_search",
            phase="error",
            fields={
                "request_id": request_id,
                "artifact_path": self._output_path,
                "column_name": column_name,
                "error": message,
            },
        )
        if (
            self._filter_popup is not None
            and self._filter_popup._column_name == column_name
            and self._filter_popup._search_token == token
        ):
            self._filter_popup.set_error(f"Unable to load values: {message}")

    def _drop_distinct_loader(self, loader: _DistinctValueLoader) -> None:
        if loader in self._distinct_loaders:
            self._distinct_loaders.remove(loader)

    def _handle_preview_loaded(self, schema: object, preview: object, summary: str) -> None:
        request_id = self._active_preview_request_id
        self._active_preview_request_id = None
        self._schema = schema
        self._current_preview = preview
        self._start_table_render(preview, filtered_columns=set(self._active_value_filters), summary=summary)
        append_timing_line(
            self._timing_log_path,
            scope="gui.debug",
            event="preview_load",
            phase="end",
            fields={
                "request_id": request_id,
                "artifact_path": self._output_path,
                "preview_row_count": preview.height,
                "preview_column_count": len(preview.columns),
                "summary": summary,
            },
        )
        if self._filter_popup is not None and self._filter_popup.search_input.text().strip() == "":
            self.request_filter_values(self._filter_popup._column_name, "", self._filter_popup._search_token)

    def _handle_preview_failed(self, message: str) -> None:
        request_id = self._active_preview_request_id
        self._active_preview_request_id = None
        self.table.setEnabled(False)
        self.export_excel_button.setEnabled(False)
        self.status_label.setText(f"Unable to load preview: {message}")
        self.status_label.setVisible(True)
        append_timing_line(
            self._timing_log_path,
            scope="gui.debug",
            event="preview_load",
            phase="error",
            fields={
                "request_id": request_id,
                "artifact_path": self._output_path,
                "error": message,
            },
        )
        self.summary_changed.emit("Unable to load preview")

    def _handle_preview_finished(self) -> None:
        self._preview_loader = None
        if self._pending_preview_refresh:
            self._pending_preview_refresh = False
            self._refresh_preview()

    def _handle_preview_controls_changed(self) -> None:
        self._preview_mode = str(self.preview_mode_combo.currentData())
        self._refresh_preview()

    def apply_column_sort(self, column_name: str, *, descending: bool, append: bool) -> None:
        existing_rank = self.sort_rank_for_column(column_name)
        updated_sorts = list(self._sort_columns)
        if existing_rank is not None:
            updated_sorts[existing_rank - 1] = (column_name, descending)
        elif append:
            updated_sorts.append((column_name, descending))
        else:
            updated_sorts = [(column_name, descending)]
        self._sort_columns = updated_sorts
        self._refresh_preview()

    def clear_column_sorts(self) -> None:
        if not self._sort_columns:
            return
        self._sort_columns = []
        self._refresh_preview()

    def sort_rank_for_column(self, column_name: str) -> int | None:
        for index, (active_name, _descending) in enumerate(self._sort_columns, start=1):
            if active_name == column_name:
                return index
        return None

    def sort_direction_for_column(self, column_name: str) -> bool | None:
        for active_name, active_descending in self._sort_columns:
            if active_name == column_name:
                return active_descending
        return None

    def remove_column_sort(self, column_name: str) -> None:
        updated_sorts = [
            (active_name, active_descending)
            for active_name, active_descending in self._sort_columns
            if active_name != column_name
        ]
        if len(updated_sorts) == len(self._sort_columns):
            return
        self._sort_columns = updated_sorts
        self._refresh_preview()

    def primary_sort_column(self) -> str | None:
        if not self._sort_columns:
            return None
        return self._sort_columns[0][0]

    def eventFilter(self, watched: object, event: object) -> bool:
        header = self.table.horizontalHeader()
        if watched is header.viewport() and isinstance(event, QEvent):
            if event.type() == QEvent.Type.MouseButtonPress:
                point = _event_position(event)
                if point is None:
                    return super().eventFilter(watched, event)
                if _header_point_is_resize_handle(header, point):
                    return super().eventFilter(watched, event)
                section_index = header.logicalIndexAt(point)
                if section_index >= 0:
                    if self._close_popup_for_column(section_index):
                        return True
                    self._open_filter_popup_for_index(section_index)
                    return True
        return super().eventFilter(watched, event)

    def _close_popup_for_column(self, index: int) -> bool:
        if (
            self._filter_popup is not None
            and self._filter_popup.isVisible()
            and self._open_filter_column_index == index
        ):
            self._filter_popup.close()
            self._filter_popup = None
            self._open_filter_column_index = None
            return True
        return False

    def _preview_distinct_values(self, column_name: str) -> list[tuple[str, object]]:
        if column_name not in self._current_preview.columns:
            return []
        series = self._current_preview.get_column(column_name).unique(maintain_order=True).head(_PREVIEW_DISTINCT_VALUE_LIMIT)
        values: list[tuple[str, object]] = []
        for value in series.to_list():
            if value is None:
                values.append(("(blank)", _NULL_FILTER_VALUE))
            else:
                values.append((str(value), value))
        return values

    def _start_table_render(self, preview: pl.DataFrame, *, filtered_columns: set[str], summary: str) -> None:
        self._table_render_generation += 1
        generation = self._table_render_generation
        _prepare_dataframe_table(
            self.table,
            preview,
            filtered_columns=filtered_columns,
            sort_columns=self._sort_columns,
        )
        self.table.setEnabled(False)
        self.status_label.setText("Rendering preview...")
        self.status_label.setVisible(True)
        self.summary_changed.emit(summary)
        if preview.height <= _TABLE_RENDER_BATCH_SIZE:
            _populate_dataframe_table_rows(self.table, preview, 0, preview.height)
            self._finish_table_render(generation, preview)
            return
        self._render_table_batch(preview, start_row=0, generation=generation)

    def _render_table_batch(self, preview: pl.DataFrame, *, start_row: int, generation: int) -> None:
        if generation != self._table_render_generation:
            return
        end_row = min(start_row + _TABLE_RENDER_BATCH_SIZE, preview.height)
        _populate_dataframe_table_rows(self.table, preview, start_row, end_row)
        if end_row >= preview.height:
            self._finish_table_render(generation, preview)
            return
        QTimer.singleShot(0, lambda: self._render_table_batch(preview, start_row=end_row, generation=generation))

    def _finish_table_render(self, generation: int, preview: pl.DataFrame) -> None:
        if generation != self._table_render_generation:
            return
        if preview.height <= 250:
            self.table.resizeColumnsToContents()
        self.table.setEnabled(True)
        self.export_excel_button.setEnabled(bool(preview.columns))
        self.status_label.setVisible(False)

    def _merge_selected_values(self, column_name: str, values: list[tuple[str, object]]) -> list[tuple[str, object]]:
        selected_values = self.selected_filter_values(column_name) or ()
        if not selected_values:
            return values
        seen = set()
        merged: list[tuple[str, object]] = []
        for value in selected_values:
            label = "(blank)" if value is _NULL_FILTER_VALUE else str(value)
            merged.append((label, value))
            seen.add(_value_identity(value))
        for label, value in values:
            identity = _value_identity(value)
            if identity in seen:
                continue
            merged.append((label, value))
            seen.add(identity)
        return merged

    def _export_current_preview(self) -> None:
        _export_frame_to_excel(self._current_preview, source_path=self._output_path, parent=self)


class _CopyablePreviewTable(QTableWidget):
    """Table widget with spreadsheet-style copy support for selected cells."""

    def keyPressEvent(self, event) -> None:  # noqa: N802
        if event.matches(QKeySequence.StandardKey.Copy):
            self._copy_selection_to_clipboard()
            event.accept()
            return
        super().keyPressEvent(event)

    def contextMenuEvent(self, event) -> None:  # noqa: N802
        index = self.indexAt(event.pos())
        if index.isValid() and not self.selectionModel().isSelected(index):
            self.clearSelection()
            self.setCurrentCell(index.row(), index.column())
            item = self.item(index.row(), index.column())
            if item is not None:
                item.setSelected(True)
        menu = QMenu(self)
        copy_action = menu.addAction("Copy")
        if not self.selectedIndexes() and self.currentItem() is None:
            copy_action.setEnabled(False)
        chosen = menu.exec(event.globalPos())
        if chosen is copy_action:
            self._copy_selection_to_clipboard()
            event.accept()
            return
        super().contextMenuEvent(event)

    def _copy_selection_to_clipboard(self) -> None:
        indexes = self.selectedIndexes()
        if not indexes:
            item = self.currentItem()
            if item is None:
                return
            QApplication.clipboard().setText(item.text())
            return
        rows = sorted({index.row() for index in indexes})
        columns = sorted({index.column() for index in indexes})
        values_by_position = {(index.row(), index.column()): index.data() for index in indexes}
        lines: list[str] = []
        for row in rows:
            fields: list[str] = []
            for column in columns:
                value = values_by_position.get((row, column), "")
                fields.append("" if value is None else str(value))
            lines.append("\t".join(fields))
        QApplication.clipboard().setText("\n".join(lines))


def populate_output_preview(
    layout: QVBoxLayout,
    output_path: Path,
    preview_spec: ArtifactPreviewSpec | None = None,
    *,
    show_summary: bool = True,
    timing_log_path: Path | None = None,
    external_preview_controls: tuple[QComboBox, QSpinBox] | None = None,
) -> QWidget:
    """Populate one dialog layout with the appropriate artifact preview widgets."""
    preview_spec = preview_spec or classify_artifact_preview(output_path)
    if preview_spec.kind == "parquet":
        return _add_parquet_preview(
            layout,
            output_path,
            preview_spec.label,
            show_summary=show_summary,
            timing_log_path=timing_log_path,
            external_preview_controls=external_preview_controls,
        )
    if preview_spec.kind == "excel":
        return _add_tabular_preview(
            layout,
            pl.read_excel(output_path, sheet_id=1, engine="calamine"),
            preview_spec.label,
            show_summary=show_summary,
            output_path=output_path,
        )
    if preview_spec.kind == "text":
        return _add_text_preview(layout, output_path, preview_spec.label)
    if preview_spec.kind == "pdf":
        return _add_placeholder_preview(
            layout,
            heading=preview_spec.label,
            message=preview_spec.placeholder_message or "PDF artifacts are recognized, but in-app PDF text inspection is not available yet.",
            output_path=output_path,
        )
    return _add_placeholder_preview(
        layout,
        heading=preview_spec.label,
        message=preview_spec.placeholder_message or "This artifact type is not previewable in the UI yet.",
        output_path=output_path,
    )


def build_preview_summary_text(output_path: Path, preview_spec: ArtifactPreviewSpec | None = None) -> str:
    """Return compact preview metadata for one artifact path."""
    preview_spec = preview_spec or classify_artifact_preview(output_path)
    if preview_spec.kind == "parquet":
        frame = pl.read_parquet(output_path)
        return _frame_summary_text(frame)
    if preview_spec.kind == "excel":
        frame = pl.read_excel(output_path, sheet_id=1, engine="calamine")
        return _frame_summary_text(frame)
    if output_path.exists():
        return f"{preview_spec.label}  \u2022  {output_path.stat().st_size:,} bytes"
    return preview_spec.label


def _add_parquet_preview(
    layout: QVBoxLayout,
    output_path: Path,
    heading: str,
    *,
    show_summary: bool,
    timing_log_path: Path | None = None,
    external_preview_controls: tuple[QComboBox, QSpinBox] | None = None,
) -> QWidget:
    if show_summary:
        meta_label = QLabel(f"{heading}  •  Loading preview…")
        meta_label.setObjectName("sectionMeta")
        layout.addWidget(meta_label)
    explorer = _ParquetExplorerWidget(
        output_path,
        timing_log_path=timing_log_path,
        external_preview_controls=external_preview_controls,
    )
    explorer.summary_changed.connect(meta_label.setText) if show_summary else None
    layout.addWidget(explorer, 1)
    return explorer


def _add_tabular_preview(
    layout: QVBoxLayout,
    frame: pl.DataFrame,
    heading: str,
    *,
    show_summary: bool,
    output_path: Path | None = None,
) -> QWidget:
    preview = _ExportableFramePreviewWidget(frame, heading=heading, show_summary=show_summary, source_path=output_path)
    layout.addWidget(preview, 1)
    return preview


class _ExportableFramePreviewWidget(QWidget):
    """Static dataframe preview with spreadsheet export support."""

    def __init__(self, frame: pl.DataFrame, *, heading: str, show_summary: bool, source_path: Path | None = None) -> None:
        super().__init__()
        self._frame = frame
        self._source_path = source_path
        self.setObjectName("outputPreviewFrame")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        if show_summary:
            meta_label = QLabel(f"{heading}  \u2022  {_frame_summary_text(frame)}")
            meta_label.setObjectName("sectionMeta")
            layout.addWidget(meta_label)
        export_row = QHBoxLayout()
        export_row.setContentsMargins(0, 0, 0, 0)
        export_row.setSpacing(8)
        export_row.addStretch(1)
        export_button = QPushButton("Export Excel")
        export_button.setObjectName("outputPreviewExportExcelButton")
        export_button.setFixedHeight(22)
        export_button.setToolTip("Export the visible preview rows to an Excel workbook.")
        export_button.setEnabled(bool(frame.columns))
        export_button.clicked.connect(self._export_frame)
        export_row.addWidget(export_button)
        layout.addLayout(export_row)
        table = _build_dataframe_table(frame)
        layout.addWidget(table, 1)

    def _export_frame(self) -> None:
        _export_frame_to_excel(self._frame, source_path=self._source_path, parent=self)


def _build_dataframe_table(frame: pl.DataFrame) -> QTableWidget:
    table = _CopyablePreviewTable()
    table.setObjectName("outputPreviewTable")
    table.setHorizontalHeader(_PreviewHeaderView(Qt.Orientation.Horizontal, table))
    table.setItemDelegate(_PreviewBodyItemDelegate(table))
    table.setAlternatingRowColors(True)
    table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
    table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
    table.setShowGrid(True)
    table.verticalHeader().setVisible(False)
    table.verticalHeader().setDefaultSectionSize(24)
    table.setWordWrap(False)
    table.setTextElideMode(Qt.TextElideMode.ElideRight)
    table.horizontalHeader().setStretchLastSection(False)
    table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
    table.horizontalHeader().setDefaultSectionSize(140)
    if hasattr(table.horizontalHeader(), "setResizeContentsPrecision"):
        table.horizontalHeader().setResizeContentsPrecision(50)
    table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    table.horizontalHeader().setMinimumHeight(30)
    table.horizontalHeader().setFixedHeight(30)
    _prepare_dataframe_table(table, frame, filtered_columns=set())
    _populate_dataframe_table_rows(table, frame, 0, frame.height)
    if frame.height <= 250:
        table.resizeColumnsToContents()
    return table


def _frame_summary_text(frame: pl.DataFrame) -> str:
    return f"{frame.height} row(s)  \u2022  {len(frame.columns)} column(s)  \u2022  Previewing up to {_PREVIEW_ROW_LIMIT} rows"


def _export_frame_to_excel(frame: pl.DataFrame, *, source_path: Path | None, parent: QWidget) -> Path | None:
    default_path = _default_excel_export_path(source_path)
    selected_path, _selected_filter = QFileDialog.getSaveFileName(
        parent,
        "Export Preview to Excel",
        str(default_path),
        "Excel Workbook (*.xlsx)",
    )
    if not selected_path:
        return None
    target_path = _with_excel_suffix(Path(selected_path))
    try:
        write_excel_atomic(frame, target_path, worksheet="Preview")
    except Exception as exc:  # pragma: no cover - defensive UI fallback
        QMessageBox.critical(parent, "Export Failed", f"Unable to export preview: {exc}")
        return None
    QMessageBox.information(parent, "Export Complete", f"Exported {frame.height} row(s) to {target_path}.")
    return target_path


def _default_excel_export_path(source_path: Path | None) -> Path:
    if source_path is None:
        return Path("preview.xlsx")
    return source_path.with_name(f"{source_path.stem}_preview.xlsx")


def _with_excel_suffix(path: Path) -> Path:
    if path.suffix.lower() == ".xlsx":
        return path
    return path.with_suffix(".xlsx")


def _prepare_dataframe_table(
    table: QTableWidget,
    frame: pl.DataFrame,
    *,
    filtered_columns: set[str] | None = None,
    sort_columns: list[tuple[str, bool]] | tuple[tuple[str, bool], ...] | None = None,
) -> None:
    preview = frame
    filtered_columns = filtered_columns or set()
    sort_columns = sort_columns or ()
    sort_markers = {
        column_name: (index + 1, descending)
        for index, (column_name, descending) in enumerate(sort_columns)
    }
    table.clearContents()
    table.setColumnCount(len(preview.columns))
    table.setHorizontalHeaderLabels([column_name for column_name in preview.columns])
    header = table.horizontalHeader()
    if isinstance(header, _PreviewHeaderView):
        header.set_preview_metadata(
            [
                {
                    "title": column_name,
                    "dtype": str(preview.schema[column_name]),
                    "filtered": column_name in filtered_columns,
                    "sort_marker": sort_markers.get(column_name),
                }
                for column_name in preview.columns
            ]
        )
    table.setRowCount(preview.height)


def _populate_dataframe_table_rows(table: QTableWidget, frame: pl.DataFrame, start_row: int, end_row: int) -> None:
    preview = frame
    for row_index in range(start_row, end_row):
        for column_index, column_name in enumerate(preview.columns):
            item = QTableWidgetItem(_cell_text(preview[row_index, column_name]))
            item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            table.setItem(row_index, column_index, item)


def _header_text(
    column_name: str,
    dtype: pl.DataType,
    *,
    filtered: bool,
    sort_marker: tuple[int, bool] | None = None,
) -> str:
    marker = "* " if filtered else ""
    sort_text = ""
    if sort_marker is not None:
        sort_rank, descending = sort_marker
        sort_text = f" {sort_rank}{'↓' if descending else '↑'}"
    return f"{marker}{column_name}{sort_text} \u25be\n{dtype}"


def _build_distinct_value_filter_expression(
    column_name: str,
    selected_values: tuple[object, ...],
    *,
    dtype: pl.DataType | None = None,
):
    if not selected_values:
        return None
    column = pl.col(column_name)
    include_null = any(value is _NULL_FILTER_VALUE for value in selected_values)
    concrete_values = [value for value in selected_values if value is not _NULL_FILTER_VALUE]
    expression = None
    if concrete_values:
        values = concrete_values if dtype is None else pl.Series(concrete_values, dtype=dtype).implode()
        expression = column.is_in(values)
    if include_null:
        null_expression = column.is_null()
        expression = null_expression if expression is None else (expression | null_expression)
    return expression


def _event_position(event: object) -> QPoint | None:
    position = getattr(event, "position", None)
    if callable(position):
        return position().toPoint()
    pos = getattr(event, "pos", lambda: None)()
    return pos if isinstance(pos, QPoint) else None


def _header_point_is_resize_handle(header: QHeaderView, point: QPoint) -> bool:
    cursor_shape = header.viewport().cursor().shape()
    if cursor_shape == Qt.CursorShape.SplitHCursor:
        return True
    logical_index = header.logicalIndexAt(point)
    if logical_index < 0:
        return False
    resize_margin = max(4, header.style().pixelMetric(QStyle.PixelMetric.PM_HeaderGripMargin, None, header))
    left = header.sectionViewportPosition(logical_index)
    right = left + header.sectionSize(logical_index)
    if point.x() >= right - resize_margin:
        return True
    return logical_index > 0 and point.x() <= left + resize_margin


def _value_identity(value: object) -> tuple[str, object]:
    if value is _NULL_FILTER_VALUE:
        return ("null", "__blank__")
    return (type(value).__name__, value)


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _add_text_preview(layout: QVBoxLayout, output_path: Path, heading: str) -> QWidget:
    meta_label = QLabel(heading)
    meta_label.setObjectName("sectionMeta")
    layout.addWidget(meta_label)
    body = QTextEdit()
    body.setObjectName("outputPreviewText")
    body.setReadOnly(True)
    body.setPlainText(output_path.read_text(encoding="utf-8"))
    layout.addWidget(body, 1)
    return body


def _add_placeholder_preview(layout: QVBoxLayout, *, heading: str, message: str, output_path: Path) -> QWidget:
    size_bytes = output_path.stat().st_size if output_path.exists() else 0
    meta_label = QLabel(f"{heading}  {size_bytes:,} bytes")
    meta_label.setObjectName("sectionMeta")
    layout.addWidget(meta_label)
    body = QTextEdit()
    body.setObjectName("outputPreviewText")
    body.setReadOnly(True)
    body.setPlainText(message)
    layout.addWidget(body, 1)
    return body


__all__ = [
    "ArtifactPreviewSpec",
    "build_preview_summary_text",
    "classify_artifact_preview",
    "populate_output_preview",
]
