"""Artifact classification and preview rendering helpers."""

from __future__ import annotations

import glob as glob_module
from datetime import date
from pathlib import Path
import pyarrow.parquet as pq
import polars as pl
from PySide6.QtCore import QDate, QEvent, QPoint, QRect, QSize, QThread, QTime, QTimer, Qt, Signal
from PySide6.QtGui import QColor, QFont, QFontMetrics, QIcon, QKeySequence, QPainter
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDateEdit,
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
    QSizePolicy,
    QSpinBox,
    QStyle,
    QStyleOptionHeader,
    QStyleOptionViewItem,
    QStyledItemDelegate,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QTimeEdit,
    QVBoxLayout,
    QWidget,
)

from data_engine.helpers import write_excel_atomic
from data_engine.platform.instrumentation import append_timing_line, new_request_id
from data_engine.ui.gui.rendering.icons import render_svg_icon_pixmap
from data_engine.ui.gui.rendering.preview_filters import (
    BooleanFilterValue,
    ColumnFilter,
    NULL_FILTER_VALUE as _NULL_FILTER_VALUE,
    PreviewSortState,
    DateFilterRange,
    NumberFilterRange,
    TextFilterCondition,
    TextFilterOperation,
    TimeFilterRange,
    build_column_filter_expression,
    build_distinct_value_filter_expression,
    column_filter_component,
    merge_selected_values,
    should_clear_distinct_filter,
    value_identity,
)
from data_engine.views import ArtifactPreviewSpec, classify_artifact_preview

_PREVIEW_ROW_LIMIT = 200
_PREVIEW_ROW_LIMIT_MIN = 1
_PREVIEW_ROW_LIMIT_MAX = 500_000
_TABLE_RENDER_BATCH_SIZE = 500
_PREVIEW_DISTINCT_VALUE_LIMIT = 500
_PREVIEW_MODE_TOP = "top"


def _dtype_supports_text_filter(dtype: pl.DataType) -> bool:
    base_type = dtype.base_type()
    return base_type in {pl.String, pl.Categorical, pl.Enum}


def _dtype_supports_date_filter(dtype: pl.DataType) -> bool:
    base_type = dtype.base_type()
    return base_type in {pl.Date, pl.Datetime}


def _dtype_supports_time_filter(dtype: pl.DataType) -> bool:
    return dtype.base_type() == pl.Time


def _dtype_supports_number_filter(dtype: pl.DataType) -> bool:
    base_type = dtype.base_type()
    return base_type in {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.Int128,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
        pl.Decimal,
    }


def _dtype_supports_boolean_filter(dtype: pl.DataType) -> bool:
    return dtype.base_type() == pl.Boolean


def _qdate_from_iso(value: str) -> QDate:
    parsed = date.fromisoformat(value)
    return QDate(parsed.year, parsed.month, parsed.day)


def _qtime_from_iso(value: str) -> QTime:
    return QTime.fromString(value, "HH:mm:ss")


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
        active_filters: dict[str, ColumnFilter],
        sort_columns: tuple[tuple[str, bool], ...],
        preview_row_limit: int,
    ) -> None:
        super().__init__()
        self._output_path = Path(output_path)
        self._active_filters = dict(active_filters)
        self._sort_columns = tuple((str(column_name), bool(descending)) for column_name, descending in sort_columns)
        self._preview_row_limit = max(_PREVIEW_ROW_LIMIT_MIN, min(preview_row_limit, _PREVIEW_ROW_LIMIT_MAX))

    def run(self) -> None:
        try:
            lazy_frame = pl.scan_parquet(self._output_path)
            schema = lazy_frame.collect_schema()
            query = lazy_frame
            filter_expressions = []
            for column_name, column_filter in self._active_filters.items():
                expression = build_column_filter_expression(column_filter, dtype=schema[column_name])
                if expression is not None:
                    filter_expressions.append(expression)
                    query = query.filter(expression)
            row_count = None if self._active_filters else _parquet_row_count_from_metadata(self._output_path)
            if row_count is None:
                row_count = (
                    query.select(pl.len().alias("__row_count__"))
                    .collect()
                    .get_column("__row_count__")
                    .item()
                )
            if filter_expressions or self._sort_columns:
                preview = _top_parquet_preview_by_row_index(
                    lazy_frame,
                    self._output_path,
                    filter_expressions=tuple(filter_expressions),
                    sort_columns=self._sort_columns,
                    row_limit=self._preview_row_limit,
                    schema_names=tuple(schema.names()),
                )
            else:
                preview = query.head(self._preview_row_limit).collect()
            preview_label = f"showing top {self._preview_row_limit} rows"
            summary = f"{row_count} rows - {len(schema.names())} columns - {preview_label}"
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
        active_filters: dict[str, ColumnFilter],
        value_filter: ColumnFilter | None,
        sort_descending: bool | None,
        search_text: str,
        value_limit: int = 500,
    ) -> None:
        super().__init__()
        self._output_path = Path(output_path)
        self._column_name = column_name
        self._token = token
        self._active_filters = dict(active_filters)
        self._value_filter = value_filter
        self._sort_descending = sort_descending
        self._search_text = search_text.strip().lower()
        self._value_limit = max(1, value_limit)

    def run(self) -> None:
        try:
            query = pl.scan_parquet(self._output_path)
            schema = query.collect_schema()
            for active_name, column_filter in self._active_filters.items():
                if active_name == self._column_name:
                    continue
                expression = build_column_filter_expression(column_filter, dtype=schema[active_name])
                if expression is not None:
                    query = query.filter(expression)
            if self._value_filter is not None:
                expression = build_column_filter_expression(self._value_filter, dtype=schema[self._column_name])
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
            value_expression = column.unique(maintain_order=True)
            if self._sort_descending is not None:
                value_expression = value_expression.sort(descending=self._sort_descending)
            series = query.select(value_expression.head(self._value_limit + 1).alias(self._column_name)).collect().get_column(self._column_name)
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
        self._explicitly_unchecked_value_identities: set[tuple[str, object]] = set()
        self._use_active_selected_values = True
        self._populating_values = False
        self._is_text_filter_column = _dtype_supports_text_filter(dtype)
        self._is_date_filter_column = _dtype_supports_date_filter(dtype)
        self._is_time_filter_column = _dtype_supports_time_filter(dtype)
        self._is_number_filter_column = _dtype_supports_number_filter(dtype)
        self._is_boolean_filter_column = _dtype_supports_boolean_filter(dtype)
        self._text_filter_rows: list[tuple[QFrame, QComboBox, QLineEdit]] = []
        self._text_filter_layout: QVBoxLayout | None = None
        self._date_filter_rows: list[tuple[QFrame, QDateEdit, QDateEdit]] = []
        self._date_filter_layout: QVBoxLayout | None = None
        self._time_filter_rows: list[tuple[QFrame, QTimeEdit, QTimeEdit]] = []
        self._time_filter_layout: QVBoxLayout | None = None
        self._number_filter_rows: list[tuple[QFrame, QLineEdit, QLineEdit]] = []
        self._number_filter_layout: QVBoxLayout | None = None
        self._boolean_filter_combo: QComboBox | None = None
        self.search_input: QLineEdit | None = None
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating, True)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        title_row = QHBoxLayout()
        title_row.setContentsMargins(0, 0, 0, 0)
        title_row.setSpacing(8)
        title = QLabel(column_name, self)
        title.setObjectName("sectionTitle")
        title_row.addWidget(title)
        title_row.addStretch(1)
        self.status_label = QLabel("", self)
        self.status_label.setObjectName("outputPreviewFilterStatusLabel")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.status_label.setVisible(False)
        title_row.addWidget(self.status_label)
        layout.addLayout(title_row)

        if self._is_text_filter_column:
            layout.addWidget(self._build_text_filter_controls())
        elif self._is_date_filter_column:
            layout.addWidget(self._build_date_filter_controls())
        elif self._is_time_filter_column:
            layout.addWidget(self._build_time_filter_controls())
        elif self._is_boolean_filter_column:
            layout.addWidget(self._build_boolean_filter_controls())
        elif self._is_number_filter_column:
            layout.addWidget(self._build_number_filter_controls())
        else:
            self.search_input = QLineEdit(self)
            self.search_input.setObjectName("outputPreviewPopupSearch")
            self.search_input.setPlaceholderText("Search values")
            self.search_input.setClearButtonEnabled(True)
            layout.addWidget(self.search_input)

        value_panel = QFrame(self)
        value_panel_layout = QVBoxLayout(value_panel)
        value_panel_layout.setContentsMargins(0, 0, 0, 0)
        value_panel_layout.setSpacing(0)
        layout.addWidget(value_panel, 1)

        controls_frame = QFrame(value_panel)
        controls_frame.setObjectName("outputPreviewSortControlBar")
        value_panel_layout.addWidget(controls_frame)

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

        self.values_list = QListWidget(value_panel)
        self.values_list.setObjectName("outputPreviewPopupList")
        self.values_list.setMinimumWidth(220)
        self.values_list.setMinimumHeight(240)
        self.values_list.itemChanged.connect(self._handle_value_item_changed)
        value_panel_layout.addWidget(self.values_list, 1)

        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.setSpacing(6)
        clear_button = QPushButton("Clear", self)
        clear_button.setObjectName("filterPopupActionButton")
        clear_button.clicked.connect(self._clear_column_state)
        footer.addWidget(clear_button)
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
        if self.search_input is not None:
            self.search_input.textChanged.connect(self._queue_search)
        self.set_values(values)

    def _build_text_filter_controls(self) -> QFrame:
        text_section = QFrame(self)
        text_section.setObjectName("outputPreviewTextFilterSection")
        self._text_filter_layout = QVBoxLayout(text_section)
        self._text_filter_layout.setContentsMargins(0, 0, 0, 0)
        self._text_filter_layout.setSpacing(6)

        active_conditions = self._active_text_filter_conditions()
        if not active_conditions:
            active_conditions = (("contains", ""),)
        for operation, value in active_conditions:
            self._add_text_filter_row(operation=operation, value=value)
        return text_section

    def _add_text_filter_row(self, *, operation: TextFilterOperation = "contains", value: str = "") -> None:
        if self._text_filter_layout is None:
            return
        row_frame = QFrame(self)
        row_frame.setObjectName("outputPreviewControlBar")
        row_layout = QHBoxLayout(row_frame)
        row_layout.setContentsMargins(6, 6, 6, 6)
        row_layout.setSpacing(6)

        combo = QComboBox(row_frame)
        combo.setObjectName("outputPreviewTextFilterCombo")
        combo.addItem("Contains", "contains")
        combo.addItem("Does Not Contain", "not_contains")
        combo.addItem("Equals", "equals")
        combo.addItem("Does Not Equal", "not_equals")
        combo.addItem("Begins With", "begins_with")
        combo.addItem("Ends With", "ends_with")
        operation_index = combo.findData(operation)
        if operation_index >= 0:
            combo.setCurrentIndex(operation_index)
        row_layout.addWidget(combo)

        line_edit = QLineEdit(row_frame)
        line_edit.setObjectName("outputPreviewTextFilterInput")
        line_edit.setPlaceholderText("Text filter")
        line_edit.setClearButtonEnabled(True)
        line_edit.setFixedHeight(combo.sizeHint().height())
        line_edit.setText(value)
        row_layout.addWidget(line_edit, 1)

        add_button = QPushButton("+", row_frame)
        add_button.setObjectName("outputPreviewTextFilterAddButton")
        add_button.setFixedSize(16, combo.sizeHint().height())
        add_button.setToolTip("Add text condition")
        add_button.clicked.connect(self._add_empty_text_filter_row)
        row_layout.addWidget(add_button)

        if self._text_filter_rows:
            remove_button = QPushButton("-", row_frame)
            remove_button.setObjectName("outputPreviewTextFilterRemoveButton")
            remove_button.setFixedSize(16, combo.sizeHint().height())
            remove_button.setToolTip("Remove text condition")
            remove_button.clicked.connect(lambda: self._remove_text_filter_row(row_frame))
            row_layout.addWidget(remove_button)

        combo.currentIndexChanged.connect(lambda _index: self._queue_condition_search())
        line_edit.textChanged.connect(lambda _text: self._queue_condition_search())
        self._text_filter_rows.append((row_frame, combo, line_edit))
        self._text_filter_layout.addWidget(row_frame)

    def _add_empty_text_filter_row(self) -> None:
        self._add_text_filter_row()
        self._queue_condition_search()

    def _remove_text_filter_row(self, row_frame: QFrame) -> None:
        if len(self._text_filter_rows) <= 1:
            return
        remaining_rows: list[tuple[QFrame, QComboBox, QLineEdit]] = []
        for frame, combo, line_edit in self._text_filter_rows:
            if frame is row_frame:
                if self._text_filter_layout is not None:
                    self._text_filter_layout.removeWidget(frame)
                frame.deleteLater()
            else:
                remaining_rows.append((frame, combo, line_edit))
        self._text_filter_rows = remaining_rows
        self._queue_condition_search()

    def _active_text_filter_conditions(self) -> tuple[TextFilterCondition, ...]:
        active_filter = column_filter_component(self._explorer.active_column_filter(self._column_name), "text")
        if active_filter is None or not active_filter.values:
            return ()
        if active_filter.operation == "all":
            return tuple((str(operation), str(value)) for operation, value in active_filter.values)
        return ((str(active_filter.operation), str(active_filter.values[0])),)

    def _build_date_filter_controls(self) -> QFrame:
        date_section = QFrame(self)
        date_section.setObjectName("outputPreviewDateFilterSection")
        self._date_filter_layout = QVBoxLayout(date_section)
        self._date_filter_layout.setContentsMargins(0, 0, 0, 0)
        self._date_filter_layout.setSpacing(6)

        active_ranges = self._active_date_filter_ranges()
        if not active_ranges:
            today = date.today().isoformat()
            self._add_date_filter_row(start_value=today, end_value=today, active=False)
            return date_section
        for start_value, end_value in active_ranges:
            self._add_date_filter_row(start_value=start_value, end_value=end_value, active=True)
        return date_section

    def _add_date_filter_row(
        self,
        *,
        start_value: str | None = None,
        end_value: str | None = None,
        active: bool = False,
    ) -> None:
        if self._date_filter_layout is None:
            return
        row_frame = QFrame(self)
        row_frame.setObjectName("outputPreviewControlBar")
        row_frame.setProperty("outputPreviewDateRangeActive", active)
        row_layout = QHBoxLayout(row_frame)
        row_layout.setContentsMargins(6, 6, 6, 6)
        row_layout.setSpacing(6)

        from_edit = QDateEdit(row_frame)
        from_edit.setObjectName("outputPreviewDateFilterFromInput")
        from_edit.setCalendarPopup(True)
        self._style_date_filter_calendar(from_edit)
        from_edit.setDisplayFormat("yyyy-MM-dd")
        from_edit.setDate(_qdate_from_iso(start_value or date.today().isoformat()))
        row_layout.addWidget(from_edit, 1)

        to_edit = QDateEdit(row_frame)
        to_edit.setObjectName("outputPreviewDateFilterToInput")
        to_edit.setCalendarPopup(True)
        self._style_date_filter_calendar(to_edit)
        to_edit.setDisplayFormat("yyyy-MM-dd")
        to_edit.setDate(_qdate_from_iso(end_value or start_value or date.today().isoformat()))
        to_edit.setFixedHeight(from_edit.sizeHint().height())
        from_edit.setFixedHeight(to_edit.sizeHint().height())
        row_layout.addWidget(to_edit, 1)

        add_button = QPushButton("+", row_frame)
        add_button.setObjectName("outputPreviewDateFilterAddButton")
        add_button.setFixedSize(16, to_edit.sizeHint().height())
        add_button.setToolTip("Add date range")
        add_button.clicked.connect(self._add_empty_date_filter_row)
        row_layout.addWidget(add_button)

        if self._date_filter_rows:
            remove_button = QPushButton("-", row_frame)
            remove_button.setObjectName("outputPreviewDateFilterRemoveButton")
            remove_button.setFixedSize(16, to_edit.sizeHint().height())
            remove_button.setToolTip("Remove date range")
            remove_button.clicked.connect(lambda: self._remove_date_filter_row(row_frame))
            row_layout.addWidget(remove_button)

        from_edit.dateChanged.connect(lambda _date: self._activate_date_filter_row(row_frame))
        to_edit.dateChanged.connect(lambda _date: self._activate_date_filter_row(row_frame))
        self._date_filter_rows.append((row_frame, from_edit, to_edit))
        self._date_filter_layout.addWidget(row_frame)

    def _style_date_filter_calendar(self, date_edit: QDateEdit) -> None:
        calendar = date_edit.calendarWidget()
        if calendar is None:
            return
        calendar.setObjectName("outputPreviewDateFilterCalendar")
        calendar.setGridVisible(False)

    def _add_empty_date_filter_row(self) -> None:
        self._add_date_filter_row()
        self._queue_condition_search()

    def _activate_date_filter_row(self, row_frame: QFrame) -> None:
        row_frame.setProperty("outputPreviewDateRangeActive", True)
        self._queue_condition_search()

    def _remove_date_filter_row(self, row_frame: QFrame) -> None:
        if len(self._date_filter_rows) <= 1:
            return
        remaining_rows: list[tuple[QFrame, QDateEdit, QDateEdit]] = []
        for frame, from_edit, to_edit in self._date_filter_rows:
            if frame is row_frame:
                if self._date_filter_layout is not None:
                    self._date_filter_layout.removeWidget(frame)
                frame.deleteLater()
            else:
                remaining_rows.append((frame, from_edit, to_edit))
        self._date_filter_rows = remaining_rows
        self._queue_condition_search()

    def _active_date_filter_ranges(self) -> tuple[DateFilterRange, ...]:
        active_filter = column_filter_component(self._explorer.active_column_filter(self._column_name), "date")
        if active_filter is None or not active_filter.values:
            return ()
        return tuple((str(start_value), str(end_value)) for start_value, end_value in active_filter.values)

    def _build_time_filter_controls(self) -> QFrame:
        time_section = QFrame(self)
        time_section.setObjectName("outputPreviewTimeFilterSection")
        self._time_filter_layout = QVBoxLayout(time_section)
        self._time_filter_layout.setContentsMargins(0, 0, 0, 0)
        self._time_filter_layout.setSpacing(6)

        active_ranges = self._active_time_filter_ranges()
        if not active_ranges:
            self._add_time_filter_row(start_value="00:00:00", end_value="23:59:59", active=False)
            return time_section
        for start_value, end_value in active_ranges:
            self._add_time_filter_row(start_value=start_value, end_value=end_value, active=True)
        return time_section

    def _add_time_filter_row(
        self,
        *,
        start_value: str | None = None,
        end_value: str | None = None,
        active: bool = False,
    ) -> None:
        if self._time_filter_layout is None:
            return
        row_frame = QFrame(self)
        row_frame.setObjectName("outputPreviewControlBar")
        row_frame.setProperty("outputPreviewTimeRangeActive", active)
        row_layout = QHBoxLayout(row_frame)
        row_layout.setContentsMargins(6, 6, 6, 6)
        row_layout.setSpacing(6)

        from_edit = QTimeEdit(row_frame)
        from_edit.setObjectName("outputPreviewTimeFilterFromInput")
        from_edit.setDisplayFormat("HH:mm:ss")
        from_edit.setTime(_qtime_from_iso(start_value or "00:00:00"))
        row_layout.addWidget(from_edit, 1)

        to_edit = QTimeEdit(row_frame)
        to_edit.setObjectName("outputPreviewTimeFilterToInput")
        to_edit.setDisplayFormat("HH:mm:ss")
        to_edit.setTime(_qtime_from_iso(end_value or start_value or "23:59:59"))
        to_edit.setFixedHeight(from_edit.sizeHint().height())
        from_edit.setFixedHeight(to_edit.sizeHint().height())
        row_layout.addWidget(to_edit, 1)

        add_button = QPushButton("+", row_frame)
        add_button.setObjectName("outputPreviewTimeFilterAddButton")
        add_button.setFixedSize(16, to_edit.sizeHint().height())
        add_button.setToolTip("Add time range")
        add_button.clicked.connect(self._add_empty_time_filter_row)
        row_layout.addWidget(add_button)

        if self._time_filter_rows:
            remove_button = QPushButton("-", row_frame)
            remove_button.setObjectName("outputPreviewTimeFilterRemoveButton")
            remove_button.setFixedSize(16, to_edit.sizeHint().height())
            remove_button.setToolTip("Remove time range")
            remove_button.clicked.connect(lambda: self._remove_time_filter_row(row_frame))
            row_layout.addWidget(remove_button)

        from_edit.timeChanged.connect(lambda _time: self._activate_time_filter_row(row_frame))
        to_edit.timeChanged.connect(lambda _time: self._activate_time_filter_row(row_frame))
        self._time_filter_rows.append((row_frame, from_edit, to_edit))
        self._time_filter_layout.addWidget(row_frame)

    def _add_empty_time_filter_row(self) -> None:
        self._add_time_filter_row()
        self._queue_condition_search()

    def _activate_time_filter_row(self, row_frame: QFrame) -> None:
        row_frame.setProperty("outputPreviewTimeRangeActive", True)
        self._queue_condition_search()

    def _remove_time_filter_row(self, row_frame: QFrame) -> None:
        if len(self._time_filter_rows) <= 1:
            return
        remaining_rows: list[tuple[QFrame, QTimeEdit, QTimeEdit]] = []
        for frame, from_edit, to_edit in self._time_filter_rows:
            if frame is row_frame:
                if self._time_filter_layout is not None:
                    self._time_filter_layout.removeWidget(frame)
                frame.deleteLater()
            else:
                remaining_rows.append((frame, from_edit, to_edit))
        self._time_filter_rows = remaining_rows
        self._queue_condition_search()

    def _active_time_filter_ranges(self) -> tuple[TimeFilterRange, ...]:
        active_filter = column_filter_component(self._explorer.active_column_filter(self._column_name), "time")
        if active_filter is None or not active_filter.values:
            return ()
        return tuple((str(start_value), str(end_value)) for start_value, end_value in active_filter.values)

    def _build_number_filter_controls(self) -> QFrame:
        number_section = QFrame(self)
        number_section.setObjectName("outputPreviewNumberFilterSection")
        self._number_filter_layout = QVBoxLayout(number_section)
        self._number_filter_layout.setContentsMargins(0, 0, 0, 0)
        self._number_filter_layout.setSpacing(6)

        active_ranges = self._active_number_filter_ranges()
        if not active_ranges:
            active_ranges = (("", ""),)
        for min_value, max_value in active_ranges:
            self._add_number_filter_row(min_value=min_value, max_value=max_value)
        return number_section

    def _add_number_filter_row(self, *, min_value: str = "", max_value: str = "") -> None:
        if self._number_filter_layout is None:
            return
        row_frame = QFrame(self)
        row_frame.setObjectName("outputPreviewControlBar")
        row_layout = QHBoxLayout(row_frame)
        row_layout.setContentsMargins(6, 6, 6, 6)
        row_layout.setSpacing(6)

        min_edit = QLineEdit(row_frame)
        min_edit.setObjectName("outputPreviewNumberFilterMinInput")
        min_edit.setPlaceholderText("Min")
        min_edit.setClearButtonEnabled(True)
        min_edit.setText(min_value)
        row_layout.addWidget(min_edit, 1)

        max_edit = QLineEdit(row_frame)
        max_edit.setObjectName("outputPreviewNumberFilterMaxInput")
        max_edit.setPlaceholderText("Max")
        max_edit.setClearButtonEnabled(True)
        max_edit.setFixedHeight(min_edit.sizeHint().height())
        min_edit.setFixedHeight(max_edit.sizeHint().height())
        max_edit.setText(max_value)
        row_layout.addWidget(max_edit, 1)

        add_button = QPushButton("+", row_frame)
        add_button.setObjectName("outputPreviewNumberFilterAddButton")
        add_button.setFixedSize(16, max_edit.sizeHint().height())
        add_button.setToolTip("Add numeric range")
        add_button.clicked.connect(self._add_empty_number_filter_row)
        row_layout.addWidget(add_button)

        if self._number_filter_rows:
            remove_button = QPushButton("-", row_frame)
            remove_button.setObjectName("outputPreviewNumberFilterRemoveButton")
            remove_button.setFixedSize(16, max_edit.sizeHint().height())
            remove_button.setToolTip("Remove numeric range")
            remove_button.clicked.connect(lambda: self._remove_number_filter_row(row_frame))
            row_layout.addWidget(remove_button)

        min_edit.textChanged.connect(lambda _text: self._queue_condition_search())
        max_edit.textChanged.connect(lambda _text: self._queue_condition_search())
        self._number_filter_rows.append((row_frame, min_edit, max_edit))
        self._number_filter_layout.addWidget(row_frame)

    def _add_empty_number_filter_row(self) -> None:
        self._add_number_filter_row()
        self._queue_condition_search()

    def _remove_number_filter_row(self, row_frame: QFrame) -> None:
        if len(self._number_filter_rows) <= 1:
            return
        remaining_rows: list[tuple[QFrame, QLineEdit, QLineEdit]] = []
        for frame, min_edit, max_edit in self._number_filter_rows:
            if frame is row_frame:
                if self._number_filter_layout is not None:
                    self._number_filter_layout.removeWidget(frame)
                frame.deleteLater()
            else:
                remaining_rows.append((frame, min_edit, max_edit))
        self._number_filter_rows = remaining_rows
        self._queue_condition_search()

    def _active_number_filter_ranges(self) -> tuple[NumberFilterRange, ...]:
        active_filter = column_filter_component(self._explorer.active_column_filter(self._column_name), "number")
        if active_filter is None or not active_filter.values:
            return ()
        return tuple((str(min_value), str(max_value)) for min_value, max_value in active_filter.values)

    def _build_boolean_filter_controls(self) -> QFrame:
        boolean_section = QFrame(self)
        boolean_section.setObjectName("outputPreviewBooleanFilterSection")
        row_layout = QHBoxLayout(boolean_section)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(6)

        combo = QComboBox(boolean_section)
        combo.setObjectName("outputPreviewBooleanFilterCombo")
        combo.addItem("Any", "")
        combo.addItem("True", "true")
        combo.addItem("False", "false")
        combo.addItem("(blank)", "blank")
        active_value = self._active_boolean_filter_value()
        active_index = combo.findData(active_value)
        if active_index >= 0:
            combo.setCurrentIndex(active_index)
        combo.currentIndexChanged.connect(lambda _index: self._queue_condition_search())
        row_layout.addWidget(combo, 1)
        self._boolean_filter_combo = combo
        return boolean_section

    def _active_boolean_filter_value(self) -> str:
        active_filter = column_filter_component(self._explorer.active_column_filter(self._column_name), "boolean")
        if active_filter is None or not active_filter.values:
            return ""
        return str(active_filter.values[0])

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
                    if self._is_date_calendar_interaction(watched, global_point):
                        return False
                    self.close()
                    return False
            elif event.type() == QEvent.Type.KeyPress and getattr(event, "key", lambda: None)() == Qt.Key.Key_Escape:
                self.close()
                return True
        return super().eventFilter(watched, event)

    def _is_date_calendar_interaction(self, watched: object, global_point: QPoint) -> bool:
        if self._is_date_calendar_widget(watched):
            return True
        widget_at_point = QApplication.widgetAt(global_point)
        if self._is_date_calendar_widget(widget_at_point):
            return True
        for _, from_edit, to_edit in self._date_filter_rows:
            for date_edit in (from_edit, to_edit):
                calendar = date_edit.calendarWidget()
                if calendar is not None and self._widget_contains_global_point(calendar, global_point):
                    return True
        return False

    def _is_date_calendar_widget(self, watched: object) -> bool:
        if not isinstance(watched, QWidget):
            return False
        widget: QWidget | None = watched
        while widget is not None:
            for _, from_edit, to_edit in self._date_filter_rows:
                if widget in {from_edit.calendarWidget(), to_edit.calendarWidget()}:
                    return True
            widget = widget.parentWidget()
        return False

    def _widget_contains_global_point(self, widget: QWidget, global_point: QPoint) -> bool:
        if not widget.isVisible():
            return False
        widget_point = widget.mapFromGlobal(global_point)
        if widget.rect().contains(widget_point):
            return True
        window = widget.window()
        if window is widget or not window.isVisible():
            return False
        window_point = window.mapFromGlobal(global_point)
        return window.rect().contains(window_point)

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
        if self.search_input is not None:
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
        if self.search_input is not None:
            self.search_input.setEnabled(True)
        self.status_label.setVisible(True)
        self.status_label.setText(message)

    def _populate_items(self) -> None:
        self.values_list.clear()
        selected_values = self._explorer.selected_filter_values(self._column_name) if self._use_active_selected_values else None
        selected_value_identities = None
        if selected_values is not None:
            selected_value_identities = {value_identity(value) for value in selected_values}
        self._populating_values = True
        try:
            for label, value in self._values:
                item = QListWidgetItem(label)
                item.setData(Qt.ItemDataRole.UserRole, value)
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                identity = value_identity(value)
                explicit_uncheck = identity in self._explicitly_unchecked_value_identities
                selected_value_missing = (
                    selected_value_identities is not None
                    and identity not in selected_value_identities
                )
                should_check = not (explicit_uncheck or selected_value_missing)
                item.setCheckState(Qt.CheckState.Checked if should_check else Qt.CheckState.Unchecked)
                self.values_list.addItem(item)
        finally:
            self._populating_values = False
        self._sync_select_all_button()

    def _handle_value_item_changed(self, item: QListWidgetItem) -> None:
        if not self._populating_values:
            value = item.data(Qt.ItemDataRole.UserRole)
            identity = value_identity(value)
            if item.checkState() == Qt.CheckState.Unchecked:
                self._explicitly_unchecked_value_identities.add(identity)
            else:
                self._explicitly_unchecked_value_identities.discard(identity)
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
        column_filter = self.combined_column_filter()
        if column_filter is None:
            self._explorer.clear_column_filter(self._column_name)
        else:
            self._explorer.apply_column_filter(column_filter)
        self.close()

    def _selected_distinct_filter(self) -> ColumnFilter | None:
        selected_values: list[object] = []
        total_values: list[object] = []
        for index in range(self.values_list.count()):
            item = self.values_list.item(index)
            value = item.data(Qt.ItemDataRole.UserRole)
            total_values.append(value)
            if item.checkState() == Qt.CheckState.Checked:
                selected_values.append(value)
        selected_tuple = tuple(selected_values)
        total_tuple = tuple(total_values)
        if should_clear_distinct_filter(selected_tuple, total_tuple, complete_domain=self._value_domain_complete):
            return None
        return ColumnFilter.distinct(self._column_name, selected_tuple)

    def _clear_column_state(self) -> None:
        for _, _, line_edit in self._text_filter_rows:
            line_edit.clear()
        for _, min_edit, max_edit in self._number_filter_rows:
            min_edit.clear()
            max_edit.clear()
        if self._boolean_filter_combo is not None:
            self._boolean_filter_combo.setCurrentIndex(0)
        for _, from_edit, to_edit in self._date_filter_rows:
            today = QDate.currentDate()
            row_frame = from_edit.parentWidget()
            from_edit.blockSignals(True)
            to_edit.blockSignals(True)
            try:
                from_edit.setDate(today)
                to_edit.setDate(today)
            finally:
                from_edit.blockSignals(False)
                to_edit.blockSignals(False)
            if isinstance(row_frame, QFrame):
                row_frame.setProperty("outputPreviewDateRangeActive", False)
        for _, from_edit, to_edit in self._time_filter_rows:
            row_frame = from_edit.parentWidget()
            from_edit.blockSignals(True)
            to_edit.blockSignals(True)
            try:
                from_edit.setTime(QTime(0, 0, 0))
                to_edit.setTime(QTime(23, 59, 59))
            finally:
                from_edit.blockSignals(False)
                to_edit.blockSignals(False)
            if isinstance(row_frame, QFrame):
                row_frame.setProperty("outputPreviewTimeRangeActive", False)
        self._explorer.clear_column_filter_and_sort(self._column_name)
        self._refresh_sort_button_state()
        self._queue_search()
        self.close()

    def _queue_search(self) -> None:
        self._search_timer.start()

    def _queue_condition_search(self) -> None:
        self._use_active_selected_values = False
        self._explicitly_unchecked_value_identities.clear()
        self.set_values([], loading=True, complete_domain=False)
        self._queue_search()

    def _dispatch_search(self) -> None:
        search_text = self.search_input.text().strip() if self.search_input is not None else ""
        self._search_token += 1
        self._explorer.request_filter_values(self._column_name, search_text, self._search_token)

    def distinct_list_column_filter(self) -> ColumnFilter | None:
        if self._date_filter_rows:
            return self._date_filter()
        if self._time_filter_rows:
            return self._time_filter()
        if self._boolean_filter_combo is not None:
            return self._boolean_filter()
        if self._number_filter_rows:
            return self._number_filter()
        return self._text_filter()

    def combined_column_filter(self) -> ColumnFilter | None:
        condition_filter = self.distinct_list_column_filter()
        distinct_filter = self._selected_distinct_filter()
        if condition_filter is None:
            return distinct_filter
        if distinct_filter is None:
            return condition_filter
        return ColumnFilter.all(self._column_name, (condition_filter, distinct_filter))

    def _text_filter(self) -> ColumnFilter | None:
        conditions: list[TextFilterCondition] = []
        for _, combo, line_edit in self._text_filter_rows:
            filter_value = line_edit.text().strip()
            if not filter_value:
                continue
            conditions.append((str(combo.currentData()), filter_value))
        if not conditions:
            return None
        if len(conditions) == 1:
            operation, value = conditions[0]
            return ColumnFilter.text(self._column_name, operation, value)
        return ColumnFilter.text_conditions(self._column_name, tuple(conditions))

    def _date_filter(self) -> ColumnFilter | None:
        ranges: list[DateFilterRange] = []
        for row_frame, from_edit, to_edit in self._date_filter_rows:
            if row_frame.property("outputPreviewDateRangeActive") is not True:
                continue
            ranges.append(
                (
                    from_edit.date().toString("yyyy-MM-dd"),
                    to_edit.date().toString("yyyy-MM-dd"),
                )
            )
        if not ranges:
            return None
        if len(ranges) == 1:
            start_value, end_value = ranges[0]
            return ColumnFilter.date_range(self._column_name, start_value, end_value)
        return ColumnFilter.date_ranges(self._column_name, tuple(ranges))

    def _time_filter(self) -> ColumnFilter | None:
        ranges: list[TimeFilterRange] = []
        for row_frame, from_edit, to_edit in self._time_filter_rows:
            if row_frame.property("outputPreviewTimeRangeActive") is not True:
                continue
            ranges.append(
                (
                    from_edit.time().toString("HH:mm:ss"),
                    to_edit.time().toString("HH:mm:ss"),
                )
            )
        if not ranges:
            return None
        if len(ranges) == 1:
            start_value, end_value = ranges[0]
            return ColumnFilter.time_range(self._column_name, start_value, end_value)
        return ColumnFilter.time_ranges(self._column_name, tuple(ranges))

    def _number_filter(self) -> ColumnFilter | None:
        ranges: list[NumberFilterRange] = []
        for _, min_edit, max_edit in self._number_filter_rows:
            min_value = min_edit.text().strip()
            max_value = max_edit.text().strip()
            if not min_value and not max_value:
                continue
            ranges.append((min_value, max_value))
        if not ranges:
            return None
        if len(ranges) == 1:
            min_value, max_value = ranges[0]
            return ColumnFilter.number_range(self._column_name, min_value, max_value)
        return ColumnFilter.number_ranges(self._column_name, tuple(ranges))

    def _boolean_filter(self) -> ColumnFilter | None:
        if self._boolean_filter_combo is None:
            return None
        value = str(self._boolean_filter_combo.currentData())
        if value == "":
            return None
        if value not in {"true", "false", "blank"}:
            return None
        filter_value: BooleanFilterValue = value
        return ColumnFilter.boolean(self._column_name, filter_value)

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
        self._refresh_sort_button_state()
        self._queue_search()


class _ParquetExplorerWidget(QWidget):
    """Lazy parquet preview with Excel-style header filter popups."""

    summary_changed = Signal(str)

    def __init__(
        self,
        output_path: Path,
        *,
        timing_log_path: Path | None = None,
        external_preview_controls: (
            tuple[QSpinBox]
            | tuple[QSpinBox, QHBoxLayout]
            | tuple[QSpinBox, QHBoxLayout, QLabel]
            | None
        ) = None,
    ) -> None:
        super().__init__()
        self.setObjectName("outputPreviewExplorer")
        self._output_path = Path(output_path)
        self._timing_log_path = timing_log_path
        self._lazy_frame = pl.scan_parquet(self._output_path)
        self._schema = None
        self._current_preview = pl.DataFrame()
        self._active_filters: dict[str, ColumnFilter] = {}
        self._filter_popup: _ParquetFilterPopup | None = None
        self._distinct_loaders: list[_DistinctValueLoader] = []
        self._preview_loader: _ParquetPreviewLoader | None = None
        self._pending_preview_refresh = False
        self._active_preview_request_id: str | None = None
        self._active_distinct_requests: dict[tuple[str, int], str] = {}
        self._open_filter_column_index: int | None = None
        self._owns_preview_controls = external_preview_controls is None
        self._external_preview_controls_layout = (
            external_preview_controls[1]
            if external_preview_controls is not None and len(external_preview_controls) >= 2
            else None
        )
        self._owns_status_label = not (external_preview_controls is not None and len(external_preview_controls) >= 3)
        self._table_render_generation = 0
        self._sort_state = PreviewSortState()
        self._preview_summary_text = ""

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        if external_preview_controls is None:
            controls = QHBoxLayout()
            controls.setContentsMargins(0, 0, 0, 0)
            controls.setSpacing(8)
            controls.addStretch(1)
            self.preview_limit_spin = QSpinBox()
            self.preview_limit_spin.setObjectName("outputPreviewLimitSpin")
            self.preview_limit_spin.setFixedHeight(22)
            controls.addWidget(self.preview_limit_spin)
            layout.addLayout(controls)
        else:
            self.preview_limit_spin = external_preview_controls[0]

        self._configure_preview_controls()

        if self._owns_status_label:
            self.status_label = QLabel("Loading preview…")
            self.status_label.setObjectName("outputPreviewStatusLabel")
            self.status_label.setWordWrap(False)
            self.status_label.setMinimumWidth(0)
            self.status_label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        else:
            self.status_label = external_preview_controls[2]
        self.export_excel_button = QPushButton("Export Excel")
        self.export_excel_button.setObjectName("outputPreviewExportExcelButton")
        self.export_excel_button.setFixedHeight(22)
        self.export_excel_button.setToolTip("Export the visible preview rows to an Excel workbook.")
        self.export_excel_button.setEnabled(False)
        self.export_excel_button.clicked.connect(self._export_current_preview)
        if external_preview_controls is None:
            stretch_index = _first_layout_stretch_index(controls)
            controls.insertWidget(max(0, stretch_index), self.status_label, 1, Qt.AlignmentFlag.AlignVCenter)
            spin_index = controls.indexOf(self.preview_limit_spin)
            controls.insertWidget(max(0, spin_index), self.export_excel_button, 0, Qt.AlignmentFlag.AlignVCenter)
        elif len(external_preview_controls) >= 2:
            controls_layout = external_preview_controls[1]
            spin_index = controls_layout.indexOf(self.preview_limit_spin)
            controls_layout.insertWidget(max(0, spin_index), self.export_excel_button, 0, Qt.AlignmentFlag.AlignVCenter)
            if self._owns_status_label:
                stretch_index = _first_layout_stretch_index(controls_layout)
                controls_layout.insertWidget(max(0, stretch_index), self.status_label, 1, Qt.AlignmentFlag.AlignVCenter)
        else:
            export_row = QHBoxLayout()
            export_row.setContentsMargins(0, 0, 0, 0)
            export_row.setSpacing(8)
            export_row.addStretch(1)
            if self._owns_status_label:
                export_row.addWidget(self.status_label, 1, Qt.AlignmentFlag.AlignVCenter)
            export_row.addWidget(self.export_excel_button, 0, Qt.AlignmentFlag.AlignVCenter)
            layout.addLayout(export_row)

        self.table = _build_dataframe_table(pl.DataFrame())
        self.table.horizontalHeader().setSectionsClickable(True)
        self.table.horizontalHeader().viewport().installEventFilter(self)
        layout.addWidget(self.table, 1)
        self._refresh_preview()

    def _configure_preview_controls(self) -> None:
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
            self.preview_limit_spin.valueChanged.disconnect(self._handle_preview_controls_changed)
        except (RuntimeError, TypeError):
            pass
        if not self._owns_preview_controls:
            self.preview_limit_spin.setVisible(False)
        try:
            self.export_excel_button.clicked.disconnect(self._export_current_preview)
        except (RuntimeError, TypeError):
            pass
        preview_loader = self._preview_loader
        self._preview_loader = None
        if preview_loader is not None:
            preview_loader.wait(5000)
            preview_loader.deleteLater()
        distinct_loaders = list(self._distinct_loaders)
        self._distinct_loaders.clear()
        for loader in distinct_loaders:
            loader.wait(5000)
            loader.deleteLater()
        self._active_distinct_requests.clear()
        self._active_preview_request_id = None
        self._active_filters.clear()
        self._sort_state = PreviewSortState()
        self._schema = None
        self._current_preview = pl.DataFrame()
        self.table.clear()
        self.table.setRowCount(0)
        self.table.setColumnCount(0)
        self.export_excel_button.setEnabled(False)
        self._preview_summary_text = ""
        self._set_preview_status(None)
        if self._external_preview_controls_layout is not None:
            self._external_preview_controls_layout.removeWidget(self.export_excel_button)
            if self._owns_status_label:
                self._external_preview_controls_layout.removeWidget(self.status_label)
            self.export_excel_button.deleteLater()
            if self._owns_status_label:
                self.status_label.deleteLater()

    def selected_filter_values(self, column_name: str) -> tuple[object, ...] | None:
        column_filter = column_filter_component(self._active_filters.get(column_name), "distinct")
        if column_filter is None:
            return None
        return column_filter.values

    def active_column_filter(self, column_name: str) -> ColumnFilter | None:
        return self._active_filters.get(column_name)

    @property
    def _sort_columns(self) -> list[tuple[str, bool]]:
        return list(self._sort_state.columns)

    @_sort_columns.setter
    def _sort_columns(self, columns: list[tuple[str, bool]] | tuple[tuple[str, bool], ...]) -> None:
        self._sort_state = PreviewSortState(
            tuple((str(column_name), bool(descending)) for column_name, descending in columns)
        )

    def apply_distinct_filter(
        self,
        column_name: str,
        selected_values: tuple[object, ...],
        all_values: tuple[object, ...],
        *,
        complete_domain: bool,
    ) -> None:
        if should_clear_distinct_filter(selected_values, all_values, complete_domain=complete_domain):
            self._active_filters.pop(column_name, None)
        else:
            self._active_filters[column_name] = ColumnFilter.distinct(column_name, selected_values)
        self._refresh_preview()

    def apply_text_filter(self, column_name: str, operation: str, value: str) -> None:
        filter_value = value.strip()
        if not filter_value:
            self._active_filters.pop(column_name, None)
        else:
            self._active_filters[column_name] = ColumnFilter.text(column_name, operation, filter_value)
        self._refresh_preview()

    def apply_column_filter(self, column_filter: ColumnFilter) -> None:
        self._active_filters[column_filter.column_name] = column_filter
        self._refresh_preview()

    def clear_column_filter(self, column_name: str) -> None:
        if column_name not in self._active_filters:
            return
        self._active_filters.pop(column_name, None)
        self._refresh_preview()

    def clear_column_filter_and_sort(self, column_name: str) -> None:
        had_filter = column_name in self._active_filters
        updated_sort_state = self._sort_state.remove(column_name)
        if not had_filter and updated_sort_state == self._sort_state:
            return
        self._active_filters.pop(column_name, None)
        self._sort_state = updated_sort_state
        self._refresh_preview()

    def request_filter_values(self, column_name: str, search_text: str, token: int) -> None:
        if self._filter_popup is None or self._filter_popup._column_name != column_name:
            return
        value_filter = self._filter_popup.distinct_list_column_filter()
        loading_values = [] if value_filter is not None else self._merge_selected_values(
            column_name,
            self._preview_distinct_values(column_name),
        )
        self._filter_popup.set_values(
            loading_values,
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
                "active_filter_count": len(self._active_filters),
            },
        )
        loader = _DistinctValueLoader(
            self._output_path,
            column_name,
            token=token,
            active_filters=self._active_filters,
            value_filter=value_filter,
            sort_descending=self.sort_direction_for_column(column_name),
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
                "preview_mode": _PREVIEW_MODE_TOP,
                "row_limit": self.preview_limit_spin.value(),
                "active_filter_count": len(self._active_filters),
                "sort_column_count": len(self._sort_state.columns),
            },
        )
        self._set_preview_status("Loading preview…")
        self.table.setEnabled(False)
        self.export_excel_button.setEnabled(False)
        loader = _ParquetPreviewLoader(
            self._output_path,
            active_filters=self._active_filters,
            sort_columns=self._sort_state.columns,
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
                "active_filter_count": len(self._active_filters),
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
        value_filter = None
        if (
            self._filter_popup is not None
            and self._filter_popup._column_name == column_name
            and self._filter_popup._search_token == token
        ):
            value_filter = self._filter_popup.distinct_list_column_filter()
        loaded_values = list(values) if value_filter is not None else self._merge_selected_values(column_name, list(values))
        popup_search_text = ""
        if (
            self._filter_popup is not None
            and self._filter_popup._column_name == column_name
            and self._filter_popup._search_token == token
        ):
            popup_search_text = (
                self._filter_popup.search_input.text().strip()
                if self._filter_popup.search_input is not None
                else ""
            )
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
        loader.deleteLater()

    def _handle_preview_loaded(self, schema: object, preview: object, summary: str) -> None:
        request_id = self._active_preview_request_id
        self._active_preview_request_id = None
        self._schema = schema
        self._current_preview = preview
        self._start_table_render(preview, filtered_columns=set(self._active_filters), summary=summary)
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
        if (
            self._filter_popup is not None
            and (self._filter_popup.search_input is None or self._filter_popup.search_input.text().strip() == "")
        ):
            self.request_filter_values(self._filter_popup._column_name, "", self._filter_popup._search_token)

    def _handle_preview_failed(self, message: str) -> None:
        request_id = self._active_preview_request_id
        self._active_preview_request_id = None
        self.table.setEnabled(False)
        self.export_excel_button.setEnabled(False)
        self._preview_summary_text = "Unable to load preview"
        self._set_preview_status(message)
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

    def _handle_preview_finished(self) -> None:
        loader = self._preview_loader
        self._preview_loader = None
        if loader is not None:
            loader.deleteLater()
        if self._pending_preview_refresh:
            self._pending_preview_refresh = False
            self._refresh_preview()

    def _handle_preview_controls_changed(self) -> None:
        self._refresh_preview()

    def apply_column_sort(self, column_name: str, *, descending: bool, append: bool) -> None:
        self._sort_state = self._sort_state.apply(column_name, descending=descending, append=append)
        self._refresh_preview()

    def clear_column_sorts(self) -> None:
        if not self._sort_state.columns:
            return
        self._sort_state = self._sort_state.clear()
        self._refresh_preview()

    def sort_rank_for_column(self, column_name: str) -> int | None:
        return self._sort_state.rank_for(column_name)

    def sort_direction_for_column(self, column_name: str) -> bool | None:
        return self._sort_state.direction_for(column_name)

    def remove_column_sort(self, column_name: str) -> None:
        updated_state = self._sort_state.remove(column_name)
        if updated_state == self._sort_state:
            return
        self._sort_state = updated_state
        self._refresh_preview()

    def primary_sort_column(self) -> str | None:
        return self._sort_state.primary_column()

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
            sort_columns=self._sort_state.columns,
        )
        self.table.setEnabled(False)
        self._preview_summary_text = summary
        self._set_preview_status("Rendering preview...")
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
        self._set_preview_status(None)

    def _merge_selected_values(self, column_name: str, values: list[tuple[str, object]]) -> list[tuple[str, object]]:
        selected_values = self.selected_filter_values(column_name) or ()
        return merge_selected_values(selected_values, values)

    def _export_current_preview(self) -> None:
        _export_frame_to_excel(self._current_preview, source_path=self._output_path, parent=self)

    def _set_preview_status(self, status: str | None) -> None:
        summary_text = self._preview_summary_text
        if summary_text and status:
            text = f"{summary_text} - {status}"
        else:
            text = summary_text or status or ""
        self.status_label.setText(text)
        self.status_label.setVisible(bool(text))
        self.summary_changed.emit(text)


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
    external_preview_controls: tuple[QSpinBox, ...] | None = None,
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
    external_preview_controls: tuple[QSpinBox, ...] | None = None,
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
    return f"{frame.height} rows - {len(frame.columns)} columns - previewing up to {_PREVIEW_ROW_LIMIT} rows"


def _first_layout_stretch_index(layout: QHBoxLayout) -> int:
    for index in range(layout.count()):
        item = layout.itemAt(index)
        if item is not None and item.spacerItem() is not None:
            return index
    return layout.count() - 1


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
    if _path_contains_glob(source_path):
        return _glob_base_path(source_path) / "parquet_preview.xlsx"
    return source_path.with_name(f"{source_path.stem}_preview.xlsx")


def _path_contains_glob(path: Path) -> bool:
    return any(any(marker in part for marker in ("*", "?", "[")) for part in path.parts)


def _glob_base_path(path: Path) -> Path:
    base_parts: list[str] = []
    for part in path.parts:
        if any(marker in part for marker in ("*", "?", "[")):
            break
        base_parts.append(part)
    return Path(*base_parts) if base_parts else Path.cwd()


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
    return build_distinct_value_filter_expression(column_name, selected_values, dtype=dtype)


def _parquet_row_count_from_metadata(path: Path) -> int | None:
    files = _parquet_metadata_paths(path)
    if not files:
        return None
    try:
        return sum(pq.read_metadata(file_path).num_rows for file_path in files)
    except Exception:
        return None


def _parquet_metadata_paths(path: Path) -> tuple[Path, ...]:
    path_text = str(path)
    if glob_module.has_magic(path_text):
        return tuple(
            sorted(
                (Path(match) for match in glob_module.glob(path_text, recursive=True) if Path(match).is_file()),
                key=lambda item: str(item).lower(),
            )
        )
    return (path,) if path.is_file() else ()


def _sort_parquet_preview_query(query: pl.LazyFrame, sort_columns: tuple[tuple[str, bool], ...]) -> pl.LazyFrame:
    return query.sort(
        [column_name for column_name, _descending in sort_columns],
        descending=[descending for _column_name, descending in sort_columns],
    )


def _sorted_top_parquet_preview(
    query: pl.LazyFrame,
    sort_columns: tuple[tuple[str, bool], ...],
    row_limit: int,
) -> pl.LazyFrame:
    sort_column_names = [column_name for column_name, _descending in sort_columns]
    descending = [descending for _column_name, descending in sort_columns]
    top_k_by: list[pl.Expr] = []
    top_k_reverse: list[bool] = []
    for column_name, is_descending in sort_columns:
        top_k_by.extend([pl.col(column_name).is_not_null(), pl.col(column_name)])
        top_k_reverse.extend([True, not is_descending])
    return query.top_k(row_limit, by=top_k_by, reverse=top_k_reverse).sort(
        sort_column_names,
        descending=descending,
    )


def _top_parquet_preview_by_row_index(
    query: pl.LazyFrame,
    path: Path,
    *,
    filter_expressions: tuple[pl.Expr, ...],
    sort_columns: tuple[tuple[str, bool], ...],
    row_limit: int,
    schema_names: tuple[str, ...],
) -> pl.DataFrame:
    row_index_column = _preview_row_index_column(schema_names)
    order_column = _preview_order_column(schema_names, row_index_column)
    indexed_query = query.with_row_index(row_index_column)
    key_query = indexed_query
    for expression in filter_expressions:
        key_query = key_query.filter(expression)
    if sort_columns:
        key_query = key_query.select([row_index_column, *_sort_column_names(sort_columns)])
        key_frame = _sorted_top_parquet_preview(key_query, sort_columns, row_limit).collect()
    else:
        key_frame = key_query.select(row_index_column).head(row_limit).collect()
    row_ids = key_frame.get_column(row_index_column).to_list()
    if not row_ids:
        return query.head(0).collect()
    file_scoped_preview = _collect_preview_rows_by_file_row_ids(
        path,
        row_ids=row_ids,
        row_index_column=row_index_column,
        order_column=order_column,
    )
    if file_scoped_preview is not None:
        return file_scoped_preview
    row_order = pl.DataFrame(
        {
            row_index_column: row_ids,
            order_column: list(range(len(row_ids))),
        }
    ).lazy()
    row_start = min(row_ids)
    row_count = max(row_ids) - row_start + 1
    return (
        indexed_query.slice(row_start, row_count)
        .join(row_order, on=row_index_column, how="inner")
        .sort(order_column)
        .drop([row_index_column, order_column])
        .collect()
    )


def _collect_preview_rows_by_file_row_ids(
    path: Path,
    *,
    row_ids: list[int],
    row_index_column: str,
    order_column: str,
) -> pl.DataFrame | None:
    file_offsets = _parquet_file_row_offsets(path)
    if not file_offsets:
        return None
    remaining_rows = list(enumerate(row_ids))
    frames: list[pl.DataFrame] = []
    for file_path, row_start, row_count in file_offsets:
        file_rows = [
            (preview_order, row_id - row_start)
            for preview_order, row_id in remaining_rows
            if row_start <= row_id < row_start + row_count
        ]
        if not file_rows:
            continue
        local_ids = [local_id for _preview_order, local_id in file_rows]
        row_order = pl.DataFrame(
            {
                row_index_column: local_ids,
                order_column: [preview_order for preview_order, _local_id in file_rows],
            }
        ).lazy()
        local_start = min(local_ids)
        local_count = max(local_ids) - local_start + 1
        frames.append(
            pl.scan_parquet(file_path)
            .with_row_index(row_index_column)
            .slice(local_start, local_count)
            .join(row_order, on=row_index_column, how="inner")
            .collect()
        )
    if not frames:
        return None
    return (
        pl.concat(frames, how="vertical")
        .sort(order_column)
        .drop([row_index_column, order_column])
    )


def _parquet_file_row_offsets(path: Path) -> tuple[tuple[Path, int, int], ...]:
    files = _parquet_metadata_paths(path)
    if not files:
        return ()
    offsets: list[tuple[Path, int, int]] = []
    row_start = 0
    try:
        for file_path in files:
            row_count = pq.read_metadata(file_path).num_rows
            offsets.append((file_path, row_start, row_count))
            row_start += row_count
    except Exception:
        return ()
    return tuple(offsets)


def _sort_column_names(sort_columns: tuple[tuple[str, bool], ...]) -> list[str]:
    return [column_name for column_name, _descending in sort_columns]


def _preview_row_index_column(schema_names: tuple[str, ...]) -> str:
    return _unique_preview_column_name("__preview_row_index", schema_names)


def _preview_order_column(schema_names: tuple[str, ...], row_index_column: str) -> str:
    return _unique_preview_column_name("__preview_order", (*schema_names, row_index_column))


def _unique_preview_column_name(base_name: str, existing_names: tuple[str, ...]) -> str:
    if base_name not in existing_names:
        return base_name
    index = 1
    while f"{base_name}_{index}" in existing_names:
        index += 1
    return f"{base_name}_{index}"


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
    return value_identity(value)


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
