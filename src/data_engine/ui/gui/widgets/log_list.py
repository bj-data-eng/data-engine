"""Custom log-run list widget and delegate for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import QRect, QSize, Qt
from PySide6.QtGui import QColor, QFont, QFontMetrics, QPainter, QPen
from PySide6.QtWidgets import QListWidget, QListWidgetItem, QStyledItemDelegate, QStyleOptionViewItem

from data_engine.platform.theme import THEMES, resolve_theme_name
from data_engine.views import RunGroupDisplay

if TYPE_CHECKING:
    from data_engine.domain import FlowRunState
    from data_engine.ui.gui.app import DataEngineWindow


RUN_GROUP_ROLE = int(Qt.ItemDataRole.UserRole) + 100


class LogRunItemDelegate(QStyledItemDelegate):
    """Paint one log row without creating child widgets."""

    def __init__(self, host_window: "DataEngineWindow", parent=None) -> None:
        super().__init__(parent)
        self._host_window = host_window

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index) -> None:
        run_group = index.data(RUN_GROUP_ROLE)
        if run_group is None:
            super().paint(painter, option, index)
            return
        display = RunGroupDisplay.from_run(run_group)
        theme_palette = THEMES[resolve_theme_name(self._host_window.theme_name)]
        frame_rect = option.rect.adjusted(1, 2, -1, -2)
        background = QColor(theme_palette.panel_bg)
        border = QColor(theme_palette.panel_border)
        text_color = QColor(theme_palette.text)
        muted_color = QColor(theme_palette.muted_text)
        hover_bg = QColor(theme_palette.hover_bg)
        hover_border = QColor(theme_palette.hover_border)
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setPen(QPen(border, 1))
        painter.setBrush(background)
        painter.drawRoundedRect(frame_rect, 5, 5)

        text_left = frame_rect.left() + 12
        status_rect = self.status_icon_rect(frame_rect)
        duration_text = display.duration_text or ""
        base_font = QFont(self._host_window.font())
        base_font.setStyleHint(QFont.StyleHint.SansSerif)
        duration_width = 0
        if duration_text:
            duration_font = QFont(base_font)
            duration_font.setPixelSize(10)
            duration_font.setWeight(QFont.Weight.Normal)
            duration_width = QFontMetrics(duration_font).horizontalAdvance(duration_text) + 10
        duration_rect = QRect(
            max(text_left, status_rect.left() - duration_width - 16),
            frame_rect.top(),
            duration_width,
            frame_rect.height(),
        )
        title_right = duration_rect.left() - 8 if duration_width else status_rect.left() - 8
        title_rect = QRect(text_left, frame_rect.top(), max(0, title_right - text_left), frame_rect.height())

        title_font = QFont(base_font)
        title_font.setPixelSize(11)
        title_font.setWeight(QFont.Weight.Bold)
        painter.setFont(title_font)
        painter.setPen(text_color)
        title_text = QFontMetrics(title_font).elidedText(
            display.primary_label,
            Qt.TextElideMode.ElideRight,
            max(title_rect.width(), 0),
        )
        painter.drawText(title_rect, int(Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextSingleLine), title_text)
        if duration_text:
            duration_font = QFont(title_font)
            duration_font.setPixelSize(10)
            duration_font.setWeight(QFont.Weight.Normal)
            painter.setFont(duration_font)
            painter.setPen(muted_color)
            painter.drawText(duration_rect, int(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight), duration_text)

        status_icon = self._host_window._render_svg_icon_pixmap(
            self._host_window._LOG_ICON_NAMES[display.status_visual_state],
            16,
            fill_color=self._host_window._LOG_ICON_COLORS[display.status_visual_state],
        )
        painter.drawPixmap(status_rect, status_icon)

        button_rect = self.view_button_rect(frame_rect)
        button_bg = QColor(Qt.GlobalColor.transparent)
        button_border = QColor(Qt.GlobalColor.transparent)
        if self._button_hovered(option, index.row()):
            button_bg = hover_bg
            button_border = hover_border
        painter.setPen(QPen(button_border, 1))
        painter.setBrush(button_bg)
        painter.drawRoundedRect(button_rect, 5, 5)
        view_icon = self._host_window._log_icon("view_log").pixmap(16, 16)
        painter.drawPixmap(
            button_rect.left() + (button_rect.width() - 16) // 2,
            button_rect.top() + (button_rect.height() - 16) // 2,
            view_icon,
        )
        painter.restore()

    def sizeHint(self, option: QStyleOptionViewItem, index) -> QSize:
        del option, index
        return QSize(0, 42)

    @staticmethod
    def status_icon_rect(frame_rect: QRect) -> QRect:
        return QRect(frame_rect.right() - 58, frame_rect.top() + (frame_rect.height() - 16) // 2, 16, 16)

    @staticmethod
    def view_button_rect(frame_rect: QRect) -> QRect:
        return QRect(frame_rect.right() - 32, frame_rect.top() + (frame_rect.height() - 22) // 2, 22, 22)

    @staticmethod
    def _button_hovered(option: QStyleOptionViewItem, row: int) -> bool:
        widget = option.widget
        if widget is None:
            return False
        hovered_row = getattr(widget, "_hovered_button_row", -1)
        return hovered_row == row


class LogRunListWidget(QListWidget):
    """List widget that renders log rows through a delegate."""

    def __init__(self, host_window: "DataEngineWindow") -> None:
        super().__init__()
        self._host_window = host_window
        self._delegate = LogRunItemDelegate(host_window, self)
        self._hovered_button_row = -1
        self.setItemDelegate(self._delegate)
        self.setUniformItemSizes(True)
        self.setMouseTracking(True)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.viewport().setMouseTracking(True)

    def set_run_group(self, item: QListWidgetItem, run_group: "FlowRunState") -> None:
        item.setData(RUN_GROUP_ROLE, run_group)
        item.setText(run_group.display_label)
        item.setToolTip("")
        item.setSizeHint(self._delegate.sizeHint(QStyleOptionViewItem(), None))

    def run_group(self, item: QListWidgetItem | None):
        if item is None:
            return None
        return item.data(RUN_GROUP_ROLE)

    def primary_label(self, item: QListWidgetItem | None) -> str:
        run_group = self.run_group(item)
        if run_group is None:
            return ""
        return RunGroupDisplay.from_run(run_group).primary_label

    def duration_text(self, item: QListWidgetItem | None) -> str | None:
        run_group = self.run_group(item)
        if run_group is None:
            return None
        return RunGroupDisplay.from_run(run_group).duration_text

    def source_label(self, item: QListWidgetItem | None) -> str:
        run_group = self.run_group(item)
        if run_group is None:
            return ""
        return RunGroupDisplay.from_run(run_group).source_label

    def mouseReleaseEvent(self, event) -> None:
        item = self.itemAt(event.position().toPoint())
        if item is not None:
            rect = self.visualItemRect(item)
            button_rect = self._delegate.view_button_rect(rect.adjusted(1, 2, -1, -2))
            if button_rect.contains(event.position().toPoint()):
                run_group = self.run_group(item)
                if run_group is not None:
                    self._host_window._show_run_log_preview(run_group)
                    event.accept()
                    return
        super().mouseReleaseEvent(event)

    def mouseMoveEvent(self, event) -> None:
        hovered_row = self._hovered_row_for_position(event.position().toPoint())
        self._set_hovered_button_row(hovered_row)
        super().mouseMoveEvent(event)

    def leaveEvent(self, event) -> None:
        self._set_hovered_button_row(-1)
        super().leaveEvent(event)

    def _hovered_row_for_position(self, pos) -> int:
        item = self.itemAt(pos)
        if item is None:
            return -1
        rect = self.visualItemRect(item)
        button_rect = self._delegate.view_button_rect(rect.adjusted(1, 2, -1, -2))
        if button_rect.contains(pos):
            return self.row(item)
        return -1

    def _set_hovered_button_row(self, row: int) -> None:
        if row == self._hovered_button_row:
            return
        previous_row = self._hovered_button_row
        self._hovered_button_row = row
        if previous_row >= 0:
            previous_item = self.item(previous_row)
            if previous_item is not None:
                self.viewport().update(self.visualItemRect(previous_item))
        if row >= 0:
            current_item = self.item(row)
            if current_item is not None:
                self.viewport().update(self.visualItemRect(current_item))
