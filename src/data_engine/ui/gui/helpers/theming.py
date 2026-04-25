"""Theme and icon helper functions for the desktop GUI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtGui import QColor, QIcon, QPixmap
from PySide6.QtWidgets import QApplication

from data_engine.ui.gui.rendering import render_svg_icon_pixmap as render_svg_icon_pixmap_helper
from data_engine.ui.gui.theme import stylesheet

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def group_icon(window: "DataEngineWindow", group_name: str) -> QIcon:
    del group_name
    return QIcon(window._render_svg_icon_pixmap("group", 18))


def group_icon_color(window: "DataEngineWindow") -> QColor:
    return QColor(window.theme_service.palette(window.theme_name).text)


def render_svg_icon_pixmap(
    window: "DataEngineWindow",
    icon_name: str,
    size: int,
    *,
    fill_color: str | None = None,
) -> QPixmap:
    return render_svg_icon_pixmap_helper(
        icon_name=icon_name,
        size=size,
        device_pixel_ratio=window.devicePixelRatioF(),
        fill_color=fill_color,
        default_fill_color=window._group_icon_color(),
    )


def view_rail_icon(window: "DataEngineWindow", view_name: str) -> QIcon:
    icon_name = window._VIEW_RAIL_ICON_NAMES[view_name]
    return QIcon(
        render_svg_icon_pixmap_helper(
            icon_name=icon_name,
            size=18,
            inset=1.0,
            device_pixel_ratio=window.devicePixelRatioF(),
            default_fill_color=window._group_icon_color(),
        )
    )


def action_bar_icon(window: "DataEngineWindow", action_name: str) -> QIcon:
    icon_name = window._ACTION_ICON_NAMES[action_name]
    return QIcon(window._render_svg_icon_pixmap(icon_name, 16))


def log_icon(window: "DataEngineWindow", icon_name: str, size: int = 16) -> QIcon:
    resolved_name = window._LOG_ICON_NAMES[icon_name]
    fill_color = window._LOG_ICON_COLORS.get(icon_name)
    return QIcon(window._render_svg_icon_pixmap(resolved_name, size, fill_color=fill_color))


def render_group_icon_pixmap(window: "DataEngineWindow", group_name: str, size: int) -> QPixmap:
    del group_name
    return window._render_svg_icon_pixmap("group", size)


def toggle_theme(window: "DataEngineWindow") -> None:
    window.theme_name = window.theme_service.toggle_name(window.theme_name)
    window._apply_theme()


def sync_theme_to_system(window: "DataEngineWindow", *args) -> None:
    del args
    window.theme_name = window.theme_service.system_name()
    window._apply_theme()


def apply_theme(window: "DataEngineWindow") -> None:
    app = QApplication.instance()
    if app is not None:
        app.setStyleSheet(stylesheet(window.theme_name))
    window.theme_toggle_button.setIcon(window._action_bar_icon("theme_toggle"))
    window.refresh_button.setIcon(window._action_bar_icon("refresh"))
    for button in (window.home_button, window.dataframes_button, window.debug_button, window.docs_button, window.settings_button):
        icon_name = button.property("viewIconName")
        if isinstance(icon_name, str):
            button.setIcon(window._view_rail_icon(icon_name))
    if window.flow_cards:
        window._populate_flow_tree()
        window._refresh_action_buttons()
        window._refresh_log_view()
