"""Theme-aware SVG icon rendering helpers."""

from __future__ import annotations

import re

from PySide6.QtCore import QRectF, Qt
from PySide6.QtGui import QColor, QPainter, QPixmap
from PySide6.QtSvg import QSvgRenderer

from data_engine.ui.gui.icons import load_svg_icon_text


def theme_svg_paths(svg_text: str, fill: str) -> str:
    """Apply one fill color to every SVG path element in one icon."""

    def _replace(match: re.Match[str]) -> str:
        attributes = re.sub(r'\sfill="[^"]*"', "", match.group(1))
        return f'<path fill="{fill}"{attributes}>'

    return re.sub(r"<path\b([^>]*)>", _replace, svg_text)


def render_svg_icon_pixmap(
    *,
    icon_name: str,
    size: int,
    device_pixel_ratio: float,
    fill_color: str | None = None,
    default_fill_color: QColor | str,
) -> QPixmap:
    """Render one registered SVG icon to one theme-aware pixmap."""
    svg_text = load_svg_icon_text(icon_name)
    if isinstance(default_fill_color, QColor):
        default_fill = default_fill_color.name()
    else:
        default_fill = str(default_fill_color)
    themed_svg = theme_svg_paths(svg_text, fill_color or default_fill)
    renderer = QSvgRenderer(themed_svg.encode("utf-8"))
    dpr = max(1.0, float(device_pixel_ratio))
    pixmap = QPixmap(int(size * dpr), int(size * dpr))
    pixmap.setDevicePixelRatio(dpr)
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    renderer.render(painter, QRectF(0, 0, size, size))
    painter.end()
    return pixmap


__all__ = ["render_svg_icon_pixmap", "theme_svg_paths"]
