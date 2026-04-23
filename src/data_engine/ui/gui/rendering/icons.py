"""Theme-aware SVG icon rendering helpers."""

from __future__ import annotations

import re

from PySide6.QtCore import QRectF, Qt
from PySide6.QtGui import QColor, QPainter, QPixmap
from PySide6.QtSvg import QSvgRenderer

from data_engine.ui.gui.icons import load_svg_icon_text


def theme_svg_paths(svg_text: str, fill: str, *, colorize_stroke: bool = False) -> str:
    """Apply one theme color to SVG path elements in one icon."""

    def _replace(match: re.Match[str]) -> str:
        attributes = match.group(1)
        attributes = re.sub(r'\sfill="[^"]*"', "", attributes)
        replacement = f'<path fill="{fill}"'
        if colorize_stroke:
            attributes = re.sub(r'\sstroke="[^"]*"', "", attributes)
            replacement += f' stroke="{fill}"'
        return f"{replacement}{attributes}>"

    return re.sub(r"<path\b([^>]*)>", _replace, svg_text)


def render_svg_icon_pixmap(
    *,
    icon_name: str,
    size: int,
    device_pixel_ratio: float,
    fill_color: str | None = None,
    default_fill_color: QColor | str,
    inset: float = 0.0,
    colorize_stroke: bool = False,
) -> QPixmap:
    """Render one registered SVG icon to one theme-aware pixmap."""
    svg_text = load_svg_icon_text(icon_name)
    if isinstance(default_fill_color, QColor):
        default_fill = default_fill_color.name()
    else:
        default_fill = str(default_fill_color)
    themed_svg = theme_svg_paths(svg_text, fill_color or default_fill, colorize_stroke=colorize_stroke)
    renderer = QSvgRenderer(themed_svg.encode("utf-8"))
    dpr = max(1.0, float(device_pixel_ratio))
    pixmap = QPixmap(int(size * dpr), int(size * dpr))
    pixmap.setDevicePixelRatio(dpr)
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    inset_value = max(0.0, float(inset))
    render_size = max(0.0, float(size) - (inset_value * 2.0))
    renderer.render(painter, QRectF(inset_value, inset_value, render_size, render_size))
    painter.end()
    return pixmap


__all__ = ["render_svg_icon_pixmap", "theme_svg_paths"]
