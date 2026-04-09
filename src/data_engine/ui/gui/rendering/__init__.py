"""Rendering helpers for the desktop UI."""

from data_engine.ui.gui.rendering.artifacts import ArtifactPreviewSpec, classify_artifact_preview, populate_output_preview
from data_engine.ui.gui.rendering.icons import render_svg_icon_pixmap, theme_svg_paths

__all__ = [
    "ArtifactPreviewSpec",
    "classify_artifact_preview",
    "populate_output_preview",
    "render_svg_icon_pixmap",
    "theme_svg_paths",
]
