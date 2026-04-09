"""Launcher entrypoints for the Data Engine PySide6 UI."""

from __future__ import annotations

import os

from PySide6.QtWidgets import QApplication

from data_engine.platform.identity import APP_DISPLAY_NAME
from data_engine.ui.gui.bootstrap import build_gui_services
from data_engine.ui.gui.app import DataEngineWindow
from data_engine.ui.gui.theme import DEFAULT_THEME, resolve_theme_name, stylesheet


def _configure_qt_webengine_environment() -> None:
    """Set stable Chromium flags before the UI spins up embedded docs."""
    existing = os.environ.get("QTWEBENGINE_CHROMIUM_FLAGS", "").strip()
    flags = [flag for flag in existing.split() if flag]
    if "--disable-skia-graphite" not in flags:
        flags.append("--disable-skia-graphite")
    os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = " ".join(flags)


def launch(theme_name: str = DEFAULT_THEME) -> None:
    """Create and run the PySide6 application."""
    _configure_qt_webengine_environment()
    services = build_gui_services()
    app = QApplication.instance() or QApplication([])
    app.setApplicationName(APP_DISPLAY_NAME)
    app.setStyle("Fusion")
    resolved_theme = services.theme_service.resolve_name(theme_name)
    app.setStyleSheet(stylesheet(resolved_theme))
    window = DataEngineWindow(theme_name=resolved_theme, services=services)
    window.show()
    if QApplication.instance() is app:
        app.exec()


def main() -> None:
    """Console entrypoint for the desktop UI."""
    launch()


if __name__ == "__main__":
    main()


__all__ = ["launch", "main"]
