"""Documentation browser helpers for the desktop UI."""

from __future__ import annotations

import importlib.resources
import os
from pathlib import Path
from typing import TYPE_CHECKING

from PySide6.QtCore import QUrl
from PySide6.QtWidgets import QFrame, QTextBrowser
from PySide6.QtWebEngineWidgets import QWebEngineView

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def create_docs_browser(window: "DataEngineWindow"):
    if os.environ.get("QT_QPA_PLATFORM") != "offscreen":
        window.docs_uses_webengine = True
        browser = QWebEngineView()
        browser.setStyleSheet("background: #ffffff;")
        return browser
    window.docs_uses_webengine = False
    browser = QTextBrowser()
    browser.setOpenExternalLinks(True)
    browser.setFrameShape(QFrame.Shape.NoFrame)
    return browser


def packaged_docs_dir() -> Path:
    package_root = importlib.resources.files("data_engine.docs")
    return Path(str(package_root.joinpath("html")))


def initialize_docs_view(window: "DataEngineWindow") -> None:
    docs_root = packaged_docs_dir()
    window.docs_root_dir = docs_root if docs_root.is_dir() else None
    if window.docs_root_dir is None:
        window.docs_status_label.setText("Packaged documentation is not available.")
        window.docs_browser.setHtml("<h2>Documentation unavailable</h2><p>Packaged documentation was not found.</p>")
        return
    if not window.docs_uses_webengine:
        target = window.docs_root_dir / window._DOCS_HOME_PAGE
        window.docs_status_label.setText(target.name)
        window.docs_browser.setHtml(
            "<h2>Documentation</h2><p>Packaged documentation is available in GUI mode.</p>"
        )
        return

    load_docs_page(window, window._DOCS_HOME_PAGE)


def load_docs_page(window: "DataEngineWindow", file_name: str) -> None:
    if window.docs_root_dir is None:
        return
    target = window.docs_root_dir / file_name
    if not target.exists():
        window.docs_status_label.setText(f"Missing page: {file_name}")
        window.docs_browser.setHtml(f"<h2>Missing documentation page</h2><p>{file_name}</p>")
        return
    window.docs_status_label.setText(target.name)
    if window.docs_uses_webengine:
        window.docs_browser.setUrl(QUrl.fromLocalFile(str(target)))
    else:
        window.docs_browser.setSource(QUrl.fromLocalFile(str(target)))


__all__ = [
    "create_docs_browser",
    "initialize_docs_view",
    "load_docs_page",
    "packaged_docs_dir",
]
