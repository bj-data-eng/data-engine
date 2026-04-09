"""Documentation browser/build helpers for the desktop UI."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from typing import TYPE_CHECKING

from PySide6.QtCore import QUrl
from PySide6.QtWidgets import QFrame, QTextBrowser
from PySide6.QtWebEngineWidgets import QWebEngineView

from data_engine.ui.gui.helpers import start_worker_thread

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def _explicit_missing_error_detail(subject: str) -> str:
    return f"{subject} did not provide any additional error details."


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


def docs_build_dir(window: "DataEngineWindow") -> Path:
    return window.workspace_paths.documentation_dir / "_build" / "html"


def initialize_docs_view(window: "DataEngineWindow") -> None:
    docs_root = docs_build_dir(window)
    window.docs_root_dir = docs_root if docs_root.is_dir() else None
    if window.docs_root_dir is None:
        window.docs_status_label.setText("Built documentation is not available yet.")
        window.docs_generate_button.setVisible(True)
        window.docs_generate_button.setEnabled(not window.docs_build_running)
        if window.docs_build_running:
            window.docs_browser.setHtml("<h2>Building documentation</h2><p>Generating the in-app docs site...</p>")
        else:
            window.docs_browser.setHtml(
                "<h2>Documentation unavailable</h2><p>Generate the Sphinx site to populate the in-app docs view.</p>"
            )
        return

    window.docs_generate_button.setVisible(False)
    window.docs_generate_button.setEnabled(True)
    load_docs_page(window, window._DOCS_HOME_PAGE)


def start_docs_build(window: "DataEngineWindow") -> None:
    if window.docs_build_running:
        return
    window.docs_build_running = True
    initialize_docs_view(window)
    start_worker_thread(window, target=window._run_docs_build_worker)


def run_docs_build_worker(window: "DataEngineWindow") -> None:
    build_dir = docs_build_dir(window)
    try:
        build_dir.parent.mkdir(parents=True, exist_ok=True)
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "sphinx",
                "-b",
                "html",
                str(window.workspace_paths.sphinx_source_dir),
                str(build_dir),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        message = completed.stdout.strip() or "Documentation build completed."
        window.signals.docs_build_finished.emit(True, message)
    except Exception as exc:
        detail = str(exc)
        if isinstance(exc, subprocess.CalledProcessError):
            detail = (exc.stderr or exc.stdout or str(exc)).strip()
        window.signals.docs_build_finished.emit(
            False,
            detail or _explicit_missing_error_detail("Documentation build"),
        )


def finish_docs_build(window: "DataEngineWindow", succeeded: bool, message: str) -> None:
    window.docs_build_running = False
    if succeeded:
        initialize_docs_view(window)
        window.docs_status_label.setText("index.html")
        return
    initialize_docs_view(window)
    if not message:
        message = _explicit_missing_error_detail("Documentation build")
    window.docs_status_label.setText(message)
    window._show_message_box_later(
        title=window.windowTitle(),
        text=f"Documentation build failed.\n\n{message}",
        tone="error",
    )


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
    "docs_build_dir",
    "finish_docs_build",
    "initialize_docs_view",
    "load_docs_page",
    "run_docs_build_worker",
    "start_docs_build",
]
