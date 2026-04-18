"""GUI application surface for the Data Engine PySide6 operator UI."""

from __future__ import annotations

import polars as pl
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from data_engine.platform.identity import APP_VERSION
from data_engine.ui.gui.bootstrap import GuiServices
from data_engine.ui.gui.app_binding import bootstrap_gui_window
from data_engine.ui.gui.control_support import GuiControlMixin
from data_engine.ui.gui.helpers import is_last_process_ui_window as helper_is_last_process_ui_window
from data_engine.ui.gui.render_support import GuiRenderingMixin
from data_engine.ui.gui.state_support import GuiStateMixin
from data_engine.ui.gui.surface import handle_close_event, handle_show_event
from data_engine.ui.gui.support import GuiWindowSupportMixin
from data_engine.ui.gui.theme import DEFAULT_THEME
from data_engine.ui.gui.widgets import (
    build_debug_view,
    build_docs_view,
    build_nav_rail,
    build_operator_view,
    build_settings_view,
)


class DataEngineWindow(GuiWindowSupportMixin, GuiRenderingMixin, GuiControlMixin, GuiStateMixin, QMainWindow):
    """Main PySide6 operator window with timers and runtime state containers."""

    _MAX_LOG_EVENTS_PER_TICK = 100
    _MAX_VISIBLE_LOG_RUNS = 50
    _MAX_DAEMON_SYNC_MISSES = 3
    _DOCS_HOME_PAGE = "index.html"
    _ACTIVE_FLOW_STATES = {"running", "polling", "scheduled", "stopping flow", "stopping runtime"}
    _VIEW_RAIL_ICON_NAMES = {
        "home": "home",
        "debug": "debug",
        "docs": "documentation",
        "settings": "settings",
    }
    _ACTION_ICON_NAMES = {
        "refresh": "started",
        "theme_toggle": "dark_light",
    }
    _LOG_ICON_NAMES = {
        "started": "started",
        "failed": "failed",
        "finished": "success",
        "view_log": "view-log",
    }
    _LOG_ICON_COLORS = {
        "started": "#0969da",
        "finished": "#1f883d",
        "failed": "#cf222e",
    }
    def __init__(self, *, theme_name: str = DEFAULT_THEME, services: GuiServices | None = None) -> None:
        super().__init__()
        bootstrap_gui_window(self, theme_name=theme_name, services=services)

    def showEvent(self, event) -> None:  # type: ignore[override]
        """Start daemon observation only after the window is actually shown."""
        handle_show_event(self, event)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        """Stop active runtimes and timers before the Qt window closes."""
        handle_close_event(self, event)

    def _build_window(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(20, 20, 20, 18)
        root_layout.setSpacing(8)

        shell = QHBoxLayout()
        shell.setContentsMargins(0, 0, 0, 0)
        shell.setSpacing(14)

        shell.addWidget(build_nav_rail(self), 0)

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(14)

        self.workspace_counts_footer_label = QLabel("")
        self.workspace_counts_footer_label.setObjectName("workspaceCountsFooter")
        self.app_version_footer_label = QLabel(f"v{APP_VERSION}")
        self.app_version_footer_label.setObjectName("workspaceCountsFooter")

        self.view_stack = QTabWidget()
        self.view_stack.setObjectName("viewStack")
        self.view_stack.tabBar().hide()
        self.view_stack.addTab(build_operator_view(self), "Home")
        self.view_stack.addTab(build_debug_view(self), "Debug")
        self.view_stack.addTab(build_docs_view(self), "Docs")
        self.view_stack.addTab(build_settings_view(self), "Settings")
        content_layout.addWidget(self.view_stack, 1)

        shell.addWidget(content, 1)

        footer_row = QHBoxLayout()
        footer_row.setContentsMargins(0, 0, 0, 0)
        footer_row.setSpacing(0)
        footer_row.addWidget(self.workspace_counts_footer_label, 0)
        footer_row.addStretch(1)
        footer_row.addWidget(self.app_version_footer_label, 0)

        root_layout.addLayout(shell, 1)
        root_layout.addLayout(footer_row, 0)
        self.workspace_counts_footer_label.setVisible(True)
        self.app_version_footer_label.setVisible(True)

__all__ = ["DataEngineWindow", "QFileDialog", "helper_is_last_process_ui_window", "pl"]
