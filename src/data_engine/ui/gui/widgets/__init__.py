"""View-layer builders for the desktop UI."""

from data_engine.ui.gui.widgets.config import build_config_value, make_label_selectable
from data_engine.ui.gui.widgets.panels import (
    build_action_bar,
    build_center_panel,
    build_debug_view,
    build_docs_view,
    build_nav_rail,
    build_operator_view,
    build_right_panel,
    build_settings_view,
    build_sidebar,
)
from data_engine.ui.gui.widgets.sidebar import build_flow_row_widget, build_group_row_widget
from data_engine.ui.gui.widgets.logs import build_log_run_widget
from data_engine.ui.gui.widgets.steps import format_operation_title, set_operation_cards

__all__ = [
    "build_action_bar",
    "build_center_panel",
    "build_debug_view",
    "build_config_value",
    "build_docs_view",
    "build_flow_row_widget",
    "build_log_run_widget",
    "build_group_row_widget",
    "build_nav_rail",
    "build_operator_view",
    "build_right_panel",
    "build_settings_view",
    "build_sidebar",
    "format_operation_title",
    "make_label_selectable",
    "set_operation_cards",
]
