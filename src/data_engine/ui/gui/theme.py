"""Qt stylesheet adapter over the shared Data Engine theme tokens."""

from __future__ import annotations

from importlib import resources

from data_engine.platform.theme import (
    DEFAULT_THEME,
    GITHUB_DARK,
    GITHUB_LIGHT,
    THEMES,
    ThemePalette,
    resolve_theme_name,
    system_theme_name,
    theme_button_text,
    toggle_theme_name,
)


def _check_mark_icon_url(theme_name: str) -> str:
    """Return one stylesheet-safe packaged path for the preview check-mark icon."""
    icon_file_name = f'check-mark-{"dark" if theme_name == "dark" else "light"}.svg'
    icon_path = resources.files("data_engine.ui.gui").joinpath("icons", icon_file_name)
    return str(icon_path).replace("\\", "/")


def _spin_chevron_icon_url(theme_name: str, direction: str) -> str:
    """Return one stylesheet-safe packaged path for a themed spinbox chevron icon."""
    variant = "dark" if theme_name == "dark" else "light"
    icon_file_name = f"{direction}-chevron-{variant}.svg"
    icon_path = resources.files("data_engine.ui.gui").joinpath("icons", icon_file_name)
    return str(icon_path).replace("\\", "/")


def stylesheet(theme_name: str = DEFAULT_THEME) -> str:
    """Return the application stylesheet for the requested theme."""
    palette = THEMES[resolve_theme_name(theme_name)]
    selected_sidebar_text = palette.selection_text if palette.name == "dark" else palette.text
    selected_sidebar_subtext = palette.selection_text if palette.name == "dark" else palette.text
    preview_check_mark_icon_url = _check_mark_icon_url(palette.name)
    spin_up_chevron_icon_url = _spin_chevron_icon_url(palette.name, "up")
    spin_down_chevron_icon_url = _spin_chevron_icon_url(palette.name, "down")
    return f"""
    QWidget {{
        background: {palette.app_bg};
        color: {palette.text};
        font-family: Helvetica, Arial;
        font-size: 14px;
    }}
    QMainWindow {{
        background: {palette.window_bg};
    }}
    QStatusBar {{
        background: {palette.window_bg};
        color: {palette.muted_text};
    }}
    QScrollBar:vertical {{
        background: {palette.panel_bg};
        width: 8px;
        margin: 0px;
        border: none;
        border-radius: 0px;
    }}
    QScrollBar:horizontal {{
        background: {palette.panel_bg};
        height: 8px;
        margin: 0px;
        border: none;
        border-radius: 0px;
    }}
    QScrollBar::handle:vertical,
    QScrollBar::handle:horizontal {{
        background: {palette.input_border};
        border: 1px solid {palette.panel_border};
        border-radius: 4px;
        min-height: 20px;
        min-width: 20px;
    }}
    QScrollBar::handle:vertical:hover,
    QScrollBar::handle:horizontal:hover {{
        background: {palette.hover_bg};
        border: 1px solid {palette.input_border};
    }}
    QScrollBar::handle:vertical:pressed,
    QScrollBar::handle:horizontal:pressed {{
        background: {palette.button_checked_bg};
        border: 1px solid {palette.button_checked_border};
    }}
    QScrollBar::add-line,
    QScrollBar::sub-line,
    QScrollBar::add-page,
    QScrollBar::sub-page {{
        background: transparent;
        border: none;
        width: 0px;
        height: 0px;
    }}
    QFrame#workspacePanel, QFrame#sidebarPanel {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QFrame#navRail {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QFrame#inspectorPanel {{
        background: transparent;
        border: none;
    }}
    QFrame#actionBar {{
        background: transparent;
        border: none;
    }}
    QFrame#actionBarGroup {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QTabWidget#viewStack,
    QTabWidget#viewStack::pane,
    QTabWidget#viewStack > QStackedWidget,
    QTabWidget#viewStack > QStackedWidget > QWidget {{
        background: transparent;
        border: none;
    }}
    QTabWidget#rightTabs, QTabWidget#rightTabs QWidget, QTabWidget#rightTabs QStackedWidget {{
        background: {palette.panel_bg};
        border: none;
    }}
    QWidget#configTab {{
        background: {palette.panel_bg};
        border: none;
    }}
    QFrame#configRow {{
        background: transparent;
        border: none;
        border-bottom: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QScrollArea#sidebarScroll, QWidget#sidebarContent {{
        background: transparent;
        border: none;
    }}
    QFrame#sidebarCueTop, QFrame#sidebarCueBottom {{
        border: none;
        min-height: 8px;
        max-height: 8px;
        background: transparent;
    }}
    QFrame#sidebarCueTop {{
        border-top: 1px solid transparent;
        border-bottom: 1px solid transparent;
    }}
    QFrame#sidebarCueTop[active="true"] {{
        border-top: 1px solid transparent;
        border-bottom: 1px solid {palette.panel_border};
    }}
    QFrame#sidebarCueBottom {{
        border-top: 1px solid transparent;
        border-bottom: 1px solid transparent;
    }}
    QFrame#sidebarCueBottom[active="true"] {{
        border-top: 1px solid {palette.panel_border};
        border-bottom: 1px solid transparent;
    }}
    QFrame#sidebarGroupRow, QFrame#sidebarFlowRow {{
        background: transparent;
        border: 1px solid transparent;
        border-radius: 5px;
    }}
    QFrame#sidebarGroupRow {{
        border-top: 1px solid {palette.panel_border};
        padding: 0px;
    }}
    QFrame#sidebarGroupRow[hovered="true"] {{
        background: transparent;
        border-top: 1px solid {palette.hover_border};
    }}
    QFrame#sidebarFlowRow[hovered="true"] {{
        background: {palette.hover_bg};
        border: 1px solid {palette.hover_border};
    }}
    QFrame#sidebarFlowRow[selected="true"] {{
        background: {palette.button_checked_bg};
        border: 1px solid {palette.button_checked_border};
    }}
    QFrame#sidebarFlowRow[selected="true"][hovered="true"] {{
        background: {palette.button_checked_bg};
        border: 1px solid {palette.button_checked_border};
    }}
    QLabel#sidebarIcon {{
        background: transparent;
    }}
    QLabel#sidebarGroupTitle {{
        background: transparent;
        color: {palette.text};
        font-size: 12px;
        font-weight: 700;
    }}
    QLabel#sidebarGroupMeta {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 10px;
        font-weight: 600;
    }}
    QLabel#sidebarFlowCode {{
        background: transparent;
        color: {palette.text};
        font-size: 11px;
        font-weight: 700;
    }}
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarFlowCode,
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarFlowMeta,
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarFlowNumber,
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarStateDot {{
        color: {selected_sidebar_text};
    }}
    QLabel#sidebarFlowNumber {{
        background: transparent;
        color: {palette.section_text};
        font-size: 10px;
        font-weight: 700;
        min-width: 22px;
    }}
    QLabel#sidebarFlowMeta {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 9px;
        font-weight: 600;
    }}
    QLabel#sidebarFlowMeta[stateColor="success"] {{
        color: {palette.accent_text};
    }}
    QLabel#sidebarFlowMeta[stateColor="warning"] {{
        color: {palette.warning_text};
    }}
    QLabel#sidebarFlowMeta[stateColor="error"] {{
        color: {palette.error_text};
    }}
    QLabel#sidebarStateDot {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 10px;
    }}
    QLabel#sidebarStateDot[stateColor="success"] {{
        color: {palette.accent_text};
    }}
    QLabel#sidebarStateDot[stateColor="warning"] {{
        color: {palette.warning_text};
    }}
    QLabel#sidebarStateDot[stateColor="error"] {{
        color: {palette.error_text};
    }}
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarFlowMeta,
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarFlowMeta[stateColor="success"],
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarFlowMeta[stateColor="warning"],
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarFlowMeta[stateColor="error"],
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarStateDot,
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarStateDot[stateColor="success"],
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarStateDot[stateColor="warning"],
    QFrame#sidebarFlowRow[selected="true"] QLabel#sidebarStateDot[stateColor="error"] {{
        color: {selected_sidebar_subtext};
    }}
    QFrame#operationList {{
        background: transparent;
        border: none;
    }}
    QScrollArea#operationScroll {{
        background: transparent;
        border: none;
    }}
    QFrame#operationCard {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QFrame#operationCard[stepState="running"],
    QFrame#operationCard[stepState="stopping"] {{
        background: {palette.button_hover};
        border: 1px solid {palette.button_checked_border};
    }}
    QFrame#operationCard[stepState="success"] {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
    }}
    QFrame#operationCard[flashState="complete"] {{
        background: {palette.button_hover};
        border: 1px solid {palette.accent_text};
    }}
    QFrame#operationCard[stepState="failed"] {{
        background: {palette.panel_bg};
        border: 1px solid {palette.error_text};
    }}
    QFrame#operationCueTop, QFrame#operationCueBottom {{
        border: none;
        min-height: 8px;
        max-height: 8px;
        background: transparent;
    }}
    QFrame#operationCueTop {{
        border-top: 1px solid transparent;
        border-bottom: 1px solid transparent;
    }}
    QFrame#operationCueTop[active="true"] {{
        border-top: 1px solid transparent;
        border-bottom: 1px solid {palette.panel_border};
    }}
    QFrame#operationCueBottom {{
        border-top: 1px solid transparent;
        border-bottom: 1px solid transparent;
    }}
    QFrame#operationCueBottom[active="true"] {{
        border-top: 1px solid {palette.panel_border};
        border-bottom: 1px solid transparent;
    }}
    QLabel#windowTitle {{
        background: transparent;
        font-size: 30px;
        font-weight: 700;
        color: {palette.text};
    }}
    QFrame#appLogoFrame {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QLabel#appLogoGlyph {{
        background: transparent;
        border: none;
    }}
    QLabel#windowSubtitle, QLabel#heroMeta {{
        background: transparent;
        color: {palette.muted_text};
    }}
    QFrame#heroHeader {{
        background: transparent;
        border: none;
    }}
    QLabel#heroTitle {{
        background: transparent;
        font-size: 24px;
        font-weight: 700;
        color: {palette.text};
    }}
    QLabel#selectionTitle {{
        background: transparent;
        color: {palette.text};
        font-size: 20px;
        font-weight: 700;
    }}
    QLabel#heroMeta {{
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }}
    QLabel#sectionTitle {{
        background: transparent;
        color: {palette.section_text};
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
    }}
    QLabel#sectionMeta,
    QLabel#outputPreviewStatusLabel {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 11px;
        font-weight: 600;
    }}
    QLabel#workspaceCountsFooter {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 9px;
        font-weight: 600;
    }}
    QLabel#fieldLabel {{
        background: transparent;
        color: {palette.section_text};
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
    }}
    QLabel#fieldValue, QLabel#bodyText {{
        background: transparent;
        color: {palette.text};
        font-size: 12px;
    }}
    QLabel#operationStep {{
        background: transparent;
        color: {palette.section_text};
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.08em;
    }}
    QLabel#operationTitle {{
        background: transparent;
        color: {palette.text};
        font-size: 13px;
        font-weight: 600;
    }}
    QFrame#operationCard[flashState="complete"] QLabel#operationTitle,
    QFrame#operationCard[flashState="complete"] QLabel#operationStep,
    QFrame#operationCard[flashState="complete"] QLabel#operationDuration {{
        color: {palette.text};
    }}
    QFrame#operationCard[stepState="running"] QLabel#operationTitle,
    QFrame#operationCard[stepState="stopping"] QLabel#operationTitle {{
        color: {palette.text};
    }}
    QLabel#operationDuration {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 12px;
        font-weight: 500;
        min-width: 52px;
    }}
    QFrame#flowErrorAlert {{
        background: {palette.summary_bg};
        border: 1px solid {palette.error_text};
        border-radius: 5px;
    }}
    QLabel#errorText {{
        background: transparent;
        color: {palette.error_text};
        font-weight: 600;
    }}
    QFrame#summaryBadge {{
        background: transparent;
        border: none;
        border-radius: 0px;
    }}
    QFrame#summaryItem {{
        background: transparent;
        border: none;
    }}
    QLabel#summaryIcon {{
        background: transparent;
        border: none;
        min-width: 14px;
        max-width: 14px;
    }}
    QLabel#summaryValue {{
        background: transparent;
        font-weight: 700;
    }}
    QLabel#summaryValue[summaryColor="#0969da"] {{
        color: #0969da;
    }}
    QLabel#summaryValue[summaryColor="{palette.warning_text}"] {{
        color: {palette.warning_text};
    }}
    QLabel#summaryValue[summaryColor="#cf222e"] {{
        color: #cf222e;
    }}
    QPushButton {{
        background: {palette.button_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 4px;
        padding: 4px 8px;
        color: {palette.text};
        font-size: 11px;
        font-weight: 600;
    }}
    QToolButton#navButton {{
        background: transparent;
        border: 1px solid transparent;
        border-radius: 5px;
        padding: 0px;
        color: {palette.muted_text};
    }}
    QToolButton#navButton:hover {{
        background: {palette.hover_bg};
        border-color: {palette.hover_border};
        color: {palette.text};
    }}
    QToolButton#navButton:checked {{
        background: {palette.selection_bg};
        border-color: {palette.selection_border};
        color: {palette.selection_text};
    }}
    QPushButton#inspectOutputButton {{
        background: {palette.summary_bg};
        border: 1px solid {palette.summary_border};
        border-radius: 5px;
        padding: 2px 8px;
        color: {palette.muted_text};
        font-size: 11px;
        font-weight: 700;
    }}
    QPushButton#inspectOutputButton:hover {{
        background: {palette.button_hover};
        border-color: {palette.hover_border};
        color: {palette.text};
    }}
    QPushButton#outputPreviewSortAscendingButton,
    QPushButton#outputPreviewSortDescendingButton,
    QPushButton#outputPreviewSelectAllButton {{
        min-width: 28px;
        max-width: 28px;
        min-height: 28px;
        max-height: 28px;
        padding: 0px;
    }}
    QPushButton#outputPreviewTextFilterAddButton,
    QPushButton#outputPreviewTextFilterRemoveButton,
    QPushButton#outputPreviewDateFilterAddButton,
    QPushButton#outputPreviewDateFilterRemoveButton {{
        min-width: 16px;
        max-width: 16px;
        padding: 0px;
    }}
    QLineEdit#outputPreviewTextFilterInput {{
        font-size: 10px;
        padding: 2px 6px;
    }}
    QPushButton#outputPreviewSortAscendingButton[sortActive="true"],
    QPushButton#outputPreviewSortDescendingButton[sortActive="true"] {{
        background: {palette.button_checked_bg};
        border: 1px solid {palette.button_checked_border};
        color: {palette.text};
    }}
    QPushButton#outputPreviewSortAscendingButton[sortActive="true"]:hover,
    QPushButton#outputPreviewSortDescendingButton[sortActive="true"]:hover {{
        background: {palette.button_checked_bg};
        border: 1px solid {palette.button_checked_border};
        color: {palette.text};
    }}
    QPushButton#inspectOutputButton:disabled {{
        color: transparent;
        background: transparent;
        border-color: transparent;
    }}
    QPushButton:hover {{
        background: {palette.button_hover};
    }}
    QPushButton#engineButton,
    QPushButton#requestControlButton,
    QPushButton#flowRunButton,
    QPushButton#flowConfigButton,
    QPushButton#resetFlowButton,
    QPushButton#refreshButton,
    QPushButton#themeToggleButton {{
        padding: 3px 8px;
        font-size: 11px;
        border-radius: 4px;
    }}
    QComboBox#workspaceSelector {{
        min-height: 22px;
        max-height: 22px;
        padding: 1px 20px 1px 8px;
        font-size: 11px;
        border-radius: 4px;
    }}
    QPushButton#requestControlButton {{
        background: {palette.request_control_bg};
        border: 1px solid {palette.request_control_border};
        color: #ffffff;
    }}
    QPushButton#requestControlButton:hover {{
        background: {palette.request_control_hover};
    }}
    QPushButton#requestControlButton:disabled {{
        background: {palette.button_disabled_bg};
        border-color: {palette.button_disabled_border};
        color: {palette.button_disabled_text};
    }}
    QPushButton#engineButton[engineState="stopped"] {{
        background: {palette.engine_start_bg};
        border: 1px solid {palette.engine_start_border};
        color: #ffffff;
    }}
    QPushButton#engineButton[engineState="stopped"]:hover {{
        background: {palette.engine_start_hover};
    }}
    QPushButton#engineButton[engineState="stopped"]:disabled {{
        background: {palette.button_disabled_bg};
        border-color: {palette.button_disabled_border};
        color: {palette.button_disabled_text};
    }}
    QPushButton#engineButton[engineState="running"] {{
        background: {palette.engine_stop_bg};
        border: 1px solid {palette.engine_stop_border};
        color: #ffffff;
    }}
    QPushButton#engineButton[engineState="running"]:hover {{
        background: {palette.engine_stop_hover};
    }}
    QPushButton#engineButton[engineState="running"]:disabled {{
        background: {palette.button_disabled_bg};
        border-color: {palette.button_disabled_border};
        color: {palette.button_disabled_text};
    }}
    QPushButton#flowRunButton[flowRunState="run"] {{
        background: {palette.button_bg};
        border: 1px solid {palette.panel_border};
        color: {palette.text};
    }}
    QPushButton#flowRunButton[flowRunState="run"]:hover {{
        background: {palette.button_hover};
    }}
    QPushButton#flowRunButton[flowRunState="run"]:disabled {{
        background: {palette.button_disabled_bg};
        border-color: {palette.button_disabled_border};
        color: {palette.button_disabled_text};
    }}
    QPushButton#flowRunButton[flowRunState="stop"] {{
        background: {palette.engine_stop_bg};
        border: 1px solid {palette.engine_stop_border};
        color: #ffffff;
    }}
    QPushButton#flowRunButton[flowRunState="stop"]:hover {{
        background: {palette.engine_stop_hover};
    }}
    QPushButton#flowRunButton[flowRunState="stop"]:disabled {{
        background: {palette.button_disabled_bg};
        border-color: {palette.button_disabled_border};
        color: {palette.button_disabled_text};
    }}
    QPushButton:checked {{
        background: {palette.button_checked_bg};
        border-color: {palette.button_checked_border};
        color: {palette.text};
    }}
    QPushButton:disabled {{
        color: {palette.button_disabled_text};
        background: {palette.button_disabled_bg};
        border-color: {palette.button_disabled_border};
    }}
    QComboBox,
    QDateEdit,
    QSpinBox {{
        background: {palette.input_bg};
        border: 1px solid {palette.hover_border};
        border-radius: 4px;
        color: {palette.text};
        font-size: 11px;
        min-height: 22px;
        padding: 1px 20px 1px 8px;
    }}
    QComboBox:hover,
    QDateEdit:hover,
    QSpinBox:hover {{
        border-color: {palette.selection_border};
    }}
    QComboBox::drop-down,
    QDateEdit::drop-down {{
        subcontrol-origin: border;
        subcontrol-position: top right;
        background: {palette.button_bg};
        border-left: 1px solid {palette.hover_border};
        border-top-right-radius: 4px;
        border-bottom-right-radius: 4px;
        width: 18px;
    }}
    QComboBox::drop-down:hover,
    QDateEdit::drop-down:hover {{
        background: {palette.hover_bg};
    }}
    QComboBox::drop-down:pressed,
    QDateEdit::drop-down:pressed {{
        background: {palette.button_hover};
    }}
    QComboBox::down-arrow,
    QDateEdit::down-arrow {{
        image: url({spin_down_chevron_icon_url});
        width: 7px;
        height: 7px;
    }}
    QSpinBox::up-button,
    QSpinBox::down-button {{
        subcontrol-origin: border;
        background: {palette.button_bg};
        border-left: 1px solid {palette.hover_border};
        width: 14px;
    }}
    QSpinBox::up-button {{
        subcontrol-position: top right;
        border-top-right-radius: 4px;
        border-bottom: 1px solid {palette.hover_border};
    }}
    QSpinBox::down-button {{
        subcontrol-position: bottom right;
        border-bottom-right-radius: 4px;
    }}
    QSpinBox::up-button:hover,
    QSpinBox::down-button:hover {{
        background: {palette.hover_bg};
    }}
    QSpinBox::up-button:pressed,
    QSpinBox::down-button:pressed {{
        background: {palette.button_hover};
    }}
    QSpinBox::up-arrow,
    QSpinBox::down-arrow {{
        width: 7px;
        height: 7px;
    }}
    QSpinBox::up-arrow {{
        image: url({spin_up_chevron_icon_url});
    }}
    QSpinBox::down-arrow {{
        image: url({spin_down_chevron_icon_url});
    }}
    QLineEdit {{
        background: {palette.input_bg};
        border: 1px solid {palette.input_border};
        border-radius: 5px;
        padding: 9px 12px;
        color: {palette.text};
    }}
    QLineEdit#pathInput {{
        border-radius: 4px;
        min-height: 22px;
        padding: 3px 8px;
        font-size: 11px;
    }}
    QLineEdit#outputPreviewFilterInput {{
        border-radius: 0px;
        padding: 2px 6px;
        margin: 0px;
        font-size: 10px;
    }}
    QListWidget, QTextEdit {{
        background: transparent;
        border: 1px solid {palette.input_border};
        border-radius: 5px;
        color: {palette.text};
    }}
    QListWidget#logList {{
        background: transparent;
        border: none;
        border-radius: 0px;
    }}
    QListWidget#logList::item {{
        padding: 0px;
        margin: 0px 0px 4px 0px;
        border: none;
    }}
    QListWidget#runLogList {{
        background: transparent;
        border: none;
        border-radius: 0px;
        outline: 0;
    }}
    QListWidget#runLogList::item {{
        padding: 0px;
        margin: 0px;
        border: none;
    }}
    QFrame#logRow, QFrame#logRunRow {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QFrame#rawLogRow {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QLabel#logCaret {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 11px;
        font-weight: 700;
        min-width: 12px;
    }}
    QLabel#logPrimary {{
        background: transparent;
        color: {palette.text};
        font-size: 11px;
        font-weight: 700;
    }}
    QLabel#logDuration {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 10px;
        font-weight: 400;
    }}
    QLabel#logSource {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 10px;
        font-weight: 600;
    }}
    QPushButton#logIconButton {{
        background: transparent;
        border: 1px solid transparent;
        border-radius: 5px;
        padding: 2px;
        min-width: 22px;
        max-width: 22px;
        min-height: 22px;
        max-height: 22px;
    }}
    QPushButton#logIconButton:hover {{
        background: {palette.hover_bg};
        border-color: {palette.hover_border};
    }}
    QPushButton#logIconButton:disabled {{
        background: transparent;
        border-color: transparent;
    }}
    QLabel#logStatus {{
        background: {palette.button_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
        color: {palette.selection_text};
        font-size: 10px;
        font-weight: 700;
        padding: 1px 6px;
        min-height: 16px;
    }}
    QLabel#logStatus[stateColor="success"] {{
        background: {palette.accent_text};
        border-color: {palette.accent_text};
        color: #ffffff;
    }}
    QLabel#logStatus[stateColor="started"] {{
        background: {palette.selection_bg};
        border-color: {palette.selection_bg};
        color: #ffffff;
    }}
    QLabel#logStatus[stateColor="warning"] {{
        background: {palette.warning_text};
        border-color: {palette.warning_text};
        color: #ffffff;
    }}
    QLabel#logStatus[stateColor="error"] {{
        background: {palette.error_text};
        border-color: {palette.error_text};
        color: #ffffff;
    }}
    QLabel#logStatusIcon {{
        background: transparent;
        min-width: 16px;
        max-width: 16px;
    }}
    QDialog#outputPreviewDialog {{
        background: {palette.app_bg};
    }}
    QFrame#outputPreviewHeader {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QFrame#outputPreviewBody {{
        background: {palette.panel_bg};
        border: none;
    }}
    QFrame#configPreviewBody {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QLabel#outputPreviewPath {{
        background: transparent;
        color: {palette.muted_text};
        font-family: "Segoe UI", Helvetica, Arial;
        font-size: 12px;
        font-weight: 500;
    }}
    QFrame#rawLogRow {{
        background: {palette.panel_bg};
        border: 1px solid {palette.panel_border};
        border-radius: 5px;
    }}
    QWidget#rawLogInspectSlot, QWidget#rawLogIconSlot {{
        background: transparent;
        border: none;
    }}
    QLabel#rawLogTimestamp {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 10px;
        font-weight: 600;
        min-width: 72px;
    }}
    QLabel#rawLogMessage {{
        background: transparent;
        color: {palette.text};
        font-size: 11px;
        font-weight: 500;
    }}
    QLabel#rawLogStatusIcon {{
        background: transparent;
        min-width: 14px;
        max-width: 14px;
    }}
    QLabel#rawLogSource {{
        background: transparent;
        color: {palette.muted_text};
        font-size: 10px;
        font-weight: 600;
    }}
    QWidget#outputPreviewExplorer {{
        background: transparent;
        border: none;
    }}
    QTableWidget#outputPreviewTable {{
        background: {palette.panel_bg};
        alternate-background-color: {palette.panel_bg};
        border: 1px solid {palette.input_border};
        border-radius: 0px;
        gridline-color: {palette.input_border};
        selection-background-color: {palette.selection_bg};
        selection-color: {palette.selection_text};
    }}
    QTableWidget#outputPreviewTable::item {{
        padding: 4px 8px;
        border: none;
    }}
    QTableWidget#outputPreviewTable QHeaderView {{
        background: {palette.panel_bg};
        border: none;
    }}
    QTableWidget#outputPreviewTable QHeaderView::section {{
        background: transparent;
        color: {palette.text};
        border-top: 1px solid {palette.input_border};
        border-bottom: 1px solid {palette.input_border};
        border-right: 1px solid {palette.input_border};
        border-left: none;
        padding: 8px 12px;
        font-weight: 700;
        font-size: 10px;
    }}
    QTableWidget#outputPreviewTable QTableCornerButton::section {{
        background: {palette.panel_bg};
        border: none;
    }}
    QTextEdit#outputPreviewText {{
        background: {palette.panel_bg};
    }}
    QFrame#outputPreviewFilterPopup {{
        background: {palette.panel_bg};
        border: 1px solid {palette.input_border};
        border-radius: 5px;
    }}
    QFrame#outputPreviewControlBar {{
        background: {palette.panel_bg};
        border: 1px solid {palette.input_border};
        border-radius: 5px;
    }}
    QFrame#outputPreviewSortControlBar {{
        background: {palette.panel_bg};
        border: 1px solid {palette.input_border};
        border-bottom: none;
        border-radius: 5px;
        border-bottom-left-radius: 0px;
        border-bottom-right-radius: 0px;
    }}
    QListWidget#outputPreviewPopupList {{
        background: {palette.panel_bg};
        border-top: none;
        border-top-left-radius: 0px;
        border-top-right-radius: 0px;
    }}
    QListWidget::item {{
        padding: 4px 6px;
        border-radius: 5px;
    }}
    QListWidget::item:hover {{
        background: {palette.hover_bg};
        color: {palette.text};
    }}
    QListWidget::item:selected {{
        background: {palette.selection_bg};
        color: {palette.selection_text};
    }}
    QListWidget#outputPreviewPopupList::indicator {{
        width: 14px;
        height: 14px;
        border-radius: 3px;
        border: 1px solid {palette.hover_border if palette.name == "dark" else palette.input_border};
        background: transparent;
    }}
    QListWidget#outputPreviewPopupList::indicator:hover {{
        border: 1px solid {palette.selection_border};
    }}
    QListWidget#outputPreviewPopupList::indicator:checked {{
        image: url({preview_check_mark_icon_url});
    }}
    QTabWidget::pane {{
        background: transparent;
        border: none;
        margin-top: 2px;
    }}
    QTabBar::tab {{
        background: transparent;
        border: none;
        padding: 8px 14px;
        margin-right: 10px;
        color: {palette.muted_text};
        font-weight: 600;
        border-radius: 5px;
    }}
    QTabBar::tab:hover {{
        background: {palette.tab_hover_bg};
        color: {palette.text};
    }}
    QTabBar::tab:selected {{
        background: {palette.tab_selected_bg};
        color: {palette.text};
        border-bottom: 2px solid {palette.text};
    }}
    QProgressBar {{
        background: {palette.progress_bg};
        border: 1px solid {palette.input_border};
        border-radius: 5px;
    }}
    QProgressBar::chunk {{
        background: {palette.progress_chunk};
        border-radius: 5px;
    }}
    """


__all__ = [
    "DEFAULT_THEME",
    "GITHUB_DARK",
    "GITHUB_LIGHT",
    "THEMES",
    "ThemePalette",
    "resolve_theme_name",
    "stylesheet",
    "system_theme_name",
    "theme_button_text",
    "toggle_theme_name",
]
