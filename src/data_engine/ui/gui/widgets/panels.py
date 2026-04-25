"""Panel and top-level view builders for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont, QPixmap
from PySide6.QtWidgets import (
    QButtonGroup,
    QComboBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QPushButton,
    QScrollArea,
    QSplitter,
    QSpinBox,
    QToolButton,
    QVBoxLayout,
    QWidget,
)
from data_engine.ui.gui.widgets.log_list import LogRunListWidget

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def build_operator_view(window: "DataEngineWindow") -> QWidget:
    container = QWidget()
    layout = QVBoxLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(10)

    title = QLabel("Flow Control")
    title.setObjectName("heroTitle")
    layout.addWidget(title)
    window.action_bar = build_action_bar(window)
    layout.addWidget(window.action_bar)

    splitter = QSplitter(Qt.Orientation.Horizontal)
    splitter.setChildrenCollapsible(False)
    sidebar = build_sidebar(window)
    center = build_center_panel(window)
    right = build_right_panel(window)
    sidebar.setMinimumWidth(320)
    right.setMinimumWidth(320)
    splitter.addWidget(sidebar)
    splitter.addWidget(center)
    splitter.addWidget(right)
    splitter.setStretchFactor(0, 2)
    splitter.setStretchFactor(1, 6)
    splitter.setStretchFactor(2, 2)
    splitter.setSizes([320, 690, 320])
    layout.addWidget(splitter, 1)
    return container


def build_nav_rail(window: "DataEngineWindow") -> QWidget:
    panel = QFrame()
    panel.setObjectName("navRail")
    panel.setFixedWidth(56)
    layout = QVBoxLayout(panel)
    layout.setContentsMargins(6, 6, 6, 6)
    layout.setSpacing(8)

    window.view_button_group = QButtonGroup(window)
    window.view_button_group.setExclusive(True)

    window.home_button = _nav_button(window, "home", "Home")
    window.dataframes_button = _nav_button(window, "dataframes", "Dataframes")
    window.debug_button = _nav_button(window, "debug", "Debug")
    window.docs_button = _nav_button(window, "docs", "Docs")
    window.settings_button = _nav_button(window, "settings", "Settings")

    for index, button in enumerate(
        (window.home_button, window.dataframes_button, window.debug_button, window.docs_button, window.settings_button)
    ):
        window.view_button_group.addButton(button, index)
        layout.addWidget(button, 0, Qt.AlignmentFlag.AlignTop)

    window.view_button_group.idClicked.connect(window._switch_view)
    window.home_button.setChecked(True)
    layout.addStretch(1)
    return panel


def _nav_button(window: "DataEngineWindow", icon_name: str, tooltip: str) -> QToolButton:
    button = QToolButton()
    button.setObjectName("navButton")
    button.setCheckable(True)
    button.setAutoExclusive(True)
    button.setProperty("viewIconName", icon_name)
    button.setIcon(window._view_rail_icon(icon_name))
    button.setIconSize(QPixmap(18, 18).size())
    button.setFixedSize(40, 40)
    del tooltip
    return button


def build_docs_view(window: "DataEngineWindow") -> QWidget:
    container = QWidget()
    container_layout = QVBoxLayout(container)
    container_layout.setContentsMargins(0, 0, 0, 0)
    container_layout.setSpacing(10)

    title = QLabel("Documentation")
    title.setObjectName("heroTitle")
    container_layout.addWidget(title)

    panel = QFrame()
    panel.setObjectName("workspacePanel")
    layout = QVBoxLayout(panel)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(10)

    header = QHBoxLayout()
    header.setContentsMargins(0, 0, 0, 0)
    header.setSpacing(8)
    window.docs_status_label = QLabel("")
    window.docs_status_label.setObjectName("sectionMeta")
    header.addWidget(window.docs_status_label, 1)
    layout.addLayout(header)

    window.docs_browser = window._create_docs_browser()
    window.docs_browser.setObjectName("docsBrowser")
    window.docs_browser.setStyleSheet("background: #ffffff;")
    layout.addWidget(window.docs_browser, 1)
    container_layout.addWidget(panel, 1)
    window._initialize_docs_view()
    return container


def build_debug_view(window: "DataEngineWindow") -> QWidget:
    container = QWidget()
    container_layout = QVBoxLayout(container)
    container_layout.setContentsMargins(0, 0, 0, 0)
    container_layout.setSpacing(10)

    title = QLabel("Debug")
    title.setObjectName("heroTitle")
    container_layout.addWidget(title)

    splitter = QSplitter(Qt.Orientation.Horizontal)
    splitter.setChildrenCollapsible(False)

    left_panel = QFrame()
    left_panel.setObjectName("workspacePanel")
    left_layout = QVBoxLayout(left_panel)
    left_layout.setContentsMargins(12, 12, 12, 12)
    left_layout.setSpacing(8)
    left_header = QHBoxLayout()
    left_header.setContentsMargins(0, 0, 0, 0)
    left_header.setSpacing(8)
    left_title = QLabel("Saved Artifacts")
    left_title.setObjectName("sectionTitle")
    left_header.addWidget(left_title, 0, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
    left_header.addStretch(1)
    window.clear_debug_artifacts_button = QPushButton("Clear")
    window.clear_debug_artifacts_button.setFixedHeight(22)
    window.clear_debug_artifacts_button.clicked.connect(window._clear_debug_artifacts)
    left_header.addWidget(window.clear_debug_artifacts_button, 0, Qt.AlignmentFlag.AlignTop)
    left_layout.addLayout(left_header)
    window.debug_artifact_list = QListWidget()
    window.debug_artifact_list.setObjectName("runLogList")
    window.debug_artifact_list.setSpacing(4)
    window.debug_artifact_list.currentItemChanged.connect(lambda *_args: window._show_selected_debug_artifact())
    left_layout.addWidget(window.debug_artifact_list, 1)

    right_panel = QFrame()
    right_panel.setObjectName("workspacePanel")
    right_layout = QVBoxLayout(right_panel)
    right_layout.setContentsMargins(12, 12, 12, 12)
    right_layout.setSpacing(10)
    right_header = QHBoxLayout()
    right_header.setContentsMargins(0, 0, 0, 0)
    right_header.setSpacing(8)
    window.debug_artifact_title_label = QLabel("Dataframe")
    window.debug_artifact_title_label.setObjectName("sectionTitle")
    window.debug_artifact_title_label.setWordWrap(True)
    right_header.addWidget(window.debug_artifact_title_label, 0, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
    right_header.addStretch(1)
    window.debug_preview_mode_combo = QComboBox()
    window.debug_preview_mode_combo.setObjectName("outputPreviewModeCombo")
    window.debug_preview_mode_combo.setFixedHeight(22)
    window.debug_preview_mode_combo.setVisible(False)
    right_header.addWidget(window.debug_preview_mode_combo, 0, Qt.AlignmentFlag.AlignTop)
    window.debug_preview_limit_spin = QSpinBox()
    window.debug_preview_limit_spin.setObjectName("outputPreviewLimitSpin")
    window.debug_preview_limit_spin.setFixedHeight(22)
    window.debug_preview_limit_spin.setVisible(False)
    right_header.addWidget(window.debug_preview_limit_spin, 0, Qt.AlignmentFlag.AlignTop)
    window.debug_preview_controls_layout = right_header
    right_layout.addLayout(right_header)

    window.debug_artifact_summary_label = QLabel("")
    window.debug_artifact_summary_label.setObjectName("workspaceCountsFooter")
    window.debug_artifact_summary_label.setWordWrap(False)
    window.debug_artifact_summary_label.setVisible(False)

    window.debug_artifact_source_label = QLabel("")
    window.debug_artifact_source_label.setObjectName("outputPreviewPath")
    window.debug_artifact_source_label.setWordWrap(True)
    window.debug_artifact_source_label.setVisible(False)
    right_layout.addWidget(window.debug_artifact_source_label)

    preview_panel = QFrame()
    preview_panel.setObjectName("outputPreviewBody")
    window.debug_preview_layout = QVBoxLayout(preview_panel)
    window.debug_preview_layout.setContentsMargins(0, 0, 0, 0)
    window.debug_preview_layout.setSpacing(8)
    right_layout.addWidget(preview_panel, 1)
    right_layout.addWidget(window.debug_artifact_summary_label, 0, Qt.AlignmentFlag.AlignLeft)

    splitter.addWidget(left_panel)
    splitter.addWidget(right_panel)
    splitter.setStretchFactor(0, 2)
    splitter.setStretchFactor(1, 5)
    splitter.setSizes([320, 760])
    container_layout.addWidget(splitter, 1)
    return container


def build_dataframes_view(window: "DataEngineWindow") -> QWidget:
    container = QWidget()
    container_layout = QVBoxLayout(container)
    container_layout.setContentsMargins(0, 0, 0, 0)
    container_layout.setSpacing(10)

    header = QHBoxLayout()
    header.setContentsMargins(0, 0, 0, 0)
    header.setSpacing(8)
    title = QLabel("Dataframes")
    title.setObjectName("heroTitle")
    header.addWidget(title, 0, Qt.AlignmentFlag.AlignVCenter)
    header.addStretch(1)

    window.dataframe_browse_file_button = QPushButton("File")
    window.dataframe_browse_file_button.setObjectName("dataframeBrowseFileButton")
    window.dataframe_browse_file_button.setFixedHeight(22)
    window.dataframe_browse_file_button.clicked.connect(window._browse_dataframe_file)
    header.addWidget(window.dataframe_browse_file_button, 0, Qt.AlignmentFlag.AlignVCenter)
    window.dataframe_browse_folder_button = QPushButton("Folder")
    window.dataframe_browse_folder_button.setObjectName("dataframeBrowseFolderButton")
    window.dataframe_browse_folder_button.setFixedHeight(22)
    window.dataframe_browse_folder_button.clicked.connect(window._browse_dataframe_folder)
    header.addWidget(window.dataframe_browse_folder_button, 0, Qt.AlignmentFlag.AlignVCenter)
    window.dataframe_source_input = QLineEdit()
    window.dataframe_source_input.setObjectName("pathInput")
    window.dataframe_source_input.setReadOnly(True)
    window.dataframe_source_input.setPlaceholderText("Choose a parquet file or folder")
    window.dataframe_source_input.setMinimumWidth(340)
    header.addWidget(window.dataframe_source_input, 1, Qt.AlignmentFlag.AlignVCenter)
    container_layout.addLayout(header)

    panel = QFrame()
    panel.setObjectName("workspacePanel")
    panel_layout = QVBoxLayout(panel)
    panel_layout.setContentsMargins(12, 12, 12, 12)
    panel_layout.setSpacing(10)
    panel_header = QHBoxLayout()
    panel_header.setContentsMargins(0, 0, 0, 0)
    panel_header.setSpacing(8)
    window.dataframe_preview_title_label = QLabel("Preview")
    window.dataframe_preview_title_label.setObjectName("sectionTitle")
    window.dataframe_preview_title_label.setWordWrap(False)
    panel_header.addWidget(window.dataframe_preview_title_label, 0, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
    panel_header.addStretch(1)
    window.dataframe_preview_mode_combo = QComboBox()
    window.dataframe_preview_mode_combo.setObjectName("outputPreviewModeCombo")
    window.dataframe_preview_mode_combo.setFixedHeight(22)
    window.dataframe_preview_mode_combo.setVisible(False)
    panel_header.addWidget(window.dataframe_preview_mode_combo, 0, Qt.AlignmentFlag.AlignTop)
    window.dataframe_preview_limit_spin = QSpinBox()
    window.dataframe_preview_limit_spin.setObjectName("outputPreviewLimitSpin")
    window.dataframe_preview_limit_spin.setFixedHeight(22)
    window.dataframe_preview_limit_spin.setVisible(False)
    panel_header.addWidget(window.dataframe_preview_limit_spin, 0, Qt.AlignmentFlag.AlignTop)
    window.dataframe_preview_controls_layout = panel_header
    panel_layout.addLayout(panel_header)

    window.dataframe_preview_summary_label = QLabel("")
    window.dataframe_preview_summary_label.setObjectName("workspaceCountsFooter")
    window.dataframe_preview_summary_label.setWordWrap(False)
    window.dataframe_preview_summary_label.setVisible(False)

    preview_panel = QFrame()
    preview_panel.setObjectName("outputPreviewBody")
    window.dataframe_preview_layout = QVBoxLayout(preview_panel)
    window.dataframe_preview_layout.setContentsMargins(0, 0, 0, 0)
    window.dataframe_preview_layout.setSpacing(8)
    panel_layout.addWidget(preview_panel, 1)
    panel_layout.addWidget(window.dataframe_preview_summary_label, 0, Qt.AlignmentFlag.AlignLeft)

    container_layout.addWidget(panel, 1)
    window._clear_dataframe_preview("Choose a parquet source to preview it here.")
    return container


def build_settings_view(window: "DataEngineWindow") -> QWidget:
    container = QWidget()
    container_layout = QVBoxLayout(container)
    container_layout.setContentsMargins(0, 0, 0, 0)
    container_layout.setSpacing(10)

    title = QLabel("Settings")
    title.setObjectName("heroTitle")
    container_layout.addWidget(title)

    splitter = QSplitter(Qt.Orientation.Horizontal)
    splitter.setChildrenCollapsible(False)
    workspace_panel = _build_workspace_settings_panel(window)
    bootstrap_panel = _build_bootstrap_settings_panel(window)
    workspace_panel.setMinimumWidth(360)
    bootstrap_panel.setMinimumWidth(360)
    splitter.addWidget(workspace_panel)
    splitter.addWidget(bootstrap_panel)
    splitter.setStretchFactor(0, 1)
    splitter.setStretchFactor(1, 1)
    splitter.setSizes([520, 520])

    container_layout.addWidget(splitter, 1)
    window._refresh_workspace_root_controls()
    return container


def _build_workspace_settings_panel(window: "DataEngineWindow") -> QWidget:
    workspace_panel = QFrame()
    workspace_panel.setObjectName("workspacePanel")
    workspace_layout = QVBoxLayout(workspace_panel)
    workspace_layout.setContentsMargins(18, 18, 18, 18)
    workspace_layout.setSpacing(12)

    workspace_title = QLabel("Workspace Folder")
    workspace_title.setObjectName("sectionTitle")
    workspace_layout.addWidget(workspace_title)

    workspace_intro = QLabel(
        "Choose the folder on this workstation that contains your workspaces. "
        "Selecting a folder updates the app immediately."
    )
    workspace_intro.setWordWrap(True)
    workspace_intro.setObjectName("bodyText")
    workspace_layout.addWidget(workspace_intro)

    window.workspace_root_status_label = QLabel("")
    window.workspace_root_status_label.setWordWrap(True)
    window.workspace_root_status_label.setObjectName("sectionMeta")
    workspace_layout.addWidget(window.workspace_root_status_label)

    window.workspace_root_input = QLineEdit()
    window.workspace_root_input.setObjectName("pathInput")
    window.workspace_root_input.setReadOnly(True)
    window.workspace_root_input.setPlaceholderText("Choose a workspace folder")
    window.workspace_root_input.setText(window.workspace_session_state.root.input_text)
    workspace_layout.addWidget(window.workspace_root_input)

    workspace_actions = QHBoxLayout()
    workspace_actions.setContentsMargins(0, 0, 0, 0)
    workspace_actions.setSpacing(8)

    window.browse_workspace_root_button = QPushButton("Browse…")
    window.browse_workspace_root_button.setFixedHeight(22)
    window.browse_workspace_root_button.clicked.connect(window._browse_workspace_collection_root_override)
    workspace_actions.addWidget(window.browse_workspace_root_button)
    workspace_actions.addStretch(1)
    workspace_layout.addLayout(workspace_actions)

    provision_title = QLabel("Provision Selected Workspace")
    provision_title.setObjectName("sectionTitle")
    workspace_layout.addWidget(provision_title)

    provision_intro = QLabel(
        "Choose the workspace to provision here. Provisioning creates the folder shape for the currently selected workspace without overwriting existing authored files."
    )
    provision_intro.setWordWrap(True)
    provision_intro.setObjectName("bodyText")
    workspace_layout.addWidget(provision_intro)

    provision_selector_intro = QLabel(
        "Workspace to provision"
    )
    provision_selector_intro.setObjectName("fieldLabel")
    workspace_layout.addWidget(provision_selector_intro)

    window.workspace_settings_selector = QComboBox()
    window.workspace_settings_selector.setObjectName("workspaceSettingsSelector")
    window.workspace_settings_selector.setMinimumWidth(200)
    window.workspace_settings_selector.setFixedHeight(22)
    window.workspace_settings_selector.currentIndexChanged.connect(window._settings_workspace_target_changed)
    workspace_layout.addWidget(window.workspace_settings_selector, 0, Qt.AlignmentFlag.AlignLeft)

    window.workspace_target_label = QLabel("")
    window.workspace_target_label.setWordWrap(True)
    window.workspace_target_label.setObjectName("sectionMeta")
    workspace_layout.addWidget(window.workspace_target_label)

    window.provision_workspace_button = QPushButton("Provision Selected Workspace")
    window.provision_workspace_button.setFixedHeight(22)
    window.provision_workspace_button.clicked.connect(window._provision_selected_workspace)
    workspace_layout.addWidget(window.provision_workspace_button, 0, Qt.AlignmentFlag.AlignLeft)

    window.workspace_provision_status_label = QLabel("")
    window.workspace_provision_status_label.setWordWrap(True)
    window.workspace_provision_status_label.setObjectName("sectionMeta")
    workspace_layout.addWidget(window.workspace_provision_status_label)

    workspace_layout.addStretch(1)
    return workspace_panel


def _build_bootstrap_settings_panel(window: "DataEngineWindow") -> QWidget:
    details_panel = QFrame()
    details_panel.setObjectName("workspacePanel")
    details_layout = QVBoxLayout(details_panel)
    details_layout.setContentsMargins(18, 18, 18, 18)
    details_layout.setSpacing(14)

    details_title = QLabel("Workspace Visibility")
    details_title.setObjectName("sectionTitle")
    details_layout.addWidget(details_title)

    intro = QLabel(
        "Read-only environment and daemon details for the current workspace."
    )
    intro.setWordWrap(True)
    intro.setObjectName("sectionMeta")
    details_layout.addWidget(intro)

    interpreter_title = QLabel("Python Interpreter")
    interpreter_title.setObjectName("sectionTitle")
    details_layout.addWidget(interpreter_title)

    details_layout.addLayout(_build_settings_fact_row("Environment", "visibility_interpreter_mode_value", window))
    details_layout.addLayout(_build_settings_fact_row("Executable", "visibility_interpreter_value", window, selectable=True))

    daemon_title = QLabel("Emergency")
    daemon_title.setObjectName("sectionTitle")
    details_layout.addWidget(daemon_title)

    daemon_intro = QLabel(
        "Use only if the selected workspace daemon stops responding."
    )
    daemon_intro.setWordWrap(True)
    daemon_intro.setObjectName("sectionMeta")
    details_layout.addWidget(daemon_intro)

    window.force_shutdown_daemon_button = QPushButton("Force Stop Daemon")
    window.force_shutdown_daemon_button.setFixedHeight(22)
    window.force_shutdown_daemon_button.clicked.connect(window._force_shutdown_daemon)
    details_layout.addWidget(window.force_shutdown_daemon_button, 0, Qt.AlignmentFlag.AlignLeft)

    window.force_shutdown_daemon_status_label = QLabel("")
    window.force_shutdown_daemon_status_label.setWordWrap(True)
    window.force_shutdown_daemon_status_label.setObjectName("sectionMeta")
    details_layout.addWidget(window.force_shutdown_daemon_status_label)

    window.reset_workspace_button = QPushButton("Reset Workspace")
    window.reset_workspace_button.setFixedHeight(22)
    window.reset_workspace_button.clicked.connect(window._reset_workspace)
    details_layout.addWidget(window.reset_workspace_button, 0, Qt.AlignmentFlag.AlignLeft)

    window.reset_workspace_status_label = QLabel("")
    window.reset_workspace_status_label.setWordWrap(True)
    window.reset_workspace_status_label.setObjectName("sectionMeta")
    details_layout.addWidget(window.reset_workspace_status_label)

    details_layout.addStretch(1)
    return details_panel


def _build_settings_fact_row(
    label_text: str,
    value_attr: str,
    window: "DataEngineWindow",
    *,
    selectable: bool = False,
) -> QVBoxLayout:
    row = QVBoxLayout()
    row.setContentsMargins(0, 0, 0, 0)
    row.setSpacing(2)

    label = QLabel(label_text)
    label.setObjectName("fieldLabel")
    row.addWidget(label)

    value = QLabel("")
    value.setWordWrap(True)
    value.setObjectName("fieldValue")
    if selectable:
        value.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
    setattr(window, value_attr, value)
    row.addWidget(value)
    return row


def build_action_bar(window: "DataEngineWindow") -> QWidget:
    frame = QFrame()
    frame.setObjectName("actionBar")
    row = QHBoxLayout(frame)
    row.setContentsMargins(0, 0, 0, 0)
    row.setSpacing(10)

    window.engine_button = QPushButton("Start Engine")
    window.engine_button.setObjectName("engineButton")
    window.engine_button.setFixedHeight(22)
    window.engine_button.clicked.connect(window._toggle_runtime)

    window.refresh_button = QPushButton()
    window.refresh_button.setObjectName("refreshButton")
    window.refresh_button.setIcon(window._action_bar_icon("refresh"))
    window.refresh_button.setIconSize(QPixmap(12, 12).size())
    window.refresh_button.setFixedSize(22, 22)
    window.refresh_button.clicked.connect(window._refresh_flows_requested)

    controls_group = QFrame()
    controls_group.setObjectName("actionBarGroup")
    window.action_bar_controls_group = controls_group
    controls_group.setVisible(False)
    controls_row = QHBoxLayout(controls_group)
    controls_row.setContentsMargins(10, 8, 10, 8)
    controls_row.setSpacing(6)
    controls_row.addWidget(window.engine_button)
    window.request_control_button = QPushButton("Request Control")
    window.request_control_button.setObjectName("requestControlButton")
    window.request_control_button.setFixedHeight(22)
    window.request_control_button.setVisible(True)
    window.request_control_button.clicked.connect(window._request_control)
    controls_row.addWidget(window.request_control_button)
    window.workspace_selector = QComboBox()
    window.workspace_selector.setObjectName("workspaceSelector")
    window.workspace_selector.setMinimumWidth(200)
    window.workspace_selector.setFixedHeight(22)
    window.workspace_selector.currentIndexChanged.connect(window._workspace_selection_changed)
    controls_row.addWidget(window.workspace_selector)
    row.addWidget(controls_group, 0)

    window.lease_status_label = QLabel("")
    window.lease_status_label.setObjectName("sectionMeta")
    window.lease_status_label.setVisible(False)
    row.addWidget(window.lease_status_label, 0, Qt.AlignmentFlag.AlignVCenter)

    row.addStretch(1)
    row.addWidget(window.refresh_button, 0, Qt.AlignmentFlag.AlignVCenter)

    window.theme_toggle_button = QPushButton()
    window.theme_toggle_button.setObjectName("themeToggleButton")
    window.theme_toggle_button.setIcon(window._action_bar_icon("theme_toggle"))
    window.theme_toggle_button.setIconSize(QPixmap(12, 12).size())
    window.theme_toggle_button.setFixedSize(22, 22)
    window.theme_toggle_button.clicked.connect(window._toggle_theme)
    row.addWidget(window.theme_toggle_button)
    return frame


def build_sidebar(window: "DataEngineWindow") -> QWidget:
    panel = QFrame()
    panel.setObjectName("sidebarPanel")
    layout = QVBoxLayout(panel)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(10)

    title = QLabel("Configured Flows")
    title.setObjectName("sectionTitle")
    layout.addWidget(title)

    window.sidebar_scroll = QScrollArea()
    window.sidebar_scroll.setWidgetResizable(True)
    window.sidebar_scroll.setFrameShape(QFrame.Shape.NoFrame)
    window.sidebar_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
    window.sidebar_scroll.setObjectName("sidebarScroll")
    window.sidebar_scroll.verticalScrollBar().valueChanged.connect(window._update_sidebar_scroll_cues)
    window.sidebar_scroll.verticalScrollBar().rangeChanged.connect(window._update_sidebar_scroll_cues)
    window.sidebar_content = QWidget()
    window.sidebar_content.setObjectName("sidebarContent")
    window.sidebar_layout = QVBoxLayout(window.sidebar_content)
    window.sidebar_layout.setContentsMargins(0, 0, 0, 0)
    window.sidebar_layout.setSpacing(2)
    window.sidebar_layout.addStretch(1)
    window.sidebar_scroll.setWidget(window.sidebar_content)
    layout.addWidget(window.sidebar_scroll, 1)
    return panel


def build_center_panel(window: "DataEngineWindow") -> QWidget:
    panel = QFrame()
    panel.setObjectName("workspacePanel")
    layout = QVBoxLayout(panel)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(10)

    header = QFrame()
    header.setObjectName("heroHeader")
    header_layout = QVBoxLayout(header)
    header_layout.setContentsMargins(0, 0, 0, 0)
    header_layout.setSpacing(6)

    header_row = QHBoxLayout()
    header_row.setContentsMargins(0, 0, 0, 0)
    header_row.setSpacing(12)
    window.operations_title_label = QLabel("Steps")
    window.operations_title_label.setObjectName("sectionTitle")
    header_row.addWidget(window.operations_title_label, 0, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
    header_row.addStretch(1)

    title_row = QHBoxLayout()
    title_row.setContentsMargins(0, 0, 0, 0)
    title_row.setSpacing(6)
    window.flow_run_button = QPushButton("Run Once")
    window.flow_run_button.setObjectName("flowRunButton")
    window.flow_run_button.setProperty("flowRunState", "run")
    window.flow_run_button.setFixedHeight(22)
    window.flow_run_button.clicked.connect(window._run_selected_flow)
    title_row.addWidget(window.flow_run_button, 0, Qt.AlignmentFlag.AlignTop)
    window.flow_config_button = QPushButton("View Config")
    window.flow_config_button.setObjectName("flowConfigButton")
    window.flow_config_button.setFixedHeight(22)
    window.flow_config_button.clicked.connect(window._show_config_preview)
    title_row.addWidget(window.flow_config_button, 0, Qt.AlignmentFlag.AlignTop)
    header_row.addLayout(title_row, 0)
    header_layout.addLayout(header_row)

    error_frame = QFrame(header)
    error_frame.setObjectName("flowErrorAlert")
    error_frame.setVisible(False)
    error_layout = QHBoxLayout(error_frame)
    error_layout.setContentsMargins(10, 6, 10, 6)
    error_layout.setSpacing(0)

    window.flow_error_label = QLabel("", error_frame)
    window.flow_error_label.setWordWrap(True)
    window.flow_error_label.setObjectName("errorText")
    error_layout.addWidget(window.flow_error_label, 1, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
    window.flow_error_frame = error_frame
    header_layout.addWidget(error_frame)

    window.operation_scroll = QScrollArea()
    window.operation_scroll.setWidgetResizable(True)
    window.operation_scroll.setFrameShape(QFrame.Shape.NoFrame)
    window.operation_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
    window.operation_scroll.setObjectName("operationScroll")
    window.operation_container = QFrame()
    window.operation_container.setObjectName("operationList")
    window.operation_layout = QVBoxLayout(window.operation_container)
    window.operation_layout.setContentsMargins(0, 0, 0, 0)
    window.operation_layout.setSpacing(6)
    window.operation_layout.addStretch(1)
    window.operation_scroll.setWidget(window.operation_container)
    window.operation_scroll.verticalScrollBar().valueChanged.connect(window._update_operation_scroll_cues)
    window.operation_scroll.verticalScrollBar().rangeChanged.connect(window._update_operation_scroll_cues)

    layout.addWidget(header)
    layout.addWidget(window.operation_scroll, 1)
    return panel


def build_right_panel(window: "DataEngineWindow") -> QWidget:
    panel = QFrame()
    panel.setObjectName("workspacePanel")
    layout = QVBoxLayout(panel)
    layout.setContentsMargins(18, 18, 18, 18)
    layout.setSpacing(10)

    title = QLabel("Logs")
    title.setObjectName("sectionTitle")
    header = QHBoxLayout()
    header.setContentsMargins(0, 0, 0, 0)
    header.setSpacing(8)
    header.addWidget(title, 0, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
    header.addStretch(1)

    window.clear_flow_log_button = QPushButton("Reset Flow")
    window.clear_flow_log_button.setObjectName("resetFlowButton")
    window.clear_flow_log_button.setFixedHeight(22)
    window.clear_flow_log_button.clicked.connect(window._clear_logs)
    header.addWidget(window.clear_flow_log_button, 0, Qt.AlignmentFlag.AlignTop)
    layout.addLayout(header)

    window.log_view = LogRunListWidget(window)
    window.log_view.setObjectName("logList")
    mono = QFont("Menlo")
    mono.setStyleHint(QFont.StyleHint.Monospace)
    window.log_view.setFont(mono)
    layout.addWidget(window.log_view, 1)
    return panel


__all__ = [
    "build_action_bar",
    "build_center_panel",
    "build_dataframes_view",
    "build_debug_view",
    "build_docs_view",
    "build_nav_rail",
    "build_operator_view",
    "build_right_panel",
    "build_settings_view",
    "build_sidebar",
]
