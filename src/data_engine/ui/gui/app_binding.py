"""Bootstrap helpers for the desktop GUI application shell."""

from __future__ import annotations

import logging
import os
import threading
from queue import Queue
from uuid import uuid4
from typing import TYPE_CHECKING

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QGuiApplication

from data_engine.domain import DaemonStatusState, FlowLogEntry, OperationSessionState, OperatorSessionState, StepOutputIndex
from data_engine.domain import WorkspaceSessionState
from data_engine.platform.identity import APP_DISPLAY_NAME, APP_INTERNAL_ID
from data_engine.platform.instrumentation import maybe_start_viztracer
from data_engine.platform.workspace_models import DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR
from data_engine.ui.gui.bootstrap import GuiServices
from data_engine.ui.gui.controllers import GuiFlowController, GuiRuntimeController
from data_engine.ui.gui.helpers import register_client_session as helper_register_client_session
from data_engine.ui.gui.runtime import QueueLogHandler, UiSignals
from data_engine.ui.gui.surface import build_default_gui_services

if TYPE_CHECKING:
    from PySide6.QtWidgets import QFrame

    from data_engine.ui.gui.app import DataEngineWindow


_DEFAULT_WINDOW_SIZE = (1480, 920)
_MINIMUM_WINDOW_SIZE = (1180, 760)
_STARTUP_SCREEN_WIDTH_RATIO = 0.78
_STARTUP_SCREEN_HEIGHT_RATIO = 0.84


def initial_window_size_for_screen(screen: object | None) -> tuple[int, int]:
    """Return the startup window size from the available screen geometry."""
    if screen is None or not hasattr(screen, "availableGeometry"):
        return _DEFAULT_WINDOW_SIZE
    geometry = screen.availableGeometry()
    width = getattr(geometry, "width", lambda: 0)()
    height = getattr(geometry, "height", lambda: 0)()
    if width <= 0 or height <= 0:
        return _DEFAULT_WINDOW_SIZE
    minimum_width, minimum_height = _MINIMUM_WINDOW_SIZE
    target_width = int(width * _STARTUP_SCREEN_WIDTH_RATIO)
    target_height = int(height * _STARTUP_SCREEN_HEIGHT_RATIO)
    return (
        min(width, max(minimum_width, target_width)),
        min(height, max(minimum_height, target_height)),
    )


def bootstrap_gui_window(window: "DataEngineWindow", *, theme_name: str, services: GuiServices | None = None) -> None:
    """Bind one GUI window to its services, runtime state, and timers."""
    window.setWindowTitle(APP_DISPLAY_NAME)
    window.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
    window.resize(*initial_window_size_for_screen(QGuiApplication.primaryScreen()))
    window.setMinimumSize(*_MINIMUM_WINDOW_SIZE)
    window.services = services or build_default_gui_services(theme_name)
    window.catalog_query_service = window.services.catalog_query_service
    window.history_query_service = window.services.history_query_service
    window.command_service = window.services.command_service
    window.daemon_service = window.services.daemon_service
    window.daemon_state_service = window.services.daemon_state_service
    window.runtime_application = window.services.runtime_application
    window.ledger_service = window.services.ledger_service
    window.log_service = window.services.log_service
    window.runtime_binding_service = window.services.runtime_binding_service
    window.runtime_state_service = window.services.runtime_state_service
    window.runtime_history_service = window.services.runtime_history_service
    window.shared_state_service = window.services.shared_state_service
    window.settings_service = window.services.settings_service
    window.theme_service = window.services.theme_service
    window.flow_controller = GuiFlowController(
        workspace_service=window.services.workspace_service,
        catalog_query_service=window.catalog_query_service,
        history_query_service=window.history_query_service,
        command_service=window.command_service,
    )
    window.runtime_controller = GuiRuntimeController(
        runtime_application=window.runtime_application,
        daemon_service=window.daemon_service,
        runtime_state_service=window.runtime_state_service,
        command_service=window.command_service,
    )
    window.theme_name = window.theme_service.resolve_name(theme_name)
    env_collection_root = os.environ.get(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR)
    initial_override = None if env_collection_root and env_collection_root.strip() else window.settings_service.workspace_collection_root()
    window.workspace_paths = window.services.workspace_service.resolve_paths(
        workspace_collection_root=initial_override,
    )
    window._operator_session_state = OperatorSessionState.from_paths(window.workspace_paths, override_root=initial_override)
    discovered = window.services.workspace_service.discover(
        app_root=window.workspace_paths.app_root,
        workspace_collection_root=initial_override,
    )
    window.workspace_session_state = WorkspaceSessionState.from_paths(
        window.workspace_paths,
        override_root=initial_override,
        discovered_workspace_ids=(item.workspace_id for item in discovered),
    )
    window.client_session_id = uuid4().hex

    window.log_queue: Queue[FlowLogEntry] = Queue()
    window.log_handler = QueueLogHandler(window.log_queue)
    logger = logging.getLogger(APP_INTERNAL_ID)
    logger.addHandler(window.log_handler)
    logger.setLevel(logging.INFO)

    window.signals = UiSignals()
    window.signals.run_finished.connect(window._finish_run)
    window.signals.runtime_finished.connect(window._finish_runtime)
    window.signals.daemon_startup_finished.connect(window._finish_daemon_startup)
    window.signals.control_action_finished.connect(window._finish_control_action)

    window.engine_runtime_stop_event = threading.Event()
    window.engine_flow_stop_event = threading.Event()
    window.manual_flow_stop_events: dict[str, threading.Event] = {}
    window.manual_flow_stopping_groups: set[str | None] = set()
    window.operation_row_widgets = []
    window.operation_tracker = OperationSessionState.empty()
    window.workspace_snapshot = None
    window.operation_flash_timers: list[QTimer] = []
    window.sidebar_flow_widgets: dict[str, QFrame] = {}
    window.sidebar_group_widgets: dict[str, QFrame] = {}
    window.step_output_index = StepOutputIndex.empty()
    window.runtime_binding = window.runtime_binding_service.open_binding(window.workspace_paths)
    helper_register_client_session(window)
    window._ui_timing_log_path = (
        window.workspace_paths.runtime_state_dir / "ui_timing.log"
        if window.workspace_paths.workspace_configured
        else None
    )
    maybe_start_viztracer(
        None if window._ui_timing_log_path is None else window.workspace_paths.runtime_state_dir / "ui_viztrace.json",
        process_name=f"gui:{window.workspace_paths.workspace_id}",
    )
    window.output_preview_dialog = None
    window.config_preview_dialog = None
    window.run_log_preview_dialog = None
    window._docs_root_dir = None
    window.docs_uses_webengine = False
    window.ui_closing = False
    window._log_view_refresh_pending = False
    window._action_buttons_refresh_pending = False
    window._worker_threads: set[threading.Thread] = set()
    window._worker_threads_lock = threading.RLock()
    window._daemon_status = DaemonStatusState.empty()
    window._last_daemon_spawn_attempt = 0.0
    window._auto_daemon_enabled = False
    window._daemon_startup_in_progress = False
    window._daemon_sync_in_progress = False
    window._daemon_sync_pending = False
    window._pending_control_actions: set[str] = set()
    window._pending_message_box: tuple[str, str, str] | None = None
    window._message_box_scheduled = False
    window._message_box_open = False
    window._message_box_generation = 0
    window._pending_workspace_switch_id = None
    window._workspace_switch_scheduled = False
    window._color_scheme_changed_slot = None
    window._last_log_view_flow_name = None
    window._last_log_view_run_keys = ()
    window._last_log_view_signature = ()

    window._build_window()
    window._reload_workspace_options()
    window._load_flows()
    style_hints = QGuiApplication.styleHints()
    window._style_hints = style_hints
    if os.environ.get("QT_QPA_PLATFORM") != "offscreen" and hasattr(style_hints, "colorSchemeChanged"):
        window._color_scheme_changed_slot = window._sync_theme_to_system
        style_hints.colorSchemeChanged.connect(window._color_scheme_changed_slot)

    window.log_timer = QTimer(window)
    window.log_timer.timeout.connect(window._poll_log_queue)
    window.log_timer.start(50)
    window.ui_refresh_timer = QTimer(window)
    window.ui_refresh_timer.setSingleShot(True)
    window.ui_refresh_timer.timeout.connect(window._flush_deferred_ui_updates)
    window.operation_timer = QTimer(window)
    window.operation_timer.timeout.connect(window._refresh_live_operation_durations)
    window.operation_timer.start(100)
    window.daemon_timer = QTimer(window)
    window.daemon_timer.timeout.connect(window._sync_from_daemon)
    window.daemon_timer.start(500)


__all__ = ["bootstrap_gui_window", "initial_window_size_for_screen"]
