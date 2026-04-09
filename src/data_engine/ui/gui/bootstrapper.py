"""Bootstrap helpers for the desktop GUI application shell."""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from queue import Queue
from uuid import uuid4
from typing import TYPE_CHECKING

from PySide6.QtCore import QTimer
from PySide6.QtGui import QGuiApplication

from data_engine.domain import DaemonStatusState, FlowLogEntry, OperationSessionState, OperatorSessionState, StepOutputIndex
from data_engine.platform.identity import APP_DISPLAY_NAME, APP_INTERNAL_ID
from data_engine.platform.workspace_models import DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR
from data_engine.ui.gui.bootstrap import GuiServices
from data_engine.ui.gui.controllers import GuiFlowController, GuiRuntimeController
from data_engine.ui.gui.helpers import register_client_session as helper_register_client_session
from data_engine.ui.gui.runtime import QueueLogHandler, UiSignals
from data_engine.ui.gui.surface import build_default_gui_services

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def bootstrap_gui_window(window: "DataEngineWindow", *, theme_name: str, services: GuiServices | None = None) -> None:
    """Bind one GUI window to its services, runtime state, and timers."""
    window.setWindowTitle(APP_DISPLAY_NAME)
    window.resize(1480, 920)
    window.setMinimumSize(1180, 760)
    window.services = services or build_default_gui_services(theme_name)
    window.workspace_service = window.services.workspace_service
    window.action_state_application = window.services.action_state_application
    window.detail_application = window.services.detail_application
    window.daemon_service = window.services.daemon_service
    window.daemon_state_service = window.services.daemon_state_service
    window.runtime_application = window.services.runtime_application
    window.control_application = window.services.control_application
    window.ledger_service = window.services.ledger_service
    window.log_service = window.services.log_service
    window.runtime_binding_service = window.services.runtime_binding_service
    window.runtime_history_service = window.services.runtime_history_service
    window.shared_state_service = window.services.shared_state_service
    window.settings_service = window.services.settings_service
    window.theme_service = window.services.theme_service
    window.workspace_provisioning_service = window.services.workspace_provisioning_service
    window.workspace_session_application = window.services.workspace_session_application
    window.flow_catalog_application = window.services.flow_catalog_application
    window.flow_controller = GuiFlowController(
        workspace_session_application=window.workspace_session_application,
        flow_catalog_application=window.flow_catalog_application,
        log_service=window.log_service,
    )
    window.runtime_controller = GuiRuntimeController(
        runtime_application=window.runtime_application,
        daemon_service=window.daemon_service,
        log_service=window.log_service,
    )
    window.theme_name = window.theme_service.resolve_name(theme_name)
    env_collection_root = os.environ.get(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR)
    initial_override = None if env_collection_root and env_collection_root.strip() else window.settings_service.workspace_collection_root()
    window.workspace_paths = window.workspace_service.resolve_paths(
        workspace_collection_root=initial_override,
    )
    window._operator_session_state = OperatorSessionState.from_paths(window.workspace_paths, override_root=initial_override)
    window.workspace_session_state = window.workspace_session_application.refresh_session(
        workspace_paths=window.workspace_paths,
        override_root=initial_override,
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
    window.signals.docs_build_finished.connect(window._finish_docs_build)
    window.signals.daemon_startup_finished.connect(window._finish_daemon_startup)

    window.engine_runtime_stop_event = threading.Event()
    window.engine_flow_stop_event = threading.Event()
    window.manual_flow_stop_events: dict[str, threading.Event] = {}
    window.operation_row_widgets = []
    window.operation_tracker = OperationSessionState.empty()
    window.operation_flash_timers: list[QTimer] = []
    window.sidebar_flow_widgets: dict[str, QFrame] = {}
    window.sidebar_group_widgets: dict[str, QFrame] = {}
    window.step_output_index = StepOutputIndex.empty()
    window.runtime_binding = window.runtime_binding_service.open_binding(window.workspace_paths)
    helper_register_client_session(window)
    window.output_preview_dialog = None
    window.config_preview_dialog = None
    window.run_log_preview_dialog = None
    window._docs_root_dir = None
    window.docs_uses_webengine = False
    window._docs_build_running = False
    window.ui_closing = False
    window._log_view_refresh_pending = False
    window._action_buttons_refresh_pending = False
    window._worker_threads: set[threading.Thread] = set()
    window._worker_threads_lock = threading.RLock()
    window._daemon_status = DaemonStatusState.empty()
    window._last_daemon_spawn_attempt = 0.0
    window._auto_daemon_enabled = False
    window._daemon_startup_in_progress = False
    window._pending_message_box: tuple[str, str, str] | None = None
    window._message_box_scheduled = False
    window._message_box_open = False
    window._message_box_generation = 0
    window._workspace_switch_generation = 0

    window._build_window()
    window._reload_workspace_options()
    window._load_flows()
    style_hints = QGuiApplication.styleHints()
    if hasattr(style_hints, "colorSchemeChanged"):
        style_hints.colorSchemeChanged.connect(window._sync_theme_to_system)

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


__all__ = ["bootstrap_gui_window"]
