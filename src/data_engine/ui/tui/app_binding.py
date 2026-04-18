"""Bootstrap helpers for the terminal TUI application shell."""

from __future__ import annotations

import os
import threading
from queue import Queue
from uuid import uuid4
from typing import TYPE_CHECKING

from data_engine.domain import FlowLogEntry, OperationSessionState, OperatorSessionState, WorkspaceSessionState
from data_engine.platform.workspace_models import DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR
from data_engine.ui.tui.bootstrap import TuiServices, build_tui_services, default_tui_service_kwargs
from data_engine.ui.tui.controllers import TuiFlowController, TuiRuntimeController
from data_engine.ui.tui.runtime import QueueLogHandler
from data_engine.ui.tui.theme import stylesheet as tui_stylesheet

if TYPE_CHECKING:
    from data_engine.ui.tui.app import DataEngineTui


def resolve_initial_tui_workspace_collection_root_override(settings_service):
    """Resolve the initial workspace collection root override for one TUI process."""
    env_collection_root = os.environ.get(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR)
    return None if env_collection_root and env_collection_root.strip() else settings_service.workspace_collection_root()


def build_initial_tui_app_state(
    *,
    workspace_service,
    runtime_binding_service,
    settings_service,
) -> dict[str, object]:
    """Build the initial runtime/session state needed to boot the TUI surface."""
    initial_override = resolve_initial_tui_workspace_collection_root_override(settings_service)
    workspace_paths = workspace_service.resolve_paths(
        workspace_collection_root=initial_override,
    )
    operator_session_state = OperatorSessionState.from_paths(workspace_paths, override_root=initial_override)
    discovered = workspace_service.discover(
        app_root=workspace_paths.app_root,
        workspace_collection_root=initial_override,
    )
    workspace_session_state = WorkspaceSessionState.from_paths(
        workspace_paths,
        override_root=initial_override,
        discovered_workspace_ids=(item.workspace_id for item in discovered),
    )
    client_session_id = uuid4().hex
    runtime_binding = runtime_binding_service.open_binding(workspace_paths)
    return {
        "workspace_paths": workspace_paths,
        "operator_session_state": operator_session_state,
        "workspace_session_state": workspace_session_state,
        "operation_tracker": OperationSessionState.empty(),
        "runtime_binding": runtime_binding,
        "client_session_id": client_session_id,
    }


def bootstrap_tui_app(app: "DataEngineTui", *, theme_name: str, services: TuiServices | None = None) -> None:
    """Bind one TUI app shell to its services, session state, and runtime objects."""
    app.services = services or build_tui_services(
        **default_tui_service_kwargs(theme_name),
        client_error_type=Exception,
    )
    app.catalog_query_service = app.services.catalog_query_service
    app.history_query_service = app.services.history_query_service
    app.command_service = app.services.command_service
    app.daemon_service = app.services.daemon_service
    app.daemon_state_service = app.services.daemon_state_service
    app.ledger_service = app.services.ledger_service
    app.log_service = app.services.log_service
    app.runtime_binding_service = app.services.runtime_binding_service
    app.runtime_state_service = app.services.runtime_state_service
    app.shared_state_service = app.services.shared_state_service
    app.settings_service = app.services.settings_service
    app.theme_service = app.services.theme_service
    app.flow_controller = TuiFlowController(
        workspace_service=app.services.workspace_service,
        catalog_query_service=app.catalog_query_service,
        history_query_service=app.history_query_service,
        command_service=app.command_service,
    )
    app.runtime_controller = TuiRuntimeController(
        runtime_application=app.services.runtime_application,
        daemon_service=app.daemon_service,
        history_query_service=app.history_query_service,
        runtime_state_service=app.runtime_state_service,
    )
    app.theme_name = app.theme_service.resolve_name(theme_name)
    app.CSS = tui_stylesheet(app.theme_name)
    initial_state = build_initial_tui_app_state(
        workspace_service=app.services.workspace_service,
        runtime_binding_service=app.runtime_binding_service,
        settings_service=app.settings_service,
    )
    app.workspace_paths = initial_state["workspace_paths"]
    app._operator_session_state = initial_state["operator_session_state"]
    app.workspace_session_state = initial_state["workspace_session_state"]
    app.runtime_binding = initial_state["runtime_binding"]
    app.client_session_id = initial_state["client_session_id"]
    app.operation_tracker = initial_state["operation_tracker"]
    app.workspace_snapshot = None
    app.log_queue: Queue[FlowLogEntry] = Queue()
    app.log_handler = QueueLogHandler(app.log_queue)
    app._daemon_wait_stop_event = threading.Event()
    app._daemon_wait_started = False
    app._last_daemon_spawn_attempt = 0.0
    app._daemon_startup_in_progress = False
    app._workspace_switch_suppressed = False
    app.selected_run_key: tuple[str, str] | None = None
    app._last_rendered_flow_signature = None
    app._last_run_list_signature = None
    app._last_detail_signature = None


__all__ = [
    "bootstrap_tui_app",
    "build_initial_tui_app_state",
    "resolve_initial_tui_workspace_collection_root_override",
]
